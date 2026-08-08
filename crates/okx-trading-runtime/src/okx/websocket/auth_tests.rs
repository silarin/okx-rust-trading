use anyhow::Result;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

#[test]
fn login_request_uses_okx_verify_signature_shape() -> Result<()> {
    let credentials = OkxWebsocketLoginCredentials {
        api_key: "api-key",
        api_secret: "secret",
        api_passphrase: "passphrase",
    };
    let request = login_request_at(&credentials, "1538054050")?;
    let value: serde_json::Value = serde_json::from_str(&request)?;

    assert_eq!(
        value,
        json!({
            "op": "login",
            "args": [{
                "apiKey": "api-key",
                "passphrase": "passphrase",
                "timestamp": "1538054050",
                "sign": "Gj2hQIVKFcXbiwCak8SmVOu5mxPCizWDdmUAhbx8Z+s="
            }]
        })
    );
    Ok(())
}

#[test]
fn login_request_rejects_malformed_timestamps() {
    let credentials = OkxWebsocketLoginCredentials {
        api_key: "api-key",
        api_secret: "secret",
        api_passphrase: "passphrase",
    };

    for timestamp in ["", " 1538054050", "1538054050.123"] {
        let error = login_request_at(&credentials, timestamp)
            .expect_err("invalid WebSocket login timestamp should fail closed");

        assert!(
            error.to_string().contains("OKX WebSocket login timestamp"),
            "invalid timestamp should be reported clearly: {error}"
        );
    }
}

#[test]
fn login_ack_parser_accepts_successful_login_event() -> Result<()> {
    assert!(parse_login_ack(
        r#"{"event":"login","code":"0","msg":""}"#,
        "private"
    )?);
    assert!(!parse_login_ack(
        r#"{"event":"subscribe","arg":{"channel":"orders","instId":"BTC-USDT"}}"#,
        "private"
    )?);
    Ok(())
}

#[test]
fn login_ack_parser_rejects_error_frames_and_failed_login() {
    for (payload, expected_code) in [
        (
            r#"{"event":"error","code":"60012","msg":"Invalid request"}"#,
            "60012",
        ),
        (
            r#"{"event":"login","code":"60009","msg":"Login failed"}"#,
            "60009",
        ),
    ] {
        let error = parse_login_ack(payload, "private").unwrap_err();

        assert_eq!(
            error,
            OkxWebsocketProtocolError::LoginRejected {
                context: "private".to_owned(),
                code: expected_code.to_owned(),
                msg: if expected_code == "60012" {
                    "Invalid request".to_owned()
                } else {
                    "Login failed".to_owned()
                },
            }
        );
        assert!(error.to_string().contains("WebSocket"));
    }
}

#[test]
fn login_ack_parser_reports_malformed_json_without_raw_payload() {
    let payload = r#"{"event":"login","apiKey":"secret-key""#;
    let error = parse_login_ack(payload, "private").unwrap_err();

    assert!(matches!(
        error,
        OkxWebsocketProtocolError::MalformedJson { .. }
    ));
    assert!(!error.to_string().contains("secret-key"));
}
