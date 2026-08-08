use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::{notice::reject_websocket_notice, protocol_error::OkxWebsocketProtocolError};

type HmacSha256 = Hmac<Sha256>;

const OKX_WEBSOCKET_LOGIN_PATH: &str = "/users/self/verify";

pub(crate) struct OkxWebsocketLoginCredentials<'a> {
    pub(crate) api_key: &'a str,
    pub(crate) api_secret: &'a str,
    pub(crate) api_passphrase: &'a str,
}

pub(super) fn validate_websocket_login_credential(context: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    ensure!(!trimmed.is_empty(), "{context} must not be empty");
    ensure!(
        value == trimmed,
        "{context} must not contain leading or trailing whitespace"
    );
    ensure!(
        !value.contains(['\n', '\r']),
        "{context} must not contain embedded newlines"
    );
    Ok(())
}

pub(super) fn parse_login_ack(
    payload: &str,
    context: &str,
) -> Result<bool, OkxWebsocketProtocolError> {
    let message: OkxWebsocketControlMessage = serde_json::from_str(payload).map_err(|error| {
        OkxWebsocketProtocolError::MalformedJson {
            context: context.to_owned(),
            parser_error: error.to_string(),
        }
    })?;
    reject_websocket_notice(message.event.as_deref(), message.code.as_deref(), context)?;
    if message.event.as_deref() == Some("error") {
        return Err(OkxWebsocketProtocolError::LoginRejected {
            context: context.to_owned(),
            code: message.code.unwrap_or_default(),
            msg: message.msg.unwrap_or_default(),
        });
    }
    if message.event.as_deref() != Some("login") {
        return Ok(false);
    }
    if message.code.as_deref() != Some("0") {
        return Err(OkxWebsocketProtocolError::LoginRejected {
            context: context.to_owned(),
            code: message.code.unwrap_or_default(),
            msg: message.msg.unwrap_or_default(),
        });
    }
    Ok(true)
}

pub(crate) fn login_request_at(
    credentials: &OkxWebsocketLoginCredentials<'_>,
    timestamp: &str,
) -> Result<String> {
    ensure!(
        !timestamp.trim().is_empty() && timestamp == timestamp.trim(),
        "OKX WebSocket login timestamp must be non-empty and trimmed"
    );
    ensure!(
        timestamp.chars().all(|digit| digit.is_ascii_digit()),
        "OKX WebSocket login timestamp must be Unix seconds"
    );
    let request = OkxWebsocketLoginRequest {
        op: "login",
        args: vec![OkxWebsocketLoginArg {
            api_key: credentials.api_key,
            passphrase: credentials.api_passphrase,
            timestamp,
            sign: okx_websocket_login_signature(timestamp, credentials.api_secret)?,
        }],
    };
    serde_json::to_string(&request).context("failed serializing OKX WebSocket login request")
}

fn okx_websocket_login_signature(timestamp: &str, api_secret: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes())
        .context("invalid OKX WebSocket HMAC key")?;
    let payload = format!("{timestamp}GET{OKX_WEBSOCKET_LOGIN_PATH}");
    mac.update(payload.as_bytes());
    Ok(BASE64.encode(mac.finalize().into_bytes()))
}

#[derive(Serialize)]
struct OkxWebsocketLoginRequest<'a> {
    op: &'static str,
    args: Vec<OkxWebsocketLoginArg<'a>>,
}

#[derive(Serialize)]
struct OkxWebsocketLoginArg<'a> {
    #[serde(rename = "apiKey")]
    api_key: &'a str,
    passphrase: &'a str,
    timestamp: &'a str,
    sign: String,
}

#[derive(Deserialize)]
struct OkxWebsocketControlMessage {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
