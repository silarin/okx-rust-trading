use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::{SinkExt as _, StreamExt as _};
use pretty_assertions::assert_eq;
use serde_json::json;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use crate::{
    okx::{client::OkxWebsocketLoginTimestampProvider, websocket::OkxWebsocketHealthReceiver},
    test_support::CapturedLogs,
};

use super::*;

const TEST_WEBSOCKET_TIMEOUT: Duration = Duration::from_secs(1);

type TestWebSocket = tokio_tungstenite::WebSocketStream<TcpStream>;

fn test_event_authority() -> OkxPrivateEventAuthority {
    OkxPrivateEventAuthority {
        instrument_ids: BTreeSet::from(["BTC-USDT".to_owned()]),
        instrument_type: OKX_SPOT_INST_TYPE.to_owned(),
        algo_subscription_selector: OkxAlgoSubscriptionSelector::Spot,
        stream_kind: OkxPrivateStreamKind::Trading,
    }
}

fn apply_private_event_message(
    cache: &OkxPrivateEventCache,
    payload: &str,
    received_at: Instant,
) -> Result<usize> {
    Ok(
        apply_private_event_message_inner(&test_event_authority(), cache, payload, received_at)?
            .count,
    )
}

fn parse_private_event_message(
    payload: &str,
    received_at: Instant,
) -> Result<Vec<OkxPrivateEventHint>> {
    parse_private_event_message_with_authority(&test_event_authority(), payload, received_at)
}

#[test]
fn private_stream_credentials_reject_surrounding_whitespace() {
    for (api_key, api_secret, api_passphrase) in [
        (" api-key", "secret", "passphrase"),
        ("api-key ", "secret", "passphrase"),
        ("api-key", " secret", "passphrase"),
        ("api-key", "secret ", "passphrase"),
        ("api-key", "secret", " passphrase"),
        ("api-key", "secret", "passphrase "),
    ] {
        let error = OkxPrivateStreamCredentials::new(
            api_key.to_owned(),
            api_secret.to_owned(),
            api_passphrase.to_owned(),
        )
        .expect_err("padded private WebSocket credentials should fail");

        assert!(
            error.to_string().contains("leading or trailing whitespace"),
            "padded private WebSocket credentials should report whitespace: {error}"
        );
    }
}

#[test]
fn private_stream_credentials_reject_empty_or_newline_values() {
    for (api_key, api_secret, api_passphrase, expected_message) in [
        ("", "secret", "passphrase", "must not be empty"),
        ("api-key", "", "passphrase", "must not be empty"),
        ("api-key", "secret", "", "must not be empty"),
        (
            "api\nkey",
            "secret",
            "passphrase",
            "must not contain embedded newlines",
        ),
        (
            "api-key",
            "sec\rret",
            "passphrase",
            "must not contain embedded newlines",
        ),
        (
            "api-key",
            "secret",
            "pass\nphrase",
            "must not contain embedded newlines",
        ),
    ] {
        let error = OkxPrivateStreamCredentials::new(
            api_key.to_owned(),
            api_secret.to_owned(),
            api_passphrase.to_owned(),
        )
        .expect_err("invalid private WebSocket credentials should fail");

        assert!(
            error.to_string().contains(expected_message),
            "invalid private WebSocket credentials should report {expected_message:?}: {error}"
        );
    }
}

#[test]
fn private_stream_credentials_debug_redacts_secret_fields() -> Result<()> {
    let credentials = OkxPrivateStreamCredentials::new(
        "debug-api-key".to_owned(),
        "debug-api-secret".to_owned(),
        "debug-api-passphrase".to_owned(),
    )?;

    let debug = format!("{credentials:?}");

    assert!(debug.contains("OkxPrivateStreamCredentials"));
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("debug-api-key"));
    assert!(!debug.contains("debug-api-secret"));
    assert!(!debug.contains("debug-api-passphrase"));
    Ok(())
}

#[test]
fn private_stream_config_debug_redacts_credentials() -> Result<()> {
    let credentials = Arc::new(OkxPrivateStreamCredentials::new(
        "config-api-key".to_owned(),
        "config-api-secret".to_owned(),
        "config-api-passphrase".to_owned(),
    )?);
    let config = OkxPrivateStreamConfig::with_reconnect_policy(
        "wss://example.test/ws/v5/private".to_owned(),
        OkxPrivateStreamKind::Trading,
        vec!["BTC-USDT".to_owned()],
        OkxApiDomain::Global,
        credentials,
        OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
    )?;

    let debug = format!("{config:?}");

    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("config-api-key"));
    assert!(!debug.contains("config-api-secret"));
    assert!(!debug.contains("config-api-passphrase"));
    Ok(())
}

#[test]
fn cloned_private_stream_config_shares_credential_handle() -> Result<()> {
    let config = private_stream_config("wss://example.test/ws/v5/private".to_owned())?;
    let cloned = config.clone();

    assert!(Arc::ptr_eq(&config.credentials, &cloned.credentials));
    Ok(())
}

#[test]
fn dropping_cloned_private_stream_configs_leaves_last_credential_owner() -> Result<()> {
    let config = private_stream_config("wss://example.test/ws/v5/private".to_owned())?;
    let cloned = config.clone();
    let credentials = Arc::clone(&config.credentials);

    assert_eq!(Arc::strong_count(&credentials), 3);
    drop(config);
    drop(cloned);

    assert_eq!(Arc::strong_count(&credentials), 1);
    Ok(())
}

#[test]
fn private_trading_subscription_uses_orders_and_fills_channels() -> Result<()> {
    let mut config = private_stream_config("wss://example.test/ws/v5/private".to_owned())?;
    config.instrument_ids = vec!["BTC-USDT".to_owned(), "ETH-USDT".to_owned()];
    let subscription = private_stream_subscription(&config)?;
    let value: serde_json::Value = serde_json::from_str(&subscription)?;

    assert_eq!(
        value,
        json!({
            "op": "subscribe",
            "args": [
                {"channel": "account"},
                {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
                {"channel": "fills", "instId": "BTC-USDT"},
                {"channel": "orders", "instType": "SPOT", "instId": "ETH-USDT"},
                {"channel": "fills", "instId": "ETH-USDT"}
            ]
        })
    );
    Ok(())
}

#[test]
fn private_trading_subscription_can_omit_optional_fills_channel() -> Result<()> {
    let config = private_stream_config("wss://example.test/ws/v5/private".to_owned())?
        .without_optional_fills();
    let subscription = private_stream_subscription(&config)?;
    let value: serde_json::Value = serde_json::from_str(&subscription)?;

    assert_eq!(
        value,
        json!({
            "op": "subscribe",
            "args": [
                {"channel": "account"},
                {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"}
            ]
        })
    );
    Ok(())
}

#[test]
fn private_business_subscription_selector_is_derived_from_api_domain() -> Result<()> {
    for (api_domain, expected_selector) in [
        (OkxApiDomain::Eea, "SPOT"),
        (OkxApiDomain::Global, "ANY"),
        (OkxApiDomain::UsAu, "ANY"),
    ] {
        let config = private_business_stream_config(
            "wss://example.test/ws/v5/business".to_owned(),
            api_domain,
        )?;
        let subscription = private_stream_subscription(&config)?;
        let value: serde_json::Value = serde_json::from_str(&subscription)?;

        assert_eq!(
            value,
            json!({
                "op": "subscribe",
                "args": [
                    {
                        "channel": "orders-algo",
                        "instType": expected_selector,
                        "instId": "BTC-USDT"
                    }
                ]
            })
        );
        assert_eq!(
            private_stream_subscription_acks(&config),
            BTreeSet::from([OkxWebsocketSubscriptionAck {
                channel: "orders-algo".to_owned(),
                inst_id: Some("BTC-USDT".to_owned()),
                inst_type: Some(expected_selector.to_owned()),
            }])
        );
    }
    Ok(())
}

#[test]
fn private_business_subscription_ack_requires_exact_selector_and_instrument() -> Result<()> {
    let config = private_business_stream_config(
        "wss://example.test/ws/v5/business".to_owned(),
        OkxApiDomain::Global,
    )?;
    let expected = private_stream_subscription_acks(&config);

    for unexpected in [
        OkxWebsocketSubscriptionAck {
            channel: "orders-algo".to_owned(),
            inst_id: Some("BTC-USDT".to_owned()),
            inst_type: Some("SPOT".to_owned()),
        },
        OkxWebsocketSubscriptionAck {
            channel: "orders-algo".to_owned(),
            inst_id: Some("ETH-USDT".to_owned()),
            inst_type: Some("ANY".to_owned()),
        },
    ] {
        let mut pending = expected.clone();
        let error = acknowledge_subscription(&mut pending, unexpected, "business")
            .expect_err("wrong algo selector or instrument must not establish readiness");
        assert!(
            error.to_string().contains("unexpected subscription"),
            "wrong algo acknowledgement should fail closed: {error}"
        );
        assert_eq!(pending, expected);
    }

    let mut pending = expected;
    acknowledge_subscription(
        &mut pending,
        OkxWebsocketSubscriptionAck {
            channel: "orders-algo".to_owned(),
            inst_id: Some("BTC-USDT".to_owned()),
            inst_type: Some("ANY".to_owned()),
        },
        "business",
    )?;
    assert!(pending.is_empty());
    Ok(())
}

#[test]
fn private_business_reconnect_reuses_the_same_domain_selector_without_fallback() -> Result<()> {
    for api_domain in [OkxApiDomain::Global, OkxApiDomain::UsAu, OkxApiDomain::Eea] {
        let config = private_business_stream_config(
            "wss://example.test/ws/v5/business".to_owned(),
            api_domain,
        )?;
        let first_generation = private_stream_subscription(&config)?;
        let first_generation_acks = private_stream_subscription_acks(&config);
        let reconnect_generation = private_stream_subscription(&config)?;
        let reconnect_generation_acks = private_stream_subscription_acks(&config);

        assert_eq!(reconnect_generation, first_generation);
        assert_eq!(reconnect_generation_acks, first_generation_acks);
    }
    Ok(())
}

#[tokio::test]
async fn private_stream_sends_text_ping_after_idle() -> Result<()> {
    let (url, received) = spawn_private_server_with_idle_pong().await?;
    let config = private_stream_config(url)?;
    let logs = CapturedLogs::default();
    let dispatch = logs.dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let received = await_test_websocket_server(received).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    assert_eq!(received[2], crate::okx::websocket::OKX_WEBSOCKET_TEXT_PING);
    let logs = logs.contents();
    assert!(
        logs.contains("ws_private_subscription_success"),
        "private subscription log should include safety event: {logs}"
    );
    Ok(())
}

#[tokio::test]
async fn private_stream_login_uses_timestamp_provider() -> Result<()> {
    let (url, received) = spawn_private_server_with_idle_pong().await?;
    let config = private_stream_config(url)?;
    let login_timestamp_provider = OkxWebsocketLoginTimestampProvider::fixed("4102444810");

    let outcome = run_private_stream_once_with_login_timestamp_provider(
        &config,
        OkxPrivateEventCache::default(),
        &login_timestamp_provider,
        None,
    )
    .await;
    let received = await_test_websocket_server(received).await?;
    let login: serde_json::Value = serde_json::from_str(&received[0])?;

    assert!(outcome.subscribed());
    assert_eq!(login["args"][0]["timestamp"], "4102444810");
    Ok(())
}

#[tokio::test]
async fn private_timing_override_accepts_readiness_after_unit_test_deadlines() -> Result<()> {
    let (url, received) = spawn_private_server_with_delayed_readiness().await?;
    let config = private_stream_config(url)?;
    let login_timestamp_provider = OkxWebsocketLoginTimestampProvider::fixed("4102444810");
    let timing = OkxPrivateStreamTiming::new(
        TEST_WEBSOCKET_TIMEOUT,
        TEST_WEBSOCKET_TIMEOUT,
        TEST_WEBSOCKET_TIMEOUT,
        TEST_WEBSOCKET_TIMEOUT,
    )?;

    let outcome = run_private_stream_once_with_login_timestamp_provider_and_timing(
        &config,
        OkxPrivateEventCache::default(),
        &login_timestamp_provider,
        None,
        timing,
    )
    .await;
    let _ = await_test_websocket_server(received).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    Ok(())
}

#[tokio::test]
async fn private_stream_reconnects_after_missing_idle_pong() -> Result<()> {
    let (url, received) = spawn_private_server_without_idle_pong().await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let error = outcome
        .error()
        .expect("missing idle pong should force reconnect");
    let received = await_test_websocket_server(received).await?;

    assert!(outcome.subscribed());
    assert_eq!(received[2], crate::okx::websocket::OKX_WEBSOCKET_TEXT_PING);
    assert!(
        error.to_string().contains("idle pong"),
        "missing private idle pong should be reported clearly: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn private_stream_processes_text_data_after_idle_ping() -> Result<()> {
    let (url, received) = spawn_private_server_with_order_after_idle_ping().await?;
    let cache = OkxPrivateEventCache::default();
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, cache.clone()).await;
    let received = await_test_websocket_server(received).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    assert_eq!(received[2], crate::okx::websocket::OKX_WEBSOCKET_TEXT_PING);
    let order = cache
        .fresh_order("BTC-USDT", "entry-1", Duration::from_secs(1))
        .expect("order data frame should be processed after idle ping");
    assert_eq!(order.order.state, "live");
    Ok(())
}

#[tokio::test]
async fn private_stream_failure_before_login_or_subscribe_increases_backoff() -> Result<()> {
    let policy =
        OkxWebsocketReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40))?;
    let config = private_stream_config_with_policy("not-a-websocket-url".to_owned(), policy)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;

    assert!(!outcome.subscribed());
    assert!(outcome.error().is_some());
    assert_eq!(
        policy.backoff_after_stream_run(Duration::from_millis(10), &outcome),
        Duration::from_millis(20)
    );
    Ok(())
}

#[tokio::test]
async fn private_stream_after_successful_subscription_resets_backoff() -> Result<()> {
    let (url, received) = spawn_private_server_with_idle_pong().await?;
    let policy =
        OkxWebsocketReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40))?;
    let config = private_stream_config_with_policy(url, policy)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let _ = await_test_websocket_server(received).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    assert_eq!(
        policy.backoff_after_stream_run(Duration::from_millis(40), &outcome),
        Duration::from_millis(10)
    );
    Ok(())
}

#[tokio::test]
async fn private_stream_rejects_subscription_error_before_success() -> Result<()> {
    let (url, received) = spawn_private_server_with_subscription_error().await?;
    let config = private_stream_config(url)?;
    let logs = CapturedLogs::default();
    let dispatch = logs.dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let _ = await_test_websocket_server(received).await?;
    let error = outcome
        .error()
        .expect("subscription error should force reconnect before success");

    assert!(!outcome.subscribed());
    assert_eq!(
        protocol_error(error),
        &OkxWebsocketProtocolError::SubscriptionErrorEvent {
            context: "private".to_owned(),
            code: "60012".to_owned(),
            msg: "Invalid request".to_owned(),
            ack: Some(Box::new(OkxWebsocketSubscriptionAck {
                channel: "orders".to_owned(),
                inst_id: Some("BTC-USDT".to_owned()),
                inst_type: Some("SPOT".to_owned()),
            })),
        }
    );
    assert!(
        error.to_string().contains("OKX private WebSocket error"),
        "private subscription error should be reported clearly: {error}"
    );
    assert!(
        !logs.contents().contains("ws_private_subscription_success"),
        "private subscription success must not be logged before OKX ACK"
    );
    Ok(())
}

#[tokio::test]
async fn private_stream_tolerates_vip_only_fills_subscription_error() -> Result<()> {
    let (url, received) = spawn_private_server_with_vip_fills_subscription_error().await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let _ = await_test_websocket_server(received).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    Ok(())
}

#[tokio::test]
async fn private_login_ack_success_before_timeout_subscribes() -> Result<()> {
    let (url, received) = spawn_private_server_with_idle_pong().await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let received = await_test_websocket_server(received).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    assert!(received_contains_subscription(&received));
    Ok(())
}

#[tokio::test]
async fn private_login_error_before_timeout_fails_without_subscription() -> Result<()> {
    let (url, received) = spawn_private_server_with_login_messages(
        vec![Message::Text(
            r#"{"event":"login","code":"60009","msg":"Login failed"}"#.into(),
        )],
        None,
    )
    .await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let received = await_test_websocket_server(received).await?;
    let error = outcome.error().expect("login error should fail the stream");

    assert!(!outcome.subscribed());
    assert_eq!(
        protocol_error(error),
        &OkxWebsocketProtocolError::LoginRejected {
            context: "private".to_owned(),
            code: "60009".to_owned(),
            msg: "Login failed".to_owned(),
        }
    );
    assert!(
        error
            .to_string()
            .contains("OKX private WebSocket login failed 60009: Login failed"),
        "explicit OKX login failure should be preserved: {error}"
    );
    assert!(!received_contains_subscription(&received));
    Ok(())
}

#[tokio::test]
async fn private_login_stream_close_before_ack_fails_without_subscription() -> Result<()> {
    let (url, received) = spawn_private_server_with_login_messages(Vec::new(), None).await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let received = await_test_websocket_server(received).await?;
    let error = outcome.error().expect("stream close should fail login");

    assert!(!outcome.subscribed());
    assert!(matches!(
        protocol_error(error),
        OkxWebsocketProtocolError::ClosedBeforeLoginAck { context }
            if context == "private"
    ));
    assert!(
        error.to_string().contains("closed before login ACK"),
        "stream close before login ACK should be reported: {error}"
    );
    assert!(!received_contains_subscription(&received));
    Ok(())
}

#[tokio::test]
async fn private_login_malformed_notice_fails_before_subscription() -> Result<()> {
    let (url, received) = spawn_private_server_with_login_messages(
        vec![
            Message::Text(r#"{"event":"notice","msg":"maintenance"}"#.into()),
            Message::Text(r#"{"event":"subscribe","arg":{"channel":"orders"}}"#.into()),
        ],
        None,
    )
    .await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let received = await_test_websocket_server(received).await?;
    let error = outcome
        .error()
        .expect("malformed notice should fail login immediately");

    assert!(!outcome.subscribed());
    assert!(matches!(
        protocol_error(error),
        OkxWebsocketProtocolError::MalformedNotice { context }
            if context == "private"
    ));
    assert!(
        error
            .to_string()
            .contains("notice omitted its required code"),
        "malformed notice should report only its structural defect: {error}"
    );
    assert!(!received_contains_subscription(&received));
    Ok(())
}

#[tokio::test]
async fn private_login_upgrade_notice_fails_before_subscription() -> Result<()> {
    let (url, received) = spawn_private_server_with_login_messages(
        vec![Message::Text(okx_websocket_upgrade_notice().into())],
        None,
    )
    .await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let received = await_test_websocket_server(received).await?;
    let error = outcome
        .error()
        .expect("upgrade notice should fail login immediately");

    assert!(!outcome.subscribed());
    assert!(matches!(
        protocol_error(error),
        OkxWebsocketProtocolError::ServiceUpgradeNotice { context, code }
            if context == "private" && code == "64008"
    ));
    assert!(!received_contains_subscription(&received));
    Ok(())
}

#[tokio::test]
async fn private_login_irrelevant_control_frames_time_out() -> Result<()> {
    let (url, received) = spawn_private_server_with_login_messages(
        vec![
            Message::Pong(Vec::new().into()),
            Message::Pong(Vec::new().into()),
            Message::Pong(Vec::new().into()),
        ],
        Some(Duration::from_millis(150)),
    )
    .await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let received = await_test_websocket_server(received).await?;
    let error = outcome
        .error()
        .expect("irrelevant control frames should time out login");

    assert!(!outcome.subscribed());
    assert!(matches!(
        protocol_error(error),
        OkxWebsocketProtocolError::TimedOutWaitingForLoginAck { context }
            if context == "private"
    ));
    assert!(
        error
            .to_string()
            .contains("timed out waiting for OKX private WebSocket login ACK"),
        "irrelevant control frames should be bounded by the login timeout: {error}"
    );
    assert!(!received_contains_subscription(&received));
    Ok(())
}

#[tokio::test]
async fn private_login_malformed_frame_returns_parse_error() -> Result<()> {
    let (url, received) =
        spawn_private_server_with_login_messages(vec![Message::Text("{not-json}".into())], None)
            .await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let received = await_test_websocket_server(received).await?;
    let error = outcome
        .error()
        .expect("malformed login frame should fail parsing");

    assert!(!outcome.subscribed());
    assert!(matches!(
        protocol_error(error),
        OkxWebsocketProtocolError::MalformedJson { context, .. }
            if context == "private"
    ));
    assert!(
        error
            .to_string()
            .contains("failed parsing OKX private WebSocket protocol JSON"),
        "malformed login frames should fail parsing rather than hanging: {error}"
    );
    assert!(
        !error.to_string().contains("timed out waiting"),
        "malformed login frames should not be reported as a timeout: {error}"
    );
    assert!(!received_contains_subscription(&received));
    Ok(())
}

#[tokio::test]
async fn websocket_health_private_login_success_emits_login_ready_event() -> Result<()> {
    let (url, received) = spawn_private_server_with_idle_pong().await?;
    let config = private_stream_config(url)?;
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let outcome = run_private_stream_once_with_health(
        &config,
        OkxPrivateEventCache::default(),
        Some(&health),
    )
    .await;
    let _ = await_test_websocket_server(received).await?;
    let events = recv_health_events(&mut health_events, 5).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::LoginAckSucceeded,
        config.health_identity(),
    )));
    Ok(())
}

#[tokio::test]
async fn websocket_health_private_subscription_success_emits_ready_event() -> Result<()> {
    let (url, received) = spawn_private_server_with_idle_pong().await?;
    let config = private_stream_config(url)?;
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let outcome = run_private_stream_once_with_health(
        &config,
        OkxPrivateEventCache::default(),
        Some(&health),
    )
    .await;
    let _ = await_test_websocket_server(received).await?;
    let events = recv_health_events(&mut health_events, 5).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
        config.health_identity(),
    )));
    Ok(())
}

#[tokio::test]
async fn websocket_health_private_pre_login_failure_emits_failure_event() -> Result<()> {
    let (url, received) = spawn_private_server_with_login_messages(Vec::new(), None).await?;
    let config = private_stream_config(url)?;
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let outcome = run_private_stream_once_with_health(
        &config,
        OkxPrivateEventCache::default(),
        Some(&health),
    )
    .await;
    let _ = await_test_websocket_server(received).await?;
    let events = recv_health_events(&mut health_events, 4).await?;

    assert!(!outcome.subscribed());
    assert!(outcome.error().is_some());
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::LoginFailed,
        config.health_identity(),
    )));
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
        config.health_identity(),
    )));
    Ok(())
}

#[tokio::test]
async fn websocket_health_private_pre_subscription_failure_emits_failure_event() -> Result<()> {
    let (url, received) = spawn_private_server_with_subscription_error().await?;
    let config = private_stream_config(url)?;
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let outcome = run_private_stream_once_with_health(
        &config,
        OkxPrivateEventCache::default(),
        Some(&health),
    )
    .await;
    let _ = await_test_websocket_server(received).await?;
    let events = recv_health_events(&mut health_events, 5).await?;

    assert!(!outcome.subscribed());
    assert!(outcome.error().is_some());
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::SubscriptionAckFailed,
        config.health_identity(),
    )));
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
        config.health_identity(),
    )));
    Ok(())
}

#[tokio::test]
async fn websocket_health_private_post_subscription_disconnect_emits_event() -> Result<()> {
    let (url, received) = spawn_private_server_with_idle_pong().await?;
    let config = private_stream_config(url)?;
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let outcome = run_private_stream_once_with_health(
        &config,
        OkxPrivateEventCache::default(),
        Some(&health),
    )
    .await;
    let _ = await_test_websocket_server(received).await?;
    let events = recv_health_events(&mut health_events, 5).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription,
        config.health_identity(),
    )));
    Ok(())
}

#[tokio::test]
async fn private_stream_upgrade_notice_fails_after_subscription_readiness() -> Result<()> {
    let (url, received) = spawn_private_server_with_subscription_messages(vec![
        private_account_subscribe_ack(),
        private_orders_subscribe_ack_for("BTC-USDT"),
        private_fills_subscribe_ack_for("BTC-USDT"),
        Message::Text(okx_websocket_upgrade_notice().into()),
    ])
    .await?;
    let config = private_stream_config(url)?;
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let outcome = run_private_stream_once_with_health(
        &config,
        OkxPrivateEventCache::default(),
        Some(&health),
    )
    .await;
    let _ = await_test_websocket_server(received).await?;
    let events = recv_health_events(&mut health_events, 5).await?;
    let error = outcome
        .error()
        .expect("upgrade notice must terminate the ready private stream");

    assert!(outcome.subscribed());
    assert!(error.to_string().contains("service upgrade notice 64008"));
    assert!(!error.to_string().contains("sensitive-connection-id"));
    assert!(!error.to_string().contains("sensitive maintenance detail"));
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::StreamFailedAfterSubscription,
        config.health_identity(),
    )));
    Ok(())
}

#[tokio::test]
async fn business_stream_upgrade_notice_fails_after_subscription_readiness() -> Result<()> {
    let (url, received) = spawn_private_server_with_subscription_messages(vec![
        Message::Text(
            r#"{"event":"subscribe","arg":{"channel":"orders-algo","instType":"ANY","instId":"BTC-USDT"}}"#
                .into(),
        ),
        Message::Text(okx_websocket_upgrade_notice().into()),
    ])
    .await?;
    let config = private_business_stream_config(url, OkxApiDomain::Global)?;
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let outcome = run_private_stream_once_with_health(
        &config,
        OkxPrivateEventCache::default(),
        Some(&health),
    )
    .await;
    let _ = await_test_websocket_server(received).await?;
    let events = recv_health_events(&mut health_events, 5).await?;
    let error = outcome
        .error()
        .expect("upgrade notice must terminate the ready business stream");

    assert!(outcome.subscribed());
    assert!(error.to_string().contains("service upgrade notice 64008"));
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::StreamFailedAfterSubscription,
        config.health_identity(),
    )));
    Ok(())
}

#[tokio::test]
async fn business_stream_upgrade_notice_fails_before_subscription_readiness() -> Result<()> {
    let (url, received) = spawn_private_server_with_subscription_messages(vec![Message::Text(
        okx_websocket_upgrade_notice().into(),
    )])
    .await?;
    let config = private_business_stream_config(url, OkxApiDomain::Global)?;
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let outcome = run_private_stream_once_with_health(
        &config,
        OkxPrivateEventCache::default(),
        Some(&health),
    )
    .await;
    let _ = await_test_websocket_server(received).await?;
    let events = recv_health_events(&mut health_events, 5).await?;
    let error = outcome
        .error()
        .expect("upgrade notice must fail the pre-ready business stream");

    assert!(!outcome.subscribed());
    assert!(matches!(
        protocol_error(error),
        OkxWebsocketProtocolError::ServiceUpgradeNotice { context, code }
            if context == "business" && code == "64008"
    ));
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::SubscriptionAckFailed,
        config.health_identity(),
    )));
    assert!(events.contains(&OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
        config.health_identity(),
    )));
    Ok(())
}

#[tokio::test]
async fn websocket_health_private_task_panic_emits_supervision_event() -> Result<()> {
    let stream_identity = OkxWebsocketStreamIdentity::new(
        OkxWebsocketStreamKind::Private,
        OkxWebsocketChannelClass::PrivateTrading,
        1,
    );
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let stream = OkxPrivateStream::spawn_test_task(stream_identity, Some(health), async {
        panic!("simulated private WebSocket task panic");
    });
    let event = recv_health_event_kind(
        &mut health_events,
        OkxWebsocketHealthEventKind::StreamTaskPanicked,
    )
    .await?;
    drop(stream);

    assert_eq!(
        event,
        OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::StreamTaskPanicked,
            stream_identity
        )
    );
    Ok(())
}

#[tokio::test]
async fn websocket_health_private_task_completion_emits_supervision_event() -> Result<()> {
    let stream_identity = OkxWebsocketStreamIdentity::new(
        OkxWebsocketStreamKind::Private,
        OkxWebsocketChannelClass::PrivateTrading,
        1,
    );
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let stream = OkxPrivateStream::spawn_test_task(stream_identity, Some(health), async {});
    let event = recv_health_event_kind(
        &mut health_events,
        OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly,
    )
    .await?;
    drop(stream);

    assert_eq!(
        event,
        OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly,
            stream_identity
        )
    );
    Ok(())
}

#[tokio::test]
async fn websocket_health_private_drop_aborts_without_supervision_event() -> Result<()> {
    let stream_identity = OkxWebsocketStreamIdentity::new(
        OkxWebsocketStreamKind::Private,
        OkxWebsocketChannelClass::PrivateTrading,
        1,
    );
    let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

    let stream = OkxPrivateStream::spawn_test_task(
        stream_identity,
        Some(health),
        std::future::pending::<()>(),
    );
    drop(stream);
    let event = tokio_time::timeout(Duration::from_millis(50), health_events.recv()).await;

    assert!(
        !matches!(event, Ok(Some(_))),
        "intentional stream drop should not emit a fatal task lifecycle event: {event:?}"
    );
    Ok(())
}

#[tokio::test]
async fn websocket_subscription_ack_private_missing_ack_times_out_before_ready() -> Result<()> {
    let (url, received) = spawn_private_server_without_subscription_ack().await?;
    let config = private_stream_config(url)?;
    let logs = CapturedLogs::default();
    let dispatch = logs.dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let received = await_test_websocket_server(received).await?;
    let error = outcome
        .error()
        .expect("missing private subscription ACK should force reconnect before readiness");

    assert!(!outcome.subscribed());
    assert!(matches!(
        protocol_error(error),
        OkxWebsocketProtocolError::TimedOutWaitingForSubscriptionAck { context }
            if context == "private"
    ));
    assert!(
        received
            .iter()
            .any(|payload| payload == crate::okx::websocket::OKX_WEBSOCKET_TEXT_PING)
    );
    assert!(
        error.to_string().contains("subscription ACK"),
        "missing private subscription ACK should time out before readiness: {error}"
    );
    assert!(
        !logs.contents().contains("ws_private_subscription_success"),
        "private stream must not report readiness without subscription ACKs"
    );
    Ok(())
}

#[tokio::test]
async fn websocket_subscription_ack_private_wrong_instrument_fails_before_ready() -> Result<()> {
    let (url, received) = spawn_private_server_with_subscription_messages(vec![
        private_account_subscribe_ack(),
        private_orders_subscribe_ack_for("ETH-USDT"),
    ])
    .await?;
    let policy =
        OkxWebsocketReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40))?;
    let config = private_stream_config_with_policy(url, policy)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let _ = await_test_websocket_server(received).await?;
    let error = outcome
        .error()
        .expect("wrong private instrument ACK should fail before readiness");

    assert!(!outcome.subscribed());
    assert_eq!(
        protocol_error(error),
        &OkxWebsocketProtocolError::UnexpectedSubscriptionAck {
            context: "private".to_owned(),
            ack: Box::new(OkxWebsocketSubscriptionAck {
                channel: "orders".to_owned(),
                inst_id: Some("ETH-USDT".to_owned()),
                inst_type: Some("SPOT".to_owned()),
            }),
        }
    );
    assert!(
        error.to_string().contains("unexpected subscription"),
        "wrong private instrument ACK should be rejected: {error}"
    );
    assert_eq!(
        policy.backoff_after_stream_run(Duration::from_millis(10), &outcome),
        Duration::from_millis(20)
    );
    Ok(())
}

#[tokio::test]
async fn websocket_subscription_ack_private_data_before_ack_is_not_ready() -> Result<()> {
    let (url, received) =
        spawn_private_server_with_subscription_messages(vec![private_order_data_frame()]).await?;
    let cache = OkxPrivateEventCache::default();
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, cache.clone()).await;
    let _ = await_test_websocket_server(received).await?;
    let error = outcome
        .error()
        .expect("data before ACK should force reconnect before readiness");

    assert!(!outcome.subscribed());
    assert!(matches!(
        protocol_error(error),
        OkxWebsocketProtocolError::DataBeforeSubscriptionAck { context, ack }
            if context == "private" && ack.channel == "orders" && ack.inst_id.as_deref() == Some("BTC-USDT")
    ));
    assert!(
        error.to_string().contains("subscription ACK"),
        "data before ACK should not make the private stream ready: {error}"
    );
    assert_eq!(
        cache.fresh_order("BTC-USDT", "entry-1", Duration::from_secs(1)),
        None
    );
    Ok(())
}

#[tokio::test]
async fn websocket_subscription_ack_private_data_then_ack_still_fails_before_ready() -> Result<()> {
    let (url, received) = spawn_private_server_with_subscription_messages(vec![
        private_order_data_frame(),
        private_account_subscribe_ack(),
        private_orders_subscribe_ack_for("BTC-USDT"),
        private_fills_subscribe_ack_for("BTC-USDT"),
    ])
    .await?;
    let cache = OkxPrivateEventCache::default();
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, cache.clone()).await;
    let _ = await_test_websocket_server(received).await?;
    let error = outcome
        .error()
        .expect("data before ACK should fail even when ACKs follow");

    assert!(!outcome.subscribed());
    assert!(matches!(
        protocol_error(error),
        OkxWebsocketProtocolError::DataBeforeSubscriptionAck { context, ack }
            if context == "private" && ack.channel == "orders" && ack.inst_id.as_deref() == Some("BTC-USDT")
    ));
    assert!(
        error.to_string().contains("subscription ACK"),
        "data before ACK should not be ignored before private readiness: {error}"
    );
    assert_eq!(
        cache.fresh_order("BTC-USDT", "entry-1", Duration::from_secs(1)),
        None
    );
    Ok(())
}

#[tokio::test]
async fn websocket_subscription_ack_private_caches_data_for_acknowledged_channel() -> Result<()> {
    let (url, received) = spawn_private_server_with_subscription_messages(vec![
        private_account_subscribe_ack(),
        private_orders_subscribe_ack_for("BTC-USDT"),
        private_order_data_frame(),
        private_fills_subscribe_ack_for("BTC-USDT"),
    ])
    .await?;
    let cache = OkxPrivateEventCache::default();
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, cache.clone()).await;
    let _ = await_test_websocket_server(received).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    let order = cache
        .fresh_order("BTC-USDT", "entry-1", Duration::from_secs(1))
        .expect("acknowledged private order data should be cached before full readiness");
    assert_eq!(order.order.state, "live");
    Ok(())
}

#[tokio::test]
async fn websocket_subscription_ack_private_allows_channel_conn_count_control_event() -> Result<()>
{
    let (url, received) = spawn_private_server_with_subscription_messages(vec![
        private_account_subscribe_ack(),
        private_channel_conn_count_event(),
        private_orders_subscribe_ack_for("BTC-USDT"),
        private_fills_subscribe_ack_for("BTC-USDT"),
    ])
    .await?;
    let config = private_stream_config(url)?;

    let outcome = run_private_stream_once(&config, OkxPrivateEventCache::default()).await;
    let _ = await_test_websocket_server(received).await?;

    assert!(outcome.subscribed());
    assert!(outcome.error().is_none());
    Ok(())
}

#[test]
fn parses_private_order_updates_into_hints() -> Result<()> {
    let received_at = Instant::now();
    let hints = parse_private_event_message(
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "side": "buy",
                "ordType": "post_only",
                "px": "100.1",
                "state": "partially_filled",
                "avgPx": "100.0",
                "accFillSz": "0.001",
                "sz": "0.002",
                "cTime": "1710000000000",
                "uTime": "1710000000123"
            }]
        }"#,
        received_at,
    )?;

    assert_eq!(
        hints,
        vec![OkxPrivateEventHint::Order(OkxPrivateOrderHint {
            order: OkxOrder {
                inst_type: "SPOT".to_owned(),
                inst_id: "BTC-USDT".to_owned(),
                order_id: "ord-1".to_owned(),
                client_order_id: "entry-1".to_owned(),
                side: "buy".to_owned(),
                order_type: "post_only".to_owned(),
                price: "100.1".to_owned(),
                state: "partially_filled".to_owned(),
                average_price: "100.0".to_owned(),
                accumulated_fill_size: "0.001".to_owned(),
                fee: String::new(),
                fee_currency: String::new(),
                rebate: String::new(),
                rebate_currency: String::new(),
                sz: "0.002".to_owned(),
                created_at_ms: "1710000000000".to_owned(),
                updated_at_ms: "1710000000123".to_owned(),
            },
            source_ts_ms: Some(1_710_000_000_123),
            received_at,
        })]
    );
    Ok(())
}

#[test]
fn rejects_private_order_updates_with_unknown_state() {
    let error = apply_private_event_message(
        &OkxPrivateEventCache::default(),
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "side": "buy",
                "ordType": "post_only",
                "px": "100.1",
                "state": "pending_cancel",
                "avgPx": "100.0",
                "accFillSz": "0.001",
                "sz": "0.002",
                "cTime": "1710000000000",
                "uTime": "1710000000123"
            }]
        }"#,
        Instant::now(),
    )
    .expect_err("unknown OKX order state should fail closed");

    assert!(
        error
            .to_string()
            .contains("undocumented state \"pending_cancel\""),
        "unknown private order state should report the unsafe value: {error}"
    );
}

#[test]
fn rejects_private_order_updates_with_malformed_average_price() {
    let error = apply_private_event_message(
        &OkxPrivateEventCache::default(),
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "side": "buy",
                "ordType": "post_only",
                "px": "100.1",
                "state": "partially_filled",
                "avgPx": "not-a-decimal",
                "accFillSz": "0.001",
                "sz": "0.002",
                "cTime": "1710000000000",
                "uTime": "1710000000123"
            }]
        }"#,
        Instant::now(),
    )
    .expect_err("malformed non-empty avgPx should fail closed");

    assert!(
        error.to_string().contains("avgPx"),
        "malformed private order avgPx should report the unsafe field: {error}"
    );
}

#[test]
fn accepts_private_order_update_with_zero_average_price_without_fill() -> Result<()> {
    let cache = OkxPrivateEventCache::default();

    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "side": "buy",
                "ordType": "post_only",
                "px": "100.1",
                "state": "live",
                "avgPx": "0",
                "accFillSz": "0",
                "sz": "0.002",
                "cTime": "1710000000000",
                "uTime": "1710000000123"
            }]
        }"#,
        Instant::now(),
    )?;

    let order = cache
        .fresh_order("BTC-USDT", "entry-1", Duration::from_secs(1))
        .expect("zero avgPx no-fill private order should be cached");
    assert_eq!(order.order.average_fill_price()?, None);
    Ok(())
}

#[test]
fn parses_private_fill_updates_with_ws_fields_into_hints() -> Result<()> {
    let received_at = Instant::now();
    let hints = parse_private_event_message(
        r#"{
            "arg": {"channel": "fills", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "tradeId": "trade-1",
                "side": "buy",
                "fillSz": "0.001",
                "fillPx": "100.0",
                "ts": "1710000000124"
            }]
        }"#,
        received_at,
    )?;

    assert_eq!(
        hints,
        vec![OkxPrivateEventHint::Fill(OkxPrivateFillHint {
            fill: OkxFill {
                inst_type: "SPOT".to_owned(),
                inst_id: "BTC-USDT".to_owned(),
                order_id: "ord-1".to_owned(),
                client_order_id: "entry-1".to_owned(),
                bill_id: String::new(),
                trade_id: "trade-1".to_owned(),
                side: "buy".to_owned(),
                fill_size: "0.001".to_owned(),
                fill_price: "100.0".to_owned(),
                fee: String::new(),
                fee_currency: String::new(),
                fee_rate: String::new(),
                execution_type: String::new(),
                fill_time_ms: String::new(),
                event_time_ms: "1710000000124".to_owned(),
            },
            source_ts_ms: Some(1_710_000_000_124),
            received_at,
        })]
    );
    Ok(())
}

#[test]
fn rejects_private_fill_updates_with_malformed_payload() {
    let error = apply_private_event_message(
        &OkxPrivateEventCache::default(),
        r#"{
            "arg": {"channel": "fills", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "tradeId": "trade-1",
                "side": "buy",
                "fillSz": "0",
                "fillPx": "100.0",
                "ts": "1710000000124"
            }]
        }"#,
        Instant::now(),
    )
    .expect_err("zero fill size should fail closed");

    assert!(
        error.to_string().contains("fillSz"),
        "malformed fill update should report the unsafe field: {error}"
    );
}

#[test]
fn rejects_private_fill_updates_with_derivative_instrument_id() {
    let error = parse_private_event_message(
        r#"{
            "arg": {"channel": "fills", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "tradeId": "trade-1",
                "side": "buy",
                "fillSz": "0.001",
                "fillPx": "100.0",
                "ts": "1710000000124"
            }]
        }"#,
        Instant::now(),
    )
    .expect_err("derivative fill instrument should fail closed");

    assert!(
        error.to_string().contains("SPOT BASE-QUOTE"),
        "derivative fill update should report SPOT-only shape: {error}"
    );
}

#[test]
fn parses_private_algo_order_updates_into_hints() -> Result<()> {
    let received_at = Instant::now();
    let hints = parse_private_event_message(
        r#"{
            "arg": {"channel": "orders-algo", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "algoId": "algo-1",
                "algoClOrdId": "stop-1",
                "side": "sell",
                "ordType": "trigger",
                "triggerPx": "99.0",
                "ordPx": "-1",
                "state": "live",
                "sz": "0.001",
                "cTime": "1710000000000",
                "uTime": "1710000000125"
            }]
        }"#,
        received_at,
    )?;

    assert_eq!(
        hints,
        vec![OkxPrivateEventHint::AlgoOrder(OkxPrivateAlgoOrderHint {
            algo_order: OkxAlgoOrder {
                inst_type: "SPOT".to_owned(),
                inst_id: "BTC-USDT".to_owned(),
                td_mode: String::new(),
                algo_id: "algo-1".to_owned(),
                client_order_id: "stop-1".to_owned(),
                side: "sell".to_owned(),
                order_type: "trigger".to_owned(),
                trigger_price: "99.0".to_owned(),
                order_price: "-1".to_owned(),
                state: "live".to_owned(),
                sz: "0.001".to_owned(),
                created_at_ms: "1710000000000".to_owned(),
                updated_at_ms: "1710000000125".to_owned(),
            },
            source_ts_ms: Some(1_710_000_000_125),
            received_at,
        })]
    );
    Ok(())
}

#[test]
fn parses_global_any_algo_updates_only_for_exact_spot_cash_instrument() -> Result<()> {
    let config = private_business_stream_config(
        "wss://example.test/ws/v5/business".to_owned(),
        OkxApiDomain::Global,
    )?;
    let authority = OkxPrivateEventAuthority::from(&config);
    let payload = private_algo_update_payload("ANY", Some("BTC-USDT"), "SPOT", "BTC-USDT", "cash");

    let hints = parse_private_event_message_with_authority(&authority, &payload, Instant::now())?;

    assert_eq!(hints.len(), 1);
    let OkxPrivateEventHint::AlgoOrder(hint) = &hints[0] else {
        panic!("exact global algo update should produce an algo-order hint");
    };
    assert_eq!(hint.algo_order.inst_id, "BTC-USDT");
    assert_eq!(hint.algo_order.inst_type, "SPOT");
    assert_eq!(hint.algo_order.td_mode, "cash");
    Ok(())
}

#[test]
fn rejects_global_any_algo_updates_with_wrong_or_missing_identity() -> Result<()> {
    let config = private_business_stream_config(
        "wss://example.test/ws/v5/business".to_owned(),
        OkxApiDomain::Global,
    )?;
    let authority = OkxPrivateEventAuthority::from(&config);

    for (payload, expected) in [
        (
            private_algo_update_payload("SPOT", Some("BTC-USDT"), "SPOT", "BTC-USDT", "cash"),
            "selector",
        ),
        (
            private_algo_update_payload("ANY", None, "SPOT", "BTC-USDT", "cash"),
            "exact arg instrument",
        ),
        (
            private_algo_update_payload("ANY", Some("ETH-USDT"), "SPOT", "ETH-USDT", "cash"),
            "unconfigured instrument",
        ),
        (
            private_algo_update_payload("ANY", Some("BTC-USDT"), "MARGIN", "BTC-USDT", "cash"),
            "non-SPOT",
        ),
        (
            private_algo_update_payload("ANY", Some("BTC-USDT"), "SPOT", "BTC-USDT", "cross"),
            "unsupported tdMode",
        ),
    ] {
        let error =
            parse_private_event_message_with_authority(&authority, &payload, Instant::now())
                .expect_err("contradictory algo-order push must fail closed");
        assert!(
            error.to_string().contains(expected),
            "algo-order rejection should report {expected:?}: {error}"
        );
    }
    Ok(())
}

#[test]
fn rejects_private_algo_order_updates_with_malformed_numeric_fields() {
    for (field, value, expected) in [
        ("triggerPx", "not-a-decimal", "triggerPx"),
        ("ordPx", "not-a-decimal", "ordPx"),
        ("sz", "not-a-decimal", "sz"),
    ] {
        let mut order = json!({
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "algoId": format!("algo-{field}"),
            "algoClOrdId": "stop-1",
            "side": "sell",
            "ordType": "trigger",
            "triggerPx": "99.0",
            "ordPx": "-1",
            "state": "live",
            "sz": "0.001",
            "cTime": "1710000000000",
            "uTime": "1710000000125"
        });
        order[field] = json!(value);
        let payload = json!({
            "arg": {"channel": "orders-algo", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [order]
        })
        .to_string();

        let error =
            apply_private_event_message(&OkxPrivateEventCache::default(), &payload, Instant::now())
                .expect_err("malformed algo numeric field should fail closed");

        assert!(
            error.to_string().contains(expected),
            "malformed private algo {field} should report {expected}: {error}"
        );
    }
}

#[test]
fn parses_private_account_updates_into_hints() -> Result<()> {
    let received_at = Instant::now();
    let hints = parse_private_event_message(
        r#"{
            "arg": {"channel": "account"},
            "data": [{
                "uTime": "1710000000126",
                "details": [{
                    "ccy": "BTC",
                    "availBal": "0.001",
                    "cashBal": "0.001",
                    "frozenBal": "0"
                }]
            }]
        }"#,
        received_at,
    )?;

    assert_eq!(
        hints,
        vec![OkxPrivateEventHint::Account(OkxPrivateAccountHint {
            balance: OkxBalance {
                details: vec![OkxBalanceDetail {
                    ccy: "BTC".to_owned(),
                    available_balance: "0.001".to_owned(),
                    cash_balance: "0.001".to_owned(),
                    frozen_balance: "0".to_owned(),
                }],
            },
            source_ts_ms: Some(1_710_000_000_126),
            received_at,
        })]
    );
    Ok(())
}

#[test]
fn rejects_private_account_updates_with_malformed_balance_decimal() {
    let error = apply_private_event_message(
        &OkxPrivateEventCache::default(),
        r#"{
            "arg": {"channel": "account"},
            "data": [{
                "uTime": "1710000000126",
                "details": [{
                    "ccy": "BTC",
                    "availBal": "not-a-decimal",
                    "cashBal": "0.001",
                    "frozenBal": "0"
                }]
            }]
        }"#,
        Instant::now(),
    )
    .expect_err("malformed account balance decimal should fail closed");

    assert!(
        error.to_string().contains("availBal"),
        "malformed account balance should report the unsafe field: {error}"
    );
}

#[test]
fn rejects_private_algo_order_updates_with_undocumented_state() {
    let error = apply_private_event_message(
        &OkxPrivateEventCache::default(),
        r#"{
            "arg": {"channel": "orders-algo", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "algoId": "algo-1",
                "algoClOrdId": "stop-1",
                "side": "sell",
                "ordType": "trigger",
                "triggerPx": "99.0",
                "orderPx": "-1",
                "state": "failed",
                "sz": "0.001",
                "cTime": "1710000000000",
                "uTime": "1710000000125"
            }]
        }"#,
        Instant::now(),
    )
    .expect_err("undocumented OKX algo state should fail closed");

    assert!(
        error.to_string().contains("undocumented state \"failed\""),
        "unknown private algo state should report the unsafe value: {error}"
    );
}

#[test]
fn private_event_cache_dedupes_by_order_fill_and_algo_keys() -> Result<()> {
    let cache = OkxPrivateEventCache::default();
    let received_at = Instant::now();
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "state": "live",
                "avgPx": "",
                "accFillSz": "0",
                "sz": "0.002",
                "uTime": "1710000000000"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "state": "partially_filled",
                "avgPx": "100",
                "accFillSz": "0.001",
                "sz": "0.002",
                "uTime": "1710000000001"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "fills", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "tradeId": "trade-1",
                "side": "buy",
                "fillSz": "0.001",
                "fillPx": "100",
                "ts": "1710000000001"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders-algo", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "algoId": "algo-1",
                "state": "live",
                "uTime": "1710000000002"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "account"},
            "data": [{
                "uTime": "1710000000002",
                "details": [{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"}]
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "account"},
            "data": [{
                "uTime": "1710000000002",
                "details": [{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"}]
            }]
        }"#,
        received_at,
    )?;

    assert_eq!(cache.order_count(), 1);
    assert_eq!(cache.fill_count(), 1);
    assert_eq!(cache.algo_order_count(), 1);
    assert_eq!(cache.account_count(), 1);
    Ok(())
}

#[test]
fn private_event_cache_caps_retained_order_fill_and_algo_hints() -> Result<()> {
    let cache = OkxPrivateEventCache::default();
    let newest_received_at = Instant::now();

    for index in 0..=OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND {
        let received_at = newest_received_at
            - Duration::from_millis((OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND - index) as u64);
        let update_ts_ms = 1_710_000_000_000_i64 + index as i64;

        let order_message = json!({
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": format!("ord-{index}"),
                "clOrdId": format!("entry-{index}"),
                "state": "live",
                "avgPx": "",
                "accFillSz": "0",
                "sz": "0.002",
                "uTime": update_ts_ms.to_string()
            }]
        })
        .to_string();
        apply_private_event_message(&cache, &order_message, received_at)?;

        let fill_message = json!({
            "arg": {"channel": "fills", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": format!("ord-{index}"),
                "clOrdId": format!("entry-{index}"),
                "tradeId": format!("trade-{index}"),
                "side": "buy",
                "fillSz": "0.001",
                "fillPx": "100",
                "ts": update_ts_ms.to_string()
            }]
        })
        .to_string();
        apply_private_event_message(&cache, &fill_message, received_at)?;

        let algo_message = json!({
            "arg": {"channel": "orders-algo", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "algoId": format!("algo-{index}"),
                "state": "live",
                "uTime": update_ts_ms.to_string()
            }]
        })
        .to_string();
        apply_private_event_message(&cache, &algo_message, received_at)?;
    }

    assert_eq!(
        cache.order_count(),
        OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND
    );
    assert_eq!(
        cache.fill_count(),
        OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND
    );
    assert_eq!(
        cache.algo_order_count(),
        OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND
    );
    assert_eq!(
        cache.fresh_order("BTC-USDT", "entry-0", Duration::from_secs(60)),
        None
    );
    assert_eq!(
        cache
            .fresh_order(
                "BTC-USDT",
                &format!("entry-{OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND}"),
                Duration::from_secs(60),
            )
            .map(|hint| hint.order.order_id),
        Some(format!("ord-{OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND}"))
    );

    let fills = cache.fresh_fills("BTC-USDT", Duration::from_secs(60));
    assert_eq!(fills.len(), OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND);
    assert!(
        !fills.iter().any(|hint| hint.fill.trade_id == "trade-0"),
        "oldest fill hint should be evicted"
    );
    assert!(
        fills.iter().any(|hint| hint.fill.trade_id
            == format!("trade-{OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND}")),
        "newest fill hint should remain cached"
    );

    let algo_orders = cache.fresh_algo_orders("BTC-USDT", Duration::from_secs(60));
    assert_eq!(
        algo_orders.len(),
        OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND
    );
    assert!(
        !algo_orders
            .iter()
            .any(|hint| hint.algo_order.algo_id == "algo-0"),
        "oldest algo hint should be evicted"
    );
    assert!(
        algo_orders.iter().any(|hint| hint.algo_order.algo_id
            == format!("algo-{OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND}")),
        "newest algo hint should remain cached"
    );
    Ok(())
}

#[test]
fn private_event_cache_returns_only_fresh_hints() -> Result<()> {
    let cache = OkxPrivateEventCache::default();
    let received_at = Instant::now();
    cache.update_order(OkxPrivateOrderHint {
        order: OkxOrder {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            order_id: "ord-1".to_owned(),
            client_order_id: "entry-1".to_owned(),
            side: "buy".to_owned(),
            order_type: "post_only".to_owned(),
            price: "100".to_owned(),
            state: "live".to_owned(),
            average_price: String::new(),
            accumulated_fill_size: "0".to_owned(),
            fee: String::new(),
            fee_currency: String::new(),
            rebate: String::new(),
            rebate_currency: String::new(),
            sz: "0.002".to_owned(),
            created_at_ms: "1710000000000".to_owned(),
            updated_at_ms: "1710000000000".to_owned(),
        },
        source_ts_ms: Some(1_710_000_000_000),
        received_at,
    })?;
    cache.update_order(OkxPrivateOrderHint {
        order: OkxOrder {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            order_id: "ord-stale".to_owned(),
            client_order_id: "entry-stale".to_owned(),
            side: "buy".to_owned(),
            order_type: "post_only".to_owned(),
            price: "100".to_owned(),
            state: "live".to_owned(),
            average_price: String::new(),
            accumulated_fill_size: "0".to_owned(),
            fee: String::new(),
            fee_currency: String::new(),
            rebate: String::new(),
            rebate_currency: String::new(),
            sz: "0.002".to_owned(),
            created_at_ms: "1710000000000".to_owned(),
            updated_at_ms: "1710000000000".to_owned(),
        },
        source_ts_ms: Some(1_710_000_000_000),
        received_at: received_at - Duration::from_secs(10),
    })?;
    cache.update_fill(OkxPrivateFillHint {
        fill: OkxFill {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            order_id: "ord-1".to_owned(),
            client_order_id: "entry-1".to_owned(),
            bill_id: String::new(),
            trade_id: "trade-1".to_owned(),
            side: "buy".to_owned(),
            fill_size: "0.001".to_owned(),
            fill_price: "100".to_owned(),
            fee: String::new(),
            fee_currency: String::new(),
            fee_rate: String::new(),
            execution_type: String::new(),
            fill_time_ms: String::new(),
            event_time_ms: "1710000000001".to_owned(),
        },
        source_ts_ms: Some(1_710_000_000_001),
        received_at,
    })?;
    cache.update_algo_order(OkxPrivateAlgoOrderHint {
        algo_order: OkxAlgoOrder {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            td_mode: "cash".to_owned(),
            algo_id: "algo-1".to_owned(),
            client_order_id: "stop-1".to_owned(),
            side: "sell".to_owned(),
            order_type: "trigger".to_owned(),
            trigger_price: "99".to_owned(),
            order_price: "-1".to_owned(),
            state: "live".to_owned(),
            sz: "0.001".to_owned(),
            created_at_ms: "1710000000000".to_owned(),
            updated_at_ms: "1710000000000".to_owned(),
        },
        source_ts_ms: Some(1_710_000_000_000),
        received_at,
    })?;
    cache.update_account(OkxPrivateAccountHint {
        balance: OkxBalance {
            details: vec![OkxBalanceDetail {
                ccy: "BTC".to_owned(),
                available_balance: "0.001".to_owned(),
                cash_balance: "0.001".to_owned(),
                frozen_balance: "0".to_owned(),
            }],
        },
        source_ts_ms: Some(1_710_000_000_002),
        received_at,
    })?;

    assert_eq!(
        cache
            .fresh_order("BTC-USDT", "entry-1", Duration::from_secs(1))
            .map(|hint| hint.order.order_id),
        Some("ord-1".to_owned())
    );
    assert_eq!(
        cache
            .fresh_order("BTC-USDT", "entry-stale", Duration::from_secs(1))
            .map(|hint| hint.order.order_id),
        None
    );
    let fills = cache.fresh_fills("BTC-USDT", Duration::from_secs(1));
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill.trade_id, "trade-1");
    let algo_orders = cache.fresh_algo_orders("BTC-USDT", Duration::from_secs(1));
    assert_eq!(algo_orders.len(), 1);
    assert_eq!(algo_orders[0].algo_order.algo_id, "algo-1");
    let account = cache
        .fresh_account(Duration::from_secs(1))
        .expect("fresh account hint should be returned");
    assert_eq!(account.balance.details[0].ccy, "BTC");
    Ok(())
}

#[test]
fn private_event_cache_discards_poisoned_hints_and_recovers() -> Result<()> {
    let cache = OkxPrivateEventCache::default();
    let received_at = Instant::now();
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "state": "live",
                "avgPx": "",
                "accFillSz": "0",
                "sz": "0.002",
                "uTime": "2000"
            }]
        }"#,
        received_at,
    )?;

    let poison_result = std::panic::catch_unwind(|| {
        let _guard = cache.inner.lock().expect("test cache lock should work");
        panic!("poison private event cache");
    });
    assert!(poison_result.is_err());
    let logs = CapturedLogs::default();
    let dispatch = logs.dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    assert_eq!(
        cache.fresh_order("BTC-USDT", "entry-1", Duration::from_secs(1)),
        None
    );
    assert!(logs.contents().contains("ws_private_hint_cache_poisoned"));

    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-2",
                "clOrdId": "entry-1",
                "state": "live",
                "avgPx": "",
                "accFillSz": "0",
                "sz": "0.002",
                "uTime": "3000"
            }]
        }"#,
        received_at,
    )?;

    assert_eq!(
        cache
            .fresh_order("BTC-USDT", "entry-1", Duration::from_secs(1))
            .map(|hint| hint.order.order_id),
        Some("ord-2".to_owned())
    );
    Ok(())
}

#[test]
fn private_event_cache_skips_empty_account_updates() -> Result<()> {
    let cache = OkxPrivateEventCache::default();

    cache.update_account(OkxPrivateAccountHint {
        balance: OkxBalance {
            details: Vec::new(),
        },
        source_ts_ms: Some(1_710_000_000_002),
        received_at: Instant::now(),
    })?;

    assert_eq!(cache.fresh_account(Duration::from_secs(1)), None);
    assert_eq!(cache.account_count(), 0);
    Ok(())
}

#[test]
fn private_event_cache_ignores_older_order_updates() -> Result<()> {
    let cache = OkxPrivateEventCache::default();
    let received_at = Instant::now();
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "state": "filled",
                "avgPx": "100",
                "accFillSz": "0.002",
                "sz": "0.002",
                "uTime": "2000"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "state": "partially_filled",
                "avgPx": "100",
                "accFillSz": "0.001",
                "sz": "0.002",
                "uTime": "1000"
            }]
        }"#,
        received_at,
    )?;

    let state = lock(&cache.inner);
    let hint = state
        .orders_by_client_id
        .get(&("BTC-USDT".to_owned(), "entry-1".to_owned()))
        .expect("private cache should retain the newer order hint");

    assert_eq!(hint.order.state, "filled");
    assert_eq!(hint.source_ts_ms, Some(2_000));
    Ok(())
}

#[test]
fn private_event_cache_ignores_older_account_updates() -> Result<()> {
    let cache = OkxPrivateEventCache::default();
    let received_at = Instant::now();
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "account"},
            "data": [{
                "uTime": "2000",
                "details": [{"ccy":"BTC","availBal":"0.002","cashBal":"0.002","frozenBal":"0"}]
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "account"},
            "data": [{
                "uTime": "1000",
                "details": [{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"}]
            }]
        }"#,
        received_at,
    )?;

    let hint = cache
        .fresh_account(Duration::from_secs(1))
        .expect("account cache should retain the newer account hint");

    assert_eq!(hint.balance.details[0].cash_balance, "0.002");
    assert_eq!(hint.source_ts_ms, Some(2_000));
    Ok(())
}

#[test]
fn private_event_cache_ignores_older_fill_updates() -> Result<()> {
    let cache = OkxPrivateEventCache::default();
    let received_at = Instant::now();
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "fills", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "tradeId": "trade-1",
                "side": "buy",
                "fillSz": "0.002",
                "fillPx": "101",
                "ts": "2000"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "fills", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "tradeId": "trade-1",
                "side": "buy",
                "fillSz": "0.001",
                "fillPx": "100",
                "ts": "1000"
            }]
        }"#,
        received_at,
    )?;

    let fills = cache.fresh_fills("BTC-USDT", Duration::from_secs(1));

    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill.fill_size, "0.002");
    assert_eq!(fills[0].source_ts_ms, Some(2_000));
    Ok(())
}

#[test]
fn private_event_cache_ignores_untimestamped_updates() -> Result<()> {
    let cache = OkxPrivateEventCache::default();
    let received_at = Instant::now();
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "state": "filled",
                "avgPx": "100",
                "accFillSz": "0.002",
                "sz": "0.002",
                "uTime": "2000"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "state": "partially_filled",
                "avgPx": "100",
                "accFillSz": "0.001",
                "sz": "0.002"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "fills", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "tradeId": "trade-1",
                "side": "buy",
                "fillSz": "0.002",
                "fillPx": "101",
                "ts": "2000"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "fills", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "tradeId": "trade-untimestamped",
                "side": "buy",
                "fillSz": "0.001",
                "fillPx": "100"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "account"},
            "data": [{
                "uTime": "2000",
                "details": [{"ccy":"BTC","availBal":"0.002","cashBal":"0.002","frozenBal":"0"}]
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "account"},
            "data": [{
                "details": [{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"}]
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders-algo", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "algoId": "algo-1",
                "state": "live",
                "uTime": "2000"
            }]
        }"#,
        received_at,
    )?;
    apply_private_event_message(
        &cache,
        r#"{
            "arg": {"channel": "orders-algo", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "algoId": "algo-1",
                "state": "canceled"
            }]
        }"#,
        received_at,
    )?;

    let order = cache
        .fresh_order("BTC-USDT", "entry-1", Duration::from_secs(1))
        .expect("order cache should retain the timestamped hint");
    let fills = cache.fresh_fills("BTC-USDT", Duration::from_secs(1));
    let account = cache
        .fresh_account(Duration::from_secs(1))
        .expect("account cache should retain the timestamped hint");
    let algo_orders = cache.fresh_algo_orders("BTC-USDT", Duration::from_secs(1));

    assert_eq!(order.order.state, "filled");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill.fill_size, "0.002");
    assert_eq!(account.balance.details[0].cash_balance, "0.002");
    assert_eq!(algo_orders.len(), 1);
    assert_eq!(algo_orders[0].algo_order.state, "live");
    Ok(())
}

#[test]
fn rejects_private_websocket_error_frames() {
    let error = parse_private_event_message(
        r#"{"event":"error","code":"60012","msg":"Invalid request"}"#,
        Instant::now(),
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("OKX private WebSocket error"),
        "OKX private WebSocket error should fail: {error}"
    );
}

#[test]
fn ignores_vip_only_fills_subscription_error() -> Result<()> {
    let hints = parse_private_event_message(
        r#"{
            "event": "error",
            "code": "64003",
            "msg": "This channel is only available to users with trading fee tier VIP4 or above.",
            "arg": {"channel": "fills", "instId": "BTC-USDT"}
        }"#,
        Instant::now(),
    )?;

    assert_eq!(hints, Vec::new());
    Ok(())
}

async fn spawn_private_server_with_idle_pong() -> Result<(String, JoinHandle<Result<Vec<String>>>)>
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        send_private_trading_subscribe_acks(&mut websocket).await?;
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                crate::okx::websocket::OKX_WEBSOCKET_TEXT_PONG.into(),
            ))
            .await?;
        websocket.close(None).await?;
        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_private_server_with_delayed_readiness()
-> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();
        received.push(next_text(&mut websocket).await?);
        tokio_time::sleep(Duration::from_millis(150)).await;
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        tokio_time::sleep(Duration::from_millis(150)).await;
        send_private_trading_subscribe_acks(&mut websocket).await?;
        websocket.close(None).await?;
        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_private_server_with_login_messages(
    messages: Vec<Message>,
    keepalive_after_messages: Option<Duration>,
) -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();
        received.push(next_text(&mut websocket).await?);
        for message in messages {
            websocket.send(message).await?;
        }
        if let Some(duration) = keepalive_after_messages {
            reply_to_text_pings_until_close(&mut websocket, &mut received, duration).await?;
        } else {
            websocket.close(None).await?;
        }
        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_private_server_with_subscription_messages(
    messages: Vec<Message>,
) -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        for message in messages {
            websocket.send(message).await?;
        }
        websocket.close(None).await?;
        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_private_server_without_subscription_ack()
-> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        reply_to_text_pings_until_close(&mut websocket, &mut received, Duration::from_millis(150))
            .await?;
        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_private_server_without_idle_pong()
-> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        send_private_trading_subscribe_acks(&mut websocket).await?;
        received.push(next_text(&mut websocket).await?);
        tokio_time::sleep(Duration::from_millis(150)).await;
        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_private_server_with_order_after_idle_ping()
-> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        send_private_trading_subscribe_acks(&mut websocket).await?;
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{
                    "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
                    "data": [{
                        "instType": "SPOT",
                        "instId": "BTC-USDT",
                        "ordId": "ord-1",
                        "clOrdId": "entry-1",
                        "side": "buy",
                        "ordType": "post_only",
                        "px": "100.1",
                        "state": "live",
                        "avgPx": "",
                        "accFillSz": "0",
                        "sz": "0.002",
                        "cTime": "1710000000000",
                        "uTime": "1710000000123"
                    }]
                }"#
                .into(),
            ))
            .await?;
        websocket.close(None).await?;
        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_private_server_with_subscription_error()
-> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{
                    "event": "error",
                    "code": "60012",
                    "msg": "Invalid request",
                    "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"}
                }"#
                .into(),
            ))
            .await?;
        websocket.close(None).await?;
        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_private_server_with_vip_fills_subscription_error()
-> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"subscribe","arg":{"channel":"account"}}"#.into(),
            ))
            .await?;
        websocket
            .send(Message::Text(
                r#"{
                    "event": "subscribe",
                    "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"}
                }"#
                .into(),
            ))
            .await?;
        websocket
            .send(Message::Text(
                r#"{
                    "event": "error",
                    "code": "64003",
                    "msg": "This channel is only available to users with trading fee tier VIP4 or above.",
                    "arg": {"channel": "fills", "instId": "BTC-USDT"}
                }"#
                .into(),
            ))
            .await?;
        websocket.close(None).await?;
        Ok(received)
    });
    Ok((url, handle))
}

async fn send_private_trading_subscribe_acks(
    websocket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> Result<()> {
    websocket.send(private_account_subscribe_ack()).await?;
    websocket
        .send(private_orders_subscribe_ack_for("BTC-USDT"))
        .await?;
    websocket
        .send(private_fills_subscribe_ack_for("BTC-USDT"))
        .await?;
    Ok(())
}

fn private_account_subscribe_ack() -> Message {
    Message::Text(r#"{"event":"subscribe","arg":{"channel":"account"}}"#.into())
}

fn private_orders_subscribe_ack_for(inst_id: &str) -> Message {
    Message::Text(
        format!(
            r#"{{
                "event": "subscribe",
                "arg": {{"channel": "orders", "instType": "SPOT", "instId": "{inst_id}"}}
            }}"#
        )
        .into(),
    )
}

fn private_fills_subscribe_ack_for(inst_id: &str) -> Message {
    Message::Text(
        format!(r#"{{"event":"subscribe","arg":{{"channel":"fills","instId":"{inst_id}"}}}}"#)
            .into(),
    )
}

fn okx_websocket_upgrade_notice() -> &'static str {
    r#"{"event":"notice","code":"64008","msg":"sensitive maintenance detail","connId":"sensitive-connection-id"}"#
}

fn private_channel_conn_count_event() -> Message {
    Message::Text(
        r#"{
            "event": "channel-conn-count",
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "connCount": "1"
        }"#
        .into(),
    )
}

fn private_order_data_frame() -> Message {
    Message::Text(
        r#"{
            "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
            "data": [{
                "instType": "SPOT",
                "instId": "BTC-USDT",
                "ordId": "ord-1",
                "clOrdId": "entry-1",
                "side": "buy",
                "ordType": "post_only",
                "px": "100.1",
                "state": "live",
                "avgPx": "",
                "accFillSz": "0",
                "sz": "0.002",
                "cTime": "1710000000000",
                "uTime": "1710000000123"
            }]
        }"#
        .into(),
    )
}

fn private_stream_config(url: String) -> Result<OkxPrivateStreamConfig> {
    private_stream_config_with_policy(
        url,
        OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
    )
}

fn private_stream_config_with_policy(
    url: String,
    reconnect_policy: OkxWebsocketReconnectPolicy,
) -> Result<OkxPrivateStreamConfig> {
    OkxPrivateStreamConfig::with_reconnect_policy(
        url,
        OkxPrivateStreamKind::Trading,
        vec!["BTC-USDT".to_owned()],
        OkxApiDomain::Global,
        Arc::new(OkxPrivateStreamCredentials::new(
            "api-key".to_owned(),
            "secret".to_owned(),
            "passphrase".to_owned(),
        )?),
        reconnect_policy,
    )
}

fn private_business_stream_config(
    url: String,
    api_domain: OkxApiDomain,
) -> Result<OkxPrivateStreamConfig> {
    OkxPrivateStreamConfig::with_reconnect_policy(
        url,
        OkxPrivateStreamKind::Business,
        vec!["BTC-USDT".to_owned()],
        api_domain,
        Arc::new(OkxPrivateStreamCredentials::new(
            "api-key".to_owned(),
            "secret".to_owned(),
            "passphrase".to_owned(),
        )?),
        OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
    )
}

fn private_algo_update_payload(
    selector: &str,
    arg_instrument_id: Option<&str>,
    row_instrument_type: &str,
    row_instrument_id: &str,
    trade_mode: &str,
) -> String {
    json!({
        "arg": {
            "channel": "orders-algo",
            "instType": selector,
            "instId": arg_instrument_id,
        },
        "data": [{
            "instType": row_instrument_type,
            "instId": row_instrument_id,
            "tdMode": trade_mode,
            "algoId": "algo-1",
            "algoClOrdId": "stop-1",
            "side": "sell",
            "ordType": "trigger",
            "triggerPx": "99.0",
            "ordPx": "-1",
            "state": "live",
            "sz": "0.001",
            "cTime": "1710000000000",
            "uTime": "1710000000125"
        }]
    })
    .to_string()
}

fn received_contains_subscription(received: &[String]) -> bool {
    received.iter().any(|payload| {
        serde_json::from_str::<serde_json::Value>(payload)
            .ok()
            .is_some_and(|value| {
                value.get("op").and_then(serde_json::Value::as_str) == Some("subscribe")
            })
    })
}

fn protocol_error(error: &anyhow::Error) -> &OkxWebsocketProtocolError {
    error
        .downcast_ref::<OkxWebsocketProtocolError>()
        .expect("private WebSocket failure should preserve typed protocol error")
}

async fn accept_test_websocket(listener: TcpListener) -> Result<TestWebSocket> {
    let (stream, _) = tokio_time::timeout(TEST_WEBSOCKET_TIMEOUT, listener.accept())
        .await
        .context("timed out accepting test WebSocket TCP connection")??;
    tokio_time::timeout(TEST_WEBSOCKET_TIMEOUT, accept_async(stream))
        .await
        .context("timed out accepting test WebSocket handshake")?
        .context("failed accepting test WebSocket handshake")
}

async fn await_test_websocket_server(
    handle: JoinHandle<Result<Vec<String>>>,
) -> Result<Vec<String>> {
    tokio_time::timeout(TEST_WEBSOCKET_TIMEOUT, handle)
        .await
        .context("timed out waiting for test WebSocket server task")?
        .context("test WebSocket server task panicked")?
}

async fn next_test_websocket_message(websocket: &mut TestWebSocket) -> Result<Message> {
    tokio_time::timeout(TEST_WEBSOCKET_TIMEOUT, websocket.next())
        .await
        .context("timed out waiting for test WebSocket client text frame")?
        .context("test WebSocket closed before text frame")?
        .context("failed reading test WebSocket client frame")
}

async fn next_text(websocket: &mut TestWebSocket) -> Result<String> {
    loop {
        let message = next_test_websocket_message(websocket).await?;
        if let Message::Text(payload) = message {
            return Ok(payload.to_string());
        }
    }
}

async fn recv_health_events(
    receiver: &mut OkxWebsocketHealthReceiver,
    count: usize,
) -> Result<Vec<OkxWebsocketHealthEvent>> {
    let mut events = Vec::new();
    for _ in 0..count {
        events.push(recv_health_event(receiver).await?);
    }
    Ok(events)
}

async fn recv_health_event_kind(
    receiver: &mut OkxWebsocketHealthReceiver,
    kind: OkxWebsocketHealthEventKind,
) -> Result<OkxWebsocketHealthEvent> {
    loop {
        let event = recv_health_event(receiver).await?;
        if event.kind() == kind {
            return Ok(event);
        }
    }
}

async fn recv_health_event(
    receiver: &mut OkxWebsocketHealthReceiver,
) -> Result<OkxWebsocketHealthEvent> {
    tokio_time::timeout(Duration::from_millis(250), receiver.recv())
        .await
        .context("timed out waiting for OKX private WebSocket health event")?
        .context("OKX private WebSocket health channel closed")
}

async fn reply_to_text_pings_until_close(
    websocket: &mut TestWebSocket,
    received: &mut Vec<String>,
    duration: Duration,
) -> Result<()> {
    let deadline = tokio_time::sleep(duration);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            () = &mut deadline => return Ok(()),
            message = websocket.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let Ok(message) = message else {
                    return Ok(());
                };
                if let Message::Text(payload) = message {
                    received.push(payload.to_string());
                    if payload.as_str() == crate::okx::websocket::OKX_WEBSOCKET_TEXT_PING {
                        websocket
                            .send(Message::Text(crate::okx::websocket::OKX_WEBSOCKET_TEXT_PONG.into()))
                            .await?;
                    }
                }
            }
        }
    }
}
