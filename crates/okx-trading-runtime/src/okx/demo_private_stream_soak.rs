use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};

use crate::{
    config::types::BotConfig,
    okx::{
        client::OkxRestClient,
        websocket::{
            OkxPrivateEventCache, OkxPrivateStream, OkxPrivateStreamConfig,
            OkxPrivateStreamCredentials, OkxPrivateStreamKind, OkxPrivateStreamTiming,
            OkxWebsocketHealthEventKind, OkxWebsocketHealthReporter, OkxWebsocketReconnectPolicy,
            OkxWebsocketStreamKind,
        },
    },
};

const PRIVATE_SOAK_DURATION: Duration = Duration::from_secs(20);
const PRIVATE_SOAK_IDLE_PING_AFTER: Duration = Duration::from_secs(5);
const PRIVATE_SOAK_IDLE_PONG_TIMEOUT: Duration = Duration::from_secs(5);
const PRIVATE_SOAK_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const PRIVATE_SOAK_HEALTH_CAPACITY: usize = 32;

pub(super) async fn run_private_stream_soak(
    client: &OkxRestClient,
    config: &BotConfig,
    instrument_id: &str,
) -> Result<()> {
    let okx = config.okx.as_ref().context("OKX config is required")?;
    let url = okx
        .base_url_ws_private
        .clone()
        .context("OKX base_url_ws_private is required for private-stream soak")?;
    let credentials = Arc::new(OkxPrivateStreamCredentials::new(
        okx.api_key.clone(),
        okx.api_secret.clone(),
        okx.api_passphrase.clone(),
    )?);
    let reconnect_policy = OkxWebsocketReconnectPolicy::new(
        Duration::from_millis(okx.websocket.reconnect_initial_backoff_ms),
        Duration::from_millis(okx.websocket.reconnect_max_backoff_ms),
    )?;
    let stream_config = OkxPrivateStreamConfig::with_reconnect_policy(
        url,
        OkxPrivateStreamKind::Trading,
        vec![instrument_id.to_owned()],
        okx.api_domain,
        credentials,
        reconnect_policy,
    )?
    .without_optional_fills();
    let timing = OkxPrivateStreamTiming::new(
        PRIVATE_SOAK_IDLE_PING_AFTER,
        PRIVATE_SOAK_IDLE_PONG_TIMEOUT,
        PRIVATE_SOAK_ACK_TIMEOUT,
        PRIVATE_SOAK_ACK_TIMEOUT,
    )?;
    let (health, mut health_events) =
        OkxWebsocketHealthReporter::channel(PRIVATE_SOAK_HEALTH_CAPACITY);
    let stream = OkxPrivateStream::spawn_with_health_and_timing(
        stream_config,
        OkxPrivateEventCache::default(),
        client.websocket_login_timestamp_provider(),
        Some(health),
        timing,
    );
    super::wait_for_websocket_subscription_ack(&mut health_events, OkxWebsocketStreamKind::Private)
        .await
        .context("OKX Demo private-stream soak failed to become ready")?;

    let soak_result = tokio::time::timeout(PRIVATE_SOAK_DURATION, async {
        while let Some(event) = health_events.recv().await {
            match event.kind() {
                OkxWebsocketHealthEventKind::ConnectAttempt
                | OkxWebsocketHealthEventKind::Connected
                | OkxWebsocketHealthEventKind::LoginAckSucceeded
                | OkxWebsocketHealthEventKind::SubscriptionAckSucceeded => {}
                OkxWebsocketHealthEventKind::LoginFailed
                | OkxWebsocketHealthEventKind::SubscriptionAckFailed
                | OkxWebsocketHealthEventKind::ReconnectScheduled
                | OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription
                | OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription
                | OkxWebsocketHealthEventKind::StreamFailedAfterSubscription
                | OkxWebsocketHealthEventKind::StreamTaskPanicked
                | OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly => {
                    bail!(
                        "OKX Demo private-stream soak observed unhealthy event: {}",
                        event.kind()
                    );
                }
            }
        }
        bail!("OKX Demo private-stream soak health channel closed unexpectedly")
    })
    .await;
    drop(stream);

    match soak_result {
        Err(_) => {
            eprintln!(
                "OKX Demo private stream remained healthy for {} seconds with a {}-second idle-ping threshold",
                PRIVATE_SOAK_DURATION.as_secs(),
                PRIVATE_SOAK_IDLE_PING_AFTER.as_secs()
            );
            Ok(())
        }
        Ok(result) => result,
    }
}
