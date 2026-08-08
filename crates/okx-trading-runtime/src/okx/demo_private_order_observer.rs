use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    config::types::BotConfig,
    okx::{
        client::{OkxRestClient, OkxWebsocketLoginTimestampProvider},
        types::OkxOrder,
        websocket::{
            OkxPrivateEventCache, OkxPrivateStream, OkxPrivateStreamConfig,
            OkxPrivateStreamCredentials, OkxPrivateStreamKind, OkxPrivateStreamTiming,
            OkxWebsocketHealthReceiver, OkxWebsocketHealthReporter, OkxWebsocketReconnectPolicy,
            OkxWebsocketStreamKind,
        },
    },
};
use anyhow::{Context, Result, bail, ensure};
use rust_decimal::Decimal;

pub(super) const PRIVATE_EVENT_TIMEOUT: Duration = Duration::from_secs(10);
const PRIVATE_EVENT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const PRIVATE_EVENT_MAX_STALENESS: Duration = Duration::from_secs(20);
const PRIVATE_STREAM_HEALTH_CAPACITY: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExpectedPrivateOrderState {
    Live,
    Canceled,
}

impl ExpectedPrivateOrderState {
    const fn label(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct PrivateOrderExpectation<'a> {
    pub(super) stage: &'static str,
    pub(super) instrument_id: &'a str,
    pub(super) order_id: &'a str,
    pub(super) client_order_id: &'a str,
    pub(super) price: &'a str,
    pub(super) size: &'a str,
    pub(super) state: ExpectedPrivateOrderState,
    pub(super) command_started_at: Instant,
    pub(super) timeout: Duration,
}

pub(super) struct DemoPrivateOrderObserver {
    stream: Option<OkxPrivateStream>,
    stream_config: OkxPrivateStreamConfig,
    cache: OkxPrivateEventCache,
    login_timestamp_provider: OkxWebsocketLoginTimestampProvider,
    timing: OkxPrivateStreamTiming,
}

impl DemoPrivateOrderObserver {
    pub(super) async fn connect(
        client: &OkxRestClient,
        config: &BotConfig,
        instrument_id: &str,
    ) -> Result<Self> {
        let okx = config.okx.as_ref().context("OKX config is required")?;
        let url = okx
            .base_url_ws_private
            .clone()
            .context("OKX base_url_ws_private is required for Demo private order observation")?;
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
            /*idle_ping_after*/ PRIVATE_EVENT_TIMEOUT,
            /*idle_pong_timeout*/ PRIVATE_EVENT_TIMEOUT,
            /*login_ack_timeout*/ PRIVATE_EVENT_TIMEOUT,
            /*subscription_ack_timeout*/ PRIVATE_EVENT_TIMEOUT,
        )?;
        let mut observer = Self {
            stream: None,
            stream_config,
            cache: OkxPrivateEventCache::default(),
            login_timestamp_provider: client.websocket_login_timestamp_provider(),
            timing,
        };
        observer
            .establish_stream("initial")
            .await
            .context("OKX Demo private order observer failed to become ready")?;
        Ok(observer)
    }

    pub(super) async fn wait_for_order(
        &self,
        expectation: PrivateOrderExpectation<'_>,
    ) -> Result<()> {
        let deadline = Instant::now() + expectation.timeout;
        let mut last_shape = "missing".to_owned();
        loop {
            if let Some(hint) = self.cache.fresh_order(
                expectation.instrument_id,
                expectation.client_order_id,
                PRIVATE_EVENT_MAX_STALENESS,
            ) {
                let observed_fill = hint.order.fill_size()?;
                ensure!(
                    observed_fill == Decimal::ZERO,
                    "OKX Demo private orders event for {} reported fill size {observed_fill}; refusing further WebSocket mutations",
                    expectation.stage
                );
                match validate_private_order(expectation, &hint.order) {
                    Ok(()) => {
                        eprintln!(
                            "OKX Demo private orders event for {} observed in {} ms",
                            expectation.stage,
                            hint.received_at
                                .saturating_duration_since(expectation.command_started_at)
                                .as_millis()
                        );
                        return Ok(());
                    }
                    Err(error) => last_shape = error.to_string(),
                }
            }
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for the OKX Demo private orders event for {}; last observation was {last_shape}",
                    expectation.stage
                );
            }
            tokio::time::sleep(PRIVATE_EVENT_POLL_INTERVAL).await;
        }
    }

    pub(super) async fn reconnect(&mut self) -> Result<()> {
        drop(self.stream.take());
        self.establish_stream("controlled reconnect")
            .await
            .context("OKX Demo private order observer controlled reconnect failed")
    }

    async fn establish_stream(&mut self, phase: &str) -> Result<()> {
        let started = Instant::now();
        let (stream, mut health_events) = self.spawn_stream();
        let ready = super::wait_for_websocket_subscription_ack(
            &mut health_events,
            OkxWebsocketStreamKind::Private,
        )
        .await;
        if let Err(error) = ready {
            drop(stream);
            return Err(error);
        }
        self.stream = Some(stream);
        eprintln!(
            "OKX Demo private order observer {phase} subscription completed in {} ms",
            started.elapsed().as_millis()
        );
        Ok(())
    }

    fn spawn_stream(&self) -> (OkxPrivateStream, OkxWebsocketHealthReceiver) {
        let (health, health_events) =
            OkxWebsocketHealthReporter::channel(PRIVATE_STREAM_HEALTH_CAPACITY);
        let stream = OkxPrivateStream::spawn_with_health_and_timing(
            self.stream_config.clone(),
            self.cache.clone(),
            self.login_timestamp_provider.clone(),
            Some(health),
            self.timing,
        );
        (stream, health_events)
    }
}

fn validate_private_order(
    expectation: PrivateOrderExpectation<'_>,
    order: &OkxOrder,
) -> Result<()> {
    order.ensure_documented_state("Demo private orders event")?;
    let price = order
        .price
        .parse::<Decimal>()
        .context("private orders event contained an invalid price")?;
    let expected_price = expectation
        .price
        .parse::<Decimal>()
        .context("Demo private order expectation contained an invalid price")?;
    let size = order.requested_size()?;
    let expected_size = expectation
        .size
        .parse::<Decimal>()
        .context("Demo private order expectation contained an invalid size")?;
    let fill_size = order.fill_size()?;
    ensure!(
        order.inst_type == "SPOT"
            && order.inst_id == expectation.instrument_id
            && order.order_id == expectation.order_id
            && order.client_order_id == expectation.client_order_id
            && order.side == "buy"
            && order.order_type == "post_only"
            && price == expected_price
            && size == expected_size
            && fill_size == Decimal::ZERO
            && order.average_fill_price()?.is_none(),
        "state {}, price {price}, size {size}, fill {fill_size}",
        order.state
    );
    match expectation.state {
        ExpectedPrivateOrderState::Live => ensure!(
            order.state == "live",
            "expected {} state but observed {}",
            expectation.state.label(),
            order.state
        ),
        ExpectedPrivateOrderState::Canceled => ensure!(
            order.state == "canceled",
            "expected {} state but observed {}",
            expectation.state.label(),
            order.state
        ),
    }
    Ok(())
}

#[cfg(test)]
#[path = "demo_private_order_observer_tests.rs"]
mod tests;
