use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    hash::Hash,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::SinkExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time as tokio_time,
};
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message};
use tracing::{info, warn};
use zeroize::Zeroizing;

use super::{
    OKX_WEBSOCKET_TEXT_PONG, OkxPrivateRuntimeEvent, OkxPrivateRuntimeEventKind,
    OkxPrivateStreamTiming, OkxRuntimeEventReporter, OkxWebsocketChannelClass,
    OkxWebsocketHealthEvent, OkxWebsocketHealthEventKind, OkxWebsocketHealthReporter,
    OkxWebsocketReconnectPolicy, OkxWebsocketStreamIdentity, OkxWebsocketStreamKind,
    OkxWebsocketStreamRunOutcome, OkxWebsocketTaskHandle,
    auth::{
        OkxWebsocketLoginCredentials, login_request_at, parse_login_ack,
        validate_websocket_login_credential,
    },
    next_websocket_message_with_keepalive_and_timing,
    notice::reject_websocket_notice,
    parse_websocket_data,
    protocol_error::OkxWebsocketProtocolError,
    report_websocket_health, should_ignore_hint,
    subscription::{
        OkxWebsocketSubscriptionAck, OkxWebsocketSubscriptionEvent, acknowledge_subscription,
        parse_subscription_event,
    },
};
use crate::config::types::OkxApiDomain;
use crate::okx::{
    client::OkxWebsocketLoginTimestampProvider,
    types::{OkxAlgoOrder, OkxBalance, OkxBalanceDetail, OkxFill, OkxOrder},
};

const OKX_PRIVATE_ORDERS_CHANNEL: &str = "orders";
const OKX_PRIVATE_FILLS_CHANNEL: &str = "fills";
const OKX_PRIVATE_ACCOUNT_CHANNEL: &str = "account";
const OKX_BUSINESS_ALGO_ORDERS_CHANNEL: &str = "orders-algo";
const OKX_SPOT_INST_TYPE: &str = "SPOT";
const OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxPrivateStreamConfig {
    pub(crate) url: String,
    pub(crate) kind: OkxPrivateStreamKind,
    pub(crate) instrument_ids: Vec<String>,
    pub(crate) instrument_type: String,
    algo_subscription_selector: OkxAlgoSubscriptionSelector,
    pub(crate) fills_subscription: OkxPrivateFillsSubscription,
    pub(crate) credentials: Arc<OkxPrivateStreamCredentials>,
    pub(crate) reconnect_policy: OkxWebsocketReconnectPolicy,
}

impl OkxPrivateStreamConfig {
    pub(crate) fn with_reconnect_policy(
        url: String,
        kind: OkxPrivateStreamKind,
        instrument_ids: Vec<String>,
        api_domain: OkxApiDomain,
        credentials: Arc<OkxPrivateStreamCredentials>,
        reconnect_policy: OkxWebsocketReconnectPolicy,
    ) -> Result<Self> {
        ensure!(
            !url.trim().is_empty(),
            "OKX private WebSocket URL must not be empty"
        );
        ensure!(
            !instrument_ids.is_empty(),
            "OKX private WebSocket stream requires at least one instrument"
        );
        let mut unique_instruments = BTreeSet::new();
        for instrument_id in instrument_ids {
            ensure!(
                !instrument_id.trim().is_empty() && instrument_id == instrument_id.trim(),
                "OKX private WebSocket instrument id must be non-empty and trimmed"
            );
            unique_instruments.insert(instrument_id);
        }
        let fills_subscription = match kind {
            OkxPrivateStreamKind::Trading => OkxPrivateFillsSubscription::Enabled,
            OkxPrivateStreamKind::Business => OkxPrivateFillsSubscription::Disabled,
        };
        Ok(Self {
            url,
            kind,
            instrument_ids: unique_instruments.into_iter().collect(),
            instrument_type: OKX_SPOT_INST_TYPE.to_owned(),
            algo_subscription_selector: OkxAlgoSubscriptionSelector::from_api_domain(api_domain),
            fills_subscription,
            credentials,
            reconnect_policy,
        })
    }

    pub(crate) fn with_validated_instrument_type(mut self, instrument_type: &str) -> Result<Self> {
        ensure!(
            instrument_type == OKX_SPOT_INST_TYPE,
            "current OKX private WebSocket runtime admits only validated SPOT instruments"
        );
        self.instrument_type = instrument_type.to_owned();
        Ok(self)
    }

    pub(crate) fn without_optional_fills(mut self) -> Self {
        self.fills_subscription = OkxPrivateFillsSubscription::Disabled;
        self
    }

    #[cfg(test)]
    pub(crate) const fn algo_subscription_selector(&self) -> OkxAlgoSubscriptionSelector {
        self.algo_subscription_selector
    }

    pub(crate) fn health_identity(&self) -> OkxWebsocketStreamIdentity {
        match self.kind {
            OkxPrivateStreamKind::Trading => OkxWebsocketStreamIdentity::new(
                OkxWebsocketStreamKind::Private,
                OkxWebsocketChannelClass::PrivateTrading,
                self.instrument_ids.len(),
            ),
            OkxPrivateStreamKind::Business => OkxWebsocketStreamIdentity::new(
                OkxWebsocketStreamKind::Business,
                OkxWebsocketChannelClass::PrivateAlgoOrders,
                self.instrument_ids.len(),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OkxAlgoSubscriptionSelector {
    Spot,
    Any,
}

impl OkxAlgoSubscriptionSelector {
    const fn from_api_domain(api_domain: OkxApiDomain) -> Self {
        match api_domain {
            OkxApiDomain::Eea => Self::Spot,
            OkxApiDomain::Global | OkxApiDomain::UsAu => Self::Any,
        }
    }

    const fn as_okx(self) -> &'static str {
        match self {
            Self::Spot => OKX_SPOT_INST_TYPE,
            Self::Any => "ANY",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OkxPrivateFillsSubscription {
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OkxPrivateStreamKind {
    Trading,
    Business,
}

impl OkxPrivateStreamKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Trading => "private",
            Self::Business => "business",
        }
    }
}

#[derive(Eq, PartialEq)]
pub(crate) struct OkxPrivateStreamCredentials {
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    api_passphrase: Zeroizing<String>,
}

impl OkxPrivateStreamCredentials {
    pub(crate) fn new(
        api_key: impl Into<Zeroizing<String>>,
        api_secret: impl Into<Zeroizing<String>>,
        api_passphrase: impl Into<Zeroizing<String>>,
    ) -> Result<Self> {
        let api_key = api_key.into();
        let api_secret = api_secret.into();
        let api_passphrase = api_passphrase.into();
        validate_websocket_login_credential("OKX private WebSocket api_key", &api_key)?;
        validate_websocket_login_credential("OKX private WebSocket api_secret", &api_secret)?;
        validate_websocket_login_credential(
            "OKX private WebSocket api_passphrase",
            &api_passphrase,
        )?;
        Ok(Self {
            api_key,
            api_secret,
            api_passphrase,
        })
    }

    fn login_credentials(&self) -> OkxWebsocketLoginCredentials<'_> {
        OkxWebsocketLoginCredentials {
            api_key: self.api_key.as_str(),
            api_secret: self.api_secret.as_str(),
            api_passphrase: self.api_passphrase.as_str(),
        }
    }
}

impl fmt::Debug for OkxPrivateStreamCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OkxPrivateStreamCredentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("api_passphrase", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OkxPrivateEventCache {
    inner: Arc<Mutex<OkxPrivateEventState>>,
}

impl OkxPrivateEventCache {
    pub(crate) fn configure_runtime_observer(&self, reporter: OkxRuntimeEventReporter) {
        lock(&self.inner).runtime_event_reporter = Some(reporter);
    }

    fn runtime_event_reporter(&self) -> Option<OkxRuntimeEventReporter> {
        lock(&self.inner).runtime_event_reporter.clone()
    }

    pub(crate) fn update_order(&self, hint: OkxPrivateOrderHint) -> Result<bool> {
        ensure_spot_inst_type(&hint.order.inst_type, &hint.order.inst_id, "order")?;
        ensure_spot_instrument_id(&hint.order.inst_id, "order")?;
        ensure!(
            !hint.order.inst_id.trim().is_empty(),
            "OKX private WebSocket order omitted instId"
        );
        ensure!(
            !hint.order.client_order_id.trim().is_empty(),
            "OKX private WebSocket order {} omitted clOrdId",
            hint.order.order_id
        );
        hint.order
            .ensure_documented_state("private WebSocket order")?;
        hint.order.fill_size()?;
        hint.order.average_fill_price()?;
        hint.order.requested_size()?;
        let key = (
            hint.order.inst_id.clone(),
            hint.order.client_order_id.clone(),
        );
        let mut cache = lock(&self.inner);
        let current_ts_ms = cache
            .orders_by_client_id
            .get(&key)
            .and_then(|current| current.source_ts_ms);
        if should_ignore_hint(current_ts_ms, hint.source_ts_ms) {
            return Ok(false);
        }
        cache.orders_by_client_id.insert(key, hint);
        evict_oldest_private_hints(
            &mut cache.orders_by_client_id,
            OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND,
            |hint| hint.received_at,
        );
        Ok(true)
    }

    pub(crate) fn update_fill(&self, hint: OkxPrivateFillHint) -> Result<bool> {
        ensure_spot_inst_type(&hint.fill.inst_type, &hint.fill.inst_id, "fill")?;
        ensure_spot_instrument_id(&hint.fill.inst_id, "fill")?;
        ensure!(
            !hint.fill.inst_id.trim().is_empty(),
            "OKX private WebSocket fill omitted instId"
        );
        hint.fill.fill_size()?;
        hint.fill.fill_price()?;
        let key = hint.fill.dedupe_key();
        ensure!(
            !key.trim().is_empty(),
            "OKX private WebSocket fill omitted dedupe fields"
        );
        let mut cache = lock(&self.inner);
        let current_ts_ms = cache
            .fills_by_dedupe_key
            .get(&key)
            .and_then(|current| current.source_ts_ms);
        if should_ignore_hint(current_ts_ms, hint.source_ts_ms) {
            return Ok(false);
        }
        cache.fills_by_dedupe_key.insert(key, hint);
        evict_oldest_private_hints(
            &mut cache.fills_by_dedupe_key,
            OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND,
            |hint| hint.received_at,
        );
        Ok(true)
    }

    pub(crate) fn update_algo_order(&self, hint: OkxPrivateAlgoOrderHint) -> Result<bool> {
        ensure_spot_inst_type(
            &hint.algo_order.inst_type,
            &hint.algo_order.inst_id,
            "algo order",
        )?;
        ensure_spot_instrument_id(&hint.algo_order.inst_id, "algo order")?;
        ensure!(
            !hint.algo_order.inst_id.trim().is_empty(),
            "OKX private WebSocket algo order omitted instId"
        );
        ensure!(
            !hint.algo_order.algo_id.trim().is_empty(),
            "OKX private WebSocket algo order omitted algoId"
        );
        hint.algo_order
            .ensure_documented_state("private WebSocket algo order")?;
        ensure_optional_positive_decimal(
            "OKX private WebSocket algo order triggerPx",
            &hint.algo_order.trigger_price,
        )?;
        ensure_optional_positive_decimal(
            "OKX private WebSocket algo order sz",
            &hint.algo_order.sz,
        )?;
        if !hint.algo_order.order_price.trim().is_empty() && hint.algo_order.order_price != "-1" {
            ensure_positive_decimal(
                "OKX private WebSocket algo order ordPx",
                &hint.algo_order.order_price,
            )?;
        }
        let key = (
            hint.algo_order.inst_id.clone(),
            hint.algo_order.algo_id.clone(),
        );
        let mut cache = lock(&self.inner);
        let current_ts_ms = cache
            .algo_orders_by_id
            .get(&key)
            .and_then(|current| current.source_ts_ms);
        if should_ignore_hint(current_ts_ms, hint.source_ts_ms) {
            return Ok(false);
        }
        cache.algo_orders_by_id.insert(key, hint);
        evict_oldest_private_hints(
            &mut cache.algo_orders_by_id,
            OKX_PRIVATE_EVENT_CACHE_MAX_HINTS_PER_KIND,
            |hint| hint.received_at,
        );
        Ok(true)
    }

    pub(crate) fn update_account(&self, hint: OkxPrivateAccountHint) -> Result<bool> {
        if hint.balance.details.is_empty() {
            return Ok(false);
        }
        for detail in &hint.balance.details {
            ensure!(
                !detail.ccy.trim().is_empty(),
                "OKX private WebSocket account update omitted ccy"
            );
            detail.available()?;
            detail.total()?;
            detail.frozen()?;
        }
        let mut cache = lock(&self.inner);
        let current_ts_ms = cache
            .account
            .as_ref()
            .and_then(|current| current.source_ts_ms);
        if should_ignore_hint(current_ts_ms, hint.source_ts_ms) {
            return Ok(false);
        }
        cache.account = Some(hint);
        Ok(true)
    }

    pub(crate) fn fresh_order(
        &self,
        inst_id: &str,
        client_order_id: &str,
        max_staleness: Duration,
    ) -> Option<OkxPrivateOrderHint> {
        lock(&self.inner)
            .orders_by_client_id
            .get(&(inst_id.to_owned(), client_order_id.to_owned()))
            .filter(|hint| hint.received_at.elapsed() <= max_staleness)
            .cloned()
    }

    pub(crate) fn fresh_fills(
        &self,
        inst_id: &str,
        max_staleness: Duration,
    ) -> Vec<OkxPrivateFillHint> {
        lock(&self.inner)
            .fills_by_dedupe_key
            .values()
            .filter(|hint| {
                hint.fill.inst_id == inst_id && hint.received_at.elapsed() <= max_staleness
            })
            .cloned()
            .collect()
    }

    pub(crate) fn fresh_algo_orders(
        &self,
        inst_id: &str,
        max_staleness: Duration,
    ) -> Vec<OkxPrivateAlgoOrderHint> {
        lock(&self.inner)
            .algo_orders_by_id
            .values()
            .filter(|hint| {
                hint.algo_order.inst_id == inst_id && hint.received_at.elapsed() <= max_staleness
            })
            .cloned()
            .collect()
    }

    pub(crate) fn fresh_account(&self, max_staleness: Duration) -> Option<OkxPrivateAccountHint> {
        lock(&self.inner)
            .account
            .as_ref()
            .filter(|hint| hint.received_at.elapsed() <= max_staleness)
            .cloned()
    }

    #[cfg(test)]
    fn order_count(&self) -> usize {
        lock(&self.inner).orders_by_client_id.len()
    }

    #[cfg(test)]
    fn fill_count(&self) -> usize {
        lock(&self.inner).fills_by_dedupe_key.len()
    }

    #[cfg(test)]
    fn algo_order_count(&self) -> usize {
        lock(&self.inner).algo_orders_by_id.len()
    }

    #[cfg(test)]
    fn account_count(&self) -> usize {
        usize::from(lock(&self.inner).account.is_some())
    }
}

#[derive(Debug, Default)]
struct OkxPrivateEventState {
    orders_by_client_id: HashMap<(String, String), OkxPrivateOrderHint>,
    fills_by_dedupe_key: HashMap<String, OkxPrivateFillHint>,
    algo_orders_by_id: HashMap<(String, String), OkxPrivateAlgoOrderHint>,
    account: Option<OkxPrivateAccountHint>,
    runtime_event_reporter: Option<OkxRuntimeEventReporter>,
}

fn evict_oldest_private_hints<K, V>(
    hints: &mut HashMap<K, V>,
    max_len: usize,
    received_at: impl Fn(&V) -> Instant,
) where
    K: Clone + Eq + Hash,
{
    while hints.len() > max_len {
        let Some(oldest_key) = hints
            .iter()
            .min_by_key(|(_, hint)| received_at(hint))
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        hints.remove(&oldest_key);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OkxPrivateOrderHint {
    pub(crate) order: OkxOrder,
    pub(crate) source_ts_ms: Option<i64>,
    pub(crate) received_at: Instant,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OkxPrivateFillHint {
    pub(crate) fill: OkxFill,
    pub(crate) source_ts_ms: Option<i64>,
    pub(crate) received_at: Instant,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OkxPrivateAlgoOrderHint {
    pub(crate) algo_order: OkxAlgoOrder,
    pub(crate) source_ts_ms: Option<i64>,
    pub(crate) received_at: Instant,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OkxPrivateAccountHint {
    pub(crate) balance: OkxBalance,
    pub(crate) source_ts_ms: Option<i64>,
    pub(crate) received_at: Instant,
}

#[derive(Debug)]
pub(crate) struct OkxPrivateStream {
    task: OkxWebsocketTaskHandle,
}

impl OkxPrivateStream {
    pub(crate) fn spawn_with_health(
        config: OkxPrivateStreamConfig,
        cache: OkxPrivateEventCache,
        login_timestamp_provider: OkxWebsocketLoginTimestampProvider,
        health: Option<OkxWebsocketHealthReporter>,
    ) -> Self {
        Self::spawn_with_health_and_timing(
            config,
            cache,
            login_timestamp_provider,
            health,
            OkxPrivateStreamTiming::default(),
        )
    }

    pub(crate) fn spawn_with_health_and_timing(
        config: OkxPrivateStreamConfig,
        cache: OkxPrivateEventCache,
        login_timestamp_provider: OkxWebsocketLoginTimestampProvider,
        health: Option<OkxWebsocketHealthReporter>,
        timing: OkxPrivateStreamTiming,
    ) -> Self {
        let stream = config.health_identity();
        let supervisor_health = health.clone();
        let task = OkxWebsocketTaskHandle::spawn(stream, supervisor_health, async move {
            run_private_stream(config, cache, login_timestamp_provider, health, timing).await;
        });
        Self { task }
    }

    #[cfg(test)]
    fn spawn_test_task<F>(
        stream: OkxWebsocketStreamIdentity,
        health: Option<OkxWebsocketHealthReporter>,
        future: F,
    ) -> Self
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        Self {
            task: OkxWebsocketTaskHandle::spawn(stream, health, future),
        }
    }
}

impl Drop for OkxPrivateStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn run_private_stream(
    config: OkxPrivateStreamConfig,
    cache: OkxPrivateEventCache,
    login_timestamp_provider: OkxWebsocketLoginTimestampProvider,
    health: Option<OkxWebsocketHealthReporter>,
    timing: OkxPrivateStreamTiming,
) {
    let mut backoff = config.reconnect_policy.initial_backoff();
    let mut reconnect_attempt = 0_u64;
    loop {
        let outcome = run_private_stream_once_with_login_timestamp_provider_and_timing(
            &config,
            cache.clone(),
            &login_timestamp_provider,
            health.as_ref(),
            timing,
        )
        .await;
        reconnect_attempt = reconnect_attempt.saturating_add(1);
        report_websocket_health(
            health.as_ref(),
            OkxWebsocketHealthEvent::reconnect_scheduled(
                config.health_identity(),
                reconnect_attempt,
                backoff,
            ),
        )
        .await;
        match outcome.error() {
            Some(error) => warn!(
                safety_event = "ws_private_reconnect",
                error = %error,
                stream_kind = config.kind.label(),
                reconnect_backoff_ms = backoff.as_millis(),
                "OKX private WebSocket stream failed; reconnecting"
            ),
            None => warn!(
                safety_event = "ws_private_reconnect",
                stream_kind = config.kind.label(),
                reconnect_backoff_ms = backoff.as_millis(),
                "OKX private WebSocket stream disconnected; reconnecting"
            ),
        }
        tokio_time::sleep(backoff).await;
        backoff = config
            .reconnect_policy
            .backoff_after_stream_run(backoff, &outcome);
    }
}

#[cfg(test)]
async fn run_private_stream_once(
    config: &OkxPrivateStreamConfig,
    cache: OkxPrivateEventCache,
) -> OkxWebsocketStreamRunOutcome {
    run_private_stream_once_with_health(config, cache, None).await
}

#[cfg(test)]
async fn run_private_stream_once_with_health(
    config: &OkxPrivateStreamConfig,
    cache: OkxPrivateEventCache,
    health: Option<&OkxWebsocketHealthReporter>,
) -> OkxWebsocketStreamRunOutcome {
    let login_timestamp_provider = OkxWebsocketLoginTimestampProvider::fixed("1538054050");
    run_private_stream_once_with_login_timestamp_provider(
        config,
        cache,
        &login_timestamp_provider,
        health,
    )
    .await
}

#[cfg(test)]
async fn run_private_stream_once_with_login_timestamp_provider(
    config: &OkxPrivateStreamConfig,
    cache: OkxPrivateEventCache,
    login_timestamp_provider: &OkxWebsocketLoginTimestampProvider,
    health: Option<&OkxWebsocketHealthReporter>,
) -> OkxWebsocketStreamRunOutcome {
    run_private_stream_once_with_login_timestamp_provider_and_timing(
        config,
        cache,
        login_timestamp_provider,
        health,
        OkxPrivateStreamTiming::default(),
    )
    .await
}

async fn run_private_stream_once_with_login_timestamp_provider_and_timing(
    config: &OkxPrivateStreamConfig,
    cache: OkxPrivateEventCache,
    login_timestamp_provider: &OkxWebsocketLoginTimestampProvider,
    health: Option<&OkxWebsocketHealthReporter>,
    timing: OkxPrivateStreamTiming,
) -> OkxWebsocketStreamRunOutcome {
    let mut subscribed = false;
    let mut pending_subscription_acks = private_stream_subscription_acks(config);
    let event_authority = OkxPrivateEventAuthority::from(config);
    let result = async {
        report_websocket_health(
            health,
            OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::ConnectAttempt,
                config.health_identity(),
            ),
        )
        .await;
        let (mut stream, _) = connect_async(config.url.as_str()).await.with_context(|| {
            format!(
                "failed connecting to OKX {} WebSocket {}",
                config.kind.label(),
                config.url
            )
        })?;
        report_websocket_health(
            health,
            OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::Connected,
                config.health_identity(),
            ),
        )
        .await;
        let login_timestamp = login_timestamp_provider
            .login_timestamp()
            .await
            .with_context(|| {
                format!(
                    "failed obtaining OKX server-time-backed WebSocket login timestamp for {} stream",
                    config.kind.label()
                )
            })?;
        let login = login_request_at(&config.credentials.login_credentials(), &login_timestamp)?;
        stream
            .send(Message::Text(login.into()))
            .await
            .context("failed logging in to OKX private WebSocket stream")?;
        if let Err(error) = wait_for_private_login(&mut stream, config.kind.label(), timing).await {
            report_websocket_health(
                health,
                OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::LoginFailed,
                    config.health_identity(),
                ),
            )
            .await;
            return Err(error);
        }
        report_websocket_health(
            health,
            OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::LoginAckSucceeded,
                config.health_identity(),
            ),
        )
        .await;

        let subscription = private_stream_subscription(config)?;
        stream
            .send(Message::Text(subscription.into()))
            .await
            .context("failed subscribing to OKX private WebSocket stream")?;
        if let Err(error) = wait_for_private_subscription_acks(
            &mut stream,
            config,
            &event_authority,
            &cache,
            &mut pending_subscription_acks,
            timing,
        )
        .await
        {
            report_websocket_health(
                health,
                OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::SubscriptionAckFailed,
                    config.health_identity(),
                ),
            )
            .await;
            return Err(error);
        }
        subscribed = true;
        report_websocket_health(
            health,
            OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                config.health_identity(),
            ),
        )
        .await;
        info!(
            safety_event = "ws_private_subscription_success",
            instrument_count = config.instrument_ids.len(),
            stream_kind = config.kind.label(),
            "subscribed to OKX private WebSocket stream"
        );

        while let Some(message) = next_websocket_message_with_keepalive_and_timing(
            &mut stream,
            config.kind.label(),
            timing.idle_ping_after(),
            timing.idle_pong_timeout(),
        )
        .await?
        {
            match message {
                Message::Text(payload) if payload.as_str() == OKX_WEBSOCKET_TEXT_PONG => {}
                Message::Text(payload) => {
                    apply_private_event_message_and_report(
                        &event_authority,
                        &cache,
                        payload.as_ref(),
                        Instant::now(),
                    )
                    .await?;
                }
                Message::Ping(payload) => {
                    stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed replying to OKX private WebSocket ping")?;
                }
                Message::Close(_) => break,
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            let outcome = OkxWebsocketStreamRunOutcome::disconnected(subscribed);
            report_private_stream_outcome(config, &outcome, health).await;
            outcome
        }
        Err(error) => {
            let outcome = OkxWebsocketStreamRunOutcome::failed(subscribed, error);
            report_private_stream_outcome(config, &outcome, health).await;
            outcome
        }
    }
}

async fn report_private_stream_outcome(
    config: &OkxPrivateStreamConfig,
    outcome: &OkxWebsocketStreamRunOutcome,
    health: Option<&OkxWebsocketHealthReporter>,
) {
    let kind = match (outcome.subscribed(), outcome.error().is_some()) {
        (true, false) => OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription,
        (true, true) => OkxWebsocketHealthEventKind::StreamFailedAfterSubscription,
        (false, true) => OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
        (false, false) => return,
    };
    report_websocket_health(
        health,
        OkxWebsocketHealthEvent::new(kind, config.health_identity()),
    )
    .await;
}

async fn wait_for_private_login<S>(
    stream: &mut WebSocketStream<S>,
    context: &str,
    timing: OkxPrivateStreamTiming,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio_time::timeout(timing.login_ack_timeout(), async {
        while let Some(message) = next_websocket_message_with_keepalive_and_timing(
            stream,
            context,
            timing.idle_ping_after(),
            timing.idle_pong_timeout(),
        )
        .await?
        {
            match message {
                Message::Text(payload) if payload.as_str() == OKX_WEBSOCKET_TEXT_PONG => {}
                Message::Text(payload) => {
                    if parse_login_ack(payload.as_ref(), context)? {
                        return Ok(());
                    }
                }
                Message::Ping(payload) => {
                    stream.send(Message::Pong(payload)).await.with_context(|| {
                        format!("failed replying to OKX {context} WebSocket ping")
                    })?;
                }
                Message::Close(_) => {
                    return Err(OkxWebsocketProtocolError::ClosedBeforeLoginAck {
                        context: context.to_owned(),
                    }
                    .into());
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        Err(OkxWebsocketProtocolError::ClosedBeforeLoginAck {
            context: context.to_owned(),
        }
        .into())
    })
    .await
    .map_err(|_| OkxWebsocketProtocolError::TimedOutWaitingForLoginAck {
        context: context.to_owned(),
    })?
}

async fn wait_for_private_subscription_acks<S>(
    stream: &mut WebSocketStream<S>,
    config: &OkxPrivateStreamConfig,
    event_authority: &OkxPrivateEventAuthority,
    cache: &OkxPrivateEventCache,
    pending_subscription_acks: &mut BTreeSet<OkxWebsocketSubscriptionAck>,
    timing: OkxPrivateStreamTiming,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio_time::timeout(timing.subscription_ack_timeout(), async {
        let mut acknowledged_subscription_acks = BTreeSet::new();
        while !pending_subscription_acks.is_empty() {
            let context = config.kind.label();
            let Some(message) = next_websocket_message_with_keepalive_and_timing(
                stream,
                context,
                timing.idle_ping_after(),
                timing.idle_pong_timeout(),
            )
            .await?
            else {
                return Err(OkxWebsocketProtocolError::ClosedBeforeSubscriptionAck {
                    context: context.to_owned(),
                }
                .into());
            };
            match message {
                Message::Text(payload) if payload.as_str() == OKX_WEBSOCKET_TEXT_PONG => {}
                Message::Text(payload) => {
                    match parse_subscription_event(payload.as_ref(), context)? {
                        OkxWebsocketSubscriptionEvent::Acknowledged(ack) => {
                            acknowledge_subscription(
                                pending_subscription_acks,
                                ack.clone(),
                                context,
                            )?;
                            acknowledged_subscription_acks.insert(ack);
                        }
                        OkxWebsocketSubscriptionEvent::Error { code, arg, .. }
                            if code == "64003"
                                && arg.as_ref().is_some_and(|ack| {
                                    ack.channel == OKX_PRIVATE_FILLS_CHANNEL
                                }) =>
                        {
                            let Some(ack) = arg else {
                                return Err(OkxWebsocketProtocolError::SubscriptionErrorEvent {
                                    context: context.to_owned(),
                                    code,
                                    msg: String::new(),
                                    ack: None,
                                }
                                .into());
                            };
                            acknowledge_subscription(
                                pending_subscription_acks,
                                ack.clone(),
                                context,
                            )?;
                            acknowledged_subscription_acks.insert(ack);
                        }
                        OkxWebsocketSubscriptionEvent::Control => {}
                        OkxWebsocketSubscriptionEvent::Data(ack) => {
                            if !acknowledged_subscription_acks.contains(&ack) {
                                return Err(OkxWebsocketProtocolError::DataBeforeSubscriptionAck {
                                    context: context.to_owned(),
                                    ack: Box::new(ack),
                                }
                                .into());
                            }
                            apply_private_event_message_and_report(
                                event_authority,
                                cache,
                                payload.as_ref(),
                                Instant::now(),
                            )
                            .await?;
                        }
                        OkxWebsocketSubscriptionEvent::Error { code, msg, arg } => {
                            return Err(OkxWebsocketProtocolError::SubscriptionErrorEvent {
                                context: context.to_owned(),
                                code,
                                msg,
                                ack: arg.map(Box::new),
                            }
                            .into());
                        }
                        OkxWebsocketSubscriptionEvent::Other => {
                            return Err(
                                OkxWebsocketProtocolError::NonAckTextBeforeSubscriptionAck {
                                    context: context.to_owned(),
                                }
                                .into(),
                            );
                        }
                    }
                }
                Message::Ping(payload) => {
                    stream.send(Message::Pong(payload)).await.with_context(|| {
                        format!("failed replying to OKX {context} WebSocket ping")
                    })?;
                }
                Message::Close(_) => {
                    return Err(OkxWebsocketProtocolError::ClosedBeforeSubscriptionAck {
                        context: context.to_owned(),
                    }
                    .into());
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
        Ok(())
    })
    .await
    .map_err(
        |_| OkxWebsocketProtocolError::TimedOutWaitingForSubscriptionAck {
            context: config.kind.label().to_owned(),
        },
    )?
}

fn private_stream_subscription_acks(
    config: &OkxPrivateStreamConfig,
) -> BTreeSet<OkxWebsocketSubscriptionAck> {
    let mut acks = BTreeSet::new();
    match config.kind {
        OkxPrivateStreamKind::Trading => {
            acks.insert(OkxWebsocketSubscriptionAck {
                channel: OKX_PRIVATE_ACCOUNT_CHANNEL.to_owned(),
                inst_id: None,
                inst_type: None,
            });
            for instrument_id in &config.instrument_ids {
                acks.insert(OkxWebsocketSubscriptionAck {
                    channel: OKX_PRIVATE_ORDERS_CHANNEL.to_owned(),
                    inst_id: Some(instrument_id.clone()),
                    inst_type: Some(config.instrument_type.clone()),
                });
                if config.fills_subscription == OkxPrivateFillsSubscription::Enabled {
                    acks.insert(OkxWebsocketSubscriptionAck {
                        channel: OKX_PRIVATE_FILLS_CHANNEL.to_owned(),
                        inst_id: Some(instrument_id.clone()),
                        inst_type: None,
                    });
                }
            }
        }
        OkxPrivateStreamKind::Business => {
            for instrument_id in &config.instrument_ids {
                acks.insert(OkxWebsocketSubscriptionAck {
                    channel: OKX_BUSINESS_ALGO_ORDERS_CHANNEL.to_owned(),
                    inst_id: Some(instrument_id.clone()),
                    inst_type: Some(config.algo_subscription_selector.as_okx().to_owned()),
                });
            }
        }
    }
    acks
}

fn private_stream_subscription(config: &OkxPrivateStreamConfig) -> Result<String> {
    ensure!(
        !config.instrument_ids.is_empty(),
        "OKX private WebSocket subscription requires at least one instrument"
    );
    ensure!(
        config.instrument_type == OKX_SPOT_INST_TYPE,
        "current OKX private WebSocket subscription admits only validated SPOT"
    );
    let mut args = Vec::new();
    match config.kind {
        OkxPrivateStreamKind::Trading => {
            args.push(OkxWebsocketSubscribeArg::account());
            for instrument_id in &config.instrument_ids {
                args.push(OkxWebsocketSubscribeArg::typed(
                    OKX_PRIVATE_ORDERS_CHANNEL,
                    instrument_id,
                    &config.instrument_type,
                ));
                if config.fills_subscription == OkxPrivateFillsSubscription::Enabled {
                    args.push(OkxWebsocketSubscribeArg::instrument(
                        OKX_PRIVATE_FILLS_CHANNEL,
                        instrument_id,
                    ));
                }
            }
        }
        OkxPrivateStreamKind::Business => {
            for instrument_id in &config.instrument_ids {
                args.push(OkxWebsocketSubscribeArg::typed(
                    OKX_BUSINESS_ALGO_ORDERS_CHANNEL,
                    instrument_id,
                    config.algo_subscription_selector.as_okx(),
                ));
            }
        }
    }
    let request = OkxWebsocketSubscribeRequest {
        op: "subscribe",
        args,
    };
    serde_json::to_string(&request).context("failed serializing OKX private subscription")
}

async fn apply_private_event_message_and_report(
    authority: &OkxPrivateEventAuthority,
    cache: &OkxPrivateEventCache,
    payload: &str,
    received_at: Instant,
) -> Result<usize> {
    let applied = apply_private_event_message_inner(authority, cache, payload, received_at)?;
    if let Some(reporter) = cache.runtime_event_reporter() {
        for event in applied.runtime_events {
            reporter.report_private(event).await?;
        }
    }
    Ok(applied.count)
}

struct AppliedPrivateEvents {
    count: usize,
    runtime_events: Vec<OkxPrivateRuntimeEvent>,
}

fn apply_private_event_message_inner(
    authority: &OkxPrivateEventAuthority,
    cache: &OkxPrivateEventCache,
    payload: &str,
    received_at: Instant,
) -> Result<AppliedPrivateEvents> {
    let events = parse_private_event_message_with_authority(authority, payload, received_at)?;
    let count = events.len();
    let mut runtime_events = Vec::new();
    for event in events {
        match event {
            OkxPrivateEventHint::Order(hint) => {
                let runtime_event = OkxPrivateRuntimeEvent {
                    kind: OkxPrivateRuntimeEventKind::Order {
                        instrument_id: hint.order.inst_id.clone(),
                        client_order_id: hint.order.client_order_id.clone(),
                    },
                    received_at: hint.received_at,
                };
                if cache.update_order(hint)? {
                    runtime_events.push(runtime_event);
                }
            }
            OkxPrivateEventHint::Fill(hint) => {
                let runtime_event = OkxPrivateRuntimeEvent {
                    kind: OkxPrivateRuntimeEventKind::Fill {
                        instrument_id: hint.fill.inst_id.clone(),
                        client_order_id: hint.fill.client_order_id.clone(),
                    },
                    received_at: hint.received_at,
                };
                if cache.update_fill(hint)? {
                    runtime_events.push(runtime_event);
                }
            }
            OkxPrivateEventHint::AlgoOrder(hint) => {
                let runtime_event = OkxPrivateRuntimeEvent {
                    kind: OkxPrivateRuntimeEventKind::AlgoOrder {
                        instrument_id: hint.algo_order.inst_id.clone(),
                    },
                    received_at: hint.received_at,
                };
                if cache.update_algo_order(hint)? {
                    runtime_events.push(runtime_event);
                }
            }
            OkxPrivateEventHint::Account(hint) => {
                let runtime_event = OkxPrivateRuntimeEvent {
                    kind: OkxPrivateRuntimeEventKind::Account,
                    received_at: hint.received_at,
                };
                if cache.update_account(hint)? {
                    runtime_events.push(runtime_event);
                }
            }
        }
    }
    Ok(AppliedPrivateEvents {
        count,
        runtime_events,
    })
}

fn parse_private_event_message_with_authority(
    authority: &OkxPrivateEventAuthority,
    payload: &str,
    received_at: Instant,
) -> Result<Vec<OkxPrivateEventHint>> {
    let message: OkxWebsocketPrivateMessage<'_> =
        serde_json::from_str(payload).context("failed parsing OKX private WebSocket message")?;
    reject_websocket_notice(
        message.event.as_deref(),
        message.code.as_deref(),
        authority.context(),
    )?;
    if message.event.as_deref() == Some("error") {
        if message.code.as_deref() == Some("64003")
            && message
                .arg
                .as_ref()
                .is_some_and(|arg| arg.channel == OKX_PRIVATE_FILLS_CHANNEL)
        {
            return Ok(Vec::new());
        }
        bail!(
            "OKX private WebSocket error {}: {}",
            message.code.unwrap_or_default(),
            message.msg.unwrap_or_default()
        );
    }
    if message.event.is_some() {
        return Ok(Vec::new());
    }

    let Some(data) = message.data else {
        return Ok(Vec::new());
    };
    let arg = message
        .arg
        .context("OKX private WebSocket data omitted arg")?;
    match arg.channel.as_str() {
        OKX_PRIVATE_ORDERS_CHANNEL => {
            let rows: Vec<OkxOrder> =
                parse_websocket_data(data, "failed parsing OKX private order update")?;
            let mut hints = Vec::with_capacity(rows.len());
            for order in rows {
                ensure_arg_matches_instrument(&arg, &order.inst_id)?;
                hints.push(OkxPrivateEventHint::Order(OkxPrivateOrderHint {
                    source_ts_ms: private_order_source_ts_ms(&order)?,
                    order,
                    received_at,
                }));
            }
            Ok(hints)
        }
        OKX_PRIVATE_FILLS_CHANNEL => {
            let rows: Vec<OkxFill> =
                parse_websocket_data(data, "failed parsing OKX private fill update")?;
            let mut hints = Vec::with_capacity(rows.len());
            for fill in rows {
                ensure_arg_matches_instrument(&arg, &fill.inst_id)?;
                hints.push(OkxPrivateEventHint::Fill(OkxPrivateFillHint {
                    source_ts_ms: parse_optional_ts_ms("OKX private fill ts", &fill.event_time_ms)?,
                    fill,
                    received_at,
                }));
            }
            Ok(hints)
        }
        OKX_PRIVATE_ACCOUNT_CHANNEL => {
            let rows: Vec<OkxWebsocketAccountData> =
                parse_websocket_data(data, "failed parsing OKX private account update")?;
            let mut hints = Vec::with_capacity(rows.len());
            for account in rows {
                hints.push(OkxPrivateEventHint::Account(OkxPrivateAccountHint {
                    source_ts_ms: parse_optional_ts_ms(
                        "OKX private account uTime",
                        &account.updated_at_ms,
                    )?,
                    balance: OkxBalance {
                        details: account.details,
                    },
                    received_at,
                }));
            }
            Ok(hints)
        }
        OKX_BUSINESS_ALGO_ORDERS_CHANNEL => {
            let rows: Vec<OkxAlgoOrder> =
                parse_websocket_data(data, "failed parsing OKX private algo order update")?;
            let mut hints = Vec::with_capacity(rows.len());
            for algo_order in rows {
                authority.ensure_algo_order_matches(&arg, &algo_order)?;
                hints.push(OkxPrivateEventHint::AlgoOrder(OkxPrivateAlgoOrderHint {
                    source_ts_ms: private_algo_order_source_ts_ms(&algo_order)?,
                    algo_order,
                    received_at,
                }));
            }
            Ok(hints)
        }
        _ => bail!(
            "OKX private WebSocket channel {} is not supported",
            arg.channel
        ),
    }
}

#[derive(Clone, Debug)]
struct OkxPrivateEventAuthority {
    instrument_ids: BTreeSet<String>,
    instrument_type: String,
    algo_subscription_selector: OkxAlgoSubscriptionSelector,
    stream_kind: OkxPrivateStreamKind,
}

impl From<&OkxPrivateStreamConfig> for OkxPrivateEventAuthority {
    fn from(config: &OkxPrivateStreamConfig) -> Self {
        Self {
            instrument_ids: config.instrument_ids.iter().cloned().collect(),
            instrument_type: config.instrument_type.clone(),
            algo_subscription_selector: config.algo_subscription_selector,
            stream_kind: config.kind,
        }
    }
}

impl OkxPrivateEventAuthority {
    const fn context(&self) -> &'static str {
        self.stream_kind.label()
    }

    fn ensure_algo_order_matches(
        &self,
        arg: &OkxWebsocketPrivateArg,
        algo_order: &OkxAlgoOrder,
    ) -> Result<()> {
        ensure!(
            self.instrument_type == OKX_SPOT_INST_TYPE,
            "current OKX private algo-order authority admits only validated SPOT"
        );
        ensure!(
            arg.inst_type.as_deref() == Some(self.algo_subscription_selector.as_okx()),
            "OKX private algo-order push selector {:?} did not match expected {}",
            arg.inst_type,
            self.algo_subscription_selector.as_okx()
        );
        ensure!(
            arg.inst_id.as_deref() == Some(algo_order.inst_id.as_str()),
            "OKX private algo-order row {} did not match exact arg instrument {:?}",
            algo_order.inst_id,
            arg.inst_id
        );
        ensure!(
            self.instrument_ids.contains(&algo_order.inst_id),
            "OKX private algo-order push returned unconfigured instrument {}",
            algo_order.inst_id
        );
        ensure!(
            algo_order.inst_type == OKX_SPOT_INST_TYPE,
            "OKX private algo-order {} has non-SPOT instType {}",
            algo_order.inst_id,
            algo_order.inst_type
        );
        ensure!(
            algo_order.td_mode.trim().is_empty() || algo_order.td_mode == "cash",
            "OKX private algo-order {} has unsupported tdMode {}",
            algo_order.inst_id,
            algo_order.td_mode
        );
        ensure_spot_instrument_id(&algo_order.inst_id, "algo order")?;
        Ok(())
    }
}

fn private_order_source_ts_ms(order: &OkxOrder) -> Result<Option<i64>> {
    if !order.updated_at_ms.trim().is_empty() {
        return parse_optional_ts_ms("OKX private order uTime", &order.updated_at_ms);
    }
    parse_optional_ts_ms("OKX private order cTime", &order.created_at_ms)
}

fn private_algo_order_source_ts_ms(algo_order: &OkxAlgoOrder) -> Result<Option<i64>> {
    if !algo_order.updated_at_ms.trim().is_empty() {
        return parse_optional_ts_ms("OKX private algo order uTime", &algo_order.updated_at_ms);
    }
    parse_optional_ts_ms("OKX private algo order cTime", &algo_order.created_at_ms)
}

fn parse_optional_ts_ms(context: &str, value: &str) -> Result<Option<i64>> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    let ts_ms = value
        .parse::<i64>()
        .with_context(|| format!("invalid {context} {value}"))?;
    ensure!(ts_ms > 0, "{context} must be positive");
    Ok(Some(ts_ms))
}

fn ensure_optional_positive_decimal(context: &str, value: &str) -> Result<()> {
    if !value.trim().is_empty() {
        ensure_positive_decimal(context, value)?;
    }
    Ok(())
}

fn ensure_positive_decimal(context: &str, value: &str) -> Result<()> {
    let parsed = value
        .parse::<Decimal>()
        .with_context(|| format!("{context} must be a decimal"))?;
    ensure!(parsed > Decimal::ZERO, "{context} must be positive");
    Ok(())
}

fn ensure_arg_matches_instrument(arg: &OkxWebsocketPrivateArg, inst_id: &str) -> Result<()> {
    ensure!(
        arg.inst_id.is_none() || arg.inst_id.as_deref() == Some(inst_id),
        "OKX private WebSocket row {} did not match arg instrument {:?}",
        inst_id,
        arg.inst_id
    );
    ensure_spot_instrument_id(inst_id, "arg")?;
    if let Some(inst_type) = &arg.inst_type {
        ensure_spot_inst_type(inst_type, inst_id, "arg")?;
    }
    Ok(())
}

fn ensure_spot_inst_type(inst_type: &str, inst_id: &str, context: &str) -> Result<()> {
    ensure!(
        inst_type.trim().is_empty() || inst_type == OKX_SPOT_INST_TYPE,
        "OKX private WebSocket {context} {inst_id} has non-SPOT instType {inst_type}"
    );
    Ok(())
}

fn ensure_spot_instrument_id(inst_id: &str, context: &str) -> Result<()> {
    let mut parts = inst_id.split('-');
    let base = parts.next().unwrap_or_default();
    let quote = parts.next().unwrap_or_default();
    ensure!(
        parts.next().is_none() && !base.is_empty() && !quote.is_empty(),
        "OKX private WebSocket {context} {inst_id} must use SPOT BASE-QUOTE instrument format"
    );
    ensure!(
        is_okx_asset_code(base) && is_okx_asset_code(quote),
        "OKX private WebSocket {context} {inst_id} must use uppercase SPOT asset codes"
    );
    Ok(())
}

fn is_okx_asset_code(value: &str) -> bool {
    matches!(value.len(), 2..=12)
        && value
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
}

fn lock<T: Default>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                safety_event = "ws_private_hint_cache_poisoned",
                "OKX private event cache mutex poisoned; discarding WebSocket hints"
            );
            let mut guard = poisoned.into_inner();
            *guard = T::default();
            mutex.clear_poison();
            guard
        }
    }
}

#[derive(Debug, PartialEq)]
enum OkxPrivateEventHint {
    Order(OkxPrivateOrderHint),
    Fill(OkxPrivateFillHint),
    AlgoOrder(OkxPrivateAlgoOrderHint),
    Account(OkxPrivateAccountHint),
}

#[derive(Serialize)]
struct OkxWebsocketSubscribeRequest<'a> {
    op: &'static str,
    args: Vec<OkxWebsocketSubscribeArg<'a>>,
}

#[derive(Serialize)]
struct OkxWebsocketSubscribeArg<'a> {
    channel: &'static str,
    #[serde(rename = "instType", skip_serializing_if = "Option::is_none")]
    inst_type: Option<&'a str>,
    #[serde(rename = "instId", skip_serializing_if = "Option::is_none")]
    inst_id: Option<&'a str>,
}

impl<'a> OkxWebsocketSubscribeArg<'a> {
    const fn account() -> Self {
        Self {
            channel: OKX_PRIVATE_ACCOUNT_CHANNEL,
            inst_type: None,
            inst_id: None,
        }
    }

    fn typed(channel: &'static str, inst_id: &'a str, inst_type: &'a str) -> Self {
        Self {
            channel,
            inst_type: Some(inst_type),
            inst_id: Some(inst_id),
        }
    }

    const fn instrument(channel: &'static str, inst_id: &'a str) -> Self {
        Self {
            channel,
            inst_type: None,
            inst_id: Some(inst_id),
        }
    }
}

#[derive(Deserialize)]
struct OkxWebsocketPrivateMessage<'a> {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    arg: Option<OkxWebsocketPrivateArg>,
    #[serde(default, borrow)]
    data: Option<&'a serde_json::value::RawValue>,
}

#[derive(Deserialize)]
struct OkxWebsocketPrivateArg {
    channel: String,
    #[serde(rename = "instType", default)]
    inst_type: Option<String>,
    #[serde(rename = "instId", default)]
    inst_id: Option<String>,
}

#[derive(Deserialize)]
struct OkxWebsocketAccountData {
    #[serde(default)]
    details: Vec<OkxBalanceDetail>,
    #[serde(rename = "uTime", default)]
    updated_at_ms: String,
}

#[cfg(test)]
#[path = "private_tests.rs"]
mod tests;
