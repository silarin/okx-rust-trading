use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    hash::Hash,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time,
};
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message};
use tracing::{info, warn};

use okx_market_model::{
    OKX_LEVEL2_BOOKS_CHANNEL, OkxLevel2ApplyOutcome, OkxLevel2Book, OkxLevel2FeatureSnapshot,
    OkxLevel2Update,
};
use okx_public_protocol::{OkxSpotInstrumentId, parse_books_message};

use crate::okx::latency::{OkxLatencyMetrics, OkxLatencyStage};
use crate::okx::types::{MarketBar, OkxTicker};

pub(crate) mod auth;
pub(crate) mod economics_preflight;
mod health;
mod notice;
mod private;
mod private_timing;
mod protocol_error;
mod runtime_event;
mod subscription;
pub mod trading;
pub(crate) mod trading_session;

pub(crate) use health::{
    OkxWebsocketChannelClass, OkxWebsocketHealthEvent, OkxWebsocketHealthEventKind,
    OkxWebsocketHealthReceiver, OkxWebsocketHealthReporter, OkxWebsocketStreamIdentity,
    OkxWebsocketStreamKind, OkxWebsocketTaskHandle,
};
use notice::reject_websocket_notice;
use protocol_error::OkxWebsocketProtocolError;
use subscription::{
    OkxWebsocketSubscriptionAck, OkxWebsocketSubscriptionEvent, acknowledge_subscription,
    parse_subscription_event,
};

#[cfg(test)]
pub(crate) use private::{OkxAlgoSubscriptionSelector, OkxPrivateAccountHint, OkxPrivateOrderHint};
pub(crate) use private::{
    OkxPrivateEventCache, OkxPrivateStream, OkxPrivateStreamConfig, OkxPrivateStreamCredentials,
    OkxPrivateStreamKind,
};
pub(crate) use private_timing::OkxPrivateStreamTiming;
pub(crate) use runtime_event::{
    OkxLevel2RuntimeEvent, OkxPrivateRuntimeEvent, OkxPrivateRuntimeEventKind,
    OkxPublicRuntimeEvent, OkxPublicRuntimeEventKind, OkxRuntimeEventReporter,
};

const OKX_PUBLIC_TICKERS_CHANNEL: &str = "tickers";
pub(crate) const OKX_PUBLIC_CANDLE_1S_CHANNEL: &str = "candle1s";
pub(crate) const OKX_PUBLIC_CANDLE_1M_CHANNEL: &str = "candle1m";
pub(crate) const OKX_PUBLIC_CANDLE_5M_CHANNEL: &str = "candle5m";
const OKX_PUBLIC_INSTRUMENTS_CHANNEL: &str = "instruments";
const OKX_SPOT_INST_TYPE: &str = "SPOT";
const OKX_MARKET_CANDLE_CACHE_MAX_BARS: usize = 32;
const OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND: usize = 1_024;
pub(crate) const OKX_WEBSOCKET_TEXT_PING: &str = "ping";
pub(crate) const OKX_WEBSOCKET_TEXT_PONG: &str = "pong";

#[cfg(not(test))]
const OKX_WEBSOCKET_IDLE_PING_AFTER: Duration = Duration::from_secs(25);
#[cfg(test)]
const OKX_WEBSOCKET_IDLE_PING_AFTER: Duration = Duration::from_millis(25);
#[cfg(not(test))]
const OKX_WEBSOCKET_IDLE_PONG_TIMEOUT: Duration = Duration::from_secs(25);
#[cfg(test)]
const OKX_WEBSOCKET_IDLE_PONG_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
pub(super) const OKX_WEBSOCKET_SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
pub(super) const OKX_WEBSOCKET_SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_millis(75);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OkxPublicMarketStreamTiming {
    idle_ping_after: Duration,
    idle_pong_timeout: Duration,
    subscription_ack_timeout: Duration,
}

impl OkxPublicMarketStreamTiming {
    #[cfg(test)]
    pub(crate) fn new(
        idle_ping_after: Duration,
        idle_pong_timeout: Duration,
        subscription_ack_timeout: Duration,
    ) -> Result<Self> {
        ensure!(
            !idle_ping_after.is_zero()
                && !idle_pong_timeout.is_zero()
                && !subscription_ack_timeout.is_zero(),
            "OKX public WebSocket timing durations must be positive"
        );
        Ok(Self {
            idle_ping_after,
            idle_pong_timeout,
            subscription_ack_timeout,
        })
    }
}

impl Default for OkxPublicMarketStreamTiming {
    fn default() -> Self {
        Self {
            idle_ping_after: OKX_WEBSOCKET_IDLE_PING_AFTER,
            idle_pong_timeout: OKX_WEBSOCKET_IDLE_PONG_TIMEOUT,
            subscription_ack_timeout: OKX_WEBSOCKET_SUBSCRIPTION_ACK_TIMEOUT,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxPublicMarketStreamConfig {
    pub url: String,
    pub instrument_ids: Vec<String>,
    pub instrument_type: String,
    pub subscribe_tickers: bool,
    pub subscribe_instruments: bool,
    pub candle_channels: Vec<String>,
    pub level2_instrument_ids: Vec<String>,
    pub(crate) reconnect_policy: OkxWebsocketReconnectPolicy,
}

impl OkxPublicMarketStreamConfig {
    pub fn new(url: String, instrument_ids: Vec<String>) -> Result<Self> {
        Self::with_reconnect_policy(
            url,
            instrument_ids,
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::default(),
        )
    }

    pub(crate) fn with_reconnect_policy(
        url: String,
        instrument_ids: Vec<String>,
        subscribe_tickers: bool,
        subscribe_instruments: bool,
        candle_channels: Vec<String>,
        reconnect_policy: OkxWebsocketReconnectPolicy,
    ) -> Result<Self> {
        Self::with_reconnect_policy_and_level2(
            url,
            instrument_ids,
            subscribe_tickers,
            subscribe_instruments,
            candle_channels,
            Vec::new(),
            reconnect_policy,
        )
    }

    pub(crate) fn with_reconnect_policy_and_level2(
        url: String,
        instrument_ids: Vec<String>,
        subscribe_tickers: bool,
        subscribe_instruments: bool,
        candle_channels: Vec<String>,
        level2_instrument_ids: Vec<String>,
        reconnect_policy: OkxWebsocketReconnectPolicy,
    ) -> Result<Self> {
        ensure!(
            !url.trim().is_empty(),
            "OKX public WebSocket URL must not be empty"
        );
        ensure!(
            !instrument_ids.is_empty(),
            "OKX public WebSocket stream requires at least one instrument"
        );
        let mut unique_instruments = BTreeSet::new();
        for instrument_id in instrument_ids {
            OkxSpotInstrumentId::try_from(instrument_id.as_str()).with_context(|| {
                format!(
                    "OKX public WebSocket instrument id {instrument_id:?} must be canonical SPOT"
                )
            })?;
            unique_instruments.insert(instrument_id);
        }
        let mut unique_candle_channels = BTreeSet::new();
        for channel in candle_channels {
            ensure!(
                !channel.trim().is_empty() && channel == channel.trim(),
                "OKX public WebSocket candle channel must be non-empty and trimmed"
            );
            ensure!(
                okx_public_candle_bar_for_channel(&channel).is_ok(),
                "OKX public WebSocket candle channel {channel} is not supported by this runtime"
            );
            unique_candle_channels.insert(channel);
        }
        let mut unique_level2_instruments = BTreeSet::new();
        for instrument_id in level2_instrument_ids {
            OkxSpotInstrumentId::try_from(instrument_id.as_str()).with_context(|| {
                format!("OKX Level-2 instrument id {instrument_id:?} must be canonical SPOT")
            })?;
            ensure!(
                unique_instruments.contains(&instrument_id),
                "OKX Level-2 instrument {instrument_id} must also belong to the stream instrument set"
            );
            unique_level2_instruments.insert(instrument_id);
        }
        ensure!(
            subscribe_tickers
                || subscribe_instruments
                || !unique_candle_channels.is_empty()
                || !unique_level2_instruments.is_empty(),
            "OKX WebSocket market stream must subscribe to tickers, instruments, candles, or books"
        );
        Ok(Self {
            url,
            instrument_ids: unique_instruments.into_iter().collect(),
            instrument_type: OKX_SPOT_INST_TYPE.to_owned(),
            subscribe_tickers,
            subscribe_instruments,
            candle_channels: unique_candle_channels.into_iter().collect(),
            level2_instrument_ids: unique_level2_instruments.into_iter().collect(),
            reconnect_policy,
        })
    }

    pub(crate) fn with_validated_instrument_type(mut self, instrument_type: &str) -> Result<Self> {
        ensure!(
            instrument_type == OKX_SPOT_INST_TYPE,
            "current OKX public WebSocket runtime admits only validated SPOT instruments"
        );
        self.instrument_type = instrument_type.to_owned();
        Ok(self)
    }

    pub(crate) fn health_identity(&self) -> OkxWebsocketStreamIdentity {
        if self.subscribe_tickers || self.subscribe_instruments {
            OkxWebsocketStreamIdentity::new(
                OkxWebsocketStreamKind::Public,
                OkxWebsocketChannelClass::PublicMarketData,
                self.instrument_ids.len(),
            )
        } else {
            OkxWebsocketStreamIdentity::new(
                OkxWebsocketStreamKind::Business,
                OkxWebsocketChannelClass::PublicCandles,
                self.instrument_ids.len(),
            )
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OkxWebsocketReconnectPolicy {
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl Default for OkxWebsocketReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(10),
        }
    }
}

impl OkxWebsocketReconnectPolicy {
    pub(crate) fn new(initial_backoff: Duration, max_backoff: Duration) -> Result<Self> {
        ensure!(
            !initial_backoff.is_zero(),
            "OKX WebSocket reconnect initial backoff must be non-zero"
        );
        ensure!(
            max_backoff >= initial_backoff,
            "OKX WebSocket reconnect max backoff must be greater than or equal to initial backoff"
        );
        Ok(Self {
            initial_backoff,
            max_backoff,
        })
    }

    const fn initial_backoff(self) -> Duration {
        self.initial_backoff
    }

    fn next_backoff(self, current: Duration) -> Duration {
        current.saturating_mul(2).min(self.max_backoff)
    }

    fn backoff_after_stream_run(
        self,
        current: Duration,
        outcome: &OkxWebsocketStreamRunOutcome,
    ) -> Duration {
        if outcome.subscribed() {
            self.initial_backoff()
        } else {
            self.next_backoff(current)
        }
    }
}

#[derive(Debug)]
pub(super) struct OkxWebsocketStreamRunOutcome {
    subscribed: bool,
    error: Option<anyhow::Error>,
}

impl OkxWebsocketStreamRunOutcome {
    pub(super) const fn disconnected(subscribed: bool) -> Self {
        Self {
            subscribed,
            error: None,
        }
    }

    pub(super) fn failed(subscribed: bool, error: anyhow::Error) -> Self {
        Self {
            subscribed,
            error: Some(error),
        }
    }

    pub(super) const fn subscribed(&self) -> bool {
        self.subscribed
    }

    pub(super) fn error(&self) -> Option<&anyhow::Error> {
        self.error.as_ref()
    }
}

#[derive(Clone, Debug, Default)]
pub struct OkxMarketDataCache {
    inner: Arc<Mutex<OkxMarketDataState>>,
}

impl OkxMarketDataCache {
    pub(crate) fn configure_runtime_observers(
        &self,
        reporter: OkxRuntimeEventReporter,
        latency: OkxLatencyMetrics,
    ) {
        let mut cache = lock(&self.inner);
        cache.runtime_event_reporter = Some(reporter);
        cache.latency = latency;
    }

    fn runtime_observers(&self) -> (Option<OkxRuntimeEventReporter>, OkxLatencyMetrics) {
        let cache = lock(&self.inner);
        (cache.runtime_event_reporter.clone(), cache.latency.clone())
    }

    pub(crate) fn protect_instruments(&self, instrument_ids: &[String]) {
        let mut cache = lock(&self.inner);
        cache
            .protected_instrument_ids
            .extend(instrument_ids.iter().cloned());
    }

    fn configure_level2_instruments(&self, instrument_ids: &[String]) -> Result<()> {
        let mut cache = lock(&self.inner);
        for instrument_id in instrument_ids {
            let instrument_id = OkxSpotInstrumentId::try_from(instrument_id.as_str())
                .with_context(|| {
                    format!(
                        "configured OKX Level-2 instrument {instrument_id:?} is not canonical SPOT"
                    )
                })?;
            cache
                .level2_books_by_instrument
                .entry(instrument_id.clone())
                .or_insert_with(|| OkxLevel2Book::new(instrument_id));
        }
        Ok(())
    }

    pub fn update_ticker(&self, hint: OkxMarketTickerHint) -> Result<()> {
        hint.ticker.validate_prices()?;
        ensure!(
            hint.ticker.inst_type == OKX_SPOT_INST_TYPE,
            "OKX WebSocket ticker {} has non-SPOT instType {}",
            hint.ticker.inst_id,
            hint.ticker.inst_type
        );
        self.insert_ticker_hint(ValidatedOkxMarketTickerHint(hint));
        Ok(())
    }

    pub fn update_candle(&self, hint: OkxMarketCandleHint) -> Result<()> {
        ensure!(
            !hint.inst_id.trim().is_empty(),
            "OKX WebSocket candle omitted instId"
        );
        ensure!(
            okx_public_candle_bar_for_channel(&hint.channel).is_ok(),
            "OKX WebSocket candle channel {} is not supported by this runtime",
            hint.channel
        );
        hint.bar.validate("OKX WebSocket candle")?;
        self.insert_candle_hint(ValidatedOkxMarketCandleHint(hint));
        Ok(())
    }

    fn insert_ticker_hint(&self, hint: ValidatedOkxMarketTickerHint) -> bool {
        let hint = hint.0;
        let mut cache = lock(&self.inner);
        let current_ts_ms = cache
            .tickers_by_inst_id
            .get(&hint.ticker.inst_id)
            .and_then(|current| current.source_ts_ms);
        if should_ignore_hint(current_ts_ms, hint.source_ts_ms) {
            return false;
        }
        cache
            .tickers_by_inst_id
            .insert(hint.ticker.inst_id.clone(), hint);
        evict_oldest_market_data_hints(
            &mut cache.tickers_by_inst_id,
            OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND,
            |hint| hint.received_at,
        );
        true
    }

    fn insert_candle_hint(&self, hint: ValidatedOkxMarketCandleHint) -> bool {
        let hint = hint.0;
        let key = (hint.inst_id.clone(), hint.channel.clone());
        let mut cache = lock(&self.inner);
        let bars = cache.candles_by_inst_id_and_channel.entry(key).or_default();
        let current = bars.get(&hint.bar.ts_ms);
        let current_ts_ms = current.and_then(|current| current.source_ts_ms);
        if should_ignore_hint(current_ts_ms, hint.source_ts_ms) {
            return false;
        }
        let newly_confirmed =
            hint.bar.confirm && current.is_none_or(|current| !current.bar.confirm);
        bars.insert(hint.bar.ts_ms, hint);
        while bars.len() > OKX_MARKET_CANDLE_CACHE_MAX_BARS {
            let Some(first_ts_ms) = bars.first_key_value().map(|(ts_ms, _)| *ts_ms) else {
                break;
            };
            bars.remove(&first_ts_ms);
        }
        evict_oldest_market_data_hints(
            &mut cache.candles_by_inst_id_and_channel,
            OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND,
            latest_candle_series_received_at,
        );
        newly_confirmed
    }

    pub fn update_instrument(&self, hint: OkxInstrumentUpdateHint) -> Result<()> {
        validate_instrument_update(&hint.instrument)?;
        self.insert_instrument_hint(hint);
        Ok(())
    }

    fn insert_instrument_hint(
        &self,
        hint: OkxInstrumentUpdateHint,
    ) -> OkxInstrumentHintDisposition {
        if hint.source_ts_ms.is_none() {
            // OKX may push SPOT state or parameter changes while both
            // contTdSwTime and upcChg are empty. Without an exchange ordering
            // timestamp the hint must wake REST revalidation, but it must not
            // replace an ordered cached hint.
            return OkxInstrumentHintDisposition::RestRevalidationRequired;
        }
        let mut cache = lock(&self.inner);
        let current_ts_ms = cache
            .instruments_by_inst_id
            .get(&hint.instrument.inst_id)
            .and_then(|current| current.source_ts_ms);
        if should_ignore_hint(current_ts_ms, hint.source_ts_ms) {
            return OkxInstrumentHintDisposition::IgnoredStale;
        }
        cache
            .instruments_by_inst_id
            .insert(hint.instrument.inst_id.clone(), hint);
        let protected_instrument_ids = cache.protected_instrument_ids.clone();
        evict_oldest_market_data_hints_except(
            &mut cache.instruments_by_inst_id,
            OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND,
            |hint| hint.received_at,
            &protected_instrument_ids,
        );
        OkxInstrumentHintDisposition::Cached
    }

    pub fn fresh_ticker(&self, inst_id: &str, max_staleness: Duration) -> Option<OkxTicker> {
        lock(&self.inner)
            .tickers_by_inst_id
            .get(inst_id)
            .and_then(|hint| {
                (hint.received_at.elapsed() <= max_staleness).then(|| hint.ticker.clone())
            })
    }

    pub fn fresh_candles(
        &self,
        inst_id: &str,
        channel: &str,
        max_staleness: Duration,
    ) -> Vec<MarketBar> {
        lock(&self.inner)
            .candles_by_inst_id_and_channel
            .get(&(inst_id.to_owned(), channel.to_owned()))
            .map(|bars| {
                bars.values()
                    .filter(|hint| hint.received_at.elapsed() <= max_staleness)
                    .map(|hint| hint.bar.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn fresh_instrument(
        &self,
        inst_id: &str,
        max_staleness: Duration,
    ) -> Option<OkxWebsocketInstrumentUpdate> {
        lock(&self.inner)
            .instruments_by_inst_id
            .get(inst_id)
            .and_then(|hint| {
                (hint.received_at.elapsed() <= max_staleness).then(|| hint.instrument.clone())
            })
    }

    pub fn fresh_level2_features(
        &self,
        instrument_id: &str,
        max_staleness: Duration,
    ) -> Option<Arc<OkxLevel2FeatureSnapshot>> {
        let instrument_id = OkxSpotInstrumentId::try_from(instrument_id).ok()?;
        lock(&self.inner)
            .level2_books_by_instrument
            .get(&instrument_id)?
            .features()
            .filter(|features| features.received_at.elapsed() <= max_staleness)
    }

    fn apply_level2(
        &self,
        update: OkxLevel2Update,
        latency: &OkxLatencyMetrics,
    ) -> Result<OkxLevel2ApplyOutcome> {
        let parsed_at = update.parsed_at;
        let mut cache = lock(&self.inner);
        let instrument_id = update.instrument_id.clone();
        let book = cache
            .level2_books_by_instrument
            .get_mut(&instrument_id)
            .with_context(|| {
                format!("OKX Level-2 update for unconfigured instrument {instrument_id}")
            })?;
        match book.apply_with_clock(update, Instant::now) {
            Ok(outcome) => {
                if let OkxLevel2ApplyOutcome::Features(features) = &outcome {
                    latency.record(
                        OkxLatencyStage::ParsedToBookApplied,
                        features
                            .book_applied_at
                            .saturating_duration_since(parsed_at),
                    );
                    latency.record(
                        OkxLatencyStage::BookAppliedToFeaturesReady,
                        features
                            .features_ready_at
                            .saturating_duration_since(features.book_applied_at),
                    );
                }
                Ok(outcome)
            }
            Err(error) => {
                if error.is_sequence_gap() {
                    latency.record_sequence_gap_invalidation();
                }
                if error.is_stale_rejection() {
                    latency.record_stale_event_rejection();
                }
                Err(error.into())
            }
        }
    }

    pub(crate) fn invalidate_level2_for_reconnect(&self, instrument_ids: &[String]) {
        let mut cache = lock(&self.inner);
        for instrument_id in instrument_ids {
            if let Ok(instrument_id) = OkxSpotInstrumentId::try_from(instrument_id.as_str())
                && let Some(book) = cache.level2_books_by_instrument.get_mut(&instrument_id)
            {
                book.invalidate();
            }
        }
    }
}

#[derive(Debug, Default)]
struct OkxMarketDataState {
    tickers_by_inst_id: HashMap<String, OkxMarketTickerHint>,
    candles_by_inst_id_and_channel: HashMap<(String, String), BTreeMap<i64, OkxMarketCandleHint>>,
    instruments_by_inst_id: HashMap<String, OkxInstrumentUpdateHint>,
    protected_instrument_ids: BTreeSet<String>,
    level2_books_by_instrument: BTreeMap<OkxSpotInstrumentId, OkxLevel2Book>,
    runtime_event_reporter: Option<OkxRuntimeEventReporter>,
    latency: OkxLatencyMetrics,
}

fn evict_oldest_market_data_hints<K, V>(
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

fn evict_oldest_market_data_hints_except<K, V>(
    hints: &mut HashMap<K, V>,
    max_len: usize,
    received_at: impl Fn(&V) -> Instant,
    protected_keys: &BTreeSet<K>,
) where
    K: Clone + Eq + Hash + Ord,
{
    while hints.len() > max_len {
        let oldest_unprotected_key = hints
            .iter()
            .filter(|(key, _)| !protected_keys.contains(key))
            .min_by_key(|(_, hint)| received_at(hint))
            .map(|(key, _)| key.clone());
        let oldest_key = oldest_unprotected_key.or_else(|| {
            hints
                .iter()
                .min_by_key(|(_, hint)| received_at(hint))
                .map(|(key, _)| key.clone())
        });
        let Some(oldest_key) = oldest_key else {
            break;
        };
        hints.remove(&oldest_key);
    }
}

fn latest_candle_series_received_at(bars: &BTreeMap<i64, OkxMarketCandleHint>) -> Instant {
    bars.values()
        .map(|hint| hint.received_at)
        .max()
        .unwrap_or_else(Instant::now)
}

#[derive(Clone, Debug, PartialEq)]
pub struct OkxMarketTickerHint {
    pub ticker: OkxTicker,
    pub source_ts_ms: Option<i64>,
    pub received_at: Instant,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OkxMarketCandleHint {
    pub inst_id: String,
    pub channel: String,
    pub bar: MarketBar,
    pub source_ts_ms: Option<i64>,
    pub received_at: Instant,
}

#[derive(Debug, PartialEq)]
struct ValidatedOkxMarketTickerHint(OkxMarketTickerHint);

#[derive(Debug, PartialEq)]
struct ValidatedOkxMarketCandleHint(OkxMarketCandleHint);

#[derive(Clone, Debug, PartialEq)]
pub struct OkxInstrumentUpdateHint {
    pub instrument: OkxWebsocketInstrumentUpdate,
    pub source_ts_ms: Option<i64>,
    pub received_at: Instant,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxWebsocketInstrumentUpdate {
    #[serde(rename = "instType")]
    pub inst_type: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "groupId")]
    pub group_id: String,
    pub state: String,
    #[serde(rename = "tickSz")]
    pub tick_size: String,
    #[serde(rename = "lotSz")]
    pub lot_size: String,
    #[serde(rename = "minSz")]
    pub min_size: String,
    #[serde(rename = "maxLmtSz", default)]
    pub max_limit_size: String,
    #[serde(rename = "maxLmtAmt", default)]
    pub max_limit_amount: String,
    #[serde(rename = "maxMktSz", default)]
    pub max_market_size: String,
    #[serde(rename = "maxMktAmt", default)]
    pub max_market_amount: String,
    #[serde(rename = "maxTriggerSz", default)]
    pub max_trigger_size: String,
    #[serde(rename = "contTdSwTime", default)]
    pub continuous_trading_switch_time: String,
    #[serde(rename = "upcChg", default)]
    pub upcoming_changes: Vec<OkxWebsocketInstrumentParameterChange>,
}

impl OkxWebsocketInstrumentUpdate {
    pub(crate) fn ensure_live(&self) -> Result<()> {
        ensure!(
            self.state == "live",
            "OKX WebSocket instrument {} state {} is not live",
            self.inst_id,
            self.state
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxWebsocketInstrumentParameterChange {
    #[serde(rename = "effTime", default)]
    pub effective_time: String,
}

#[derive(Debug)]
pub struct OkxPublicMarketStream {
    task: OkxWebsocketTaskHandle,
}

impl OkxPublicMarketStream {
    pub fn spawn(config: OkxPublicMarketStreamConfig, cache: OkxMarketDataCache) -> Self {
        Self::spawn_with_health(config, cache, None)
    }

    pub(crate) fn spawn_with_health(
        config: OkxPublicMarketStreamConfig,
        cache: OkxMarketDataCache,
        health: Option<OkxWebsocketHealthReporter>,
    ) -> Self {
        Self::spawn_with_health_and_timing(
            config,
            cache,
            health,
            OkxPublicMarketStreamTiming::default(),
        )
    }

    pub(crate) fn spawn_with_health_and_timing(
        config: OkxPublicMarketStreamConfig,
        cache: OkxMarketDataCache,
        health: Option<OkxWebsocketHealthReporter>,
        timing: OkxPublicMarketStreamTiming,
    ) -> Self {
        let stream = config.health_identity();
        let supervisor_health = health.clone();
        let task = OkxWebsocketTaskHandle::spawn(stream, supervisor_health, async move {
            run_public_market_stream(config, cache, health, timing).await;
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

impl Drop for OkxPublicMarketStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) async fn report_websocket_health(
    health: Option<&OkxWebsocketHealthReporter>,
    event: OkxWebsocketHealthEvent,
) {
    if let Some(health) = health {
        health.report(event).await;
    }
}

async fn run_public_market_stream(
    config: OkxPublicMarketStreamConfig,
    cache: OkxMarketDataCache,
    health: Option<OkxWebsocketHealthReporter>,
    timing: OkxPublicMarketStreamTiming,
) {
    let mut backoff = config.reconnect_policy.initial_backoff();
    let mut reconnect_attempt = 0_u64;
    loop {
        let outcome = run_public_market_stream_once_with_health_and_timing(
            &config,
            cache.clone(),
            health.as_ref(),
            timing,
        )
        .await;
        if !config.level2_instrument_ids.is_empty() {
            cache.invalidate_level2_for_reconnect(&config.level2_instrument_ids);
            let (_, latency) = cache.runtime_observers();
            latency.record_reconnect();
        }
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
                safety_event = "ws_public_reconnect",
                error = %error,
                reconnect_backoff_ms = backoff.as_millis(),
                "OKX public WebSocket stream failed; reconnecting"
            ),
            None => warn!(
                safety_event = "ws_public_reconnect",
                reconnect_backoff_ms = backoff.as_millis(),
                "OKX public WebSocket stream disconnected; reconnecting"
            ),
        }
        time::sleep(backoff).await;
        backoff = config
            .reconnect_policy
            .backoff_after_stream_run(backoff, &outcome);
    }
}

#[cfg(test)]
async fn run_public_market_stream_once(
    config: &OkxPublicMarketStreamConfig,
    cache: OkxMarketDataCache,
) -> OkxWebsocketStreamRunOutcome {
    run_public_market_stream_once_with_health(config, cache, None).await
}

#[cfg(test)]
async fn run_public_market_stream_once_with_health(
    config: &OkxPublicMarketStreamConfig,
    cache: OkxMarketDataCache,
    health: Option<&OkxWebsocketHealthReporter>,
) -> OkxWebsocketStreamRunOutcome {
    run_public_market_stream_once_with_health_and_timing(
        config,
        cache,
        health,
        OkxPublicMarketStreamTiming::default(),
    )
    .await
}

async fn run_public_market_stream_once_with_health_and_timing(
    config: &OkxPublicMarketStreamConfig,
    cache: OkxMarketDataCache,
    health: Option<&OkxWebsocketHealthReporter>,
    timing: OkxPublicMarketStreamTiming,
) -> OkxWebsocketStreamRunOutcome {
    let mut subscribed = false;
    let mut pending_subscription_acks = public_market_subscription_acks(config);
    let result = async {
        cache.configure_level2_instruments(&config.level2_instrument_ids)?;
        if config.subscribe_instruments {
            cache.protect_instruments(&config.instrument_ids);
        }
        report_websocket_health(
            health,
            OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::ConnectAttempt,
                config.health_identity(),
            ),
        )
        .await;
        let (mut stream, _) = connect_async(config.url.as_str())
            .await
            .with_context(|| format!("failed connecting to OKX public WebSocket {}", config.url))?;
        report_websocket_health(
            health,
            OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::Connected,
                config.health_identity(),
            ),
        )
        .await;
        let subscription = public_market_subscription(
            &config.instrument_ids,
            &config.instrument_type,
            config.subscribe_tickers,
            config.subscribe_instruments,
            &config.candle_channels,
            &config.level2_instrument_ids,
        )?;
        stream
            .send(Message::Text(subscription.into()))
            .await
            .context("failed subscribing to OKX public market data stream")?;
        if let Err(error) = wait_for_public_subscription_acks(
            &mut stream,
            &cache,
            &mut pending_subscription_acks,
            &config.level2_instrument_ids,
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
            safety_event = "ws_public_subscription_success",
            instrument_count = config.instrument_ids.len(),
            subscribe_tickers = config.subscribe_tickers,
            subscribe_instruments = config.subscribe_instruments,
            candle_channel_count = config.candle_channels.len(),
            level2_instrument_count = config.level2_instrument_ids.len(),
            "subscribed to OKX public market data WebSocket stream"
        );

        while let Some(message) = next_websocket_message_with_keepalive_and_timing(
            &mut stream,
            "public market data",
            timing.idle_ping_after,
            timing.idle_pong_timeout,
        )
        .await?
        {
            match message {
                Message::Text(payload) if payload.as_str() == OKX_WEBSOCKET_TEXT_PONG => {}
                Message::Text(payload) => {
                    apply_public_market_data_message_and_report(
                        &cache,
                        payload.as_ref(),
                        Instant::now(),
                        &config.level2_instrument_ids,
                    )
                    .await?;
                }
                Message::Ping(payload) => {
                    stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed replying to OKX public WebSocket ping")?;
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
            report_public_stream_outcome(config, &outcome, health).await;
            outcome
        }
        Err(error) => {
            let outcome = OkxWebsocketStreamRunOutcome::failed(subscribed, error);
            report_public_stream_outcome(config, &outcome, health).await;
            outcome
        }
    }
}

async fn report_public_stream_outcome(
    config: &OkxPublicMarketStreamConfig,
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

async fn wait_for_public_subscription_acks<S>(
    stream: &mut WebSocketStream<S>,
    cache: &OkxMarketDataCache,
    pending_subscription_acks: &mut BTreeSet<OkxWebsocketSubscriptionAck>,
    level2_instrument_ids: &[String],
    timing: OkxPublicMarketStreamTiming,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    time::timeout(timing.subscription_ack_timeout, async {
        let mut acknowledged_subscription_acks = BTreeSet::new();
        while !pending_subscription_acks.is_empty() {
            let Some(message) = next_websocket_message_with_keepalive_and_timing(
                stream,
                "public market data",
                timing.idle_ping_after,
                timing.idle_pong_timeout,
            )
            .await?
            else {
                return Err(OkxWebsocketProtocolError::ClosedBeforeSubscriptionAck {
                    context: "public".to_owned(),
                }
                .into());
            };
            match message {
                Message::Text(payload) if payload.as_str() == OKX_WEBSOCKET_TEXT_PONG => {}
                Message::Text(payload) => {
                    match parse_subscription_event(payload.as_ref(), "public")? {
                        OkxWebsocketSubscriptionEvent::Acknowledged(ack) => {
                            acknowledge_subscription(
                                pending_subscription_acks,
                                ack.clone(),
                                "public",
                            )?;
                            acknowledged_subscription_acks.insert(ack);
                        }
                        OkxWebsocketSubscriptionEvent::Control => {}
                        OkxWebsocketSubscriptionEvent::Data(ack) => {
                            if !acknowledged_subscription_acks.contains(&ack) {
                                return Err(OkxWebsocketProtocolError::DataBeforeSubscriptionAck {
                                    context: "public".to_owned(),
                                    ack: Box::new(ack),
                                }
                                .into());
                            }
                            apply_public_market_data_message_and_report(
                                cache,
                                payload.as_ref(),
                                Instant::now(),
                                level2_instrument_ids,
                            )
                            .await?;
                        }
                        OkxWebsocketSubscriptionEvent::Error { code, msg, arg } => {
                            return Err(OkxWebsocketProtocolError::SubscriptionErrorEvent {
                                context: "public".to_owned(),
                                code,
                                msg,
                                ack: arg.map(Box::new),
                            }
                            .into());
                        }
                        OkxWebsocketSubscriptionEvent::Other => {
                            return Err(
                                OkxWebsocketProtocolError::NonAckTextBeforeSubscriptionAck {
                                    context: "public".to_owned(),
                                }
                                .into(),
                            );
                        }
                    }
                }
                Message::Ping(payload) => {
                    stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed replying to OKX public WebSocket ping")?;
                }
                Message::Close(_) => {
                    return Err(OkxWebsocketProtocolError::ClosedBeforeSubscriptionAck {
                        context: "public".to_owned(),
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
            context: "public".to_owned(),
        },
    )?
}

fn public_market_subscription_acks(
    config: &OkxPublicMarketStreamConfig,
) -> BTreeSet<OkxWebsocketSubscriptionAck> {
    let mut acks = BTreeSet::new();
    if config.subscribe_instruments {
        acks.insert(OkxWebsocketSubscriptionAck {
            channel: OKX_PUBLIC_INSTRUMENTS_CHANNEL.to_owned(),
            inst_id: None,
            inst_type: Some(config.instrument_type.clone()),
        });
    }
    for instrument_id in &config.instrument_ids {
        if config.subscribe_tickers {
            acks.insert(OkxWebsocketSubscriptionAck {
                channel: OKX_PUBLIC_TICKERS_CHANNEL.to_owned(),
                inst_id: Some(instrument_id.clone()),
                inst_type: None,
            });
        }
        for channel in &config.candle_channels {
            acks.insert(OkxWebsocketSubscriptionAck {
                channel: channel.clone(),
                inst_id: Some(instrument_id.clone()),
                inst_type: None,
            });
        }
    }
    for instrument_id in &config.level2_instrument_ids {
        acks.insert(OkxWebsocketSubscriptionAck {
            channel: OKX_LEVEL2_BOOKS_CHANNEL.to_owned(),
            inst_id: Some(instrument_id.clone()),
            inst_type: None,
        });
    }
    acks
}

pub(crate) async fn next_websocket_message_with_keepalive_and_timing<S>(
    stream: &mut WebSocketStream<S>,
    context: &str,
    idle_ping_after: Duration,
    idle_pong_timeout: Duration,
) -> Result<Option<Message>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match time::timeout(idle_ping_after, stream.next()).await {
        Ok(Some(message)) => message
            .with_context(|| format!("failed reading OKX {context} WebSocket message"))
            .map(Some),
        Ok(None) => Ok(None),
        Err(_) => {
            stream
                .send(Message::Text(OKX_WEBSOCKET_TEXT_PING.into()))
                .await
                .with_context(|| format!("failed sending OKX {context} WebSocket idle ping"))?;
            wait_for_idle_pong(stream, context, idle_pong_timeout).await
        }
    }
}

async fn wait_for_idle_pong<S>(
    stream: &mut WebSocketStream<S>,
    context: &str,
    idle_pong_timeout: Duration,
) -> Result<Option<Message>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let timeout = time::sleep(idle_pong_timeout);
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            () = &mut timeout => {
                bail!("timed out waiting for OKX {context} WebSocket idle pong");
            }
            message = stream.next() => {
                let message = message
                    .with_context(|| format!("OKX {context} WebSocket stream closed before idle pong"))?
                    .with_context(|| format!("failed reading OKX {context} WebSocket idle pong"))?;
                match message {
                    Message::Text(payload) if payload.as_str() == OKX_WEBSOCKET_TEXT_PONG => {
                        return Ok(Some(Message::Text(payload)));
                    }
                    Message::Ping(payload) => {
                        stream
                            .send(Message::Pong(payload))
                            .await
                            .with_context(|| format!("failed replying to OKX {context} WebSocket ping"))?;
                    }
                    Message::Pong(payload) => return Ok(Some(Message::Pong(payload))),
                    Message::Close(_) => bail!(
                        "OKX {context} WebSocket stream closed before idle pong"
                    ),
                    Message::Text(payload) => return Ok(Some(Message::Text(payload))),
                    Message::Binary(_) | Message::Frame(_) => bail!(
                        "OKX {context} WebSocket returned non-pong message after idle ping"
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
fn public_ticker_subscription(instrument_ids: &[String]) -> Result<String> {
    ensure!(
        !instrument_ids.is_empty(),
        "OKX public WebSocket ticker subscription requires at least one instrument"
    );
    let request = OkxWebsocketSubscribeRequest {
        op: "subscribe",
        args: instrument_ids
            .iter()
            .map(|instrument_id| OkxWebsocketSubscribeArg {
                channel: OKX_PUBLIC_TICKERS_CHANNEL,
                inst_id: Some(instrument_id),
                inst_type: None,
            })
            .collect(),
    };
    serde_json::to_string(&request).context("failed serializing OKX ticker subscription")
}

fn public_market_subscription(
    instrument_ids: &[String],
    instrument_type: &str,
    subscribe_tickers: bool,
    subscribe_instruments: bool,
    candle_channels: &[String],
    level2_instrument_ids: &[String],
) -> Result<String> {
    ensure!(
        !instrument_ids.is_empty(),
        "OKX public WebSocket market subscription requires at least one instrument"
    );
    ensure!(
        instrument_type == OKX_SPOT_INST_TYPE,
        "current OKX public WebSocket subscription admits only validated SPOT"
    );
    ensure!(
        subscribe_tickers
            || subscribe_instruments
            || !candle_channels.is_empty()
            || !level2_instrument_ids.is_empty(),
        "OKX public WebSocket market subscription requires tickers, instruments, candles, or books"
    );
    let mut args = Vec::new();
    if subscribe_instruments {
        args.push(OkxWebsocketSubscribeArg {
            channel: OKX_PUBLIC_INSTRUMENTS_CHANNEL,
            inst_id: None,
            inst_type: Some(instrument_type),
        });
    }
    for instrument_id in instrument_ids {
        if subscribe_tickers {
            args.push(OkxWebsocketSubscribeArg {
                channel: OKX_PUBLIC_TICKERS_CHANNEL,
                inst_id: Some(instrument_id),
                inst_type: None,
            });
        }
        for channel in candle_channels {
            args.push(OkxWebsocketSubscribeArg {
                channel,
                inst_id: Some(instrument_id),
                inst_type: None,
            });
        }
    }
    for instrument_id in level2_instrument_ids {
        args.push(OkxWebsocketSubscribeArg {
            channel: OKX_LEVEL2_BOOKS_CHANNEL,
            inst_id: Some(instrument_id),
            inst_type: None,
        });
    }
    let request = OkxWebsocketSubscribeRequest {
        op: "subscribe",
        args,
    };
    serde_json::to_string(&request).context("failed serializing OKX market data subscription")
}

async fn apply_public_market_data_message_and_report(
    cache: &OkxMarketDataCache,
    payload: &str,
    received_at: Instant,
    level2_instrument_ids: &[String],
) -> Result<usize> {
    let (reporter, latency) = cache.runtime_observers();
    let applied = apply_public_market_data_message_inner(
        cache,
        payload,
        received_at,
        &latency,
        level2_instrument_ids,
    )?;
    if let Some(reporter) = reporter {
        for event in applied.public_events {
            reporter.report_public(event).await?;
        }
        for features in applied.level2_features {
            reporter.report_level2(features);
        }
    }
    Ok(applied.count)
}

struct AppliedPublicMarketData {
    count: usize,
    public_events: Vec<OkxPublicRuntimeEvent>,
    level2_features: Vec<Arc<OkxLevel2FeatureSnapshot>>,
}

fn apply_public_market_data_message_inner(
    cache: &OkxMarketDataCache,
    payload: &str,
    received_at: Instant,
    latency: &OkxLatencyMetrics,
    level2_instrument_ids: &[String],
) -> Result<AppliedPublicMarketData> {
    let hints = parse_public_market_data_message(payload, received_at)?;
    let count = hints.len();
    let mut public_events = Vec::new();
    let mut level2_features = Vec::new();
    for hint in hints {
        match hint {
            OkxMarketDataHint::Ticker(hint) => {
                cache.insert_ticker_hint(hint);
            }
            OkxMarketDataHint::Candle(hint) => {
                let runtime_event = hint.0.bar.confirm.then(|| OkxPublicRuntimeEvent {
                    kind: OkxPublicRuntimeEventKind::ConfirmedCandle {
                        instrument_id: hint.0.inst_id.clone(),
                        bar_ts_ms: hint.0.bar.ts_ms,
                    },
                    received_at: hint.0.received_at,
                });
                if cache.insert_candle_hint(hint)
                    && let Some(runtime_event) = runtime_event
                {
                    public_events.push(runtime_event);
                }
            }
            OkxMarketDataHint::Instrument(hint) => {
                let runtime_event = OkxPublicRuntimeEvent {
                    kind: OkxPublicRuntimeEventKind::InstrumentUpdated {
                        instrument_id: hint.instrument.inst_id.clone(),
                    },
                    received_at: hint.received_at,
                };
                if cache
                    .insert_instrument_hint(*hint)
                    .requires_rest_revalidation()
                {
                    public_events.push(runtime_event);
                }
            }
            OkxMarketDataHint::Level2(update) => {
                ensure!(
                    level2_instrument_ids
                        .iter()
                        .any(|instrument_id| instrument_id == update.instrument_id.as_str()),
                    "OKX Level-2 update for instrument {} was not requested by this stream",
                    update.instrument_id
                );
                latency.record(
                    OkxLatencyStage::FrameReceivedToParsed,
                    update
                        .parsed_at
                        .saturating_duration_since(update.received_at),
                );
                if let OkxLevel2ApplyOutcome::Features(features) =
                    cache.apply_level2(update, latency)?
                {
                    level2_features.push(features);
                }
            }
        }
    }
    Ok(AppliedPublicMarketData {
        count,
        public_events,
        level2_features,
    })
}

fn parse_public_market_data_message(
    payload: &str,
    received_at: Instant,
) -> Result<Vec<OkxMarketDataHint>> {
    let message: OkxWebsocketPublicMessage<'_> =
        serde_json::from_str(payload).context("failed parsing OKX public WebSocket message")?;
    reject_websocket_notice(message.event.as_deref(), message.code.as_deref(), "public")?;
    if message.event.as_deref() == Some("error") {
        bail!(
            "OKX public WebSocket error {}: {}",
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
        .context("OKX public WebSocket data omitted arg")?;

    match arg.channel.as_str() {
        OKX_PUBLIC_TICKERS_CHANNEL => {
            let rows: Vec<OkxWebsocketTickerData> =
                parse_websocket_data(data, "failed parsing OKX public ticker update")?;
            let mut hints = Vec::with_capacity(rows.len());
            for row in rows {
                ensure!(
                    arg.inst_id.is_none() || arg.inst_id.as_deref() == Some(row.inst_id.as_str()),
                    "OKX public WebSocket ticker {} did not match arg instrument {:?}",
                    row.inst_id,
                    arg.inst_id
                );
                let ticker = OkxTicker {
                    inst_type: row.inst_type,
                    inst_id: row.inst_id,
                    bid_px: row.bid_px,
                    ask_px: row.ask_px,
                    last: row.last,
                };
                ticker.validate_prices()?;
                ensure!(
                    ticker.inst_type == OKX_SPOT_INST_TYPE,
                    "OKX WebSocket ticker {} has non-SPOT instType {}",
                    ticker.inst_id,
                    ticker.inst_type
                );
                hints.push(OkxMarketDataHint::Ticker(ValidatedOkxMarketTickerHint(
                    OkxMarketTickerHint {
                        ticker,
                        source_ts_ms: parse_optional_ts_ms("OKX WebSocket ticker ts", &row.ts)?,
                        received_at,
                    },
                )));
            }
            Ok(hints)
        }
        channel if okx_public_candle_bar_for_channel(channel).is_ok() => {
            let inst_id = arg
                .inst_id
                .context("OKX public WebSocket candle data omitted arg instId")?;
            ensure!(
                !inst_id.trim().is_empty(),
                "OKX WebSocket candle omitted instId"
            );
            let rows: Vec<Vec<String>> =
                parse_websocket_data(data, "failed parsing OKX public candle update")?;
            let mut hints = Vec::with_capacity(rows.len());
            for row in rows {
                let bar = parse_candle_data(&row)?;
                hints.push(OkxMarketDataHint::Candle(ValidatedOkxMarketCandleHint(
                    OkxMarketCandleHint {
                        inst_id: inst_id.clone(),
                        channel: arg.channel.clone(),
                        source_ts_ms: Some(bar.ts_ms),
                        bar,
                        received_at,
                    },
                )));
            }
            Ok(hints)
        }
        OKX_PUBLIC_INSTRUMENTS_CHANNEL => {
            let inst_type = arg
                .inst_type
                .as_deref()
                .context("OKX public WebSocket instrument data omitted arg instType")?;
            ensure!(
                inst_type == OKX_SPOT_INST_TYPE,
                "OKX public WebSocket instruments arg has non-SPOT instType {inst_type}"
            );
            let rows: Vec<OkxWebsocketInstrumentUpdate> =
                parse_websocket_data(data, "failed parsing OKX public instrument update")?;
            let mut hints = Vec::with_capacity(rows.len());
            for instrument in rows {
                ensure!(
                    instrument.inst_type == inst_type,
                    "OKX public WebSocket instrument {} did not match arg instType {inst_type}",
                    instrument.inst_id
                );
                let source_ts_ms = instrument_update_source_ts_ms(&instrument)?;
                validate_instrument_update(&instrument)?;
                hints.push(OkxMarketDataHint::Instrument(Box::new(
                    OkxInstrumentUpdateHint {
                        instrument,
                        source_ts_ms,
                        received_at,
                    },
                )));
            }
            Ok(hints)
        }
        OKX_LEVEL2_BOOKS_CHANNEL => {
            let inst_id = arg.inst_id.context("OKX books data omitted arg instId")?;
            let expected_instrument = OkxSpotInstrumentId::try_from(inst_id.as_str())
                .with_context(|| {
                    format!("OKX books data instrument {inst_id:?} is not canonical SPOT")
                })?;
            let action = message
                .action
                .as_deref()
                .context("OKX books data omitted action")?;
            let message = parse_books_message(
                &expected_instrument,
                OKX_LEVEL2_BOOKS_CHANNEL,
                &inst_id,
                action,
                data.get(),
            )?;
            let parsed_at = Instant::now();
            Ok(vec![OkxMarketDataHint::Level2(
                OkxLevel2Update::from_validated(message, received_at, parsed_at),
            )])
        }
        _ => bail!(
            "OKX public WebSocket channel {} is not supported",
            arg.channel
        ),
    }
}

pub(crate) fn okx_public_candle_channel_for_bar(bar: &str) -> Result<&'static str> {
    match bar {
        "1m" => Ok(OKX_PUBLIC_CANDLE_1M_CHANNEL),
        "1s" => Ok(OKX_PUBLIC_CANDLE_1S_CHANNEL),
        "5m" => Ok(OKX_PUBLIC_CANDLE_5M_CHANNEL),
        _ => bail!("OKX WebSocket candle bar {bar} is not supported by this runtime"),
    }
}

pub(crate) fn okx_public_candle_bar_for_channel(channel: &str) -> Result<&'static str> {
    match channel {
        OKX_PUBLIC_CANDLE_1M_CHANNEL => Ok("1m"),
        OKX_PUBLIC_CANDLE_1S_CHANNEL => Ok("1s"),
        OKX_PUBLIC_CANDLE_5M_CHANNEL => Ok("5m"),
        _ => bail!("OKX WebSocket candle channel {channel} is not supported by this runtime"),
    }
}

pub(super) fn parse_websocket_data<T>(data: &RawValue, context: &str) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    serde_json::from_str(data.get()).with_context(|| context.to_owned())
}

fn parse_candle_data(values: &[String]) -> Result<MarketBar> {
    ensure!(
        values.len() >= 9,
        "OKX WebSocket candle payload must contain at least 9 fields"
    );
    let ts_ms = parse_required_i64("OKX WebSocket candle ts", &values[0])?;
    ensure!(ts_ms > 0, "OKX WebSocket candle ts must be positive");
    let open = parse_positive_f64("OKX WebSocket candle open", &values[1])?;
    let high = parse_positive_f64("OKX WebSocket candle high", &values[2])?;
    let low = parse_positive_f64("OKX WebSocket candle low", &values[3])?;
    let close = parse_positive_f64("OKX WebSocket candle close", &values[4])?;
    for (name, value) in [
        ("volume", &values[5]),
        ("volumeCcy", &values[6]),
        ("volumeCcyQuote", &values[7]),
    ] {
        parse_non_negative_f64(&format!("OKX WebSocket candle {name}"), value)?;
    }
    ensure!(
        values[8] == "0" || values[8] == "1",
        "OKX WebSocket candle confirm flag must be 0 or 1"
    );
    let bar = MarketBar {
        ts_ms,
        open,
        high,
        low,
        close,
        confirm: values[8] == "1",
    };
    bar.validate("OKX WebSocket candle")?;
    Ok(bar)
}

fn validate_instrument_update(instrument: &OkxWebsocketInstrumentUpdate) -> Result<()> {
    ensure!(
        instrument.inst_type == OKX_SPOT_INST_TYPE,
        "OKX WebSocket instrument {} has non-SPOT instType {}",
        instrument.inst_id,
        instrument.inst_type
    );
    ensure!(
        !instrument.inst_id.trim().is_empty() && instrument.inst_id == instrument.inst_id.trim(),
        "OKX WebSocket instrument update omitted trimmed instId"
    );
    ensure!(
        !instrument.state.trim().is_empty() && instrument.state == instrument.state.trim(),
        "OKX WebSocket instrument {} omitted trimmed state",
        instrument.inst_id
    );
    parse_positive_f64("OKX WebSocket instrument tickSz", &instrument.tick_size)?;
    parse_positive_f64("OKX WebSocket instrument lotSz", &instrument.lot_size)?;
    parse_positive_f64("OKX WebSocket instrument minSz", &instrument.min_size)?;
    for (name, value) in [
        ("maxLmtSz", &instrument.max_limit_size),
        ("maxLmtAmt", &instrument.max_limit_amount),
        ("maxMktSz", &instrument.max_market_size),
        ("maxMktAmt", &instrument.max_market_amount),
        ("maxTriggerSz", &instrument.max_trigger_size),
    ] {
        if !value.trim().is_empty() {
            parse_positive_f64(&format!("OKX WebSocket instrument {name}"), value)?;
        }
    }
    Ok(())
}

fn instrument_update_source_ts_ms(
    instrument: &OkxWebsocketInstrumentUpdate,
) -> Result<Option<i64>> {
    let mut source_ts_ms = parse_optional_ts_ms(
        "OKX WebSocket instrument contTdSwTime",
        &instrument.continuous_trading_switch_time,
    )?;
    for change in &instrument.upcoming_changes {
        let Some(effective_time) = parse_optional_ts_ms(
            "OKX WebSocket instrument upcChg effTime",
            &change.effective_time,
        )?
        else {
            continue;
        };
        source_ts_ms =
            Some(source_ts_ms.map_or(effective_time, |current| current.max(effective_time)));
    }
    Ok(source_ts_ms)
}

fn parse_optional_ts_ms(context: &str, value: &str) -> Result<Option<i64>> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    let ts_ms = parse_required_i64(context, value)?;
    ensure!(ts_ms > 0, "{context} must be positive");
    Ok(Some(ts_ms))
}

fn parse_required_i64(context: &str, value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .with_context(|| format!("invalid {context} {value}"))
}

fn parse_positive_f64(context: &str, value: &str) -> Result<f64> {
    let parsed = value
        .parse::<f64>()
        .with_context(|| format!("invalid {context} {value}"))?;
    ensure!(
        parsed.is_finite() && parsed > 0.0,
        "{context} must be finite and positive"
    );
    Ok(parsed)
}

fn parse_non_negative_f64(context: &str, value: &str) -> Result<f64> {
    let parsed = value
        .parse::<f64>()
        .with_context(|| format!("invalid {context} {value}"))?;
    ensure!(
        parsed.is_finite() && parsed >= 0.0,
        "{context} must be finite and non-negative"
    );
    Ok(parsed)
}

fn should_ignore_hint(current_ts_ms: Option<i64>, incoming_ts_ms: Option<i64>) -> bool {
    match incoming_ts_ms {
        Some(incoming) => current_ts_ms.is_some_and(|current| incoming < current),
        None => true,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OkxInstrumentHintDisposition {
    Cached,
    RestRevalidationRequired,
    IgnoredStale,
}

impl OkxInstrumentHintDisposition {
    const fn requires_rest_revalidation(self) -> bool {
        matches!(self, Self::Cached | Self::RestRevalidationRequired)
    }
}

fn lock<T: Default>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                safety_event = "ws_public_hint_cache_poisoned",
                "OKX market data cache mutex poisoned; discarding WebSocket hints"
            );
            let mut guard = poisoned.into_inner();
            *guard = T::default();
            mutex.clear_poison();
            guard
        }
    }
}

#[derive(Serialize)]
struct OkxWebsocketSubscribeRequest<'a> {
    op: &'static str,
    args: Vec<OkxWebsocketSubscribeArg<'a>>,
}

#[derive(Serialize)]
struct OkxWebsocketSubscribeArg<'a> {
    channel: &'a str,
    #[serde(rename = "instId", skip_serializing_if = "Option::is_none")]
    inst_id: Option<&'a str>,
    #[serde(rename = "instType", skip_serializing_if = "Option::is_none")]
    inst_type: Option<&'a str>,
}

#[derive(Deserialize)]
struct OkxWebsocketPublicMessage<'a> {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    arg: Option<OkxWebsocketPublicArg>,
    #[serde(default, borrow)]
    data: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct OkxWebsocketPublicArg {
    channel: String,
    #[serde(rename = "instId", default)]
    inst_id: Option<String>,
    #[serde(rename = "instType", default)]
    inst_type: Option<String>,
}

#[derive(Deserialize)]
struct OkxWebsocketTickerData {
    #[serde(rename = "instType")]
    inst_type: String,
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "bidPx")]
    bid_px: String,
    #[serde(rename = "askPx")]
    ask_px: String,
    last: String,
    #[serde(default)]
    ts: String,
}

#[derive(Debug, PartialEq)]
enum OkxMarketDataHint {
    Ticker(ValidatedOkxMarketTickerHint),
    Candle(ValidatedOkxMarketCandleHint),
    Instrument(Box<OkxInstrumentUpdateHint>),
    Level2(OkxLevel2Update),
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use futures_util::{SinkExt as _, StreamExt as _};
    use pretty_assertions::assert_eq;
    use rust_decimal::Decimal;
    use serde_json::json;
    use tokio::{
        net::{TcpListener, TcpStream},
        task::JoinHandle,
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    use crate::test_support::CapturedLogs;

    use super::*;

    const TEST_WEBSOCKET_TIMEOUT: Duration = Duration::from_secs(1);

    type TestWebSocket = tokio_tungstenite::WebSocketStream<TcpStream>;

    #[test]
    fn public_ticker_subscription_uses_okx_tickers_channel() -> Result<()> {
        let subscription =
            public_ticker_subscription(&["BTC-USDT".to_owned(), "ETH-USDT".to_owned()])?;
        let value: serde_json::Value = serde_json::from_str(&subscription)?;

        assert_eq!(
            value,
            json!({
                "op": "subscribe",
                "args": [
                    {"channel": "tickers", "instId": "BTC-USDT"},
                    {"channel": "tickers", "instId": "ETH-USDT"}
                ]
            })
        );
        Ok(())
    }

    #[test]
    fn public_market_subscription_uses_ticker_channel() -> Result<()> {
        let subscription = public_market_subscription(
            &["BTC-USDT".to_owned()],
            "SPOT",
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            &[],
            &[],
        )?;
        let value: serde_json::Value = serde_json::from_str(&subscription)?;

        assert_eq!(
            value,
            json!({
                "op": "subscribe",
                "args": [
                    {"channel": "tickers", "instId": "BTC-USDT"}
                ]
            })
        );
        Ok(())
    }

    #[test]
    fn public_market_subscription_uses_candle_channel() -> Result<()> {
        let subscription = public_market_subscription(
            &["BTC-USDT".to_owned()],
            "SPOT",
            /*subscribe_tickers*/ false,
            /*subscribe_instruments*/ false,
            &["candle1m".to_owned()],
            &[],
        )?;
        let value: serde_json::Value = serde_json::from_str(&subscription)?;

        assert_eq!(
            value,
            json!({
                "op": "subscribe",
                "args": [
                    {"channel": "candle1m", "instId": "BTC-USDT"}
                ]
            })
        );
        Ok(())
    }

    #[test]
    fn public_candle_channel_mapping_accepts_one_minute_bar() -> Result<()> {
        assert_eq!(okx_public_candle_channel_for_bar("1m")?, "candle1m");
        assert_eq!(okx_public_candle_bar_for_channel("candle1m")?, "1m");
        Ok(())
    }

    #[test]
    fn public_market_subscription_uses_instruments_channel() -> Result<()> {
        let subscription = public_market_subscription(
            &["BTC-USDT".to_owned()],
            "SPOT",
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ true,
            &["candle1m".to_owned()],
            &[],
        )?;
        let value: serde_json::Value = serde_json::from_str(&subscription)?;

        assert_eq!(
            value,
            json!({
                "op": "subscribe",
                "args": [
                    {"channel": "instruments", "instType": "SPOT"},
                    {"channel": "tickers", "instId": "BTC-USDT"},
                    {"channel": "candle1m", "instId": "BTC-USDT"}
                ]
            })
        );
        Ok(())
    }

    #[test]
    fn level2_public_subscription_uses_selected_instrument_books_channel() -> Result<()> {
        let subscription = public_market_subscription(
            &["ETH-USDT".to_owned()],
            "SPOT",
            false,
            false,
            &[],
            &["ETH-USDT".to_owned()],
        )?;
        let value: serde_json::Value = serde_json::from_str(&subscription)?;
        assert_eq!(
            value,
            json!({
                "op": "subscribe",
                "args": [{"channel": "books", "instId": "ETH-USDT"}]
            })
        );
        Ok(())
    }

    #[test]
    fn level2_snapshot_uses_configured_instrument_and_rejects_unconfigured() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        let btc_level2 = ["BTC-USDT".to_owned()];
        cache.configure_level2_instruments(&btc_level2)?;
        let latency = OkxLatencyMetrics::default();
        let received_at = Instant::now();
        let payload = include_str!("../../../../fixtures/public-market/books-snapshot.json");
        let applied = apply_public_market_data_message_inner(
            &cache,
            payload,
            received_at,
            &latency,
            &btc_level2,
        )?;
        assert_eq!(applied.count, 1);
        assert_eq!(applied.level2_features.len(), 1);
        assert_eq!(applied.level2_features[0].sequence_id, 100);
        assert_eq!(applied.level2_features[0].best_bid, Decimal::from(100_u64));

        let update = include_str!("../../../../fixtures/public-market/books-update.json");
        let applied = apply_public_market_data_message_inner(
            &cache,
            update,
            received_at + Duration::from_millis(100),
            &latency,
            &btc_level2,
        )?;
        assert_eq!(applied.level2_features[0].generation, 2);
        assert_eq!(applied.level2_features[0].best_bid, Decimal::new(1005, 1));
        assert_eq!(applied.level2_features[0].best_ask, Decimal::new(1015, 1));

        let eth_payload = payload.replace("BTC-USDT", "ETH-USDT");
        let error = match apply_public_market_data_message_inner(
            &cache,
            &eth_payload,
            received_at,
            &latency,
            &btc_level2,
        ) {
            Ok(_) => panic!("Level-2 instrument not requested by this stream must fail"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("was not requested by this stream")
        );

        let error = match apply_public_market_data_message_inner(
            &cache,
            &eth_payload,
            received_at,
            &latency,
            &["ETH-USDT".to_owned()],
        ) {
            Ok(_) => panic!("unconfigured Level-2 instrument must fail"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("unconfigured instrument ETH-USDT")
        );

        let eth_cache = OkxMarketDataCache::default();
        eth_cache.configure_level2_instruments(&["ETH-USDT".to_owned()])?;
        let applied = apply_public_market_data_message_inner(
            &eth_cache,
            &eth_payload,
            received_at,
            &latency,
            &["ETH-USDT".to_owned()],
        )?;
        assert_eq!(
            applied.level2_features[0].instrument_id.as_str(),
            "ETH-USDT"
        );
        Ok(())
    }

    #[test]
    fn level2_gap_reconnect_and_staleness_require_a_fresh_usable_snapshot() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        let level2_instrument_ids = ["BTC-USDT".to_owned()];
        cache.configure_level2_instruments(&level2_instrument_ids)?;
        let latency = OkxLatencyMetrics::default();
        let snapshot = include_str!("../../../../fixtures/public-market/books-snapshot.json");
        let received_at = Instant::now() - Duration::from_secs(1);

        let applied = apply_public_market_data_message_inner(
            &cache,
            snapshot,
            received_at,
            &latency,
            &level2_instrument_ids,
        )?;
        assert_eq!(applied.level2_features.len(), 1);
        assert!(
            cache
                .fresh_level2_features("BTC-USDT", Duration::from_millis(100))
                .is_none(),
            "stale Level-2 state must not be exposed as usable input"
        );

        let gap = include_str!("../../../../fixtures/public-market/books-update.json")
            .replace(r#""prevSeqId": 100"#, r#""prevSeqId": 999"#);
        let error = match apply_public_market_data_message_inner(
            &cache,
            &gap,
            Instant::now(),
            &latency,
            &level2_instrument_ids,
        ) {
            Ok(_) => panic!("sequence gap must invalidate the book"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("sequence gap"));
        assert!(
            cache
                .fresh_level2_features("BTC-USDT", Duration::MAX)
                .is_none(),
            "a sequence gap must clear usable Level-2 state"
        );
        assert_eq!(latency.snapshot().counters.sequence_gap_invalidations, 1);

        let applied = apply_public_market_data_message_inner(
            &cache,
            snapshot,
            Instant::now(),
            &latency,
            &level2_instrument_ids,
        )?;
        assert_eq!(applied.level2_features.len(), 1);
        assert!(
            cache
                .fresh_level2_features("BTC-USDT", Duration::MAX)
                .is_some()
        );

        cache.invalidate_level2_for_reconnect(&level2_instrument_ids);
        assert!(
            cache
                .fresh_level2_features("BTC-USDT", Duration::MAX)
                .is_none(),
            "a reconnect must clear usable Level-2 state until another snapshot"
        );
        Ok(())
    }

    #[test]
    fn level2_reconnect_invalidation_is_instrument_scoped() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        let level2_instrument_ids = ["BTC-USDT".to_owned(), "ETH-USDT".to_owned()];
        cache.configure_level2_instruments(&level2_instrument_ids)?;
        let latency = OkxLatencyMetrics::default();
        let btc_snapshot = include_str!("../../../../fixtures/public-market/books-snapshot.json");
        let eth_snapshot = btc_snapshot.replace("BTC-USDT", "ETH-USDT");

        apply_public_market_data_message_inner(
            &cache,
            btc_snapshot,
            Instant::now(),
            &latency,
            &level2_instrument_ids,
        )?;
        apply_public_market_data_message_inner(
            &cache,
            &eth_snapshot,
            Instant::now(),
            &latency,
            &level2_instrument_ids,
        )?;
        assert!(
            cache
                .fresh_level2_features("BTC-USDT", Duration::MAX)
                .is_some()
        );
        assert!(
            cache
                .fresh_level2_features("ETH-USDT", Duration::MAX)
                .is_some()
        );

        cache.invalidate_level2_for_reconnect(&["ETH-USDT".to_owned()]);

        assert!(
            cache
                .fresh_level2_features("BTC-USDT", Duration::MAX)
                .is_some(),
            "reconnecting one stream must not invalidate another instrument"
        );
        assert!(
            cache
                .fresh_level2_features("ETH-USDT", Duration::MAX)
                .is_none(),
            "the reconnecting stream must invalidate its own instrument"
        );
        Ok(())
    }

    #[test]
    fn confirmed_candle_runtime_event_is_emitted_only_once_per_bar() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        let latency = OkxLatencyMetrics::default();
        let payload = r#"{
            "arg":{"channel":"candle1m","instId":"BTC-USDT"},
            "data":[["1700000000000","100","101","99","100.5","1","1","100","1"]]
        }"#;
        let first =
            apply_public_market_data_message_inner(&cache, payload, Instant::now(), &latency, &[])?;
        let duplicate =
            apply_public_market_data_message_inner(&cache, payload, Instant::now(), &latency, &[])?;
        assert_eq!(first.public_events.len(), 1);
        assert!(duplicate.public_events.is_empty());
        Ok(())
    }

    #[test]
    fn untimestamped_instrument_update_requests_rest_revalidation_without_replacing_timestamped_hint()
    -> Result<()> {
        let cache = OkxMarketDataCache::default();
        let latency = OkxLatencyMetrics::default();
        let received_at = Instant::now();
        cache.update_instrument(instrument_hint(
            "BTC-USDT",
            "live",
            "0.1",
            Some(2_000),
            received_at,
        ))?;
        let payload = r#"{
            "arg":{"channel":"instruments","instType":"SPOT"},
            "data":[{
                "instType":"SPOT",
                "instId":"BTC-USDT",
                "groupId":"12",
                "state":"suspend",
                "tickSz":"0.2",
                "lotSz":"0.00000001",
                "minSz":"0.00001"
            }]
        }"#;

        let applied = apply_public_market_data_message_inner(
            &cache,
            payload,
            received_at + Duration::from_millis(1),
            &latency,
            &[],
        )?;

        assert_eq!(applied.count, 1);
        assert_eq!(
            applied.public_events,
            vec![OkxPublicRuntimeEvent {
                kind: OkxPublicRuntimeEventKind::InstrumentUpdated {
                    instrument_id: "BTC-USDT".to_owned(),
                },
                received_at: received_at + Duration::from_millis(1),
            }]
        );
        assert_eq!(
            cache.fresh_instrument("BTC-USDT", Duration::from_secs(1)),
            Some(instrument_update("BTC-USDT", "live", "0.1")),
            "a timestamp-free hint must trigger REST revalidation without replacing ordered cache state"
        );
        Ok(())
    }

    #[tokio::test]
    async fn public_market_stream_sends_text_ping_after_idle() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_idle_pong().await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let received = await_test_websocket_server(received).await?;

        assert!(outcome.subscribed());
        assert!(outcome.error().is_none());
        assert_eq!(received[1], OKX_WEBSOCKET_TEXT_PING);
        assert!(logs.contents().contains("ws_public_subscription_success"));
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_public_subscription_success_emits_ready_event() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_idle_pong().await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

        let outcome = run_public_market_stream_once_with_health(
            &config,
            OkxMarketDataCache::default(),
            Some(&health),
        )
        .await;
        let _ = await_test_websocket_server(received).await?;
        let events = recv_health_events(&mut health_events, 4).await?;

        assert!(outcome.subscribed());
        assert!(outcome.error().is_none());
        assert!(events.contains(&OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
            config.health_identity(),
        )));
        Ok(())
    }

    #[tokio::test]
    async fn public_timing_override_accepts_ack_after_unit_test_deadline() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_late_subscription_ack().await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let timing = OkxPublicMarketStreamTiming::new(
            TEST_WEBSOCKET_TIMEOUT,
            TEST_WEBSOCKET_TIMEOUT,
            TEST_WEBSOCKET_TIMEOUT,
        )?;
        let outcome = run_public_market_stream_once_with_health_and_timing(
            &config,
            OkxMarketDataCache::default(),
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
    async fn websocket_health_reconnect_event_includes_backoff() -> Result<()> {
        let policy =
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40))?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            "not-a-websocket-url".to_owned(),
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            policy,
        )?;
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

        let stream = OkxPublicMarketStream::spawn_with_health(
            config.clone(),
            OkxMarketDataCache::default(),
            Some(health),
        );
        let event = recv_health_event_kind(
            &mut health_events,
            OkxWebsocketHealthEventKind::ReconnectScheduled,
        )
        .await?;
        drop(stream);

        assert_eq!(event.stream(), config.health_identity());
        assert_eq!(event.reconnect_attempt(), Some(1));
        assert_eq!(event.reconnect_backoff(), Some(Duration::from_millis(10)));
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_channel_saturation_does_not_block_public_stream() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_idle_pong().await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(1);

        let outcome = time::timeout(
            Duration::from_millis(250),
            run_public_market_stream_once_with_health(
                &config,
                OkxMarketDataCache::default(),
                Some(&health),
            ),
        )
        .await
        .context("public WebSocket stream blocked behind saturated health channel")?;
        let _ = await_test_websocket_server(received).await?;
        let first_event = recv_health_event(&mut health_events).await?;

        assert!(outcome.subscribed());
        assert!(outcome.error().is_none());
        assert_eq!(
            first_event.kind(),
            OkxWebsocketHealthEventKind::ConnectAttempt
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_public_task_panic_emits_supervision_event() -> Result<()> {
        let stream_identity = OkxWebsocketStreamIdentity::new(
            OkxWebsocketStreamKind::Public,
            OkxWebsocketChannelClass::PublicMarketData,
            1,
        );
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

        let stream = OkxPublicMarketStream::spawn_test_task(stream_identity, Some(health), async {
            panic!("simulated public WebSocket task panic");
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
    async fn websocket_health_public_task_completion_emits_supervision_event() -> Result<()> {
        let stream_identity = OkxWebsocketStreamIdentity::new(
            OkxWebsocketStreamKind::Public,
            OkxWebsocketChannelClass::PublicMarketData,
            1,
        );
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

        let stream =
            OkxPublicMarketStream::spawn_test_task(stream_identity, Some(health), async {});
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
    async fn websocket_health_task_lifecycle_waits_behind_saturated_channel() -> Result<()> {
        let stream_identity = OkxWebsocketStreamIdentity::new(
            OkxWebsocketStreamKind::Public,
            OkxWebsocketChannelClass::PublicMarketData,
            1,
        );
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(1);
        health
            .report(OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::ConnectAttempt,
                stream_identity,
            ))
            .await;

        let stream =
            OkxPublicMarketStream::spawn_test_task(stream_identity, Some(health), async {});
        time::sleep(Duration::from_millis(50)).await;
        let queued_event = recv_health_event(&mut health_events).await?;
        let lifecycle_event = recv_health_event_kind(
            &mut health_events,
            OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly,
        )
        .await?;
        drop(stream);

        assert_eq!(
            queued_event,
            OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::ConnectAttempt,
                stream_identity
            )
        );
        assert_eq!(
            lifecycle_event,
            OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly,
                stream_identity
            )
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_public_drop_aborts_without_supervision_event() -> Result<()> {
        let stream_identity = OkxWebsocketStreamIdentity::new(
            OkxWebsocketStreamKind::Public,
            OkxWebsocketChannelClass::PublicMarketData,
            1,
        );
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

        let stream = OkxPublicMarketStream::spawn_test_task(
            stream_identity,
            Some(health),
            std::future::pending::<()>(),
        );
        drop(stream);
        let event = time::timeout(Duration::from_millis(50), health_events.recv()).await;

        assert!(
            !matches!(event, Ok(Some(_))),
            "intentional stream drop should not emit a fatal task lifecycle event: {event:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_task_lifecycle_logs_when_channel_closed() -> Result<()> {
        let stream_identity = OkxWebsocketStreamIdentity::new(
            OkxWebsocketStreamKind::Public,
            OkxWebsocketChannelClass::PublicMarketData,
            1,
        );
        let (health, health_events) = OkxWebsocketHealthReporter::channel(1);
        drop(health_events);
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let stream =
            OkxPublicMarketStream::spawn_test_task(stream_identity, Some(health), async {});
        for _ in 0..10 {
            let contents = logs.contents();
            if contents.contains("ws_health_critical_event_delivery_failed") {
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
        drop(stream);
        let logs = logs.contents();

        assert!(logs.contains("ws_health_critical_event_delivery_failed"));
        assert!(logs.contains("stream_task_exited_unexpectedly"));
        assert!(logs.contains("channel_closed"));
        Ok(())
    }

    #[tokio::test]
    async fn public_market_stream_rejects_subscription_error_before_success() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_subscription_error().await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("subscription error should force reconnect before success");

        assert!(!outcome.subscribed());
        assert!(
            error.to_string().contains("OKX public WebSocket error"),
            "subscription error should be reported clearly: {error}"
        );
        assert!(
            !logs.contents().contains("ws_public_subscription_success"),
            "public subscription success must not be logged before OKX ACK"
        );
        Ok(())
    }

    #[tokio::test]
    async fn public_market_stream_upgrade_notice_fails_before_subscription_readiness() -> Result<()>
    {
        let (url, received) = spawn_public_market_server_with_messages(vec![Message::Text(
            okx_websocket_upgrade_notice().into(),
        )])
        .await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("upgrade notice must fail the pre-ready public stream immediately");

        assert!(!outcome.subscribed());
        assert!(
            error.to_string().contains("service upgrade notice 64008"),
            "upgrade notice should retain only its sanitized code: {error}"
        );
        assert!(!error.to_string().contains("sensitive-connection-id"));
        assert!(!error.to_string().contains("sensitive maintenance detail"));
        Ok(())
    }

    #[tokio::test]
    async fn public_market_stream_upgrade_notice_fails_after_subscription_readiness() -> Result<()>
    {
        let (url, received) = spawn_public_market_server_with_messages(vec![
            public_ticker_subscribe_ack(),
            Message::Text(okx_websocket_upgrade_notice().into()),
        ])
        .await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(8);

        let outcome = run_public_market_stream_once_with_health(
            &config,
            OkxMarketDataCache::default(),
            Some(&health),
        )
        .await;
        let _ = await_test_websocket_server(received).await?;
        let events = recv_health_events(&mut health_events, 4).await?;
        let error = outcome
            .error()
            .expect("upgrade notice must terminate the ready public stream");

        assert!(outcome.subscribed());
        assert!(error.to_string().contains("service upgrade notice 64008"));
        assert!(events.contains(&OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
            config.health_identity(),
        )));
        assert!(events.contains(&OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::StreamFailedAfterSubscription,
            config.health_identity(),
        )));
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_missing_ack_times_out_before_ready() -> Result<()> {
        let (url, received) = spawn_public_market_server_without_subscription_ack().await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let received = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("missing subscription ACK should force reconnect before readiness");

        assert!(!outcome.subscribed());
        assert!(matches!(
            protocol_error(error),
            OkxWebsocketProtocolError::TimedOutWaitingForSubscriptionAck { context }
                if context == "public"
        ));
        assert!(
            received
                .iter()
                .any(|payload| payload == OKX_WEBSOCKET_TEXT_PING)
        );
        assert!(
            error.to_string().contains("subscription ACK"),
            "missing subscription ACK should time out before readiness: {error}"
        );
        assert!(
            !logs.contents().contains("ws_public_subscription_success"),
            "public stream must not report readiness without subscription ACK"
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_wrong_instrument_fails_before_ready() -> Result<()> {
        let (url, received) =
            spawn_public_market_server_with_messages(vec![public_ticker_subscribe_ack_for(
                "ETH-USDT",
            )])
            .await?;
        let policy =
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40))?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            policy,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("wrong instrument ACK should fail before readiness");

        assert!(!outcome.subscribed());
        assert_eq!(
            protocol_error(error),
            &OkxWebsocketProtocolError::UnexpectedSubscriptionAck {
                context: "public".to_owned(),
                ack: Box::new(OkxWebsocketSubscriptionAck {
                    channel: "tickers".to_owned(),
                    inst_id: Some("ETH-USDT".to_owned()),
                    inst_type: None,
                }),
            }
        );
        assert!(
            error.to_string().contains("unexpected subscription"),
            "wrong instrument ACK should be rejected: {error}"
        );
        assert_eq!(
            policy.backoff_after_stream_run(Duration::from_millis(10), &outcome),
            Duration::from_millis(20)
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_wrong_channel_fails_before_ready() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_messages(vec![public_subscribe_ack(
            "candle5m",
            Some("BTC-USDT"),
        )])
        .await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("wrong channel ACK should fail before readiness");

        assert!(!outcome.subscribed());
        assert_eq!(
            protocol_error(error),
            &OkxWebsocketProtocolError::UnexpectedSubscriptionAck {
                context: "public".to_owned(),
                ack: Box::new(OkxWebsocketSubscriptionAck {
                    channel: "candle5m".to_owned(),
                    inst_id: Some("BTC-USDT".to_owned()),
                    inst_type: None,
                }),
            }
        );
        assert!(
            error.to_string().contains("unexpected subscription"),
            "wrong channel ACK should be rejected: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_wrong_inst_type_fails_before_ready() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_messages(vec![Message::Text(
            r#"{"event":"subscribe","arg":{"channel":"instruments","instType":"MARGIN"}}"#.into(),
        )])
        .await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ false,
            /*subscribe_instruments*/ true,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("wrong instType ACK should fail before readiness");

        assert!(!outcome.subscribed());
        assert_eq!(
            protocol_error(error),
            &OkxWebsocketProtocolError::UnexpectedSubscriptionAck {
                context: "public".to_owned(),
                ack: Box::new(OkxWebsocketSubscriptionAck {
                    channel: "instruments".to_owned(),
                    inst_id: None,
                    inst_type: Some("MARGIN".to_owned()),
                }),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_duplicate_before_ready_fails() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_messages(vec![
            public_ticker_subscribe_ack(),
            public_ticker_subscribe_ack(),
        ])
        .await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            vec!["candle1m".to_owned()],
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("duplicate ACK should fail before all subscriptions are ready");

        assert!(!outcome.subscribed());
        assert!(matches!(
            protocol_error(error),
            OkxWebsocketProtocolError::UnexpectedSubscriptionAck { context, ack }
                if context == "public" && ack.channel == "tickers"
        ));
        assert!(
            error.to_string().contains("unexpected subscription"),
            "duplicate ACK before readiness should be rejected: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_data_before_ack_is_not_ready() -> Result<()> {
        let (url, received) =
            spawn_public_market_server_with_messages(vec![public_ticker_data_frame()]).await?;
        let cache = OkxMarketDataCache::default();
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, cache.clone()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("data before ACK should force reconnect before readiness");

        assert!(!outcome.subscribed());
        assert!(matches!(
            protocol_error(error),
            OkxWebsocketProtocolError::DataBeforeSubscriptionAck { context, ack }
                if context == "public" && ack.channel == "tickers" && ack.inst_id.as_deref() == Some("BTC-USDT")
        ));
        assert!(
            error.to_string().contains("subscription ACK"),
            "data before ACK should not make the stream ready: {error}"
        );
        assert_eq!(cache.fresh_ticker("BTC-USDT", Duration::from_secs(1)), None);
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_data_then_ack_still_fails_before_ready() -> Result<()>
    {
        let (url, received) = spawn_public_market_server_with_messages(vec![
            public_ticker_data_frame(),
            public_ticker_subscribe_ack(),
        ])
        .await?;
        let cache = OkxMarketDataCache::default();
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, cache.clone()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("data before ACK should fail even when ACK follows");

        assert!(!outcome.subscribed());
        assert!(matches!(
            protocol_error(error),
            OkxWebsocketProtocolError::DataBeforeSubscriptionAck { context, ack }
                if context == "public" && ack.channel == "tickers" && ack.inst_id.as_deref() == Some("BTC-USDT")
        ));
        assert!(
            error.to_string().contains("subscription ACK"),
            "data before ACK should not be ignored before readiness: {error}"
        );
        assert_eq!(cache.fresh_ticker("BTC-USDT", Duration::from_secs(1)), None);
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_allows_data_for_acknowledged_channel() -> Result<()>
    {
        let (url, received) = spawn_public_market_server_with_messages(vec![
            public_ticker_subscribe_ack(),
            public_ticker_data_frame(),
            public_instruments_subscribe_ack(),
        ])
        .await?;
        let cache = OkxMarketDataCache::default();
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ true,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, cache.clone()).await;
        let _ = await_test_websocket_server(received).await?;

        assert!(outcome.subscribed());
        assert!(outcome.error().is_none());
        assert_eq!(
            cache
                .fresh_ticker("BTC-USDT", Duration::from_secs(1))
                .expect("acknowledged ticker data should be cached before full readiness")
                .last,
            "100.15"
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_rejects_data_for_unacknowledged_channel()
    -> Result<()> {
        let (url, received) = spawn_public_market_server_with_messages(vec![
            public_ticker_subscribe_ack(),
            public_ticker_data_frame_for("ETH-USDT"),
            public_instruments_subscribe_ack(),
        ])
        .await?;
        let cache = OkxMarketDataCache::default();
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ true,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, cache.clone()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("unacknowledged ticker data should fail before readiness");

        assert!(!outcome.subscribed());
        assert!(matches!(
            protocol_error(error),
            OkxWebsocketProtocolError::DataBeforeSubscriptionAck { context, ack }
                if context == "public" && ack.channel == "tickers" && ack.inst_id.as_deref() == Some("ETH-USDT")
        ));
        assert!(
            error.to_string().contains("subscription ACK"),
            "unacknowledged ticker data should be rejected: {error}"
        );
        assert_eq!(cache.fresh_ticker("ETH-USDT", Duration::from_secs(1)), None);
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_late_ack_after_timeout_is_not_ready() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_late_subscription_ack().await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let received = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("late ACK after timeout should not mark the stream ready");

        assert!(!outcome.subscribed());
        assert!(
            received
                .iter()
                .any(|payload| payload == OKX_WEBSOCKET_TEXT_PING)
        );
        assert!(
            error.to_string().contains("subscription ACK"),
            "late ACK should not satisfy the earlier subscription timeout: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_subscription_ack_public_close_before_ack_is_typed() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_messages(Vec::new()).await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let _ = await_test_websocket_server(received).await?;
        let error = outcome
            .error()
            .expect("close before ACK should fail before readiness");

        assert!(!outcome.subscribed());
        assert!(matches!(
            protocol_error(error),
            OkxWebsocketProtocolError::ClosedBeforeSubscriptionAck { context }
                if context == "public"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn public_market_stream_reconnects_after_missing_idle_pong() -> Result<()> {
        let (url, received) = spawn_public_market_server_without_idle_pong().await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let error = outcome
            .error()
            .expect("missing idle pong should force reconnect");
        let received = await_test_websocket_server(received).await?;

        assert!(outcome.subscribed());
        assert_eq!(received[1], OKX_WEBSOCKET_TEXT_PING);
        assert!(
            error.to_string().contains("idle pong"),
            "missing idle pong should be reported clearly: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn public_market_stream_replies_to_protocol_ping_frames() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_protocol_ping().await?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let stream = tokio::spawn(async move {
            run_public_market_stream_once(&config, OkxMarketDataCache::default()).await
        });
        let received = await_test_websocket_server(received).await?;
        stream.abort();
        if let Err(error) = stream.await {
            assert!(
                error.is_cancelled(),
                "public stream task should only be cancelled after protocol pong: {error}"
            );
        }

        assert_eq!(received[1], "protocol-pong");
        Ok(())
    }

    #[tokio::test]
    async fn public_market_stream_processes_text_data_after_idle_ping() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_ticker_after_idle_ping().await?;
        let cache = OkxMarketDataCache::default();
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(1))?,
        )?;

        let outcome = run_public_market_stream_once(&config, cache.clone()).await;
        let received = await_test_websocket_server(received).await?;

        assert!(outcome.subscribed());
        assert!(outcome.error().is_none());
        assert_eq!(received[1], OKX_WEBSOCKET_TEXT_PING);
        assert_eq!(
            cache
                .fresh_ticker("BTC-USDT", Duration::from_secs(1))
                .expect("ticker data frame should be processed after idle ping")
                .last,
            "100.15"
        );
        Ok(())
    }

    #[tokio::test]
    async fn public_stream_failure_before_subscribe_increases_backoff() -> Result<()> {
        let policy =
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40))?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            "not-a-websocket-url".to_owned(),
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            policy,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;

        assert!(!outcome.subscribed());
        assert!(outcome.error().is_some());
        assert_eq!(
            policy.backoff_after_stream_run(Duration::from_millis(10), &outcome),
            Duration::from_millis(20)
        );
        Ok(())
    }

    #[tokio::test]
    async fn public_stream_after_successful_subscription_resets_backoff() -> Result<()> {
        let (url, received) = spawn_public_market_server_with_idle_pong().await?;
        let policy =
            OkxWebsocketReconnectPolicy::new(Duration::from_millis(10), Duration::from_millis(40))?;
        let config = OkxPublicMarketStreamConfig::with_reconnect_policy(
            url,
            vec!["BTC-USDT".to_owned()],
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ false,
            Vec::new(),
            policy,
        )?;

        let outcome = run_public_market_stream_once(&config, OkxMarketDataCache::default()).await;
        let _ = await_test_websocket_server(received).await?;

        assert!(outcome.subscribed());
        assert!(outcome.error().is_none());
        assert_eq!(
            policy.backoff_after_stream_run(Duration::from_millis(40), &outcome),
            Duration::from_millis(10)
        );
        Ok(())
    }

    #[test]
    fn parses_public_ticker_updates_into_hints() -> Result<()> {
        let received_at = Instant::now();
        let hints = parse_public_market_data_message(
            r#"{
                "arg": {"channel": "tickers", "instId": "BTC-USDT"},
                "data": [{
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "bidPx": "100.1",
                    "askPx": "100.2",
                    "last": "100.15",
                    "ts": "1710000000123"
                }]
            }"#,
            received_at,
        )?;

        assert_eq!(
            hints,
            vec![OkxMarketDataHint::Ticker(ValidatedOkxMarketTickerHint(
                OkxMarketTickerHint {
                    ticker: OkxTicker {
                        inst_type: "SPOT".to_owned(),
                        inst_id: "BTC-USDT".to_owned(),
                        bid_px: "100.1".to_owned(),
                        ask_px: "100.2".to_owned(),
                        last: "100.15".to_owned(),
                    },
                    source_ts_ms: Some(1_710_000_000_123),
                    received_at,
                },
            ))]
        );
        Ok(())
    }

    #[test]
    fn rejects_public_ticker_updates_with_non_spot_inst_type() {
        let error = parse_public_market_data_message(
            r#"{
                "arg": {"channel": "tickers", "instId": "BTC-USDT"},
                "data": [{
                    "instType": "SWAP",
                    "instId": "BTC-USDT",
                    "bidPx": "100.1",
                    "askPx": "100.2",
                    "last": "100.15",
                    "ts": "1710000000123"
                }]
            }"#,
            Instant::now(),
        )
        .expect_err("non-SPOT ticker update should fail");

        assert!(
            error.to_string().contains("non-SPOT"),
            "non-SPOT ticker update should report the instrument type: {error}"
        );
    }

    #[test]
    fn parses_public_candle_updates_into_hints() -> Result<()> {
        let received_at = Instant::now();
        let hints = parse_public_market_data_message(
            r#"{
                "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
                "data": [["1710000000000","100","105","95","101","1","100","100","0"]]
            }"#,
            received_at,
        )?;

        assert_eq!(
            hints,
            vec![OkxMarketDataHint::Candle(ValidatedOkxMarketCandleHint(
                OkxMarketCandleHint {
                    inst_id: "BTC-USDT".to_owned(),
                    channel: "candle1m".to_owned(),
                    bar: MarketBar {
                        ts_ms: 1_710_000_000_000,
                        open: 100.0,
                        high: 105.0,
                        low: 95.0,
                        close: 101.0,
                        confirm: false,
                    },
                    source_ts_ms: Some(1_710_000_000_000),
                    received_at,
                },
            ))]
        );
        Ok(())
    }

    #[test]
    fn rejects_public_candle_updates_with_invalid_market_bar_shape() {
        let error = parse_public_market_data_message(
            r#"{
                "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
                "data": [["1710000000000","100","100","95","101","1","100","100","1"]]
            }"#,
            Instant::now(),
        )
        .expect_err("invalid WebSocket candle shape should fail");

        assert!(
            error
                .to_string()
                .contains("OKX WebSocket candle high must be at least close"),
            "invalid WebSocket candle should use shared candle validation: {error}"
        );
    }

    #[test]
    fn parses_public_instrument_updates_into_hints() -> Result<()> {
        let received_at = Instant::now();
        let hints = parse_public_market_data_message(
            r#"{
                "arg": {"channel": "instruments", "instType": "SPOT"},
                "data": [{
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "groupId": "12",
                    "state": "live",
                    "tickSz": "0.1",
                    "lotSz": "0.00000001",
                    "minSz": "0.00001",
                    "contTdSwTime": "1704876947000",
                    "upcChg": [{
                        "param": "tickSz",
                        "newValue": "0.0001",
                        "effTime": "1704876948000"
                    }]
                }]
            }"#,
            received_at,
        )?;

        assert_eq!(
            hints,
            vec![OkxMarketDataHint::Instrument(Box::new(
                OkxInstrumentUpdateHint {
                    instrument: OkxWebsocketInstrumentUpdate {
                        inst_type: "SPOT".to_owned(),
                        inst_id: "BTC-USDT".to_owned(),
                        group_id: "12".to_owned(),
                        state: "live".to_owned(),
                        tick_size: "0.1".to_owned(),
                        lot_size: "0.00000001".to_owned(),
                        min_size: "0.00001".to_owned(),
                        max_limit_size: String::new(),
                        max_limit_amount: String::new(),
                        max_market_size: String::new(),
                        max_market_amount: String::new(),
                        max_trigger_size: String::new(),
                        continuous_trading_switch_time: "1704876947000".to_owned(),
                        upcoming_changes: vec![OkxWebsocketInstrumentParameterChange {
                            effective_time: "1704876948000".to_owned(),
                        }],
                    },
                    source_ts_ms: Some(1_704_876_948_000),
                    received_at,
                },
            ))]
        );
        Ok(())
    }

    #[test]
    fn rejects_public_instrument_updates_without_fee_group_id() {
        let error = parse_public_market_data_message(
            r#"{
                "arg": {"channel": "instruments", "instType": "SPOT"},
                "data": [{
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "state": "live",
                    "tickSz": "0.1",
                    "lotSz": "0.00000001",
                    "minSz": "0.00001"
                }]
            }"#,
            Instant::now(),
        )
        .expect_err("instrument update without groupId should fail closed");

        assert!(
            format!("{error:#}").contains("groupId"),
            "missing fee group should identify groupId: {error:#}"
        );
    }

    #[test]
    fn rejects_public_instrument_updates_with_non_spot_inst_type() {
        let error = parse_public_market_data_message(
            r#"{
                "arg": {"channel": "instruments", "instType": "SWAP"},
                "data": [{
                    "instType": "SWAP",
                    "instId": "BTC-USDT",
                    "groupId": "4",
                    "state": "live",
                    "tickSz": "0.1",
                    "lotSz": "0.00000001",
                    "minSz": "0.00001"
                }]
            }"#,
            Instant::now(),
        )
        .expect_err("non-SPOT instrument update should fail");

        assert!(
            error.to_string().contains("non-SPOT"),
            "non-SPOT instrument update should report the instrument type: {error}"
        );
    }

    #[test]
    fn rejects_public_instrument_updates_with_invalid_trade_parameters() {
        let error = parse_public_market_data_message(
            r#"{
                "arg": {"channel": "instruments", "instType": "SPOT"},
                "data": [{
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "groupId": "12",
                    "state": "live",
                    "tickSz": "0",
                    "lotSz": "0.00000001",
                    "minSz": "0.00001"
                }]
            }"#,
            Instant::now(),
        )
        .expect_err("zero tick size should fail closed");

        assert!(
            error.to_string().contains("tickSz"),
            "invalid instrument parameter should report the unsafe field: {error}"
        );
    }

    #[test]
    fn ignores_public_subscribe_acknowledgements() -> Result<()> {
        let hints = parse_public_market_data_message(
            r#"{"event":"subscribe","arg":{"channel":"tickers","instId":"BTC-USDT"}}"#,
            Instant::now(),
        )?;

        assert_eq!(hints, []);
        Ok(())
    }

    #[test]
    fn rejects_public_websocket_error_frames() {
        let error = parse_public_market_data_message(
            r#"{"event":"error","code":"60012","msg":"Invalid request"}"#,
            Instant::now(),
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("OKX public WebSocket error"),
            "OKX public WebSocket error should fail: {error}"
        );
    }

    #[test]
    fn market_data_cache_returns_only_fresh_tickers() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("BTC-USDT", "100"),
            source_ts_ms: Some(1_710_000_000_123),
            received_at: Instant::now() - Duration::from_secs(5),
        })?;
        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("ETH-USDT", "200"),
            source_ts_ms: Some(1_710_000_000_124),
            received_at: Instant::now(),
        })?;

        assert_eq!(cache.fresh_ticker("BTC-USDT", Duration::from_secs(1)), None);
        assert_eq!(
            cache.fresh_ticker("ETH-USDT", Duration::from_secs(1)),
            Some(ticker("ETH-USDT", "200"))
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_caps_retained_ticker_candle_and_instrument_hints() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        let newest_received_at = Instant::now();

        for index in 0..=OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND {
            let received_at = newest_received_at
                - Duration::from_millis((OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND - index) as u64);
            let inst_id = format!("COIN{index}-USDT");
            let ts_ms = 1_710_000_000_000_i64 + index as i64;

            cache.update_ticker(OkxMarketTickerHint {
                ticker: ticker(&inst_id, "100"),
                source_ts_ms: Some(ts_ms),
                received_at,
            })?;
            cache.update_candle(candle_hint(&inst_id, ts_ms, 100.0, received_at))?;
            cache.update_instrument(instrument_hint(
                &inst_id,
                "live",
                "0.1",
                Some(ts_ms),
                received_at,
            ))?;
        }

        {
            let state = lock(&cache.inner);
            assert_eq!(
                state.tickers_by_inst_id.len(),
                OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND
            );
            assert_eq!(
                state.candles_by_inst_id_and_channel.len(),
                OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND
            );
            assert_eq!(
                state.instruments_by_inst_id.len(),
                OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND
            );
        }
        assert_eq!(
            cache.fresh_ticker("COIN0-USDT", Duration::from_secs(60)),
            None
        );
        assert_eq!(
            cache.fresh_candles("COIN0-USDT", "candle1m", Duration::from_secs(60)),
            []
        );
        assert_eq!(
            cache.fresh_instrument("COIN0-USDT", Duration::from_secs(60)),
            None
        );

        let newest_inst_id = format!("COIN{OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND}-USDT");
        assert_eq!(
            cache.fresh_ticker(&newest_inst_id, Duration::from_secs(60)),
            Some(ticker(&newest_inst_id, "100"))
        );
        assert_eq!(
            cache
                .fresh_candles(&newest_inst_id, "candle1m", Duration::from_secs(60))
                .len(),
            1
        );
        assert_eq!(
            cache.fresh_instrument(&newest_inst_id, Duration::from_secs(60)),
            Some(instrument_update(&newest_inst_id, "live", "0.1"))
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_preserves_protected_instrument_hints_when_capped() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        let protected_inst_id = "BTC-USDT".to_owned();
        let newest_received_at = Instant::now();
        cache.protect_instruments(std::slice::from_ref(&protected_inst_id));
        cache.update_instrument(instrument_hint(
            &protected_inst_id,
            "suspend",
            "0.1",
            Some(1_710_000_000_000),
            newest_received_at - Duration::from_secs(30),
        ))?;

        for index in 0..=OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND {
            let received_at = newest_received_at
                - Duration::from_millis((OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND - index) as u64);
            let inst_id = format!("COIN{index}-USDT");
            let ts_ms = 1_710_000_000_001_i64 + index as i64;
            cache.update_instrument(instrument_hint(
                &inst_id,
                "live",
                "0.1",
                Some(ts_ms),
                received_at,
            ))?;
        }

        assert_eq!(
            lock(&cache.inner).instruments_by_inst_id.len(),
            OKX_MARKET_DATA_CACHE_MAX_HINTS_PER_KIND
        );
        assert_eq!(
            cache.fresh_instrument(&protected_inst_id, Duration::from_secs(60)),
            Some(instrument_update(&protected_inst_id, "suspend", "0.1"))
        );
        assert_eq!(
            cache.fresh_instrument("COIN0-USDT", Duration::from_secs(60)),
            None
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_discards_poisoned_hints_and_recovers() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("BTC-USDT", "100"),
            source_ts_ms: Some(1_710_000_000_123),
            received_at: Instant::now(),
        })?;

        let poison_result = std::panic::catch_unwind(|| {
            let _guard = cache.inner.lock().expect("test cache lock should work");
            panic!("poison market data cache");
        });
        assert!(poison_result.is_err());
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        assert_eq!(cache.fresh_ticker("BTC-USDT", Duration::from_secs(1)), None);
        assert!(logs.contents().contains("ws_public_hint_cache_poisoned"));

        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("BTC-USDT", "101"),
            source_ts_ms: Some(1_710_000_000_124),
            received_at: Instant::now(),
        })?;

        assert_eq!(
            cache.fresh_ticker("BTC-USDT", Duration::from_secs(1)),
            Some(ticker("BTC-USDT", "101"))
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_ignores_older_ticker_hints() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("BTC-USDT", "100"),
            source_ts_ms: Some(2_000),
            received_at: Instant::now(),
        })?;
        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("BTC-USDT", "90"),
            source_ts_ms: Some(1_000),
            received_at: Instant::now(),
        })?;

        assert_eq!(
            cache.fresh_ticker("BTC-USDT", Duration::from_secs(1)),
            Some(ticker("BTC-USDT", "100"))
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_ignores_untimestamped_ticker_hints() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("BTC-USDT", "100"),
            source_ts_ms: Some(2_000),
            received_at: Instant::now(),
        })?;
        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("BTC-USDT", "90"),
            source_ts_ms: None,
            received_at: Instant::now(),
        })?;

        assert_eq!(
            cache.fresh_ticker("BTC-USDT", Duration::from_secs(1)),
            Some(ticker("BTC-USDT", "100"))
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_rejects_ticker_hints_without_spot_inst_type() {
        let cache = OkxMarketDataCache::default();
        let error = cache
            .update_ticker(OkxMarketTickerHint {
                ticker: OkxTicker {
                    inst_type: String::new(),
                    inst_id: "BTC-USDT".to_owned(),
                    bid_px: "100".to_owned(),
                    ask_px: "100".to_owned(),
                    last: "100".to_owned(),
                },
                source_ts_ms: Some(2_000),
                received_at: Instant::now(),
            })
            .expect_err("ticker hints must expose explicit SPOT instType");

        assert!(
            error.to_string().contains("non-SPOT"),
            "missing ticker instType should fail closed: {error}"
        );
        assert_eq!(cache.fresh_ticker("BTC-USDT", Duration::from_secs(1)), None);
    }

    #[test]
    fn market_data_cache_replaces_same_timestamp_ticker_hints() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("BTC-USDT", "100"),
            source_ts_ms: Some(2_000),
            received_at: Instant::now(),
        })?;
        cache.update_ticker(OkxMarketTickerHint {
            ticker: ticker("BTC-USDT", "101"),
            source_ts_ms: Some(2_000),
            received_at: Instant::now(),
        })?;

        assert_eq!(
            cache.fresh_ticker("BTC-USDT", Duration::from_secs(1)),
            Some(ticker("BTC-USDT", "101"))
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_dedupes_and_orders_candle_hints() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_candle(candle_hint("BTC-USDT", 2_000, 101.0, Instant::now()))?;
        cache.update_candle(candle_hint("BTC-USDT", 1_000, 100.0, Instant::now()))?;
        cache.update_candle(candle_hint("BTC-USDT", 2_000, 99.0, Instant::now()))?;

        assert_eq!(
            cache.fresh_candles("BTC-USDT", "candle1m", Duration::from_secs(1)),
            vec![
                MarketBar {
                    ts_ms: 1_000,
                    open: 100.0,
                    high: 105.0,
                    low: 95.0,
                    close: 100.0,
                    confirm: true,
                },
                MarketBar {
                    ts_ms: 2_000,
                    open: 99.0,
                    high: 104.0,
                    low: 94.0,
                    close: 99.0,
                    confirm: true,
                }
            ]
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_keys_candle_hints_by_channel() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_candle(candle_hint("BTC-USDT", 1_000, 100.0, Instant::now()))?;
        let mut five_minute_hint = candle_hint("BTC-USDT", 1_000, 200.0, Instant::now());
        five_minute_hint.channel = "candle5m".to_owned();
        cache.update_candle(five_minute_hint)?;

        assert_eq!(
            cache.fresh_candles("BTC-USDT", "candle1m", Duration::from_secs(1)),
            vec![MarketBar {
                ts_ms: 1_000,
                open: 100.0,
                high: 105.0,
                low: 95.0,
                close: 100.0,
                confirm: true,
            }]
        );
        assert_eq!(
            cache.fresh_candles("BTC-USDT", "candle5m", Duration::from_secs(1)),
            vec![MarketBar {
                ts_ms: 1_000,
                open: 200.0,
                high: 205.0,
                low: 195.0,
                close: 200.0,
                confirm: true,
            }]
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_replaces_same_timestamp_candle_confirmation() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        let mut unconfirmed = candle_hint("BTC-USDT", 2_000, 100.0, Instant::now());
        unconfirmed.bar.confirm = false;
        cache.update_candle(unconfirmed)?;
        cache.update_candle(candle_hint("BTC-USDT", 2_000, 101.0, Instant::now()))?;

        assert_eq!(
            cache.fresh_candles("BTC-USDT", "candle1m", Duration::from_secs(1)),
            vec![MarketBar {
                ts_ms: 2_000,
                open: 101.0,
                high: 106.0,
                low: 96.0,
                close: 101.0,
                confirm: true,
            }]
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_returns_only_fresh_instrument_hints() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_instrument(instrument_hint(
            "BTC-USDT",
            "live",
            "0.1",
            Some(2_000),
            Instant::now() - Duration::from_secs(5),
        ))?;
        cache.update_instrument(instrument_hint(
            "ETH-USDT",
            "live",
            "0.01",
            Some(2_001),
            Instant::now(),
        ))?;

        assert_eq!(
            cache.fresh_instrument("BTC-USDT", Duration::from_secs(1)),
            None
        );
        assert_eq!(
            cache.fresh_instrument("ETH-USDT", Duration::from_secs(1)),
            Some(instrument_update("ETH-USDT", "live", "0.01"))
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_ignores_older_instrument_hints() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_instrument(instrument_hint(
            "BTC-USDT",
            "suspend",
            "0.1",
            Some(2_000),
            Instant::now(),
        ))?;
        cache.update_instrument(instrument_hint(
            "BTC-USDT",
            "live",
            "0.01",
            Some(1_000),
            Instant::now(),
        ))?;

        assert_eq!(
            cache.fresh_instrument("BTC-USDT", Duration::from_secs(1)),
            Some(instrument_update("BTC-USDT", "suspend", "0.1"))
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_replaces_same_timestamp_instrument_hints() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_instrument(instrument_hint(
            "BTC-USDT",
            "live",
            "0.1",
            Some(2_000),
            Instant::now(),
        ))?;
        cache.update_instrument(instrument_hint(
            "BTC-USDT",
            "suspend",
            "0.2",
            Some(2_000),
            Instant::now(),
        ))?;

        assert_eq!(
            cache.fresh_instrument("BTC-USDT", Duration::from_secs(1)),
            Some(instrument_update("BTC-USDT", "suspend", "0.2"))
        );
        Ok(())
    }

    #[test]
    fn market_data_cache_does_not_store_untimestamped_instrument_hints() -> Result<()> {
        let cache = OkxMarketDataCache::default();
        cache.update_instrument(instrument_hint(
            "BTC-USDT",
            "suspend",
            "0.1",
            Some(2_000),
            Instant::now(),
        ))?;
        cache.update_instrument(instrument_hint(
            "BTC-USDT",
            "live",
            "0.01",
            None,
            Instant::now(),
        ))?;

        assert_eq!(
            cache.fresh_instrument("BTC-USDT", Duration::from_secs(1)),
            Some(instrument_update("BTC-USDT", "suspend", "0.1"))
        );
        Ok(())
    }

    #[test]
    fn websocket_reconnect_policy_doubles_until_max_backoff() -> Result<()> {
        let policy = OkxWebsocketReconnectPolicy::new(
            Duration::from_millis(250),
            Duration::from_millis(1_000),
        )?;

        assert_eq!(policy.initial_backoff(), Duration::from_millis(250));
        assert_eq!(
            policy.next_backoff(Duration::from_millis(250)),
            Duration::from_millis(500)
        );
        assert_eq!(
            policy.next_backoff(Duration::from_millis(500)),
            Duration::from_millis(1_000)
        );
        assert_eq!(
            policy.next_backoff(Duration::from_millis(1_000)),
            Duration::from_millis(1_000)
        );
        Ok(())
    }

    fn ticker(inst_id: &str, price: &str) -> OkxTicker {
        OkxTicker {
            inst_type: "SPOT".to_owned(),
            inst_id: inst_id.to_owned(),
            bid_px: price.to_owned(),
            ask_px: price.to_owned(),
            last: price.to_owned(),
        }
    }

    fn candle_hint(
        inst_id: &str,
        ts_ms: i64,
        close: f64,
        received_at: Instant,
    ) -> OkxMarketCandleHint {
        OkxMarketCandleHint {
            inst_id: inst_id.to_owned(),
            channel: "candle1m".to_owned(),
            bar: MarketBar {
                ts_ms,
                open: close,
                high: close + 5.0,
                low: close - 5.0,
                close,
                confirm: true,
            },
            source_ts_ms: Some(ts_ms),
            received_at,
        }
    }

    fn instrument_hint(
        inst_id: &str,
        state: &str,
        tick_size: &str,
        source_ts_ms: Option<i64>,
        received_at: Instant,
    ) -> OkxInstrumentUpdateHint {
        OkxInstrumentUpdateHint {
            instrument: instrument_update(inst_id, state, tick_size),
            source_ts_ms,
            received_at,
        }
    }

    fn instrument_update(
        inst_id: &str,
        state: &str,
        tick_size: &str,
    ) -> OkxWebsocketInstrumentUpdate {
        OkxWebsocketInstrumentUpdate {
            inst_type: "SPOT".to_owned(),
            inst_id: inst_id.to_owned(),
            group_id: "12".to_owned(),
            state: state.to_owned(),
            tick_size: tick_size.to_owned(),
            lot_size: "0.00000001".to_owned(),
            min_size: "0.00001".to_owned(),
            max_limit_size: "999".to_owned(),
            max_limit_amount: "100000".to_owned(),
            max_market_size: String::new(),
            max_market_amount: "100000".to_owned(),
            max_trigger_size: "999".to_owned(),
            continuous_trading_switch_time: String::new(),
            upcoming_changes: Vec::new(),
        }
    }

    async fn spawn_public_market_server_with_idle_pong()
    -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let mut websocket = accept_test_websocket(listener).await?;
            let mut received = Vec::new();
            received.push(next_text(&mut websocket).await?);
            websocket.send(public_ticker_subscribe_ack()).await?;
            received.push(next_text(&mut websocket).await?);
            websocket
                .send(Message::Text(OKX_WEBSOCKET_TEXT_PONG.into()))
                .await?;
            websocket.close(None).await?;
            Ok(received)
        });
        Ok((url, handle))
    }

    async fn spawn_public_market_server_with_messages(
        messages: Vec<Message>,
    ) -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let mut websocket = accept_test_websocket(listener).await?;
            let received = vec![next_text(&mut websocket).await?];
            for message in messages {
                websocket.send(message).await?;
            }
            websocket.close(None).await?;
            Ok(received)
        });
        Ok((url, handle))
    }

    async fn spawn_public_market_server_without_subscription_ack()
    -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let mut websocket = accept_test_websocket(listener).await?;
            let mut received = Vec::new();
            received.push(next_text(&mut websocket).await?);
            reply_to_text_pings_until_close(
                &mut websocket,
                &mut received,
                Duration::from_millis(150),
            )
            .await?;
            Ok(received)
        });
        Ok((url, handle))
    }

    async fn spawn_public_market_server_with_late_subscription_ack()
    -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let mut websocket = accept_test_websocket(listener).await?;
            let mut received = Vec::new();
            received.push(next_text(&mut websocket).await?);
            reply_to_text_pings_until_close(
                &mut websocket,
                &mut received,
                Duration::from_millis(150),
            )
            .await?;
            let _ = websocket.send(public_ticker_subscribe_ack()).await;
            let _ = websocket.close(None).await;
            Ok(received)
        });
        Ok((url, handle))
    }

    async fn spawn_public_market_server_without_idle_pong()
    -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let mut websocket = accept_test_websocket(listener).await?;
            let mut received = Vec::new();
            received.push(next_text(&mut websocket).await?);
            websocket.send(public_ticker_subscribe_ack()).await?;
            received.push(next_text(&mut websocket).await?);
            time::sleep(Duration::from_millis(150)).await;
            Ok(received)
        });
        Ok((url, handle))
    }

    async fn spawn_public_market_server_with_ticker_after_idle_ping()
    -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let mut websocket = accept_test_websocket(listener).await?;
            let mut received = Vec::new();
            received.push(next_text(&mut websocket).await?);
            websocket.send(public_ticker_subscribe_ack()).await?;
            received.push(next_text(&mut websocket).await?);
            websocket
                .send(Message::Text(
                    r#"{
                        "arg": {"channel": "tickers", "instId": "BTC-USDT"},
                        "data": [{
                            "instType": "SPOT",
                            "instId": "BTC-USDT",
                            "bidPx": "100.1",
                            "askPx": "100.2",
                            "last": "100.15",
                            "ts": "1710000000123"
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

    async fn spawn_public_market_server_with_protocol_ping()
    -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let mut websocket = accept_test_websocket(listener).await?;
            let mut received = Vec::new();
            received.push(next_text(&mut websocket).await?);
            websocket.send(public_ticker_subscribe_ack()).await?;
            websocket.send(Message::Ping(vec![1, 2, 3].into())).await?;
            loop {
                let message = next_test_websocket_message(&mut websocket).await?;
                if matches!(message, Message::Pong(_)) {
                    received.push("protocol-pong".to_owned());
                    break;
                }
            }
            Ok(received)
        });
        Ok((url, handle))
    }

    async fn spawn_public_market_server_with_subscription_error()
    -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let mut websocket = accept_test_websocket(listener).await?;
            let received = vec![next_text(&mut websocket).await?];
            websocket
                .send(Message::Text(
                    r#"{"event":"error","code":"60012","msg":"Invalid request"}"#.into(),
                ))
                .await?;
            websocket.close(None).await?;
            Ok(received)
        });
        Ok((url, handle))
    }

    fn public_ticker_subscribe_ack() -> Message {
        public_ticker_subscribe_ack_for("BTC-USDT")
    }

    fn okx_websocket_upgrade_notice() -> &'static str {
        r#"{"event":"notice","code":"64008","msg":"sensitive maintenance detail","connId":"sensitive-connection-id"}"#
    }

    fn public_ticker_subscribe_ack_for(inst_id: &str) -> Message {
        public_subscribe_ack("tickers", Some(inst_id))
    }

    fn public_instruments_subscribe_ack() -> Message {
        Message::Text(
            r#"{"event":"subscribe","arg":{"channel":"instruments","instType":"SPOT"}}"#.into(),
        )
    }

    fn public_subscribe_ack(channel: &str, inst_id: Option<&str>) -> Message {
        let arg = match inst_id {
            Some(inst_id) => format!(r#"{{"channel":"{channel}","instId":"{inst_id}"}}"#),
            None => format!(r#"{{"channel":"{channel}"}}"#),
        };
        Message::Text(format!(r#"{{"event":"subscribe","arg":{arg}}}"#).into())
    }

    fn public_ticker_data_frame() -> Message {
        public_ticker_data_frame_for("BTC-USDT")
    }

    fn public_ticker_data_frame_for(inst_id: &str) -> Message {
        Message::Text(
            format!(
                r#"{{
                "arg": {{"channel": "tickers", "instId": "{inst_id}"}},
                "data": [{{
                    "instType": "SPOT",
                    "instId": "{inst_id}",
                    "bidPx": "100.1",
                    "askPx": "100.2",
                    "last": "100.15",
                    "ts": "1710000000123"
                }}]
            }}"#
            )
            .into(),
        )
    }

    async fn accept_test_websocket(listener: TcpListener) -> Result<TestWebSocket> {
        let (stream, _) = time::timeout(TEST_WEBSOCKET_TIMEOUT, listener.accept())
            .await
            .context("timed out accepting test WebSocket TCP connection")??;
        time::timeout(TEST_WEBSOCKET_TIMEOUT, accept_async(stream))
            .await
            .context("timed out accepting test WebSocket handshake")?
            .context("failed accepting test WebSocket handshake")
    }

    async fn await_test_websocket_server(
        handle: JoinHandle<Result<Vec<String>>>,
    ) -> Result<Vec<String>> {
        time::timeout(TEST_WEBSOCKET_TIMEOUT, handle)
            .await
            .context("timed out waiting for test WebSocket server task")?
            .context("test WebSocket server task panicked")?
    }

    async fn next_test_websocket_message(websocket: &mut TestWebSocket) -> Result<Message> {
        time::timeout(TEST_WEBSOCKET_TIMEOUT, websocket.next())
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
        time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .context("timed out waiting for OKX WebSocket health event")?
            .context("OKX WebSocket health channel closed")
    }

    async fn reply_to_text_pings_until_close(
        websocket: &mut TestWebSocket,
        received: &mut Vec<String>,
        duration: Duration,
    ) -> Result<()> {
        let deadline = time::sleep(duration);
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
                        if payload.as_str() == OKX_WEBSOCKET_TEXT_PING {
                            websocket
                                .send(Message::Text(OKX_WEBSOCKET_TEXT_PONG.into()))
                                .await?;
                        }
                    }
                }
            }
        }
    }

    fn protocol_error(error: &anyhow::Error) -> &OkxWebsocketProtocolError {
        error
            .downcast_ref::<OkxWebsocketProtocolError>()
            .expect("WebSocket ACK failure should preserve typed protocol error")
    }
}
