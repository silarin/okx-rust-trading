use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque, hash_map::Entry},
    fmt,
    future::Future,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{Method, Proxy, header};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use sha2::Sha256;
use time::OffsetDateTime;
use tracing::{debug, info, warn};
use url::Url;
use zeroize::Zeroizing;

use crate::{
    config::{
        runtime::timeout_ms_to_secs,
        types::{BotConfig, OkxConfig, RequestedTradingInstrument},
        validation::okx_simulated_trading_from_routing,
    },
    okx::capability::{
        AccountLevelDiagnostic, AccountLevelDiagnosticSnapshot, RequestedCapability,
        ValidatedCapabilityGeneration,
    },
    okx::trading_instrument::{
        TradingInstrumentExchangeEvidence, ValidatedQuoteUsdRate, ValidatedSpotPriceLimit,
        ValidatedTradingInstrument,
    },
    okx::types::{
        MarketBar, OkxAccountConfig, OkxAlgoOrder, OkxAlgoOrderAck, OkxBalance, OkxFill,
        OkxIndexTicker, OkxInstrument, OkxMaximumAvailableSize, OkxMaximumOrderSize, OkxOrder,
        OkxOrderAck, OkxPriceLimit, OkxTicker, OkxTradeFeeRate, OkxTradeFeeResponse, OrderKind,
        OrderSide,
    },
    okx::websocket::{
        OkxMarketDataCache, OkxPrivateEventCache, OkxWebsocketInstrumentUpdate,
        okx_public_candle_channel_for_bar,
    },
    okx::zero_fee_pair_selection::{
        OkxSelectionInstrument, OkxSelectionOrderBook, OkxSelectionTicker,
    },
};

#[cfg(test)]
use crate::okx::types::OkxOcoOrder;

pub(crate) use crate::okx::queries::{
    AlgoHistoryFilter, OKX_ALGO_HISTORY_PAGE_LIMIT, OKX_OPEN_ALGO_ORDERS_PAGE_LIMIT,
    OKX_OPEN_ORDERS_PAGE_LIMIT, OKX_ORDER_FILLS_PAGE_LIMIT, OKX_ORDER_HISTORY_PAGE_LIMIT,
    algo_order_history_query, okx_query, open_algo_orders_query, open_orders_query,
    order_fills_query, order_history_query,
};

type HmacSha256 = Hmac<Sha256>;

const OKX_API_KEY: &str = "OK-ACCESS-KEY";
const OKX_API_SIGN: &str = "OK-ACCESS-SIGN";
const OKX_API_TIMESTAMP: &str = "OK-ACCESS-TIMESTAMP";
const OKX_API_PASSPHRASE: &str = "OK-ACCESS-PASSPHRASE";
const OKX_DOCUMENTED_SIMULATED_TRADING: &str = "x-simulated-trading";
const OKX_ORDER_EXP_TIME: &str = "expTime";
const OKX_SERVER_TIME_PATH: &str = "/api/v5/public/time";
const OKX_SPOT_INST_TYPE: &str = "SPOT";
const OKX_TRADING_TUPLE_EVIDENCE_MAX_AGE: Duration = Duration::from_secs(30);
const OKX_STALE_MARKET_EVIDENCE_MAX_ATTEMPTS: usize = 3;
const OKX_STALE_MARKET_EVIDENCE_RETRY_DELAY: Duration = Duration::from_millis(250);
const OKX_STARTUP_TICKER_MAX_AGE: Duration = Duration::from_secs(5);
const OKX_INDEX_TICKER_MAX_AGE: Duration = Duration::from_secs(5);
pub(crate) const OKX_SERVER_TIME_TTL: Duration = Duration::from_secs(30);
pub(crate) const OKX_SERVER_TIME_REFRESH_MARGIN: Duration = Duration::from_secs(10);
const OKX_ORDER_EXPIRY_WINDOW_MS: i128 = 2_000;
const OKX_CANCEL_ALL_AFTER_MIN_TIMEOUT_SECS: u64 = 10;
const OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS: u64 = 120;
pub(crate) const OKX_CANCEL_ALL_AFTER_TAG: &str = "okxrusttrading";
const OKX_RATE_LIMIT_CODES: [&str; 3] = ["50011", "50040", "50061"];
const OKX_DUPLICATE_ALGO_CLIENT_ORDER_ID_CODES: [&str; 1] = ["51065"];
const OKX_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(2);
const OKX_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(2);
const OKX_RATE_LIMIT_PACING_SAFETY_MARGIN: Duration = Duration::from_millis(50);
const OKX_CANCEL_ALL_AFTER_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);
const OKX_GATEWAY_LATENCY_WARN_THRESHOLD: Duration = Duration::from_millis(250);
const OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW: u64 = 100;
// Single audited cap for OKX REST response bodies. Normal OKX envelopes used
// by this client are far smaller; larger bodies fail closed before parsing.
pub(crate) const OKX_REST_MAX_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
const OKX_LIVE_CANDLE_REST_FALLBACK_CACHE_MAX_KEYS: usize = 64;
const MILLIS_PER_SECOND: i128 = 1_000;
const OKX_ORDER_HISTORY_MAX_PAGES: usize = 20;
const OKX_OPEN_ORDERS_MAX_PAGES: usize = 20;
const OKX_ORDER_FILLS_MAX_PAGES: usize = 20;
const OKX_OPEN_ALGO_ORDERS_MAX_PAGES: usize = 20;
const OKX_ALGO_HISTORY_MAX_PAGES: usize = 20;
const OKX_ALGO_HISTORY_STATES: [&str; 3] = ["effective", "canceled", "order_failed"];
const OKX_SPOT_MARKET_BAN_AMEND: bool = true;
const OKX_SPOT_MARKET_SLIPPAGE_PCT: &str = "0";
const OKX_PRICE_AMEND_TYPE_REJECT: &str = "0";

pub trait OkxClient {
    fn record_order_decision(&self, _decided_at: Instant) {}

    fn instruments(&self, inst_id: &str) -> impl Future<Output = Result<OkxInstrument>> + Send;

    fn candles(
        &self,
        inst_id: &str,
        bar: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<MarketBar>>> + Send;

    fn live_candles(
        &self,
        inst_id: &str,
        bar: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<MarketBar>>> + Send;

    fn ticker(&self, inst_id: &str) -> impl Future<Output = Result<OkxTicker>> + Send;

    fn fresh_quote_usd_rate(
        &self,
        instrument: &ValidatedTradingInstrument,
    ) -> impl Future<Output = Result<ValidatedQuoteUsdRate>> + Send;

    fn balances(&self) -> impl Future<Output = Result<Vec<OkxBalance>>> + Send;

    fn spot_trade_fee(&self, inst_id: &str)
    -> impl Future<Output = Result<OkxTradeFeeRate>> + Send;

    fn open_orders(&self, inst_id: &str) -> impl Future<Output = Result<Vec<OkxOrder>>> + Send;

    fn order_history(&self, inst_id: &str) -> impl Future<Output = Result<Vec<OkxOrder>>> + Send;

    fn order_fills(&self, inst_id: &str) -> impl Future<Output = Result<Vec<OkxFill>>> + Send;

    fn open_algo_orders(
        &self,
        inst_id: &str,
    ) -> impl Future<Output = Result<Vec<OkxAlgoOrder>>> + Send;

    fn algo_order_history(
        &self,
        inst_id: &str,
    ) -> impl Future<Output = Result<Vec<OkxAlgoOrder>>> + Send;

    fn place_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        kind: OrderKind,
        size: &str,
        price: Option<&str>,
        client_order_id: &str,
    ) -> impl Future<Output = Result<OkxOrderAck>> + Send;

    fn cancel_order(
        &self,
        inst_id: &str,
        client_order_id: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    fn amend_order(
        &self,
        request: OkxOrderAmend<'_>,
    ) -> impl Future<Output = Result<OkxOrderAck>> + Send;

    fn place_trigger_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        size: &str,
        trigger_price: &str,
        client_order_id: &str,
    ) -> impl Future<Output = Result<OkxAlgoOrderAck>> + Send;

    fn cancel_algo_order(
        &self,
        inst_id: &str,
        algo_id: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    fn order(
        &self,
        inst_id: &str,
        client_order_id: &str,
    ) -> impl Future<Output = Result<Option<OkxOrder>>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OkxOrderAmend<'a> {
    pub inst_id: &'a str,
    pub side: OrderSide,
    pub client_order_id: &'a str,
    pub new_size: Option<&'a str>,
    pub new_price: Option<&'a str>,
}

impl OkxOrderAmend<'_> {
    pub(crate) fn validate(self) -> Result<()> {
        validate_order_amend(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OkxOrderSubmitReconciliation<'a> {
    pub inst_id: &'a str,
    pub side: OrderSide,
    pub kind: OrderKind,
    pub size: &'a str,
    pub price: Option<&'a str>,
    pub client_order_id: &'a str,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OkxOcoProtection<'a> {
    pub(crate) inst_id: &'a str,
    pub(crate) size: &'a str,
    pub(crate) take_profit_trigger_price: &'a str,
    pub(crate) stop_loss_trigger_price: &'a str,
    pub(crate) client_order_id: &'a str,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OkxOcoAmend<'a> {
    pub(crate) inst_id: &'a str,
    pub(crate) algo_id: &'a str,
    pub(crate) client_order_id: &'a str,
    pub(crate) new_size: &'a str,
    pub(crate) new_take_profit_trigger_price: &'a str,
    pub(crate) new_stop_loss_trigger_price: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OkxCancelAllAfterTimeout {
    seconds: u64,
}

impl OkxCancelAllAfterTimeout {
    pub(crate) const DISARM_SECONDS: u64 = 0;
    pub(crate) const MIN_SECONDS: u64 = OKX_CANCEL_ALL_AFTER_MIN_TIMEOUT_SECS;
    pub(crate) const MAX_SECONDS: u64 = OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS;

    pub(crate) fn new(seconds: u64) -> Result<Self> {
        ensure!(
            (OKX_CANCEL_ALL_AFTER_MIN_TIMEOUT_SECS..=OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS)
                .contains(&seconds),
            "OKX cancel-all-after timeout must be between {OKX_CANCEL_ALL_AFTER_MIN_TIMEOUT_SECS} and {OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS} seconds"
        );
        Ok(Self { seconds })
    }

    pub(crate) const fn disarm() -> Self {
        Self {
            seconds: Self::DISARM_SECONDS,
        }
    }

    pub(crate) const fn seconds(self) -> u64 {
        self.seconds
    }

    pub(crate) const fn is_disarm(self) -> bool {
        self.seconds == Self::DISARM_SECONDS
    }

    fn okx_seconds(self) -> String {
        self.seconds.to_string()
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct OkxCancelAllAfterAck {
    #[serde(rename = "triggerTime")]
    pub(crate) trigger_time: String,
    #[serde(default)]
    tag: String,
    pub(crate) ts: String,
}

#[derive(Clone)]
pub struct OkxRestClient {
    http: reqwest::Client,
    base_url: Url,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    api_passphrase: Zeroizing<String>,
    simulated_trading: bool,
    server_time_clock: ServerTimeClock,
    rate_limit_pacer: RateLimitPacer,
    gateway_latency_recorder: OkxGatewayLatencyRecorder,
    market_data_cache: OkxMarketDataCache,
    market_data_max_staleness: Duration,
    recent_live_candle_rest_fallbacks:
        Arc<Mutex<HashMap<LiveCandleFallbackKey, LiveCandleFallback>>>,
    private_event_cache: OkxPrivateEventCache,
    instrument_snapshots: Arc<Mutex<HashMap<String, OkxInstrument>>>,
    instrument_metadata_safety_latches: Arc<Mutex<HashMap<String, String>>>,
    validated_capability_generations:
        Arc<Mutex<HashMap<String, Arc<ValidatedCapabilityGeneration>>>>,
    account_level_diagnostic: Arc<Mutex<Option<AccountLevelDiagnosticSnapshot>>>,
    #[cfg(test)]
    test_account_spot_trade_quote_currencies: Arc<Mutex<HashMap<String, String>>>,
}

#[derive(Clone)]
pub(crate) struct OkxWebsocketLoginTimestampProvider {
    source: OkxWebsocketLoginTimestampSource,
}

#[derive(Clone)]
enum OkxWebsocketLoginTimestampSource {
    ServerTime(Arc<OkxRestClient>),
    #[cfg(test)]
    Fixed(String),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct LiveCandleFallbackKey {
    inst_id: String,
    bar: String,
    limit: usize,
}

#[derive(Clone, Debug)]
struct LiveCandleFallback {
    candles: Vec<MarketBar>,
    fetched_at: Instant,
}

impl OkxRestClient {
    pub fn from_config(config: &BotConfig) -> Result<Self> {
        let okx = config.okx.as_ref().context("OKX config is required")?;
        Self::new(okx, okx_simulated_trading_from_routing(okx))
    }

    pub fn new(okx: &OkxConfig, simulated_trading: bool) -> Result<Self> {
        let timeout = std::time::Duration::from_secs(timeout_ms_to_secs(okx.request_timeout_ms));
        Self::new_with_timeout(okx, simulated_trading, timeout)
    }

    pub(crate) fn new_with_timeout(
        okx: &OkxConfig,
        simulated_trading: bool,
        timeout: Duration,
    ) -> Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(timeout);
        if let Some(proxy_url) = &okx.proxy_url {
            builder = builder.proxy(Proxy::all(proxy_url).context("invalid OKX proxy_url")?);
        }
        Ok(Self {
            http: builder.build().context("failed building OKX HTTP client")?,
            base_url: Url::parse(&okx.base_url).context("invalid OKX base_url")?,
            api_key: okx.api_key.clone(),
            api_secret: okx.api_secret.clone(),
            api_passphrase: okx.api_passphrase.clone(),
            simulated_trading,
            server_time_clock: ServerTimeClock::default(),
            rate_limit_pacer: RateLimitPacer::default(),
            gateway_latency_recorder: OkxGatewayLatencyRecorder::default(),
            market_data_cache: OkxMarketDataCache::default(),
            market_data_max_staleness: Duration::from_millis(okx.websocket.max_staleness_ms),
            recent_live_candle_rest_fallbacks: Arc::new(Mutex::new(HashMap::new())),
            private_event_cache: OkxPrivateEventCache::default(),
            instrument_snapshots: Arc::new(Mutex::new(HashMap::new())),
            instrument_metadata_safety_latches: Arc::new(Mutex::new(HashMap::new())),
            validated_capability_generations: Arc::new(Mutex::new(HashMap::new())),
            account_level_diagnostic: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            test_account_spot_trade_quote_currencies: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub(crate) fn market_data_cache(&self) -> OkxMarketDataCache {
        self.market_data_cache.clone()
    }

    pub(crate) fn private_event_cache(&self) -> OkxPrivateEventCache {
        self.private_event_cache.clone()
    }

    pub(crate) fn websocket_login_timestamp_provider(&self) -> OkxWebsocketLoginTimestampProvider {
        OkxWebsocketLoginTimestampProvider {
            source: OkxWebsocketLoginTimestampSource::ServerTime(Arc::new(self.clone())),
        }
    }

    pub async fn instruments(&self, inst_id: &str) -> Result<OkxInstrument> {
        self.instrument_for_type(OKX_SPOT_INST_TYPE, inst_id).await
    }

    async fn requested_public_instrument(
        &self,
        requested: &RequestedTradingInstrument,
    ) -> Result<OkxInstrument> {
        self.instrument_for_type(requested.inst_type.as_okx(), requested.instrument.as_str())
            .await
    }

    #[cfg(test)]
    pub(crate) async fn validate_requested_public_instrument(
        &self,
        requested: &RequestedTradingInstrument,
    ) -> Result<OkxInstrument> {
        self.requested_public_instrument(requested).await
    }

    pub(crate) async fn instrument_for_type(
        &self,
        requested_inst_type: &str,
        inst_id: &str,
    ) -> Result<OkxInstrument> {
        self.ensure_instrument_metadata_safety_latch_clear(inst_id)?;
        let validated_context = self.validated_trading_instrument_if_present(inst_id)?;
        let path = "/api/v5/public/instruments";
        let query = okx_query(&[("instType", requested_inst_type), ("instId", inst_id)]);
        let instruments = self.public_get::<OkxInstrument>(path, Some(&query)).await?;
        let validation = (|| {
            ensure!(
                instruments.len() == 1,
                "OKX returned {} instrument specs for {inst_id}",
                instruments.len()
            );
            let instrument = instruments
                .into_iter()
                .next()
                .context("OKX instrument response unexpectedly became empty")?;
            ensure!(
                instrument.inst_id == inst_id,
                "OKX returned instrument spec {} for requested {inst_id}",
                instrument.inst_id
            );
            ensure!(
                instrument.inst_type == requested_inst_type,
                "OKX returned instType {} for requested {} instrument {inst_id}",
                instrument.inst_type,
                requested_inst_type
            );
            let returned_identity = format!("{}-{}", instrument.base_ccy, instrument.quote_ccy);
            ensure!(
                returned_identity == inst_id,
                "OKX instrument {inst_id} returned currencies {}/{} that do not compose the exact requested identity",
                instrument.base_ccy,
                instrument.quote_ccy
            );
            instrument.ensure_trade_quote_currency(&instrument.quote_ccy)?;
            instrument.fee_group_id()?;
            instrument.tick_size()?;
            instrument.lot_size()?;
            instrument.min_size()?;
            instrument.ensure_live()?;
            instrument.validate_order_limits()?;
            self.ensure_fresh_instrument_hint_matches_rest_snapshot(&instrument)?;
            if let Some(validated) = &validated_context {
                validated.ensure_public_refresh_matches(&instrument)?;
            }
            self.remember_instrument_snapshot(instrument.clone())?;
            Ok(instrument)
        })();
        match validation {
            Ok(instrument) => Ok(instrument),
            Err(error) if validated_context.is_some() => {
                Err(self.latch_instrument_metadata_failure(inst_id, error))
            }
            Err(error) => Err(error),
        }
    }

    pub async fn candles(&self, inst_id: &str, bar: &str, limit: usize) -> Result<Vec<MarketBar>> {
        let path = if bar == "1s" {
            "/api/v5/market/history-candles"
        } else {
            "/api/v5/market/candles"
        };
        let limit = limit.to_string();
        let query = okx_query(&[("instId", inst_id), ("bar", bar), ("limit", &limit)]);
        let mut candles = self.public_get::<MarketBar>(path, Some(&query)).await?;
        for candle in &candles {
            candle.validate("OKX REST candle")?;
        }
        candles.sort_by_key(|bar| bar.ts_ms);
        Ok(candles)
    }

    pub async fn live_candles(
        &self,
        inst_id: &str,
        bar: &str,
        limit: usize,
    ) -> Result<Vec<MarketBar>> {
        let Ok(channel) = okx_public_candle_channel_for_bar(bar) else {
            return self.candles(inst_id, bar, limit).await;
        };
        if let Some(candles) = fresh_live_candles_from_cache(
            &self.market_data_cache,
            inst_id,
            channel,
            self.market_data_max_staleness,
            limit,
        ) {
            return Ok(candles);
        }
        if let Some(candles) = self.recent_live_candle_rest_fallback(inst_id, bar, limit)? {
            let hints = self.market_data_cache.fresh_candles(
                inst_id,
                channel,
                self.market_data_max_staleness,
            );
            return Ok(merge_live_candle_hints(candles, hints, limit));
        }
        let candles = self.candles(inst_id, bar, limit).await?;
        self.remember_live_candle_rest_fallback(inst_id, bar, limit, candles.clone())?;
        let hints =
            self.market_data_cache
                .fresh_candles(inst_id, channel, self.market_data_max_staleness);
        Ok(merge_live_candle_hints(candles, hints, limit))
    }

    fn recent_live_candle_rest_fallback(
        &self,
        inst_id: &str,
        bar: &str,
        limit: usize,
    ) -> Result<Option<Vec<MarketBar>>> {
        let mut fallbacks = self
            .recent_live_candle_rest_fallbacks
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX live candle REST fallback cache lock poisoned"))?;
        let key = live_candle_fallback_key(inst_id, bar, limit);
        let Some(fallback) = fallbacks.get(&key) else {
            return Ok(None);
        };
        if fallback.fetched_at.elapsed() <= self.market_data_max_staleness {
            return Ok(Some(fallback.candles.clone()));
        }
        fallbacks.remove(&key);
        Ok(None)
    }

    fn remember_live_candle_rest_fallback(
        &self,
        inst_id: &str,
        bar: &str,
        limit: usize,
        candles: Vec<MarketBar>,
    ) -> Result<()> {
        let mut fallbacks = self
            .recent_live_candle_rest_fallbacks
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX live candle REST fallback cache lock poisoned"))?;
        prune_expired_live_candle_fallbacks(&mut fallbacks, self.market_data_max_staleness);
        fallbacks.insert(
            live_candle_fallback_key(inst_id, bar, limit),
            LiveCandleFallback {
                candles,
                fetched_at: Instant::now(),
            },
        );
        while fallbacks.len() > OKX_LIVE_CANDLE_REST_FALLBACK_CACHE_MAX_KEYS {
            evict_oldest_live_candle_fallback(&mut fallbacks);
        }
        Ok(())
    }

    pub async fn ticker(&self, inst_id: &str) -> Result<OkxTicker> {
        self.ticker_with_max_age(inst_id, self.market_data_max_staleness)
            .await
    }

    async fn ticker_with_max_age(
        &self,
        inst_id: &str,
        max_staleness: Duration,
    ) -> Result<OkxTicker> {
        self.ensure_fresh_instrument_hint_matches_snapshot(inst_id)?;
        if let Some(ticker) = self.market_data_cache.fresh_ticker(inst_id, max_staleness) {
            return Ok(ticker);
        }
        debug!(
            safety_event = "rest_fallback_ws_hint_unavailable",
            instrument_id = inst_id,
            hint_kind = "public_ticker",
            max_staleness_ms = max_staleness.as_millis(),
            "falling back to REST after stale or missing WebSocket market hint"
        );

        let path = "/api/v5/market/ticker";
        let query = okx_query(&[("instId", inst_id)]);
        let mut tickers = self.public_get::<OkxRestTicker>(path, Some(&query)).await?;
        ensure!(
            tickers.len() == 1,
            "OKX returned {} tickers for {inst_id}",
            tickers.len()
        );
        let rest_ticker = tickers.remove(0);
        let ticker = rest_ticker.ticker;
        ensure!(
            ticker.inst_id == inst_id,
            "OKX returned ticker {} for requested {inst_id}",
            ticker.inst_id
        );
        ensure_spot_inst_type(&ticker.inst_type, &ticker.inst_id, "ticker")?;
        ticker.validate_prices()?;
        let server_now_ms = self
            .server_time_clock
            .unix_millis()?
            .context("OKX REST ticker freshness requires synchronized server time")?;
        ensure_fresh_rest_ticker_timestamp(&rest_ticker.timestamp, server_now_ms, max_staleness)?;
        Ok(ticker)
    }

    async fn startup_ticker(&self, inst_id: &str) -> Result<OkxTicker> {
        for attempt in 1..=OKX_STALE_MARKET_EVIDENCE_MAX_ATTEMPTS {
            match self
                .ticker_with_max_age(inst_id, OKX_STARTUP_TICKER_MAX_AGE)
                .await
            {
                Ok(ticker) => return Ok(ticker),
                Err(error)
                    if error.downcast_ref::<StaleRestTimestampError>().is_some()
                        && attempt < OKX_STALE_MARKET_EVIDENCE_MAX_ATTEMPTS =>
                {
                    warn!(
                        safety_event = "startup_stale_rest_ticker_retry",
                        instrument_id = inst_id,
                        attempt,
                        max_attempts = OKX_STALE_MARKET_EVIDENCE_MAX_ATTEMPTS,
                        retry_delay_ms = OKX_STALE_MARKET_EVIDENCE_RETRY_DELAY.as_millis(),
                        "discarding stale REST ticker cache response and retrying startup evidence acquisition"
                    );
                    tokio::time::sleep(OKX_STALE_MARKET_EVIDENCE_RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("startup ticker attempt loop always returns")
    }

    async fn quote_usd_rate_for_quote(&self, quote_ccy: &str) -> Result<ValidatedQuoteUsdRate> {
        if quote_ccy == "USD" {
            return ValidatedQuoteUsdRate::identity(quote_ccy);
        }

        for attempt in 1..=OKX_STALE_MARKET_EVIDENCE_MAX_ATTEMPTS {
            match self.quote_usd_rate_from_index(quote_ccy).await {
                Ok(rate) => return Ok(rate),
                Err(error)
                    if error.downcast_ref::<StaleRestTimestampError>().is_some()
                        && attempt < OKX_STALE_MARKET_EVIDENCE_MAX_ATTEMPTS =>
                {
                    warn!(
                        safety_event = "stale_rest_index_ticker_retry",
                        quote_currency = quote_ccy,
                        attempt,
                        max_attempts = OKX_STALE_MARKET_EVIDENCE_MAX_ATTEMPTS,
                        retry_delay_ms = OKX_STALE_MARKET_EVIDENCE_RETRY_DELAY.as_millis(),
                        "discarding stale REST index-ticker cache response and retrying conversion evidence acquisition"
                    );
                    tokio::time::sleep(OKX_STALE_MARKET_EVIDENCE_RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("quote-to-USD index ticker attempt loop always returns")
    }

    async fn quote_usd_rate_from_index(&self, quote_ccy: &str) -> Result<ValidatedQuoteUsdRate> {
        let index_inst_id = format!("{quote_ccy}-USD");
        let path = "/api/v5/market/index-tickers";
        let query = okx_query(&[("instId", index_inst_id.as_str())]);
        let mut rows = self
            .public_get::<OkxIndexTicker>(path, Some(&query))
            .await?;
        ensure!(
            rows.len() == 1,
            "OKX returned {} index tickers for {index_inst_id}",
            rows.len()
        );
        let ticker = rows.remove(0);
        let rate = ValidatedQuoteUsdRate::from_index_ticker(quote_ccy, &ticker)?;
        let server_now_ms = self
            .server_time_clock
            .unix_millis()?
            .context("OKX index ticker freshness requires synchronized server time")?;
        let source_timestamp_ms = rate
            .source_timestamp_ms()
            .context("OKX index ticker conversion omitted source timestamp")?;
        ensure_fresh_rest_timestamp(
            "OKX REST index ticker timestamp",
            source_timestamp_ms,
            server_now_ms,
            OKX_INDEX_TICKER_MAX_AGE,
        )?;
        Ok(rate)
    }

    pub(crate) async fn fresh_quote_usd_rate(
        &self,
        instrument: &ValidatedTradingInstrument,
    ) -> Result<ValidatedQuoteUsdRate> {
        ensure!(
            instrument.has_usd_order_amount_limit()?,
            "fresh quote-to-USD evidence was requested without a USD-denominated order limit"
        );
        self.quote_usd_rate_for_quote(instrument.quote_ccy()).await
    }

    async fn fresh_spot_price_limit(&self, inst_id: &str) -> Result<ValidatedSpotPriceLimit> {
        #[cfg(test)]
        if self
            .validated_trading_instrument_if_present(inst_id)?
            .is_none()
        {
            self.validated_order_route(inst_id)?;
            return Ok(ValidatedSpotPriceLimit::disabled_for_unvalidated_test_route(inst_id));
        }
        self.validated_order_route(inst_id)?;
        self.ensure_fresh_instrument_hint_matches_snapshot(inst_id)?;
        self.refresh_server_time_if_expiring().await?;
        let path = "/api/v5/public/price-limit";
        let query = okx_query(&[("instId", inst_id)]);
        let mut rows = self.public_get::<OkxPriceLimit>(path, Some(&query)).await?;
        ensure!(
            rows.len() == 1,
            "OKX returned {} price-limit rows for {inst_id}",
            rows.len()
        );
        let server_now_ms = self
            .server_time_clock
            .unix_millis()?
            .context("OKX price-limit freshness requires synchronized server time")?;
        let evidence = ValidatedSpotPriceLimit::from_response(
            inst_id,
            rows.remove(0),
            server_now_ms,
            self.market_data_max_staleness,
        )?;
        debug!(
            instrument_id = inst_id,
            price_limit_timestamp_ms = evidence.source_timestamp_ms(),
            "validated fresh OKX SPOT price-limit evidence"
        );
        Ok(evidence)
    }

    async fn ensure_fresh_spot_order_price(
        &self,
        inst_id: &str,
        side: OrderSide,
        price: &str,
        context: &str,
    ) -> Result<()> {
        let price = parse_positive_decimal_field(context, price)?;
        self.fresh_spot_price_limit(inst_id)
            .await?
            .ensure_price(side, price, context)
    }

    fn ensure_fresh_instrument_hint_matches_snapshot(&self, inst_id: &str) -> Result<()> {
        self.ensure_instrument_metadata_safety_latch_clear(inst_id)?;
        let Some(instrument) = self
            .market_data_cache
            .fresh_instrument(inst_id, self.market_data_max_staleness)
        else {
            return Ok(());
        };
        if let Err(error) = instrument.ensure_live() {
            return Err(self.latch_instrument_metadata_failure(inst_id, error));
        }
        if let Some(snapshot) = self.instrument_snapshot(&instrument.inst_id)?
            && let Err(error) = ensure_instrument_hint_matches_rest_snapshot(&instrument, &snapshot)
        {
            return Err(self.latch_instrument_metadata_failure(inst_id, error));
        }
        Ok(())
    }

    fn ensure_fresh_instrument_hint_matches_rest_snapshot(
        &self,
        snapshot: &OkxInstrument,
    ) -> Result<()> {
        self.ensure_instrument_metadata_safety_latch_clear(&snapshot.inst_id)?;
        let Some(instrument) = self
            .market_data_cache
            .fresh_instrument(&snapshot.inst_id, self.market_data_max_staleness)
        else {
            return Ok(());
        };
        if let Err(error) = instrument.ensure_live() {
            return Err(self.latch_instrument_metadata_failure(&snapshot.inst_id, error));
        }
        ensure_instrument_hint_matches_rest_snapshot(&instrument, snapshot)
            .map_err(|error| self.latch_instrument_metadata_failure(&snapshot.inst_id, error))
    }

    fn remember_instrument_snapshot(&self, instrument: OkxInstrument) -> Result<()> {
        lock_instrument_snapshots(&self.instrument_snapshots)?
            .insert(instrument.inst_id.clone(), instrument);
        Ok(())
    }

    fn instrument_snapshot(&self, inst_id: &str) -> Result<Option<OkxInstrument>> {
        Ok(lock_instrument_snapshots(&self.instrument_snapshots)?
            .get(inst_id)
            .cloned())
    }

    fn ensure_instrument_metadata_safety_latch_clear(&self, inst_id: &str) -> Result<()> {
        let latches = self
            .instrument_metadata_safety_latches
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX instrument metadata safety latch lock poisoned"))?;
        if let Some(reason) = latches.get(inst_id) {
            bail!(
                "OKX instrument metadata safety latch for {inst_id} is set: {reason}; process restart requires fresh authoritative instrument reconciliation"
            );
        }
        Ok(())
    }

    fn latch_instrument_metadata_failure(
        &self,
        inst_id: &str,
        error: anyhow::Error,
    ) -> anyhow::Error {
        let reason = format!("{error:#}");
        let Ok(mut latches) = self.instrument_metadata_safety_latches.lock() else {
            return anyhow::anyhow!(
                "OKX instrument metadata safety latch lock poisoned for {inst_id} while handling: {reason}"
            );
        };
        let (newly_latched, latched_reason) = match latches.entry(inst_id.to_owned()) {
            Entry::Occupied(entry) => (false, entry.get().clone()),
            Entry::Vacant(entry) => (true, entry.insert(reason).clone()),
        };
        drop(latches);
        if newly_latched {
            warn!(
                safety_event = "instrument_metadata_safety_latched",
                instrument_id = inst_id,
                "contradictory OKX instrument metadata latched; order creation and amendment remain blocked until process restart"
            );
        }
        anyhow::anyhow!(
            "OKX instrument metadata safety latch for {inst_id} is set: {latched_reason}; process restart requires fresh authoritative instrument reconciliation"
        )
    }

    pub async fn balances(&self) -> Result<Vec<OkxBalance>> {
        let balances = self
            .private_request::<Vec<OkxBalance>, EmptyBody>(
                Method::GET,
                "/api/v5/account/balance",
                None,
                None,
            )
            .await?;
        for balance in &balances {
            balance
                .validate()
                .context("OKX account balance response is invalid")?;
        }
        self.observe_private_account_hint(&balances);
        Ok(balances)
    }

    pub(crate) async fn account_config(&self) -> Result<OkxAccountConfig> {
        let result = async {
            let mut configs = self
                .private_request::<Vec<OkxAccountConfig>, EmptyBody>(
                    Method::GET,
                    "/api/v5/account/config",
                    None,
                    None,
                )
                .await?;
            ensure!(
                configs.len() == 1,
                "OKX returned {} account configuration rows",
                configs.len()
            );
            let config = configs.remove(0);
            self.observe_account_level_diagnostic(&config)?;
            Ok(config)
        }
        .await;
        if result.is_err() {
            self.revoke_capability_generations()?;
        }
        result
    }

    pub(crate) async fn spot_trade_fee(&self, inst_id: &str) -> Result<OkxTradeFeeRate> {
        let expected_group_id = self.instrument_fee_group_id(inst_id)?;
        self.spot_trade_fee_for_group(inst_id, &expected_group_id)
            .await
    }

    pub(crate) async fn spot_trade_fee_for_group(
        &self,
        inst_id: &str,
        expected_group_id: &str,
    ) -> Result<OkxTradeFeeRate> {
        let inst_type = self.instrument_type_for_fee_group(inst_id, expected_group_id)?;
        let query = okx_query(&[("instType", &inst_type), ("instId", inst_id)]);
        let mut fees = self
            .private_request::<Vec<OkxTradeFeeResponse>, EmptyBody>(
                Method::GET,
                "/api/v5/account/trade-fee",
                Some(&query),
                None,
            )
            .await?;
        ensure!(
            fees.len() == 1,
            "OKX returned {} SPOT fee-rate rows for {inst_id}",
            fees.len()
        );
        let fee = fees.remove(0);
        fee.into_spot_group_rate(inst_id, expected_group_id)
    }

    pub(crate) async fn maximum_order_size(
        &self,
        inst_id: &str,
        td_mode: &str,
        price: &str,
        trade_quote_currency: &str,
    ) -> Result<OkxMaximumOrderSize> {
        parse_positive_decimal_field("OKX maximum order size px", price)?;
        ensure!(
            !trade_quote_currency.trim().is_empty(),
            "OKX maximum order size tradeQuoteCcy must not be empty"
        );
        let query = okx_query(&[
            ("instId", inst_id),
            ("tdMode", td_mode),
            ("px", price),
            ("tradeQuoteCcy", trade_quote_currency),
        ]);
        let mut rows = self
            .private_account_sizing_request::<Vec<OkxMaximumOrderSize>, EmptyBody>(
                Method::GET,
                "/api/v5/account/max-size",
                Some(&query),
                None,
            )
            .await?;
        ensure!(
            rows.len() == 1,
            "OKX returned {} maximum order size rows for {inst_id}",
            rows.len()
        );
        let row = rows.remove(0);
        ensure!(
            row.inst_id == inst_id,
            "OKX maximum order size returned instId {} for requested {inst_id}",
            row.inst_id
        );
        row.max_buy_base()?;
        row.max_sell_quote()?;
        Ok(row)
    }

    pub(crate) async fn maximum_available_size(
        &self,
        inst_id: &str,
        td_mode: &str,
        trade_quote_currency: &str,
    ) -> Result<OkxMaximumAvailableSize> {
        ensure!(
            !trade_quote_currency.trim().is_empty(),
            "OKX maximum available size tradeQuoteCcy must not be empty"
        );
        let query = okx_query(&[
            ("instId", inst_id),
            ("tdMode", td_mode),
            ("tradeQuoteCcy", trade_quote_currency),
        ]);
        let mut rows = self
            .private_account_sizing_request::<Vec<OkxMaximumAvailableSize>, EmptyBody>(
                Method::GET,
                "/api/v5/account/max-avail-size",
                Some(&query),
                None,
            )
            .await?;
        ensure!(
            rows.len() == 1,
            "OKX returned {} maximum available size rows for {inst_id}",
            rows.len()
        );
        let row = rows.remove(0);
        ensure!(
            row.inst_id == inst_id,
            "OKX maximum available size returned instId {} for requested {inst_id}",
            row.inst_id
        );
        row.available_buy_quote()?;
        row.available_sell_base()?;
        Ok(row)
    }

    pub(crate) async fn selection_account_spot_instruments(
        &self,
    ) -> Result<Vec<OkxSelectionInstrument>> {
        let query = okx_query(&[("instType", OKX_SPOT_INST_TYPE)]);
        self.private_request::<Vec<OkxSelectionInstrument>, EmptyBody>(
            Method::GET,
            "/api/v5/account/instruments",
            Some(&query),
            None,
        )
        .await
    }

    pub(crate) async fn validate_trading_instrument(
        &self,
        requested: &RequestedTradingInstrument,
        account_config: &OkxAccountConfig,
    ) -> Result<Arc<ValidatedCapabilityGeneration>> {
        let result = self
            .validate_trading_instrument_generation(requested, account_config)
            .await;
        if result.is_err() {
            self.revoke_capability_generations()?;
        }
        result
    }

    async fn validate_trading_instrument_generation(
        &self,
        requested: &RequestedTradingInstrument,
        account_config: &OkxAccountConfig,
    ) -> Result<Arc<ValidatedCapabilityGeneration>> {
        let evidence_started_at = Instant::now();
        let requested_capability = RequestedCapability::from_trading_instrument(requested)?;
        let account_level = self.matching_observed_account_level_diagnostic(account_config)?;
        let public = self.requested_public_instrument(requested).await?;
        let account = self.requested_account_instrument(requested).await?;
        let ticker = self.startup_ticker(requested.instrument.as_str()).await?;
        let price = ticker.last_decimal()?;
        let trade_quote_ccy = public.quote_ccy.as_str();
        let quote_usd_rate = if public.has_usd_order_amount_limit()? {
            Some(self.quote_usd_rate_for_quote(trade_quote_ccy).await?)
        } else {
            None
        };
        let maximum = self
            .maximum_order_size(
                requested.instrument.as_str(),
                requested.td_mode.as_okx(),
                &ticker.last,
                trade_quote_ccy,
            )
            .await?;
        let available = self
            .maximum_available_size(
                requested.instrument.as_str(),
                requested.td_mode.as_okx(),
                trade_quote_ccy,
            )
            .await?;
        let balances = self.balances().await?;
        ensure!(
            evidence_started_at.elapsed() <= OKX_TRADING_TUPLE_EVIDENCE_MAX_AGE,
            "OKX requested trading tuple evidence for {} became stale before startup validation completed",
            requested.instrument
        );
        let validated = Arc::new(ValidatedTradingInstrument::from_exchange_evidence(
            requested,
            TradingInstrumentExchangeEvidence {
                public,
                account,
                account_config,
                price,
                maximum: &maximum,
                available: &available,
                balances: &balances,
                quote_usd_rate: quote_usd_rate.as_ref(),
            },
        )?);
        let fee = self
            .spot_trade_fee_for_group(validated.inst_id(), validated.fee_group_id()?)
            .await?;
        let generation = Arc::new(ValidatedCapabilityGeneration::cash_spot(
            requested_capability,
            validated,
            account_level,
            fee,
            OKX_TRADING_TUPLE_EVIDENCE_MAX_AGE,
        )?);
        self.remember_validated_capability_generation(Arc::clone(&generation))?;
        Ok(generation)
    }

    async fn requested_account_instrument(
        &self,
        requested: &RequestedTradingInstrument,
    ) -> Result<OkxInstrument> {
        let inst_type = requested.inst_type.as_okx();
        let inst_id = requested.instrument.as_str();
        let exact_query = okx_query(&[("instType", inst_type), ("instId", inst_id)]);
        let rows = self
            .private_request::<Vec<OkxInstrument>, EmptyBody>(
                Method::GET,
                "/api/v5/account/instruments",
                Some(&exact_query),
                None,
            )
            .await?;
        let mut exact = rows
            .into_iter()
            .filter(|row| row.inst_id == inst_id)
            .collect::<Vec<_>>();
        if exact.is_empty() {
            let type_query = okx_query(&[("instType", inst_type)]);
            exact = self
                .private_request::<Vec<OkxInstrument>, EmptyBody>(
                    Method::GET,
                    "/api/v5/account/instruments",
                    Some(&type_query),
                    None,
                )
                .await?
                .into_iter()
                .filter(|row| row.inst_id == inst_id)
                .collect();
        }
        ensure!(
            exact.len() == 1,
            "OKX account instruments returned {} exact rows for {inst_type} {inst_id}",
            exact.len()
        );
        Ok(exact.remove(0))
    }

    pub(crate) fn validated_trading_instrument(
        &self,
        inst_id: &str,
    ) -> Result<Arc<ValidatedTradingInstrument>> {
        self.validated_trading_instrument_if_present(inst_id)?
            .with_context(|| {
                format!(
                    "OKX trading tuple for {inst_id} was not validated before the requested operation"
                )
            })
    }

    fn validated_trading_instrument_if_present(
        &self,
        inst_id: &str,
    ) -> Result<Option<Arc<ValidatedTradingInstrument>>> {
        Ok(self
            .validated_capability_generations
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX validated capability cache lock poisoned"))?
            .get(inst_id)
            .map(|generation| generation.cash_spot_context()))
    }

    #[cfg(test)]
    pub(crate) fn account_spot_trade_quote_currency(&self, inst_id: &str) -> Result<String> {
        if let Ok(validated) = self.validated_trading_instrument(inst_id) {
            return Ok(validated.trade_quote_ccy().to_owned());
        }
        #[cfg(test)]
        if let Some(value) = self
            .test_account_spot_trade_quote_currencies
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX test trade quote cache lock poisoned"))?
            .get(inst_id)
            .cloned()
        {
            return Ok(value);
        }
        bail!("OKX trading tuple for {inst_id} was not validated before order mutation")
    }

    fn validated_order_route(&self, inst_id: &str) -> Result<(&'static str, String)> {
        if let Ok(validated) = self.validated_trading_instrument(inst_id) {
            return Ok((
                validated.td_mode().as_okx(),
                validated.trade_quote_ccy().to_owned(),
            ));
        }
        #[cfg(test)]
        {
            Ok(("cash", self.account_spot_trade_quote_currency(inst_id)?))
        }
        #[cfg(not(test))]
        bail!("OKX trading tuple for {inst_id} was not validated before order mutation")
    }

    pub(crate) fn websocket_order_route(&self, inst_id: &str) -> Result<(&'static str, String)> {
        self.validated_order_route(inst_id)
    }

    pub(crate) fn validated_instrument_type(&self, inst_id: &str) -> Result<String> {
        if let Ok(validated) = self.validated_trading_instrument(inst_id) {
            return Ok(validated.inst_type().as_okx().to_owned());
        }
        #[cfg(test)]
        {
            spot_instrument_currencies(inst_id)?;
            Ok(OKX_SPOT_INST_TYPE.to_owned())
        }
        #[cfg(not(test))]
        bail!("OKX trading tuple for {inst_id} was not validated before instrument-scoped access")
    }

    fn instrument_fee_group_id(&self, inst_id: &str) -> Result<String> {
        if let Some(validated) = self.validated_trading_instrument_if_present(inst_id)? {
            return Ok(validated.fee_group_id()?.to_owned());
        }
        if let Some(snapshot) = self.instrument_snapshot(inst_id)? {
            return Ok(snapshot.fee_group_id()?.to_owned());
        }
        bail!(
            "OKX instrument fee group for {inst_id} was not validated before the fee-rate request"
        )
    }

    fn instrument_type_for_fee_group(
        &self,
        inst_id: &str,
        expected_group_id: &str,
    ) -> Result<String> {
        if let Some(validated) = self.validated_trading_instrument_if_present(inst_id)? {
            ensure!(
                validated.fee_group_id()? == expected_group_id,
                "OKX requested fee group {expected_group_id} for {inst_id} contradicts validated groupId {}",
                validated.fee_group_id()?
            );
            return Ok(validated.inst_type().as_okx().to_owned());
        }
        if let Some(snapshot) = self.instrument_snapshot(inst_id)? {
            ensure_spot_inst_type(
                &snapshot.inst_type,
                inst_id,
                "instrument fee-group authority",
            )?;
            ensure!(
                snapshot.fee_group_id()? == expected_group_id,
                "OKX requested fee group {expected_group_id} for {inst_id} contradicts instrument groupId {}",
                snapshot.fee_group_id()?
            );
            return Ok(snapshot.inst_type);
        }
        #[cfg(test)]
        {
            spot_instrument_currencies(inst_id)?;
            Ok(OKX_SPOT_INST_TYPE.to_owned())
        }
        #[cfg(not(test))]
        bail!("OKX instrument metadata for {inst_id} was not validated before the fee-rate request")
    }

    fn remember_validated_capability_generation(
        &self,
        generation: Arc<ValidatedCapabilityGeneration>,
    ) -> Result<()> {
        let validated = generation.cash_spot_context();
        let inst_id = validated.inst_id().to_owned();
        let diagnostic = self
            .account_level_diagnostic
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX account-level diagnostic lock poisoned"))?;
        let current = diagnostic
            .as_ref()
            .context("OKX account-level diagnostic was not observed before capability caching")?;
        ensure!(
            current.value() == generation.account_level_diagnostic().value(),
            "OKX account-level diagnostic changed before capability caching"
        );
        let mut generations = self
            .validated_capability_generations
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX validated capability cache lock poisoned"))?;
        if let Some(existing) = generations.get(&inst_id) {
            let existing_context = existing.cash_spot_context();
            ensure!(
                existing.requested() == generation.requested()
                    && existing_context.inst_type() == validated.inst_type()
                    && existing_context.td_mode() == validated.td_mode()
                    && existing_context.trade_quote_ccy() == validated.trade_quote_ccy()
                    && existing_context.fee_group_id()? == validated.fee_group_id()?
                    && existing_context.inst_id_code()? == validated.inst_id_code()?,
                "OKX validated trading tuple for {inst_id} changed during startup"
            );
            existing_context.ensure_public_refresh_matches(validated.instrument())?;
        }
        generations.insert(inst_id, generation);
        Ok(())
    }

    #[cfg(test)]
    fn remember_validated_trading_instrument(
        &self,
        validated: Arc<ValidatedTradingInstrument>,
    ) -> Result<()> {
        use crate::config::types::{
            RequestedInstrumentId, RequestedInstrumentType, RequestedTradeMode,
        };

        let account_config = OkxAccountConfig {
            uid: "test".to_owned(),
            main_uid: "test".to_owned(),
            account_level: "1".to_owned(),
            perm: "read_only,trade".to_owned(),
            auto_loan: false,
            enable_spot_borrow: false,
            spot_borrow_auto_repay: false,
            fee_type: "0".to_owned(),
            kyc_level: String::new(),
        };
        let account_level = self.observe_account_level_diagnostic(&account_config)?;
        let requested =
            RequestedCapability::from_trading_instrument(&RequestedTradingInstrument {
                instrument: RequestedInstrumentId::new(validated.inst_id().to_owned())
                    .map_err(anyhow::Error::msg)?,
                inst_type: RequestedInstrumentType::Spot,
                td_mode: RequestedTradeMode::Cash,
            })?;
        let fee = OkxTradeFeeRate {
            inst_type: validated.inst_type().as_okx().to_owned(),
            level: "test".to_owned(),
            group_id: validated.fee_group_id()?.to_owned(),
            maker: "-0.001".to_owned(),
            taker: "-0.001".to_owned(),
            ts: "1".to_owned(),
        };
        let generation = Arc::new(ValidatedCapabilityGeneration::cash_spot(
            requested,
            validated,
            account_level,
            fee,
            OKX_TRADING_TUPLE_EVIDENCE_MAX_AGE,
        )?);
        self.remember_validated_capability_generation(generation)
    }

    fn observe_account_level_diagnostic(
        &self,
        account_config: &OkxAccountConfig,
    ) -> Result<AccountLevelDiagnosticSnapshot> {
        let snapshot = AccountLevelDiagnosticSnapshot::observe(account_config)?;
        let mut observed = self
            .account_level_diagnostic
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX account-level diagnostic lock poisoned"))?;
        let changed = observed
            .as_ref()
            .is_some_and(|current| current.value() != snapshot.value());
        *observed = Some(snapshot.clone());
        drop(observed);
        if changed {
            self.revoke_capability_generations()?;
            bail!(
                "OKX account-level diagnostic changed; capability readiness was revoked and requires a fresh complete evidence generation"
            );
        }
        Ok(snapshot)
    }

    fn matching_observed_account_level_diagnostic(
        &self,
        account_config: &OkxAccountConfig,
    ) -> Result<AccountLevelDiagnosticSnapshot> {
        let expected = AccountLevelDiagnostic::parse(&account_config.account_level)?;
        let observed = self
            .account_level_diagnostic
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX account-level diagnostic lock poisoned"))?;
        let Some(snapshot) = observed.as_ref() else {
            drop(observed);
            #[cfg(test)]
            {
                return self.observe_account_level_diagnostic(account_config);
            }
            #[cfg(not(test))]
            bail!(
                "OKX account-level diagnostic was not observed at the authenticated REST response boundary"
            );
        };
        if snapshot.value() != expected {
            drop(observed);
            self.revoke_capability_generations()?;
            bail!(
                "OKX account-level diagnostic changed before capability validation; readiness was revoked and requires a fresh complete evidence generation"
            );
        }
        Ok(snapshot.clone())
    }

    fn revoke_capability_generations(&self) -> Result<()> {
        self.validated_capability_generations
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX validated capability cache lock poisoned"))?
            .clear();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn prepare_account_spot_trade_quote_currency(
        &self,
        inst_id: &str,
    ) -> Result<String> {
        let (base_ccy, quote_ccy) = spot_instrument_currencies(inst_id)?;
        let query = okx_query(&[("instType", OKX_SPOT_INST_TYPE), ("instId", inst_id)]);
        let mut instruments = self
            .private_request::<Vec<OkxAccountSpotTradeQuoteInstrument>, EmptyBody>(
                Method::GET,
                "/api/v5/account/instruments",
                Some(&query),
                None,
            )
            .await?;
        ensure!(
            instruments.len() == 1,
            "OKX account instruments returned {} rows for exact {inst_id}; no account-valid tradeQuoteCcy is available",
            instruments.len()
        );
        let instrument = instruments.remove(0);
        ensure!(
            instrument.inst_id == inst_id
                && instrument.inst_type == OKX_SPOT_INST_TYPE
                && instrument.state == "live"
                && instrument.base_ccy == base_ccy
                && instrument.quote_ccy == quote_ccy,
            "OKX account instrument contract for {inst_id} contradicts the configured live SPOT identity"
        );
        let trade_quote_currency = instrument
            .trade_quote_currencies
            .iter()
            .find(|currency| currency.as_str() == quote_ccy)
            .with_context(|| {
                format!(
                    "OKX account instrument {inst_id} tradeQuoteCcyList {:?} does not admit configured quote {quote_ccy}",
                    instrument.trade_quote_currencies
                )
            })?
            .clone();
        let mut currencies = self
            .test_account_spot_trade_quote_currencies
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX test trade quote cache lock poisoned"))?;
        if let Some(existing) = currencies.get(inst_id) {
            ensure!(
                existing == &trade_quote_currency,
                "OKX account tradeQuoteCcy for {inst_id} changed during test preparation"
            );
        }
        currencies.insert(inst_id.to_owned(), trade_quote_currency.clone());
        Ok(trade_quote_currency)
    }

    #[cfg(test)]
    pub(crate) fn remember_account_spot_trade_quote_currency(
        &self,
        inst_id: &str,
        trade_quote_currency: &str,
    ) -> Result<()> {
        ensure!(
            !trade_quote_currency.trim().is_empty(),
            "OKX test tradeQuoteCcy must not be empty"
        );
        self.test_account_spot_trade_quote_currencies
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX test trade quote cache lock poisoned"))?
            .insert(inst_id.to_owned(), trade_quote_currency.to_owned());
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn maximum_spot_order_size(
        &self,
        inst_id: &str,
        price: &str,
        trade_quote_currency: &str,
    ) -> Result<OkxMaximumOrderSize> {
        self.maximum_order_size(inst_id, "cash", price, trade_quote_currency)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn maximum_spot_available_size(
        &self,
        inst_id: &str,
        trade_quote_currency: &str,
    ) -> Result<OkxMaximumAvailableSize> {
        self.maximum_available_size(inst_id, "cash", trade_quote_currency)
            .await
    }

    pub(crate) async fn selection_ticker(&self, inst_id: &str) -> Result<OkxSelectionTicker> {
        let path = "/api/v5/market/ticker";
        let query = okx_query(&[("instId", inst_id)]);
        let mut tickers = self
            .public_get::<OkxSelectionTicker>(path, Some(&query))
            .await?;
        ensure!(
            tickers.len() == 1,
            "OKX returned {} selection tickers for {inst_id}",
            tickers.len()
        );
        let ticker = tickers.remove(0);
        ensure!(
            ticker.inst_id == inst_id,
            "OKX returned selection ticker {} for requested {inst_id}",
            ticker.inst_id
        );
        Ok(ticker)
    }

    pub(crate) async fn selection_order_book(
        &self,
        inst_id: &str,
        depth: usize,
    ) -> Result<OkxSelectionOrderBook> {
        ensure!(
            (1..=400).contains(&depth),
            "OKX selection order-book depth must be between 1 and 400"
        );
        let path = "/api/v5/market/books";
        let depth = depth.to_string();
        let query = okx_query(&[("instId", inst_id), ("sz", &depth)]);
        let mut books = self
            .public_get::<OkxSelectionOrderBook>(path, Some(&query))
            .await?;
        ensure!(
            books.len() == 1,
            "OKX returned {} selection order books for {inst_id}",
            books.len()
        );
        Ok(books.remove(0))
    }

    pub async fn open_orders(&self, inst_id: &str) -> Result<Vec<OkxOrder>> {
        let inst_type = self.validated_instrument_type(inst_id)?;
        let mut orders = Vec::new();
        let mut seen_order_ids = HashSet::new();
        let mut after = None;

        for _ in 0..OKX_OPEN_ORDERS_MAX_PAGES {
            let query = open_orders_query(&inst_type, inst_id, after.as_deref());
            let page = self
                .private_request::<Vec<OkxOrder>, EmptyBody>(
                    Method::GET,
                    "/api/v5/trade/orders-pending",
                    Some(&query),
                    None,
                )
                .await?;
            ensure_orders_match_instrument(&page, inst_id, "open orders")?;

            if page.is_empty() {
                return Ok(orders);
            }

            let page_is_full = page.len() == OKX_OPEN_ORDERS_PAGE_LIMIT;
            let next_after = if page_is_full {
                Some(
                    page.last()
                        .context("OKX open orders page unexpectedly empty")?
                        .order_id
                        .clone(),
                )
            } else {
                None
            };

            for order in page {
                let order_id = order.order_id.trim();
                ensure!(
                    !order_id.is_empty(),
                    "OKX open orders returned order with empty ordId for {inst_id}"
                );
                if seen_order_ids.insert(order.order_id.clone()) {
                    orders.push(order);
                }
            }

            let Some(next_after) = next_after else {
                return Ok(orders);
            };
            ensure!(
                !next_after.trim().is_empty(),
                "OKX open orders page is missing pagination cursor for {inst_id}"
            );
            ensure!(
                after.as_deref() != Some(next_after.as_str()),
                "OKX open orders pagination cursor repeated for {inst_id}: {next_after}"
            );
            after = Some(next_after);
        }

        bail!(
            "OKX open orders pagination exceeded {OKX_OPEN_ORDERS_MAX_PAGES} pages for {inst_id}; refusing to use partial open orders"
        )
    }

    pub async fn order_history(&self, inst_id: &str) -> Result<Vec<OkxOrder>> {
        let mut orders = Vec::new();
        let mut seen_order_ids = HashSet::new();

        self.append_order_history_endpoint(
            inst_id,
            "/api/v5/trade/orders-history",
            "order history",
            &mut orders,
            &mut seen_order_ids,
        )
        .await?;
        self.append_order_history_endpoint(
            inst_id,
            "/api/v5/trade/orders-history-archive",
            "order history archive",
            &mut orders,
            &mut seen_order_ids,
        )
        .await?;

        Ok(orders)
    }

    async fn append_order_history_endpoint(
        &self,
        inst_id: &str,
        path: &str,
        context: &str,
        orders: &mut Vec<OkxOrder>,
        seen_order_ids: &mut HashSet<String>,
    ) -> Result<()> {
        let inst_type = self.validated_instrument_type(inst_id)?;
        let mut after = None;

        for _ in 0..OKX_ORDER_HISTORY_MAX_PAGES {
            let query = order_history_query(&inst_type, inst_id, after.as_deref());
            let page = self
                .private_request::<Vec<OkxOrder>, EmptyBody>(Method::GET, path, Some(&query), None)
                .await?;
            ensure_orders_match_instrument(&page, inst_id, context)?;

            if page.is_empty() {
                return Ok(());
            }

            let page_is_full = page.len() == OKX_ORDER_HISTORY_PAGE_LIMIT;
            let next_after = if page_is_full {
                Some(
                    page.last()
                        .context("OKX order history page unexpectedly empty")?
                        .order_id
                        .clone(),
                )
            } else {
                None
            };

            for order in page {
                let order_id = order.order_id.trim();
                ensure!(
                    !order_id.is_empty(),
                    "OKX {context} returned order with empty ordId for {inst_id}"
                );
                if seen_order_ids.insert(order.order_id.clone()) {
                    orders.push(order);
                }
            }

            let Some(next_after) = next_after else {
                return Ok(());
            };
            ensure!(
                !next_after.trim().is_empty(),
                "OKX {context} page is missing pagination cursor for {inst_id}"
            );
            ensure!(
                after.as_deref() != Some(next_after.as_str()),
                "OKX {context} pagination cursor repeated for {inst_id}: {next_after}"
            );
            after = Some(next_after);
        }

        bail!(
            "OKX {context} pagination exceeded {OKX_ORDER_HISTORY_MAX_PAGES} pages for {inst_id}; refusing to use partial history"
        );
    }

    pub async fn order_fills(&self, inst_id: &str) -> Result<Vec<OkxFill>> {
        let mut fills = Vec::new();
        let mut seen_fill_ids = HashSet::new();

        self.append_order_fills_endpoint(
            inst_id,
            "/api/v5/trade/fills",
            "order fills",
            &mut fills,
            &mut seen_fill_ids,
        )
        .await?;
        self.append_order_fills_endpoint(
            inst_id,
            "/api/v5/trade/fills-history",
            "order fills history",
            &mut fills,
            &mut seen_fill_ids,
        )
        .await?;

        self.observe_private_fill_hints(inst_id, &fills);
        Ok(fills)
    }

    async fn append_order_fills_endpoint(
        &self,
        inst_id: &str,
        path: &str,
        context: &str,
        fills: &mut Vec<OkxFill>,
        seen_fill_ids: &mut HashSet<String>,
    ) -> Result<()> {
        let inst_type = self.validated_instrument_type(inst_id)?;
        let mut after = None;

        for _ in 0..OKX_ORDER_FILLS_MAX_PAGES {
            let query = order_fills_query(&inst_type, inst_id, after.as_deref());
            let page = self
                .private_request::<Vec<OkxFill>, EmptyBody>(Method::GET, path, Some(&query), None)
                .await?;
            ensure_fills_match_instrument(&page, inst_id, context)?;

            if page.is_empty() {
                return Ok(());
            }

            let page_is_full = page.len() == OKX_ORDER_FILLS_PAGE_LIMIT;
            let next_after = if page_is_full {
                Some(
                    page.last()
                        .context("OKX order fills page unexpectedly empty")?
                        .bill_id
                        .clone(),
                )
            } else {
                None
            };

            for fill in page {
                let key = fill.dedupe_key();
                ensure!(
                    !key.trim().is_empty(),
                    "OKX {context} returned fill with no stable identity for {inst_id}"
                );
                if seen_fill_ids.insert(key) {
                    fills.push(fill);
                }
            }

            let Some(next_after) = next_after else {
                return Ok(());
            };
            ensure!(
                !next_after.trim().is_empty(),
                "OKX {context} page is missing billId pagination cursor for {inst_id}"
            );
            ensure!(
                after.as_deref() != Some(next_after.as_str()),
                "OKX {context} pagination cursor repeated for {inst_id}: {next_after}"
            );
            after = Some(next_after);
        }

        bail!(
            "OKX {context} pagination exceeded {OKX_ORDER_FILLS_MAX_PAGES} pages for {inst_id}; refusing to use partial fills"
        );
    }

    pub async fn open_algo_orders(&self, inst_id: &str) -> Result<Vec<OkxAlgoOrder>> {
        let inst_type = self.validated_instrument_type(inst_id)?;
        let mut orders = Vec::new();
        let mut seen_algo_ids = HashSet::new();
        let mut after = None;

        for _ in 0..OKX_OPEN_ALGO_ORDERS_MAX_PAGES {
            let query = open_algo_orders_query(&inst_type, inst_id, after.as_deref());
            let page = self
                .private_request::<Vec<OkxAlgoOrder>, EmptyBody>(
                    Method::GET,
                    "/api/v5/trade/orders-algo-pending",
                    Some(&query),
                    None,
                )
                .await?;
            ensure_algo_orders_match_instrument(&page, inst_id, "open algo orders")?;

            if page.is_empty() {
                self.observe_private_algo_order_hints(inst_id, &orders, "open algo orders");
                return Ok(orders);
            }

            let page_is_full = page.len() == OKX_OPEN_ALGO_ORDERS_PAGE_LIMIT;
            let next_after = if page_is_full {
                Some(
                    page.last()
                        .context("OKX open algo orders page unexpectedly empty")?
                        .algo_id
                        .clone(),
                )
            } else {
                None
            };

            for order in page {
                let algo_id = order.algo_id.trim();
                ensure!(
                    !algo_id.is_empty(),
                    "OKX open algo orders returned algo with empty algoId for {inst_id}"
                );
                if seen_algo_ids.insert(order.algo_id.clone()) {
                    orders.push(order);
                }
            }

            let Some(next_after) = next_after else {
                self.observe_private_algo_order_hints(inst_id, &orders, "open algo orders");
                return Ok(orders);
            };
            ensure!(
                !next_after.trim().is_empty(),
                "OKX open algo orders page is missing pagination cursor for {inst_id}"
            );
            ensure!(
                after.as_deref() != Some(next_after.as_str()),
                "OKX open algo orders pagination cursor repeated for {inst_id}: {next_after}"
            );
            after = Some(next_after);
        }

        bail!(
            "OKX open algo orders pagination exceeded {OKX_OPEN_ALGO_ORDERS_MAX_PAGES} pages for {inst_id}; refusing to use partial open algo orders"
        )
    }

    pub async fn algo_order_history(&self, inst_id: &str) -> Result<Vec<OkxAlgoOrder>> {
        let mut orders = Vec::new();
        let mut seen_algo_ids = HashSet::new();

        for state in OKX_ALGO_HISTORY_STATES {
            self.append_algo_order_history_state(inst_id, state, &mut orders, &mut seen_algo_ids)
                .await?;
        }

        self.observe_private_algo_order_hints(inst_id, &orders, "algo order history");
        Ok(orders)
    }

    async fn append_algo_order_history_state(
        &self,
        inst_id: &str,
        state: &str,
        orders: &mut Vec<OkxAlgoOrder>,
        seen_algo_ids: &mut HashSet<String>,
    ) -> Result<()> {
        let inst_type = self.validated_instrument_type(inst_id)?;
        let mut after = None;

        for _ in 0..OKX_ALGO_HISTORY_MAX_PAGES {
            let query = algo_order_history_query(
                &inst_type,
                inst_id,
                AlgoHistoryFilter::State(state),
                after.as_deref(),
            );
            let page = self
                .private_request::<Vec<OkxAlgoOrder>, EmptyBody>(
                    Method::GET,
                    "/api/v5/trade/orders-algo-history",
                    Some(&query),
                    None,
                )
                .await?;
            ensure_algo_orders_match_instrument(&page, inst_id, "algo order history")?;

            if page.is_empty() {
                return Ok(());
            }

            let page_is_full = page.len() == OKX_ALGO_HISTORY_PAGE_LIMIT;
            let next_after = if page_is_full {
                Some(
                    page.last()
                        .context("OKX algo order history page unexpectedly empty")?
                        .algo_id
                        .clone(),
                )
            } else {
                None
            };

            for order in page {
                if seen_algo_ids.insert(order.algo_id.clone()) {
                    orders.push(order);
                }
            }

            let Some(next_after) = next_after else {
                return Ok(());
            };
            ensure!(
                !next_after.trim().is_empty(),
                "OKX algo order history {state} page is missing pagination cursor for {inst_id}"
            );
            ensure!(
                after.as_deref() != Some(next_after.as_str()),
                "OKX algo order history {state} pagination cursor repeated for {inst_id}: {next_after}"
            );
            after = Some(next_after);
        }

        bail!(
            "OKX algo order history pagination exceeded {OKX_ALGO_HISTORY_MAX_PAGES} {state} pages for {inst_id}; refusing to use partial history"
        );
    }

    pub async fn place_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        kind: OrderKind,
        size: &str,
        price: Option<&str>,
        client_order_id: &str,
    ) -> Result<OkxOrderAck> {
        let (td_mode, trade_quote_currency) = self.validated_order_route(inst_id)?;
        let market_order = kind == OrderKind::Market;
        let request = PlaceOrderRequest {
            inst_id,
            td_mode,
            side: side.as_okx(),
            order_type: kind.as_okx(),
            sz: size,
            price,
            target_currency: spot_market_target_currency(side, kind),
            trade_quote_currency: &trade_quote_currency,
            ban_amend: market_order.then_some(OKX_SPOT_MARKET_BAN_AMEND),
            slippage_pct: market_order.then_some(OKX_SPOT_MARKET_SLIPPAGE_PCT),
            price_amend_type: OKX_PRICE_AMEND_TYPE_REJECT,
            tag: OKX_CANCEL_ALL_AFTER_TAG,
            client_order_id,
        };
        let requested = OkxOrderSubmitReconciliation {
            inst_id,
            side,
            kind,
            size,
            price,
            client_order_id,
        };
        let acknowledgements = match if market_order {
            self.ensure_fresh_instrument_hint_matches_snapshot(inst_id)?;
            self.private_regular_order_request("/api/v5/trade/order", &request)
                .await
        } else {
            let price = price.context("OKX limit/post-only order requires px")?;
            let rate_limit_bucket = self
                .reserve_private_request(&Method::POST, "/api/v5/trade/order", None, Some(&request))
                .await?;
            self.ensure_fresh_instrument_hint_matches_snapshot(inst_id)?;
            self.ensure_fresh_spot_order_price(inst_id, side, price, "OKX order px")
                .await?;
            self.private_regular_order_request_after_reservation(
                "/api/v5/trade/order",
                &request,
                rate_limit_bucket,
            )
            .await
        } {
            Ok(acknowledgements) => acknowledgements,
            Err(err) => {
                return self.reconcile_order_submit_failure(requested, err).await;
            }
        };
        let acknowledgement = match single_order_ack(acknowledgements, client_order_id, "order") {
            Ok(acknowledgement) => acknowledgement,
            Err(err) => {
                return self.reconcile_order_submit_failure(requested, err).await;
            }
        };
        if acknowledgement.status_code == "0" {
            return Ok(acknowledgement);
        }
        self.reconcile_order_submit_failure(requested, order_ack_rejection(acknowledgement))
            .await
    }

    pub async fn cancel_order(&self, inst_id: &str, client_order_id: &str) -> Result<()> {
        self.validated_order_route(inst_id)?;
        let request = CancelOrderRequest {
            inst_id,
            client_order_id,
        };
        let acknowledgements = match self
            .private_order_mutation_request(
                Method::POST,
                "/api/v5/trade/cancel-order",
                None,
                Some(&request),
                PrivateRequestExpiry::None,
            )
            .await
        {
            Ok(acknowledgements) => acknowledgements,
            Err(error) if has_okx_api_error_code(&error, "1") => {
                let resolved = self
                    .order_cancel_target_is_terminal_or_missing(inst_id, client_order_id)
                    .await
                    .with_context(|| {
                        format!(
                            "OKX cancel {client_order_id} returned an aggregate API error and REST reconciliation failed"
                        )
                    })?;
                if resolved {
                    return Ok(());
                }
                return Err(error).with_context(|| {
                    format!(
                        "OKX cancel {client_order_id} returned an aggregate API error while REST still reports the order live"
                    )
                });
            }
            Err(error) => return Err(error),
        };
        let acknowledgement = single_order_ack(acknowledgements, client_order_id, "cancel")?;
        if acknowledgement.status_code == "0" {
            return Ok(());
        }
        if self
            .order_cancel_target_is_terminal_or_missing(inst_id, client_order_id)
            .await?
        {
            return Ok(());
        }
        Err(order_ack_rejection(acknowledgement))
    }

    pub async fn amend_order(&self, amend: OkxOrderAmend<'_>) -> Result<OkxOrderAck> {
        self.validated_order_route(amend.inst_id)?;
        amend.validate()?;
        let request = AmendOrderRequest {
            inst_id: amend.inst_id,
            client_order_id: amend.client_order_id,
            new_size: amend.new_size,
            new_price: amend.new_price,
            price_amend_type: OKX_PRICE_AMEND_TYPE_REJECT,
        };
        let acknowledgements = match if let Some(new_price) = amend.new_price {
            let rate_limit_bucket = self
                .reserve_private_request(
                    &Method::POST,
                    "/api/v5/trade/amend-order",
                    None,
                    Some(&request),
                )
                .await?;
            self.ensure_fresh_instrument_hint_matches_snapshot(amend.inst_id)?;
            self.ensure_fresh_spot_order_price(
                amend.inst_id,
                amend.side,
                new_price,
                "OKX amend newPx",
            )
            .await?;
            self.private_order_mutation_request_after_reservation(
                Method::POST,
                "/api/v5/trade/amend-order",
                None,
                Some(&request),
                PrivateRequestExpiry::TradeCommand,
                rate_limit_bucket,
            )
            .await
        } else {
            self.ensure_fresh_instrument_hint_matches_snapshot(amend.inst_id)?;
            self.private_order_mutation_request(
                Method::POST,
                "/api/v5/trade/amend-order",
                None,
                Some(&request),
                PrivateRequestExpiry::TradeCommand,
            )
            .await
        } {
            Ok(acknowledgements) => acknowledgements,
            Err(err) => return self.reconcile_order_amend_failure(amend, err).await,
        };
        let acknowledgement = single_order_ack(acknowledgements, amend.client_order_id, "amend")?;
        if acknowledgement.status_code != "0" {
            return self
                .reconcile_order_amend_failure(amend, order_ack_rejection(acknowledgement))
                .await;
        }
        match self.order(amend.inst_id, amend.client_order_id).await {
            Ok(Some(order)) => {
                verify_reconciled_order_amend(&order, amend).map_err(|verify_err| {
                    anyhow::anyhow!(
                        "OKX order {} amend was accepted but confirmation lookup did not confirm the requested amendment: {verify_err:#}",
                        amend.client_order_id
                    )
                })?;
                Ok(reconciled_order_ack(order))
            }
            Ok(None) => bail!(
                "OKX order {} amend was accepted but confirmation lookup did not find the order",
                amend.client_order_id
            ),
            Err(lookup_err) => Err(lookup_err).context(format!(
                "OKX order {} amend was accepted but confirmation lookup failed",
                amend.client_order_id
            )),
        }
    }

    pub async fn place_trigger_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        size: &str,
        trigger_price: &str,
        client_order_id: &str,
    ) -> Result<OkxAlgoOrderAck> {
        let (td_mode, trade_quote_currency) = self.validated_order_route(inst_id)?;
        self.ensure_fresh_instrument_hint_matches_snapshot(inst_id)?;
        let request = PlaceTriggerOrderRequest {
            inst_id,
            td_mode,
            side: side.as_okx(),
            order_type: "trigger",
            sz: size,
            trigger_price,
            trigger_price_type: "last",
            order_price: "-1",
            trade_quote_currency: &trade_quote_currency,
            tag: OKX_CANCEL_ALL_AFTER_TAG,
            client_order_id,
        };
        let acknowledgements = match self
            .private_algo_mutation_request(
                Method::POST,
                "/api/v5/trade/order-algo",
                None,
                Some(&request),
                PrivateRequestExpiry::None,
            )
            .await
        {
            Ok(acknowledgements) => acknowledgements,
            Err(err) => {
                return self
                    .reconcile_algo_submit_failure(
                        inst_id,
                        side,
                        size,
                        trigger_price,
                        client_order_id,
                        err,
                    )
                    .await;
            }
        };
        let acknowledgement = single_algo_ack(
            acknowledgements,
            AlgoAckIdentity::ClientOrderId(client_order_id),
            "algo order",
        )?;
        if acknowledgement.status_code == "0" {
            return Ok(acknowledgement);
        }
        let err = anyhow::anyhow!(
            "OKX algo order {client_order_id} rejected: {} {}",
            acknowledgement.status_code,
            acknowledgement.status_message
        );
        if is_okx_duplicate_algo_client_order_id_code(&acknowledgement.status_code) {
            return self
                .reconcile_algo_submit_failure(
                    inst_id,
                    side,
                    size,
                    trigger_price,
                    client_order_id,
                    err,
                )
                .await;
        }
        Err(err)
    }

    #[cfg(test)]
    pub(crate) async fn place_spot_oco(
        &self,
        protection: OkxOcoProtection<'_>,
    ) -> Result<OkxAlgoOrderAck> {
        validate_oco_protection(protection)?;
        let (td_mode, trade_quote_currency) = self.validated_order_route(protection.inst_id)?;
        self.ensure_fresh_instrument_hint_matches_snapshot(protection.inst_id)?;
        let request = PlaceOcoOrderRequest {
            inst_id: protection.inst_id,
            td_mode,
            side: "sell",
            order_type: "oco",
            sz: protection.size,
            take_profit_trigger_price: protection.take_profit_trigger_price,
            take_profit_trigger_price_type: "last",
            take_profit_order_price: "-1",
            stop_loss_trigger_price: protection.stop_loss_trigger_price,
            stop_loss_trigger_price_type: "last",
            stop_loss_order_price: "-1",
            trade_quote_currency: &trade_quote_currency,
            tag: OKX_CANCEL_ALL_AFTER_TAG,
            client_order_id: protection.client_order_id,
        };
        let acknowledgements = match self
            .private_algo_mutation_request(
                Method::POST,
                "/api/v5/trade/order-algo",
                None,
                Some(&request),
                PrivateRequestExpiry::None,
            )
            .await
        {
            Ok(acknowledgements) => acknowledgements,
            Err(submit_error) => {
                return self
                    .reconcile_oco_submit_failure(protection, submit_error)
                    .await;
            }
        };
        let acknowledgement = single_algo_ack(
            acknowledgements,
            AlgoAckIdentity::ClientOrderId(protection.client_order_id),
            "SPOT OCO",
        )?;
        if acknowledgement.status_code == "0" {
            return Ok(acknowledgement);
        }
        let submit_error = anyhow::anyhow!(
            "OKX SPOT OCO {} rejected: {} {}",
            protection.client_order_id,
            acknowledgement.status_code,
            acknowledgement.status_message
        );
        if is_okx_duplicate_algo_client_order_id_code(&acknowledgement.status_code) {
            return self
                .reconcile_oco_submit_failure(protection, submit_error)
                .await;
        }
        Err(submit_error)
    }

    #[cfg(test)]
    pub(crate) async fn oco_order_by_client_order_id(
        &self,
        inst_id: &str,
        client_order_id: &str,
    ) -> Result<Option<OkxOcoOrder>> {
        self.oco_order_detail(inst_id, "algoClOrdId", client_order_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn oco_order_by_algo_id(
        &self,
        inst_id: &str,
        algo_id: &str,
    ) -> Result<Option<OkxOcoOrder>> {
        self.oco_order_detail(inst_id, "algoId", algo_id).await
    }

    #[cfg(test)]
    async fn oco_order_detail(
        &self,
        inst_id: &str,
        query_field: &str,
        query_value: &str,
    ) -> Result<Option<OkxOcoOrder>> {
        ensure!(
            matches!(query_field, "algoId" | "algoClOrdId") && !query_value.trim().is_empty(),
            "OKX OCO detail requires one documented non-empty identifier"
        );
        let query = okx_query(&[(query_field, query_value)]);
        let mut orders = self
            .private_request::<Vec<OkxOcoOrder>, EmptyBody>(
                Method::GET,
                "/api/v5/trade/order-algo",
                Some(&query),
                None,
            )
            .await?;
        if orders.is_empty() {
            return Ok(None);
        }
        ensure!(
            orders.len() == 1,
            "OKX returned {} OCO rows for {query_field}={query_value}",
            orders.len()
        );
        let order = orders.remove(0);
        ensure_oco_order_matches(&order, inst_id, "OCO detail")?;
        match query_field {
            "algoId" => ensure!(
                order.algo_id == query_value,
                "OKX returned OCO {} for requested algoId {query_value}",
                order.algo_id
            ),
            "algoClOrdId" => ensure!(
                order.client_order_id == query_value,
                "OKX returned OCO {} with algoClOrdId {} for requested {query_value}",
                order.algo_id,
                order.client_order_id
            ),
            _ => unreachable!("query field was validated"),
        }
        Ok(Some(order))
    }

    #[cfg(test)]
    pub(crate) async fn open_spot_oco_orders(&self, inst_id: &str) -> Result<Vec<OkxOcoOrder>> {
        let inst_type = self.validated_instrument_type(inst_id)?;
        let query = okx_query(&[
            ("ordType", "oco"),
            ("instType", &inst_type),
            ("instId", inst_id),
            ("limit", "100"),
        ]);
        let orders = self
            .private_request::<Vec<OkxOcoOrder>, EmptyBody>(
                Method::GET,
                "/api/v5/trade/orders-algo-pending",
                Some(&query),
                None,
            )
            .await?;
        ensure!(
            orders.len() < 100,
            "OKX open OCO response reached the 100-row limit for {inst_id}; refusing potentially incomplete state"
        );
        for order in &orders {
            ensure_oco_order_matches(order, inst_id, "open OCO orders")?;
            ensure!(
                order.is_pending(),
                "OKX open OCO endpoint returned non-pending state {:?} for {}",
                order.state,
                order.algo_id
            );
        }
        Ok(orders)
    }

    #[cfg(test)]
    pub(crate) async fn oco_history_by_algo_id(
        &self,
        inst_id: &str,
        algo_id: &str,
    ) -> Result<Option<OkxOcoOrder>> {
        let inst_type = self.validated_instrument_type(inst_id)?;
        let query = okx_query(&[
            ("ordType", "oco"),
            ("instType", &inst_type),
            ("instId", inst_id),
            ("algoId", algo_id),
            ("limit", "100"),
        ]);
        let mut orders = self
            .private_request::<Vec<OkxOcoOrder>, EmptyBody>(
                Method::GET,
                "/api/v5/trade/orders-algo-history",
                Some(&query),
                None,
            )
            .await?;
        ensure!(
            orders.len() <= 1,
            "OKX returned {} OCO history rows for {algo_id}",
            orders.len()
        );
        let Some(order) = orders.pop() else {
            return Ok(None);
        };
        ensure_oco_order_matches(&order, inst_id, "OCO history")?;
        ensure!(
            order.algo_id == algo_id && order.is_terminal(),
            "OKX OCO history returned algo {} in state {:?} for requested {algo_id}",
            order.algo_id,
            order.state
        );
        Ok(Some(order))
    }

    #[cfg(test)]
    pub(crate) async fn amend_spot_oco(&self, amend: OkxOcoAmend<'_>) -> Result<OkxAlgoOrderAck> {
        validate_oco_amend(amend)?;
        let request = AmendOcoOrderRequest {
            inst_id: amend.inst_id,
            algo_id: amend.algo_id,
            client_order_id: amend.client_order_id,
            cancel_on_fail: true,
            new_size: amend.new_size,
            new_take_profit_trigger_price: amend.new_take_profit_trigger_price,
            new_take_profit_trigger_price_type: "last",
            new_take_profit_order_price: "-1",
            new_stop_loss_trigger_price: amend.new_stop_loss_trigger_price,
            new_stop_loss_trigger_price_type: "last",
            new_stop_loss_order_price: "-1",
        };
        let acknowledgements = self
            .private_algo_mutation_request(
                Method::POST,
                "/api/v5/trade/amend-algos",
                None,
                Some(&request),
                PrivateRequestExpiry::None,
            )
            .await?;
        let acknowledgement = single_algo_ack(
            acknowledgements,
            AlgoAckIdentity::AlgoId(amend.algo_id),
            "amend SPOT OCO",
        )?;
        ensure!(
            acknowledgement.status_code == "0",
            "OKX SPOT OCO {} amend rejected: {} {}",
            amend.algo_id,
            acknowledgement.status_code,
            acknowledgement.status_message
        );
        Ok(acknowledgement)
    }

    #[cfg(test)]
    pub(crate) async fn cancel_spot_oco(&self, inst_id: &str, algo_id: &str) -> Result<()> {
        let request = [CancelAlgoOrderRequest { inst_id, algo_id }];
        let acknowledgements = match self
            .private_algo_mutation_request(
                Method::POST,
                "/api/v5/trade/cancel-algos",
                None,
                Some(&request),
                PrivateRequestExpiry::None,
            )
            .await
        {
            Ok(acknowledgements) => acknowledgements,
            Err(error) if has_okx_api_error_code(&error, "1") => {
                return match self
                    .oco_cancel_target_is_terminal_or_missing(inst_id, algo_id)
                    .await
                {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(error).with_context(|| {
                        format!(
                            "OKX cancel SPOT OCO {algo_id} returned an aggregate API error while REST still reports the algo order live"
                        )
                    }),
                    Err(reconciliation_error) => Err(error).with_context(|| {
                        format!(
                            "OKX cancel SPOT OCO {algo_id} returned an aggregate API error and REST reconciliation failed: {reconciliation_error:#}"
                        )
                    }),
                };
            }
            Err(error) => return Err(error),
        };
        let acknowledgement = single_algo_ack(
            acknowledgements,
            AlgoAckIdentity::AlgoId(algo_id),
            "cancel SPOT OCO",
        )?;
        if acknowledgement.status_code == "0" {
            return Ok(());
        }
        match self.oco_order_by_algo_id(inst_id, algo_id).await? {
            Some(order) if order.is_terminal() => return Ok(()),
            Some(_) => {}
            None => {
                if self
                    .oco_history_by_algo_id(inst_id, algo_id)
                    .await?
                    .is_some_and(|order| order.is_terminal())
                {
                    return Ok(());
                }
            }
        }
        bail!(
            "OKX cancel SPOT OCO {algo_id} rejected: {} {}",
            acknowledgement.status_code,
            acknowledgement.status_message
        )
    }

    pub async fn cancel_algo_order(&self, inst_id: &str, algo_id: &str) -> Result<()> {
        self.validated_order_route(inst_id)?;
        let request = [CancelAlgoOrderRequest { inst_id, algo_id }];
        let acknowledgements = match self
            .private_algo_mutation_request(
                Method::POST,
                "/api/v5/trade/cancel-algos",
                None,
                Some(&request),
                PrivateRequestExpiry::None,
            )
            .await
        {
            Ok(acknowledgements) => acknowledgements,
            Err(error) if has_okx_api_error_code(&error, "1") => {
                return match self
                    .algo_cancel_target_is_terminal_or_missing(inst_id, algo_id)
                    .await
                {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(error).with_context(|| {
                        format!(
                            "OKX cancel algo {algo_id} returned an aggregate API error while REST still reports the algo order live"
                        )
                    }),
                    Err(reconciliation_error) => Err(error).with_context(|| {
                        format!(
                            "OKX cancel algo {algo_id} returned an aggregate API error and REST reconciliation failed: {reconciliation_error:#}"
                        )
                    }),
                };
            }
            Err(error) => return Err(error),
        };
        let acknowledgement = single_algo_ack(
            acknowledgements,
            AlgoAckIdentity::AlgoId(algo_id),
            "cancel algo",
        )?;
        if acknowledgement.status_code == "0" {
            return Ok(());
        }
        if self
            .algo_cancel_target_is_terminal_or_missing(inst_id, algo_id)
            .await?
        {
            return Ok(());
        }
        bail!(
            "OKX cancel algo {algo_id} rejected: {} {}",
            acknowledgement.status_code,
            acknowledgement.status_message
        );
    }

    pub(crate) async fn cancel_all_after(
        &self,
        timeout: OkxCancelAllAfterTimeout,
    ) -> Result<OkxCancelAllAfterAck> {
        let timeout_seconds = timeout.okx_seconds();
        let request = CancelAllAfterRequest {
            timeout_seconds: &timeout_seconds,
            tag: OKX_CANCEL_ALL_AFTER_TAG,
        };
        let mut acknowledgements = self
            .private_request::<Vec<OkxCancelAllAfterAck>, _>(
                Method::POST,
                "/api/v5/trade/cancel-all-after",
                None,
                Some(&request),
            )
            .await?;
        ensure!(
            acknowledgements.len() == 1,
            "OKX returned {} cancel-all-after acknowledgements",
            acknowledgements.len()
        );
        let acknowledgement = acknowledgements.remove(0);
        if timeout.is_disarm() {
            ensure!(
                acknowledgement.trigger_time.trim() == "0",
                "OKX cancel-all-after disarm acknowledgement returned triggerTime {}; expected 0",
                acknowledgement.trigger_time
            );
        } else {
            ensure!(
                !acknowledgement.trigger_time.trim().is_empty(),
                "OKX cancel-all-after acknowledgement omitted triggerTime"
            );
        }
        ensure!(
            !acknowledgement.ts.trim().is_empty(),
            "OKX cancel-all-after acknowledgement omitted ts"
        );
        ensure!(
            acknowledgement.tag.trim() == OKX_CANCEL_ALL_AFTER_TAG,
            "OKX cancel-all-after acknowledgement returned tag {}; expected {OKX_CANCEL_ALL_AFTER_TAG}",
            acknowledgement.tag
        );
        Ok(acknowledgement)
    }

    pub(crate) async fn prepare_websocket_place_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        kind: OrderKind,
        price: Option<&str>,
    ) -> Result<String> {
        ensure!(
            matches!(kind, OrderKind::Limit | OrderKind::PostOnly),
            "OKX WebSocket place preparation supports only limit or post-only orders"
        );
        let price = price.context("OKX WebSocket limit/post-only order requires px")?;
        let rate_limit_bucket =
            okx_rate_limit_bucket(&Method::POST, "/api/v5/trade/order", None, Some(inst_id))?;
        self.rate_limit_pacer.wait(&rate_limit_bucket).await?;
        self.ensure_fresh_instrument_hint_matches_snapshot(inst_id)?;
        self.ensure_fresh_spot_order_price(inst_id, side, price, "OKX WebSocket order px")
            .await?;
        let timing = self.private_request_timing().await?;
        PrivateRequestExpiry::TradeCommand
            .header_value(timing.unix_millis)?
            .context("OKX WebSocket order expTime was not generated")
    }

    pub(crate) async fn prepare_websocket_cancel_order(&self, inst_id: &str) -> Result<()> {
        let rate_limit_bucket = okx_rate_limit_bucket(
            &Method::POST,
            "/api/v5/trade/cancel-order",
            None,
            Some(inst_id),
        )?;
        self.rate_limit_pacer.wait(&rate_limit_bucket).await
    }

    pub(crate) async fn prepare_websocket_amend_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        new_price: Option<&str>,
    ) -> Result<String> {
        let rate_limit_bucket = okx_rate_limit_bucket(
            &Method::POST,
            "/api/v5/trade/amend-order",
            None,
            Some(inst_id),
        )?;
        self.rate_limit_pacer.wait(&rate_limit_bucket).await?;
        self.ensure_fresh_instrument_hint_matches_snapshot(inst_id)?;
        if let Some(new_price) = new_price {
            self.ensure_fresh_spot_order_price(
                inst_id,
                side,
                new_price,
                "OKX WebSocket amend newPx",
            )
            .await?;
        }
        let timing = self.private_request_timing().await?;
        PrivateRequestExpiry::TradeCommand
            .header_value(timing.unix_millis)?
            .context("OKX WebSocket amend expTime was not generated")
    }

    pub(crate) async fn prepare_websocket_order_command_timing(&self) -> Result<()> {
        self.private_request_timing().await?;
        Ok(())
    }

    pub(crate) async fn websocket_login_timestamp(&self) -> Result<String> {
        let timing = self
            .private_request_timing()
            .await
            .context("failed syncing OKX server time for WebSocket login")?;
        websocket_login_timestamp_from_unix_millis(timing.unix_millis)
    }

    pub(crate) async fn economics_preflight_server_time(&self) -> Result<()> {
        self.refresh_server_time().await
    }

    pub(crate) async fn refresh_server_time_if_expiring(&self) -> Result<bool> {
        if !self.server_time_cache_needs_refresh()? {
            return Ok(false);
        }
        self.refresh_server_time().await?;
        Ok(true)
    }

    pub(crate) fn server_time_cache_needs_refresh(&self) -> Result<bool> {
        self.server_time_clock
            .needs_refresh(OKX_SERVER_TIME_REFRESH_MARGIN)
    }

    pub async fn order(&self, inst_id: &str, client_order_id: &str) -> Result<Option<OkxOrder>> {
        let query = okx_query(&[("instId", inst_id), ("clOrdId", client_order_id)]);
        let mut orders = match self
            .private_request::<Vec<OkxOrder>, EmptyBody>(
                Method::GET,
                "/api/v5/trade/order",
                Some(&query),
                None,
            )
            .await
        {
            Ok(orders) => orders,
            Err(error) if has_okx_api_error_code(&error, "51603") => {
                self.observe_private_order_hint(inst_id, client_order_id, None);
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if orders.is_empty() {
            self.observe_private_order_hint(inst_id, client_order_id, None);
            return Ok(None);
        }
        ensure!(
            orders.len() == 1,
            "OKX returned {} orders for {client_order_id}",
            orders.len()
        );
        let order = orders.remove(0);
        ensure_order_matches_instrument(&order, inst_id, "order lookup")?;
        ensure!(
            order.client_order_id == client_order_id,
            "OKX returned order {} with clOrdId {} for requested {client_order_id}",
            order.order_id,
            order.client_order_id
        );
        self.observe_private_order_hint(inst_id, client_order_id, Some(&order));
        Ok(Some(order))
    }

    fn observe_private_order_hint(
        &self,
        inst_id: &str,
        client_order_id: &str,
        rest_order: Option<&OkxOrder>,
    ) {
        let Some(hint) = self.private_event_cache.fresh_order(
            inst_id,
            client_order_id,
            self.market_data_max_staleness,
        ) else {
            return;
        };

        let rest_updated_at_ms = rest_order.map(OkxOrder::updated_at_ms).unwrap_or_default();
        let hint_source_ts_ms = hint.source_ts_ms.unwrap_or_default();
        if rest_order.is_none() || hint_source_ts_ms > rest_updated_at_ms {
            debug!(
                instrument_id = inst_id,
                client_order_id,
                private_hint_state = %hint.order.state,
                private_hint_source_ts_ms = hint_source_ts_ms,
                rest_updated_at_ms,
                rest_found = rest_order.is_some(),
                "observed OKX private order WebSocket hint ahead of REST lookup; keeping REST order lookup authoritative"
            );
        }
    }

    fn observe_private_fill_hints(&self, inst_id: &str, rest_fills: &[OkxFill]) {
        let private_hints = self
            .private_event_cache
            .fresh_fills(inst_id, self.market_data_max_staleness);
        if private_hints.is_empty() {
            return;
        }

        let rest_fill_keys: HashSet<String> = rest_fills.iter().map(OkxFill::dedupe_key).collect();
        let private_hint_count = private_hints.len();
        let unmatched_hint_count = private_hints
            .iter()
            .filter(|hint| !rest_fill_keys.contains(&hint.fill.dedupe_key()))
            .count();
        if unmatched_hint_count > 0 {
            debug!(
                instrument_id = inst_id,
                private_hint_count,
                unmatched_hint_count,
                rest_fill_count = rest_fills.len(),
                "observed OKX private fill WebSocket hints not present in REST fill lookup; keeping REST fills authoritative"
            );
        }
    }

    fn observe_private_algo_order_hints(
        &self,
        inst_id: &str,
        rest_orders: &[OkxAlgoOrder],
        context: &str,
    ) {
        let private_hints = self
            .private_event_cache
            .fresh_algo_orders(inst_id, self.market_data_max_staleness);
        if private_hints.is_empty() {
            return;
        }

        let rest_algo_ids: HashSet<&str> = rest_orders
            .iter()
            .map(|order| order.algo_id.as_str())
            .collect();
        let private_hint_count = private_hints.len();
        let unmatched_hint_count = private_hints
            .iter()
            .filter(|hint| !rest_algo_ids.contains(hint.algo_order.algo_id.as_str()))
            .count();
        if unmatched_hint_count > 0 {
            debug!(
                instrument_id = inst_id,
                context,
                private_hint_count,
                unmatched_hint_count,
                rest_algo_order_count = rest_orders.len(),
                "observed OKX private algo WebSocket hints not present in REST algo lookup; keeping REST algo lookup authoritative"
            );
        }
    }

    fn observe_private_account_hint(&self, rest_balances: &[OkxBalance]) {
        let Some(hint) = self
            .private_event_cache
            .fresh_account(self.market_data_max_staleness)
        else {
            return;
        };

        if !hint.balance.details.iter().eq(rest_balances
            .iter()
            .flat_map(|balance| balance.details.iter()))
        {
            debug!(
                private_hint_currency_count = hint.balance.details.len(),
                rest_balance_row_count = rest_balances.len(),
                private_hint_source_ts_ms = hint.source_ts_ms.unwrap_or_default(),
                "observed OKX private account WebSocket hint that differs from REST balance snapshot; keeping REST balances authoritative"
            );
        }
    }

    async fn algo_order_by_client_order_id(
        &self,
        inst_id: &str,
        client_order_id: &str,
    ) -> Result<Option<OkxAlgoOrder>> {
        let query = okx_query(&[("algoClOrdId", client_order_id)]);
        let mut orders = self
            .private_request::<Vec<OkxAlgoOrder>, EmptyBody>(
                Method::GET,
                "/api/v5/trade/order-algo",
                Some(&query),
                None,
            )
            .await?;
        if orders.is_empty() {
            return Ok(None);
        }
        ensure!(
            orders.len() == 1,
            "OKX returned {} algo orders for {client_order_id}",
            orders.len()
        );
        let order = orders.remove(0);
        ensure_algo_order_matches_instrument(&order, inst_id, "algo order lookup")?;
        ensure!(
            order.client_order_id == client_order_id,
            "OKX returned algo {} with algoClOrdId {} for requested {client_order_id}",
            order.algo_id,
            order.client_order_id
        );
        Ok(Some(order))
    }

    pub(crate) async fn reconcile_order_submit_failure(
        &self,
        requested: OkxOrderSubmitReconciliation<'_>,
        submit_err: anyhow::Error,
    ) -> Result<OkxOrderAck> {
        match self
            .order(requested.inst_id, requested.client_order_id)
            .await
        {
            Ok(Some(order)) => {
                if order.parsed_side() != Some(requested.side) {
                    return Err(submit_err.context(format!(
                        "OKX order {} submit failed and reconciliation lookup returned side {} for requested {}",
                        requested.client_order_id,
                        order.side,
                        requested.side.as_okx()
                    )));
                }
                if order.parsed_kind() != Some(requested.kind) {
                    return Err(submit_err.context(format!(
                        "OKX order {} submit failed and reconciliation lookup returned ordType {} for requested {}",
                        requested.client_order_id,
                        order.order_type,
                        requested.kind.as_okx()
                    )));
                }
                let actual_size = order.requested_size()?;
                let expected_size =
                    parse_positive_decimal_field("OKX submit expected sz", requested.size)?;
                if actual_size != expected_size {
                    return Err(submit_err.context(format!(
                        "OKX order {} submit failed and reconciliation lookup returned sz {} for requested sz {}",
                        requested.client_order_id,
                        order.sz,
                        requested.size
                    )));
                }
                if let Some(expected_price) = requested.price {
                    let actual_price =
                        parse_positive_decimal_field("OKX submit reconciled px", &order.price)?;
                    let expected_price_decimal =
                        parse_positive_decimal_field("OKX submit expected px", expected_price)?;
                    if actual_price != expected_price_decimal {
                        return Err(submit_err.context(format!(
                            "OKX order {} submit failed and reconciliation lookup returned px {} for requested px {}",
                            requested.client_order_id,
                            order.price,
                            expected_price
                        )));
                    }
                }
                Ok(reconciled_order_ack(order))
            }
            Ok(None) => Err(submit_err.context(format!(
                "OKX order {} submit failed and reconciliation lookup did not find the order",
                requested.client_order_id
            ))),
            Err(lookup_err) => Err(submit_err.context(format!(
                "OKX order {} submit failed and reconciliation lookup failed: {lookup_err:#}",
                requested.client_order_id
            ))),
        }
    }

    pub(crate) async fn reconcile_order_amend_failure(
        &self,
        amend: OkxOrderAmend<'_>,
        amend_err: anyhow::Error,
    ) -> Result<OkxOrderAck> {
        match self.order(amend.inst_id, amend.client_order_id).await {
            Ok(Some(order)) => {
                verify_reconciled_order_amend(&order, amend).map_err(|verify_err| {
                    amend_err.context(format!(
                        "OKX order {} amend failed and reconciliation lookup did not confirm the requested amendment: {verify_err:#}",
                        amend.client_order_id
                    ))
                })?;
                Ok(reconciled_order_ack(order))
            }
            Ok(None) => Err(amend_err.context(format!(
                "OKX order {} amend failed and reconciliation lookup did not find the order",
                amend.client_order_id
            ))),
            Err(lookup_err) => Err(amend_err.context(format!(
                "OKX order {} amend failed and reconciliation lookup failed: {lookup_err:#}",
                amend.client_order_id
            ))),
        }
    }

    async fn reconcile_algo_submit_failure(
        &self,
        inst_id: &str,
        side: OrderSide,
        size: &str,
        trigger_price: &str,
        client_order_id: &str,
        submit_err: anyhow::Error,
    ) -> Result<OkxAlgoOrderAck> {
        match self
            .algo_order_by_client_order_id(inst_id, client_order_id)
            .await
        {
            Ok(Some(order)) => {
                if !order.is_live() {
                    return Err(submit_err.context(format!(
                        "OKX algo order {client_order_id} submit failed and reconciliation lookup returned state {} instead of live protection",
                        order.state
                    )));
                }
                if order.parsed_side() != Some(side) {
                    return Err(submit_err.context(format!(
                        "OKX algo order {client_order_id} submit failed and reconciliation lookup returned side {} for requested {}",
                        order.side,
                        side.as_okx()
                    )));
                }
                if !order.is_trigger_market_order() {
                    return Err(submit_err.context(format!(
                        "OKX algo order {client_order_id} submit failed and reconciliation lookup returned ordType {} orderPx {} for requested trigger market order",
                        order.order_type,
                        order.order_price
                    )));
                }
                let requested_size = Decimal::from_str(size)
                    .with_context(|| format!("OKX requested algo size {size} was invalid"))?;
                let requested_trigger_price = Decimal::from_str(trigger_price).with_context(
                    || format!("OKX requested algo triggerPx {trigger_price} was invalid"),
                )?;
                let reconciled_size = order.requested_size()?;
                if reconciled_size != requested_size {
                    return Err(submit_err.context(format!(
                        "OKX algo order {client_order_id} submit failed and reconciliation lookup returned size {reconciled_size} for requested {requested_size}",
                    )));
                }
                let reconciled_trigger_price = order.trigger_price()?;
                if reconciled_trigger_price != requested_trigger_price {
                    return Err(submit_err.context(format!(
                        "OKX algo order {client_order_id} submit failed and reconciliation lookup returned triggerPx {reconciled_trigger_price} for requested {requested_trigger_price}",
                    )));
                }
                Ok(reconciled_algo_ack(order))
            }
            Ok(None) => Err(submit_err.context(format!(
                "OKX algo order {client_order_id} submit failed and reconciliation lookup did not find the algo order"
            ))),
            Err(lookup_err) => Err(submit_err.context(format!(
                "OKX algo order {client_order_id} submit failed and reconciliation lookup failed: {lookup_err:#}"
            ))),
        }
    }

    #[cfg(test)]
    async fn reconcile_oco_submit_failure(
        &self,
        requested: OkxOcoProtection<'_>,
        submit_error: anyhow::Error,
    ) -> Result<OkxAlgoOrderAck> {
        match self
            .oco_order_by_client_order_id(requested.inst_id, requested.client_order_id)
            .await
        {
            Ok(Some(order)) => {
                verify_reconciled_oco(&order, requested).map_err(|verify_error| {
                    submit_error.context(format!(
                        "OKX SPOT OCO {} submit failed and REST reconciliation did not match the request: {verify_error:#}",
                        requested.client_order_id
                    ))
                })?;
                Ok(reconciled_oco_ack(order))
            }
            Ok(None) => Err(submit_error.context(format!(
                "OKX SPOT OCO {} submit failed and REST detail did not find it",
                requested.client_order_id
            ))),
            Err(lookup_error) => Err(submit_error.context(format!(
                "OKX SPOT OCO {} submit failed and REST detail lookup failed: {lookup_error:#}",
                requested.client_order_id
            ))),
        }
    }

    async fn order_cancel_target_is_terminal_or_missing(
        &self,
        inst_id: &str,
        client_order_id: &str,
    ) -> Result<bool> {
        Ok(self
            .order(inst_id, client_order_id)
            .await?
            .is_none_or(|order| order.is_terminal()))
    }

    async fn algo_cancel_target_is_terminal_or_missing(
        &self,
        inst_id: &str,
        algo_id: &str,
    ) -> Result<bool> {
        if self
            .open_algo_orders(inst_id)
            .await?
            .iter()
            .any(|order| order.algo_id == algo_id && order.is_live())
        {
            return Ok(false);
        }

        let Some(order) = self.algo_order_history_by_id(inst_id, algo_id).await? else {
            return Ok(true);
        };
        Ok(order.is_effective() || order.is_terminal_without_execution())
    }

    #[cfg(test)]
    async fn oco_cancel_target_is_terminal_or_missing(
        &self,
        inst_id: &str,
        algo_id: &str,
    ) -> Result<bool> {
        match self.oco_order_by_algo_id(inst_id, algo_id).await? {
            Some(order) => Ok(order.is_terminal()),
            None => Ok(self
                .oco_history_by_algo_id(inst_id, algo_id)
                .await?
                .is_none_or(|order| order.is_terminal())),
        }
    }

    async fn algo_order_history_by_id(
        &self,
        inst_id: &str,
        algo_id: &str,
    ) -> Result<Option<OkxAlgoOrder>> {
        let inst_type = self.validated_instrument_type(inst_id)?;
        let query = algo_order_history_query(
            &inst_type,
            inst_id,
            AlgoHistoryFilter::AlgoId(algo_id),
            /*after*/ None,
        );
        let mut orders = self
            .private_request::<Vec<OkxAlgoOrder>, EmptyBody>(
                Method::GET,
                "/api/v5/trade/orders-algo-history",
                Some(&query),
                None,
            )
            .await?;
        ensure_algo_orders_match_instrument(&orders, inst_id, "algo order history")?;
        if orders.is_empty() {
            return Ok(None);
        }
        ensure!(
            orders.len() == 1,
            "OKX returned {} algo history rows for {algo_id}",
            orders.len()
        );
        let order = orders.remove(0);
        ensure!(
            order.algo_id == algo_id,
            "OKX returned algo history row {} for requested {algo_id}",
            order.algo_id
        );
        Ok(Some(order))
    }

    async fn public_get<T>(&self, path: &str, query: Option<&str>) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let rate_limit_bucket = okx_rate_limit_bucket(&Method::GET, path, query, None)?;
        self.rate_limit_pacer.wait(&rate_limit_bucket).await?;
        let url = self.url(path, query)?;
        let mut request = self.http.get(url);
        if self.simulated_trading {
            request = request.header(OKX_DOCUMENTED_SIMULATED_TRADING, "1");
        }
        let response = request.send().await.context("OKX public request failed")?;
        parse_response(
            response,
            &rate_limit_bucket,
            &self.rate_limit_pacer,
            &self.gateway_latency_recorder,
        )
        .await
    }

    async fn private_request<T, B>(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        body: Option<&B>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + OkxRateLimitBody,
    {
        self.private_request_with_expiry(method, path, query, body, PrivateRequestExpiry::None)
            .await
    }

    async fn private_regular_order_request<B>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<Vec<OkxOrderAck>>
    where
        B: Serialize + OkxRateLimitBody,
    {
        self.private_order_mutation_request(
            Method::POST,
            path,
            None,
            Some(body),
            PrivateRequestExpiry::TradeCommand,
        )
        .await
    }

    async fn private_regular_order_request_after_reservation<B>(
        &self,
        path: &str,
        body: &B,
        rate_limit_bucket: RateLimitBucket,
    ) -> Result<Vec<OkxOrderAck>>
    where
        B: Serialize + OkxRateLimitBody,
    {
        self.private_order_mutation_request_after_reservation(
            Method::POST,
            path,
            None,
            Some(body),
            PrivateRequestExpiry::TradeCommand,
            rate_limit_bucket,
        )
        .await
    }

    async fn private_account_sizing_request<T, B>(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        body: Option<&B>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + OkxRateLimitBody,
    {
        let (response, rate_limit_bucket) = self
            .send_private_request(method, path, query, body, PrivateRequestExpiry::None)
            .await?;
        parse_account_sizing_response(
            response,
            &rate_limit_bucket,
            &self.rate_limit_pacer,
            &self.gateway_latency_recorder,
        )
        .await
    }

    async fn private_order_mutation_request<B>(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        body: Option<&B>,
        expiry: PrivateRequestExpiry,
    ) -> Result<Vec<OkxOrderAck>>
    where
        B: Serialize + OkxRateLimitBody,
    {
        let (response, rate_limit_bucket) = self
            .send_private_request(method, path, query, body, expiry)
            .await?;
        parse_order_mutation_response(
            response,
            &rate_limit_bucket,
            &self.rate_limit_pacer,
            &self.gateway_latency_recorder,
        )
        .await
    }

    async fn private_order_mutation_request_after_reservation<B>(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        body: Option<&B>,
        expiry: PrivateRequestExpiry,
        rate_limit_bucket: RateLimitBucket,
    ) -> Result<Vec<OkxOrderAck>>
    where
        B: Serialize + OkxRateLimitBody,
    {
        let (response, rate_limit_bucket) = self
            .send_private_request_after_reservation(
                method,
                path,
                query,
                body,
                expiry,
                rate_limit_bucket,
            )
            .await?;
        parse_order_mutation_response(
            response,
            &rate_limit_bucket,
            &self.rate_limit_pacer,
            &self.gateway_latency_recorder,
        )
        .await
    }

    async fn private_algo_mutation_request<B>(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        body: Option<&B>,
        expiry: PrivateRequestExpiry,
    ) -> Result<Vec<OkxAlgoOrderAck>>
    where
        B: Serialize + OkxRateLimitBody,
    {
        let (response, rate_limit_bucket) = self
            .send_private_request(method, path, query, body, expiry)
            .await?;
        parse_algo_mutation_response(
            response,
            &rate_limit_bucket,
            &self.rate_limit_pacer,
            &self.gateway_latency_recorder,
        )
        .await
    }

    async fn private_request_with_expiry<T, B>(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        body: Option<&B>,
        expiry: PrivateRequestExpiry,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + OkxRateLimitBody,
    {
        let (response, rate_limit_bucket) = self
            .send_private_request(method, path, query, body, expiry)
            .await?;
        parse_response(
            response,
            &rate_limit_bucket,
            &self.rate_limit_pacer,
            &self.gateway_latency_recorder,
        )
        .await
    }

    async fn send_private_request<B>(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        body: Option<&B>,
        expiry: PrivateRequestExpiry,
    ) -> Result<(reqwest::Response, RateLimitBucket)>
    where
        B: Serialize + OkxRateLimitBody,
    {
        let rate_limit_bucket = self
            .reserve_private_request(&method, path, query, body)
            .await?;
        self.send_private_request_after_reservation(
            method,
            path,
            query,
            body,
            expiry,
            rate_limit_bucket,
        )
        .await
    }

    async fn reserve_private_request<B>(
        &self,
        method: &Method,
        path: &str,
        query: Option<&str>,
        body: Option<&B>,
    ) -> Result<RateLimitBucket>
    where
        B: OkxRateLimitBody,
    {
        let body_inst_id = body.and_then(OkxRateLimitBody::rate_limit_inst_id);
        let rate_limit_bucket = okx_rate_limit_bucket(method, path, query, body_inst_id)?;
        self.rate_limit_pacer.wait(&rate_limit_bucket).await?;
        Ok(rate_limit_bucket)
    }

    async fn send_private_request_after_reservation<B>(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        body: Option<&B>,
        expiry: PrivateRequestExpiry,
        rate_limit_bucket: RateLimitBucket,
    ) -> Result<(reqwest::Response, RateLimitBucket)>
    where
        B: Serialize + OkxRateLimitBody,
    {
        let request_target = request_target(path, query);
        let body = match body {
            Some(body) => serde_json::to_string(body).context("failed serializing OKX request")?,
            None => String::new(),
        };
        let timing = self.private_request_timing().await?;
        let signature = sign(
            self.api_secret.as_str(),
            &timing.timestamp,
            method.as_str(),
            &request_target,
            &body,
        )?;
        let url = self.url(path, query)?;
        let mut request = self
            .http
            .request(method, url)
            .header(OKX_API_KEY, self.api_key.as_str())
            .header(OKX_API_SIGN, signature)
            .header(OKX_API_TIMESTAMP, timing.timestamp)
            .header(OKX_API_PASSPHRASE, self.api_passphrase.as_str());
        if let Some(exp_time) = expiry.header_value(timing.unix_millis)? {
            request = request.header(OKX_ORDER_EXP_TIME, exp_time);
        }
        if self.simulated_trading {
            request = request.header(OKX_DOCUMENTED_SIMULATED_TRADING, "1");
        }
        if !body.is_empty() {
            request = request
                .header(header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let response = request.send().await.context("OKX private request failed")?;
        Ok((response, rate_limit_bucket))
    }

    fn url(&self, path: &str, query: Option<&str>) -> Result<Url> {
        let mut url = self
            .base_url
            .join(path.trim_start_matches('/'))
            .context("failed building OKX request URL")?;
        url.set_query(query);
        Ok(url)
    }

    async fn private_request_timing(&self) -> Result<PrivateRequestTiming> {
        if let Some(unix_millis) = self.server_time_clock.unix_millis()? {
            return PrivateRequestTiming::new(unix_millis);
        }
        self.refresh_server_time().await?;
        let unix_millis = self
            .server_time_clock
            .unix_millis()?
            .context("OKX server time sync did not produce a signing timestamp")?;
        PrivateRequestTiming::new(unix_millis)
    }

    async fn refresh_server_time(&self) -> Result<()> {
        let local_before_millis = current_unix_millis();
        let mut server_times = self
            .public_get::<OkxServerTime>(OKX_SERVER_TIME_PATH, None)
            .await
            .context("failed syncing OKX server time")?;
        let local_after_millis = current_unix_millis();
        ensure!(
            server_times.len() == 1,
            "OKX server time returned {} rows",
            server_times.len()
        );
        ensure!(
            local_after_millis >= local_before_millis,
            "local clock moved backward while syncing OKX server time"
        );
        let server_millis =
            parse_unix_millis("OKX server time", &server_times.remove(0).timestamp)?;
        let local_midpoint_millis =
            local_before_millis + ((local_after_millis - local_before_millis) / 2);
        self.server_time_clock
            .record(server_millis, local_midpoint_millis)
    }
}

impl OkxWebsocketLoginTimestampProvider {
    pub(crate) async fn login_timestamp(&self) -> Result<String> {
        match &self.source {
            OkxWebsocketLoginTimestampSource::ServerTime(client) => {
                client.websocket_login_timestamp().await
            }
            #[cfg(test)]
            OkxWebsocketLoginTimestampSource::Fixed(timestamp) => Ok(timestamp.clone()),
        }
    }

    #[cfg(test)]
    pub(crate) fn fixed(timestamp: impl Into<String>) -> Self {
        Self {
            source: OkxWebsocketLoginTimestampSource::Fixed(timestamp.into()),
        }
    }
}

#[derive(Clone, Default)]
struct ServerTimeClock {
    state: Arc<Mutex<Option<ServerTimeSnapshot>>>,
}

impl ServerTimeClock {
    fn needs_refresh(&self, refresh_margin: Duration) -> Result<bool> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX server time clock lock poisoned"))?;
        let Some(snapshot) = *state else {
            return Ok(true);
        };
        let elapsed = snapshot.measured_at.elapsed();
        if elapsed > OKX_SERVER_TIME_TTL {
            *state = None;
            return Ok(true);
        }
        Ok(OKX_SERVER_TIME_TTL.saturating_sub(elapsed) <= refresh_margin)
    }

    fn unix_millis(&self) -> Result<Option<i128>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX server time clock lock poisoned"))?;
        let Some(snapshot) = *state else {
            return Ok(None);
        };
        if snapshot.measured_at.elapsed() > OKX_SERVER_TIME_TTL {
            *state = None;
            return Ok(None);
        }
        let adjusted_millis = current_unix_millis()
            .checked_add(snapshot.offset_millis)
            .context("OKX server time offset overflowed local timestamp")?;
        Ok(Some(adjusted_millis))
    }

    fn record(&self, server_millis: i128, local_millis: i128) -> Result<()> {
        let offset_millis = server_millis
            .checked_sub(local_millis)
            .context("OKX server time offset overflowed")?;
        *self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX server time clock lock poisoned"))? =
            Some(ServerTimeSnapshot {
                offset_millis,
                measured_at: Instant::now(),
            });
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ServerTimeSnapshot {
    offset_millis: i128,
    measured_at: Instant,
}

struct PrivateRequestTiming {
    timestamp: String,
    unix_millis: i128,
}

impl PrivateRequestTiming {
    fn new(unix_millis: i128) -> Result<Self> {
        Ok(Self {
            timestamp: format_okx_timestamp(unix_millis)?,
            unix_millis,
        })
    }
}

fn websocket_login_timestamp_from_unix_millis(unix_millis: i128) -> Result<String> {
    let unix_seconds = unix_millis
        .checked_div(MILLIS_PER_SECOND)
        .context("OKX WebSocket login timestamp overflowed")?;
    ensure!(
        unix_seconds > 0,
        "OKX WebSocket login timestamp must be positive"
    );
    Ok(unix_seconds.to_string())
}

#[derive(Clone, Copy)]
enum PrivateRequestExpiry {
    None,
    TradeCommand,
}

impl PrivateRequestExpiry {
    fn header_value(self, unix_millis: i128) -> Result<Option<String>> {
        match self {
            Self::None => Ok(None),
            Self::TradeCommand => unix_millis
                .checked_add(OKX_ORDER_EXPIRY_WINDOW_MS)
                .context("OKX trade command expTime overflowed signing timestamp")
                .map(|exp_time| Some(exp_time.to_string())),
        }
    }
}

#[derive(Clone, Default)]
struct RateLimitPacer {
    state: Arc<Mutex<RateLimitPacerState>>,
}

#[derive(Default)]
struct RateLimitPacerState {
    cooldowns: HashMap<String, Instant>,
    requests: HashMap<String, VecDeque<Instant>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RateLimitBucket {
    key: String,
    limit: usize,
    window: Duration,
}

impl RateLimitBucket {
    fn new(key: String, limit: usize) -> Self {
        Self {
            key,
            limit,
            window: OKX_RATE_LIMIT_WINDOW,
        }
    }

    fn with_window(key: String, limit: usize, window: Duration) -> Self {
        Self { key, limit, window }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RateLimitScope {
    Ip,
    IpInstrumentType,
    User,
    UserInstrumentType,
    UserInstrument,
    UserTag,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OkxRateLimitRule {
    method: &'static str,
    path: &'static str,
    limit: usize,
    scope: RateLimitScope,
}

// Keep this as the single audited inventory of OKX API v5 endpoint limits used
// by this client. Each row mirrors the endpoint detail's rate-limit count and
// scope so tests can catch duplicate or accidentally untracked local rules.
const OKX_RATE_LIMIT_RULES: &[OkxRateLimitRule] = &[
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/public/time",
        limit: 10,
        scope: RateLimitScope::Ip,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/public/instruments",
        limit: 20,
        scope: RateLimitScope::IpInstrumentType,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/public/price-limit",
        limit: 20,
        scope: RateLimitScope::Ip,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/public/market-data-history",
        limit: 5,
        scope: RateLimitScope::Ip,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/market/candles",
        limit: 40,
        scope: RateLimitScope::Ip,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/market/history-candles",
        limit: 20,
        scope: RateLimitScope::Ip,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/market/history-trades",
        limit: 20,
        scope: RateLimitScope::Ip,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/market/ticker",
        limit: 20,
        scope: RateLimitScope::Ip,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/market/index-tickers",
        limit: 20,
        scope: RateLimitScope::Ip,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/market/books",
        limit: 40,
        scope: RateLimitScope::Ip,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/account/instruments",
        limit: 20,
        scope: RateLimitScope::UserInstrumentType,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/account/config",
        limit: 5,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/account/balance",
        limit: 10,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/account/trade-fee",
        limit: 5,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/account/max-size",
        limit: 20,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/account/max-avail-size",
        limit: 20,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/trade/order",
        limit: 60,
        scope: RateLimitScope::UserInstrument,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/trade/order-algo",
        limit: 20,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/trade/orders-pending",
        limit: 60,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/trade/orders-history",
        limit: 40,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/trade/orders-history-archive",
        limit: 20,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/trade/fills",
        limit: 60,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/trade/fills-history",
        limit: 10,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "POST",
        path: "/api/v5/trade/order",
        limit: 60,
        scope: RateLimitScope::UserInstrument,
    },
    OkxRateLimitRule {
        method: "POST",
        path: "/api/v5/trade/cancel-order",
        limit: 60,
        scope: RateLimitScope::UserInstrument,
    },
    OkxRateLimitRule {
        method: "POST",
        path: "/api/v5/trade/amend-order",
        limit: 60,
        scope: RateLimitScope::UserInstrument,
    },
    OkxRateLimitRule {
        method: "POST",
        path: "/api/v5/trade/order-algo",
        limit: 20,
        scope: RateLimitScope::UserInstrument,
    },
    OkxRateLimitRule {
        method: "POST",
        path: "/api/v5/trade/cancel-algos",
        limit: 20,
        scope: RateLimitScope::UserInstrument,
    },
    #[cfg(test)]
    OkxRateLimitRule {
        method: "POST",
        path: "/api/v5/trade/amend-algos",
        limit: 20,
        scope: RateLimitScope::UserInstrument,
    },
    OkxRateLimitRule {
        method: "POST",
        path: "/api/v5/trade/cancel-all-after",
        limit: 1,
        scope: RateLimitScope::UserTag,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/trade/orders-algo-pending",
        limit: 20,
        scope: RateLimitScope::User,
    },
    OkxRateLimitRule {
        method: "GET",
        path: "/api/v5/trade/orders-algo-history",
        limit: 20,
        scope: RateLimitScope::User,
    },
];

impl RateLimitPacer {
    async fn wait(&self, bucket: &RateLimitBucket) -> Result<()> {
        loop {
            let Some(remaining) = self.reserve_or_wait(bucket, Instant::now())? else {
                return Ok(());
            };
            tokio::time::sleep(remaining).await;
        }
    }

    #[cfg(test)]
    fn reservation_count(&self, bucket: &RateLimitBucket) -> Result<usize> {
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX rate limit pacer lock poisoned"))?
            .requests
            .get(&bucket.key)
            .map_or(0, VecDeque::len))
    }

    fn record_rate_limit(&self, bucket: &RateLimitBucket) -> Result<()> {
        self.record_rate_limit_at(bucket, Instant::now())
    }

    fn record_rate_limit_at(&self, bucket: &RateLimitBucket, now: Instant) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX rate limit pacer lock poisoned"))?
            .cooldowns
            .insert(bucket.key.clone(), now + OKX_RATE_LIMIT_COOLDOWN);
        Ok(())
    }

    fn reserve_or_wait(&self, bucket: &RateLimitBucket, now: Instant) -> Result<Option<Duration>> {
        ensure!(
            bucket.limit > 0,
            "OKX rate limit bucket {} must have a positive limit",
            bucket.key
        );
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("OKX rate limit pacer lock poisoned"))?;

        if let Some(until) = state.cooldowns.get(&bucket.key).copied() {
            if until > now {
                return Ok(Some(until.duration_since(now)));
            }
            state.cooldowns.remove(&bucket.key);
        }

        let pacing_window = bucket
            .window
            .saturating_add(OKX_RATE_LIMIT_PACING_SAFETY_MARGIN);
        let requests = state.requests.entry(bucket.key.clone()).or_default();
        while requests
            .front()
            .is_some_and(|request_at| now.duration_since(*request_at) >= pacing_window)
        {
            requests.pop_front();
        }
        if requests.len() < bucket.limit {
            requests.push_back(now);
            return Ok(None);
        }

        let oldest = requests
            .front()
            .copied()
            .context("OKX rate limit bucket is full without a recorded request")?;
        Ok(Some((oldest + pacing_window).duration_since(now)))
    }
}

fn ensure_orders_match_instrument(orders: &[OkxOrder], inst_id: &str, context: &str) -> Result<()> {
    for order in orders {
        ensure_order_matches_instrument(order, inst_id, context)?;
    }
    Ok(())
}

fn ensure_order_matches_instrument(order: &OkxOrder, inst_id: &str, context: &str) -> Result<()> {
    ensure!(
        order.inst_id == inst_id,
        "OKX {context} returned order {} for instrument {} while requesting {inst_id}",
        order.order_id,
        order.inst_id
    );
    ensure_spot_inst_type(&order.inst_type, &order.inst_id, context)?;
    order.ensure_documented_state(context)
}

fn ensure_algo_orders_match_instrument(
    orders: &[OkxAlgoOrder],
    inst_id: &str,
    context: &str,
) -> Result<()> {
    for order in orders {
        ensure_algo_order_matches_instrument(order, inst_id, context)?;
    }
    Ok(())
}

fn ensure_algo_order_matches_instrument(
    order: &OkxAlgoOrder,
    inst_id: &str,
    context: &str,
) -> Result<()> {
    ensure!(
        order.inst_id == inst_id,
        "OKX {context} returned algo {} for instrument {} while requesting {inst_id}",
        order.algo_id,
        order.inst_id
    );
    ensure_spot_inst_type(&order.inst_type, &order.inst_id, context)?;
    order.ensure_documented_state(context)
}

#[cfg(test)]
fn ensure_oco_order_matches(order: &OkxOcoOrder, inst_id: &str, context: &str) -> Result<()> {
    ensure!(
        order.inst_id == inst_id,
        "OKX {context} returned OCO {} for instrument {} while requesting {inst_id}",
        order.algo_id,
        order.inst_id
    );
    order.ensure_contract(context)
}

fn ensure_fills_match_instrument(fills: &[OkxFill], inst_id: &str, context: &str) -> Result<()> {
    for fill in fills {
        ensure!(
            fill.inst_id == inst_id,
            "OKX {context} returned fill {} for instrument {} while requesting {inst_id}",
            fill.dedupe_key(),
            fill.inst_id
        );
        ensure_spot_inst_type(&fill.inst_type, &fill.inst_id, context)?;
    }
    Ok(())
}

fn ensure_spot_inst_type(inst_type: &str, inst_id: &str, context: &str) -> Result<()> {
    ensure!(
        inst_type == OKX_SPOT_INST_TYPE,
        "OKX {context} returned instType {inst_type} for {inst_id}; expected {OKX_SPOT_INST_TYPE}"
    );
    Ok(())
}

#[cfg(test)]
fn spot_instrument_currencies(inst_id: &str) -> Result<(&str, &str)> {
    let (base_ccy, quote_ccy) = inst_id
        .split_once('-')
        .context("OKX spot order instrument id must use BASE-QUOTE format")?;
    ensure!(
        !base_ccy.is_empty() && !quote_ccy.is_empty() && !quote_ccy.contains('-'),
        "OKX spot order instrument id must use BASE-QUOTE format"
    );
    Ok((base_ccy, quote_ccy))
}

fn lock_instrument_snapshots(
    snapshots: &Mutex<HashMap<String, OkxInstrument>>,
) -> Result<std::sync::MutexGuard<'_, HashMap<String, OkxInstrument>>> {
    snapshots
        .lock()
        .map_err(|_| anyhow::anyhow!("OKX instrument snapshot cache lock poisoned"))
}

fn ensure_instrument_hint_matches_rest_snapshot(
    hint: &OkxWebsocketInstrumentUpdate,
    snapshot: &OkxInstrument,
) -> Result<()> {
    ensure!(
        hint.inst_id == snapshot.inst_id,
        "OKX WebSocket instrument hint returned {} while REST snapshot tracks {}",
        hint.inst_id,
        snapshot.inst_id
    );
    ensure_spot_inst_type(&hint.inst_type, &hint.inst_id, "WebSocket instrument hint")?;
    ensure!(
        hint.group_id == snapshot.fee_group_id()?,
        "OKX WebSocket instrument {} groupId {} disagrees with REST snapshot groupId {}",
        snapshot.inst_id,
        hint.group_id,
        snapshot.fee_group_id()?
    );

    ensure_required_instrument_hint_decimal_matches(
        &snapshot.inst_id,
        "tickSz",
        snapshot.tick_size()?,
        parse_required_instrument_hint_decimal("OKX WebSocket instrument tickSz", &hint.tick_size)?,
    )?;
    ensure_required_instrument_hint_decimal_matches(
        &snapshot.inst_id,
        "lotSz",
        snapshot.lot_size()?,
        parse_required_instrument_hint_decimal("OKX WebSocket instrument lotSz", &hint.lot_size)?,
    )?;
    ensure_required_instrument_hint_decimal_matches(
        &snapshot.inst_id,
        "minSz",
        snapshot.min_size()?,
        parse_required_instrument_hint_decimal("OKX WebSocket instrument minSz", &hint.min_size)?,
    )?;
    ensure_optional_instrument_hint_decimal_matches(
        &snapshot.inst_id,
        "maxLmtSz",
        snapshot.max_limit_size()?,
        parse_optional_instrument_hint_decimal(
            "OKX WebSocket instrument maxLmtSz",
            &hint.max_limit_size,
        )?,
    )?;
    ensure_optional_instrument_hint_decimal_matches(
        &snapshot.inst_id,
        "maxLmtAmt",
        snapshot.max_limit_amount()?,
        parse_optional_instrument_hint_decimal(
            "OKX WebSocket instrument maxLmtAmt",
            &hint.max_limit_amount,
        )?,
    )?;
    ensure_optional_instrument_hint_decimal_matches(
        &snapshot.inst_id,
        "maxMktSz",
        snapshot.max_market_size_usdt()?,
        parse_optional_instrument_hint_decimal(
            "OKX WebSocket instrument maxMktSz",
            &hint.max_market_size,
        )?,
    )?;
    ensure_optional_instrument_hint_decimal_matches(
        &snapshot.inst_id,
        "maxMktAmt",
        snapshot.max_market_amount()?,
        parse_optional_instrument_hint_decimal(
            "OKX WebSocket instrument maxMktAmt",
            &hint.max_market_amount,
        )?,
    )?;
    ensure_optional_instrument_hint_decimal_matches(
        &snapshot.inst_id,
        "maxTriggerSz",
        snapshot.max_trigger_size()?,
        parse_optional_instrument_hint_decimal(
            "OKX WebSocket instrument maxTriggerSz",
            &hint.max_trigger_size,
        )?,
    )
}

fn parse_required_instrument_hint_decimal(context: &str, value: &str) -> Result<Decimal> {
    parse_positive_decimal_field(context, value)
}

fn parse_optional_instrument_hint_decimal(context: &str, value: &str) -> Result<Option<Decimal>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_positive_decimal_field(context, value)?))
}

fn ensure_required_instrument_hint_decimal_matches(
    inst_id: &str,
    field: &str,
    rest_snapshot: Decimal,
    websocket_hint: Decimal,
) -> Result<()> {
    ensure!(
        rest_snapshot == websocket_hint,
        "OKX WebSocket instrument hint for {inst_id} changed {field} from REST snapshot {rest_snapshot} to {websocket_hint}; refusing to use stale instrument metadata"
    );
    Ok(())
}

fn ensure_optional_instrument_hint_decimal_matches(
    inst_id: &str,
    field: &str,
    rest_snapshot: Option<Decimal>,
    websocket_hint: Option<Decimal>,
) -> Result<()> {
    let Some(websocket_hint) = websocket_hint else {
        return Ok(());
    };
    ensure!(
        rest_snapshot == Some(websocket_hint),
        "OKX WebSocket instrument hint for {inst_id} changed {field} from REST snapshot {} to {}; refusing to use stale instrument metadata",
        format_optional_decimal(rest_snapshot),
        websocket_hint
    );
    Ok(())
}

fn format_optional_decimal(value: Option<Decimal>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "<empty>".to_owned())
}

fn validate_order_amend(amend: OkxOrderAmend<'_>) -> Result<()> {
    ensure_trimmed_non_empty("OKX amend instId", amend.inst_id)?;
    ensure_trimmed_non_empty("OKX amend clOrdId", amend.client_order_id)?;
    ensure!(
        amend.new_size.is_some() || amend.new_price.is_some(),
        "OKX amend order requires new_size or new_price"
    );
    if let Some(new_size) = amend.new_size {
        parse_positive_decimal_field("OKX amend newSz", new_size)?;
    }
    if let Some(new_price) = amend.new_price {
        parse_positive_decimal_field("OKX amend newPx", new_price)?;
    }
    Ok(())
}

#[cfg(test)]
fn validate_oco_protection(protection: OkxOcoProtection<'_>) -> Result<()> {
    ensure_trimmed_non_empty("OKX OCO instId", protection.inst_id)?;
    ensure_trimmed_non_empty("OKX OCO algoClOrdId", protection.client_order_id)?;
    ensure!(
        protection.client_order_id.len() <= 32
            && protection
                .client_order_id
                .chars()
                .all(|character| character.is_ascii_alphanumeric()),
        "OKX OCO algoClOrdId must contain at most 32 ASCII alphanumeric characters"
    );
    parse_positive_decimal_field("OKX OCO sz", protection.size)?;
    let take_profit =
        parse_positive_decimal_field("OKX OCO tpTriggerPx", protection.take_profit_trigger_price)?;
    let stop_loss =
        parse_positive_decimal_field("OKX OCO slTriggerPx", protection.stop_loss_trigger_price)?;
    ensure!(
        take_profit > stop_loss,
        "OKX sell OCO take-profit trigger {take_profit} must be above stop-loss trigger {stop_loss}"
    );
    Ok(())
}

#[cfg(test)]
fn validate_oco_amend(amend: OkxOcoAmend<'_>) -> Result<()> {
    ensure_trimmed_non_empty("OKX OCO amend instId", amend.inst_id)?;
    ensure_trimmed_non_empty("OKX OCO amend algoId", amend.algo_id)?;
    validate_oco_protection(OkxOcoProtection {
        inst_id: amend.inst_id,
        size: amend.new_size,
        take_profit_trigger_price: amend.new_take_profit_trigger_price,
        stop_loss_trigger_price: amend.new_stop_loss_trigger_price,
        client_order_id: amend.client_order_id,
    })
}

#[cfg(test)]
fn verify_reconciled_oco(order: &OkxOcoOrder, requested: OkxOcoProtection<'_>) -> Result<()> {
    ensure_oco_order_matches(order, requested.inst_id, "OCO submit reconciliation")?;
    ensure!(
        order.client_order_id == requested.client_order_id && order.is_pending(),
        "OKX OCO submit reconciliation returned algoClOrdId {} in state {:?} for requested {}",
        order.client_order_id,
        order.state,
        requested.client_order_id
    );
    let expected_size = parse_positive_decimal_field("OKX requested OCO sz", requested.size)?;
    let expected_take_profit = parse_positive_decimal_field(
        "OKX requested OCO tpTriggerPx",
        requested.take_profit_trigger_price,
    )?;
    let expected_stop_loss = parse_positive_decimal_field(
        "OKX requested OCO slTriggerPx",
        requested.stop_loss_trigger_price,
    )?;
    ensure!(
        order.requested_size()? == expected_size
            && order.take_profit_trigger_price()? == expected_take_profit
            && order.stop_loss_trigger_price()? == expected_stop_loss,
        "OKX OCO submit reconciliation returned size/trigger values that differ from the requested contract"
    );
    Ok(())
}

fn verify_reconciled_order_amend(order: &OkxOrder, amend: OkxOrderAmend<'_>) -> Result<()> {
    ensure_order_matches_instrument(order, amend.inst_id, "amend reconciliation")?;
    ensure!(
        order.parsed_side() == Some(amend.side),
        "OKX amend reconciliation returned side {} for requested {}",
        order.side,
        amend.side.as_okx()
    );
    ensure!(
        order.client_order_id == amend.client_order_id,
        "OKX amend reconciliation returned clOrdId {} for requested {}",
        order.client_order_id,
        amend.client_order_id
    );
    if let Some(new_size) = amend.new_size {
        let actual_size = order.requested_size()?;
        let expected_size = parse_positive_decimal_field("OKX amend expected newSz", new_size)?;
        ensure!(
            actual_size == expected_size,
            "OKX amend reconciliation returned sz {} for requested newSz {}",
            order.sz,
            new_size
        );
    }
    if let Some(new_price) = amend.new_price {
        let actual_price = parse_positive_decimal_field("OKX amend reconciled px", &order.price)?;
        let expected_price = parse_positive_decimal_field("OKX amend expected newPx", new_price)?;
        ensure!(
            actual_price == expected_price,
            "OKX amend reconciliation returned px {} for requested newPx {}",
            order.price,
            new_price
        );
    }
    Ok(())
}

fn parse_positive_decimal_field(label: &str, value: &str) -> Result<Decimal> {
    ensure_trimmed_non_empty(label, value)?;
    let value =
        Decimal::from_str(value).with_context(|| format!("{label} must be a decimal value"))?;
    ensure!(value > Decimal::ZERO, "{label} must be positive");
    Ok(value)
}

fn ensure_trimmed_non_empty(label: &str, value: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{label} must not be empty");
    ensure!(value == value.trim(), "{label} must be trimmed");
    Ok(())
}

fn single_order_ack(
    mut acknowledgements: Vec<OkxOrderAck>,
    client_order_id: &str,
    context: &str,
) -> Result<OkxOrderAck> {
    ensure!(
        acknowledgements.len() == 1,
        "OKX returned {} {context} acknowledgements for {client_order_id}",
        acknowledgements.len()
    );
    let mut acknowledgement = acknowledgements.remove(0);
    if acknowledgement.client_order_id.trim().is_empty() && acknowledgement.status_code != "0" {
        acknowledgement.client_order_id = client_order_id.to_owned();
    } else {
        ensure!(
            acknowledgement.client_order_id == client_order_id,
            "OKX {context} acknowledgement returned clOrdId {} for requested {client_order_id}",
            acknowledgement.client_order_id
        );
    }
    if acknowledgement.status_code == "0" {
        ensure!(
            !acknowledgement.order_id.trim().is_empty(),
            "OKX {context} acknowledgement omitted ordId for {client_order_id}"
        );
    }
    Ok(acknowledgement)
}

#[derive(Clone, Copy)]
enum AlgoAckIdentity<'a> {
    ClientOrderId(&'a str),
    AlgoId(&'a str),
}

fn single_algo_ack(
    mut acknowledgements: Vec<OkxAlgoOrderAck>,
    identity: AlgoAckIdentity<'_>,
    context: &str,
) -> Result<OkxAlgoOrderAck> {
    let requested_id = match identity {
        AlgoAckIdentity::ClientOrderId(client_order_id) => client_order_id,
        AlgoAckIdentity::AlgoId(algo_id) => algo_id,
    };
    ensure!(
        acknowledgements.len() == 1,
        "OKX returned {} {context} acknowledgements for {requested_id}",
        acknowledgements.len()
    );
    let acknowledgement = acknowledgements.remove(0);
    match identity {
        AlgoAckIdentity::ClientOrderId(client_order_id) => ensure!(
            acknowledgement.client_order_id.trim().is_empty()
                || acknowledgement.client_order_id == client_order_id,
            "OKX {context} acknowledgement returned algoClOrdId {} for requested {client_order_id}",
            acknowledgement.client_order_id
        ),
        AlgoAckIdentity::AlgoId(algo_id) => ensure!(
            acknowledgement.algo_id.trim().is_empty() || acknowledgement.algo_id == algo_id,
            "OKX {context} acknowledgement returned algoId {} for requested {algo_id}",
            acknowledgement.algo_id
        ),
    }
    if acknowledgement.status_code == "0" {
        ensure!(
            !acknowledgement.algo_id.trim().is_empty(),
            "OKX {context} acknowledgement omitted algoId for {requested_id}"
        );
        match identity {
            AlgoAckIdentity::ClientOrderId(client_order_id) => ensure!(
                acknowledgement.client_order_id == client_order_id,
                "OKX {context} acknowledgement returned algoClOrdId {} for requested {client_order_id}",
                acknowledgement.client_order_id
            ),
            AlgoAckIdentity::AlgoId(algo_id) => ensure!(
                acknowledgement.algo_id == algo_id,
                "OKX {context} acknowledgement returned algoId {} for requested {algo_id}",
                acknowledgement.algo_id
            ),
        }
    }
    Ok(acknowledgement)
}

fn reconciled_order_ack(order: OkxOrder) -> OkxOrderAck {
    OkxOrderAck {
        order_id: order.order_id,
        client_order_id: order.client_order_id,
        status_code: "0".to_owned(),
        status_message: String::new(),
        status_sub_code: String::new(),
        timestamp: order.updated_at_ms,
    }
}

fn reconciled_algo_ack(order: OkxAlgoOrder) -> OkxAlgoOrderAck {
    OkxAlgoOrderAck {
        algo_id: order.algo_id,
        client_order_id: order.client_order_id,
        status_code: "0".to_owned(),
        status_message: String::new(),
    }
}

#[cfg(test)]
fn reconciled_oco_ack(order: OkxOcoOrder) -> OkxAlgoOrderAck {
    OkxAlgoOrderAck {
        algo_id: order.algo_id,
        client_order_id: order.client_order_id,
        status_code: "0".to_owned(),
        status_message: String::new(),
    }
}

const fn spot_market_target_currency(side: OrderSide, kind: OrderKind) -> Option<&'static str> {
    match (side, kind) {
        (OrderSide::Buy, OrderKind::Market) => Some("quote_ccy"),
        (OrderSide::Sell, OrderKind::Market) => Some("base_ccy"),
        (OrderSide::Buy, OrderKind::Limit | OrderKind::PostOnly)
        | (OrderSide::Sell, OrderKind::Limit | OrderKind::PostOnly) => None,
    }
}

fn fresh_live_candles_from_cache(
    cache: &OkxMarketDataCache,
    inst_id: &str,
    channel: &str,
    max_staleness: Duration,
    limit: usize,
) -> Option<Vec<MarketBar>> {
    let hints = cache.fresh_candles(inst_id, channel, max_staleness);
    (hints.len() >= limit).then(|| merge_live_candle_hints(Vec::new(), hints, limit))
}

fn live_candle_fallback_key(inst_id: &str, bar: &str, limit: usize) -> LiveCandleFallbackKey {
    LiveCandleFallbackKey {
        inst_id: inst_id.to_owned(),
        bar: bar.to_owned(),
        limit,
    }
}

fn prune_expired_live_candle_fallbacks(
    fallbacks: &mut HashMap<LiveCandleFallbackKey, LiveCandleFallback>,
    max_age: Duration,
) {
    fallbacks.retain(|_, fallback| fallback.fetched_at.elapsed() <= max_age);
}

fn evict_oldest_live_candle_fallback(
    fallbacks: &mut HashMap<LiveCandleFallbackKey, LiveCandleFallback>,
) {
    let Some(oldest_key) = fallbacks
        .iter()
        .min_by_key(|(_, fallback)| fallback.fetched_at)
        .map(|(key, _)| key.clone())
    else {
        return;
    };
    fallbacks.remove(&oldest_key);
}

fn merge_live_candle_hints(
    rest_candles: Vec<MarketBar>,
    websocket_hints: Vec<MarketBar>,
    limit: usize,
) -> Vec<MarketBar> {
    let mut candles_by_ts_ms = BTreeMap::new();
    for candle in rest_candles.into_iter().chain(websocket_hints) {
        candles_by_ts_ms.insert(candle.ts_ms, candle);
    }
    let skip_count = candles_by_ts_ms.len().saturating_sub(limit);
    candles_by_ts_ms.into_values().skip(skip_count).collect()
}

impl OkxClient for OkxRestClient {
    async fn instruments(&self, inst_id: &str) -> Result<OkxInstrument> {
        OkxRestClient::instruments(self, inst_id).await
    }

    async fn candles(&self, inst_id: &str, bar: &str, limit: usize) -> Result<Vec<MarketBar>> {
        OkxRestClient::candles(self, inst_id, bar, limit).await
    }

    async fn live_candles(&self, inst_id: &str, bar: &str, limit: usize) -> Result<Vec<MarketBar>> {
        OkxRestClient::live_candles(self, inst_id, bar, limit).await
    }

    async fn ticker(&self, inst_id: &str) -> Result<OkxTicker> {
        OkxRestClient::ticker(self, inst_id).await
    }

    async fn fresh_quote_usd_rate(
        &self,
        instrument: &ValidatedTradingInstrument,
    ) -> Result<ValidatedQuoteUsdRate> {
        OkxRestClient::fresh_quote_usd_rate(self, instrument).await
    }

    async fn balances(&self) -> Result<Vec<OkxBalance>> {
        OkxRestClient::balances(self).await
    }

    async fn spot_trade_fee(&self, inst_id: &str) -> Result<OkxTradeFeeRate> {
        OkxRestClient::spot_trade_fee(self, inst_id).await
    }

    async fn open_orders(&self, inst_id: &str) -> Result<Vec<OkxOrder>> {
        OkxRestClient::open_orders(self, inst_id).await
    }

    async fn order_history(&self, inst_id: &str) -> Result<Vec<OkxOrder>> {
        OkxRestClient::order_history(self, inst_id).await
    }

    async fn order_fills(&self, inst_id: &str) -> Result<Vec<OkxFill>> {
        OkxRestClient::order_fills(self, inst_id).await
    }

    async fn open_algo_orders(&self, inst_id: &str) -> Result<Vec<OkxAlgoOrder>> {
        OkxRestClient::open_algo_orders(self, inst_id).await
    }

    async fn algo_order_history(&self, inst_id: &str) -> Result<Vec<OkxAlgoOrder>> {
        OkxRestClient::algo_order_history(self, inst_id).await
    }

    async fn place_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        kind: OrderKind,
        size: &str,
        price: Option<&str>,
        client_order_id: &str,
    ) -> Result<OkxOrderAck> {
        OkxRestClient::place_order(self, inst_id, side, kind, size, price, client_order_id).await
    }

    async fn cancel_order(&self, inst_id: &str, client_order_id: &str) -> Result<()> {
        OkxRestClient::cancel_order(self, inst_id, client_order_id).await
    }

    async fn amend_order(&self, request: OkxOrderAmend<'_>) -> Result<OkxOrderAck> {
        OkxRestClient::amend_order(self, request).await
    }

    async fn place_trigger_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        size: &str,
        trigger_price: &str,
        client_order_id: &str,
    ) -> Result<OkxAlgoOrderAck> {
        OkxRestClient::place_trigger_order(
            self,
            inst_id,
            side,
            size,
            trigger_price,
            client_order_id,
        )
        .await
    }

    async fn cancel_algo_order(&self, inst_id: &str, algo_id: &str) -> Result<()> {
        OkxRestClient::cancel_algo_order(self, inst_id, algo_id).await
    }

    async fn order(&self, inst_id: &str, client_order_id: &str) -> Result<Option<OkxOrder>> {
        OkxRestClient::order(self, inst_id, client_order_id).await
    }
}

#[derive(Serialize)]
struct PlaceOrderRequest<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "tdMode")]
    td_mode: &'a str,
    side: &'a str,
    #[serde(rename = "ordType")]
    order_type: &'a str,
    sz: &'a str,
    #[serde(rename = "px", skip_serializing_if = "Option::is_none")]
    price: Option<&'a str>,
    #[serde(rename = "tgtCcy", skip_serializing_if = "Option::is_none")]
    target_currency: Option<&'a str>,
    #[serde(rename = "tradeQuoteCcy")]
    trade_quote_currency: &'a str,
    #[serde(rename = "banAmend", skip_serializing_if = "Option::is_none")]
    ban_amend: Option<bool>,
    #[serde(rename = "slippagePct", skip_serializing_if = "Option::is_none")]
    slippage_pct: Option<&'a str>,
    #[serde(rename = "pxAmendType")]
    price_amend_type: &'static str,
    tag: &'a str,
    #[serde(rename = "clOrdId")]
    client_order_id: &'a str,
}

#[derive(Serialize)]
struct CancelOrderRequest<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "clOrdId")]
    client_order_id: &'a str,
}

#[derive(Serialize)]
struct AmendOrderRequest<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "clOrdId")]
    client_order_id: &'a str,
    #[serde(rename = "newSz", skip_serializing_if = "Option::is_none")]
    new_size: Option<&'a str>,
    #[serde(rename = "newPx", skip_serializing_if = "Option::is_none")]
    new_price: Option<&'a str>,
    #[serde(rename = "pxAmendType")]
    price_amend_type: &'static str,
}

#[derive(Serialize)]
struct PlaceTriggerOrderRequest<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "tdMode")]
    td_mode: &'a str,
    side: &'a str,
    #[serde(rename = "ordType")]
    order_type: &'a str,
    sz: &'a str,
    #[serde(rename = "triggerPx")]
    trigger_price: &'a str,
    #[serde(rename = "triggerPxType")]
    trigger_price_type: &'a str,
    #[serde(rename = "orderPx")]
    order_price: &'a str,
    #[serde(rename = "tradeQuoteCcy")]
    trade_quote_currency: &'a str,
    tag: &'a str,
    #[serde(rename = "algoClOrdId")]
    client_order_id: &'a str,
}

#[cfg(test)]
#[derive(Serialize)]
struct PlaceOcoOrderRequest<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "tdMode")]
    td_mode: &'a str,
    side: &'a str,
    #[serde(rename = "ordType")]
    order_type: &'a str,
    sz: &'a str,
    #[serde(rename = "tpTriggerPx")]
    take_profit_trigger_price: &'a str,
    #[serde(rename = "tpTriggerPxType")]
    take_profit_trigger_price_type: &'a str,
    #[serde(rename = "tpOrdPx")]
    take_profit_order_price: &'a str,
    #[serde(rename = "slTriggerPx")]
    stop_loss_trigger_price: &'a str,
    #[serde(rename = "slTriggerPxType")]
    stop_loss_trigger_price_type: &'a str,
    #[serde(rename = "slOrdPx")]
    stop_loss_order_price: &'a str,
    #[serde(rename = "tradeQuoteCcy")]
    trade_quote_currency: &'a str,
    tag: &'a str,
    #[serde(rename = "algoClOrdId")]
    client_order_id: &'a str,
}

#[cfg(test)]
#[derive(Serialize)]
struct AmendOcoOrderRequest<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "algoId")]
    algo_id: &'a str,
    #[serde(rename = "algoClOrdId")]
    client_order_id: &'a str,
    #[serde(rename = "cxlOnFail")]
    cancel_on_fail: bool,
    #[serde(rename = "newSz")]
    new_size: &'a str,
    #[serde(rename = "newTpTriggerPx")]
    new_take_profit_trigger_price: &'a str,
    #[serde(rename = "newTpTriggerPxType")]
    new_take_profit_trigger_price_type: &'a str,
    #[serde(rename = "newTpOrdPx")]
    new_take_profit_order_price: &'a str,
    #[serde(rename = "newSlTriggerPx")]
    new_stop_loss_trigger_price: &'a str,
    #[serde(rename = "newSlTriggerPxType")]
    new_stop_loss_trigger_price_type: &'a str,
    #[serde(rename = "newSlOrdPx")]
    new_stop_loss_order_price: &'a str,
}

#[derive(Serialize)]
struct CancelAlgoOrderRequest<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "algoId")]
    algo_id: &'a str,
}

#[derive(Serialize)]
struct CancelAllAfterRequest<'a> {
    #[serde(rename = "timeOut")]
    timeout_seconds: &'a str,
    tag: &'a str,
}

#[derive(Serialize)]
struct EmptyBody;

trait OkxRateLimitBody {
    fn rate_limit_inst_id(&self) -> Option<&str> {
        None
    }
}

impl OkxRateLimitBody for EmptyBody {}

impl OkxRateLimitBody for PlaceOrderRequest<'_> {
    fn rate_limit_inst_id(&self) -> Option<&str> {
        Some(self.inst_id)
    }
}

impl OkxRateLimitBody for CancelOrderRequest<'_> {
    fn rate_limit_inst_id(&self) -> Option<&str> {
        Some(self.inst_id)
    }
}

impl OkxRateLimitBody for AmendOrderRequest<'_> {
    fn rate_limit_inst_id(&self) -> Option<&str> {
        Some(self.inst_id)
    }
}

impl OkxRateLimitBody for PlaceTriggerOrderRequest<'_> {
    fn rate_limit_inst_id(&self) -> Option<&str> {
        Some(self.inst_id)
    }
}

#[cfg(test)]
impl OkxRateLimitBody for PlaceOcoOrderRequest<'_> {
    fn rate_limit_inst_id(&self) -> Option<&str> {
        Some(self.inst_id)
    }
}

#[cfg(test)]
impl OkxRateLimitBody for AmendOcoOrderRequest<'_> {
    fn rate_limit_inst_id(&self) -> Option<&str> {
        Some(self.inst_id)
    }
}

impl<const N: usize> OkxRateLimitBody for [CancelAlgoOrderRequest<'_>; N] {
    fn rate_limit_inst_id(&self) -> Option<&str> {
        self.first().map(|request| request.inst_id)
    }
}

impl OkxRateLimitBody for CancelAllAfterRequest<'_> {}

#[derive(Deserialize)]
struct OkxServerTime {
    #[serde(rename = "ts")]
    timestamp: String,
}

#[derive(Deserialize)]
struct OkxRestTicker {
    #[serde(flatten)]
    ticker: OkxTicker,
    #[serde(rename = "ts")]
    timestamp: String,
}

#[cfg(test)]
#[derive(Deserialize)]
struct OkxAccountSpotTradeQuoteInstrument {
    #[serde(rename = "instType")]
    inst_type: String,
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "baseCcy")]
    base_ccy: String,
    #[serde(rename = "quoteCcy")]
    quote_ccy: String,
    #[serde(rename = "tradeQuoteCcyList")]
    trade_quote_currencies: Vec<String>,
    state: String,
}

#[derive(Deserialize)]
#[cfg(test)]
struct OkxEnvelope<T> {
    code: String,
    msg: String,
    data: T,
    #[serde(flatten)]
    timing: OkxEnvelopeTiming,
}

#[derive(Deserialize)]
struct OkxEnvelopeRaw<'a> {
    code: String,
    msg: String,
    #[serde(default, borrow)]
    data: Option<&'a RawValue>,
    #[serde(flatten)]
    timing: OkxEnvelopeTiming,
}

#[derive(Deserialize)]
struct OkxOrderMutationEnvelope {
    code: String,
    msg: String,
    data: Vec<OkxOrderAck>,
    #[serde(flatten)]
    timing: OkxEnvelopeTiming,
}

#[derive(Deserialize)]
struct OkxAlgoMutationEnvelope {
    code: String,
    msg: String,
    #[serde(default)]
    data: Vec<OkxAlgoOrderAck>,
    #[serde(flatten)]
    timing: OkxEnvelopeTiming,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
struct OkxEnvelopeTiming {
    #[serde(rename = "inTime", default)]
    in_time_microseconds: Option<String>,
    #[serde(rename = "outTime", default)]
    out_time_microseconds: Option<String>,
}

impl OkxEnvelopeTiming {
    fn gateway_latency(&self) -> Option<OkxEnvelopeLatency> {
        let in_time_microseconds = self.in_time_microseconds.as_ref()?.parse::<i128>().ok()?;
        let out_time_microseconds = self.out_time_microseconds.as_ref()?.parse::<i128>().ok()?;
        let gateway_latency_microseconds =
            out_time_microseconds.checked_sub(in_time_microseconds)?;
        if gateway_latency_microseconds < 0 {
            return None;
        }
        Some(OkxEnvelopeLatency {
            in_time_microseconds,
            out_time_microseconds,
            gateway_latency_microseconds,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OkxEnvelopeLatency {
    in_time_microseconds: i128,
    out_time_microseconds: i128,
    gateway_latency_microseconds: i128,
}

#[derive(Clone, Debug, Default)]
struct OkxGatewayLatencyRecorder {
    window: Arc<Mutex<OkxGatewayLatencyWindow>>,
}

#[derive(Debug, Default)]
struct OkxGatewayLatencyWindow {
    sample_count: u64,
    slow_sample_count: u64,
    total_gateway_latency_microseconds: u128,
    max_gateway_latency_microseconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OkxGatewayLatencySummary {
    sample_count: u64,
    slow_sample_count: u64,
    average_gateway_latency_microseconds: u64,
    max_gateway_latency_microseconds: u64,
}

impl OkxGatewayLatencyRecorder {
    fn record(&self, timing: OkxEnvelopeLatency) -> Option<OkxGatewayLatencySummary> {
        let gateway_latency_microseconds =
            u64::try_from(timing.gateway_latency_microseconds).ok()?;
        let mut window = match self.window.lock() {
            Ok(window) => window,
            Err(poisoned) => {
                warn!(
                    safety_event = "okx_gateway_latency_recorder_poisoned",
                    recovery_action = "reset_window",
                    "OKX gateway latency recorder mutex poisoned; resetting telemetry window"
                );
                let mut window = poisoned.into_inner();
                *window = OkxGatewayLatencyWindow::default();
                self.window.clear_poison();
                window
            }
        };
        window.sample_count = window.sample_count.saturating_add(1);
        if okx_gateway_latency_exceeds_warn_threshold(timing) {
            window.slow_sample_count = window.slow_sample_count.saturating_add(1);
        }
        window.total_gateway_latency_microseconds = window
            .total_gateway_latency_microseconds
            .saturating_add(u128::from(gateway_latency_microseconds));
        window.max_gateway_latency_microseconds = window
            .max_gateway_latency_microseconds
            .max(gateway_latency_microseconds);

        if window.sample_count < OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW {
            return None;
        }

        let summary = OkxGatewayLatencySummary {
            sample_count: window.sample_count,
            slow_sample_count: window.slow_sample_count,
            average_gateway_latency_microseconds: u64::try_from(
                window.total_gateway_latency_microseconds / u128::from(window.sample_count),
            )
            .ok()?,
            max_gateway_latency_microseconds: window.max_gateway_latency_microseconds,
        };
        *window = OkxGatewayLatencyWindow::default();
        Some(summary)
    }
}

fn emit_okx_gateway_timing(rate_limit_bucket: &RateLimitBucket, timing: OkxEnvelopeLatency) {
    if okx_gateway_latency_exceeds_warn_threshold(timing) {
        warn!(
            okx_rate_limit_key = %rate_limit_bucket.key,
            okx_gateway_in_time_us = timing.in_time_microseconds,
            okx_gateway_out_time_us = timing.out_time_microseconds,
            okx_gateway_latency_us = timing.gateway_latency_microseconds,
            okx_gateway_latency_warn_threshold_us = OKX_GATEWAY_LATENCY_WARN_THRESHOLD.as_micros(),
            "slow OKX REST gateway timing"
        );
    } else {
        debug!(
            okx_rate_limit_key = %rate_limit_bucket.key,
            okx_gateway_in_time_us = timing.in_time_microseconds,
            okx_gateway_out_time_us = timing.out_time_microseconds,
            okx_gateway_latency_us = timing.gateway_latency_microseconds,
            "captured OKX REST gateway timing"
        );
    }
}

fn emit_okx_gateway_latency_summary(summary: OkxGatewayLatencySummary) {
    info!(
        okx_gateway_latency_sample_count = summary.sample_count,
        okx_gateway_latency_slow_sample_count = summary.slow_sample_count,
        okx_gateway_latency_average_us = summary.average_gateway_latency_microseconds,
        okx_gateway_latency_max_us = summary.max_gateway_latency_microseconds,
        okx_gateway_latency_warn_threshold_us = OKX_GATEWAY_LATENCY_WARN_THRESHOLD.as_micros(),
        "summarized OKX REST gateway timing"
    );
}

fn okx_gateway_latency_exceeds_warn_threshold(timing: OkxEnvelopeLatency) -> bool {
    timing.gateway_latency_microseconds >= OKX_GATEWAY_LATENCY_WARN_THRESHOLD.as_micros() as i128
}

async fn parse_response<T>(
    response: reqwest::Response,
    rate_limit_bucket: &RateLimitBucket,
    rate_limit_pacer: &RateLimitPacer,
    gateway_latency_recorder: &OkxGatewayLatencyRecorder,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        rate_limit_pacer.record_rate_limit(rate_limit_bucket)?;
    }
    let body = match read_okx_response_body(response).await {
        Ok(body) => body,
        Err(error) if status == reqwest::StatusCode::TOO_MANY_REQUESTS => {
            bail!(
                "OKX rate limit for {}: HTTP {status}; {error}",
                rate_limit_bucket.key
            );
        }
        Err(error) if !status.is_success() => {
            bail!("OKX HTTP {status}; {error}");
        }
        Err(error) => {
            return Err(error).context("failed reading OKX response body");
        }
    };
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        bail!(
            "OKX rate limit for {}: HTTP {status}; {}",
            rate_limit_bucket.key,
            body.summary()
        );
    }
    ensure!(status.is_success(), "OKX HTTP {status}; {}", body.summary());
    let envelope: OkxEnvelopeRaw<'_> = serde_json::from_slice(body.bytes())
        .with_context(|| format!("failed parsing OKX response body; {}", body.summary()))?;
    if let Some(timing) = envelope.timing.gateway_latency() {
        emit_okx_gateway_timing(rate_limit_bucket, timing);
        if let Some(summary) = gateway_latency_recorder.record(timing) {
            emit_okx_gateway_latency_summary(summary);
        }
    }
    if is_okx_rate_limit_code(&envelope.code) {
        rate_limit_pacer.record_rate_limit(rate_limit_bucket)?;
        bail!(
            "OKX API rate limit {}: {} ({rate_limit_key})",
            envelope.code,
            envelope.msg,
            rate_limit_key = rate_limit_bucket.key
        );
    }
    let okx_error_message = if envelope.code.starts_with("501") {
        "response message omitted"
    } else {
        envelope.msg.as_str()
    };
    if envelope.code != "0" {
        return Err(OkxApiError {
            code: envelope.code,
            message: okx_error_message.to_owned(),
        }
        .into());
    }
    let data = envelope
        .data
        .map(|data| data.get().as_bytes())
        .unwrap_or(b"null");
    serde_json::from_slice(data)
        .with_context(|| format!("failed parsing OKX response body; {}", body.summary()))
}

async fn parse_order_mutation_response(
    response: reqwest::Response,
    rate_limit_bucket: &RateLimitBucket,
    rate_limit_pacer: &RateLimitPacer,
    gateway_latency_recorder: &OkxGatewayLatencyRecorder,
) -> Result<Vec<OkxOrderAck>> {
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        rate_limit_pacer.record_rate_limit(rate_limit_bucket)?;
    }
    let body = match read_okx_response_body(response).await {
        Ok(body) => body,
        Err(error) if status == reqwest::StatusCode::TOO_MANY_REQUESTS => {
            bail!(
                "OKX rate limit for {}: HTTP {status}; {error}",
                rate_limit_bucket.key
            );
        }
        Err(error) if !status.is_success() => {
            bail!("OKX HTTP {status}; {error}");
        }
        Err(error) => {
            return Err(error).context("failed reading OKX order response body");
        }
    };
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        bail!(
            "OKX rate limit for {}: HTTP {status}; {}",
            rate_limit_bucket.key,
            body.summary()
        );
    }
    ensure!(status.is_success(), "OKX HTTP {status}; {}", body.summary());
    let envelope: OkxOrderMutationEnvelope = serde_json::from_slice(body.bytes())
        .with_context(|| format!("failed parsing OKX order response body; {}", body.summary()))?;
    if let Some(timing) = envelope.timing.gateway_latency() {
        emit_okx_gateway_timing(rate_limit_bucket, timing);
        if let Some(summary) = gateway_latency_recorder.record(timing) {
            emit_okx_gateway_latency_summary(summary);
        }
    }
    if is_okx_rate_limit_code(&envelope.code) {
        rate_limit_pacer.record_rate_limit(rate_limit_bucket)?;
        bail!(
            "OKX API rate limit {}: {} ({rate_limit_key})",
            envelope.code,
            envelope.msg,
            rate_limit_key = rate_limit_bucket.key
        );
    }
    if envelope.code != "0" {
        return Err(OkxOrderMutationApiError {
            code: envelope.code,
            message: envelope.msg,
            acknowledgements: envelope.data,
            timing: envelope.timing,
        }
        .into());
    }
    Ok(envelope.data)
}

async fn parse_algo_mutation_response(
    response: reqwest::Response,
    rate_limit_bucket: &RateLimitBucket,
    rate_limit_pacer: &RateLimitPacer,
    gateway_latency_recorder: &OkxGatewayLatencyRecorder,
) -> Result<Vec<OkxAlgoOrderAck>> {
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        rate_limit_pacer.record_rate_limit(rate_limit_bucket)?;
    }
    let body = match read_okx_response_body(response).await {
        Ok(body) => body,
        Err(error) if status == reqwest::StatusCode::TOO_MANY_REQUESTS => {
            bail!(
                "OKX rate limit for {}: HTTP {status}; {error}",
                rate_limit_bucket.key
            );
        }
        Err(error) if !status.is_success() => {
            bail!("OKX HTTP {status}; {error}");
        }
        Err(error) => {
            return Err(error).context("failed reading OKX algo mutation response body");
        }
    };
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        bail!(
            "OKX rate limit for {}: HTTP {status}; {}",
            rate_limit_bucket.key,
            body.summary()
        );
    }
    ensure!(status.is_success(), "OKX HTTP {status}; {}", body.summary());
    let envelope: OkxAlgoMutationEnvelope =
        serde_json::from_slice(body.bytes()).with_context(|| {
            format!(
                "failed parsing OKX algo mutation response body; {}",
                body.summary()
            )
        })?;
    if let Some(timing) = envelope.timing.gateway_latency() {
        emit_okx_gateway_timing(rate_limit_bucket, timing);
        if let Some(summary) = gateway_latency_recorder.record(timing) {
            emit_okx_gateway_latency_summary(summary);
        }
    }
    if is_okx_rate_limit_code(&envelope.code) {
        rate_limit_pacer.record_rate_limit(rate_limit_bucket)?;
        bail!(
            "OKX API rate limit {}: {} ({rate_limit_key})",
            envelope.code,
            envelope.msg,
            rate_limit_key = rate_limit_bucket.key
        );
    }
    if envelope.code != "0" {
        return Err(OkxAlgoMutationApiError {
            code: envelope.code,
            message: envelope.msg,
            acknowledgements: envelope.data,
            timing: envelope.timing,
        }
        .into());
    }
    Ok(envelope.data)
}

async fn parse_account_sizing_response<T>(
    response: reqwest::Response,
    rate_limit_bucket: &RateLimitBucket,
    rate_limit_pacer: &RateLimitPacer,
    gateway_latency_recorder: &OkxGatewayLatencyRecorder,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        rate_limit_pacer.record_rate_limit(rate_limit_bucket)?;
    }
    let body = match read_okx_response_body(response).await {
        Ok(body) => body,
        Err(error) if status == reqwest::StatusCode::TOO_MANY_REQUESTS => {
            bail!(
                "OKX rate limit for {}: HTTP {status}; {error}",
                rate_limit_bucket.key
            );
        }
        Err(error) if !status.is_success() => {
            bail!("OKX account sizing HTTP {status}; {error}");
        }
        Err(error) => {
            return Err(error).context("failed reading OKX account sizing response body");
        }
    };
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        bail!(
            "OKX rate limit for {}: HTTP {status}; {}",
            rate_limit_bucket.key,
            body.summary()
        );
    }
    let envelope: OkxEnvelopeRaw<'_> = serde_json::from_slice(body.bytes()).with_context(|| {
        format!(
            "failed parsing OKX account sizing response body; {}",
            body.summary()
        )
    })?;
    if let Some(timing) = envelope.timing.gateway_latency() {
        emit_okx_gateway_timing(rate_limit_bucket, timing);
        if let Some(summary) = gateway_latency_recorder.record(timing) {
            emit_okx_gateway_latency_summary(summary);
        }
    }
    if !status.is_success() {
        let message = if envelope.code.starts_with("501") {
            "response message omitted"
        } else {
            envelope.msg.as_str()
        };
        bail!(
            "OKX account sizing HTTP {status}: code={:?} msg={:?}",
            envelope.code,
            message
        );
    }
    if is_okx_rate_limit_code(&envelope.code) {
        rate_limit_pacer.record_rate_limit(rate_limit_bucket)?;
        bail!(
            "OKX API rate limit {}: {} ({rate_limit_key})",
            envelope.code,
            envelope.msg,
            rate_limit_key = rate_limit_bucket.key
        );
    }
    if envelope.code != "0" {
        return Err(OkxApiError {
            code: envelope.code,
            message: envelope.msg,
        }
        .into());
    }
    let data = envelope
        .data
        .map(|data| data.get().as_bytes())
        .unwrap_or(b"null");
    serde_json::from_slice(data).with_context(|| {
        format!(
            "failed parsing OKX account sizing response body; {}",
            body.summary()
        )
    })
}

#[derive(Debug)]
struct OkxApiError {
    code: String,
    message: String,
}

impl fmt::Display for OkxApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "OKX API error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for OkxApiError {}

#[derive(Debug)]
struct OkxOrderMutationApiError {
    code: String,
    message: String,
    acknowledgements: Vec<OkxOrderAck>,
    timing: OkxEnvelopeTiming,
}

impl fmt::Display for OkxOrderMutationApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "OKX order API error code={:?} msg={:?}",
            self.code, self.message
        )?;
        if self.acknowledgements.is_empty() {
            write!(formatter, "; data=[]")?;
        }
        for (index, acknowledgement) in self.acknowledgements.iter().enumerate() {
            write!(
                formatter,
                "; item[{index}] sCode={:?} sMsg={:?} subCode={:?} ordId={:?} clOrdId={:?} ts={:?}",
                acknowledgement.status_code,
                acknowledgement.status_message,
                acknowledgement.status_sub_code,
                acknowledgement.order_id,
                acknowledgement.client_order_id,
                acknowledgement.timestamp,
            )?;
        }
        write!(
            formatter,
            "; inTime={:?} outTime={:?}",
            self.timing.in_time_microseconds.as_deref().unwrap_or(""),
            self.timing.out_time_microseconds.as_deref().unwrap_or(""),
        )
    }
}

impl std::error::Error for OkxOrderMutationApiError {}

#[derive(Debug)]
struct OkxAlgoMutationApiError {
    code: String,
    message: String,
    acknowledgements: Vec<OkxAlgoOrderAck>,
    timing: OkxEnvelopeTiming,
}

impl fmt::Display for OkxAlgoMutationApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "OKX algo API error code={:?} msg={:?}",
            self.code, self.message
        )?;
        if self.acknowledgements.is_empty() {
            write!(formatter, "; data=[]")?;
        }
        for (index, acknowledgement) in self.acknowledgements.iter().enumerate() {
            write!(
                formatter,
                "; item[{index}] sCode={:?} sMsg={:?} algoId={:?} algoClOrdId={:?}",
                acknowledgement.status_code,
                acknowledgement.status_message,
                acknowledgement.algo_id,
                acknowledgement.client_order_id,
            )?;
        }
        write!(
            formatter,
            "; inTime={:?} outTime={:?}",
            self.timing.in_time_microseconds.as_deref().unwrap_or(""),
            self.timing.out_time_microseconds.as_deref().unwrap_or(""),
        )
    }
}

impl std::error::Error for OkxAlgoMutationApiError {}

fn order_ack_rejection(acknowledgement: OkxOrderAck) -> anyhow::Error {
    OkxOrderMutationApiError {
        code: "0".to_owned(),
        message: String::new(),
        acknowledgements: vec![acknowledgement],
        timing: OkxEnvelopeTiming::default(),
    }
    .into()
}

fn has_okx_api_error_code(error: &anyhow::Error, expected_code: &str) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<OkxApiError>()
            .is_some_and(|error| error.code == expected_code)
            || source
                .downcast_ref::<OkxOrderMutationApiError>()
                .is_some_and(|error| error.code == expected_code)
            || source
                .downcast_ref::<OkxAlgoMutationApiError>()
                .is_some_and(|error| error.code == expected_code)
    })
}

struct OkxResponseBody {
    bytes: Vec<u8>,
    summary: OkxResponseBodySummary,
}

impl OkxResponseBody {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn summary(&self) -> OkxResponseBodySummary {
        self.summary
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OkxResponseBodySummary {
    Complete { len: usize },
    DeclaredOverLimit { declared_len: u64, limit: usize },
    ReadOverLimit { observed_len: usize, limit: usize },
}

impl fmt::Display for OkxResponseBodySummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Complete { len } => write!(formatter, "response body omitted ({len} bytes)"),
            Self::DeclaredOverLimit {
                declared_len,
                limit,
            } => write!(
                formatter,
                "response body omitted (declared {declared_len} bytes exceeds {limit} byte limit)"
            ),
            Self::ReadOverLimit {
                observed_len,
                limit,
            } => write!(
                formatter,
                "response body omitted (read at least {observed_len} bytes, exceeding {limit} byte limit)"
            ),
        }
    }
}

#[derive(Debug)]
enum OkxResponseBodyReadError {
    Transport(reqwest::Error),
    OverLimit(OkxResponseBodySummary),
}

impl fmt::Display for OkxResponseBodyReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::OverLimit(summary) => write!(formatter, "{summary}"),
        }
    }
}

impl std::error::Error for OkxResponseBodyReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::OverLimit(_) => None,
        }
    }
}

async fn read_okx_response_body(
    mut response: reqwest::Response,
) -> std::result::Result<OkxResponseBody, OkxResponseBodyReadError> {
    if let Some(declared_len) = response.content_length()
        && declared_len > OKX_REST_MAX_RESPONSE_BODY_BYTES as u64
    {
        return Err(OkxResponseBodyReadError::OverLimit(
            OkxResponseBodySummary::DeclaredOverLimit {
                declared_len,
                limit: OKX_REST_MAX_RESPONSE_BODY_BYTES,
            },
        ));
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(OkxResponseBodyReadError::Transport)?
    {
        let next_len = body.len().saturating_add(chunk.len());
        if next_len > OKX_REST_MAX_RESPONSE_BODY_BYTES {
            return Err(OkxResponseBodyReadError::OverLimit(
                OkxResponseBodySummary::ReadOverLimit {
                    observed_len: next_len,
                    limit: OKX_REST_MAX_RESPONSE_BODY_BYTES,
                },
            ));
        }
        body.extend_from_slice(&chunk);
    }

    let summary = OkxResponseBodySummary::Complete { len: body.len() };
    Ok(OkxResponseBody {
        bytes: body,
        summary,
    })
}

fn is_okx_rate_limit_code(code: &str) -> bool {
    OKX_RATE_LIMIT_CODES.contains(&code)
}

fn is_okx_duplicate_algo_client_order_id_code(code: &str) -> bool {
    OKX_DUPLICATE_ALGO_CLIENT_ORDER_ID_CODES.contains(&code)
}

fn current_unix_millis() -> i128 {
    OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000
}

fn parse_unix_millis(context: &str, value: &str) -> Result<i128> {
    ensure!(!value.trim().is_empty(), "{context} must not be empty");
    value
        .parse::<i128>()
        .with_context(|| format!("{context} must be Unix milliseconds: {value}"))
}

fn ensure_fresh_rest_ticker_timestamp(
    timestamp: &str,
    server_now_ms: i128,
    max_staleness: Duration,
) -> Result<()> {
    let generated_at_ms = parse_unix_millis("OKX REST ticker timestamp", timestamp)?;
    ensure_fresh_rest_timestamp(
        "OKX REST ticker timestamp",
        generated_at_ms,
        server_now_ms,
        max_staleness,
    )
}

fn ensure_fresh_rest_timestamp(
    context: &str,
    generated_at_ms: i128,
    server_now_ms: i128,
    max_staleness: Duration,
) -> Result<()> {
    ensure!(
        server_now_ms > 0,
        "synchronized OKX server time must be positive"
    );
    ensure!(generated_at_ms > 0, "{context} must be positive");
    ensure!(
        generated_at_ms <= server_now_ms,
        "{context} is in the future by {} ms",
        generated_at_ms - server_now_ms
    );
    let age_ms = server_now_ms
        .checked_sub(generated_at_ms)
        .with_context(|| format!("{context} age overflowed"))?;
    let max_staleness_ms = i128::try_from(max_staleness.as_millis())
        .with_context(|| format!("{context} maximum staleness is out of range"))?;
    if age_ms > max_staleness_ms {
        bail!(StaleRestTimestampError {
            context: context.to_owned(),
            age_ms,
            max_staleness_ms,
        });
    }
    Ok(())
}

#[derive(Debug)]
struct StaleRestTimestampError {
    context: String,
    age_ms: i128,
    max_staleness_ms: i128,
}

impl fmt::Display for StaleRestTimestampError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} is stale by {} ms; maximum age is {} ms",
            self.context, self.age_ms, self.max_staleness_ms
        )
    }
}

impl std::error::Error for StaleRestTimestampError {}

pub(crate) fn format_okx_timestamp(unix_millis: i128) -> Result<String> {
    let seconds = unix_millis.div_euclid(MILLIS_PER_SECOND);
    let millis = unix_millis.rem_euclid(MILLIS_PER_SECOND);
    let seconds = i64::try_from(seconds).context("OKX timestamp seconds out of range")?;
    let datetime = OffsetDateTime::from_unix_timestamp(seconds)
        .context("OKX timestamp is outside supported range")?;
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        datetime.year(),
        datetime.month() as u8,
        datetime.day(),
        datetime.hour(),
        datetime.minute(),
        datetime.second(),
        millis
    ))
}

pub(crate) fn sign(
    secret: &str,
    timestamp: &str,
    method: &str,
    request_target: &str,
    body: &str,
) -> Result<String> {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).context("invalid OKX API secret")?;
    mac.update(format!("{timestamp}{method}{request_target}{body}").as_bytes());
    Ok(BASE64.encode(mac.finalize().into_bytes()))
}

fn request_target(path: &str, query: Option<&str>) -> String {
    match query {
        Some(query) if !query.is_empty() => format!("{path}?{query}"),
        Some(_) | None => path.to_owned(),
    }
}

fn okx_rate_limit_bucket(
    method: &Method,
    path: &str,
    query: Option<&str>,
    body_inst_id: Option<&str>,
) -> Result<RateLimitBucket> {
    let method = method.as_str();
    let rule = OKX_RATE_LIMIT_RULES
        .iter()
        .find(|rule| rule.method == method && rule.path == path)
        .with_context(|| format!("missing OKX rate limit rule for {method} {path}"))?;
    match rule.scope {
        RateLimitScope::Ip => Ok(ip_scoped_bucket(method, path, rule.limit)),
        RateLimitScope::IpInstrumentType => {
            ip_instrument_type_scoped_bucket(method, path, query, rule.limit)
        }
        RateLimitScope::User => Ok(user_scoped_bucket(method, path, rule.limit)),
        RateLimitScope::UserInstrumentType => {
            user_instrument_type_scoped_bucket(method, path, query, rule.limit)
        }
        RateLimitScope::UserInstrument => {
            user_instrument_scoped_bucket(method, path, query, body_inst_id, rule.limit)
        }
        RateLimitScope::UserTag => Ok(user_tag_scoped_bucket(
            method,
            path,
            OKX_CANCEL_ALL_AFTER_TAG,
            rule.limit,
        )),
    }
}

fn ip_scoped_bucket(method: &str, path: &str, limit: usize) -> RateLimitBucket {
    RateLimitBucket::new(format!("{method} {path}|ip"), limit)
}

fn user_scoped_bucket(method: &str, path: &str, limit: usize) -> RateLimitBucket {
    RateLimitBucket::new(format!("{method} {path}|user"), limit)
}

fn user_instrument_type_scoped_bucket(
    method: &str,
    path: &str,
    query: Option<&str>,
    limit: usize,
) -> Result<RateLimitBucket> {
    let instrument_type = required_rate_limit_value(
        "OKX User ID + Instrument Type rate limit",
        "instType",
        query_value(query, "instType"),
    )?;
    Ok(RateLimitBucket::new(
        format!("{method} {path}|user+instType:{instrument_type}"),
        limit,
    ))
}

fn user_tag_scoped_bucket(method: &str, path: &str, tag: &str, limit: usize) -> RateLimitBucket {
    RateLimitBucket::with_window(
        format!("{method} {path}|user+tag:{tag}"),
        limit,
        OKX_CANCEL_ALL_AFTER_RATE_LIMIT_WINDOW,
    )
}

fn ip_instrument_type_scoped_bucket(
    method: &str,
    path: &str,
    query: Option<&str>,
    limit: usize,
) -> Result<RateLimitBucket> {
    let instrument_type = required_rate_limit_value(
        "OKX IP + Instrument Type rate limit",
        "instType",
        query_value(query, "instType"),
    )?;
    Ok(RateLimitBucket::new(
        format!("{method} {path}|ip+instType:{instrument_type}"),
        limit,
    ))
}

fn user_instrument_scoped_bucket(
    method: &str,
    path: &str,
    query: Option<&str>,
    body_inst_id: Option<&str>,
    limit: usize,
) -> Result<RateLimitBucket> {
    let inst_id = required_rate_limit_value(
        "OKX User ID + Instrument ID rate limit",
        "instId",
        body_inst_id.or_else(|| query_value(query, "instId")),
    )?;
    Ok(RateLimitBucket::new(
        format!("{method} {path}|user+instId:{inst_id}"),
        limit,
    ))
}

fn required_rate_limit_value<'a>(
    context: &str,
    field: &str,
    value: Option<&'a str>,
) -> Result<&'a str> {
    let value = value.with_context(|| format!("{context} requires OKX {field}"))?;
    ensure!(
        !value.trim().is_empty(),
        "{context} requires non-empty OKX {field}"
    );
    Ok(value)
}

fn query_value<'a>(query: Option<&'a str>, name: &str) -> Option<&'a str> {
    query?.split('&').find_map(|field| {
        let (field_name, value) = field.split_once('=')?;
        (field_name == name).then_some(value)
    })
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "client_endpoint_tests.rs"]
mod endpoint_tests;

#[cfg(test)]
#[path = "client_market_data_tests.rs"]
mod market_data_tests;
