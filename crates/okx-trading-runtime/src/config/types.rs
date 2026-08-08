use std::{collections::BTreeMap, fmt, str::FromStr};

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de};
use zeroize::Zeroizing;

const MAX_DECIMAL_FRACTIONAL_DIGITS: usize = 28;

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BotConfig {
    pub product: ProductConfig,
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub okx: Option<OkxConfig>,
    #[serde(default)]
    pub instruments: Vec<InstrumentConfig>,
    #[serde(default)]
    pub strategies: StrategyConfig,
}

impl fmt::Debug for BotConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BotConfig")
            .field("product", &self.product)
            .field("runtime", &self.runtime)
            .field("okx", &self.okx)
            .field("instruments", &self.instruments)
            .field("strategies", &self.strategies)
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProductConfig {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    #[serde(default = "default_trader_id")]
    pub trader_id: String,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_tick_timeout_ms")]
    pub tick_timeout_ms: u64,
    #[serde(default)]
    pub order_intent: Option<RuntimeOrderIntent>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum RuntimeOrderIntent {
    #[serde(rename = "demo-okx-spot-confirmed")]
    DemoOkxSpotConfirmed,
    #[serde(rename = "live-okx-spot-confirmed")]
    LiveOkxSpotConfirmed,
}

#[derive(Clone, PartialEq)]
pub struct OkxConfig {
    pub api_key: Zeroizing<String>,
    pub api_secret: Zeroizing<String>,
    pub api_passphrase: Zeroizing<String>,
    pub account_id: String,
    pub api_domain: OkxApiDomain,
    pub account_jurisdiction: OkxAccountJurisdiction,
    pub trading_service: OkxTradingService,
    pub base_url: String,
    pub base_url_ws_public: Option<String>,
    pub base_url_ws_private: Option<String>,
    pub base_url_ws_business: Option<String>,
    pub proxy_url: Option<String>,
    pub request_timeout_ms: u64,
    pub websocket: OkxWebsocketConfig,
}

impl<'de> Deserialize<'de> for OkxConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawOkxConfig::deserialize(deserializer)?;
        if raw.region.is_some() {
            return Err(de::Error::custom(
                "okx.region is ambiguous and unsupported; replace it with explicit okx.api_domain and okx.account_jurisdiction",
            ));
        }
        let api_domain = raw.api_domain.ok_or_else(|| {
            de::Error::custom("okx.api_domain is required; API transport must be explicit")
        })?;
        let account_jurisdiction = raw.account_jurisdiction.ok_or_else(|| {
            de::Error::custom(
                "okx.account_jurisdiction is required; legal jurisdiction must be explicit",
            )
        })?;
        let defaults = okx_endpoint_defaults(api_domain, raw.trading_service);

        Ok(Self {
            api_key: raw.api_key,
            api_secret: raw.api_secret,
            api_passphrase: raw.api_passphrase,
            account_id: raw.account_id,
            api_domain,
            account_jurisdiction,
            trading_service: raw.trading_service,
            base_url: raw.base_url.unwrap_or_else(|| defaults.base_url.to_owned()),
            base_url_ws_public: Some(
                raw.base_url_ws_public
                    .unwrap_or_else(|| defaults.base_url_ws_public.to_owned()),
            ),
            base_url_ws_private: Some(
                raw.base_url_ws_private
                    .unwrap_or_else(|| defaults.base_url_ws_private.to_owned()),
            ),
            base_url_ws_business: Some(
                raw.base_url_ws_business
                    .unwrap_or_else(|| defaults.base_url_ws_business.to_owned()),
            ),
            proxy_url: raw.proxy_url,
            request_timeout_ms: raw.request_timeout_ms,
            websocket: raw.websocket,
        })
    }
}

impl fmt::Debug for OkxConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OkxConfig")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("api_passphrase", &"<redacted>")
            .field("account_id", &"<redacted>")
            .field("api_domain", &self.api_domain)
            .field("account_jurisdiction", &self.account_jurisdiction)
            .field("trading_service", &self.trading_service)
            .field("base_url", &self.base_url)
            .field("base_url_ws_public", &self.base_url_ws_public)
            .field("base_url_ws_private", &self.base_url_ws_private)
            .field("base_url_ws_business", &self.base_url_ws_business)
            .field("proxy_url", &self.proxy_url)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("websocket", &self.websocket)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOkxConfig {
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    api_passphrase: Zeroizing<String>,
    account_id: String,
    #[serde(default)]
    api_domain: Option<OkxApiDomain>,
    #[serde(default)]
    account_jurisdiction: Option<OkxAccountJurisdiction>,
    #[serde(default)]
    region: Option<de::IgnoredAny>,
    #[serde(default = "default_okx_trading_service")]
    trading_service: OkxTradingService,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    base_url_ws_public: Option<String>,
    #[serde(default)]
    base_url_ws_private: Option<String>,
    #[serde(default)]
    base_url_ws_business: Option<String>,
    #[serde(default)]
    proxy_url: Option<String>,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default)]
    websocket: OkxWebsocketConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OkxApiDomain {
    Global,
    UsAu,
    Eea,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OkxAccountJurisdiction {
    Singapore,
    Eea,
    UnitedStates,
    Australia,
    Other,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OkxTradingService {
    Production,
    Demo,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OkxWebsocketConfig {
    #[serde(default = "default_okx_websocket_max_staleness_ms")]
    pub max_staleness_ms: u64,
    #[serde(default = "default_okx_websocket_reconnect_initial_backoff_ms")]
    pub reconnect_initial_backoff_ms: u64,
    #[serde(default = "default_okx_websocket_reconnect_max_backoff_ms")]
    pub reconnect_max_backoff_ms: u64,
}

impl Default for OkxWebsocketConfig {
    fn default() -> Self {
        Self {
            max_staleness_ms: default_okx_websocket_max_staleness_ms(),
            reconnect_initial_backoff_ms: default_okx_websocket_reconnect_initial_backoff_ms(),
            reconnect_max_backoff_ms: default_okx_websocket_reconnect_max_backoff_ms(),
        }
    }
}

#[derive(Clone, Copy)]
struct OkxEndpointDefaults {
    base_url: &'static str,
    base_url_ws_public: &'static str,
    base_url_ws_private: &'static str,
    base_url_ws_business: &'static str,
}

struct OkxEndpointDefaultRoute {
    api_domain: OkxApiDomain,
    trading_service: OkxTradingService,
    defaults: OkxEndpointDefaults,
}

// Reverified 2026-07-20 against the official OKX API v5 regional-domain
// reference and change log. These are API transports only. In particular,
// my.okx.com is a shared Singapore/EEA web-service domain and is never an API
// transport or account-jurisdiction signal.
const OKX_ENDPOINT_DEFAULT_ROUTES: [OkxEndpointDefaultRoute; 6] = [
    OkxEndpointDefaultRoute {
        api_domain: OkxApiDomain::Global,
        trading_service: OkxTradingService::Production,
        defaults: OkxEndpointDefaults {
            base_url: "https://openapi.okx.com",
            base_url_ws_public: "wss://ws.okx.com:8443/ws/v5/public",
            base_url_ws_private: "wss://ws.okx.com:8443/ws/v5/private",
            base_url_ws_business: "wss://ws.okx.com:8443/ws/v5/business",
        },
    },
    OkxEndpointDefaultRoute {
        api_domain: OkxApiDomain::Global,
        trading_service: OkxTradingService::Demo,
        defaults: OkxEndpointDefaults {
            base_url: "https://openapi.okx.com",
            base_url_ws_public: "wss://wspap.okx.com:8443/ws/v5/public",
            base_url_ws_private: "wss://wspap.okx.com:8443/ws/v5/private",
            base_url_ws_business: "wss://wspap.okx.com:8443/ws/v5/business",
        },
    },
    OkxEndpointDefaultRoute {
        api_domain: OkxApiDomain::UsAu,
        trading_service: OkxTradingService::Production,
        defaults: OkxEndpointDefaults {
            base_url: "https://us.okx.com",
            base_url_ws_public: "wss://wsus.okx.com:8443/ws/v5/public",
            base_url_ws_private: "wss://wsus.okx.com:8443/ws/v5/private",
            base_url_ws_business: "wss://wsus.okx.com:8443/ws/v5/business",
        },
    },
    OkxEndpointDefaultRoute {
        api_domain: OkxApiDomain::UsAu,
        trading_service: OkxTradingService::Demo,
        defaults: OkxEndpointDefaults {
            base_url: "https://us.okx.com",
            base_url_ws_public: "wss://wsuspap.okx.com:8443/ws/v5/public",
            base_url_ws_private: "wss://wsuspap.okx.com:8443/ws/v5/private",
            base_url_ws_business: "wss://wsuspap.okx.com:8443/ws/v5/business",
        },
    },
    OkxEndpointDefaultRoute {
        api_domain: OkxApiDomain::Eea,
        trading_service: OkxTradingService::Production,
        defaults: OkxEndpointDefaults {
            base_url: "https://eea.okx.com",
            base_url_ws_public: "wss://wseea.okx.com:8443/ws/v5/public",
            base_url_ws_private: "wss://wseea.okx.com:8443/ws/v5/private",
            base_url_ws_business: "wss://wseea.okx.com:8443/ws/v5/business",
        },
    },
    OkxEndpointDefaultRoute {
        api_domain: OkxApiDomain::Eea,
        trading_service: OkxTradingService::Demo,
        defaults: OkxEndpointDefaults {
            base_url: "https://eea.okx.com",
            base_url_ws_public: "wss://wseeapap.okx.com:8443/ws/v5/public",
            base_url_ws_private: "wss://wseeapap.okx.com:8443/ws/v5/private",
            base_url_ws_business: "wss://wseeapap.okx.com:8443/ws/v5/business",
        },
    },
];

fn okx_endpoint_defaults(
    api_domain: OkxApiDomain,
    trading_service: OkxTradingService,
) -> OkxEndpointDefaults {
    OKX_ENDPOINT_DEFAULT_ROUTES
        .iter()
        .find(|route| route.api_domain == api_domain && route.trading_service == trading_service)
        .map(|route| route.defaults)
        .expect("every supported OKX API domain/trading service pair must have endpoint defaults")
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InstrumentConfig {
    pub instrument_id: RequestedInstrumentId,
    pub base_currency: String,
    pub quote_currency: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl InstrumentConfig {
    pub fn okx_instrument_id(&self) -> String {
        self.instrument_id.as_str().to_owned()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RequestedInstrumentId(String);

impl RequestedInstrumentId {
    pub fn new(value: String) -> Result<Self, String> {
        if value.is_empty() {
            return Err("requested instrument must not be empty".to_owned());
        }
        if value != value.trim() {
            return Err(
                "requested instrument must not contain leading or trailing whitespace".to_owned(),
            );
        }
        if value.len() > 64 || !value.is_ascii() || value.chars().any(char::is_control) {
            return Err(
                "requested instrument must be canonical printable ASCII of at most 64 bytes"
                    .to_owned(),
            );
        }
        let Some((base, quote)) = value.split_once('-') else {
            return Err("requested instrument must use exact OKX BASE-QUOTE format".to_owned());
        };
        if base.is_empty()
            || quote.is_empty()
            || quote.contains('-')
            || !base
                .chars()
                .chain(quote.chars())
                .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
        {
            return Err(
                "requested instrument must use uppercase OKX BASE-QUOTE asset codes".to_owned(),
            );
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RequestedInstrumentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl fmt::Display for RequestedInstrumentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
pub enum RequestedInstrumentType {
    #[serde(rename = "SPOT")]
    Spot,
    #[serde(rename = "MARGIN")]
    Margin,
    #[serde(rename = "SWAP")]
    Swap,
    #[serde(rename = "FUTURES")]
    Futures,
    #[serde(rename = "OPTION")]
    Option,
    #[serde(rename = "EVENTS")]
    Events,
}

impl RequestedInstrumentType {
    pub const fn as_okx(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
            Self::Margin => "MARGIN",
            Self::Swap => "SWAP",
            Self::Futures => "FUTURES",
            Self::Option => "OPTION",
            Self::Events => "EVENTS",
        }
    }
}

impl fmt::Display for RequestedInstrumentType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_okx())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
pub enum RequestedTradeMode {
    #[serde(rename = "cash")]
    Cash,
    #[serde(rename = "cross")]
    Cross,
    #[serde(rename = "isolated")]
    Isolated,
    #[serde(rename = "spot_isolated")]
    SpotIsolated,
}

impl RequestedTradeMode {
    pub const fn as_okx(self) -> &'static str {
        match self {
            Self::Cash => "cash",
            Self::Cross => "cross",
            Self::Isolated => "isolated",
            Self::SpotIsolated => "spot_isolated",
        }
    }
}

impl fmt::Display for RequestedTradeMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_okx())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RequestedTradingInstrument {
    pub instrument: RequestedInstrumentId,
    pub inst_type: RequestedInstrumentType,
    pub td_mode: RequestedTradeMode,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    #[serde(default)]
    pub instances: Vec<StrategyInstanceConfig>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StrategyInstanceConfig {
    pub kind: StrategyKind,
    pub id: String,
    pub enabled: bool,
    pub trading_instrument: RequestedTradingInstrument,
    pub bar: String,
    pub params: StrategyParamsConfig,
}

impl StrategyInstanceConfig {
    pub fn instrument_id(&self) -> &str {
        self.trading_instrument.instrument.as_str()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    OkxEmaAtrMakerTrend,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StrategyParamsConfig {
    OkxEmaAtrMakerTrend(OkxEmaAtrMakerTrendConfig),
}

impl StrategyParamsConfig {
    pub const fn okx_ema_atr_maker_trend(&self) -> &OkxEmaAtrMakerTrendConfig {
        match self {
            Self::OkxEmaAtrMakerTrend(config) => config,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStrategyInstanceConfig {
    kind: StrategyKind,
    id: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    instrument: RequestedInstrumentId,
    inst_type: RequestedInstrumentType,
    td_mode: RequestedTradeMode,
    bar: String,
    params: toml::Value,
}

impl<'de> Deserialize<'de> for StrategyInstanceConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        RawStrategyInstanceConfig::deserialize(deserializer)?
            .try_into()
            .map_err(de::Error::custom)
    }
}

impl TryFrom<RawStrategyInstanceConfig> for StrategyInstanceConfig {
    type Error = String;

    fn try_from(raw: RawStrategyInstanceConfig) -> Result<Self, Self::Error> {
        let params = match raw.kind {
            StrategyKind::OkxEmaAtrMakerTrend => raw
                .params
                .try_into()
                .map(StrategyParamsConfig::OkxEmaAtrMakerTrend)
                .map_err(|err| format!("invalid okx_ema_atr_maker_trend params: {err}"))?,
        };

        Ok(Self {
            kind: raw.kind,
            id: raw.id,
            enabled: raw.enabled,
            trading_instrument: RequestedTradingInstrument {
                instrument: raw.instrument,
                inst_type: raw.inst_type,
                td_mode: raw.td_mode,
            },
            bar: raw.bar,
            params,
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OkxEmaAtrMakerTrendConfig {
    #[serde(default = "default_fast_ema_period")]
    pub fast_ema_period: usize,
    #[serde(default = "default_slow_ema_period")]
    pub slow_ema_period: usize,
    #[serde(default = "default_atr_period")]
    pub atr_period: usize,
    #[serde(
        default = "default_order_quantity",
        deserialize_with = "deserialize_decimal_string"
    )]
    pub quantity: Decimal,
    #[serde(default, deserialize_with = "deserialize_decimal_string")]
    pub operator_owned_base_balance: Decimal,
    #[serde(default = "default_max_entry_order_age_ms")]
    pub max_entry_order_age_ms: u64,
    #[serde(default, deserialize_with = "deserialize_optional_decimal_string")]
    pub max_quote_notional: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_decimal_string_map")]
    pub max_quote_notional_by_instrument: BTreeMap<String, Decimal>,
    #[serde(
        default = "default_entry_offset_atr_multiple",
        deserialize_with = "deserialize_decimal_string"
    )]
    pub entry_offset_atr_multiple: Decimal,
    #[serde(
        default = "default_min_entry_offset_bps",
        deserialize_with = "deserialize_decimal_string"
    )]
    pub min_entry_offset_bps: Decimal,
    #[serde(
        default = "default_max_entry_offset_bps",
        deserialize_with = "deserialize_decimal_string"
    )]
    pub max_entry_offset_bps: Decimal,
    #[serde(
        default = "default_take_profit_atr_multiple",
        deserialize_with = "deserialize_decimal_string"
    )]
    pub take_profit_atr_multiple: Decimal,
    #[serde(
        default = "default_stop_loss_atr_multiple",
        deserialize_with = "deserialize_decimal_string"
    )]
    pub stop_loss_atr_multiple: Decimal,
}

impl OkxEmaAtrMakerTrendConfig {
    const DEFAULT_FAST_EMA_PERIOD: usize = 20;
    const DEFAULT_SLOW_EMA_PERIOD: usize = 100;
    const DEFAULT_ATR_PERIOD: usize = 14;
    const DEFAULT_MAX_QUOTE_NOTIONAL: Option<Decimal> = None;
    const DEFAULT_MAX_ENTRY_ORDER_AGE_MS: u64 = 15_000;

    fn default_quantity() -> Decimal {
        Decimal::new(1, 3)
    }

    fn default_entry_offset_atr_multiple() -> Decimal {
        Decimal::new(1, 1)
    }

    fn default_min_entry_offset_bps() -> Decimal {
        Decimal::ONE
    }

    fn default_max_entry_offset_bps() -> Decimal {
        Decimal::new(150, 1)
    }

    fn default_take_profit_atr_multiple() -> Decimal {
        Decimal::new(2, 0)
    }

    fn default_stop_loss_atr_multiple() -> Decimal {
        Decimal::new(15, 1)
    }
}

impl Default for OkxEmaAtrMakerTrendConfig {
    fn default() -> Self {
        Self {
            fast_ema_period: Self::DEFAULT_FAST_EMA_PERIOD,
            slow_ema_period: Self::DEFAULT_SLOW_EMA_PERIOD,
            atr_period: Self::DEFAULT_ATR_PERIOD,
            quantity: Self::default_quantity(),
            operator_owned_base_balance: Decimal::ZERO,
            max_entry_order_age_ms: Self::DEFAULT_MAX_ENTRY_ORDER_AGE_MS,
            max_quote_notional: Self::DEFAULT_MAX_QUOTE_NOTIONAL,
            max_quote_notional_by_instrument: BTreeMap::new(),
            entry_offset_atr_multiple: Self::default_entry_offset_atr_multiple(),
            min_entry_offset_bps: Self::default_min_entry_offset_bps(),
            max_entry_offset_bps: Self::default_max_entry_offset_bps(),
            take_profit_atr_multiple: Self::default_take_profit_atr_multiple(),
            stop_loss_atr_multiple: Self::default_stop_loss_atr_multiple(),
        }
    }
}

fn deserialize_decimal_string<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    DecimalString::deserialize(deserializer).map(DecimalString::into_decimal)
}

fn deserialize_optional_decimal_string<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<DecimalString>::deserialize(deserializer)
        .map(|value| value.map(DecimalString::into_decimal))
}

fn deserialize_decimal_string_map<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = BTreeMap::<String, DecimalString>::deserialize(deserializer)?;
    Ok(values
        .into_iter()
        .map(|(key, value)| (key, value.into_decimal()))
        .collect())
}

struct DecimalString(Decimal);

impl DecimalString {
    fn parse(value: &str) -> Result<Self, String> {
        parse_decimal_config_string(value).map(Self)
    }

    fn into_decimal(self) -> Decimal {
        self.0
    }
}

impl<'de> Deserialize<'de> for DecimalString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(DecimalStringVisitor)
    }
}

struct DecimalStringVisitor;

impl<'de> de::Visitor<'de> for DecimalStringVisitor {
    type Value = DecimalString;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a non-empty decimal string without surrounding whitespace")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        DecimalString::parse(value).map_err(E::custom)
    }
}

#[derive(Clone, Copy)]
enum DecimalStringViolation {
    Empty,
    SurroundingWhitespace,
    ScientificNotation,
    TooManyFractionalDigits,
}

impl DecimalStringViolation {
    fn detect(value: &str) -> Option<Self> {
        if value.is_empty() {
            Some(Self::Empty)
        } else if value != value.trim() {
            Some(Self::SurroundingWhitespace)
        } else if value.contains(['e', 'E']) {
            Some(Self::ScientificNotation)
        } else if value
            .split_once('.')
            .is_some_and(|(_, fractional)| fractional.len() > MAX_DECIMAL_FRACTIONAL_DIGITS)
        {
            Some(Self::TooManyFractionalDigits)
        } else {
            None
        }
    }

    fn message(self) -> String {
        match self {
            Self::Empty => "decimal string must not be empty".to_owned(),
            Self::SurroundingWhitespace => {
                "decimal string must not contain leading or trailing whitespace".to_owned()
            }
            Self::ScientificNotation => "decimal string must use plain decimal notation".to_owned(),
            Self::TooManyFractionalDigits => format!(
                "decimal string must not exceed {MAX_DECIMAL_FRACTIONAL_DIGITS} fractional digits"
            ),
        }
    }
}

fn parse_decimal_config_string(value: &str) -> Result<Decimal, String> {
    if let Some(violation) = DecimalStringViolation::detect(value) {
        return Err(violation.message());
    }
    Decimal::from_str(value).map_err(|err| format!("invalid decimal string {value:?}: {err}"))
}

fn default_trader_id() -> String {
    "PUBLIC-DEMO-OPERATOR".to_owned()
}

fn default_poll_interval_ms() -> u64 {
    2_000
}

fn default_tick_timeout_ms() -> u64 {
    5_000
}

fn default_request_timeout_ms() -> u64 {
    60_000
}

fn default_okx_websocket_max_staleness_ms() -> u64 {
    3_000
}

fn default_okx_websocket_reconnect_initial_backoff_ms() -> u64 {
    500
}

fn default_okx_websocket_reconnect_max_backoff_ms() -> u64 {
    10_000
}

fn default_okx_trading_service() -> OkxTradingService {
    OkxTradingService::Production
}

fn default_enabled() -> bool {
    true
}

fn default_order_quantity() -> Decimal {
    OkxEmaAtrMakerTrendConfig::default_quantity()
}

fn default_fast_ema_period() -> usize {
    OkxEmaAtrMakerTrendConfig::DEFAULT_FAST_EMA_PERIOD
}

fn default_slow_ema_period() -> usize {
    OkxEmaAtrMakerTrendConfig::DEFAULT_SLOW_EMA_PERIOD
}

fn default_atr_period() -> usize {
    OkxEmaAtrMakerTrendConfig::DEFAULT_ATR_PERIOD
}

fn default_max_entry_order_age_ms() -> u64 {
    OkxEmaAtrMakerTrendConfig::DEFAULT_MAX_ENTRY_ORDER_AGE_MS
}

fn default_entry_offset_atr_multiple() -> Decimal {
    OkxEmaAtrMakerTrendConfig::default_entry_offset_atr_multiple()
}

fn default_min_entry_offset_bps() -> Decimal {
    OkxEmaAtrMakerTrendConfig::default_min_entry_offset_bps()
}

fn default_max_entry_offset_bps() -> Decimal {
    OkxEmaAtrMakerTrendConfig::default_max_entry_offset_bps()
}

fn default_take_profit_atr_multiple() -> Decimal {
    OkxEmaAtrMakerTrendConfig::default_take_profit_atr_multiple()
}

fn default_stop_loss_atr_multiple() -> Decimal {
    OkxEmaAtrMakerTrendConfig::default_stop_loss_atr_multiple()
}
