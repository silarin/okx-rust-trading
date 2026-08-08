use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail, ensure};
use rust_decimal::Decimal;

use super::types::{
    BotConfig, InstrumentConfig, OkxApiDomain, OkxConfig, OkxEmaAtrMakerTrendConfig,
    OkxTradingService, OkxWebsocketConfig, RequestedInstrumentType, RequestedTradeMode,
    RequestedTradingInstrument, RuntimeConfig, RuntimeOrderIntent, StrategyInstanceConfig,
    StrategyKind, StrategyParamsConfig,
};
use crate::strategies::okx_ema_atr_maker_trend::{
    OKX_EMA_ATR_MAKER_TREND_BAR, strategy_ownership_tag_for_config,
};

const MIN_POLL_INTERVAL_MS: u64 = 250;
const CANCEL_ALL_AFTER_REFRESH_MULTIPLIER: u64 = 3;
const MILLIS_PER_SECOND: u64 = 1_000;
const OKX_CANCEL_ALL_AFTER_MIN_TIMEOUT_SECS: u64 = 10;
const OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS: u64 = 120;
const OKX_EMA_ATR_MAKER_TREND_QUALIFIED_INSTRUMENT: &str = "BTC-USDT";

impl BotConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.product.name == "okx-rust-trading",
            "product.name must be \"okx-rust-trading\""
        );
        let Some(okx) = &self.okx else {
            bail!("runtime profile requires [okx]");
        };

        validate_runtime(&self.runtime)?;
        validate_runtime_order_intent(self, okx.trading_service)?;
        validate_strategy_cancel_all_after_timing(self)?;
        self.validate_crypto_instruments()?;
        validate_okx(self)?;
        validate_strategy_instances(self)?;
        validate_live_strategy_quote_notional_denomination(self)?;
        Ok(())
    }

    fn validate_crypto_instruments(&self) -> Result<()> {
        ensure!(
            !self.instruments.is_empty(),
            "at least one OKX spot instrument must be configured"
        );
        ensure!(
            self.instruments.iter().any(|instrument| instrument.enabled),
            "at least one OKX spot instrument must be enabled"
        );

        for instrument in &self.instruments {
            validate_instrument(instrument)?;
        }
        validate_unique_enabled_instrument_ids(&self.instruments)?;

        Ok(())
    }
}

fn validate_runtime(runtime: &RuntimeConfig) -> Result<()> {
    ensure!(
        runtime.poll_interval_ms >= MIN_POLL_INTERVAL_MS,
        "runtime.poll_interval_ms must be at least {MIN_POLL_INTERVAL_MS}"
    );
    ensure!(
        runtime.tick_timeout_ms > 0,
        "runtime.tick_timeout_ms must be positive"
    );
    ensure!(
        runtime.tick_timeout_ms >= runtime.poll_interval_ms,
        "runtime.tick_timeout_ms must be greater than or equal to runtime.poll_interval_ms"
    );
    Ok(())
}

fn validate_runtime_order_intent(
    config: &BotConfig,
    trading_service: OkxTradingService,
) -> Result<()> {
    let order_intent = config.runtime.order_intent;
    if has_enabled_strategy_instances(config) {
        validate_strategy_order_intent(order_intent, trading_service)
    } else if let Some(order_intent) = order_intent {
        validate_order_intent_matches_trading_service(order_intent, trading_service)
    } else {
        Ok(())
    }
}

fn validate_strategy_cancel_all_after_timing(config: &BotConfig) -> Result<()> {
    let enabled_strategy_count = enabled_strategy_instances(config).count();
    if enabled_strategy_count > 0 {
        validate_aggregate_dispatch_budget_against_cancel_all_after(
            enabled_strategy_count,
            config.runtime.poll_interval_ms,
            config.runtime.tick_timeout_ms,
        )?;
    }
    Ok(())
}

fn has_enabled_strategy_instances(config: &BotConfig) -> bool {
    enabled_strategy_instances(config).next().is_some()
}

fn validate_order_intent_matches_trading_service(
    order_intent: RuntimeOrderIntent,
    trading_service: OkxTradingService,
) -> Result<()> {
    match (trading_service, order_intent) {
        (OkxTradingService::Demo, RuntimeOrderIntent::DemoOkxSpotConfirmed)
        | (OkxTradingService::Production, RuntimeOrderIntent::LiveOkxSpotConfirmed) => Ok(()),
        (OkxTradingService::Demo, RuntimeOrderIntent::LiveOkxSpotConfirmed) => {
            bail!("runtime.order_intent live-okx-spot-confirmed is not valid for OKX DEMO profiles")
        }
        (OkxTradingService::Production, RuntimeOrderIntent::DemoOkxSpotConfirmed) => {
            bail!(
                "runtime.order_intent demo-okx-spot-confirmed is not valid for OKX PRODUCTION profiles"
            )
        }
    }
}

fn validate_strategy_order_intent(
    order_intent: Option<RuntimeOrderIntent>,
    trading_service: OkxTradingService,
) -> Result<()> {
    let Some(order_intent) = order_intent else {
        match trading_service {
            OkxTradingService::Demo => bail!(
                "strategy-enabled OKX DEMO profiles require runtime.order_intent = \"demo-okx-spot-confirmed\" before order-capable startup"
            ),
            OkxTradingService::Production => bail!(
                "strategy-enabled OKX PRODUCTION profiles require runtime.order_intent = \"live-okx-spot-confirmed\" before order-capable startup"
            ),
        }
    };
    validate_order_intent_matches_trading_service(order_intent, trading_service)
}

fn validate_aggregate_dispatch_budget_against_cancel_all_after(
    enabled_strategy_count: usize,
    poll_interval_ms: u64,
    tick_timeout_ms: u64,
) -> Result<()> {
    let cancel_all_after_timeout_ms = cancel_all_after_timeout_seconds(poll_interval_ms)?
        .checked_mul(MILLIS_PER_SECOND)
        .context("OKX cancel-all-after timeout is too large for millisecond validation")?;
    let aggregate_worst_case_ms =
        checked_aggregate_strategy_dispatch_budget_ms(enabled_strategy_count, tick_timeout_ms)
            .with_context(|| {
                format!(
                    "aggregate strategy dispatch budget overflowed: enabled_strategy_count={enabled_strategy_count}, per_strategy_timeout_ms={tick_timeout_ms}, aggregate_worst_case_ms=overflow, cancel_all_after_window_ms={cancel_all_after_timeout_ms}"
                )
            })?;
    ensure!(
        aggregate_worst_case_ms <= cancel_all_after_timeout_ms,
        "aggregate strategy dispatch budget exceeds the OKX Cancel-All-After safety window: enabled_strategy_count={enabled_strategy_count}, per_strategy_timeout_ms={tick_timeout_ms}, aggregate_worst_case_ms={aggregate_worst_case_ms}, cancel_all_after_window_ms={cancel_all_after_timeout_ms}"
    );
    Ok(())
}

fn checked_aggregate_strategy_dispatch_budget_ms(
    enabled_strategy_count: usize,
    tick_timeout_ms: u64,
) -> Option<u64> {
    u64::try_from(enabled_strategy_count)
        .ok()?
        .checked_mul(2)?
        .checked_mul(tick_timeout_ms)
}

fn cancel_all_after_timeout_seconds(poll_interval_ms: u64) -> Result<u64> {
    let poll_interval_secs = poll_interval_ms.div_ceil(MILLIS_PER_SECOND);
    let requested_timeout = poll_interval_secs
        .checked_mul(CANCEL_ALL_AFTER_REFRESH_MULTIPLIER)
        .context("runtime.poll_interval_ms is too large for OKX cancel-all-after timeout")?
        .max(OKX_CANCEL_ALL_AFTER_MIN_TIMEOUT_SECS);
    ensure!(
        requested_timeout <= OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS,
        "runtime.poll_interval_ms {poll_interval_ms} is too large for OKX cancel-all-after refresh"
    );
    Ok(requested_timeout)
}

fn validate_instrument(instrument: &InstrumentConfig) -> Result<()> {
    validate_okx_spot_symbol(
        "instrument instrument_id",
        instrument.instrument_id.as_str(),
    )?;
    validate_okx_asset_code("instrument base_currency", &instrument.base_currency)?;
    validate_okx_asset_code("instrument quote_currency", &instrument.quote_currency)?;
    ensure!(
        instrument.base_currency != instrument.quote_currency,
        "instrument base_currency {} must not equal quote_currency {}",
        instrument.base_currency,
        instrument.quote_currency
    );
    reject_okx_derivative_marker("instrument base_currency", &instrument.base_currency)?;
    reject_okx_derivative_marker("instrument quote_currency", &instrument.quote_currency)?;
    let expected_instrument_id =
        format!("{}-{}", instrument.base_currency, instrument.quote_currency);
    ensure!(
        instrument.instrument_id.as_str() == expected_instrument_id,
        "instrument instrument_id {} must exactly match configured base_currency and quote_currency as {expected_instrument_id}",
        instrument.instrument_id
    );
    Ok(())
}

fn validate_unique_enabled_instrument_ids(instruments: &[InstrumentConfig]) -> Result<()> {
    let mut seen = HashSet::new();
    for instrument in instruments.iter().filter(|instrument| instrument.enabled) {
        let instrument_id = instrument.okx_instrument_id();
        ensure!(
            seen.insert(instrument_id.clone()),
            "enabled OKX spot instrument_ids must be unique; duplicate {instrument_id}"
        );
    }
    Ok(())
}

fn validate_live_strategy_quote_notional_denomination(config: &BotConfig) -> Result<()> {
    for instance in enabled_strategy_instances(config) {
        match instance.kind {
            StrategyKind::OkxEmaAtrMakerTrend => {
                validate_live_quote_notional_denomination(
                    "OkxEmaAtrMakerTrend",
                    instance,
                    strategy_max_quote_notional(&instance.params),
                    strategy_max_quote_notional_by_instrument(&instance.params),
                )?;
            }
        }
    }

    Ok(())
}

fn validate_live_quote_notional_denomination(
    strategy_name: &str,
    instance: &StrategyInstanceConfig,
    max_quote_notional: Option<Decimal>,
    max_quote_notional_by_instrument: &std::collections::BTreeMap<String, Decimal>,
) -> Result<()> {
    if max_quote_notional.is_none() && max_quote_notional_by_instrument.is_empty() {
        return Ok(());
    }

    let mut selected_quotes = HashSet::new();
    let instrument_id = instance.instrument_id();
    {
        let quote = okx_spot_quote_asset(&format!("{strategy_name} instrument_id"), instrument_id)?;
        selected_quotes.insert(quote);
    }

    if max_quote_notional.is_none() {
        ensure!(
            max_quote_notional_by_instrument.contains_key(instrument_id),
            "OKX {strategy_name} profiles using max_quote_notional_by_instrument without shared max_quote_notional must configure a cap for selected instrument {instrument_id}",
        );
    }

    if selected_quotes.len() <= 1 {
        return Ok(());
    }

    ensure!(
        max_quote_notional.is_none(),
        "OKX mixed-quote {strategy_name} profiles must use max_quote_notional_by_instrument instead of shared max_quote_notional",
    );
    ensure!(
        max_quote_notional_by_instrument.contains_key(instrument_id),
        "OKX mixed-quote {strategy_name} profiles must configure max_quote_notional_by_instrument for selected instrument {instrument_id}",
    );

    Ok(())
}

fn validate_okx(config: &BotConfig) -> Result<()> {
    let Some(okx) = &config.okx else {
        return Ok(());
    };

    validate_okx_secret_field("OKX api_key", &okx.api_key)?;
    validate_okx_secret_field("OKX api_secret", &okx.api_secret)?;
    validate_okx_secret_field("OKX passphrase", &okx.api_passphrase)?;
    ensure!(
        !okx.account_id.trim().is_empty(),
        "OKX account_id must not be empty"
    );
    validate_okx_account_id(&okx.account_id)?;
    ensure!(
        !okx.base_url.trim().is_empty(),
        "OKX base_url must not be empty"
    );
    validate_https_base_url("OKX base_url", &okx.base_url)?;
    validate_optional_wss_url("OKX base_url_ws_public", &okx.base_url_ws_public)?;
    validate_optional_wss_url("OKX base_url_ws_private", &okx.base_url_ws_private)?;
    validate_optional_wss_url("OKX base_url_ws_business", &okx.base_url_ws_business)?;
    validate_okx_api_domain_routing(okx)?;
    validate_okx_websocket_service_routing(okx)?;
    validate_optional_proxy_url("OKX proxy_url", &okx.proxy_url)?;
    validate_okx_websocket_config(&okx.websocket)?;
    ensure!(
        okx.request_timeout_ms > 0,
        "OKX request_timeout_ms must be non-zero"
    );
    Ok(())
}

fn validate_okx_api_domain_routing(okx: &OkxConfig) -> Result<()> {
    validate_okx_api_domain_url("OKX base_url", &okx.base_url, okx.api_domain)?;
    for (context, value) in [
        ("OKX base_url_ws_public", okx.base_url_ws_public.as_deref()),
        (
            "OKX base_url_ws_private",
            okx.base_url_ws_private.as_deref(),
        ),
        (
            "OKX base_url_ws_business",
            okx.base_url_ws_business.as_deref(),
        ),
    ] {
        if let Some(value) = value {
            validate_okx_api_domain_url(context, value, okx.api_domain)?;
        }
    }
    Ok(())
}

fn validate_okx_api_domain_url(context: &str, value: &str, expected: OkxApiDomain) -> Result<()> {
    let host = url::Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()))
        .context("validated OKX route must include a host")?;
    ensure!(
        host != "my.okx.com",
        "{context} must not use my.okx.com; the shared Singapore/EEA web-service domain is not an API transport or jurisdiction signal"
    );
    if let Some(actual) = known_okx_api_domain(&host) {
        ensure!(
            actual == expected,
            "known OKX API host {host} must match okx.api_domain {expected:?}"
        );
    }
    Ok(())
}

fn known_okx_api_domain(host: &str) -> Option<OkxApiDomain> {
    match host {
        "openapi.okx.com" | "www.okx.com" | "ws.okx.com" | "wspap.okx.com" => {
            Some(OkxApiDomain::Global)
        }
        "us.okx.com" | "wsus.okx.com" | "wsuspap.okx.com" => Some(OkxApiDomain::UsAu),
        "eea.okx.com" | "wseea.okx.com" | "wseeapap.okx.com" => Some(OkxApiDomain::Eea),
        _ => None,
    }
}

fn validate_okx_websocket_config(config: &OkxWebsocketConfig) -> Result<()> {
    ensure!(
        config.max_staleness_ms > 0,
        "OKX websocket.max_staleness_ms must be non-zero"
    );
    ensure!(
        config.reconnect_initial_backoff_ms > 0,
        "OKX websocket.reconnect_initial_backoff_ms must be non-zero"
    );
    ensure!(
        config.reconnect_max_backoff_ms >= config.reconnect_initial_backoff_ms,
        "OKX websocket.reconnect_max_backoff_ms must be greater than or equal to reconnect_initial_backoff_ms"
    );
    Ok(())
}

fn validate_okx_secret_field(context: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    ensure!(!trimmed.is_empty(), "{context} must not be empty");
    ensure!(
        value == trimmed,
        "{context} must not contain leading or trailing whitespace"
    );
    Ok(())
}

fn validate_okx_account_id(value: &str) -> Result<()> {
    let account_id = value.trim();
    ensure!(
        account_id == value,
        "OKX account_id must not contain leading or trailing whitespace"
    );
    ensure!(
        account_id.starts_with("OKX-"),
        "OKX account_id must use an OKX-prefixed identifier such as OKX-PUBLIC-DEMO"
    );
    ensure!(
        account_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')),
        "OKX account_id may contain only ASCII letters, digits, '-', '_' or '.'"
    );
    Ok(())
}

fn validate_okx_websocket_service_routing(okx: &OkxConfig) -> Result<()> {
    let urls = [
        okx.base_url_ws_public.as_deref(),
        okx.base_url_ws_private.as_deref(),
        okx.base_url_ws_business.as_deref(),
    ];
    let has_demo_url = urls
        .iter()
        .flatten()
        .filter_map(|value| okx_websocket_service(value))
        .any(|service| service == OkxTradingService::Demo);
    let has_production_url = urls
        .iter()
        .flatten()
        .filter_map(|value| okx_websocket_service(value))
        .any(|service| service == OkxTradingService::Production);

    ensure!(
        !(has_demo_url && has_production_url),
        "OKX demo and production WS routing must not be mixed; all configured okx.base_url_ws_* values must use one OKX service"
    );
    for value in urls.iter().flatten() {
        let Some(websocket_service) = okx_websocket_service(value) else {
            continue;
        };
        ensure!(
            websocket_service == okx.trading_service,
            "known OKX WebSocket host {value} must match okx.trading_service {:?}",
            okx.trading_service
        );
    }
    Ok(())
}

fn okx_websocket_service(value: &str) -> Option<OkxTradingService> {
    url::Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()))
        .and_then(|host| {
            if host.ends_with("pap.okx.com") {
                Some(OkxTradingService::Demo)
            } else if host == "okx.com" || host.ends_with(".okx.com") {
                Some(OkxTradingService::Production)
            } else {
                None
            }
        })
}

pub(crate) fn okx_simulated_trading_from_routing(okx: &OkxConfig) -> bool {
    okx.trading_service == OkxTradingService::Demo
}

fn validate_https_base_url(context: &str, value: &str) -> Result<()> {
    validate_url_with_scheme(context, value, "https")
}

fn validate_optional_wss_url(context: &str, value: &Option<String>) -> Result<()> {
    if let Some(value) = value {
        ensure!(!value.trim().is_empty(), "{context} must not be empty");
        validate_url_with_scheme(context, value, "wss")?;
    }
    Ok(())
}

fn validate_optional_proxy_url(context: &str, value: &Option<String>) -> Result<()> {
    if let Some(value) = value {
        ensure!(!value.trim().is_empty(), "{context} must not be empty");
        validate_url_with_allowed_schemes(context, value, &["http", "https"])?;
    }
    Ok(())
}

fn validate_url_with_scheme(context: &str, value: &str, scheme: &str) -> Result<()> {
    validate_url_with_allowed_schemes(context, value, &[scheme])
}

fn validate_url_with_allowed_schemes(context: &str, value: &str, schemes: &[&str]) -> Result<()> {
    let trimmed = value.trim();
    ensure!(
        trimmed == value,
        "{context} must not contain leading or trailing whitespace"
    );
    let url = url::Url::parse(trimmed)
        .map_err(|err| anyhow::anyhow!("{context} must be a valid URL: {err}"))?;
    ensure!(
        schemes.contains(&url.scheme()),
        "{context} must use {}",
        schemes.join(" or ")
    );
    ensure!(url.has_host(), "{context} must include a host");
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "{context} must not include credentials"
    );
    Ok(())
}

pub(crate) fn validate_okx_spot_symbol(context: &str, value: &str) -> Result<()> {
    let _ = okx_spot_quote_asset(context, value)?;
    Ok(())
}

pub(crate) fn okx_spot_quote_asset<'a>(context: &str, value: &'a str) -> Result<&'a str> {
    let symbol = value.trim();
    ensure!(
        !symbol.is_empty(),
        "{context} must not contain empty symbols"
    );
    ensure!(
        symbol == value,
        "{context} symbol {value:?} must not contain leading or trailing whitespace"
    );

    let mut parts = symbol.split('-');
    let base = parts.next().unwrap_or_default();
    let quote = parts.next().unwrap_or_default();
    ensure!(
        parts.next().is_none(),
        "{context} symbol {symbol} must use OKX spot format BASE-QUOTE"
    );
    ensure!(
        is_okx_asset_code(base) && is_okx_asset_code(quote),
        "{context} symbol {symbol} must use uppercase OKX spot format BASE-QUOTE"
    );
    Ok(quote)
}

fn validate_okx_asset_code(context: &str, value: &str) -> Result<()> {
    ensure!(!value.is_empty(), "{context} must not be empty");
    ensure!(
        value == value.trim(),
        "{context} {value:?} must not contain leading or trailing whitespace"
    );
    ensure!(
        is_okx_asset_code(value),
        "{context} {value} must use an uppercase OKX asset code"
    );
    Ok(())
}

fn is_okx_asset_code(value: &str) -> bool {
    matches!(value.len(), 2..=12)
        && value
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
}

fn validate_strategy_instances(config: &BotConfig) -> Result<()> {
    validate_strategy_instance_declarations(&config.strategies.instances)?;

    for instance in enabled_strategy_instances(config) {
        match instance.kind {
            StrategyKind::OkxEmaAtrMakerTrend => {
                validate_okx_ema_atr_maker_trend_instance(config, instance)?;
            }
        }
    }

    Ok(())
}

fn validate_strategy_instance_declarations(instances: &[StrategyInstanceConfig]) -> Result<()> {
    let mut enabled_ids = HashSet::new();
    let mut okx_ema_atr_ownership_tags = HashMap::new();

    for instance in instances {
        validate_strategy_instance_identity(instance)?;
        validate_requested_trading_instrument(&instance.trading_instrument)?;
        if instance.enabled {
            validate_enabled_strategy_id_is_unique(instance, &mut enabled_ids)?;
        }
        match instance.kind {
            StrategyKind::OkxEmaAtrMakerTrend => {
                validate_okx_ema_atr_maker_trend_ownership_tag_is_unique(
                    instance,
                    &mut okx_ema_atr_ownership_tags,
                )?;
            }
        }
    }

    Ok(())
}

fn validate_strategy_instance_identity(instance: &StrategyInstanceConfig) -> Result<()> {
    ensure!(
        !instance.id.trim().is_empty(),
        "strategy instance id must not be empty"
    );
    ensure!(
        instance.id.trim() == instance.id,
        "strategy instance id must not contain leading or trailing whitespace"
    );
    ensure!(
        instance
            .id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')),
        "strategy instance id may contain only ASCII letters, digits, '-', '_' or '.'"
    );
    Ok(())
}

fn validate_enabled_strategy_id_is_unique<'a>(
    instance: &'a StrategyInstanceConfig,
    enabled_ids: &mut HashSet<&'a str>,
) -> Result<()> {
    ensure!(
        enabled_ids.insert(instance.id.as_str()),
        "enabled strategy instance ids must be unique; duplicate id {}",
        instance.id
    );
    Ok(())
}

fn validate_okx_ema_atr_maker_trend_ownership_tag_is_unique<'a>(
    instance: &'a StrategyInstanceConfig,
    ownership_tags: &mut HashMap<String, &'a str>,
) -> Result<()> {
    let tag = strategy_ownership_tag_for_config(&instance.id);
    if let Some(existing_id) = ownership_tags.get(&tag) {
        bail!(
            "configured OkxEmaAtrMakerTrend strategy ownership tags must be unique; ids {existing_id} and {} both derive tag {tag}",
            instance.id
        );
    }
    ownership_tags.insert(tag, instance.id.as_str());
    Ok(())
}

fn validate_okx_ema_atr_maker_trend_instance(
    config: &BotConfig,
    instance: &StrategyInstanceConfig,
) -> Result<()> {
    let params = instance.params.okx_ema_atr_maker_trend();
    validate_okx_ema_atr_maker_trend_parameters(params)?;
    validate_strategy_instrument_and_bar_selection(config, instance, "OkxEmaAtrMakerTrend")?;
    ensure!(
        instance.instrument_id() == OKX_EMA_ATR_MAKER_TREND_QUALIFIED_INSTRUMENT,
        "OkxEmaAtrMakerTrend qualification evidence applies only to {OKX_EMA_ATR_MAKER_TREND_QUALIFIED_INSTRUMENT}; requested {} requires a separate strategy-promotion review",
        instance.instrument_id()
    );

    validate_quote_notional_by_instrument_keys(
        config,
        instance,
        "OkxEmaAtrMakerTrend",
        &params.max_quote_notional_by_instrument,
    )?;

    Ok(())
}

fn validate_strategy_instrument_and_bar_selection(
    config: &BotConfig,
    instance: &StrategyInstanceConfig,
    strategy_name: &str,
) -> Result<()> {
    let enabled_instrument_ids = enabled_instrument_ids(config);
    validate_strategy_instrument_selection(instance, strategy_name, &enabled_instrument_ids)?;
    validate_strategy_bar_selection(instance, strategy_name)
}

fn validate_strategy_instrument_selection(
    instance: &StrategyInstanceConfig,
    strategy_name: &str,
    enabled_instrument_ids: &HashSet<String>,
) -> Result<()> {
    let instrument_id = instance.instrument_id();
    ensure!(
        !instrument_id.trim().is_empty(),
        "{strategy_name} instrument must not be empty when enabled"
    );
    validate_okx_spot_symbol(&format!("{strategy_name} instrument"), instrument_id)?;
    ensure!(
        enabled_instrument_ids.contains(instrument_id),
        "{strategy_name} instrument {instrument_id} must reference an enabled configured OKX spot instrument"
    );
    Ok(())
}

fn validate_strategy_bar_selection(
    instance: &StrategyInstanceConfig,
    strategy_name: &str,
) -> Result<()> {
    ensure!(
        instance.bar == OKX_EMA_ATR_MAKER_TREND_BAR,
        "{strategy_name} bar must be {OKX_EMA_ATR_MAKER_TREND_BAR} for this strategy"
    );
    Ok(())
}

fn validate_quote_notional_by_instrument_keys(
    config: &BotConfig,
    instance: &StrategyInstanceConfig,
    strategy_name: &str,
    max_quote_notional_by_instrument: &std::collections::BTreeMap<String, Decimal>,
) -> Result<()> {
    let enabled_instrument_ids = enabled_instrument_ids(config);
    for instrument_id in max_quote_notional_by_instrument.keys() {
        validate_quote_notional_by_instrument_key(
            instance,
            strategy_name,
            instrument_id,
            &enabled_instrument_ids,
        )?;
    }
    Ok(())
}

fn validate_quote_notional_by_instrument_key(
    instance: &StrategyInstanceConfig,
    strategy_name: &str,
    instrument_id: &str,
    enabled_instrument_ids: &HashSet<String>,
) -> Result<()> {
    let selected_instrument_id = instance.instrument_id();
    ensure!(
        selected_instrument_id == instrument_id,
        "{strategy_name} max_quote_notional_by_instrument key {instrument_id} must reference selected {strategy_name} instrument {selected_instrument_id}"
    );
    ensure!(
        enabled_instrument_ids.contains(instrument_id),
        "{strategy_name} max_quote_notional_by_instrument key {instrument_id} must reference an enabled configured OKX spot instrument_id"
    );
    Ok(())
}

pub(crate) fn validate_requested_trading_instrument(
    requested: &RequestedTradingInstrument,
) -> Result<()> {
    let instrument = requested.instrument.as_str();
    ensure!(
        !instrument.is_empty(),
        "strategy instrument must not be empty"
    );
    ensure!(
        instrument == instrument.trim(),
        "strategy instrument {instrument:?} must not contain leading or trailing whitespace"
    );
    ensure!(
        instrument.len() <= 64,
        "strategy instrument must not exceed the documented 64-byte identifier boundary"
    );
    ensure!(
        instrument.is_ascii() && !instrument.chars().any(char::is_control),
        "strategy instrument must contain canonical printable ASCII only"
    );
    validate_okx_spot_symbol("strategy instrument", instrument)?;

    match (requested.inst_type, requested.td_mode) {
        (RequestedInstrumentType::Spot, RequestedTradeMode::Cash) => Ok(()),
        (RequestedInstrumentType::Spot, RequestedTradeMode::Cross) => bail!(
            "OKX SPOT + cross is roadmap-only for acctLv 3 and is not admitted by the current cash-SPOT runtime"
        ),
        (RequestedInstrumentType::Spot, mode) => bail!(
            "OKX SPOT trade mode {mode} is unsupported; current runtime admits only tdMode cash"
        ),
        (inst_type, mode) => bail!(
            "OKX instrument tuple {inst_type} + {mode} is unsupported; current runtime admits only SPOT + cash"
        ),
    }
}

fn validate_okx_ema_atr_maker_trend_parameters(params: &OkxEmaAtrMakerTrendConfig) -> Result<()> {
    validate_okx_ema_atr_maker_trend_periods(params)?;
    validate_okx_ema_atr_maker_trend_quote_sizing(params)?;
    validate_okx_ema_atr_maker_trend_entry_lifetime(params)?;
    validate_okx_ema_atr_maker_trend_entry_offsets(params)?;
    validate_okx_ema_atr_maker_trend_exit_multiples(params)?;
    Ok(())
}

fn validate_okx_ema_atr_maker_trend_entry_lifetime(
    params: &OkxEmaAtrMakerTrendConfig,
) -> Result<()> {
    ensure!(
        (1_000..=60_000).contains(&params.max_entry_order_age_ms),
        "OkxEmaAtrMakerTrend max_entry_order_age_ms must be between 1000 and 60000"
    );
    Ok(())
}

fn validate_okx_ema_atr_maker_trend_periods(params: &OkxEmaAtrMakerTrendConfig) -> Result<()> {
    ensure!(
        params.fast_ema_period > 0,
        "OkxEmaAtrMakerTrend fast_ema_period must be positive"
    );
    ensure!(
        params.slow_ema_period > params.fast_ema_period,
        "OkxEmaAtrMakerTrend slow_ema_period must be greater than fast_ema_period"
    );
    ensure!(
        params.atr_period > 1,
        "OkxEmaAtrMakerTrend atr_period must be greater than 1"
    );
    Ok(())
}

fn validate_okx_ema_atr_maker_trend_quote_sizing(params: &OkxEmaAtrMakerTrendConfig) -> Result<()> {
    ensure!(
        params.quantity > Decimal::ZERO,
        "OkxEmaAtrMakerTrend quantity must be positive"
    );
    ensure!(
        params.operator_owned_base_balance >= Decimal::ZERO,
        "OkxEmaAtrMakerTrend operator_owned_base_balance must be non-negative"
    );
    if let Some(max_quote_notional) = params.max_quote_notional {
        ensure!(
            max_quote_notional > Decimal::ZERO,
            "OkxEmaAtrMakerTrend max_quote_notional must be positive"
        );
    }
    for (instrument_id, max_quote_notional) in &params.max_quote_notional_by_instrument {
        validate_okx_spot_symbol(
            "OkxEmaAtrMakerTrend max_quote_notional_by_instrument",
            instrument_id,
        )?;
        ensure!(
            *max_quote_notional > Decimal::ZERO,
            "OkxEmaAtrMakerTrend max_quote_notional_by_instrument values must be positive"
        );
    }
    Ok(())
}

fn validate_okx_ema_atr_maker_trend_entry_offsets(
    params: &OkxEmaAtrMakerTrendConfig,
) -> Result<()> {
    ensure!(
        params.entry_offset_atr_multiple > Decimal::ZERO
            && params.entry_offset_atr_multiple <= Decimal::ONE,
        "OkxEmaAtrMakerTrend entry_offset_atr_multiple must be positive and reasonable"
    );
    ensure!(
        params.min_entry_offset_bps > Decimal::ZERO
            && params.min_entry_offset_bps <= Decimal::new(100, 0),
        "OkxEmaAtrMakerTrend min_entry_offset_bps must be positive and reasonable"
    );
    ensure!(
        params.max_entry_offset_bps > Decimal::ZERO
            && params.max_entry_offset_bps <= Decimal::new(100, 0),
        "OkxEmaAtrMakerTrend max_entry_offset_bps must be positive and reasonable"
    );
    ensure!(
        params.min_entry_offset_bps <= params.max_entry_offset_bps,
        "OkxEmaAtrMakerTrend min_entry_offset_bps must be less than or equal to max_entry_offset_bps"
    );
    Ok(())
}

fn validate_okx_ema_atr_maker_trend_exit_multiples(
    params: &OkxEmaAtrMakerTrendConfig,
) -> Result<()> {
    ensure!(
        params.take_profit_atr_multiple > Decimal::ZERO
            && params.take_profit_atr_multiple <= Decimal::new(10, 0),
        "OkxEmaAtrMakerTrend take_profit_atr_multiple must be positive and reasonable"
    );
    ensure!(
        params.stop_loss_atr_multiple > Decimal::ZERO
            && params.stop_loss_atr_multiple <= Decimal::new(10, 0),
        "OkxEmaAtrMakerTrend stop_loss_atr_multiple must be positive and reasonable"
    );
    Ok(())
}

fn enabled_strategy_instances(config: &BotConfig) -> impl Iterator<Item = &StrategyInstanceConfig> {
    config
        .strategies
        .instances
        .iter()
        .filter(|instance| instance.enabled)
}

fn strategy_max_quote_notional(params: &StrategyParamsConfig) -> Option<Decimal> {
    match params {
        StrategyParamsConfig::OkxEmaAtrMakerTrend(config) => config.max_quote_notional,
    }
}

fn strategy_max_quote_notional_by_instrument(
    params: &StrategyParamsConfig,
) -> &std::collections::BTreeMap<String, Decimal> {
    match params {
        StrategyParamsConfig::OkxEmaAtrMakerTrend(config) => {
            &config.max_quote_notional_by_instrument
        }
    }
}

fn enabled_instrument_ids(config: &BotConfig) -> HashSet<String> {
    config
        .instruments
        .iter()
        .filter(|instrument| instrument.enabled)
        .map(InstrumentConfig::okx_instrument_id)
        .collect()
}

fn reject_okx_derivative_marker(context: &str, value: &str) -> Result<()> {
    let normalized = value.trim().to_ascii_uppercase();
    for marker in ["SWAP", "FUTURES", "FUTURE", "OPTION", "MARGIN", "LEVERAGE"] {
        if normalized.split('-').any(|part| part == marker) || normalized.contains(marker) {
            bail!("{context} contains prohibited OKX derivative or margin marker {marker}");
        }
    }
    Ok(())
}
