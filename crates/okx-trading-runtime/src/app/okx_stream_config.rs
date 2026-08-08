//! Derives OKX-specific WebSocket stream configuration from a validated runtime
//! profile without owning stream tasks or exchange state.

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use crate::{
    config::types::{
        BotConfig, OkxConfig, OkxTradingService, RequestedTradingInstrument, StrategyKind,
    },
    okx::websocket::{
        OkxPrivateStreamConfig, OkxPrivateStreamCredentials, OkxPrivateStreamKind,
        OkxPublicMarketStreamConfig, OkxWebsocketReconnectPolicy,
        okx_public_candle_channel_for_bar,
    },
};
use anyhow::{Context, Result};

pub(super) fn required_okx_config(config: &BotConfig) -> Result<&OkxConfig> {
    config
        .okx
        .as_ref()
        .context("validated runtime profile is missing required [okx] config")
}

pub(super) fn build_market_stream_configs(
    config: &BotConfig,
    has_enabled_strategies: bool,
) -> Result<Vec<OkxPublicMarketStreamConfig>> {
    if !has_enabled_strategies {
        return Ok(Vec::new());
    }
    let okx = required_okx_config(config)?;
    let public_url = okx
        .base_url_ws_public
        .clone()
        .context("OKX base_url_ws_public is required for strategy WebSocket market data")?;
    let business_url = okx.base_url_ws_business.clone().context(
        "OKX base_url_ws_business is required for strategy candle WebSocket market data",
    )?;
    let instrument_ids = enabled_strategy_instrument_ids(config)
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let reconnect_policy = websocket_reconnect_policy(config)?;
    let level2_instrument_ids = instrument_ids.clone();
    let mut configs = vec![
        OkxPublicMarketStreamConfig::with_reconnect_policy_and_level2(
            public_url,
            instrument_ids.clone(),
            /*subscribe_tickers*/ true,
            /*subscribe_instruments*/ true,
            Vec::new(),
            level2_instrument_ids,
            reconnect_policy,
        )?,
    ];
    let candle_channels = enabled_strategy_candle_channels(config);
    if !candle_channels.is_empty() {
        configs.push(OkxPublicMarketStreamConfig::with_reconnect_policy(
            business_url,
            instrument_ids,
            /*subscribe_tickers*/ false,
            /*subscribe_instruments*/ false,
            candle_channels,
            reconnect_policy,
        )?);
    }
    Ok(configs)
}

pub(super) fn build_private_stream_configs(
    config: &BotConfig,
    has_enabled_strategies: bool,
) -> Result<Vec<OkxPrivateStreamConfig>> {
    if !has_enabled_strategies {
        return Ok(Vec::new());
    }
    let okx = required_okx_config(config)?;

    let instrument_ids: Vec<String> = enabled_strategy_instrument_ids(config)
        .into_iter()
        .map(str::to_owned)
        .collect();
    let credentials = Arc::new(OkxPrivateStreamCredentials::new(
        okx.api_key.clone(),
        okx.api_secret.clone(),
        okx.api_passphrase.clone(),
    )?);
    let private_url = okx
        .base_url_ws_private
        .clone()
        .context("OKX base_url_ws_private is required for strategy private WebSocket streams")?;
    let business_url = okx
        .base_url_ws_business
        .clone()
        .context("OKX base_url_ws_business is required for strategy business WebSocket streams")?;

    let trading_stream = OkxPrivateStreamConfig::with_reconnect_policy(
        private_url,
        OkxPrivateStreamKind::Trading,
        instrument_ids.clone(),
        okx.api_domain,
        Arc::clone(&credentials),
        websocket_reconnect_policy(config)?,
    )?;
    let trading_stream = match okx.trading_service {
        OkxTradingService::Production => trading_stream,
        OkxTradingService::Demo => trading_stream.without_optional_fills(),
    };

    Ok(vec![
        trading_stream,
        OkxPrivateStreamConfig::with_reconnect_policy(
            business_url,
            OkxPrivateStreamKind::Business,
            instrument_ids,
            okx.api_domain,
            credentials,
            websocket_reconnect_policy(config)?,
        )?,
    ])
}

fn websocket_reconnect_policy(config: &BotConfig) -> Result<OkxWebsocketReconnectPolicy> {
    let okx = required_okx_config(config)?;
    OkxWebsocketReconnectPolicy::new(
        Duration::from_millis(okx.websocket.reconnect_initial_backoff_ms),
        Duration::from_millis(okx.websocket.reconnect_max_backoff_ms),
    )
}

pub(super) fn enabled_strategy_instrument_ids(config: &BotConfig) -> Vec<&str> {
    let mut instrument_ids = Vec::new();
    let mut seen_instrument_ids = HashSet::new();
    for instance in config
        .strategies
        .instances
        .iter()
        .filter(|instance| instance.enabled)
    {
        match instance.kind {
            StrategyKind::OkxEmaAtrMakerTrend => {
                let instrument_id = instance.instrument_id();
                if seen_instrument_ids.insert(instrument_id) {
                    instrument_ids.push(instrument_id);
                }
            }
        }
    }
    instrument_ids
}

pub(super) fn enabled_strategy_trading_instruments(
    config: &BotConfig,
) -> Vec<&RequestedTradingInstrument> {
    config
        .strategies
        .instances
        .iter()
        .filter(|instance| instance.enabled)
        .map(|instance| &instance.trading_instrument)
        .collect()
}

fn enabled_strategy_candle_channels(config: &BotConfig) -> Vec<String> {
    let mut channels = Vec::new();
    let mut seen_channels = BTreeSet::new();
    for instance in config
        .strategies
        .instances
        .iter()
        .filter(|instance| instance.enabled)
    {
        match instance.kind {
            StrategyKind::OkxEmaAtrMakerTrend => {
                let Ok(channel) = okx_public_candle_channel_for_bar(&instance.bar) else {
                    continue;
                };
                if seen_channels.insert(channel) {
                    channels.push(channel.to_owned());
                }
            }
        }
    }
    channels
}

#[cfg(test)]
#[path = "okx_stream_config_tests.rs"]
mod tests;
