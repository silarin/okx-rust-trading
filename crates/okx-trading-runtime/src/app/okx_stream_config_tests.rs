use std::{path::Path, sync::Arc, time::Duration};

use anyhow::Result;
use pretty_assertions::assert_eq;

use crate::{
    config::{loader::load_config_path_with_secret_resolver, types::OkxApiDomain},
    okx::websocket::{
        OkxAlgoSubscriptionSelector, OkxPrivateStreamConfig, OkxPrivateStreamCredentials,
        OkxPrivateStreamKind, OkxPublicMarketStreamConfig, OkxWebsocketReconnectPolicy,
    },
};

use super::{
    build_market_stream_configs, build_private_stream_configs, enabled_strategy_candle_channels,
    enabled_strategy_instrument_ids, websocket_reconnect_policy,
};

#[test]
fn stream_config_builders_fail_closed_without_okx_config() {
    let mut config =
        load_profile_config("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");
    config.okx = None;

    expect_missing_okx_config(
        build_market_stream_configs(&config, /*has_enabled_strategies*/ true),
        "market stream builder",
    );
    expect_missing_okx_config(
        build_private_stream_configs(&config, /*has_enabled_strategies*/ true),
        "private stream builder",
    );
    expect_missing_okx_config(
        websocket_reconnect_policy(&config),
        "reconnect policy builder",
    );
}

#[test]
fn strategy_enabled_profile_prepares_market_streams() -> Result<()> {
    let config =
        load_profile_config("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");
    let stream_configs =
        build_market_stream_configs(&config, /*has_enabled_strategies*/ true)?;

    assert_eq!(
        stream_configs,
        vec![
            OkxPublicMarketStreamConfig {
                url: "wss://wseeapap.okx.com:8443/ws/v5/public".to_owned(),
                instrument_ids: vec!["BTC-USDT".to_owned()],
                instrument_type: "SPOT".to_owned(),
                subscribe_tickers: true,
                subscribe_instruments: true,
                candle_channels: Vec::new(),
                level2_instrument_ids: vec!["BTC-USDT".to_owned()],
                reconnect_policy: OkxWebsocketReconnectPolicy::new(
                    Duration::from_millis(500),
                    Duration::from_millis(10_000),
                )?,
            },
            OkxPublicMarketStreamConfig {
                url: "wss://wseeapap.okx.com:8443/ws/v5/business".to_owned(),
                instrument_ids: vec!["BTC-USDT".to_owned()],
                instrument_type: "SPOT".to_owned(),
                subscribe_tickers: false,
                subscribe_instruments: false,
                candle_channels: vec!["candle1m".to_owned()],
                level2_instrument_ids: Vec::new(),
                reconnect_policy: OkxWebsocketReconnectPolicy::new(
                    Duration::from_millis(500),
                    Duration::from_millis(10_000),
                )?,
            },
        ]
    );
    Ok(())
}

#[test]
fn strategy_selected_instrument_is_used_for_level2() -> Result<()> {
    let mut config =
        load_profile_config("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");
    config.strategies.instances[0].trading_instrument.instrument =
        crate::config::types::RequestedInstrumentId::new("ETH-USDT".to_owned())
            .expect("canonical instrument");

    let stream_configs =
        build_market_stream_configs(&config, /*has_enabled_strategies*/ true)?;

    assert_eq!(
        stream_configs[0].level2_instrument_ids,
        ["ETH-USDT".to_owned()]
    );
    Ok(())
}

#[test]
fn strategy_enabled_profile_prepares_private_streams() -> Result<()> {
    let config =
        load_profile_config("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");
    let market_stream_configs =
        build_market_stream_configs(&config, /*has_enabled_strategies*/ true)?;
    let private_stream_configs =
        build_private_stream_configs(&config, /*has_enabled_strategies*/ true)?;
    let reconnect_policy = OkxWebsocketReconnectPolicy::new(
        Duration::from_millis(500),
        Duration::from_millis(10_000),
    )?;
    let credentials = Arc::new(OkxPrivateStreamCredentials::new(
        "demo-key".to_owned(),
        "demo-secret".to_owned(),
        "demo-passphrase".to_owned(),
    )?);
    let expected_trading_stream = OkxPrivateStreamConfig::with_reconnect_policy(
        "wss://wseeapap.okx.com:8443/ws/v5/private".to_owned(),
        OkxPrivateStreamKind::Trading,
        vec!["BTC-USDT".to_owned()],
        OkxApiDomain::Eea,
        Arc::clone(&credentials),
        reconnect_policy,
    )?
    .without_optional_fills();
    let expected_business_stream = OkxPrivateStreamConfig::with_reconnect_policy(
        "wss://wseeapap.okx.com:8443/ws/v5/business".to_owned(),
        OkxPrivateStreamKind::Business,
        vec!["BTC-USDT".to_owned()],
        OkxApiDomain::Eea,
        credentials,
        reconnect_policy,
    )?;

    assert_eq!(
        market_stream_configs,
        vec![
            OkxPublicMarketStreamConfig {
                url: "wss://wseeapap.okx.com:8443/ws/v5/public".to_owned(),
                instrument_ids: vec!["BTC-USDT".to_owned()],
                instrument_type: "SPOT".to_owned(),
                subscribe_tickers: true,
                subscribe_instruments: true,
                candle_channels: Vec::new(),
                level2_instrument_ids: vec!["BTC-USDT".to_owned()],
                reconnect_policy,
            },
            OkxPublicMarketStreamConfig {
                url: "wss://wseeapap.okx.com:8443/ws/v5/business".to_owned(),
                instrument_ids: vec!["BTC-USDT".to_owned()],
                instrument_type: "SPOT".to_owned(),
                subscribe_tickers: false,
                subscribe_instruments: false,
                candle_channels: vec!["candle1m".to_owned()],
                level2_instrument_ids: Vec::new(),
                reconnect_policy,
            },
        ]
    );
    assert_eq!(
        private_stream_configs,
        vec![expected_trading_stream, expected_business_stream]
    );
    assert!(Arc::ptr_eq(
        &private_stream_configs[0].credentials,
        &private_stream_configs[1].credentials
    ));
    Ok(())
}

#[test]
fn business_stream_selector_is_derived_only_from_api_domain() -> Result<()> {
    for (api_domain, expected_selector) in [
        (OkxApiDomain::Eea, OkxAlgoSubscriptionSelector::Spot),
        (OkxApiDomain::Global, OkxAlgoSubscriptionSelector::Any),
        (OkxApiDomain::UsAu, OkxAlgoSubscriptionSelector::Any),
    ] {
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("checked-in OKX config")
            .api_domain = api_domain;

        let private_stream_configs =
            build_private_stream_configs(&config, /*has_enabled_strategies*/ true)?;

        assert_eq!(
            private_stream_configs[1].algo_subscription_selector(),
            expected_selector
        );
        assert_eq!(private_stream_configs[1].instrument_type, "SPOT");
        assert_eq!(private_stream_configs[1].instrument_ids, ["BTC-USDT"]);
    }
    Ok(())
}

#[test]
fn startup_fee_preflight_uses_enabled_strategy_instruments_once() {
    let config =
        load_profile_config("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");

    assert_eq!(enabled_strategy_instrument_ids(&config), ["BTC-USDT"]);
}

#[test]
fn enabled_strategy_instrument_ids_preserve_first_seen_order() {
    let mut config =
        load_profile_config("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");
    let mut second_instance = config.strategies.instances[0].clone();
    second_instance.id = "okx-ema-atr-maker-alt".to_owned();
    second_instance.trading_instrument.instrument =
        crate::config::types::RequestedInstrumentId::new("ETH-USDT".to_owned())
            .expect("canonical instrument");
    let mut duplicate_instance = config.strategies.instances[0].clone();
    duplicate_instance.id = "okx-ema-atr-maker-duplicate".to_owned();
    duplicate_instance.trading_instrument.instrument =
        crate::config::types::RequestedInstrumentId::new("BTC-USDT".to_owned())
            .expect("canonical instrument");
    let mut third_instance = config.strategies.instances[0].clone();
    third_instance.id = "okx-ema-atr-maker-sol".to_owned();
    third_instance.trading_instrument.instrument =
        crate::config::types::RequestedInstrumentId::new("SOL-USDT".to_owned())
            .expect("canonical instrument");
    config.strategies.instances.push(second_instance);
    config.strategies.instances.push(duplicate_instance);
    config.strategies.instances.push(third_instance);

    assert_eq!(
        enabled_strategy_instrument_ids(&config),
        ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
    );
    assert_eq!(
        enabled_strategy_candle_channels(&config),
        ["candle1m".to_owned()]
    );
}

fn load_profile_config(path: &str) -> crate::config::types::BotConfig {
    load_config_path_with_secret_resolver(Path::new(path), test_secret_resolver)
        .expect("checked-in OKX profile should load")
}

fn expect_missing_okx_config<T>(result: Result<T>, context: &str) {
    let error = match result {
        Ok(_) => panic!("{context} should fail closed without OKX config"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("missing required [okx] config"),
        "{context} should report missing OKX config: {error}"
    );
}

fn test_secret_resolver(name: &str) -> Option<String> {
    match name {
        "OKX_API_KEY" => Some("demo-key".to_owned()),
        "OKX_API_SECRET" => Some("demo-secret".to_owned()),
        "OKX_API_PASSPHRASE" => Some("demo-passphrase".to_owned()),
        _ => None,
    }
}
