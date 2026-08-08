use std::path::Path;

use okx_trading_runtime::{
    config::{
        loader::load_config_path_with_secret_resolver,
        runtime::{masked_okx_account_id, masked_okx_api_key},
        types::{
            BotConfig, OkxAccountJurisdiction, OkxApiDomain, OkxTradingService, ProductConfig,
            RuntimeConfig, StrategyConfig,
        },
    },
    validate_selected_profile_with_args,
};
use pretty_assertions::assert_eq;

fn load_example() -> BotConfig {
    load_config_path_with_secret_resolver(Path::new("config/example.toml"), test_secret_resolver)
        .expect("checked-in example should load")
}

fn test_secret_resolver(name: &str) -> Option<String> {
    match name {
        "OKX_API_KEY" => Some("test-api-key".to_owned()),
        "OKX_API_SECRET" => Some("test-api-secret".to_owned()),
        "OKX_API_PASSPHRASE" => Some("test-passphrase".to_owned()),
        _ => None,
    }
}

#[test]
fn checked_in_example_preserves_inert_demo_startup_contract() {
    let config = load_example();
    let okx = config.okx.as_ref().expect("example configures OKX");

    assert_eq!(config.product.name, "okx-rust-trading");
    assert_eq!(config.runtime.trader_id, "PUBLIC-DEMO-OPERATOR");
    assert_eq!(config.runtime.poll_interval_ms, 2_000);
    assert_eq!(config.runtime.order_intent, None);
    assert_eq!(okx.api_domain, OkxApiDomain::Global);
    assert_eq!(okx.account_jurisdiction, OkxAccountJurisdiction::Other);
    assert_eq!(okx.trading_service, OkxTradingService::Demo);
    assert_eq!(okx.base_url, "https://openapi.okx.com");
    assert_eq!(okx.account_id, "OKX-PUBLIC-DEMO");
    assert_eq!(
        config
            .instruments
            .iter()
            .filter(|instrument| instrument.enabled)
            .map(|instrument| instrument.okx_instrument_id())
            .collect::<Vec<_>>(),
        ["BTC-USDT"]
    );
    assert!(config.strategies.instances.is_empty());
}

#[test]
fn checked_in_example_masks_okx_identifiers_and_debug_secrets() {
    let config = load_example();
    let masked_key = masked_okx_api_key(&config);
    let masked_account = masked_okx_account_id(&config);
    assert_ne!(masked_key, "test-api-key");
    assert!(masked_key.starts_with("te") && masked_key.ends_with("ey"));
    assert_ne!(masked_account, "OKX-PUBLIC-DEMO");
    assert!(masked_account.starts_with("OK") && masked_account.ends_with("MO"));

    let debug = format!("{:?}", config.okx.expect("example configures OKX"));
    assert!(debug.contains("<redacted>"));
    for secret in [
        "test-api-key",
        "test-api-secret",
        "test-passphrase",
        "OKX-PUBLIC-DEMO",
    ] {
        assert!(!debug.contains(secret), "debug output exposed {secret:?}");
    }
}

#[test]
fn public_profile_validation_rejects_cli_overrides_before_loading() {
    for (args, expected) in [
        (
            vec!["--live".to_owned()],
            "field-level CLI overrides and mode flags are prohibited",
        ),
        (
            vec!["runtime.trader_id=PUBLIC-DEMO-OPERATOR-2".to_owned()],
            "field-level CLI overrides and mode flags are prohibited",
        ),
        (
            vec!["example".to_owned(), "extra".to_owned()],
            "accepts at most one complete profile selector",
        ),
        (
            vec!["".to_owned()],
            "runtime config profile selector must not be empty",
        ),
    ] {
        let error = validate_selected_profile_with_args(args).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn runtime_masking_reports_unconfigured_without_okx_profile() {
    let config = BotConfig {
        product: ProductConfig {
            name: "runtime-test".to_owned(),
        },
        runtime: RuntimeConfig {
            trader_id: "PUBLIC-DEMO-OPERATOR".to_owned(),
            poll_interval_ms: 2_000,
            tick_timeout_ms: 5_000,
            order_intent: None,
        },
        okx: None,
        instruments: Vec::new(),
        strategies: StrategyConfig::default(),
    };

    assert_eq!(masked_okx_api_key(&config), "unconfigured");
    assert_eq!(masked_okx_account_id(&config), "unconfigured");
}
