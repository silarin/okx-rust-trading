use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
};

use okx_trading_runtime::config::{
    loader::{
        load_config_from_str_with_secret_resolver, load_config_path_with_secret_resolver,
        selected_config_path_from_args,
    },
    runtime::{masked_okx_account_id, masked_okx_api_key},
    types::{
        BotConfig, OkxAccountJurisdiction, OkxApiDomain, OkxEmaAtrMakerTrendConfig,
        OkxTradingService, OkxWebsocketConfig, RequestedInstrumentId, RuntimeOrderIntent,
        StrategyKind,
    },
};
use pretty_assertions::assert_eq;
use rust_decimal::Decimal;

const BASE_PROFILE: &str = r#"
[product]
name = "okx-rust-trading"

[runtime]
trader_id = "PUBLIC-DEMO-OPERATOR"
poll_interval_ms = 1000
order_intent = "demo-okx-spot-confirmed"

[okx]
api_key = "${OKX_API_KEY}"
api_secret = "${OKX_API_SECRET}"
api_passphrase = "${OKX_API_PASSPHRASE}"
account_id = "OKX-PUBLIC-DEMO"
api_domain = "EEA"
account_jurisdiction = "EEA"
trading_service = "DEMO"
base_url = "https://eea.okx.com"
base_url_ws_public = "wss://wseeapap.okx.com:8443/ws/v5/public"
base_url_ws_private = "wss://wseeapap.okx.com:8443/ws/v5/private"
base_url_ws_business = "wss://wseeapap.okx.com:8443/ws/v5/business"
request_timeout_ms = 60000

[okx.websocket]
max_staleness_ms = 1000
reconnect_initial_backoff_ms = 500
reconnect_max_backoff_ms = 10000

[[instruments]]
instrument_id = "BTC-USDT"
base_currency = "BTC"
quote_currency = "USDT"
enabled = true

[[strategies.instances]]
kind = "okx_ema_atr_maker_trend"
id = "okx-ema-atr-maker-btc-usdt"
instrument = "BTC-USDT"
inst_type = "SPOT"
td_mode = "cash"
bar = "1m"

[strategies.instances.params]
fast_ema_period = 2
slow_ema_period = 5
atr_period = 3
quantity = "0.001"
max_quote_notional = "500"
entry_offset_atr_multiple = "0.1"
min_entry_offset_bps = "1.0"
max_entry_offset_bps = "15.0"
take_profit_atr_multiple = "1.5"
stop_loss_atr_multiple = "1.0"
"#;

fn profile_without_okx_endpoint_overrides() -> String {
    BASE_PROFILE
        .replace("base_url = \"https://eea.okx.com\"\n", "")
        .replace(
            "base_url_ws_public = \"wss://wseeapap.okx.com:8443/ws/v5/public\"\n",
            "",
        )
        .replace(
            "base_url_ws_private = \"wss://wseeapap.okx.com:8443/ws/v5/private\"\n",
            "",
        )
        .replace(
            "base_url_ws_business = \"wss://wseeapap.okx.com:8443/ws/v5/business\"\n",
            "",
        )
}

#[test]
fn loads_direct_okx_spot_profile() {
    let config = load(BASE_PROFILE).expect("profile should load");

    assert_eq!(config.product.name, "okx-rust-trading");
    assert_eq!(config.instruments[0].okx_instrument_id(), "BTC-USDT");
    assert_eq!(config.strategies.instances[0].instrument_id(), "BTC-USDT");
    assert_eq!(config.strategies.instances[0].bar, "1m");
    assert_eq!(
        format!(
            "{}:{}",
            config.strategies.instances[0].instrument_id(),
            config.strategies.instances[0].bar
        ),
        "BTC-USDT:1m"
    );
}

#[test]
fn operator_instrument_requires_an_explicit_consistent_exact_identity() {
    let config = load(BASE_PROFILE).expect("exact operator instrument identity should load");
    assert_eq!(config.instruments[0].okx_instrument_id(), "BTC-USDT");

    let missing = BASE_PROFILE.replace("instrument_id = \"BTC-USDT\"\n", "");
    load(&missing).expect_err("missing operator instrument identity must fail strict parsing");

    let mismatched = BASE_PROFILE.replace(
        "instrument_id = \"BTC-USDT\"",
        "instrument_id = \"ETH-USDT\"",
    );
    let error = load(&mismatched).expect_err("operator identity and currencies must agree");
    assert!(
        error
            .to_string()
            .contains("must exactly match configured base_currency and quote_currency")
    );
}

#[test]
fn default_config_path_points_to_inert_example_profile() {
    let args = Vec::<String>::new();

    assert_eq!(
        selected_config_path_from_args(args).expect("default path should resolve"),
        PathBuf::from("config/example.toml")
    );
}

#[test]
fn profile_selector_resolves_names_and_explicit_paths() {
    for (args, expected) in [
        (vec!["live".to_owned()], PathBuf::from("config/live.toml")),
        (
            vec!["config/custom.toml".to_owned()],
            PathBuf::from("config/custom.toml"),
        ),
    ] {
        assert_eq!(
            selected_config_path_from_args(args).expect("profile selector should resolve"),
            expected
        );
    }
}

#[test]
fn profile_selector_rejects_overrides_and_extra_args() {
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
            vec!["demo".to_owned(), "live".to_owned()],
            "accepts at most one complete profile selector",
        ),
        (
            vec!["".to_owned()],
            "runtime config profile selector must not be empty",
        ),
    ] {
        let error = selected_config_path_from_args(args).unwrap_err();

        assert!(
            error.to_string().contains(expected),
            "profile selector should fail with {expected:?}: {error}"
        );
    }
}

#[test]
fn rejects_derivative_symbol_markers() {
    let profile = BASE_PROFILE
        .replace("quote_currency = \"USDT\"", "quote_currency = \"SWAP\"")
        .replace(
            "instrument_id = \"BTC-USDT\"",
            "instrument_id = \"BTC-SWAP\"",
        );
    let err = load(&profile).expect_err("derivative asset should fail");

    assert!(
        err.to_string()
            .contains("must use an uppercase OKX asset code")
            || err.to_string().contains("prohibited OKX derivative")
    );
}

#[test]
fn requested_instrument_id_accepts_other_canonical_spot_identifiers() {
    for instrument in ["ETH-USDT", "BTC-EUR", "SOL-USDC"] {
        assert_eq!(
            RequestedInstrumentId::new(instrument.to_owned())
                .expect("canonical requested instrument")
                .as_str(),
            instrument
        );
    }
}

#[test]
fn requested_trading_tuple_requires_all_three_fields() {
    for required_line in [
        "instrument = \"BTC-USDT\"\n",
        "inst_type = \"SPOT\"\n",
        "td_mode = \"cash\"\n",
    ] {
        let profile = BASE_PROFILE.replace(required_line, "");
        let error = load(&profile).expect_err("missing tuple field must fail");
        assert!(error.to_string().contains("failed parsing TOML profile"));
    }
}

#[test]
fn requested_instrument_rejects_blank_case_and_separator_variants() {
    for invalid in ["", "btc-usdt", "BTC/USDT", "BTC_USDT", " BTC-USDT"] {
        let profile = BASE_PROFILE.replace(
            "instrument = \"BTC-USDT\"",
            &format!("instrument = \"{invalid}\""),
        );
        load(&profile).expect_err("non-canonical instrument must fail");
    }
}

#[test]
fn requested_tuple_enums_preserve_exact_spelling_and_repository_policy() {
    for invalid_line in [
        "inst_type = \"spot\"",
        "inst_type = \"UNKNOWN\"",
        "td_mode = \"CASH\"",
        "td_mode = \"unknown\"",
    ] {
        let profile = if invalid_line.starts_with("inst_type") {
            BASE_PROFILE.replace("inst_type = \"SPOT\"", invalid_line)
        } else {
            BASE_PROFILE.replace("td_mode = \"cash\"", invalid_line)
        };
        load(&profile).expect_err("unknown or incorrectly cased enum must fail parsing");
    }

    for (field, value, expected) in [
        (
            "inst_type",
            "MARGIN",
            "current runtime admits only SPOT + cash",
        ),
        (
            "td_mode",
            "isolated",
            "current runtime admits only tdMode cash",
        ),
        (
            "td_mode",
            "spot_isolated",
            "current runtime admits only tdMode cash",
        ),
        ("td_mode", "cross", "roadmap-only"),
    ] {
        let original = if field == "inst_type" {
            "inst_type = \"SPOT\""
        } else {
            "td_mode = \"cash\""
        };
        let profile = BASE_PROFILE.replace(original, &format!("{field} = \"{value}\""));
        let error = load(&profile).expect_err("recognized but unsupported tuple must fail policy");
        assert!(
            error.to_string().contains(expected),
            "unexpected tuple policy error for {field}={value}: {error}"
        );
    }
}

#[test]
fn rejects_unqualified_instrument_for_the_checked_in_strategy() {
    let profile = BASE_PROFILE
        .replace("instrument = \"BTC-USDT\"", "instrument = \"ETH-USDT\"")
        .replace(
            "instrument_id = \"BTC-USDT\"",
            "instrument_id = \"ETH-USDT\"",
        )
        .replace("base_currency = \"BTC\"", "base_currency = \"ETH\"");
    let error = load(&profile).expect_err("strategy qualification must remain instrument-specific");
    assert!(
        error
            .to_string()
            .contains("qualification evidence applies only")
    );
}

#[test]
fn rejects_lowercase_base_or_quote_assets() {
    for (old_line, new_line) in [
        ("base_currency = \"BTC\"", "base_currency = \"btc\""),
        ("quote_currency = \"USDT\"", "quote_currency = \"usdt\""),
    ] {
        let profile = BASE_PROFILE.replace(old_line, new_line);
        let error = load(&profile).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("must use an uppercase OKX asset code"),
            "lowercase asset should fail validation: {error}"
        );
    }
}

#[test]
fn rejects_whitespace_in_base_or_quote_assets() {
    for (old_line, new_line) in [
        ("base_currency = \"BTC\"", "base_currency = \" BTC\""),
        ("quote_currency = \"USDT\"", "quote_currency = \"USDT \""),
    ] {
        let profile = BASE_PROFILE.replace(old_line, new_line);
        let error = load(&profile).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("must not contain leading or trailing whitespace"),
            "whitespace-padded asset should fail validation: {error}"
        );
    }
}

#[test]
fn rejects_same_base_and_quote_assets() {
    let profile = BASE_PROFILE
        .replace("base_currency = \"BTC\"", "base_currency = \"USDT\"")
        .replace(
            "instrument_id = \"BTC-USDT\"",
            "instrument_id = \"USDT-USDT\"",
        );
    let error = load(&profile).unwrap_err();

    assert!(
        error.to_string().contains("must not equal quote_currency"),
        "same base/quote should fail validation: {error}"
    );
}

#[test]
fn rejects_unsupported_instrument_symbol_and_currency_fields() {
    let profile = BASE_PROFILE.replace(
        r#"base_currency = "BTC"
quote_currency = "USDT""#,
        r#"symbol = "BTC-USDT"
base_currency = "BTC"
quote_currency = "USDT"
currency = "USDT""#,
    );
    let error =
        load(&profile).expect_err("unsupported instrument fields should fail strict parsing");

    assert!(
        error.to_string().contains("failed parsing TOML profile"),
        "unsupported symbol/currency fields should be rejected as unknown: {error}"
    );
}

#[test]
fn rejects_unsupported_instrument_exchange_field() {
    let profile = BASE_PROFILE.replace(
        r#"quote_currency = "USDT""#,
        r#"quote_currency = "USDT"
exchange = "OKX""#,
    );
    let error =
        load(&profile).expect_err("unsupported instrument exchange should fail strict parsing");

    assert!(
        error.to_string().contains("failed parsing TOML profile"),
        "unsupported exchange field should be rejected as unknown: {error}"
    );
}

#[test]
fn rejects_duplicate_enabled_configured_instrument_ids() {
    let profile = BASE_PROFILE.replace(
        r#"[[instruments]]
instrument_id = "BTC-USDT"
base_currency = "BTC"
quote_currency = "USDT"
enabled = true"#,
        r#"[[instruments]]
instrument_id = "BTC-USDT"
base_currency = "BTC"
quote_currency = "USDT"
enabled = true

[[instruments]]
instrument_id = "BTC-USDT"
base_currency = "BTC"
quote_currency = "USDT"
enabled = true"#,
    );
    let error = load(&profile).unwrap_err();

    assert!(
        error.to_string().contains("instrument_ids must be unique"),
        "duplicate exact instrument IDs should fail validation: {error}"
    );
}

#[test]
fn rejects_strategy_instrument_without_enabled_configured_instrument() {
    let profile = BASE_PROFILE
        .replace("instrument = \"BTC-USDT\"", "instrument = \"ETH-USDT\"")
        .replace(
            "id = \"okx-ema-atr-maker-btc-usdt\"",
            "id = \"okx-ema-atr-maker-eth-usdt\"",
        );
    let error = load(&profile).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("must reference an enabled configured OKX spot instrument"),
        "strategy instrument must reference enabled configured instrument ID: {error}"
    );
}

#[test]
fn rejects_unsupported_strategy_instrument_ids_and_bar_types_fields() {
    let profile = BASE_PROFILE.replace(
        r#"instrument = "BTC-USDT"
inst_type = "SPOT"
td_mode = "cash"
bar = "1m""#,
        r#"instrument_ids = ["BTC-USDT"]
bar_types = ["BTC-USDT:1m"]"#,
    );
    let error = load(&profile).expect_err("unsupported strategy fields should fail strict parsing");

    assert!(
        error.to_string().contains("failed parsing TOML profile"),
        "unsupported strategy instrument_ids/bar_types should be rejected as unknown: {error}"
    );
}

#[test]
fn rejects_unknown_fields() {
    let profile = BASE_PROFILE.replace(
        "request_timeout_ms = 60000",
        "request_timeout_ms = 60000\nlive_trading_enabled = true",
    );
    let err = load(&profile).expect_err("unknown field should fail");

    assert!(err.to_string().contains("failed parsing TOML profile"));
}

#[test]
fn defaults_okx_websocket_config_to_tuning_defaults() {
    let profile = strategy_empty_profile(BASE_PROFILE).replace(
        r#"
[okx.websocket]
max_staleness_ms = 1000
reconnect_initial_backoff_ms = 500
reconnect_max_backoff_ms = 10000
"#,
        "",
    );

    let config = load(&profile).expect("profile should default OKX WebSocket tuning config");
    let okx = config.okx.expect("OKX config should parse");

    assert_eq!(okx.websocket, OkxWebsocketConfig::default());
}

#[test]
fn rejects_unknown_okx_websocket_fields() {
    let profile = BASE_PROFILE.replace(
        "reconnect_max_backoff_ms = 10000",
        "reconnect_max_backoff_ms = 10000\nsubscribe_orders = true",
    );
    let err = load(&profile).expect_err("unknown WebSocket field should fail");

    assert!(err.to_string().contains("failed parsing TOML profile"));
}

#[test]
fn rejects_unsupported_okx_websocket_mode_field() {
    let profile = BASE_PROFILE.replace(
        "max_staleness_ms = 1000",
        "mode = \"PUBLIC_AND_PRIVATE\"\nmax_staleness_ms = 1000",
    );
    let err = load(&profile).expect_err("unsupported WebSocket mode field should fail");

    assert!(err.to_string().contains("failed parsing TOML profile"));
}

#[test]
fn rejects_unsupported_okx_websocket_trading_order_command_mode_field() {
    let profile = BASE_PROFILE.replace(
        "[[instruments]]",
        "[okx.websocket.trading]\norder_command_mode = \"WEBSOCKET_WITH_REST_RECONCILIATION\"\n\n[[instruments]]",
    );
    let err =
        load(&profile).expect_err("unsupported WebSocket order command mode field should fail");

    assert!(err.to_string().contains("failed parsing TOML profile"));
}

#[test]
fn rejects_unsupported_empty_okx_websocket_trading_block() {
    let profile = BASE_PROFILE.replace(
        "[[instruments]]",
        "[okx.websocket.trading]\n\n[[instruments]]",
    );
    let err = load(&profile).expect_err("unsupported WebSocket trading block should fail");

    assert!(err.to_string().contains("failed parsing TOML profile"));
}

#[test]
fn accepts_strategy_enabled_profile_without_websocket_selectors() {
    let config = load(BASE_PROFILE)
        .expect("strategy-enabled profile without mode selectors should validate");
    let okx = config.okx.as_ref().expect("OKX config should parse");

    assert_eq!(
        okx.websocket,
        OkxWebsocketConfig {
            max_staleness_ms: 1_000,
            reconnect_initial_backoff_ms: 500,
            reconnect_max_backoff_ms: 10_000,
        }
    );
    assert_eq!(
        config
            .strategies
            .instances
            .iter()
            .filter(|instance| instance.enabled)
            .count(),
        1
    );
}

#[test]
fn trading_safety_matrix_strategy_enabled_demo_profile_requires_exact_order_intent() {
    let missing_intent = BASE_PROFILE.replace("order_intent = \"demo-okx-spot-confirmed\"\n", "");
    let error = load(&missing_intent).expect_err("demo strategy should require order intent");
    assert!(
        error
            .to_string()
            .contains("strategy-enabled OKX DEMO profiles require runtime.order_intent"),
        "missing demo order intent should fail clearly: {error}"
    );

    let config = load(BASE_PROFILE).expect("exact demo order intent should validate");
    assert_eq!(
        config.runtime.order_intent,
        Some(RuntimeOrderIntent::DemoOkxSpotConfirmed)
    );

    let live_intent = BASE_PROFILE.replace(
        "order_intent = \"demo-okx-spot-confirmed\"",
        "order_intent = \"live-okx-spot-confirmed\"",
    );
    let error = load(&live_intent).expect_err("demo strategy should reject live order intent");
    assert!(
        error
            .to_string()
            .contains("live-okx-spot-confirmed is not valid for OKX DEMO profiles"),
        "demo with live intent should fail clearly: {error}"
    );
}

#[test]
fn trading_safety_matrix_strategy_enabled_production_profile_requires_live_order_intent() {
    let production_profile = BASE_PROFILE
        .replace(
            "trading_service = \"DEMO\"",
            "trading_service = \"PRODUCTION\"",
        )
        .replace("wseeapap.okx.com", "wseea.okx.com");
    let demo_intent_error =
        load(&production_profile).expect_err("production strategy should reject demo order intent");
    assert!(
        demo_intent_error
            .to_string()
            .contains("demo-okx-spot-confirmed is not valid for OKX PRODUCTION profiles"),
        "production with demo intent should fail clearly: {demo_intent_error}"
    );

    let missing_intent =
        production_profile.replace("order_intent = \"demo-okx-spot-confirmed\"\n", "");
    let missing_intent_error =
        load(&missing_intent).expect_err("production strategy should require live order intent");
    assert!(
        missing_intent_error
            .to_string()
            .contains("strategy-enabled OKX PRODUCTION profiles require runtime.order_intent"),
        "production missing order intent should fail clearly: {missing_intent_error}"
    );

    let live_intent = production_profile.replace(
        "order_intent = \"demo-okx-spot-confirmed\"",
        "order_intent = \"live-okx-spot-confirmed\"",
    );
    let config = load(&live_intent).expect("exact production order intent should validate");
    assert_eq!(
        config.runtime.order_intent,
        Some(RuntimeOrderIntent::LiveOkxSpotConfirmed)
    );
}

#[test]
fn rejects_unknown_order_intent_values() {
    let profile = BASE_PROFILE.replace(
        "order_intent = \"demo-okx-spot-confirmed\"",
        "order_intent = \"paper-okx-spot-confirmed\"",
    );
    let error = load(&profile).expect_err("unknown order intent value should fail parsing");

    assert!(
        error.to_string().contains("failed parsing TOML profile"),
        "unknown order intent should fail strict parsing: {error}"
    );
}

#[test]
fn strategy_empty_demo_profile_does_not_require_order_intent() {
    let profile = strategy_empty_profile(BASE_PROFILE);
    let config = load(&profile).expect("strategy-empty demo profile should not require intent");

    assert_eq!(config.runtime.order_intent, None);
    assert_eq!(config.strategies.instances, []);
}

#[test]
fn validates_strategy_tick_timeout_runtime_bounds() {
    let zero_timeout = BASE_PROFILE.replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 0",
    );
    let error = load(&zero_timeout).expect_err("zero tick timeout should fail");
    assert!(
        error
            .to_string()
            .contains("runtime.tick_timeout_ms must be positive"),
        "zero tick timeout should fail clearly: {error}"
    );

    let below_poll = BASE_PROFILE.replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 500",
    );
    let error = load(&below_poll).expect_err("tick timeout below poll interval should fail");
    assert!(
        error
            .to_string()
            .contains("runtime.tick_timeout_ms must be greater than or equal"),
        "tick timeout below poll interval should fail clearly: {error}"
    );
}

#[test]
fn aggregate_strategy_dispatch_budget_accepts_single_strategy_at_boundary() {
    let profile = BASE_PROFILE.replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 5000",
    );

    load(&profile).expect("one 5-second strategy and reconciliation should fit a 10-second CAA");
}

#[test]
fn aggregate_strategy_dispatch_budget_rejects_single_strategy_over_boundary() {
    let profile = BASE_PROFILE.replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 5001",
    );
    let error = load(&profile).expect_err("aggregate strategy budget should exceed CAA");

    assert_aggregate_strategy_dispatch_budget_error(&error, 1, "5001", "10002", "10000");
}

#[test]
fn aggregate_strategy_dispatch_budget_accepts_multiple_strategies_that_fit() {
    let profile = profile_with_additional_strategy("okx-ema-atr-maker-eth-usdt", true).replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 2500",
    );

    load(&profile).expect("two serialized 2.5-second strategy budgets should fit a 10-second CAA");
}

#[test]
fn aggregate_strategy_dispatch_budget_rejects_multiple_strategies_that_exceed() {
    let profile = profile_with_additional_strategy("okx-ema-atr-maker-eth-usdt", true).replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 2501",
    );
    let error = load(&profile).expect_err("aggregate serialized strategy budget should exceed CAA");

    assert_aggregate_strategy_dispatch_budget_error(&error, 2, "2501", "10004", "10000");
}

#[test]
fn aggregate_strategy_dispatch_budget_ignores_disabled_strategies() {
    let profile = profile_with_additional_strategy("okx-ema-atr-maker-eth-usdt", false).replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 5000",
    );

    load(&profile).expect("disabled strategies must not consume the aggregate dispatch budget");
}

#[test]
fn aggregate_strategy_dispatch_budget_overflow_fails_closed() {
    let profile = BASE_PROFILE.replace(
        "poll_interval_ms = 1000",
        &format!("poll_interval_ms = 1000\ntick_timeout_ms = {}", u64::MAX),
    );
    let error = load(&profile).expect_err("aggregate serialized strategy budget should overflow");

    assert_aggregate_strategy_dispatch_budget_error(
        &error,
        1,
        &u64::MAX.to_string(),
        "overflow",
        "10000",
    );
}

fn assert_aggregate_strategy_dispatch_budget_error(
    error: &anyhow::Error,
    enabled_strategy_count: usize,
    per_strategy_timeout_ms: &str,
    aggregate_worst_case_ms: &str,
    cancel_all_after_window_ms: &str,
) {
    let message = error.to_string();
    assert!(
        message.contains(&format!("enabled_strategy_count={enabled_strategy_count}")),
        "error should report the enabled strategy count: {message}"
    );
    assert!(
        message.contains(&format!(
            "per_strategy_timeout_ms={per_strategy_timeout_ms}"
        )),
        "error should report the per-strategy timeout: {message}"
    );
    assert!(
        message.contains(&format!(
            "aggregate_worst_case_ms={aggregate_worst_case_ms}"
        )),
        "error should report the aggregate worst-case duration: {message}"
    );
    assert!(
        message.contains(&format!(
            "cancel_all_after_window_ms={cancel_all_after_window_ms}"
        )),
        "error should report the Cancel-All-After duration: {message}"
    );
}

#[test]
fn strategy_params_deserialize_for_okx_ema_atr_maker_trend() {
    let config = load(BASE_PROFILE).expect("profile should load decimal string params");
    let instance = &config.strategies.instances[0];
    let params = instance.params.okx_ema_atr_maker_trend();

    assert_eq!(instance.kind, StrategyKind::OkxEmaAtrMakerTrend);
    assert_eq!(
        params,
        &OkxEmaAtrMakerTrendConfig {
            fast_ema_period: 2,
            slow_ema_period: 5,
            atr_period: 3,
            quantity: Decimal::new(1, 3),
            operator_owned_base_balance: Decimal::ZERO,
            max_entry_order_age_ms: 15_000,
            max_quote_notional: Some(Decimal::new(500, 0)),
            max_quote_notional_by_instrument: BTreeMap::new(),
            entry_offset_atr_multiple: Decimal::new(1, 1),
            min_entry_offset_bps: Decimal::new(10, 1),
            max_entry_offset_bps: Decimal::new(150, 1),
            take_profit_atr_multiple: Decimal::new(15, 1),
            stop_loss_atr_multiple: Decimal::ONE,
        }
    );
}

#[test]
fn rejects_entry_order_age_outside_bounded_window() {
    for max_entry_order_age_ms in [999, 60_001] {
        let profile = BASE_PROFILE.replace(
            "quantity = \"0.001\"",
            &format!("quantity = \"0.001\"\nmax_entry_order_age_ms = {max_entry_order_age_ms}"),
        );
        let error = load(&profile).expect_err("unsafe entry order age should fail");
        assert!(
            error
                .to_string()
                .contains("max_entry_order_age_ms must be between 1000 and 60000"),
            "invalid entry age should be explicit: {error}"
        );
    }
}

#[test]
fn parses_optional_decimal_string_param() {
    let profile = BASE_PROFILE.replace(
        "max_quote_notional = \"500\"",
        "max_quote_notional = \"12.34\"",
    );
    let config = load(&profile).expect("profile should load optional decimal string param");
    let params = config.strategies.instances[0]
        .params
        .okx_ema_atr_maker_trend();

    assert_eq!(params.max_quote_notional, Some(Decimal::new(1234, 2)));
}

#[test]
fn parses_operator_owned_base_balance_decimal_string() {
    let profile = BASE_PROFILE.replace(
        "quantity = \"0.001\"",
        "quantity = \"0.001\"\noperator_owned_base_balance = \"1.25\"",
    );
    let config = load(&profile).expect("profile should load operator-owned balance");
    let params = config.strategies.instances[0]
        .params
        .okx_ema_atr_maker_trend();

    assert_eq!(params.operator_owned_base_balance, Decimal::new(125, 2));
}

#[test]
fn parses_per_instrument_quote_cap_decimal_strings() {
    let profile = BASE_PROFILE.replace(
        "max_quote_notional = \"500\"",
        "max_quote_notional_by_instrument = { \"BTC-USDT\" = \"12.34\" }",
    );
    let config = load(&profile).expect("profile should load per-instrument decimal caps");
    let params = config.strategies.instances[0]
        .params
        .okx_ema_atr_maker_trend();

    let mut expected_caps = BTreeMap::new();
    expected_caps.insert("BTC-USDT".to_owned(), Decimal::new(1234, 2));
    assert_eq!(params.max_quote_notional, None);
    assert_eq!(params.max_quote_notional_by_instrument, expected_caps);
}

#[test]
fn rejects_quote_cap_key_without_selected_configured_instrument_id() {
    let profile = BASE_PROFILE.replace(
        "max_quote_notional = \"500\"",
        "max_quote_notional_by_instrument = { \"ETH-USDT\" = \"500\" }",
    );
    let error = load(&profile).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("must reference selected OkxEmaAtrMakerTrend instrument"),
        "quote cap keys should stay tied to selected configured instrument IDs: {error}"
    );
}

#[test]
fn rejects_okx_ema_atr_maker_trend_unsupported_bar() {
    for bar in ["1s", "5m"] {
        let profile = BASE_PROFILE.replace("bar = \"1m\"", &format!("bar = \"{bar}\""));
        let error = load(&profile).unwrap_err();

        assert!(
            error.to_string().contains("bar must be 1m"),
            "OkxEmaAtrMakerTrend should reject unsupported bar {bar}: {error}"
        );
    }
}

#[test]
fn rejects_legacy_okx_m5_maker_trend_kind_after_rename() {
    let profile = BASE_PROFILE.replace(
        "kind = \"okx_ema_atr_maker_trend\"",
        "kind = \"okx_m5_maker_trend\"",
    );
    let error = load(&profile).expect_err("legacy strategy kind should fail strict parsing");

    assert!(
        error.to_string().contains("failed parsing TOML profile"),
        "legacy strategy kind should fail during strict TOML parsing: {error}"
    );
}

#[test]
fn accepts_okx_ema_atr_maker_trend_one_minute_timing_without_one_second_cadence() {
    let slow_poll = BASE_PROFILE.replace("poll_interval_ms = 1000", "poll_interval_ms = 2000");
    load(&slow_poll).expect("1m strategy should not require 1-second polling");

    let stale_ws = BASE_PROFILE.replace("max_staleness_ms = 1000", "max_staleness_ms = 3000");
    load(&stale_ws).expect("1m strategy should not require 1-second WebSocket staleness");
}

#[test]
fn rejects_unknown_okx_ema_atr_maker_trend_params() {
    let profile = BASE_PROFILE.replace(
        "stop_loss_atr_multiple = \"1.0\"",
        "stop_loss_atr_multiple = \"1.0\"\nunknown_param = \"1\"",
    );
    let serde_error = toml::from_str::<BotConfig>(&profile)
        .expect_err("unknown strategy params should fail strict TOML parsing");

    assert!(
        serde_error
            .to_string()
            .contains("invalid okx_ema_atr_maker_trend params"),
        "unknown strategy param should be rejected by kind-specific params parser: {serde_error}"
    );
    assert!(
        serde_error.to_string().contains("unknown field"),
        "unknown strategy param should preserve strict unknown-field rejection: {serde_error}"
    );

    let load_error = load(&profile).expect_err("unknown strategy params should fail profile load");
    assert!(
        load_error
            .to_string()
            .contains("failed parsing TOML profile"),
        "loader should preserve the existing strict parse error surface: {load_error}"
    );
}

#[test]
fn okx_ema_atr_maker_trend_default_values_remain_unchanged() {
    fn assert_decimal(_: Decimal) {}
    fn assert_optional_decimal(_: Option<Decimal>) {}
    fn assert_decimal_map(_: &BTreeMap<String, Decimal>) {}

    let config = OkxEmaAtrMakerTrendConfig::default();
    let expected = OkxEmaAtrMakerTrendConfig {
        fast_ema_period: 20,
        slow_ema_period: 100,
        atr_period: 14,
        quantity: Decimal::new(1, 3),
        operator_owned_base_balance: Decimal::ZERO,
        max_entry_order_age_ms: 15_000,
        max_quote_notional: None,
        max_quote_notional_by_instrument: BTreeMap::new(),
        entry_offset_atr_multiple: Decimal::new(1, 1),
        min_entry_offset_bps: Decimal::ONE,
        max_entry_offset_bps: Decimal::new(150, 1),
        take_profit_atr_multiple: Decimal::new(2, 0),
        stop_loss_atr_multiple: Decimal::new(15, 1),
    };

    assert_decimal(config.quantity);
    assert_decimal(config.operator_owned_base_balance);
    assert_optional_decimal(config.max_quote_notional);
    assert_decimal_map(&config.max_quote_notional_by_instrument);
    assert_decimal(config.entry_offset_atr_multiple);
    assert_decimal(config.min_entry_offset_bps);
    assert_decimal(config.max_entry_offset_bps);
    assert_decimal(config.take_profit_atr_multiple);
    assert_decimal(config.stop_loss_atr_multiple);
    assert_eq!(config, expected);
}

#[test]
fn rejects_numeric_toml_for_decimal_string_strategy_fields() {
    for (old_line, new_line) in [
        ("quantity = \"0.001\"", "quantity = 0.001"),
        (
            "quantity = \"0.001\"",
            "quantity = \"0.001\"\noperator_owned_base_balance = 1",
        ),
        ("max_quote_notional = \"500\"", "max_quote_notional = 500"),
        (
            "entry_offset_atr_multiple = \"0.1\"",
            "entry_offset_atr_multiple = 0.1",
        ),
        ("min_entry_offset_bps = \"1.0\"", "min_entry_offset_bps = 1"),
        (
            "max_entry_offset_bps = \"15.0\"",
            "max_entry_offset_bps = 15",
        ),
        (
            "take_profit_atr_multiple = \"1.5\"",
            "take_profit_atr_multiple = 1.5",
        ),
        (
            "stop_loss_atr_multiple = \"1.0\"",
            "stop_loss_atr_multiple = 1",
        ),
    ] {
        let profile = BASE_PROFILE.replace(old_line, new_line);
        let error = load(&profile).expect_err("numeric TOML decimal field should fail");

        assert!(
            error.to_string().contains("failed parsing TOML profile"),
            "{new_line:?} should be rejected as non-string exact decimal config: {error}"
        );
    }
}

#[test]
fn rejects_malformed_decimal_string_strategy_fields() {
    for (old_line, new_line, expected) in [
        (
            "quantity = \"0.001\"",
            "quantity = \"\"",
            "must not be empty",
        ),
        (
            "quantity = \"0.001\"",
            "quantity = \" 0.001\"",
            "must not contain leading or trailing whitespace",
        ),
        (
            "quantity = \"0.001\"",
            "quantity = \"1e-3\"",
            "must use plain decimal notation",
        ),
        (
            "quantity = \"0.001\"",
            "quantity = \"0.00000000000000000000000000001\"",
            "must not exceed 28 fractional digits",
        ),
        (
            "take_profit_atr_multiple = \"1.5\"",
            "take_profit_atr_multiple = \"abc\"",
            "invalid decimal string",
        ),
    ] {
        let profile = BASE_PROFILE.replace(old_line, new_line);
        let error = load(&profile).expect_err("malformed decimal string should fail");
        let error_chain = format!("{error:#}");

        assert!(
            error_chain.contains(expected),
            "{new_line:?} should fail with {expected:?}: {error}"
        );
    }
}

#[test]
fn rejects_numeric_toml_for_per_instrument_quote_caps() {
    let profile = BASE_PROFILE.replace(
        "max_quote_notional = \"500\"",
        "max_quote_notional_by_instrument = { \"BTC-USDT\" = 500 }",
    );
    let error = load(&profile).expect_err("numeric per-instrument quote cap should fail");

    assert!(
        error.to_string().contains("failed parsing TOML profile"),
        "per-instrument quote cap values should require strings: {error}"
    );
}

#[test]
fn rejects_invalid_ema_atr_period_strategy_values() {
    for (old_line, new_line, expected) in [
        (
            "fast_ema_period = 2",
            "fast_ema_period = 0",
            "fast_ema_period must be positive",
        ),
        (
            "slow_ema_period = 5",
            "slow_ema_period = 2",
            "slow_ema_period must be greater than fast_ema_period",
        ),
        (
            "atr_period = 3",
            "atr_period = 1",
            "atr_period must be greater than 1",
        ),
    ] {
        let profile = BASE_PROFILE.replace(old_line, new_line);
        let error = load(&profile).expect_err("invalid EMA/ATR period should fail");

        assert!(
            error.to_string().contains(expected),
            "{new_line:?} should fail with {expected:?}: {error}"
        );
    }
}

#[test]
fn rejects_invalid_decimal_strategy_values() {
    for (old_line, new_line, expected) in [
        (
            "quantity = \"0.001\"",
            "quantity = \"0\"",
            "quantity must be positive",
        ),
        (
            "quantity = \"0.001\"",
            "quantity = \"-0.001\"",
            "quantity must be positive",
        ),
        (
            "quantity = \"0.001\"",
            "quantity = \"0.001\"\noperator_owned_base_balance = \"-1\"",
            "operator_owned_base_balance must be non-negative",
        ),
        (
            "max_quote_notional = \"500\"",
            "max_quote_notional = \"0\"",
            "max_quote_notional must be positive",
        ),
        (
            "max_quote_notional = \"500\"",
            "max_quote_notional = \"-1\"",
            "max_quote_notional must be positive",
        ),
        (
            "entry_offset_atr_multiple = \"0.1\"",
            "entry_offset_atr_multiple = \"1.1\"",
            "entry_offset_atr_multiple must be positive and reasonable",
        ),
        (
            "min_entry_offset_bps = \"1.0\"",
            "min_entry_offset_bps = \"0\"",
            "min_entry_offset_bps must be positive and reasonable",
        ),
        (
            "max_entry_offset_bps = \"15.0\"",
            "max_entry_offset_bps = \"101\"",
            "max_entry_offset_bps must be positive and reasonable",
        ),
        (
            "min_entry_offset_bps = \"1.0\"",
            "min_entry_offset_bps = \"20.0\"",
            "min_entry_offset_bps must be less than or equal",
        ),
        (
            "take_profit_atr_multiple = \"1.5\"",
            "take_profit_atr_multiple = \"11\"",
            "take_profit_atr_multiple must be positive and reasonable",
        ),
        (
            "stop_loss_atr_multiple = \"1.0\"",
            "stop_loss_atr_multiple = \"11\"",
            "stop_loss_atr_multiple must be positive and reasonable",
        ),
    ] {
        let profile = BASE_PROFILE.replace(old_line, new_line);
        let error = load(&profile).expect_err("invalid decimal strategy value should fail");

        assert!(
            error.to_string().contains(expected),
            "{new_line:?} should fail with {expected:?}: {error}"
        );
    }
}

#[test]
fn numeric_boundary_rejects_order_critical_decimal_string_edges() {
    for (case, old_line, new_line, expected) in [
        (
            "malformed order size",
            "quantity = \"0.001\"",
            "quantity = \"1.2.3\"",
            "failed parsing TOML profile",
        ),
        (
            "zero order size",
            "quantity = \"0.001\"",
            "quantity = \"0\"",
            "quantity must be positive",
        ),
        (
            "negative order size",
            "quantity = \"0.001\"",
            "quantity = \"-0.001\"",
            "quantity must be positive",
        ),
        (
            "over-precision order size",
            "quantity = \"0.001\"",
            "quantity = \"0.00000000000000000000000000001\"",
            "must not exceed 28 fractional digits",
        ),
        (
            "rounding over-precision order size",
            "quantity = \"0.001\"",
            "quantity = \"0.00000000000000000000000000009\"",
            "must not exceed 28 fractional digits",
        ),
        (
            "extreme order size",
            "quantity = \"0.001\"",
            "quantity = \"100000000000000000000000000000\"",
            "failed parsing TOML profile",
        ),
        (
            "scientific notation order size",
            "quantity = \"0.001\"",
            "quantity = \"1e-3\"",
            "failed parsing TOML profile",
        ),
        (
            "malformed quote cap",
            "max_quote_notional = \"500\"",
            "max_quote_notional = \"abc\"",
            "failed parsing TOML profile",
        ),
        (
            "zero quote cap",
            "max_quote_notional = \"500\"",
            "max_quote_notional = \"0\"",
            "max_quote_notional must be positive",
        ),
        (
            "negative quote cap",
            "max_quote_notional = \"500\"",
            "max_quote_notional = \"-1\"",
            "max_quote_notional must be positive",
        ),
        (
            "very large quote cap",
            "max_quote_notional = \"500\"",
            "max_quote_notional = \"100000000000000000000000000000\"",
            "failed parsing TOML profile",
        ),
        (
            "whitespace-padded quote cap",
            "max_quote_notional = \"500\"",
            "max_quote_notional = \"500 \"",
            "must not contain leading or trailing whitespace",
        ),
        (
            "over-precision per-instrument quote cap",
            "max_quote_notional = \"500\"",
            "max_quote_notional_by_instrument = { \"BTC-USDT\" = \"0.00000000000000000000000000001\" }",
            "must not exceed 28 fractional digits",
        ),
        (
            "extreme per-instrument quote cap",
            "max_quote_notional = \"500\"",
            "max_quote_notional_by_instrument = { \"BTC-USDT\" = \"100000000000000000000000000000\" }",
            "failed parsing TOML profile",
        ),
        (
            "scientific notation per-instrument quote cap",
            "max_quote_notional = \"500\"",
            "max_quote_notional_by_instrument = { \"BTC-USDT\" = \"5e2\" }",
            "failed parsing TOML profile",
        ),
        (
            "malformed entry offset",
            "entry_offset_atr_multiple = \"0.1\"",
            "entry_offset_atr_multiple = \"nan\"",
            "failed parsing TOML profile",
        ),
        (
            "zero entry offset",
            "entry_offset_atr_multiple = \"0.1\"",
            "entry_offset_atr_multiple = \"0\"",
            "entry_offset_atr_multiple must be positive and reasonable",
        ),
        (
            "negative entry offset",
            "entry_offset_atr_multiple = \"0.1\"",
            "entry_offset_atr_multiple = \"-0.1\"",
            "entry_offset_atr_multiple must be positive and reasonable",
        ),
        (
            "scientific notation entry offset",
            "entry_offset_atr_multiple = \"0.1\"",
            "entry_offset_atr_multiple = \"1e-1\"",
            "failed parsing TOML profile",
        ),
        (
            "over-precision min entry bps",
            "min_entry_offset_bps = \"1.0\"",
            "min_entry_offset_bps = \"0.00000000000000000000000000001\"",
            "must not exceed 28 fractional digits",
        ),
        (
            "extreme max entry bps",
            "max_entry_offset_bps = \"15.0\"",
            "max_entry_offset_bps = \"100000000000000000000000000000\"",
            "failed parsing TOML profile",
        ),
        (
            "scientific notation max entry bps",
            "max_entry_offset_bps = \"15.0\"",
            "max_entry_offset_bps = \"1e1\"",
            "failed parsing TOML profile",
        ),
        (
            "malformed take profit",
            "take_profit_atr_multiple = \"1.5\"",
            "take_profit_atr_multiple = \"1..5\"",
            "failed parsing TOML profile",
        ),
        (
            "zero take profit",
            "take_profit_atr_multiple = \"1.5\"",
            "take_profit_atr_multiple = \"0\"",
            "take_profit_atr_multiple must be positive and reasonable",
        ),
        (
            "negative take profit",
            "take_profit_atr_multiple = \"1.5\"",
            "take_profit_atr_multiple = \"-1.5\"",
            "take_profit_atr_multiple must be positive and reasonable",
        ),
        (
            "scientific notation take profit",
            "take_profit_atr_multiple = \"1.5\"",
            "take_profit_atr_multiple = \"1e0\"",
            "failed parsing TOML profile",
        ),
        (
            "over-precision stop loss",
            "stop_loss_atr_multiple = \"1.0\"",
            "stop_loss_atr_multiple = \"0.00000000000000000000000000001\"",
            "must not exceed 28 fractional digits",
        ),
        (
            "extreme stop loss",
            "stop_loss_atr_multiple = \"1.0\"",
            "stop_loss_atr_multiple = \"100000000000000000000000000000\"",
            "failed parsing TOML profile",
        ),
        (
            "whitespace-padded stop loss",
            "stop_loss_atr_multiple = \"1.0\"",
            "stop_loss_atr_multiple = \" 1.0\"",
            "must not contain leading or trailing whitespace",
        ),
    ] {
        let profile = BASE_PROFILE.replace(old_line, new_line);
        let error = load(&profile).unwrap_err();
        let error_chain = format!("{error:#}");

        assert!(
            error_chain.contains(expected),
            "{case} should fail with {expected:?}: {error}"
        );
    }
}

#[test]
fn numeric_boundary_accepts_very_small_valid_decimal_strings() {
    let profile = BASE_PROFILE
        .replace("quantity = \"0.001\"", "quantity = \"0.00000001\"")
        .replace(
            "max_quote_notional = \"500\"",
            "max_quote_notional = \"0.0001\"",
        )
        .replace(
            "entry_offset_atr_multiple = \"0.1\"",
            "entry_offset_atr_multiple = \"0.00000001\"",
        )
        .replace(
            "min_entry_offset_bps = \"1.0\"",
            "min_entry_offset_bps = \"0.00000001\"",
        )
        .replace(
            "max_entry_offset_bps = \"15.0\"",
            "max_entry_offset_bps = \"0.00000002\"",
        )
        .replace(
            "take_profit_atr_multiple = \"1.5\"",
            "take_profit_atr_multiple = \"0.00000001\"",
        )
        .replace(
            "stop_loss_atr_multiple = \"1.0\"",
            "stop_loss_atr_multiple = \"0.00000001\"",
        );

    let config = load(&profile).expect("small positive decimal strings should validate");
    let params = config.strategies.instances[0]
        .params
        .okx_ema_atr_maker_trend();

    assert_eq!(params.quantity, Decimal::new(1, 8));
    assert_eq!(params.max_quote_notional, Some(Decimal::new(1, 4)));
    assert_eq!(params.entry_offset_atr_multiple, Decimal::new(1, 8));
    assert_eq!(params.min_entry_offset_bps, Decimal::new(1, 8));
    assert_eq!(params.max_entry_offset_bps, Decimal::new(2, 8));
    assert_eq!(params.take_profit_atr_multiple, Decimal::new(1, 8));
    assert_eq!(params.stop_loss_atr_multiple, Decimal::new(1, 8));
}

#[test]
fn accepts_enabled_okx_ema_atr_maker_trend_ids_that_collided_under_legacy_prefix_tag() {
    let profile = profile_with_additional_strategy("okx-ema-atr-maker-eth-usdt", true).replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 2500",
    );

    let config = load(&profile)
        .expect("enabled strategies with distinct full-ID ownership tags should validate");

    assert_eq!(
        config
            .strategies
            .instances
            .iter()
            .filter(|instance| instance.enabled)
            .count(),
        2
    );
}

#[test]
fn accepts_disabled_okx_ema_atr_maker_trend_with_distinct_ownership_tag() {
    let profile = profile_with_additional_strategy("okx-ema-atr-maker-eth-usdt", false);

    let config =
        load(&profile).expect("disabled strategies with distinct ownership tags should validate");

    assert_eq!(
        config
            .strategies
            .instances
            .iter()
            .filter(|instance| instance.enabled)
            .count(),
        1
    );
}

#[test]
fn disabled_invalid_okx_ema_atr_maker_trend_does_not_affect_startup() {
    let profile = format!(
        "{BASE_PROFILE}{}",
        r#"
[[strategies.instances]]
kind = "okx_ema_atr_maker_trend"
id = "disabled-invalid-okx-ema-atr-maker"
enabled = false
instrument = "BTC-SWAP"
inst_type = "SPOT"
td_mode = "cash"
bar = "5m"

[strategies.instances.params]
fast_ema_period = 0
slow_ema_period = 0
atr_period = 1
quantity = "0"
max_quote_notional = "-1"
max_quote_notional_by_instrument = { "BTC-SWAP" = "-1" }
entry_offset_atr_multiple = "1.1"
min_entry_offset_bps = "20.0"
max_entry_offset_bps = "10.0"
take_profit_atr_multiple = "11"
stop_loss_atr_multiple = "11"
"#
    );

    let config = load(&profile).expect("disabled invalid strategy settings should be ignored");

    assert_eq!(
        config
            .strategies
            .instances
            .iter()
            .filter(|instance| instance.enabled)
            .count(),
        1
    );
}

#[test]
fn trading_safety_matrix_rejects_enabled_okx_ema_atr_maker_trend_ownership_tag_collisions() {
    let profile = BASE_PROFILE.replace(
        "id = \"okx-ema-atr-maker-btc-usdt\"",
        "id = \"collider-2pvu\"",
    );
    let profile = format!(
        "{profile}{}",
        strategy_instance_block("collider-d3ea", true)
    )
    .replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 2500",
    );

    let error = load(&profile).unwrap_err();

    assert!(
        error.to_string().contains("ownership tags must be unique"),
        "enabled ownership tag collision should fail validation: {error}"
    );
}

#[test]
fn trading_safety_matrix_rejects_disabled_okx_ema_atr_maker_trend_ownership_tag_collisions() {
    let profile = BASE_PROFILE.replace(
        "id = \"okx-ema-atr-maker-btc-usdt\"",
        "id = \"collider-2pvu\"",
    );
    let profile = format!(
        "{profile}{}",
        strategy_instance_block("collider-d3ea", false)
    );

    let error = load(&profile).unwrap_err();

    assert!(
        error.to_string().contains("ownership tags must be unique"),
        "disabled ownership tag collision should fail validation: {error}"
    );
}

#[test]
fn rejects_duplicate_enabled_strategy_instance_ids() {
    let profile = profile_with_additional_strategy("okx-ema-atr-maker-btc-usdt", true).replace(
        "poll_interval_ms = 1000",
        "poll_interval_ms = 1000\ntick_timeout_ms = 2500",
    );

    let error = load(&profile).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("enabled strategy instance ids must be unique"),
        "duplicate enabled strategy IDs should fail validation: {error}"
    );
}

#[test]
fn rejects_invalid_okx_websocket_timing_values() {
    for (old_line, new_line, expected) in [
        (
            "max_staleness_ms = 1000",
            "max_staleness_ms = 0",
            "websocket.max_staleness_ms must be non-zero",
        ),
        (
            "reconnect_initial_backoff_ms = 500",
            "reconnect_initial_backoff_ms = 0",
            "websocket.reconnect_initial_backoff_ms must be non-zero",
        ),
        (
            "reconnect_max_backoff_ms = 10000",
            "reconnect_max_backoff_ms = 499",
            "websocket.reconnect_max_backoff_ms must be greater than or equal",
        ),
    ] {
        let profile = BASE_PROFILE.replace(old_line, new_line);
        let error = load(&profile).unwrap_err();

        assert!(
            error.to_string().contains(expected),
            "invalid OKX WebSocket timing value should fail with {expected:?}: {error}"
        );
    }
}

#[test]
fn rejects_account_id_environment_placeholders() {
    let profile = BASE_PROFILE.replace(
        "account_id = \"OKX-PUBLIC-DEMO\"",
        "account_id = \"${OKX_ACCOUNT_ID}\"",
    );
    let error = load(&profile).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("environment placeholders are not allowed in okx.account_id"),
        "account_id placeholder should be rejected as non-secret config: {error}"
    );
}

#[test]
fn rejects_non_okx_account_ids() {
    for account_id in ["master", "BINANCE-master", "okx-master"] {
        let profile = BASE_PROFILE.replace(
            "account_id = \"OKX-PUBLIC-DEMO\"",
            &format!("account_id = \"{account_id}\""),
        );
        let error = load(&profile).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("OKX account_id must use an OKX-prefixed identifier"),
            "account_id {account_id:?} should fail as non-OKX config: {error}"
        );
    }
}

#[test]
fn rejects_okx_websocket_url_environment_placeholders() {
    for field in [
        "base_url_ws_public",
        "base_url_ws_private",
        "base_url_ws_business",
    ] {
        let suffix = match field {
            "base_url_ws_public" => "public",
            "base_url_ws_private" => "private",
            "base_url_ws_business" => "business",
            _ => unreachable!("field list is exhaustive"),
        };
        let old_line = format!("{field} = \"wss://wseeapap.okx.com:8443/ws/v5/{suffix}\"");
        let profile = BASE_PROFILE.replace(&old_line, &format!("{field} = \"${{OKX_WS_URL}}\""));
        let error = load(&profile).unwrap_err();

        assert!(
            error.to_string().contains(&format!(
                "environment placeholders are not allowed in okx.{field}"
            )),
            "{field} placeholder should be rejected as non-secret routing config: {error}"
        );
    }
}

#[test]
fn rejects_okx_proxy_url_environment_placeholders() {
    let profile = BASE_PROFILE.replace(
        "request_timeout_ms = 60000",
        "request_timeout_ms = 60000\nproxy_url = \"${OKX_PROXY_URL}\"",
    );
    let error = load(&profile).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("environment placeholders are not allowed in okx.proxy_url"),
        "proxy_url placeholder should be rejected as non-secret routing config: {error}"
    );
}

#[test]
fn rejects_quote_notional_cap_key_environment_placeholders() {
    let profile = BASE_PROFILE.replace(
        "max_quote_notional = \"500\"",
        "max_quote_notional_by_instrument = { \"${OKX_SYMBOL}\" = \"500\" }",
    );
    let error = load(&profile).unwrap_err();

    assert!(
        error.to_string().contains("environment placeholders are not allowed in strategies.instances[0].params.max_quote_notional_by_instrument"),
        "per-instrument quote cap keys should reject placeholders as non-secret config: {error}"
    );
}

#[test]
fn live_profile_can_be_strategy_empty() {
    let profile = BASE_PROFILE
        .replace("order_intent = \"demo-okx-spot-confirmed\"\n", "")
        .replace(
            r#"
[[strategies.instances]]
kind = "okx_ema_atr_maker_trend"
id = "okx-ema-atr-maker-btc-usdt"
instrument = "BTC-USDT"
inst_type = "SPOT"
td_mode = "cash"
bar = "1m"

[strategies.instances.params]
fast_ema_period = 2
slow_ema_period = 5
atr_period = 3
quantity = "0.001"
max_quote_notional = "500"
entry_offset_atr_multiple = "0.1"
min_entry_offset_bps = "1.0"
max_entry_offset_bps = "15.0"
take_profit_atr_multiple = "1.5"
stop_loss_atr_multiple = "1.0"
"#,
            "",
        )
        .replace("wseeapap.okx.com", "wseea.okx.com");
    let profile = profile.replace(
        "trading_service = \"DEMO\"",
        "trading_service = \"PRODUCTION\"",
    );

    let config = load(&profile).expect("strategy-empty live profile should load");

    assert_eq!(config.strategies.instances, []);
}

#[test]
fn checked_in_example_is_inert_demo_only_and_uses_placeholders() {
    let path = "config/example.toml";
    let example = load_checked_in_profile(path).expect("example profile should load");
    let okx = example
        .okx
        .as_ref()
        .expect("example profile should configure OKX");

    assert_eq!(example.product.name, "okx-rust-trading");
    assert_eq!(example.runtime.trader_id, "PUBLIC-DEMO-OPERATOR");
    assert_eq!(example.runtime.order_intent, None);
    assert_eq!(okx.account_id, "OKX-PUBLIC-DEMO");
    assert_eq!(okx.api_domain, OkxApiDomain::Global);
    assert_eq!(okx.account_jurisdiction, OkxAccountJurisdiction::Other);
    assert_eq!(okx.trading_service, OkxTradingService::Demo);
    assert_eq!(okx.base_url, "https://openapi.okx.com");
    assert_eq!(
        okx.base_url_ws_public.as_deref(),
        Some("wss://wspap.okx.com:8443/ws/v5/public")
    );
    assert_eq!(example.instruments[0].okx_instrument_id(), "BTC-USDT");
    assert!(example.strategies.instances.is_empty());

    let contents = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path),
    )
    .expect("checked-in example should be readable");
    let parsed =
        toml::from_str::<toml::Value>(&contents).expect("checked-in example should parse as TOML");
    let okx = parsed
        .get("okx")
        .and_then(toml::Value::as_table)
        .expect("checked-in example should contain an [okx] table");
    for (field, expected) in [
        ("api_key", "${OKX_API_KEY}"),
        ("api_secret", "${OKX_API_SECRET}"),
        ("api_passphrase", "${OKX_API_PASSPHRASE}"),
        ("account_id", "OKX-PUBLIC-DEMO"),
    ] {
        assert_eq!(
            okx.get(field).and_then(toml::Value::as_str),
            Some(expected),
            "{path} should set okx.{field} to {expected}"
        );
    }
}

#[test]
fn resolves_okx_secret_placeholders_from_file_environment_variables() {
    let dir = tempfile::tempdir().expect("temp dir should be available");
    let api_key_path = write_secret_file(dir.path(), "okx_api_key", "file-api-key\n");
    let api_secret_path = write_secret_file(dir.path(), "okx_api_secret", "file-api-secret\n");
    let api_passphrase_path =
        write_secret_file(dir.path(), "okx_api_passphrase", "file-passphrase\n");
    let file_env = HashMap::from([
        ("OKX_API_KEY_FILE", api_key_path),
        ("OKX_API_SECRET_FILE", api_secret_path),
        ("OKX_API_PASSPHRASE_FILE", api_passphrase_path),
    ]);

    let config =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| file_env.get(name).cloned())
            .expect("file-based OKX secrets should resolve");
    let okx = config.okx.expect("OKX config should resolve");

    assert_eq!(okx.api_key.as_str(), "file-api-key");
    assert_eq!(okx.api_secret.as_str(), "file-api-secret");
    assert_eq!(okx.api_passphrase.as_str(), "file-passphrase");
}

#[test]
fn resolves_okx_secret_placeholders_from_direct_secret_file_paths() {
    let dir = tempfile::tempdir().expect("temp dir should be available");
    let api_key_path = write_secret_file(dir.path(), "okx_api_key", "file-api-key\n");
    let api_secret_path = write_secret_file(dir.path(), "okx_api_secret", "file-api-secret\n");
    let api_passphrase_path =
        write_secret_file(dir.path(), "okx_api_passphrase", "file-passphrase\n");
    let env = HashMap::from([
        ("OKX_API_KEY", api_key_path),
        ("OKX_API_SECRET", api_secret_path),
        ("OKX_API_PASSPHRASE", api_passphrase_path),
    ]);

    let config =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
            .expect("direct path-valued OKX secrets should resolve");
    let okx = config.okx.expect("OKX config should resolve");

    assert_eq!(okx.api_key.as_str(), "file-api-key");
    assert_eq!(okx.api_secret.as_str(), "file-api-secret");
    assert_eq!(okx.api_passphrase.as_str(), "file-passphrase");
}

#[test]
fn direct_okx_secret_environment_variables_take_precedence_over_file_variables() {
    let dir = tempfile::tempdir().expect("temp dir should be available");
    let api_key_path = write_secret_file(dir.path(), "okx_api_key", "file-api-key");
    let api_secret_path = write_secret_file(dir.path(), "okx_api_secret", "file-api-secret");
    let api_passphrase_path = write_secret_file(dir.path(), "okx_api_passphrase", "file-pass");
    let env = HashMap::from([
        ("OKX_API_KEY", "direct-api-key".to_owned()),
        ("OKX_API_SECRET", "direct-api-secret".to_owned()),
        ("OKX_API_PASSPHRASE", "direct-passphrase".to_owned()),
        ("OKX_API_KEY_FILE", api_key_path),
        ("OKX_API_SECRET_FILE", api_secret_path),
        ("OKX_API_PASSPHRASE_FILE", api_passphrase_path),
    ]);

    let config =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
            .expect("direct OKX secrets should resolve before file fallbacks");
    let okx = config.okx.expect("OKX config should resolve");

    assert_eq!(okx.api_key.as_str(), "direct-api-key");
    assert_eq!(okx.api_secret.as_str(), "direct-api-secret");
    assert_eq!(okx.api_passphrase.as_str(), "direct-passphrase");
}

#[test]
fn okx_secret_environment_variables_reject_surrounding_whitespace() {
    for padded_api_key in [
        " demo-api-key",
        "demo-api-key ",
        "demo-api-key\n",
        "demo-api-key\r\n",
    ] {
        let env = HashMap::from([
            ("OKX_API_KEY", padded_api_key.to_owned()),
            ("OKX_API_SECRET", "demo-api-secret".to_owned()),
            ("OKX_API_PASSPHRASE", "demo-passphrase".to_owned()),
        ]);
        let error =
            load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
                .unwrap_err();

        assert!(
            error.to_string().contains("leading or trailing whitespace"),
            "whitespace-padded OKX secret env {padded_api_key:?} should fail validation: {error}"
        );
    }
}

#[test]
fn literal_okx_secrets_reject_surrounding_whitespace() {
    for (original, replacement) in [
        (
            "api_key = \"${OKX_API_KEY}\"",
            "api_key = \" demo-api-key\"",
        ),
        (
            "api_key = \"${OKX_API_KEY}\"",
            "api_key = \"demo-api-key \"",
        ),
        (
            "api_secret = \"${OKX_API_SECRET}\"",
            "api_secret = \" demo-api-secret\"",
        ),
        (
            "api_secret = \"${OKX_API_SECRET}\"",
            "api_secret = \"demo-api-secret \"",
        ),
        (
            "api_passphrase = \"${OKX_API_PASSPHRASE}\"",
            "api_passphrase = \" demo-passphrase\"",
        ),
        (
            "api_passphrase = \"${OKX_API_PASSPHRASE}\"",
            "api_passphrase = \"demo-passphrase \"",
        ),
    ] {
        let profile = BASE_PROFILE.replace(original, replacement);
        let error = load(&profile).unwrap_err();

        assert!(
            error.to_string().contains("leading or trailing whitespace"),
            "literal OKX secret {replacement:?} should fail validation: {error}"
        );
    }
}

#[test]
fn rejects_missing_okx_secret_environment_variables() {
    let error = load_config_from_str_with_secret_resolver(BASE_PROFILE, |_| None).unwrap_err();

    assert!(
        error.to_string().contains("OKX_API_KEY"),
        "missing OKX secret env should fail with the missing placeholder name: {error}"
    );
}

#[test]
fn okx_secret_file_environment_variables_reject_missing_files() {
    let env = HashMap::from([
        (
            "OKX_API_KEY_FILE",
            "/tmp/okx-rust-trading-missing-okx-api-key".to_owned(),
        ),
        ("OKX_API_SECRET", "demo-api-secret".to_owned()),
        ("OKX_API_PASSPHRASE", "demo-passphrase".to_owned()),
    ]);
    let error =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
            .unwrap_err();
    let debug = format!("{error:?}");

    assert!(
        debug.contains("failed reading secret file"),
        "missing OKX secret file should fail with file read context: {error}"
    );
}

#[test]
fn okx_secret_file_environment_variables_reject_empty_files() {
    let dir = tempfile::tempdir().expect("temp dir should be available");
    let api_key_path = write_secret_file(dir.path(), "okx_api_key", "\n");
    let env = HashMap::from([
        ("OKX_API_KEY_FILE", api_key_path),
        ("OKX_API_SECRET", "demo-api-secret".to_owned()),
        ("OKX_API_PASSPHRASE", "demo-passphrase".to_owned()),
    ]);
    let error =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
            .unwrap_err();

    assert!(
        error.to_string().contains("secret file")
            && error.to_string().contains("must not be empty"),
        "empty OKX secret file should fail validation: {error}"
    );
}

#[test]
fn okx_secret_file_environment_variables_reject_assignment_contents() {
    let dir = tempfile::tempdir().expect("temp dir should be available");
    let api_key_path = write_secret_file(dir.path(), "okx_api_key", "OKX_API_KEY=demo-key\n");
    let env = HashMap::from([
        ("OKX_API_KEY_FILE", api_key_path),
        ("OKX_API_SECRET", "demo-api-secret".to_owned()),
        ("OKX_API_PASSPHRASE", "demo-passphrase".to_owned()),
    ]);
    let error =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
            .unwrap_err();

    assert!(
        error.to_string().contains("not an environment assignment"),
        "assignment-style OKX secret file should fail validation: {error}"
    );
}

#[test]
fn okx_secret_file_environment_variables_reject_export_assignment_contents() {
    let dir = tempfile::tempdir().expect("temp dir should be available");
    let api_key_path = write_secret_file(dir.path(), "okx_api_key", "export OKX_API_KEY=demo\n");
    let env = HashMap::from([
        ("OKX_API_KEY_FILE", api_key_path),
        ("OKX_API_SECRET", "demo-api-secret".to_owned()),
        ("OKX_API_PASSPHRASE", "demo-passphrase".to_owned()),
    ]);
    let error =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
            .unwrap_err();

    assert!(
        error.to_string().contains("not an environment assignment"),
        "export-style OKX secret file should fail validation: {error}"
    );
}

#[test]
fn okx_secret_file_environment_variables_reject_cross_environment_assignment_contents() {
    let dir = tempfile::tempdir().expect("temp dir should be available");
    let api_key_path = write_secret_file(
        dir.path(),
        "okx_api_key",
        "OKX_API_KEY=production-key-in-demo-file\n",
    );
    let env = HashMap::from([
        ("OKX_API_KEY_FILE", api_key_path),
        ("OKX_API_SECRET", "demo-api-secret".to_owned()),
        ("OKX_API_PASSPHRASE", "demo-passphrase".to_owned()),
    ]);
    let error =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
            .unwrap_err();

    assert!(
        error.to_string().contains("not an environment assignment"),
        "cross-environment OKX assignment in a secret file should fail validation: {error}"
    );
}

#[test]
fn okx_secret_file_environment_variables_reject_multiline_contents() {
    let dir = tempfile::tempdir().expect("temp dir should be available");
    let api_key_path = write_secret_file(dir.path(), "okx_api_key", "demo-key\nextra-line\n");
    let env = HashMap::from([
        ("OKX_API_KEY_FILE", api_key_path),
        ("OKX_API_SECRET", "demo-api-secret".to_owned()),
        ("OKX_API_PASSPHRASE", "demo-passphrase".to_owned()),
    ]);
    let error =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
            .unwrap_err();

    assert!(
        error.to_string().contains("without embedded newlines"),
        "multi-line OKX secret file should fail validation: {error}"
    );
}

#[test]
fn okx_secret_file_environment_variables_reject_surrounding_whitespace() {
    let dir = tempfile::tempdir().expect("temp dir should be available");
    let api_key_path = write_secret_file(dir.path(), "okx_api_key", " demo-key\n");
    let env = HashMap::from([
        ("OKX_API_KEY_FILE", api_key_path),
        ("OKX_API_SECRET", "demo-api-secret".to_owned()),
        ("OKX_API_PASSPHRASE", "demo-passphrase".to_owned()),
    ]);
    let error =
        load_config_from_str_with_secret_resolver(BASE_PROFILE, |name| env.get(name).cloned())
            .unwrap_err();

    assert!(
        error.to_string().contains("leading or trailing whitespace"),
        "whitespace-padded OKX secret file should fail validation: {error}"
    );
}

#[test]
fn derives_okx_endpoints_from_api_domain_and_trading_service() {
    for (api_domain, trading_service, expected) in [
        (
            "GLOBAL",
            "PRODUCTION",
            (
                OkxApiDomain::Global,
                OkxTradingService::Production,
                "https://openapi.okx.com",
                "wss://ws.okx.com:8443/ws/v5/public",
                "wss://ws.okx.com:8443/ws/v5/private",
                "wss://ws.okx.com:8443/ws/v5/business",
            ),
        ),
        (
            "GLOBAL",
            "DEMO",
            (
                OkxApiDomain::Global,
                OkxTradingService::Demo,
                "https://openapi.okx.com",
                "wss://wspap.okx.com:8443/ws/v5/public",
                "wss://wspap.okx.com:8443/ws/v5/private",
                "wss://wspap.okx.com:8443/ws/v5/business",
            ),
        ),
        (
            "US_AU",
            "PRODUCTION",
            (
                OkxApiDomain::UsAu,
                OkxTradingService::Production,
                "https://us.okx.com",
                "wss://wsus.okx.com:8443/ws/v5/public",
                "wss://wsus.okx.com:8443/ws/v5/private",
                "wss://wsus.okx.com:8443/ws/v5/business",
            ),
        ),
        (
            "US_AU",
            "DEMO",
            (
                OkxApiDomain::UsAu,
                OkxTradingService::Demo,
                "https://us.okx.com",
                "wss://wsuspap.okx.com:8443/ws/v5/public",
                "wss://wsuspap.okx.com:8443/ws/v5/private",
                "wss://wsuspap.okx.com:8443/ws/v5/business",
            ),
        ),
        (
            "EEA",
            "PRODUCTION",
            (
                OkxApiDomain::Eea,
                OkxTradingService::Production,
                "https://eea.okx.com",
                "wss://wseea.okx.com:8443/ws/v5/public",
                "wss://wseea.okx.com:8443/ws/v5/private",
                "wss://wseea.okx.com:8443/ws/v5/business",
            ),
        ),
        (
            "EEA",
            "DEMO",
            (
                OkxApiDomain::Eea,
                OkxTradingService::Demo,
                "https://eea.okx.com",
                "wss://wseeapap.okx.com:8443/ws/v5/public",
                "wss://wseeapap.okx.com:8443/ws/v5/private",
                "wss://wseeapap.okx.com:8443/ws/v5/business",
            ),
        ),
    ] {
        let profile = profile_without_okx_endpoint_overrides()
            .replace(
                "api_domain = \"EEA\"",
                &format!("api_domain = \"{api_domain}\""),
            )
            .replace(
                "trading_service = \"DEMO\"",
                &format!("trading_service = \"{trading_service}\""),
            )
            .replace(
                "order_intent = \"demo-okx-spot-confirmed\"",
                if trading_service == "PRODUCTION" {
                    "order_intent = \"live-okx-spot-confirmed\""
                } else {
                    "order_intent = \"demo-okx-spot-confirmed\""
                },
            );
        let config = load(&profile).expect("regional OKX profile should load");
        let okx = config.okx.expect("OKX config should parse");

        assert_eq!(
            (
                okx.api_domain,
                okx.trading_service,
                okx.base_url.as_str(),
                okx.base_url_ws_public.as_deref(),
                okx.base_url_ws_private.as_deref(),
                okx.base_url_ws_business.as_deref(),
            ),
            (
                expected.0,
                expected.1,
                expected.2,
                Some(expected.3),
                Some(expected.4),
                Some(expected.5),
            )
        );
    }
}

#[test]
fn api_domain_and_account_jurisdiction_parse_independently() {
    let singapore_global = profile_without_okx_endpoint_overrides()
        .replace("api_domain = \"EEA\"", "api_domain = \"GLOBAL\"")
        .replace(
            "account_jurisdiction = \"EEA\"",
            "account_jurisdiction = \"SINGAPORE\"",
        );
    let okx = load(&singapore_global)
        .expect("Singapore jurisdiction with Global API transport should load")
        .okx
        .expect("OKX config should parse");
    assert_eq!(okx.api_domain, OkxApiDomain::Global);
    assert_eq!(okx.account_jurisdiction, OkxAccountJurisdiction::Singapore);
    assert_eq!(okx.base_url, "https://openapi.okx.com");

    let eea = load(&profile_without_okx_endpoint_overrides())
        .expect("EEA jurisdiction with EEA API transport should load")
        .okx
        .expect("OKX config should parse");
    assert_eq!(eea.api_domain, OkxApiDomain::Eea);
    assert_eq!(eea.account_jurisdiction, OkxAccountJurisdiction::Eea);
    assert_eq!(eea.base_url, "https://eea.okx.com");
}

#[test]
fn legacy_region_and_missing_explicit_routing_fields_fail_closed() {
    let legacy = BASE_PROFILE.replace(
        "api_domain = \"EEA\"\naccount_jurisdiction = \"EEA\"",
        "region = \"EU\"",
    );
    let error = load(&legacy).expect_err("legacy region must not infer jurisdiction");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("okx.region is ambiguous")
            && error_chain.contains("okx.api_domain")
            && error_chain.contains("okx.account_jurisdiction"),
        "{error_chain}"
    );

    for (profile, expected) in [
        (
            BASE_PROFILE.replace("api_domain = \"EEA\"\n", ""),
            "okx.api_domain is required",
        ),
        (
            BASE_PROFILE.replace("account_jurisdiction = \"EEA\"\n", ""),
            "okx.account_jurisdiction is required",
        ),
    ] {
        let error = load(&profile).expect_err("missing explicit field must fail");
        assert!(format!("{error:#}").contains(expected), "{error:#}");
    }
}

#[test]
fn shared_my_okx_web_domain_is_not_accepted_as_api_or_jurisdiction_evidence() {
    let profile = BASE_PROFILE.replace(
        "base_url = \"https://eea.okx.com\"",
        "base_url = \"https://my.okx.com\"",
    );
    let error = load(&profile).expect_err("my.okx.com must not be an API transport");
    assert!(
        error
            .to_string()
            .contains("shared Singapore/EEA web-service domain")
    );
}

#[test]
fn known_okx_api_hosts_must_match_the_explicit_api_domain() {
    let profile = BASE_PROFILE.replace("api_domain = \"EEA\"", "api_domain = \"GLOBAL\"");
    let error = load(&profile).expect_err("known EEA hosts cannot be paired with Global transport");
    assert!(error.to_string().contains("must match okx.api_domain"));
}

#[test]
fn eea_demo_default_endpoints_use_eea_rest_and_eea_paper_websocket_hosts() {
    let config = load(&profile_without_okx_endpoint_overrides())
        .expect("EU demo profile should derive OKX endpoint defaults");
    let okx = config.okx.expect("OKX config should parse");

    assert_eq!(
        (
            okx.api_domain,
            okx.trading_service,
            okx.base_url.as_str(),
            okx.base_url_ws_public.as_deref(),
            okx.base_url_ws_private.as_deref(),
            okx.base_url_ws_business.as_deref(),
        ),
        (
            OkxApiDomain::Eea,
            OkxTradingService::Demo,
            "https://eea.okx.com",
            Some("wss://wseeapap.okx.com:8443/ws/v5/public"),
            Some("wss://wseeapap.okx.com:8443/ws/v5/private"),
            Some("wss://wseeapap.okx.com:8443/ws/v5/business"),
        )
    );
}

#[test]
fn accepts_custom_https_okx_rest_base_url() {
    let profile = BASE_PROFILE.replace(
        "base_url = \"https://eea.okx.com\"",
        "base_url = \"https://okx-rest-proxy.example\"",
    );

    let config = load(&profile).expect("custom HTTPS OKX REST host should validate");
    let okx = config.okx.expect("OKX config should parse");

    assert_eq!(okx.base_url, "https://okx-rest-proxy.example");
}

#[test]
fn accepts_custom_wss_okx_websocket_base_urls() {
    let profile = BASE_PROFILE
        .replace(
            "base_url_ws_public = \"wss://wseeapap.okx.com:8443/ws/v5/public\"",
            "base_url_ws_public = \"wss://okx-ws-proxy.example/ws/v5/public\"",
        )
        .replace(
            "base_url_ws_private = \"wss://wseeapap.okx.com:8443/ws/v5/private\"",
            "base_url_ws_private = \"wss://okx-ws-proxy.example/ws/v5/private\"",
        )
        .replace(
            "base_url_ws_business = \"wss://wseeapap.okx.com:8443/ws/v5/business\"",
            "base_url_ws_business = \"wss://okx-ws-proxy.example/ws/v5/business\"",
        );

    let config = load(&profile).expect("custom WSS OKX WebSocket hosts should validate");
    let okx = config.okx.expect("OKX config should parse");

    assert_eq!(
        okx.base_url_ws_public.as_deref(),
        Some("wss://okx-ws-proxy.example/ws/v5/public")
    );
    assert_eq!(
        okx.base_url_ws_private.as_deref(),
        Some("wss://okx-ws-proxy.example/ws/v5/private")
    );
    assert_eq!(
        okx.base_url_ws_business.as_deref(),
        Some("wss://okx-ws-proxy.example/ws/v5/business")
    );
}

#[test]
fn accepts_custom_wss_proxy_alongside_known_service_hosts() {
    let profile = BASE_PROFILE.replace(
        "base_url_ws_public = \"wss://wseeapap.okx.com:8443/ws/v5/public\"",
        "base_url_ws_public = \"wss://okx-ws-proxy.example/ws/v5/public\"",
    );

    let config = load(&profile).expect("custom WSS proxy should not imply production routing");
    let okx = config.okx.expect("OKX config should parse");

    assert_eq!(okx.trading_service, OkxTradingService::Demo);
    assert_eq!(
        okx.base_url_ws_public.as_deref(),
        Some("wss://okx-ws-proxy.example/ws/v5/public")
    );
}

#[test]
fn accepts_custom_okx_proxy_url() {
    for proxy_url in [
        "http://okx-proxy.example:8080",
        "https://okx-proxy.example:8443",
    ] {
        let profile = BASE_PROFILE.replace(
            "request_timeout_ms = 60000",
            &format!("request_timeout_ms = 60000\nproxy_url = \"{proxy_url}\""),
        );

        let config = load(&profile).expect("custom OKX proxy URL should validate");
        let okx = config.okx.expect("OKX config should parse");

        assert_eq!(okx.proxy_url.as_deref(), Some(proxy_url));
    }
}

#[test]
fn rejects_invalid_okx_rest_base_url_values() {
    for (invalid, expected) in [
        ("http://okx-rest-proxy.example", "must use https"),
        (
            "https://user:pass@okx-rest-proxy.example",
            "must not include credentials",
        ),
        (
            " https://okx-rest-proxy.example",
            "must not contain leading or trailing whitespace",
        ),
        ("not a url", "must be a valid URL"),
    ] {
        let profile = BASE_PROFILE.replace(
            "base_url = \"https://eea.okx.com\"",
            &format!("base_url = \"{invalid}\""),
        );
        let error = load(&profile).unwrap_err();

        assert!(
            error.to_string().contains(expected),
            "invalid OKX REST base URL {invalid:?} should fail with {expected:?}: {error}"
        );
    }
}

#[test]
fn rejects_invalid_okx_websocket_base_url_values() {
    for field in [
        "base_url_ws_public",
        "base_url_ws_private",
        "base_url_ws_business",
    ] {
        for (invalid, expected) in [
            ("http://okx-ws-proxy.example", "must use wss"),
            ("https://okx-ws-proxy.example", "must use wss"),
            (
                "wss://user:pass@okx-ws-proxy.example",
                "must not include credentials",
            ),
            (
                " wss://okx-ws-proxy.example",
                "must not contain leading or trailing whitespace",
            ),
            ("not a url", "must be a valid URL"),
            ("", "must not be empty"),
        ] {
            let suffix = match field {
                "base_url_ws_public" => "public",
                "base_url_ws_private" => "private",
                "base_url_ws_business" => "business",
                _ => unreachable!("field list is exhaustive"),
            };
            let old_line = format!("{field} = \"wss://wseeapap.okx.com:8443/ws/v5/{suffix}\"");
            let profile = BASE_PROFILE.replace(&old_line, &format!("{field} = \"{invalid}\""));
            let error = load(&profile).unwrap_err();

            assert!(
                error.to_string().contains(expected),
                "invalid OKX WebSocket URL {field}={invalid:?} should fail with {expected:?}: {error}"
            );
        }
    }
}

#[test]
fn rejects_mixed_okx_demo_and_production_websocket_routing() {
    let profile = BASE_PROFILE.replace(
        "base_url_ws_private = \"wss://wseeapap.okx.com:8443/ws/v5/private\"",
        "base_url_ws_private = \"wss://wseea.okx.com:8443/ws/v5/private\"",
    );
    let error = load(&profile).unwrap_err();

    assert!(
        error.to_string().contains("must not be mixed"),
        "mixed OKX demo and production WebSocket routing should fail validation: {error}"
    );
}

#[test]
fn rejects_known_okx_websocket_hosts_mismatching_trading_service() {
    let profile = BASE_PROFILE
        .replace(
            "trading_service = \"DEMO\"",
            "trading_service = \"PRODUCTION\"",
        )
        .replace(
            "order_intent = \"demo-okx-spot-confirmed\"",
            "order_intent = \"live-okx-spot-confirmed\"",
        );
    let error = load(&profile).unwrap_err();

    assert!(
        error.to_string().contains("must match okx.trading_service"),
        "known OKX demo WebSocket hosts should not validate for production service: {error}"
    );

    let profile = BASE_PROFILE
        .replace(
            "base_url_ws_public = \"wss://wseeapap.okx.com:8443/ws/v5/public\"",
            "base_url_ws_public = \"wss://wseea.okx.com:8443/ws/v5/public\"",
        )
        .replace(
            "base_url_ws_private = \"wss://wseeapap.okx.com:8443/ws/v5/private\"",
            "base_url_ws_private = \"wss://wseea.okx.com:8443/ws/v5/private\"",
        )
        .replace(
            "base_url_ws_business = \"wss://wseeapap.okx.com:8443/ws/v5/business\"",
            "base_url_ws_business = \"wss://wseea.okx.com:8443/ws/v5/business\"",
        );
    let error = load(&profile).unwrap_err();

    assert!(
        error.to_string().contains("must match okx.trading_service"),
        "known OKX production WebSocket hosts should not validate for demo service: {error}"
    );
}

#[test]
fn rejects_invalid_okx_proxy_url_values() {
    for (invalid, expected) in [
        ("wss://okx-proxy.example", "must use http or https"),
        ("socks5://okx-proxy.example", "must use http or https"),
        (
            "http://user:pass@okx-proxy.example",
            "must not include credentials",
        ),
        (
            " http://okx-proxy.example",
            "must not contain leading or trailing whitespace",
        ),
        ("not a url", "must be a valid URL"),
        ("", "must not be empty"),
    ] {
        let profile = BASE_PROFILE.replace(
            "request_timeout_ms = 60000",
            &format!("request_timeout_ms = 60000\nproxy_url = \"{invalid}\""),
        );
        let error = load(&profile).unwrap_err();

        assert!(
            error.to_string().contains(expected),
            "invalid OKX proxy URL {invalid:?} should fail with {expected:?}: {error}"
        );
    }
}

#[test]
fn masks_okx_identifiers_for_logs() {
    let config = load(BASE_PROFILE).expect("profile should load");

    assert_eq!(masked_okx_api_key(&config), "de****ey");
    assert_eq!(masked_okx_account_id(&config), "OK***********MO");
}

fn strategy_empty_profile(profile: &str) -> String {
    profile
        .replace("order_intent = \"demo-okx-spot-confirmed\"\n", "")
        .replace(
            r#"
[[strategies.instances]]
kind = "okx_ema_atr_maker_trend"
id = "okx-ema-atr-maker-btc-usdt"
instrument = "BTC-USDT"
inst_type = "SPOT"
td_mode = "cash"
bar = "1m"

[strategies.instances.params]
fast_ema_period = 2
slow_ema_period = 5
atr_period = 3
quantity = "0.001"
max_quote_notional = "500"
entry_offset_atr_multiple = "0.1"
min_entry_offset_bps = "1.0"
max_entry_offset_bps = "15.0"
take_profit_atr_multiple = "1.5"
stop_loss_atr_multiple = "1.0"
"#,
            "",
        )
}

fn profile_with_additional_strategy(id: &str, enabled: bool) -> String {
    format!("{BASE_PROFILE}{}", strategy_instance_block(id, enabled))
}

fn strategy_instance_block(id: &str, enabled: bool) -> String {
    format!(
        r#"
[[strategies.instances]]
kind = "okx_ema_atr_maker_trend"
id = "{id}"
enabled = {enabled}
instrument = "BTC-USDT"
inst_type = "SPOT"
td_mode = "cash"
bar = "1m"

[strategies.instances.params]
fast_ema_period = 2
slow_ema_period = 5
atr_period = 3
quantity = "0.001"
max_quote_notional = "500"
entry_offset_atr_multiple = "0.1"
min_entry_offset_bps = "1.0"
max_entry_offset_bps = "15.0"
take_profit_atr_multiple = "1.5"
stop_loss_atr_multiple = "1.0"
"#
    )
}

fn load(contents: &str) -> anyhow::Result<BotConfig> {
    load_config_from_str_with_secret_resolver(contents, test_secret_resolver)
}

fn load_checked_in_profile(path: &str) -> anyhow::Result<BotConfig> {
    load_config_path_with_secret_resolver(Path::new(path), test_secret_resolver)
}

fn test_secret_resolver(name: &str) -> Option<String> {
    match name {
        "OKX_API_KEY" => Some("demo-key".to_owned()),
        "OKX_API_SECRET" => Some("demo-secret".to_owned()),
        "OKX_API_PASSPHRASE" => Some("demo-passphrase".to_owned()),
        _ => None,
    }
}

fn write_secret_file(dir: &Path, name: &str, value: &str) -> String {
    let path = dir.join(name);
    fs::write(&path, value).expect("secret file should be writable");
    path.to_string_lossy().into_owned()
}
