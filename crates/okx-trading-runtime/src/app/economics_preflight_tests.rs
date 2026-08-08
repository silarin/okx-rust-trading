use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, bail};
use pretty_assertions::assert_eq;
use rust_decimal::Decimal;
use tempfile::tempdir;

use super::*;
use crate::{
    config::loader::load_config_path_with_secret_resolver,
    okx::types::{OkxAccountConfig, OkxTradeFeeRate},
};

fn command(profile_selector: &str, output: PathBuf) -> EconomicsPreflightCommand {
    EconomicsPreflightCommand {
        profile_selector: profile_selector.to_owned(),
        output,
        rest_samples: MIN_REST_SAMPLES,
        websocket_samples: MIN_WEBSOCKET_SAMPLES,
        request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
        acknowledge_read_only_production: false,
    }
}

fn external_output(name: &str) -> (tempfile::TempDir, PathBuf) {
    let directory = tempdir().expect("external temporary directory");
    let path = directory.path().join(name);
    (directory, path)
}

fn load_profile(path: &str) -> BotConfig {
    let source = if path == "config/live.toml" {
        "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml"
    } else {
        path
    };
    let mut config = load_config_path_with_secret_resolver(Path::new(source), |name| match name {
        "OKX_API_KEY" => Some("preflight-test-key".to_owned()),
        "OKX_API_SECRET" => Some("preflight-test-secret".to_owned()),
        "OKX_API_PASSPHRASE" => Some("preflight-test-passphrase".to_owned()),
        _ => None,
    })
    .expect("test profile should load");
    if path == "config/live.toml" {
        config.runtime.order_intent = None;
        config.strategies.instances.clear();
        let okx = config.okx.as_mut().expect("test profile configures OKX");
        okx.trading_service = OkxTradingService::Production;
    }
    config
}

fn fee(raw_maker: &str, raw_taker: &str) -> OkxTradeFeeRate {
    OkxTradeFeeRate {
        inst_type: "SPOT".to_owned(),
        level: "Lv1".to_owned(),
        group_id: "12".to_owned(),
        maker: raw_maker.to_owned(),
        taker: raw_taker.to_owned(),
        ts: "1".to_owned(),
    }
}

fn instrument(inst_id: &str, base_ccy: &str, quote_ccy: &str) -> OkxInstrument {
    OkxInstrument {
        inst_type: "SPOT".to_owned(),
        inst_id: inst_id.to_owned(),
        group_id: "12".to_owned(),
        inst_id_code: Some(123_456),
        state: "live".to_owned(),
        base_ccy: base_ccy.to_owned(),
        quote_ccy: quote_ccy.to_owned(),
        trade_quote_currencies: vec![quote_ccy.to_owned()],
        tick_size: "0.01".to_owned(),
        lot_size: "0.0001".to_owned(),
        min_size: "0.0001".to_owned(),
        max_limit_size: "100".to_owned(),
        max_limit_amount: "100000".to_owned(),
        max_market_size: "100000".to_owned(),
        max_market_amount: "100000".to_owned(),
        max_trigger_size: "100".to_owned(),
        initial_price_limit_pct: "0.05".to_owned(),
        float_price_limit_pct: "0.03".to_owned(),
        maximum_price_limit_pct: "0.15".to_owned(),
    }
}

fn account_config() -> OkxAccountConfig {
    OkxAccountConfig {
        uid: "sensitive-uid-123".to_owned(),
        main_uid: "sensitive-main-uid-456".to_owned(),
        account_level: "1".to_owned(),
        perm: "read_only".to_owned(),
        auto_loan: false,
        enable_spot_borrow: false,
        spot_borrow_auto_repay: false,
        fee_type: "1".to_owned(),
        kyc_level: String::new(),
    }
}

fn distribution() -> LatencyDistribution {
    LatencyDistribution::from_samples(3, 0, vec![10, 20, 30]).expect("distribution")
}

fn artifact() -> EconomicsPreflightArtifact {
    EconomicsPreflightArtifact {
        schema: SCHEMA.to_owned(),
        generated_at_ms: 1_700_000_000_000,
        product: PRODUCT.to_owned(),
        package_version: "0.1.0".to_owned(),
        profile: ProfileArtifact {
            selector: "live".to_owned(),
            region: "EU".to_owned(),
            trading_service: "PRODUCTION".to_owned(),
            instrument_id: "BTC-USDT".to_owned(),
        },
        account_safety: AccountSafetyArtifact {
            spot_mode: true,
            borrowing_disabled: true,
            fee_type: "quote_currency".to_owned(),
        },
        fees: fee_artifact(&[fee("-0.0008", "-0.001")]).expect("fee artifact"),
        latency: LatencyArtifact {
            server_time_round_trip: distribution(),
            account_config_round_trip: distribution(),
            spot_fee_round_trip: distribution(),
            instrument_metadata_round_trip: distribution(),
            ticker_round_trip: distribution(),
            public_websocket_connect_and_subscribe: distribution(),
            private_websocket_login_and_subscribe: distribution(),
            trading_command_session_prepare: distribution(),
            trading_command_session_prepare_note: TRADING_SESSION_LATENCY_NOTE.to_owned(),
        },
        safety_assertions: SafetyAssertions::all_false(),
    }
}

#[test]
fn economics_preflight_cli_parses_strict_bounded_options() -> Result<()> {
    let parsed = EconomicsPreflightCommand::parse([
        "live".to_owned(),
        "--output".to_owned(),
        "/private/tmp/economics.json".to_owned(),
        "--rest-samples".to_owned(),
        "25".to_owned(),
        "--websocket-samples".to_owned(),
        "4".to_owned(),
        "--request-timeout-ms".to_owned(),
        "2500".to_owned(),
        "--acknowledge-read-only-production".to_owned(),
    ])?;

    assert_eq!(parsed.profile_selector, "live");
    assert_eq!(parsed.output, PathBuf::from("/private/tmp/economics.json"));
    assert_eq!(parsed.rest_samples, 25);
    assert_eq!(parsed.websocket_samples, 4);
    assert_eq!(parsed.request_timeout_ms, 2_500);
    assert!(parsed.acknowledge_read_only_production);
    Ok(())
}

#[test]
fn economics_preflight_cli_uses_documented_defaults() -> Result<()> {
    let parsed = EconomicsPreflightCommand::parse([
        "demo".to_owned(),
        "--output".to_owned(),
        "/private/tmp/economics.json".to_owned(),
    ])?;

    assert_eq!(parsed.rest_samples, DEFAULT_REST_SAMPLES);
    assert_eq!(parsed.websocket_samples, DEFAULT_WEBSOCKET_SAMPLES);
    assert_eq!(parsed.request_timeout_ms, DEFAULT_REQUEST_TIMEOUT_MS);
    assert!(!parsed.acknowledge_read_only_production);
    Ok(())
}

#[test]
fn economics_preflight_cli_rejects_unknown_duplicate_missing_and_relative_options() {
    for (args, expected) in [
        (
            vec!["live", "--unknown"],
            "unknown economics-preflight option",
        ),
        (
            vec![
                "live",
                "--output",
                "/private/tmp/a.json",
                "--output",
                "/private/tmp/b.json",
            ],
            "duplicate economics-preflight option --output",
        ),
        (vec!["live"], "requires --output"),
        (
            vec!["live", "--output", "relative.json"],
            "absolute external path",
        ),
        (vec!["live", "--output"], "option --output requires a value"),
    ] {
        let error = EconomicsPreflightCommand::parse(args.into_iter().map(str::to_owned))
            .expect_err("invalid CLI should fail");
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error:#}"
        );
    }
}

#[test]
fn economics_preflight_cli_enforces_sample_and_timeout_bounds() {
    for (option, value, expected) in [
        ("--rest-samples", "2", "between 3 and 100"),
        ("--rest-samples", "101", "between 3 and 100"),
        ("--websocket-samples", "0", "between 1 and 10"),
        ("--websocket-samples", "11", "between 1 and 10"),
        ("--request-timeout-ms", "99", "between 100 and 60000"),
        ("--request-timeout-ms", "60001", "between 100 and 60000"),
    ] {
        let error = EconomicsPreflightCommand::parse(
            [
                "demo",
                "--output",
                "/private/tmp/economics.json",
                option,
                value,
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect_err("out-of-range option should fail");
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error:#}"
        );
    }
}

#[test]
fn production_acknowledgement_is_required_before_client_construction() {
    let (_directory, output) = external_output("production.json");
    let mut production = command("live", output);
    let config = load_profile("config/live.toml");

    let error = validate_before_client_construction(&production, &config)
        .expect_err("Production without acknowledgement should fail");
    assert!(
        error
            .to_string()
            .contains("--acknowledge-read-only-production")
    );

    production.acknowledge_read_only_production = true;
    validate_before_client_construction(&production, &config)
        .expect("acknowledged Production read-only preflight should validate");
}

#[test]
fn demo_does_not_require_production_acknowledgement() {
    let (_directory, output) = external_output("demo.json");
    validate_before_client_construction(
        &command("demo", output),
        &load_profile("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml"),
    )
    .expect("Demo preflight should not require Production acknowledgement");
}

#[test]
fn economics_preflight_uses_the_exact_enabled_dto_instrument() {
    let (_directory, output) = external_output("demo-eth.json");
    let mut config =
        load_profile("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");
    config.instruments[0].instrument_id =
        crate::config::types::RequestedInstrumentId::new("ETH-USDT".to_owned())
            .expect("canonical ETH-USDT");
    config.instruments[0].base_currency = "ETH".to_owned();

    let validated = validate_before_client_construction(&command("demo", output), &config)
        .expect("canonical DTO-selected ETH-USDT should be accepted");

    assert_eq!(validated.profile.instrument_id, "ETH-USDT");
}

#[test]
fn economics_preflight_rejects_zero_or_multiple_enabled_instruments() {
    let (_directory, output) = external_output("demo-cardinality.json");
    let command = command("demo", output);
    let mut config =
        load_profile("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");
    config.instruments[0].enabled = false;
    let error = validate_before_client_construction(&command, &config)
        .expect_err("zero enabled instruments must fail");
    assert!(error.to_string().contains("exactly one enabled"));

    let mut config =
        load_profile("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");
    config.instruments.push(config.instruments[0].clone());
    let error = validate_before_client_construction(&command, &config)
        .expect_err("multiple enabled instruments must fail");
    assert!(error.to_string().contains("exactly one enabled"));
}

#[test]
fn fee_calculations_preserve_commission_and_rebate_signs_exactly() -> Result<()> {
    let commission = fee_artifact(&[fee("-0.0008", "-0.001")])?;
    assert_eq!(commission.normalized_maker_cost_rate, "0.0008");
    assert_eq!(commission.normalized_taker_cost_rate, "0.001");
    assert_eq!(commission.maker_semantics, "commission");
    assert_eq!(commission.maker_cost_bps, "8");
    assert_eq!(commission.taker_cost_bps, "10");
    assert_eq!(commission.maker_maker_round_trip_bps, "16");
    assert_eq!(commission.maker_taker_round_trip_bps, "18");
    assert_eq!(commission.taker_taker_round_trip_bps, "20");

    let rebate = fee_artifact(&[fee("0.0002", "-0.001")])?;
    assert_eq!(rebate.normalized_maker_cost_rate, "-0.0002");
    assert_eq!(rebate.maker_semantics, "rebate");
    assert_eq!(rebate.maker_cost_bps, "-2");
    assert_eq!(rebate.maker_maker_round_trip_bps, "-4");
    assert_eq!(rebate.maker_taker_round_trip_bps, "8");
    Ok(())
}

#[test]
fn fee_calculations_reject_malformed_decimal_and_unsupported_taker_rebate() {
    let malformed =
        fee_artifact(&[fee("not-decimal", "-0.001")]).expect_err("malformed maker should fail");
    assert!(malformed.to_string().contains("must be a decimal"));

    let taker_rebate = fee_artifact(&[fee("-0.0008", "0.0001")])
        .expect_err("taker rebate should fail conservatively");
    assert!(
        taker_rebate
            .to_string()
            .contains("taker fee sign is unsupported")
    );
}

#[test]
fn economics_preflight_rejects_fee_group_mismatch_or_change() {
    let mut mismatched = fee("-0.0008", "-0.001");
    mismatched.group_id = "13".to_owned();
    let mismatch = validate_fee(&mismatched, "BTC-USDT", "12")
        .expect_err("fee response must match the sampled instrument group");
    assert!(
        mismatch
            .to_string()
            .contains("does not match instrument groupId")
    );

    let changed = fee_artifact(&[fee("-0.0008", "-0.001"), mismatched])
        .expect_err("fee group must remain stable across samples");
    assert!(
        changed
            .to_string()
            .contains("fee rate changed during economics preflight")
    );
}

#[test]
fn economics_preflight_instrument_metadata_must_match_configured_identity_and_currencies() {
    let expected = PreflightInstrument {
        instrument_id: "ETH-USDT".to_owned(),
        base_currency: "ETH".to_owned(),
        quote_currency: "USDT".to_owned(),
    };
    validate_fee_group_instrument(&instrument("ETH-USDT", "ETH", "USDT"), &expected)
        .expect("exact configured/API identity should pass");

    let mismatch = validate_fee_group_instrument(&instrument("BTC-USDT", "BTC", "USDT"), &expected)
        .expect_err("another instrument must fail");
    assert!(mismatch.to_string().contains("exact SPOT ETH-USDT"));

    let mismatch = validate_fee_group_instrument(&instrument("ETH-USDT", "BTC", "USDT"), &expected)
        .expect_err("API currencies must agree with configured operator currencies");
    assert!(
        mismatch
            .to_string()
            .contains("contradict configured operator currencies")
    );

    let mut missing_trade_quote = instrument("ETH-USDT", "ETH", "USDT");
    missing_trade_quote.trade_quote_currencies.clear();
    let mismatch = validate_fee_group_instrument(&missing_trade_quote, &expected)
        .expect_err("missing trade quote evidence must fail");
    assert!(mismatch.to_string().contains("omitted tradeQuoteCcyList"));
}

#[test]
fn account_safety_excludes_identifiers_and_rejects_borrowing() -> Result<()> {
    let safety = consistent_account_safety(&[account_config()])?;
    assert_eq!(
        safety,
        AccountSafetyArtifact {
            spot_mode: true,
            borrowing_disabled: true,
            fee_type: "quote_currency".to_owned(),
        }
    );

    let mut borrowing = account_config();
    borrowing.enable_spot_borrow = true;
    let error = validate_account_config(&borrowing).expect_err("borrowing should fail");
    assert!(error.to_string().contains("borrowing"));
    Ok(())
}

#[test]
fn percentile_calculation_is_deterministic_and_empty_is_explicit() {
    let samples = (1..=100).rev().collect::<Vec<_>>();
    let result = LatencyDistribution::from_samples(101, 1, samples).expect("distribution");
    assert_eq!(result.minimum_microseconds, 1);
    assert_eq!(result.p50_microseconds, 50);
    assert_eq!(result.p95_microseconds, 95);
    assert_eq!(result.p99_microseconds, 99);
    assert_eq!(result.maximum_microseconds, 100);
    assert_eq!(result.successes, 100);
    assert_eq!(result.failures, 1);
    assert!(LatencyDistribution::from_samples(0, 0, Vec::new()).is_none());
}

#[tokio::test]
async fn latency_collection_reports_partial_failures_without_load_parallelism() -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let (distribution, values) = collect_samples(
        "partial",
        4,
        3,
        Duration::from_millis(50),
        Duration::ZERO,
        move || {
            let call = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if call == 1 {
                    bail!("synthetic failure")
                }
                Ok(call)
            }
        },
        |_| Ok(()),
    )
    .await?;

    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(values, [0, 2, 3]);
    assert_eq!(distribution.attempts, 4);
    assert_eq!(distribution.successes, 3);
    assert_eq!(distribution.failures, 1);
    Ok(())
}

#[tokio::test]
async fn latency_collection_rejects_insufficient_successes_and_timeouts() {
    let insufficient = collect_samples(
        "insufficient",
        3,
        3,
        Duration::from_millis(10),
        Duration::ZERO,
        || async { Err::<(), _>(anyhow::anyhow!("failure")) },
        |_| Ok(()),
    )
    .await
    .expect_err("all failures should reject the report");
    assert!(insufficient.to_string().contains("0 successful samples"));

    let timeout = collect_samples(
        "timeout",
        1,
        1,
        Duration::from_millis(1),
        Duration::ZERO,
        std::future::pending::<Result<()>>,
        |_| Ok(()),
    )
    .await
    .expect_err("timeout should reject the report");
    assert!(timeout.to_string().contains("0 successful samples"));
}

#[test]
fn latency_collection_uses_monotonic_instant_and_not_wall_clock() {
    let source = include_str!("economics_preflight.rs");
    let start = source.find("async fn collect_samples").expect("collector");
    let end = source[start..]
        .find("fn validate_account_config")
        .map(|offset| start + offset)
        .expect("collector end");
    let collector = &source[start..end];
    assert!(collector.contains("Instant::now()"));
    assert!(collector.contains("started_at.elapsed()"));
    assert!(!collector.contains("SystemTime"));
}

#[test]
fn artifact_serialization_is_deterministic_strict_and_sanitized() -> Result<()> {
    let artifact = artifact();
    let first = serde_json::to_string_pretty(&artifact)?;
    let second = serde_json::to_string_pretty(&artifact)?;
    assert_eq!(first, second);
    for forbidden in [
        "sensitive-uid-123",
        "sensitive-main-uid-456",
        "preflight-test-key",
        "preflight-test-secret",
        "preflight-test-passphrase",
        "apiKey",
        "apiSecret",
        "passphrase",
        "mainUid",
        "accountId",
        "permission",
    ] {
        assert!(!first.contains(forbidden), "artifact leaked {forbidden:?}");
    }
    assert!(first.contains(TRADING_SESSION_LATENCY_NOTE));
    let assertions = &artifact.safety_assertions;
    assert!(!assertions.strategies_constructed);
    assert!(!assertions.orders_submitted);
    assert!(!assertions.orders_amended);
    assert!(!assertions.orders_cancelled);
    assert!(!assertions.cancel_all_after_called);
    assert!(!assertions.balances_read);
    assert!(!assertions.positions_read);
    assert!(!assertions.order_history_read);

    let mut value = serde_json::to_value(&artifact)?;
    value
        .as_object_mut()
        .expect("artifact object")
        .insert("unknown".to_owned(), serde_json::Value::Bool(true));
    serde_json::from_value::<EconomicsPreflightArtifact>(value)
        .expect_err("unknown artifact field should fail");
    Ok(())
}

#[test]
fn output_is_external_create_new_atomic_and_restrictive() -> Result<()> {
    let (directory, output) = external_output("artifact.json");
    let target = ValidatedOutput::new(&output)?;
    target.write(&artifact())?;
    assert!(output.is_file());
    let contents = fs::read_to_string(&output)?;
    assert!(contents.ends_with('\n'));
    assert_eq!(
        serde_json::from_str::<EconomicsPreflightArtifact>(&contents)?,
        artifact()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(fs::metadata(&output)?.permissions().mode() & 0o777, 0o600);
    }
    let overwrite = ValidatedOutput::new(&output).expect_err("existing output should fail");
    assert!(overwrite.to_string().contains("already exists"));
    assert_eq!(
        fs::read_dir(directory.path())?.count(),
        1,
        "temporary artifact should be removed"
    );
    Ok(())
}

#[test]
fn output_rejects_repository_paths_and_cleans_temporary_after_finalize_race() -> Result<()> {
    let repository_output = repository_root().join("must-not-write.json");
    let error =
        ValidatedOutput::new(&repository_output).expect_err("repository output should be rejected");
    assert!(error.to_string().contains("outside the repository"));

    let (directory, output) = external_output("raced.json");
    let target = ValidatedOutput::new(&output)?;
    fs::write(&output, "operator-owned")?;
    target
        .write(&artifact())
        .expect_err("final create-new race should fail");
    assert_eq!(fs::read_to_string(&output)?, "operator-owned");
    let names = fs::read_dir(directory.path())?
        .map(|entry| entry.expect("directory entry").file_name())
        .collect::<Vec<_>>();
    assert_eq!(names, [std::ffi::OsString::from("raced.json")]);
    Ok(())
}

#[test]
fn preflight_source_has_no_runtime_strategy_or_mutation_reachability() {
    let app = include_str!("economics_preflight.rs");
    for forbidden in [
        "app::live",
        "build_trading_engine",
        "build_strategies",
        "preflight_strategy_enabled_account",
        "crate::strategies",
        ".place_order(",
        ".amend_order(",
        ".cancel_order(",
        ".cancel_all_after(",
        ".balances(",
        ".positions(",
        ".open_orders(",
        ".order_history(",
        "reconciliation",
    ] {
        assert!(
            !app.contains(forbidden),
            "preflight app reached {forbidden:?}"
        );
    }

    let client = include_str!("../okx/economics_preflight.rs");
    for forbidden in [
        ".place_order(",
        ".amend_order(",
        ".cancel_order(",
        ".cancel_all_after(",
        ".balances(",
        ".open_orders(",
        ".order_history(",
        ".fills(",
    ] {
        assert!(
            !client.contains(forbidden),
            "narrow client reached {forbidden:?}"
        );
    }
}

#[test]
fn exact_decimal_to_basis_points_uses_no_binary_float() -> Result<()> {
    let calculated = fee_artifact(&[fee("0.0000123456789", "-0.0009876543211")])?;
    assert_eq!(calculated.maker_cost_bps, "-0.123456789");
    assert_eq!(calculated.taker_cost_bps, "9.876543211");
    assert_eq!(calculated.maker_taker_round_trip_bps, "9.753086422");
    assert_eq!(
        Decimal::from_str_exact("9.753086422")?.to_string(),
        "9.753086422"
    );
    Ok(())
}
