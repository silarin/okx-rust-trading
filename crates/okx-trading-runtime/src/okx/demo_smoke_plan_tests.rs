use pretty_assertions::assert_eq;

use super::*;
use crate::config::types::{RequestedInstrumentType, RequestedTradeMode};

const FILE_CREDENTIALS: [(&str, &str); 3] = [
    ("OKX_API_KEY_FILE", "/tmp/key"),
    ("OKX_API_SECRET_FILE", "/tmp/secret"),
    ("OKX_API_PASSPHRASE_FILE", "/tmp/passphrase"),
];

fn no_mutation_reason() -> String {
    "none of OKX_DEMO_SMOKE_ORDER, OKX_DEMO_SMOKE_WEBSOCKET_ORDER, OKX_DEMO_SMOKE_WEBSOCKET_AMEND, OKX_DEMO_SMOKE_CAA_EXPIRY, OKX_DEMO_SMOKE_POST_ONLY_CROSS, OKX_DEMO_SMOKE_WEBSOCKET_EXPIRED, OKX_DEMO_SMOKE_FILL_LIFECYCLE, OKX_DEMO_SMOKE_SPOT_OCO, or OKX_DEMO_SMOKE_ACQUISITION_PROBE is set to 1"
        .to_owned()
}

fn missing_credentials() -> CheckPlan {
    CheckPlan::Skip(
        "missing demo credential environment values: OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE"
            .to_owned(),
    )
}

fn separate_caa() -> CheckPlan {
    CheckPlan::Skip(
        "OKX_DEMO_SMOKE_CAA is not set to 1; this check mutates OKX demo account dead-man-switch state"
            .to_owned(),
    )
}

fn separate_soak() -> CheckPlan {
    CheckPlan::Skip("OKX_DEMO_SMOKE_PRIVATE_SOAK is not set to 1".to_owned())
}

#[test]
fn skips_everything_without_enable_gate() {
    let env = SmokeEnvironment::default();

    assert_eq!(
        SmokePlan::from_environment(&env),
        SmokePlan {
            enabled: false,
            private_checks: CheckPlan::Skip("OKX_DEMO_SMOKE is not set to 1".to_owned()),
            private_soak: CheckPlan::Skip("OKX_DEMO_SMOKE is not set to 1".to_owned()),
            cancel_all_after: CheckPlan::Skip("OKX_DEMO_SMOKE is not set to 1".to_owned()),
            order_mutation: OrderMutationPlan::Skip("OKX_DEMO_SMOKE is not set to 1".to_owned()),
        }
    );
}

#[test]
fn runs_public_and_skips_private_without_credentials() {
    let env = SmokeEnvironment::from_pairs(&[(SMOKE_ENABLED_ENV, "1")]);

    assert_eq!(
        SmokePlan::from_environment(&env),
        SmokePlan {
            enabled: true,
            private_checks: missing_credentials(),
            private_soak: separate_soak(),
            cancel_all_after: separate_caa(),
            order_mutation: OrderMutationPlan::Skip(no_mutation_reason()),
        }
    );
}

#[test]
fn runs_private_with_credentials_and_keeps_mutations_separate() {
    let env = SmokeEnvironment::from_pairs(&[
        (SMOKE_ENABLED_ENV, "1"),
        ("OKX_API_KEY", "key"),
        ("OKX_API_SECRET", "secret"),
        ("OKX_API_PASSPHRASE", "passphrase"),
    ]);

    assert_eq!(
        SmokePlan::from_environment(&env),
        SmokePlan {
            enabled: true,
            private_checks: CheckPlan::Run,
            private_soak: separate_soak(),
            cancel_all_after: separate_caa(),
            order_mutation: OrderMutationPlan::Skip(no_mutation_reason()),
        }
    );
}

#[test]
fn refuses_each_order_mutation_without_credentials() {
    for kind in OrderMutationKind::ALL {
        let env = SmokeEnvironment::from_pairs(&[(SMOKE_ENABLED_ENV, "1"), (kind.gate(), "1")]);

        assert_eq!(
            SmokePlan::from_environment(&env),
            SmokePlan {
                enabled: true,
                private_checks: missing_credentials(),
                private_soak: separate_soak(),
                cancel_all_after: separate_caa(),
                order_mutation: OrderMutationPlan::Skip(format!(
                    "{}=1 was set, but order mutation requires demo credentials: OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE",
                    kind.gate()
                )),
            }
        );
    }
}

#[test]
fn runs_caa_only_with_extra_gate_and_credentials() {
    let mut pairs = vec![(SMOKE_ENABLED_ENV, "1"), (SMOKE_CAA_ENV, "1")];
    pairs.extend(FILE_CREDENTIALS);
    let env = SmokeEnvironment::from_pairs(&pairs);

    assert_eq!(
        SmokePlan::from_environment(&env),
        SmokePlan {
            enabled: true,
            private_checks: CheckPlan::Run,
            private_soak: separate_soak(),
            cancel_all_after: CheckPlan::Run,
            order_mutation: OrderMutationPlan::Skip(no_mutation_reason()),
        }
    );
}

#[test]
fn runs_each_order_mutation_with_its_own_caa_lifecycle() {
    for kind in OrderMutationKind::ALL {
        let mut pairs = vec![
            (SMOKE_ENABLED_ENV, "1"),
            (SMOKE_CAA_ENV, "1"),
            (kind.gate(), "1"),
        ];
        pairs.extend(FILE_CREDENTIALS);
        let env = SmokeEnvironment::from_pairs(&pairs);

        assert_eq!(
            SmokePlan::from_environment(&env),
            SmokePlan {
                enabled: true,
                private_checks: CheckPlan::Run,
                private_soak: separate_soak(),
                cancel_all_after: CheckPlan::Skip(format!(
                    "{}=1 includes its own Cancel-All-After lifecycle",
                    kind.gate()
                )),
                order_mutation: OrderMutationPlan::Run(kind),
            }
        );
    }
}

#[test]
fn refuses_multiple_order_mutations() {
    let mut pairs = vec![
        (SMOKE_ENABLED_ENV, "1"),
        (SMOKE_ORDER_ENV, "1"),
        (SMOKE_WEBSOCKET_ORDER_ENV, "1"),
        (SMOKE_WEBSOCKET_AMEND_ENV, "1"),
        (SMOKE_CAA_EXPIRY_ENV, "1"),
        (SMOKE_POST_ONLY_CROSS_ENV, "1"),
        (SMOKE_WEBSOCKET_EXPIRED_ENV, "1"),
        (SMOKE_FILL_LIFECYCLE_ENV, "1"),
        (SMOKE_SPOT_OCO_ENV, "1"),
        (SMOKE_ACQUISITION_PROBE_ENV, "1"),
    ];
    pairs.extend(FILE_CREDENTIALS);
    let env = SmokeEnvironment::from_pairs(&pairs);

    assert_eq!(
        SmokePlan::from_environment(&env),
        SmokePlan {
            enabled: true,
            private_checks: CheckPlan::Run,
            private_soak: separate_soak(),
            cancel_all_after: separate_caa(),
            order_mutation: OrderMutationPlan::Skip(
                "multiple OKX Demo order-mutation gates are set: OKX_DEMO_SMOKE_ORDER, OKX_DEMO_SMOKE_WEBSOCKET_ORDER, OKX_DEMO_SMOKE_WEBSOCKET_AMEND, OKX_DEMO_SMOKE_CAA_EXPIRY, OKX_DEMO_SMOKE_POST_ONLY_CROSS, OKX_DEMO_SMOKE_WEBSOCKET_EXPIRED, OKX_DEMO_SMOKE_FILL_LIFECYCLE, OKX_DEMO_SMOKE_SPOT_OCO, OKX_DEMO_SMOKE_ACQUISITION_PROBE; choose exactly one"
                    .to_owned()
            ),
        }
    );
}

#[test]
fn runs_private_soak_only_with_extra_gate_and_credentials() {
    let mut pairs = vec![(SMOKE_ENABLED_ENV, "1"), (SMOKE_PRIVATE_SOAK_ENV, "1")];
    pairs.extend(FILE_CREDENTIALS);
    let env = SmokeEnvironment::from_pairs(&pairs);

    assert_eq!(
        SmokePlan::from_environment(&env),
        SmokePlan {
            enabled: true,
            private_checks: CheckPlan::Run,
            private_soak: CheckPlan::Run,
            cancel_all_after: separate_caa(),
            order_mutation: OrderMutationPlan::Skip(no_mutation_reason()),
        }
    );
}

#[test]
fn refuses_production_trading_service_before_any_demo_mutation() {
    assert!(ensure_demo_trading_service(OkxTradingService::Demo).is_ok());
    let error = ensure_demo_trading_service(OkxTradingService::Production)
        .expect_err("Production routing must be rejected by the Demo harness");
    assert!(error.to_string().contains("trading_service = DEMO"));
}

#[test]
fn functional_tuple_is_strict_eth_usdt_and_has_no_strategy_qualification() -> Result<()> {
    let (config, requested) = load_demo_functional_profile_for_smoke(&SmokeEnvironment::default())?;
    assert!(config.strategies.instances.is_empty());

    assert_eq!(requested.instrument.as_str(), "ETH-USDT");
    assert_eq!(requested.inst_type, RequestedInstrumentType::Spot);
    assert_eq!(requested.td_mode, RequestedTradeMode::Cash);
    assert_eq!(config.instruments[0].okx_instrument_id(), "ETH-USDT");
    Ok(())
}

#[test]
fn functional_tuple_dto_rejects_missing_and_unknown_fields() {
    let missing = DEMO_FUNCTIONAL_PROFILE.replace("td_mode = \"cash\"\n", "");
    let unknown = format!("{DEMO_FUNCTIONAL_PROFILE}fallback = \"SOL-USDT\"\n");

    assert!(toml::from_str::<DemoFunctionalProfileDto>(&missing).is_err());
    assert!(toml::from_str::<DemoFunctionalProfileDto>(&unknown).is_err());
}

#[test]
fn functional_profile_rejects_operator_and_tuple_identity_disagreement() {
    let mismatch = DEMO_FUNCTIONAL_PROFILE.replace(
        "[trading_tuple]\ninstrument = \"ETH-USDT\"",
        "[trading_tuple]\ninstrument = \"BTC-USDT\"",
    );
    let error = load_demo_functional_profile_from_str(&mismatch, &SmokeEnvironment::default())
        .expect_err("functional operator and trading-tuple identities must agree");
    assert!(
        error
            .to_string()
            .contains("operator instrument matching its trading_tuple")
    );
}

#[test]
fn functional_profile_requires_matching_eth_or_btc_identity_dtos() -> Result<()> {
    for instrument_id in ["ETH-USDT", "BTC-USDT"] {
        let contents = DEMO_FUNCTIONAL_PROFILE
            .replace("ETH-USDT", instrument_id)
            .replace(
                "base_currency = \"ETH\"",
                &format!(
                    "base_currency = \"{}\"",
                    instrument_id
                        .strip_suffix("-USDT")
                        .expect("test instrument uses USDT quote")
                ),
            );
        let (config, requested) =
            load_demo_functional_profile_from_str(&contents, &SmokeEnvironment::default())?;

        assert_eq!(requested.instrument.as_str(), instrument_id);
        assert_eq!(config.instruments[0].okx_instrument_id(), instrument_id);
        assert!(config.strategies.instances.is_empty());
    }
    Ok(())
}
