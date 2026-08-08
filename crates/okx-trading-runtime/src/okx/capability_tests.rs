use std::{sync::Arc, time::Duration};

use super::{
    AccountLevelDiagnostic, AccountLevelDiagnosticSnapshot, RequestedCapability,
    ValidatedCapabilityGeneration,
};
use crate::{
    config::types::{
        RequestedInstrumentId, RequestedInstrumentType, RequestedTradeMode,
        RequestedTradingInstrument,
    },
    okx::{
        trading_instrument::ValidatedTradingInstrument,
        types::{OkxAccountConfig, OkxInstrument, OkxTradeFeeRate},
    },
};

#[test]
fn requested_capability_preserves_the_configured_tuple() {
    let requested = requested("ETH-USDT");
    let capability = RequestedCapability::from_trading_instrument(&requested)
        .expect("canonical configured tuple should validate");

    assert_eq!(capability.trading_instrument(), &requested);
    assert_eq!(
        capability.trading_instrument().instrument.as_str(),
        "ETH-USDT"
    );
    assert_eq!(
        capability.trading_instrument().inst_type,
        RequestedInstrumentType::Spot
    );
    assert_eq!(
        capability.trading_instrument().td_mode,
        RequestedTradeMode::Cash
    );
}

#[test]
fn every_documented_account_level_keeps_the_same_cash_spot_capability() {
    let requested = requested("ETH-USDT");
    for (level, expected_diagnostic) in [
        ("1", AccountLevelDiagnostic::One),
        ("2", AccountLevelDiagnostic::Two),
        ("3", AccountLevelDiagnostic::Three),
        ("4", AccountLevelDiagnostic::Four),
    ] {
        let generation = generation(&requested, &account_config(level))
            .unwrap_or_else(|error| panic!("acctLv {level} must not decide capability: {error}"));
        assert_eq!(generation.inst_id(), "ETH-USDT");
        assert_eq!(generation.inst_type().as_okx(), "SPOT");
        assert_eq!(generation.td_mode().as_okx(), "cash");
        assert_eq!(
            generation.account_level_diagnostic().value(),
            expected_diagnostic
        );
    }
}

#[test]
fn missing_malformed_and_undocumented_account_levels_fail_diagnostic_parsing() {
    for level in ["", "0", "5", "01", " 1 ", "unknown"] {
        let error = AccountLevelDiagnosticSnapshot::observe(&account_config(level))
            .expect_err("invalid acctLv evidence must fail closed");
        assert!(
            error
                .to_string()
                .contains("missing, malformed, or undocumented"),
            "acctLv {level:?} returned an unexpected error: {error}"
        );
    }
}

#[test]
fn stale_account_level_snapshot_cannot_construct_a_generation() {
    let requested = requested("ETH-USDT");
    let instrument = Arc::new(
        ValidatedTradingInstrument::from_test_instrument(instrument("ETH-USDT", "ETH", "USDT"))
            .expect("instrument fixture should validate"),
    );
    let diagnostic = AccountLevelDiagnosticSnapshot::stale_for_test(
        &account_config("2"),
        Duration::from_secs(31),
    )
    .expect("stale test snapshot should construct");
    let error = ValidatedCapabilityGeneration::cash_spot(
        RequestedCapability::from_trading_instrument(&requested).expect("requested capability"),
        instrument,
        diagnostic,
        fee(),
        Duration::from_secs(30),
    )
    .expect_err("stale diagnostic evidence must fail generation construction");
    assert!(error.to_string().contains("became stale"));
}

fn generation(
    requested: &RequestedTradingInstrument,
    account: &OkxAccountConfig,
) -> anyhow::Result<ValidatedCapabilityGeneration> {
    let instrument = Arc::new(ValidatedTradingInstrument::from_test_instrument(
        instrument(requested.instrument.as_str(), "ETH", "USDT"),
    )?);
    ValidatedCapabilityGeneration::cash_spot(
        RequestedCapability::from_trading_instrument(requested)?,
        instrument,
        AccountLevelDiagnosticSnapshot::observe(account)?,
        fee(),
        Duration::from_secs(30),
    )
}

fn requested(inst_id: &str) -> RequestedTradingInstrument {
    RequestedTradingInstrument {
        instrument: RequestedInstrumentId::new(inst_id.to_owned()).expect("instrument"),
        inst_type: RequestedInstrumentType::Spot,
        td_mode: RequestedTradeMode::Cash,
    }
}

fn account_config(account_level: &str) -> OkxAccountConfig {
    OkxAccountConfig {
        uid: "1".to_owned(),
        main_uid: "1".to_owned(),
        account_level: account_level.to_owned(),
        perm: "read_only,trade".to_owned(),
        auto_loan: false,
        enable_spot_borrow: false,
        spot_borrow_auto_repay: false,
        fee_type: "0".to_owned(),
        kyc_level: String::new(),
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
        max_limit_amount: String::new(),
        max_market_size: "100".to_owned(),
        max_market_amount: String::new(),
        max_trigger_size: "100".to_owned(),
        initial_price_limit_pct: "0.05".to_owned(),
        float_price_limit_pct: "0.03".to_owned(),
        maximum_price_limit_pct: "0.15".to_owned(),
    }
}

fn fee() -> OkxTradeFeeRate {
    OkxTradeFeeRate {
        inst_type: "SPOT".to_owned(),
        level: "Lv1".to_owned(),
        group_id: "12".to_owned(),
        maker: "-0.001".to_owned(),
        taker: "-0.002".to_owned(),
        ts: "1".to_owned(),
    }
}
