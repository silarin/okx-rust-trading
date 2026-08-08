use pretty_assertions::assert_eq;
use rust_decimal::Decimal;
use serde_json::json;

use super::{
    MarketBar, OkxAccountConfig, OkxBalance, OkxBalanceDetail, OkxFill, OkxInstrument, OkxOrder,
    OkxSpotFeeType, OkxSpotFillAccounting, OkxTicker, OkxTradeFeeRate,
};

#[test]
fn market_bar_deserialization_rejects_short_okx_payload() {
    let error = serde_json::from_value::<MarketBar>(json!(["1000", "100"]))
        .expect_err("short OKX candle payload should fail");

    assert!(
        error
            .to_string()
            .contains("OKX candle payload must contain at least 9 fields"),
        "short candle payload should report missing OKX fields: {error}"
    );
}

#[test]
fn market_bar_deserialization_reports_invalid_numeric_fields() {
    let error = serde_json::from_value::<MarketBar>(json!([
        "not-a-timestamp",
        "100",
        "105",
        "95",
        "100",
        "1",
        "1",
        "1",
        "1"
    ]))
    .expect_err("invalid OKX candle timestamp should fail");

    assert!(
        error.to_string().contains("invalid OKX candle ts field"),
        "invalid candle timestamp should report the malformed OKX field: {error}"
    );
}

#[test]
fn market_bar_deserialization_accepts_valid_confirm_flags() {
    let confirmed = serde_json::from_value::<MarketBar>(market_bar_payload(
        "1000", "100", "105", "95", "101", "1",
    ))
    .expect("confirmed candle should parse");
    let unconfirmed = serde_json::from_value::<MarketBar>(market_bar_payload(
        "2000", "100", "105", "95", "101", "0",
    ))
    .expect("unconfirmed candle should parse");

    assert_eq!(
        confirmed,
        MarketBar {
            ts_ms: 1_000,
            open: 100.0,
            high: 105.0,
            low: 95.0,
            close: 101.0,
            confirm: true,
        }
    );
    assert_eq!(
        unconfirmed,
        MarketBar {
            ts_ms: 2_000,
            open: 100.0,
            high: 105.0,
            low: 95.0,
            close: 101.0,
            confirm: false,
        }
    );
}

#[test]
fn market_bar_deserialization_rejects_invalid_boundary_values() {
    for (payload, expected) in [
        (
            market_bar_payload("1000", "NaN", "105", "95", "101", "1"),
            "OKX candle open must be finite and positive",
        ),
        (
            market_bar_payload("1000", "100", "inf", "95", "101", "1"),
            "OKX candle high must be finite and positive",
        ),
        (
            market_bar_payload("1000", "100", "105", "-inf", "101", "1"),
            "OKX candle low must be finite and positive",
        ),
        (
            market_bar_payload("1000", "0", "105", "95", "101", "1"),
            "OKX candle open must be finite and positive",
        ),
        (
            market_bar_payload("1000", "100", "0", "95", "101", "1"),
            "OKX candle high must be finite and positive",
        ),
        (
            market_bar_payload("1000", "100", "105", "0", "101", "1"),
            "OKX candle low must be finite and positive",
        ),
        (
            market_bar_payload("1000", "100", "105", "95", "0", "1"),
            "OKX candle close must be finite and positive",
        ),
        (
            market_bar_payload("1000", "-1", "105", "95", "101", "1"),
            "OKX candle open must be finite and positive",
        ),
        (
            market_bar_payload("1000", "100", "-1", "95", "101", "1"),
            "OKX candle high must be finite and positive",
        ),
        (
            market_bar_payload("1000", "100", "105", "-1", "101", "1"),
            "OKX candle low must be finite and positive",
        ),
        (
            market_bar_payload("1000", "100", "105", "95", "-1", "1"),
            "OKX candle close must be finite and positive",
        ),
        (
            market_bar_payload("1000", "-0", "105", "95", "101", "1"),
            "OKX candle open must be finite and positive",
        ),
        (
            market_bar_payload("1000", "100", "99", "95", "98", "1"),
            "OKX candle high must be at least open",
        ),
        (
            market_bar_payload("1000", "99", "100", "95", "101", "1"),
            "OKX candle high must be at least close",
        ),
        (
            market_bar_payload("1000", "93", "94", "95", "92", "1"),
            "OKX candle high must be at least low",
        ),
        (
            market_bar_payload("1000", "100", "105", "103", "104", "1"),
            "OKX candle low must be at most open",
        ),
        (
            market_bar_payload("1000", "104", "105", "103", "102", "1"),
            "OKX candle low must be at most close",
        ),
        (
            market_bar_payload("1000", "103", "104", "105", "102", "1"),
            "OKX candle high must be at least low",
        ),
        (
            market_bar_payload("0", "100", "105", "95", "101", "1"),
            "OKX candle ts must be positive",
        ),
        (
            market_bar_payload("-1", "100", "105", "95", "101", "1"),
            "OKX candle ts must be positive",
        ),
        (
            market_bar_payload("1000", "100", "105", "95", "101", "2"),
            "OKX candle confirm flag must be 0 or 1",
        ),
    ] {
        let error = serde_json::from_value::<MarketBar>(payload)
            .expect_err("invalid OKX candle should fail validation");

        assert!(
            error.to_string().contains(expected),
            "invalid candle should report {expected:?}: {error}"
        );
    }
}

#[test]
fn ticker_decimal_accessors_preserve_okx_price_strings() {
    let ticker = ticker_with_ask(
        "100.123456789123456789",
        "100.223456789123456789",
        "101.000000000000000001",
    );

    assert_eq!(
        ticker.bid_decimal().expect("bidPx should parse as Decimal"),
        "100.123456789123456789"
            .parse()
            .expect("decimal literal should parse")
    );
    assert_eq!(
        ticker.ask_decimal().expect("askPx should parse as Decimal"),
        "100.223456789123456789"
            .parse()
            .expect("decimal literal should parse")
    );
    assert_eq!(
        ticker.last_decimal().expect("last should parse as Decimal"),
        "101.000000000000000001"
            .parse()
            .expect("decimal literal should parse")
    );
}

fn market_bar_payload(
    ts_ms: &str,
    open: &str,
    high: &str,
    low: &str,
    close: &str,
    confirm: &str,
) -> serde_json::Value {
    json!([ts_ms, open, high, low, close, "1", "1", "1", confirm])
}

#[test]
fn ticker_price_accessors_reject_non_positive_or_malformed_values() {
    let zero_bid = ticker("0", "100");
    let bid_error = zero_bid
        .bid()
        .expect_err("zero OKX ticker bid should fail closed");
    assert!(
        bid_error
            .to_string()
            .contains("OKX ticker bidPx must be positive"),
        "zero bid should report the malformed OKX field: {bid_error}"
    );

    let negative_ask = ticker_with_ask("100", "-1", "100");
    let ask_error = negative_ask
        .validate_prices()
        .expect_err("negative OKX ticker ask should fail closed");
    assert!(
        ask_error
            .to_string()
            .contains("OKX ticker askPx must be positive"),
        "negative ask should report the malformed OKX field: {ask_error}"
    );

    let non_finite_last = ticker("100", "NaN");
    let last_error = non_finite_last
        .last()
        .expect_err("malformed OKX ticker last should fail closed");
    assert!(
        last_error
            .to_string()
            .contains("OKX ticker last must be a decimal"),
        "malformed last should report the OKX field: {last_error}"
    );
}

#[test]
fn instrument_limit_helpers_reject_oversized_orders() {
    let instrument = instrument();
    let oversized_limit = "1.1".parse().expect("decimal literal should parse");
    let limit_error = instrument
        .ensure_limit_size(oversized_limit, "test limit size")
        .expect_err("limit order larger than OKX maxLmtSz should fail");

    assert!(
        limit_error
            .to_string()
            .contains("test limit size 1.1 exceeds OKX maxLmtSz 1"),
        "oversized limit order should report maxLmtSz: {limit_error}"
    );

    let mut usd_instrument = instrument.clone();
    usd_instrument.inst_id = "BTC-USD".to_owned();
    usd_instrument.quote_ccy = "USD".to_owned();
    usd_instrument.trade_quote_currencies = vec!["USD".to_owned()];
    let oversized_limit_amount = "100000.1".parse().expect("decimal literal should parse");
    let limit_amount_error = usd_instrument
        .ensure_limit_quote_amount(oversized_limit_amount, "test limit quote amount")
        .expect_err("limit order notional larger than OKX maxLmtAmt should fail");

    assert!(
        limit_amount_error
            .to_string()
            .contains("test limit quote amount 100000.1 exceeds OKX maxLmtAmt 100000"),
        "oversized limit order amount should report maxLmtAmt: {limit_amount_error}"
    );

    let oversized_trigger = "2.1".parse().expect("decimal literal should parse");
    let trigger_error = instrument
        .ensure_trigger_size(oversized_trigger, "test trigger size")
        .expect_err("trigger order larger than OKX maxTriggerSz should fail");

    assert!(
        trigger_error
            .to_string()
            .contains("test trigger size 2.1 exceeds OKX maxTriggerSz 2"),
        "oversized trigger order should report maxTriggerSz: {trigger_error}"
    );

    let oversized_market_buy_amount = "100000.1".parse().expect("decimal literal should parse");
    let market_buy_error = usd_instrument
        .ensure_market_buy_quote_amount(oversized_market_buy_amount, "test market buy quote amount")
        .expect_err("market buy quote amount larger than OKX maxMktAmt should fail");

    assert!(
        market_buy_error
            .to_string()
            .contains("test market buy quote amount 100000.1 exceeds OKX maxMktAmt 100000"),
        "oversized market buy quote amount should report maxMktAmt: {market_buy_error}"
    );
}

#[test]
fn usd_denominated_limit_helpers_reject_non_usd_without_conversion_evidence() {
    let instrument = instrument();
    let amount = Decimal::ONE;

    let limit_error = instrument
        .ensure_limit_quote_amount(amount, "test limit quote amount")
        .expect_err("USDT must not be compared directly with USD maxLmtAmt");
    assert!(
        limit_error
            .to_string()
            .contains("maxLmtAmt is USD-denominated"),
        "missing limit conversion evidence should fail closed: {limit_error}"
    );

    let market_error = instrument
        .ensure_market_buy_quote_amount(amount, "test market buy quote amount")
        .expect_err("USDT must not be compared directly with USD maxMktAmt");
    assert!(
        market_error
            .to_string()
            .contains("maxMktAmt is USD-denominated"),
        "missing market conversion evidence should fail closed: {market_error}"
    );
}

#[test]
fn numeric_boundary_spot_market_sell_size_uses_usdt_notional_for_max_market_size() {
    let mut instrument = instrument();
    instrument.max_market_size = "100".to_owned();
    let price = Decimal::new(100, 0);

    instrument
        .ensure_spot_market_sell_size(Decimal::new(9999, 4), price, "test market sell size")
        .expect("USDT notional below maxMktSz should pass");
    instrument
        .ensure_spot_market_sell_size(Decimal::ONE, price, "test market sell size")
        .expect("USDT notional exactly at maxMktSz should pass");
    let error = instrument
        .ensure_spot_market_sell_size(Decimal::new(10_001, 4), price, "test market sell size")
        .expect_err("USDT notional above maxMktSz should fail");

    assert!(
        error
            .to_string()
            .contains("test market sell size USDT notional 100.0100 exceeds OKX maxMktSz 100"),
        "maxMktSz rejection should compare exact USDT notional: {error}"
    );
}

#[test]
fn numeric_boundary_spot_market_sell_size_fails_closed_without_usdt_conversion_or_positive_price() {
    let mut non_usdt = instrument();
    non_usdt.inst_id = "ETH-BTC".to_owned();
    non_usdt.base_ccy = "ETH".to_owned();
    non_usdt.quote_ccy = "BTC".to_owned();
    non_usdt.trade_quote_currencies = vec!["BTC".to_owned()];
    non_usdt.max_market_size = "100".to_owned();
    let currency_error = non_usdt
        .ensure_spot_market_sell_size(Decimal::ONE, Decimal::ONE, "test market sell size")
        .expect_err("non-USDT quote must not be compared directly with maxMktSz");
    assert!(
        currency_error
            .to_string()
            .contains("maxMktSz is USDT-denominated but quoteCcy BTC cannot be converted"),
        "missing USDT conversion authority should fail closed: {currency_error}"
    );

    let price_error = instrument()
        .ensure_spot_market_sell_size(Decimal::ONE, Decimal::ZERO, "test market sell size")
        .expect_err("zero reference price must fail before market order construction");
    assert!(
        price_error
            .to_string()
            .contains("reference price must be positive"),
        "invalid reference price should report the numeric boundary: {price_error}"
    );

    let size_error = instrument()
        .ensure_spot_market_sell_size(-Decimal::ONE, Decimal::ONE, "test market sell size")
        .expect_err("negative base size must fail before market order construction");
    assert!(
        size_error.to_string().contains("must be positive"),
        "invalid base size should report the numeric boundary: {size_error}"
    );

    let mut overflow = instrument();
    overflow.max_market_size = Decimal::MAX.to_string();
    let overflow_error = overflow
        .ensure_spot_market_sell_size(Decimal::MAX, Decimal::MAX, "test market sell size")
        .expect_err("unrepresentable USDT notional must fail closed");
    assert!(
        overflow_error
            .to_string()
            .contains("USDT notional overflowed Decimal"),
        "Decimal overflow should fail before market order construction: {overflow_error}"
    );
}

#[test]
fn instrument_trade_quote_currency_helper_requires_supported_quote() {
    let mut instrument = instrument();
    instrument
        .ensure_trade_quote_currency("USDT")
        .expect("listed trade quote currency should validate");

    instrument.trade_quote_currencies.clear();
    let omitted_error = instrument
        .ensure_trade_quote_currency("USDT")
        .expect_err("missing tradeQuoteCcyList should fail closed");
    assert!(
        omitted_error
            .to_string()
            .contains("OKX instrument BTC-USDT omitted tradeQuoteCcyList"),
        "omitted tradeQuoteCcyList should be reported: {omitted_error}"
    );

    instrument.trade_quote_currencies = vec!["USDC".to_owned()];
    let unsupported_error = instrument
        .ensure_trade_quote_currency("USDT")
        .expect_err("unsupported trade quote currency should fail closed");
    assert!(
        unsupported_error
            .to_string()
            .contains("tradeQuoteCcyList [\"USDC\"] does not include USDT"),
        "unsupported tradeQuoteCcyList should be reported: {unsupported_error}"
    );
}

#[test]
fn instrument_websocket_inst_id_code_parses_optional_code() {
    let mut instrument = instrument();
    assert_eq!(
        instrument
            .websocket_inst_id_code()
            .expect("instIdCode should parse"),
        Some(123_456)
    );

    instrument.inst_id_code = None;
    assert_eq!(
        instrument
            .websocket_inst_id_code()
            .expect("missing instIdCode should remain optional"),
        None
    );
}

#[test]
fn instrument_fee_group_id_requires_a_positive_ascii_decimal_identifier() {
    let mut instrument = instrument();
    instrument.group_id = "17".to_owned();
    assert_eq!(instrument.fee_group_id().expect("documented groupId"), "17");

    for invalid in ["", " 12", "12 ", "group-12", "０１２", "0"] {
        instrument.group_id = invalid.to_owned();
        instrument
            .fee_group_id()
            .expect_err("malformed groupId must fail closed");
    }
}

#[test]
fn instrument_deserializes_documented_inst_id_code_shapes() {
    for (payload, expected_code) in [
        (
            json!({"instType":"SPOT","instId":"BTC-USDT","instIdCode":123456,"groupId":"12","state":"live","baseCcy":"BTC","quoteCcy":"USDT","tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","initPxLmtPct":"","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}),
            Some(123_456),
        ),
        (
            json!({"instType":"SPOT","instId":"BTC-USDT","instIdCode":"123456","groupId":"12","state":"live","baseCcy":"BTC","quoteCcy":"USDT","tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","initPxLmtPct":"","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}),
            Some(123_456),
        ),
        (
            json!({"instType":"SPOT","instId":"BTC-USDT","instIdCode":null,"groupId":"12","state":"live","baseCcy":"BTC","quoteCcy":"USDT","tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","initPxLmtPct":"","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}),
            None,
        ),
        (
            json!({"instType":"SPOT","instId":"BTC-USDT","groupId":"12","state":"live","baseCcy":"BTC","quoteCcy":"USDT","tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","initPxLmtPct":"","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}),
            None,
        ),
    ] {
        let instrument = serde_json::from_value::<OkxInstrument>(payload)
            .expect("documented instIdCode shape should deserialize");

        assert_eq!(instrument.inst_id_code, expected_code);
    }
}

#[test]
fn instrument_requires_price_limit_fields_but_allows_an_inactive_initial_band() {
    let mut payload = json!({
        "instType":"SPOT",
        "instId":"BTC-USDT",
        "groupId":"12",
        "state":"live",
        "baseCcy":"BTC",
        "quoteCcy":"USDT",
        "tickSz":"0.1",
        "lotSz":"0.0001",
        "minSz":"0.0001",
        "initPxLmtPct":"",
        "floatPxLmtPct":"0.03",
        "maxPxLmtPct":"0.15"
    });
    let parsed = serde_json::from_value::<OkxInstrument>(payload.clone())
        .expect("a present empty initial listing band should deserialize");
    assert_eq!(
        parsed
            .price_limit_percentages()
            .expect("price-limit percentages"),
        (None, Decimal::new(3, 2), Decimal::new(15, 2))
    );

    payload
        .as_object_mut()
        .expect("fixture object")
        .remove("initPxLmtPct");
    serde_json::from_value::<OkxInstrument>(payload)
        .expect_err("a missing initial listing-band field must fail closed");
}

#[test]
fn instrument_websocket_inst_id_code_rejects_invalid_code() {
    let mut instrument = instrument();
    instrument.inst_id_code = Some(0);
    let positive_error = instrument
        .websocket_inst_id_code()
        .expect_err("zero instIdCode should fail closed");
    assert!(
        positive_error
            .to_string()
            .contains("OKX instrument BTC-USDT instIdCode must be positive"),
        "zero instIdCode should be reported: {positive_error}"
    );
}

#[test]
fn mmp_canceled_is_treated_as_terminal_without_fill() {
    let order = order("mmp_canceled");

    assert!(order.is_terminal());
    assert!(order.is_terminal_without_fill());
}

#[test]
fn regular_order_state_validation_accepts_documented_states() {
    for state in [
        "live",
        "partially_filled",
        "filled",
        "canceled",
        "mmp_canceled",
    ] {
        let order = order(state);

        order
            .ensure_documented_state("order state test")
            .expect("documented OKX regular order state should validate");
    }
}

#[test]
fn regular_order_state_validation_rejects_unknown_states() {
    let order = order("pending_cancel");
    let error = order
        .ensure_documented_state("order state test")
        .expect_err("undocumented OKX regular order state should fail closed");

    assert!(
        error
            .to_string()
            .contains("undocumented state \"pending_cancel\""),
        "unknown regular order state should report the unsafe value: {error}"
    );
}

#[test]
fn regular_order_average_price_treats_zero_as_absent_without_fill() {
    let mut order = order("live");
    order.average_price = "0".to_owned();

    assert_eq!(
        order
            .average_fill_price()
            .expect("zero avgPx should be valid while accFillSz is zero"),
        None
    );
}

#[test]
fn regular_order_average_price_rejects_zero_with_fill() {
    let mut order = order("partially_filled");
    order.average_price = "0".to_owned();
    order.accumulated_fill_size = "0.001".to_owned();

    let error = order
        .average_fill_price()
        .expect_err("zero avgPx with a positive fill should fail closed");

    assert!(
        error
            .to_string()
            .contains("OKX order avgPx must be positive when accFillSz is positive"),
        "zero avgPx with fill should report the unsafe field: {error}"
    );
}

#[test]
fn fill_accessors_parse_okx_numeric_fields() {
    let fill = fill("bill-1", "0.001", "100.5");

    assert_eq!(
        fill.fill_size().expect("fill size should parse"),
        "0.001".parse().expect("decimal literal should parse")
    );
    assert_eq!(
        fill.fill_price().expect("fill price should parse"),
        "100.5".parse().expect("decimal literal should parse")
    );
    assert_eq!(fill.fill_time_ms(), 1_700_000_000_000);
    assert_eq!(fill.dedupe_key(), "bill-1");
}

#[test]
fn fill_identity_prefers_bill_id_and_falls_back_to_trade_id() {
    let mut fill = fill("bill-1", "0.001", "100.5");
    fill.trade_id = "trade-1".to_owned();

    assert_eq!(fill.dedupe_key(), "bill-1");

    fill.bill_id.clear();

    assert_eq!(fill.dedupe_key(), "trade-1");
}

#[test]
fn fill_time_prefers_execution_time_and_falls_back_to_event_time() {
    let mut fill = fill("bill-1", "0.001", "100.5");
    fill.event_time_ms = "1700000000001".to_owned();

    assert_eq!(fill.fill_time_ms(), 1_700_000_000_000);

    fill.fill_time_ms.clear();

    assert_eq!(fill.fill_time_ms(), 1_700_000_000_001);
}

#[test]
fn fill_accessors_reject_non_positive_values() {
    let zero_size = fill("bill-1", "0", "100.5");
    let size_error = zero_size
        .fill_size()
        .expect_err("zero OKX fill size should fail closed");
    assert!(
        size_error
            .to_string()
            .contains("OKX fill fillSz must be positive"),
        "zero fill size should report the malformed OKX field: {size_error}"
    );

    let zero_price = fill("bill-1", "0.001", "0");
    let price_error = zero_price
        .fill_price()
        .expect_err("zero OKX fill price should fail closed");
    assert!(
        price_error
            .to_string()
            .contains("OKX fill fillPx must be positive"),
        "zero fill price should report the malformed OKX field: {price_error}"
    );
}

#[test]
fn account_config_preflight_is_independent_of_documented_account_level() {
    for account_level in ["1", "2", "3", "4"] {
        let config = account_config(account_level, "read_only,trade", /*auto_loan*/ false);
        config
            .ensure_spot_trading_enabled()
            .unwrap_or_else(|error| {
                panic!("acctLv {account_level} must not decide the cash-SPOT boundary: {error}")
            });
    }
}

#[test]
fn account_config_live_kyc_accepts_only_documented_eligible_levels() {
    for kyc_level in ["2", "3"] {
        let mut config = account_config("1", "read_only,trade", false);
        config.kyc_level = kyc_level.to_owned();
        let validated = config
            .validated_live_kyc_level()
            .unwrap_or_else(|error| panic!("kycLv {kyc_level} should be eligible: {error}"));
        assert_eq!(validated.as_okx(), kyc_level);
    }

    for kyc_level in ["", "0", "1", "4", "02", "level2", "unknown"] {
        let mut config = account_config("1", "read_only,trade", false);
        config.kyc_level = kyc_level.to_owned();
        let error = config
            .validated_live_kyc_level()
            .expect_err("ineligible or malformed live KYC evidence should fail closed");
        assert!(
            error
                .to_string()
                .contains("Production order placement requires OKX kycLv 2 or 3"),
            "kycLv {kyc_level:?} should report the Production admission boundary: {error}"
        );
    }
}

#[test]
fn account_config_economics_preflight_is_independent_of_documented_account_level() {
    for account_level in ["1", "2", "3", "4"] {
        account_config(account_level, "read_only", /*auto_loan*/ false)
            .ensure_spot_economics_safe()
            .unwrap_or_else(|error| {
                panic!("acctLv {account_level} must not decide read-only SPOT economics: {error}")
            });
    }
}

#[test]
fn account_config_preflight_rejects_missing_malformed_or_undocumented_account_levels() {
    for account_level in ["", "0", "5", "01", " 1 ", "unknown"] {
        let error = account_config(account_level, "read_only,trade", false)
            .ensure_spot_trading_enabled()
            .expect_err("invalid account-level diagnostics should fail closed");
        assert!(
            error
                .to_string()
                .contains("missing, malformed, or undocumented"),
            "acctLv {account_level:?} should report invalid diagnostics: {error}"
        );
    }
}

#[test]
fn account_config_mutation_preflight_requires_trade_permission() {
    let read_only = account_config("1", "read_only", /*auto_loan*/ false);
    let read_only_error = read_only
        .ensure_spot_trading_enabled()
        .expect_err("read-only API key should fail closed");
    assert!(
        read_only_error.to_string().contains("do not include trade"),
        "missing trade permission should be reported: {read_only_error}"
    );
}

#[test]
fn account_config_preflight_rejects_every_borrow_or_auto_loan_setting() {
    let mut cases = Vec::new();

    cases.push((
        "autoLoan",
        account_config("2", "read_only,trade", /*auto_loan*/ true),
    ));

    let mut spot_borrow = account_config("2", "read_only,trade", false);
    spot_borrow.enable_spot_borrow = true;
    cases.push(("enableSpotBorrow", spot_borrow));

    let mut auto_repay = account_config("2", "read_only,trade", false);
    auto_repay.spot_borrow_auto_repay = true;
    cases.push(("spotBorrowAutoRepay", auto_repay));

    for (setting, config) in cases {
        let error = config
            .ensure_spot_trading_enabled()
            .expect_err("borrow-enabled account should fail closed");
        assert!(
            error.to_string().contains("borrowing"),
            "enabled {setting} should be reported: {error}"
        );
    }
}

#[test]
fn balance_validation_requires_official_balance_fields() {
    let mut balance = balance_detail("BTC", "0.001", "0.001", "0");
    balance
        .validate()
        .expect("complete OKX balance fields should validate");

    balance.details[0].cash_balance.clear();
    let cash_error = balance
        .validate()
        .expect_err("missing cashBal should fail closed");
    assert!(
        cash_error
            .to_string()
            .contains("OKX balance cashBal must be provided"),
        "missing cashBal should be reported: {cash_error}"
    );

    let balance = balance_detail("BTC", "0.001", "0.001", "");
    let frozen_error = balance
        .validate()
        .expect_err("missing frozenBal should fail closed");
    assert!(
        frozen_error
            .to_string()
            .contains("OKX balance frozenBal must be provided"),
        "missing frozenBal should be reported: {frozen_error}"
    );

    let balance = balance_detail("BTC", "not-decimal", "0.001", "0");
    let decimal_error = balance
        .validate()
        .expect_err("malformed availBal should fail closed");
    assert!(
        decimal_error
            .to_string()
            .contains("OKX balance availBal must be a decimal"),
        "malformed availBal should be reported: {decimal_error}"
    );
}

#[test]
fn trade_fee_rate_counts_negative_okx_values_as_commission() {
    let fee = trade_fee_rate("-0.0008", "-0.001");

    assert_eq!(
        fee.round_trip_commission_rate()
            .expect("fee rates should parse"),
        "0.0018".parse().expect("decimal literal should parse")
    );
    fee.ensure_round_trip_commission_at_most(
        "BTC-USDT",
        "0.002".parse().expect("decimal literal should parse"),
    )
    .expect("fee rate within assumption should validate");

    let error = fee
        .ensure_round_trip_commission_at_most(
            "BTC-USDT",
            "0.0016".parse().expect("decimal literal should parse"),
        )
        .expect_err("fee rate above assumption should fail closed");
    assert!(
        error
            .to_string()
            .contains("exceeds strategy fee assumption"),
        "fee-rate mismatch should be reported: {error}"
    );
}

#[test]
fn trade_fee_rate_treats_positive_okx_values_as_rebates() {
    let fee = trade_fee_rate("0.0001", "-0.001");

    assert_eq!(
        fee.round_trip_commission_rate()
            .expect("fee rates should parse"),
        "0.001".parse().expect("decimal literal should parse")
    );
    assert_eq!(
        (
            fee.normalized_maker_cost_rate()
                .expect("maker rebate should normalize"),
            fee.normalized_taker_cost_rate()
                .expect("taker commission should normalize"),
        ),
        (
            "-0.0001".parse().expect("decimal literal should parse"),
            "0.001".parse().expect("decimal literal should parse"),
        )
    );
}

#[test]
fn spot_fill_accounting_applies_fee_currency_and_rebates() {
    let mut received_currency_fee = fill("bill-base-fee", "0.001", "100000");
    received_currency_fee.fee = "-0.000001".to_owned();
    received_currency_fee.fee_currency = "BTC".to_owned();
    assert_eq!(
        received_currency_fee
            .spot_accounting("BTC", "USDT")
            .expect("received-currency fee should account"),
        OkxSpotFillAccounting {
            base_change: Decimal::new(999, 6),
            quote_change: Decimal::new(-100, 0),
        }
    );

    let mut quote_currency_fee = fill("bill-quote-fee", "0.001", "100000");
    quote_currency_fee.fee = "-0.1".to_owned();
    quote_currency_fee.fee_currency = "USDT".to_owned();
    assert_eq!(
        quote_currency_fee
            .spot_accounting("BTC", "USDT")
            .expect("quote-currency fee should account"),
        OkxSpotFillAccounting {
            base_change: Decimal::new(1, 3),
            quote_change: Decimal::new(-1001, 1),
        }
    );

    let mut base_rebate = fill("bill-rebate", "0.001", "100000");
    base_rebate.fee = "0.000001".to_owned();
    base_rebate.fee_currency = "BTC".to_owned();
    assert_eq!(
        base_rebate
            .spot_accounting("BTC", "USDT")
            .expect("base rebate should account"),
        OkxSpotFillAccounting {
            base_change: Decimal::new(1001, 6),
            quote_change: Decimal::new(-100, 0),
        }
    );
}

#[test]
fn spot_fill_accounting_rejects_unknown_fee_currency() {
    let mut fill = fill("bill-invalid-fee", "0.001", "100000");
    fill.fee = "-0.1".to_owned();
    fill.fee_currency = "OKB".to_owned();

    let error = fill
        .spot_accounting("BTC", "USDT")
        .expect_err("third-currency fee should fail closed");

    assert!(
        error.to_string().contains("feeCcy \"OKB\""),
        "unknown fee currency should be explicit: {error}"
    );
}

#[test]
fn order_accounting_combines_split_fee_and_rebate_currencies() {
    let mut order = order("filled");
    order.side = "buy".to_owned();
    order.average_price = "100000".to_owned();
    order.accumulated_fill_size = "0.001".to_owned();
    order.fee = "-0.1".to_owned();
    order.fee_currency = "USDT".to_owned();
    order.rebate = "0.000001".to_owned();
    order.rebate_currency = "BTC".to_owned();

    assert_eq!(
        order
            .cumulative_spot_accounting("BTC", "USDT")
            .expect("split fee and rebate should account"),
        OkxSpotFillAccounting {
            base_change: Decimal::new(1001, 6),
            quote_change: Decimal::new(-1001, 1),
        }
    );
}

#[test]
fn account_config_accepts_documented_spot_fee_types_only() {
    let received_currency = account_config("1", "read_only,trade", false);
    assert_eq!(
        received_currency
            .spot_fee_type()
            .expect("feeType 0 should be supported"),
        OkxSpotFeeType::ReceivedCurrency
    );

    let mut quote_currency = received_currency.clone();
    quote_currency.fee_type = "1".to_owned();
    assert_eq!(
        quote_currency
            .spot_fee_type()
            .expect("feeType 1 should be supported"),
        OkxSpotFeeType::QuoteCurrency
    );

    quote_currency.fee_type = "2".to_owned();
    let error = quote_currency
        .ensure_spot_trading_enabled()
        .expect_err("unknown fee type should fail closed");
    assert!(
        error.to_string().contains("feeType \"2\" is unsupported"),
        "unsupported fee type should be explicit: {error}"
    );
}

fn ticker(bid_px: &str, last: &str) -> OkxTicker {
    ticker_with_ask(bid_px, "100.1", last)
}

fn ticker_with_ask(bid_px: &str, ask_px: &str, last: &str) -> OkxTicker {
    OkxTicker {
        inst_type: "SPOT".to_owned(),
        inst_id: "BTC-USDT".to_owned(),
        bid_px: bid_px.to_owned(),
        ask_px: ask_px.to_owned(),
        last: last.to_owned(),
    }
}

fn account_config(account_level: &str, permissions: &str, auto_loan: bool) -> OkxAccountConfig {
    OkxAccountConfig {
        uid: "1001".to_owned(),
        main_uid: "1001".to_owned(),
        account_level: account_level.to_owned(),
        perm: permissions.to_owned(),
        auto_loan,
        enable_spot_borrow: false,
        spot_borrow_auto_repay: false,
        fee_type: "0".to_owned(),
        kyc_level: String::new(),
    }
}

fn trade_fee_rate(maker: &str, taker: &str) -> OkxTradeFeeRate {
    OkxTradeFeeRate {
        inst_type: "SPOT".to_owned(),
        level: "Lv1".to_owned(),
        group_id: "12".to_owned(),
        maker: maker.to_owned(),
        taker: taker.to_owned(),
        ts: "1763979985847".to_owned(),
    }
}

fn balance_detail(ccy: &str, available: &str, cash: &str, frozen: &str) -> OkxBalance {
    OkxBalance {
        details: vec![OkxBalanceDetail {
            ccy: ccy.to_owned(),
            available_balance: available.to_owned(),
            cash_balance: cash.to_owned(),
            frozen_balance: frozen.to_owned(),
        }],
    }
}

fn instrument() -> OkxInstrument {
    OkxInstrument {
        inst_type: "SPOT".to_owned(),
        inst_id: "BTC-USDT".to_owned(),
        group_id: "12".to_owned(),
        inst_id_code: Some(123_456),
        state: "live".to_owned(),
        base_ccy: "BTC".to_owned(),
        quote_ccy: "USDT".to_owned(),
        trade_quote_currencies: vec!["USDT".to_owned()],
        tick_size: "0.1".to_owned(),
        lot_size: "0.0001".to_owned(),
        min_size: "0.0001".to_owned(),
        max_limit_size: "1".to_owned(),
        max_limit_amount: "100000".to_owned(),
        max_market_size: String::new(),
        max_market_amount: "100000".to_owned(),
        max_trigger_size: "2".to_owned(),
        initial_price_limit_pct: "0.05".to_owned(),
        float_price_limit_pct: "0.03".to_owned(),
        maximum_price_limit_pct: "0.15".to_owned(),
    }
}

fn order(state: &str) -> OkxOrder {
    OkxOrder {
        inst_type: "SPOT".to_owned(),
        inst_id: "BTC-USDT".to_owned(),
        order_id: "ord-1".to_owned(),
        client_order_id: "client-ord-1".to_owned(),
        side: "sell".to_owned(),
        order_type: "limit".to_owned(),
        price: "100".to_owned(),
        state: state.to_owned(),
        average_price: String::new(),
        accumulated_fill_size: "0".to_owned(),
        fee: "0".to_owned(),
        fee_currency: "USDT".to_owned(),
        rebate: "0".to_owned(),
        rebate_currency: "BTC".to_owned(),
        sz: "0.001".to_owned(),
        created_at_ms: "1000".to_owned(),
        updated_at_ms: "1000".to_owned(),
    }
}

fn fill(bill_id: &str, fill_size: &str, fill_price: &str) -> OkxFill {
    OkxFill {
        inst_type: "SPOT".to_owned(),
        inst_id: "BTC-USDT".to_owned(),
        order_id: "ord-1".to_owned(),
        client_order_id: "client-ord-1".to_owned(),
        bill_id: bill_id.to_owned(),
        trade_id: String::new(),
        side: "buy".to_owned(),
        fill_size: fill_size.to_owned(),
        fill_price: fill_price.to_owned(),
        fee: "0".to_owned(),
        fee_currency: "BTC".to_owned(),
        fee_rate: "0".to_owned(),
        execution_type: "M".to_owned(),
        fill_time_ms: "1700000000000".to_owned(),
        event_time_ms: String::new(),
    }
}
