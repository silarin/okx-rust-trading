use std::time::Duration;

use rust_decimal::Decimal;

use super::{
    TradingInstrumentExchangeEvidence, ValidatedQuoteUsdRate, ValidatedSpotPriceLimit,
    ValidatedTradeMode, ValidatedTradingInstrument,
};
use crate::{
    config::types::{
        RequestedInstrumentId, RequestedInstrumentType, RequestedTradeMode,
        RequestedTradingInstrument,
    },
    okx::types::{
        OkxAccountConfig, OkxBalance, OkxBalanceDetail, OkxIndexTicker, OkxInstrument,
        OkxMaximumAvailableSize, OkxMaximumOrderSize, OkxPriceLimit, OrderSide,
    },
};

#[test]
fn exact_live_spot_cash_product_evidence_is_independent_of_account_level() {
    for account_level in ["1", "2", "3", "4"] {
        let validated = validate(
            requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
            instrument(),
            instrument(),
            account_config(account_level),
            maximum(),
            available(),
            balances(),
        )
        .expect("exact SPOT + cash evidence should pass");
        assert_eq!(validated.td_mode(), ValidatedTradeMode::Cash);
        assert_eq!(validated.inst_id(), "BTC-USDT");
        assert_eq!(validated.inst_id_code().expect("code"), Some(123_456));
        assert_eq!(validated.trade_quote_ccy(), "USDT");
    }
}

#[test]
fn spot_cross_is_recognized_but_rejected_as_roadmap_only() {
    let error = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cross),
        instrument(),
        instrument(),
        account_config("3"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("SPOT + cross must remain unreachable");
    assert!(error.to_string().contains("roadmap-only"));
}

#[test]
fn isolated_and_non_spot_tuples_fail_before_runtime_construction() {
    for requested in [
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Isolated),
        requested(
            RequestedInstrumentType::Spot,
            RequestedTradeMode::SpotIsolated,
        ),
        requested(RequestedInstrumentType::Margin, RequestedTradeMode::Cross),
        requested(RequestedInstrumentType::Swap, RequestedTradeMode::Cross),
        requested(RequestedInstrumentType::Futures, RequestedTradeMode::Cross),
        requested(
            RequestedInstrumentType::Option,
            RequestedTradeMode::Isolated,
        ),
        requested(
            RequestedInstrumentType::Events,
            RequestedTradeMode::Isolated,
        ),
    ] {
        validate(
            requested,
            instrument(),
            instrument(),
            account_config("2"),
            maximum(),
            available(),
            balances(),
        )
        .expect_err("unsupported tuple must fail");
    }
}

#[test]
fn public_and_account_metadata_disagreement_fails() {
    let mut account = instrument();
    account.tick_size = "0.1".to_owned();
    let error = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        account,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("precision disagreement must fail");
    assert!(error.to_string().contains("precision"));
}

#[test]
fn public_and_account_trade_quote_sets_must_agree_exactly() {
    let mut public = instrument();
    public.trade_quote_currencies = vec!["USDT".to_owned(), "USDC".to_owned()];
    let mut account = instrument();
    account.trade_quote_currencies = vec!["USDC".to_owned(), "USDT".to_owned()];
    validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        public.clone(),
        account,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect("trade-quote list ordering must not create a metadata contradiction");

    let error = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        public,
        instrument(),
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("missing account trade-quote entries must fail closed");
    assert!(
        error.to_string().contains("tradeQuoteCcyList"),
        "trade-quote disagreement should name the authoritative field: {error}"
    );
}

#[test]
fn malformed_or_duplicate_trade_quote_entries_fail_closed() {
    for currencies in [
        vec!["USDT".to_owned(), "USDT".to_owned()],
        vec!["USDT".to_owned(), String::new()],
        vec!["USDT".to_owned(), "usdc".to_owned()],
    ] {
        let mut public = instrument();
        public.trade_quote_currencies = currencies.clone();
        let mut account = instrument();
        account.trade_quote_currencies = currencies;
        let error = validate(
            requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
            public,
            account,
            account_config("1"),
            maximum(),
            available(),
            balances(),
        )
        .expect_err("malformed trade-quote entries must fail closed");
        assert!(
            error.to_string().contains("tradeQuoteCcyList"),
            "malformed trade-quote entries should name the authoritative field: {error}"
        );
    }
}

#[test]
fn refreshed_public_trade_quote_set_cannot_contradict_startup_context() {
    let validated = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        instrument(),
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect("startup evidence should validate");
    let mut refresh = instrument();
    refresh.trade_quote_currencies.push("USDC".to_owned());
    let error = validated
        .ensure_public_refresh_matches(&refresh)
        .expect_err("refreshed trade-quote disagreement must fail closed");
    assert!(
        format!("{error:#}").contains("tradeQuoteCcyList"),
        "refresh contradiction should name the authoritative field: {error:#}"
    );
}

#[test]
fn public_and_account_price_limit_percentages_must_agree_exactly() {
    let mut account = instrument();
    account.float_price_limit_pct = "0.04".to_owned();
    let error = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        account,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("price-limit percentage disagreement must fail closed");
    assert!(
        error.to_string().contains("price-limit percentage"),
        "metadata disagreement should name the authoritative fields: {error}"
    );
}

#[test]
fn inactive_initial_listing_band_can_be_absent_when_authorities_agree() {
    let mut public = instrument();
    public.initial_price_limit_pct.clear();
    let validated = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        public.clone(),
        public,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect("an inactive initial listing band may be absent");
    assert_eq!(
        validated
            .price_limit_percentages()
            .expect("validated percentages"),
        (None, Decimal::new(3, 2), Decimal::new(15, 2))
    );
    let refresh_error = validated
        .ensure_public_refresh_matches(&instrument())
        .expect_err("a later initial-band contradiction must fail closed");
    assert!(
        format!("{refresh_error:#}").contains("price-limit percentage"),
        "refresh contradiction should name the price-limit authority: {refresh_error:#}"
    );

    let mut contradictory = instrument();
    contradictory.initial_price_limit_pct.clear();
    let error = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        contradictory,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("present and absent initial bands must disagree");
    assert!(error.to_string().contains("price-limit percentage"));
}

#[test]
fn malformed_price_limit_percentages_and_refresh_contradictions_fail_closed() {
    for value in [" ", "0", "-0.01", "not-a-decimal"] {
        let mut public = instrument();
        public.initial_price_limit_pct = value.to_owned();
        let error = validate(
            requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
            public.clone(),
            public,
            account_config("1"),
            maximum(),
            available(),
            balances(),
        )
        .expect_err("malformed price-limit percentages must fail closed");
        assert!(
            error.to_string().contains("initPxLmtPct"),
            "malformed percentage should identify the field: {error}"
        );
    }
    for field in ["float", "maximum"] {
        let mut public = instrument();
        match field {
            "float" => public.float_price_limit_pct.clear(),
            "maximum" => public.maximum_price_limit_pct.clear(),
            _ => unreachable!(),
        }
        let error = validate(
            requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
            public.clone(),
            public,
            account_config("1"),
            maximum(),
            available(),
            balances(),
        )
        .expect_err("active price-limit percentages must remain required");
        assert!(
            error.to_string().contains(if field == "float" {
                "floatPxLmtPct"
            } else {
                "maxPxLmtPct"
            }),
            "missing active percentage should identify the field: {error}"
        );
    }

    let validated = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        instrument(),
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect("startup evidence should validate");
    let mut refresh = instrument();
    refresh.maximum_price_limit_pct = "0.16".to_owned();
    let error = validated
        .ensure_public_refresh_matches(&refresh)
        .expect_err("fresh public percentage contradiction must fail closed");
    assert!(
        format!("{error:#}").contains("price-limit percentage"),
        "refresh contradiction should name the price-limit authority: {error:#}"
    );
}

#[test]
fn dynamic_spot_price_limits_validate_side_specific_boundaries() {
    let evidence = ValidatedSpotPriceLimit::from_response(
        "BTC-USDT",
        OkxPriceLimit {
            inst_type: "MARGIN".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            buy_limit: "101".to_owned(),
            sell_limit: "99".to_owned(),
            timestamp_ms: "10000".to_owned(),
            enabled: true,
        },
        10_000,
        Duration::from_millis(3_000),
    )
    .expect("documented MARGIN response metadata must not broaden SPOT admission");

    evidence
        .ensure_price(OrderSide::Buy, Decimal::new(101, 0), "test buy")
        .expect("buy at upper boundary should pass");
    evidence
        .ensure_price(OrderSide::Sell, Decimal::new(99, 0), "test sell")
        .expect("sell at lower boundary should pass");
    evidence
        .ensure_price(OrderSide::Buy, Decimal::new(102, 0), "test buy")
        .expect_err("buy above upper boundary must fail");
    evidence
        .ensure_price(OrderSide::Sell, Decimal::new(98, 0), "test sell")
        .expect_err("sell below lower boundary must fail");
}

#[test]
fn disabled_dynamic_price_limits_require_empty_limits_and_add_no_band() {
    let evidence = ValidatedSpotPriceLimit::from_response(
        "BTC-USDT",
        OkxPriceLimit {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            buy_limit: String::new(),
            sell_limit: String::new(),
            timestamp_ms: "10000".to_owned(),
            enabled: false,
        },
        10_000,
        Duration::from_millis(3_000),
    )
    .expect("disabled evidence with empty limits should pass");
    evidence
        .ensure_price(OrderSide::Buy, Decimal::MAX, "disabled buy")
        .expect("disabled exchange limits must not invent a local band");

    for (buy_limit, sell_limit) in [("101", ""), ("", "99")] {
        ValidatedSpotPriceLimit::from_response(
            "BTC-USDT",
            OkxPriceLimit {
                inst_type: "SPOT".to_owned(),
                inst_id: "BTC-USDT".to_owned(),
                buy_limit: buy_limit.to_owned(),
                sell_limit: sell_limit.to_owned(),
                timestamp_ms: "10000".to_owned(),
                enabled: false,
            },
            10_000,
            Duration::from_millis(3_000),
        )
        .expect_err("disabled evidence with a populated limit must fail closed");
    }
}

#[test]
fn malformed_dynamic_price_limit_evidence_fails_closed() {
    let cases = [
        ("SWAP", "BTC-USDT", "101", "99", "10000", true),
        ("SPOT", "ETH-USDT", "101", "99", "10000", true),
        ("SPOT", "BTC-USDT", "0", "99", "10000", true),
        ("SPOT", "BTC-USDT", "101", "-1", "10000", true),
        ("SPOT", "BTC-USDT", "101", "99", "0", true),
        ("SPOT", "BTC-USDT", "101", "99", "10001", true),
        ("SPOT", "BTC-USDT", "101", "99", "6999", true),
    ];
    for (inst_type, inst_id, buy_limit, sell_limit, timestamp_ms, enabled) in cases {
        ValidatedSpotPriceLimit::from_response(
            "BTC-USDT",
            OkxPriceLimit {
                inst_type: inst_type.to_owned(),
                inst_id: inst_id.to_owned(),
                buy_limit: buy_limit.to_owned(),
                sell_limit: sell_limit.to_owned(),
                timestamp_ms: timestamp_ms.to_owned(),
                enabled,
            },
            10_000,
            Duration::from_millis(3_000),
        )
        .expect_err("malformed, stale, future, or contradictory evidence must fail closed");
    }
}

#[test]
fn usd_order_amount_limits_use_exact_validated_conversion_evidence() {
    let validated = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        instrument(),
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect("startup evidence should validate");
    let rate = quote_usd_rate("USDT", "2");

    validated
        .ensure_limit_quote_amount(Decimal::new(500_000, 0), &rate, "test limit notional")
        .expect("exact converted maxLmtAmt boundary should pass");
    let limit_error = validated
        .ensure_limit_quote_amount(Decimal::new(500_001, 0), &rate, "test limit notional")
        .expect_err("converted amount above maxLmtAmt must fail");
    assert!(
        limit_error
            .to_string()
            .contains("USD amount 1000002 exceeds OKX maxLmtAmt 1000000"),
        "limit error should report converted USD authority: {limit_error}"
    );

    validated
        .ensure_market_buy_quote_amount(Decimal::new(500_000, 0), &rate, "test market notional")
        .expect("exact converted maxMktAmt boundary should pass");
    let market_error = validated
        .ensure_market_buy_quote_amount(Decimal::new(500_001, 0), &rate, "test market notional")
        .expect_err("converted amount above maxMktAmt must fail");
    assert!(
        market_error
            .to_string()
            .contains("USD amount 1000002 exceeds OKX maxMktAmt 1000000"),
        "market error should report converted USD authority: {market_error}"
    );
}

#[test]
fn usd_order_amount_limits_reject_mismatched_or_overflowing_conversion_evidence() {
    let validated = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        instrument(),
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect("startup evidence should validate");

    let mismatch = quote_usd_rate("USDC", "1");
    let mismatch_error = validated
        .ensure_limit_quote_amount(Decimal::ONE, &mismatch, "test limit notional")
        .expect_err("mismatched conversion source must fail");
    assert!(
        mismatch_error
            .to_string()
            .contains("contradicts validated quote currency USDT"),
        "conversion mismatch should report the validated quote: {mismatch_error}"
    );

    let overflow = quote_usd_rate("USDT", &Decimal::MAX.to_string());
    let overflow_error = validated
        .ensure_limit_quote_amount(Decimal::TWO, &overflow, "test limit notional")
        .expect_err("conversion overflow must fail without panicking");
    assert!(
        overflow_error
            .to_string()
            .contains("USD conversion overflowed Decimal"),
        "conversion overflow should be contextual: {overflow_error}"
    );
}

#[test]
fn public_and_account_fee_group_disagreement_fails() {
    let mut account = instrument();
    account.group_id = "13".to_owned();
    let error = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        account,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("fee-group disagreement must fail");
    assert!(error.to_string().contains("identity metadata"));
}

#[test]
fn unavailable_or_currency_incompatible_account_instrument_fails() {
    let mut unavailable = instrument();
    unavailable.state = "suspend".to_owned();
    validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        unavailable,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("account-unavailable instrument must fail");

    let mut incompatible = instrument();
    incompatible.trade_quote_currencies = vec!["USD".to_owned()];
    validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        incompatible,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("account trade-quote disagreement must fail");
}

#[test]
fn invalid_or_disagreeing_instrument_codes_fail() {
    let mut invalid = instrument();
    invalid.inst_id_code = Some(0);
    validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        invalid.clone(),
        invalid,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("zero instIdCode must fail");

    let mut account = instrument();
    account.inst_id_code = Some(654_321);
    validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        account,
        account_config("1"),
        maximum(),
        available(),
        balances(),
    )
    .expect_err("disagreeing instIdCode must fail");
}

#[test]
fn sizing_rejection_or_balance_contradiction_fails() {
    let mut malformed = maximum();
    malformed.max_buy = "-1".to_owned();
    validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        instrument(),
        account_config("1"),
        malformed,
        available(),
        balances(),
    )
    .expect_err("negative capacity must fail");

    let mut contradictory = available();
    contradictory.available_sell = "2".to_owned();
    validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        instrument(),
        account_config("1"),
        maximum(),
        contradictory,
        balances(),
    )
    .expect_err("capacity exceeding cash balance must fail");
}

#[test]
fn sizing_decimal_overflow_fails_closed_without_panicking() {
    let max_decimal = Decimal::MAX.to_string();

    let mut multiplication_maximum = maximum();
    multiplication_maximum.max_buy = max_decimal.clone();
    let mut multiplication_available = available();
    multiplication_available.available_buy = max_decimal.clone();
    let multiplication_balances = vec![OkxBalance {
        details: vec![balance("BTC", "1"), balance("USDT", max_decimal.as_str())],
    }];
    let multiplication_error = validate(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        instrument(),
        account_config("1"),
        multiplication_maximum,
        multiplication_available,
        multiplication_balances,
    )
    .expect_err("maxBuy multiplication overflow must fail closed");
    assert!(
        multiplication_error
            .to_string()
            .contains("overflowed Decimal"),
        "multiplication overflow should be contextual: {multiplication_error}"
    );

    let mut division_maximum = maximum();
    division_maximum.max_buy = "0".to_owned();
    division_maximum.max_sell = max_decimal.clone();
    let mut division_available = available();
    division_available.available_buy = max_decimal.clone();
    division_available.available_sell = max_decimal.clone();
    let division_balances = vec![OkxBalance {
        details: vec![
            balance("BTC", max_decimal.as_str()),
            balance("USDT", max_decimal.as_str()),
        ],
    }];
    let division_error = validate_at_price(
        requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
        instrument(),
        instrument(),
        account_config("1"),
        Decimal::new(1, 1),
        division_maximum,
        division_available,
        division_balances,
    )
    .expect_err("maxSell division overflow must fail closed");
    assert!(
        division_error.to_string().contains("overflowed Decimal"),
        "division overflow should be contextual: {division_error}"
    );
}

#[test]
fn cash_spot_max_size_currency_accepts_only_empty_or_exact_base() {
    for currency in ["", "BTC"] {
        let mut maximum = maximum();
        maximum.ccy = currency.to_owned();
        validate(
            requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
            instrument(),
            instrument(),
            account_config("1"),
            maximum,
            available(),
            balances(),
        )
        .unwrap_or_else(|error| {
            panic!("cash-SPOT max-size ccy {currency:?} should be accepted: {error}")
        });
    }

    for currency in ["ETH", " "] {
        let mut maximum = maximum();
        maximum.ccy = currency.to_owned();
        let error = validate(
            requested(RequestedInstrumentType::Spot, RequestedTradeMode::Cash),
            instrument(),
            instrument(),
            account_config("1"),
            maximum,
            available(),
            balances(),
        )
        .expect_err("contradictory cash-SPOT max-size ccy must fail");
        assert!(
            error
                .to_string()
                .contains("cash-SPOT max-size margin currency"),
            "unexpected error for ccy {currency:?}: {error}"
        );
    }
}

fn validate(
    requested: RequestedTradingInstrument,
    public: OkxInstrument,
    account: OkxInstrument,
    account_config: OkxAccountConfig,
    maximum: OkxMaximumOrderSize,
    available: OkxMaximumAvailableSize,
    balances: Vec<OkxBalance>,
) -> anyhow::Result<ValidatedTradingInstrument> {
    validate_at_price(
        requested,
        public,
        account,
        account_config,
        Decimal::new(100, 0),
        maximum,
        available,
        balances,
    )
}

#[allow(clippy::too_many_arguments)]
fn validate_at_price(
    requested: RequestedTradingInstrument,
    public: OkxInstrument,
    account: OkxInstrument,
    account_config: OkxAccountConfig,
    price: Decimal,
    maximum: OkxMaximumOrderSize,
    available: OkxMaximumAvailableSize,
    balances: Vec<OkxBalance>,
) -> anyhow::Result<ValidatedTradingInstrument> {
    let quote_usd_rate = if public.has_usd_order_amount_limit()? {
        Some(if public.quote_ccy == "USD" {
            ValidatedQuoteUsdRate::identity(&public.quote_ccy)?
        } else {
            ValidatedQuoteUsdRate::from_index_ticker(
                &public.quote_ccy,
                &OkxIndexTicker {
                    inst_id: format!("{}-USD", public.quote_ccy),
                    index_price: "1".to_owned(),
                    timestamp_ms: "1".to_owned(),
                },
            )?
        })
    } else {
        None
    };
    ValidatedTradingInstrument::from_exchange_evidence(
        &requested,
        TradingInstrumentExchangeEvidence {
            public,
            account,
            account_config: &account_config,
            price,
            maximum: &maximum,
            available: &available,
            balances: &balances,
            quote_usd_rate: quote_usd_rate.as_ref(),
        },
    )
}

fn requested(
    inst_type: RequestedInstrumentType,
    td_mode: RequestedTradeMode,
) -> RequestedTradingInstrument {
    RequestedTradingInstrument {
        instrument: RequestedInstrumentId::new("BTC-USDT".to_owned()).expect("instrument"),
        inst_type,
        td_mode,
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
        tick_size: "0.01".to_owned(),
        lot_size: "0.0001".to_owned(),
        min_size: "0.0001".to_owned(),
        max_limit_size: "100".to_owned(),
        max_limit_amount: "1000000".to_owned(),
        max_market_size: "100".to_owned(),
        max_market_amount: "1000000".to_owned(),
        max_trigger_size: "100".to_owned(),
        initial_price_limit_pct: "0.05".to_owned(),
        float_price_limit_pct: "0.03".to_owned(),
        maximum_price_limit_pct: "0.15".to_owned(),
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

fn maximum() -> OkxMaximumOrderSize {
    OkxMaximumOrderSize {
        inst_id: "BTC-USDT".to_owned(),
        ccy: "BTC".to_owned(),
        max_buy: "1".to_owned(),
        max_sell: "100".to_owned(),
    }
}

fn available() -> OkxMaximumAvailableSize {
    OkxMaximumAvailableSize {
        inst_id: "BTC-USDT".to_owned(),
        available_buy: "100000".to_owned(),
        available_sell: "1".to_owned(),
    }
}

fn balances() -> Vec<OkxBalance> {
    vec![OkxBalance {
        details: vec![balance("BTC", "1"), balance("USDT", "100000")],
    }]
}

fn balance(currency: &str, amount: &str) -> OkxBalanceDetail {
    OkxBalanceDetail {
        ccy: currency.to_owned(),
        available_balance: amount.to_owned(),
        cash_balance: amount.to_owned(),
        frozen_balance: "0".to_owned(),
    }
}

fn quote_usd_rate(quote_ccy: &str, price: &str) -> ValidatedQuoteUsdRate {
    ValidatedQuoteUsdRate::from_index_ticker(
        quote_ccy,
        &OkxIndexTicker {
            inst_id: format!("{quote_ccy}-USD"),
            index_price: price.to_owned(),
            timestamp_ms: "1".to_owned(),
        },
    )
    .expect("quote-to-USD evidence")
}
