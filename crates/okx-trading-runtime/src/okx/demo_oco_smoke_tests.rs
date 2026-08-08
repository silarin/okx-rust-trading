use anyhow::Result;
use rust_decimal::Decimal;

use super::*;

fn oco_json(state: &str, actual_side: &str, actual_size: &str) -> String {
    format!(
        r#"{{
          "instType":"SPOT","instId":"ETH-USDT","algoId":"algo-1",
          "algoClOrdId":"OKXOCOTEST1","ordId":"ord-1","side":"sell",
          "ordType":"oco","state":"{state}","sz":"0.00002",
          "tpTriggerPx":"110000","tpTriggerPxType":"last","tpOrdPx":"-1",
          "slTriggerPx":"90000","slTriggerPxType":"last","slOrdPx":"-1",
          "actualSide":"{actual_side}","actualSz":"{actual_size}","actualPx":"100000",
          "tag":"okxrusttrading","cTime":"1","uTime":"2"
        }}"#
    )
}

fn eth_usdt_instrument() -> Result<OkxInstrument> {
    Ok(serde_json::from_str(
        r#"{
          "instType":"SPOT","instId":"ETH-USDT","groupId":"12","state":"live",
          "baseCcy":"ETH","quoteCcy":"USDT","tradeQuoteCcyList":["USDT"],
          "tickSz":"0.1","lotSz":"0.00000001","minSz":"0.00001",
          "maxLmtSz":"1","maxLmtAmt":"1000000",
          "maxMktSz":"1000000","maxMktAmt":"1000000","maxTriggerSz":"1",
          "initPxLmtPct":"0.05","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"
        }"#,
    )?)
}

fn fill_json(
    instrument_id: &str,
    client_order_id: &str,
    bill_id: &str,
    side: &str,
    size: &str,
    fee: &str,
    fee_currency: &str,
) -> String {
    format!(
        r#"{{
          "instType":"SPOT","instId":"{instrument_id}","ordId":"order-1",
          "clOrdId":"{client_order_id}","billId":"{bill_id}","tradeId":"trade-{bill_id}",
          "side":"{side}","fillSz":"{size}","fillPx":"1000",
          "fee":"{fee}","feeCcy":"{fee_currency}","execType":"T",
          "fillTime":"1710000000000"
        }}"#
    )
}

fn acquisition_order_json(client_order_id: &str, fill_size: &str, fee: &str) -> String {
    format!(
        r#"{{
          "instType":"SPOT","instId":"ETH-USDT","ordId":"order-1",
          "clOrdId":"{client_order_id}","side":"buy","ordType":"limit",
          "px":"1000","state":"canceled","avgPx":"1000",
          "accFillSz":"{fill_size}","fee":"{fee}","feeCcy":"ETH",
          "rebate":"0","rebateCcy":"","sz":"0.00011",
          "cTime":"1710000000000","uTime":"1710000000100"
        }}"#
    )
}

#[test]
fn parses_exact_spot_oco_and_documented_sibling_outcomes() -> Result<()> {
    let live: OkxOcoOrder = serde_json::from_str(&oco_json("live", "", ""))?;
    live.ensure_contract("test")?;
    assert!(ExpectedOcoState::Pending.matches(&live));

    for (side, expected) in [
        ("tp", ExpectedOcoState::Executed("tp")),
        ("sl", ExpectedOcoState::Executed("sl")),
    ] {
        let executed: OkxOcoOrder = serde_json::from_str(&oco_json("effective", side, "0.00002"))?;
        executed.ensure_contract("test")?;
        executed.ensure_clean_execution(side)?;
        assert!(expected.matches(&executed));
        assert!(!ExpectedOcoState::Pending.matches(&executed));
    }
    Ok(())
}

#[test]
fn rejects_non_spot_non_oco_and_partial_execution_rows() -> Result<()> {
    let mut wrong_instrument: serde_json::Value = serde_json::from_str(&oco_json("live", "", ""))?;
    wrong_instrument["instType"] = serde_json::json!("MARGIN");
    let wrong_instrument: OkxOcoOrder = serde_json::from_value(wrong_instrument)?;
    assert!(wrong_instrument.ensure_contract("test").is_err());

    let mut wrong_type: serde_json::Value = serde_json::from_str(&oco_json("live", "", ""))?;
    wrong_type["ordType"] = serde_json::json!("conditional");
    let wrong_type: OkxOcoOrder = serde_json::from_value(wrong_type)?;
    assert!(wrong_type.ensure_contract("test").is_err());

    let partial: OkxOcoOrder =
        serde_json::from_str(&oco_json("partially_effective", "tp", "0.00001"))?;
    assert!(partial.ensure_clean_execution("tp").is_err());
    Ok(())
}

#[test]
fn protected_operator_baseline_fails_closed_on_any_deficit() {
    let baseline = CurrencyBalance {
        available: Decimal::ONE,
        total: Decimal::ONE,
        frozen: Decimal::ZERO,
    };
    let below = CurrencyBalance {
        available: Decimal::new(9999, 4),
        total: Decimal::new(9999, 4),
        frozen: Decimal::ZERO,
    };

    assert!(ensure_operator_baseline(baseline, below, "ETH").is_err());
    assert!(total_delta_from_baseline(baseline, below, "ETH").is_err());
}

#[test]
fn frozen_or_unavailable_baseline_is_rejected_before_mutation() {
    let frozen = CurrencyBalance {
        available: Decimal::new(9, 1),
        total: Decimal::ONE,
        frozen: Decimal::new(1, 1),
    };
    assert!(ensure_unfrozen_baseline(frozen, "ETH").is_err());
}

#[test]
fn quantity_contract_requires_exact_decimal_lot_value() -> Result<()> {
    let order: OkxOcoOrder = serde_json::from_str(&oco_json("live", "", ""))?;
    ensure_protected_quantity(&order, Decimal::new(2, 5))?;
    assert!(ensure_protected_quantity(&order, Decimal::new(3, 5)).is_err());
    Ok(())
}

#[test]
fn canceled_state_is_terminal_and_not_executed() -> Result<()> {
    let canceled: OkxOcoOrder = serde_json::from_str(&oco_json("canceled", "", ""))?;
    canceled.ensure_contract("test")?;
    assert!(canceled.is_terminal());
    assert!(ExpectedOcoState::Canceled.matches(&canceled));
    assert!(canceled.ensure_clean_execution("tp").is_err());
    Ok(())
}

#[test]
fn cleanup_refuses_an_oco_without_test_owned_id_and_tag() -> Result<()> {
    let owned: OkxOcoOrder = serde_json::from_str(&oco_json("live", "", ""))?;
    ensure_test_owned_oco(&owned)?;

    let mut unowned = owned.clone();
    unowned.client_order_id = "OPERATOROCO1".to_owned();
    assert!(ensure_test_owned_oco(&unowned).is_err());
    unowned.client_order_id = "OKXOCOTEST1".to_owned();
    unowned.tag = "other".to_owned();
    assert!(ensure_test_owned_oco(&unowned).is_err());
    Ok(())
}

#[test]
fn acquisition_plan_preserves_one_lot_resize_after_received_currency_fee() -> Result<()> {
    let instrument = eth_usdt_instrument()?;
    let fee = Decimal::new(1, 3);
    let plan = acquisition_plan(
        &instrument,
        OkxSpotFeeType::ReceivedCurrency,
        fee,
        Decimal::from(100_000u32),
    )?;
    let net = plan.size * (Decimal::ONE - fee);
    let protected = quantize_decimal_down(net, instrument.lot_size()?)?;

    assert!(plan.required_quote <= HARD_QUOTE_NOTIONAL_CAP);
    assert!(protected - instrument.lot_size()? >= instrument.min_size()?);
    assert_eq!(plan.price, Decimal::from(100_500u32));
    assert!(
        acquisition_plan(
            &instrument,
            OkxSpotFeeType::ReceivedCurrency,
            fee,
            Decimal::from(2_000_000u32),
        )
        .is_err(),
        "the 20-USDT acquisition cap must remain enforced"
    );
    Ok(())
}

#[test]
fn acquisition_capacity_requires_exchange_and_balance_evidence_to_agree() -> Result<()> {
    let instrument = eth_usdt_instrument()?;
    let plan = acquisition_plan(
        &instrument,
        OkxSpotFeeType::QuoteCurrency,
        Decimal::new(1, 3),
        Decimal::from(100_000u32),
    )?;
    let maximum_without_margin_currency = OkxMaximumOrderSize {
        inst_id: "ETH-USDT".to_owned(),
        ccy: String::new(),
        max_buy: plan.size.to_string(),
        max_sell: "100".to_owned(),
    };
    let available = OkxMaximumAvailableSize {
        inst_id: "ETH-USDT".to_owned(),
        available_buy: "100".to_owned(),
        available_sell: "1".to_owned(),
    };
    let base = CurrencyBalance {
        available: Decimal::ONE,
        total: Decimal::ONE,
        frozen: Decimal::ZERO,
    };
    let quote = CurrencyBalance {
        available: Decimal::from(100u32),
        total: Decimal::from(100u32),
        frozen: Decimal::ZERO,
    };

    validate_acquisition_capacity(
        &instrument,
        plan,
        &maximum_without_margin_currency,
        &available,
        base,
        quote,
    )?;

    let mut maximum = maximum_without_margin_currency.clone();
    maximum.ccy = "ETH".to_owned();
    validate_acquisition_capacity(&instrument, plan, &maximum, &available, base, quote)?;

    let mut unrelated_currency = maximum.clone();
    unrelated_currency.ccy = "USDT".to_owned();
    assert!(
        validate_acquisition_capacity(
            &instrument,
            plan,
            &unrelated_currency,
            &available,
            base,
            quote,
        )
        .is_err()
    );

    let mut mismatched_maximum = maximum.clone();
    mismatched_maximum.inst_id = "BTC-USDT".to_owned();
    assert!(
        validate_acquisition_capacity(
            &instrument,
            plan,
            &mismatched_maximum,
            &available,
            base,
            quote,
        )
        .is_err()
    );

    let mut insufficient_maximum = maximum.clone();
    insufficient_maximum.max_buy = (plan.size - instrument.lot_size()?).to_string();
    assert!(
        validate_acquisition_capacity(
            &instrument,
            plan,
            &insufficient_maximum,
            &available,
            base,
            quote
        )
        .is_err()
    );

    let mut insufficient_available = available.clone();
    insufficient_available.available_buy = "0".to_owned();
    assert!(
        validate_acquisition_capacity(
            &instrument,
            plan,
            &maximum,
            &insufficient_available,
            base,
            quote,
        )
        .is_err()
    );

    let mut contradictory_available = available.clone();
    contradictory_available.available_buy = "99".to_owned();
    assert!(
        validate_acquisition_capacity(
            &instrument,
            plan,
            &maximum,
            &contradictory_available,
            base,
            quote
        )
        .is_err()
    );

    let mut contradictory_base = available.clone();
    contradictory_base.available_sell = "0.5".to_owned();
    assert!(
        validate_acquisition_capacity(
            &instrument,
            plan,
            &maximum,
            &contradictory_base,
            base,
            quote,
        )
        .is_err()
    );

    let mut malformed_maximum = maximum.clone();
    malformed_maximum.max_buy = "not-a-decimal".to_owned();
    assert!(
        validate_acquisition_capacity(
            &instrument,
            plan,
            &malformed_maximum,
            &available,
            base,
            quote,
        )
        .is_err()
    );

    let mut negative_maximum = maximum;
    negative_maximum.max_sell = "-1".to_owned();
    assert!(
        validate_acquisition_capacity(
            &instrument,
            plan,
            &negative_maximum,
            &available,
            base,
            quote,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn acquisition_run_identity_is_stable_across_buy_and_cleanup() -> Result<()> {
    let identity = AcquisitionRunIdentity::from_digits("1710000000123")?;

    assert_eq!(
        identity.acquisition_client_order_id,
        "OKXOCOPB1710000000123"
    );
    assert_eq!(identity.cleanup_client_order_id, "OKXOCOPS1710000000123");
    Ok(())
}

#[test]
fn cleanup_sale_is_capped_by_run_owned_fills_not_unrelated_balance() -> Result<()> {
    let instrument = eth_usdt_instrument()?;
    let baseline = CurrencyBalance {
        available: Decimal::ONE,
        total: Decimal::ONE,
        frozen: Decimal::ZERO,
    };
    let current = CurrencyBalance {
        available: Decimal::new(100_021, 5),
        total: Decimal::new(100_021, 5),
        frozen: Decimal::ZERO,
    };
    let run_owned = Decimal::new(11, 5);

    let sell_size = acquisition_cleanup_sale_size(&instrument, baseline, current, run_owned)?;

    assert_eq!(sell_size, run_owned);
    Ok(())
}

#[test]
fn cleanup_sale_is_capped_by_safe_total_and_available_deltas() -> Result<()> {
    let instrument = eth_usdt_instrument()?;
    let baseline = CurrencyBalance {
        available: Decimal::ONE,
        total: Decimal::ONE,
        frozen: Decimal::ZERO,
    };
    let total_limited = CurrencyBalance {
        available: Decimal::new(100_007, 5),
        total: Decimal::new(100_007, 5),
        frozen: Decimal::ZERO,
    };
    let available_limited = CurrencyBalance {
        available: Decimal::new(100_005, 5),
        total: Decimal::new(100_009, 5),
        frozen: Decimal::new(4, 5),
    };
    let run_owned = Decimal::new(11, 5);

    assert_eq!(
        acquisition_cleanup_sale_size(&instrument, baseline, total_limited, run_owned)?,
        Decimal::new(7, 5)
    );
    assert_eq!(
        acquisition_cleanup_sale_size(&instrument, baseline, available_limited, run_owned)?,
        Decimal::new(5, 5)
    );
    Ok(())
}

#[test]
fn cleanup_sale_rejects_any_protected_baseline_deficit() -> Result<()> {
    let instrument = eth_usdt_instrument()?;
    let baseline = CurrencyBalance {
        available: Decimal::ONE,
        total: Decimal::ONE,
        frozen: Decimal::ZERO,
    };
    let total_deficit = CurrencyBalance {
        available: Decimal::new(99999, 5),
        total: Decimal::new(99999, 5),
        frozen: Decimal::ZERO,
    };
    let available_deficit = CurrencyBalance {
        available: Decimal::new(99999, 5),
        total: Decimal::new(100_001, 5),
        frozen: Decimal::new(2, 5),
    };

    assert!(
        acquisition_cleanup_sale_size(&instrument, baseline, total_deficit, Decimal::new(11, 5))
            .is_err()
    );
    assert!(
        acquisition_cleanup_sale_size(
            &instrument,
            baseline,
            available_deficit,
            Decimal::new(11, 5)
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn run_owned_fill_accounting_requires_exact_unique_buy_evidence() -> Result<()> {
    let instrument = eth_usdt_instrument()?;
    let client_order_id = "OKXOCOPB1710000000123";
    let first: OkxFill = serde_json::from_str(&fill_json(
        "ETH-USDT",
        client_order_id,
        "bill-1",
        "buy",
        "0.00006",
        "-0.00000006",
        "ETH",
    ))?;
    let second: OkxFill = serde_json::from_str(&fill_json(
        "ETH-USDT",
        client_order_id,
        "bill-2",
        "buy",
        "0.00005",
        "0",
        "ETH",
    ))?;

    let accounting = run_owned_fill_accounting(
        &[first.clone(), second],
        &instrument,
        client_order_id,
        OrderSide::Buy,
        "acquisition",
    )?;
    assert_eq!(accounting.base_change, Decimal::new(10994, 8));
    assert!(accounting.quote_change < Decimal::ZERO);

    let duplicate = run_owned_fill_accounting(
        &[first.clone(), first.clone()],
        &instrument,
        client_order_id,
        OrderSide::Buy,
        "acquisition",
    );
    assert!(duplicate.is_err());

    let wrong_instrument: OkxFill = serde_json::from_str(&fill_json(
        "BTC-USDT",
        client_order_id,
        "bill-3",
        "buy",
        "0.00001",
        "0",
        "BTC",
    ))?;
    assert!(
        run_owned_fill_accounting(
            &[wrong_instrument],
            &instrument,
            client_order_id,
            OrderSide::Buy,
            "acquisition"
        )
        .is_err()
    );

    let wrong_side: OkxFill = serde_json::from_str(&fill_json(
        "ETH-USDT",
        client_order_id,
        "bill-4",
        "sell",
        "0.00001",
        "0",
        "USDT",
    ))?;
    assert!(
        run_owned_fill_accounting(
            &[wrong_side],
            &instrument,
            client_order_id,
            OrderSide::Buy,
            "acquisition"
        )
        .is_err()
    );

    let mut wrong_execution = first.clone();
    wrong_execution.execution_type = "M".to_owned();
    assert!(
        run_owned_fill_accounting(
            &[wrong_execution],
            &instrument,
            client_order_id,
            OrderSide::Buy,
            "acquisition"
        )
        .is_err()
    );

    let mut malformed_fee = first;
    malformed_fee.fee = "not-a-decimal".to_owned();
    assert!(
        run_owned_fill_accounting(
            &[malformed_fee],
            &instrument,
            client_order_id,
            OrderSide::Buy,
            "acquisition"
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn positive_terminal_acquisition_requires_matching_fill_evidence() -> Result<()> {
    let client_order_id = "OKXOCOPB1710000000123";
    let terminal: OkxOrder =
        serde_json::from_str(&acquisition_order_json(client_order_id, "0.00011", "0"))?;

    assert!(
        ensure_acquisition_accounting_matches_terminal(
            OkxSpotFillAccounting::default(),
            Some(&terminal),
            "ETH",
            "USDT",
            client_order_id
        )
        .is_err()
    );
    assert!(
        ensure_acquisition_accounting_matches_terminal(
            OkxSpotFillAccounting {
                base_change: Decimal::new(11, 5),
                quote_change: Decimal::new(-11, 2),
            },
            None,
            "ETH",
            "USDT",
            client_order_id
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn no_run_owned_acquisition_fill_produces_no_cleanup_sale() -> Result<()> {
    let instrument = eth_usdt_instrument()?;
    let client_order_id = "OKXOCOPB1710000000123";
    let unrelated: OkxFill = serde_json::from_str(&fill_json(
        "ETH-USDT",
        "OTHERORDER1",
        "bill-other",
        "buy",
        "0.001",
        "0",
        "ETH",
    ))?;
    let accounting = run_owned_fill_accounting(
        &[unrelated],
        &instrument,
        client_order_id,
        OrderSide::Buy,
        "acquisition",
    )?;
    let baseline = CurrencyBalance {
        available: Decimal::ONE,
        total: Decimal::ONE,
        frozen: Decimal::ZERO,
    };
    let current = CurrencyBalance {
        available: Decimal::new(100_100, 5),
        total: Decimal::new(100_100, 5),
        frozen: Decimal::ZERO,
    };

    assert_eq!(accounting, OkxSpotFillAccounting::default());
    assert_eq!(
        acquisition_cleanup_sale_size(&instrument, baseline, current, accounting.base_change)?,
        Decimal::ZERO
    );
    Ok(())
}
