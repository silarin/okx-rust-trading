use super::*;
use pretty_assertions::assert_eq;

#[test]
fn entry_quantity_preserves_decimal_quantity_and_quote_cap() {
    let runner = test_runner("okx-ema-atr-maker-btc-usdt");

    assert_eq!(
        runner
            .entry_quantity(dec!("100000"))
            .expect("configured quantity should remain exact"),
        dec!("0.001")
    );
    assert_eq!(
        runner
            .entry_quantity(dec!("1000000"))
            .expect("quote cap should reduce quantity exactly"),
        dec!("500") / (dec!("1000000") * dec!("1.001"))
    );
}

#[tokio::test]
async fn numeric_boundary_precision_entry_order_shape_uses_decimal_quantization() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = test_runner(strategy_id);
    let client = MockOkxClient {
        instrument: precision_instrument(),
        candles: numeric_boundary_precision_bars(),
        ticker: ticker_with_last("100.095"),
        ..MockOkxClient::default()
    };

    runner.initialize(&client).await?;
    runner.evaluate_entry(&client).await?;

    assert_eq!(
        runner.signal.current_atr_offset,
        Some(dec!("0.1")),
        "ATR-derived entry offset must cross into exact Decimal before price math"
    );
    assert_eq!(
        client.placed_orders(),
        vec![PlacedOrder {
            inst_id: "BTC-USDT".to_owned(),
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: "0.001".to_owned(),
            price: Some("99.99".to_owned()),
            purpose: Some(OrderPurpose::Entry),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn entry_order_quote_cap_uses_final_limit_price_not_ticker_last() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = test_runner(strategy_id);
    let client = MockOkxClient {
        ticker: ticker_with_bid_last("500200", "499900"),
        ..MockOkxClient::default()
    };

    runner.initialize(&client).await?;
    let expected_price = quantize_decimal_down(
        runner
            .signal
            .entry_price_from_bid(dec!("500200"))?
            .expect("entry price should be available"),
        instrument().tick_size()?,
    )?;
    let expected_size =
        quantize_decimal_down(dec!("500") / expected_price, instrument().lot_size()?)?;

    runner.evaluate_entry(&client).await?;

    assert_eq!(expected_size, dec!("0.0009"));
    assert!(
        expected_size * expected_price <= dec!("500"),
        "final entry notional must stay within the quote cap"
    );
    assert_eq!(
        client.placed_orders(),
        vec![PlacedOrder {
            inst_id: "BTC-USDT".to_owned(),
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: decimal_to_okx(expected_size),
            price: Some(decimal_to_okx(expected_price)),
            purpose: Some(OrderPurpose::Entry),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn entry_order_lot_rounding_keeps_quote_notional_at_or_below_cap() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = test_runner(strategy_id);
    runner.quantity = dec!("1");
    runner.max_quote_notional = Some(dec!("10"));
    let client = MockOkxClient {
        ticker: ticker_with_bid_last("33333", "1"),
        ..MockOkxClient::default()
    };

    runner.initialize(&client).await?;
    runner.evaluate_entry(&client).await?;

    let placed_orders = client.placed_orders();
    assert_eq!(placed_orders.len(), 1);
    let placed_order = &placed_orders[0];
    let size = placed_order
        .size
        .parse::<Decimal>()
        .expect("entry size should be a decimal");
    let price = placed_order
        .price
        .as_ref()
        .expect("entry order should include a price")
        .parse::<Decimal>()
        .expect("entry price should be a decimal");
    assert_eq!(size, dec!("0.0002"));
    assert!(
        size * price * dec!("1.001") <= dec!("10"),
        "worst-case entry cost including maker commission must stay within the quote cap"
    );
    Ok(())
}

#[tokio::test]
async fn entry_order_fails_closed_when_cap_size_is_below_min_size() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = test_runner(strategy_id);
    runner.max_quote_notional = Some(dec!("50"));
    let client = MockOkxClient {
        ticker: ticker_with_bid_last("600000", "1"),
        ..MockOkxClient::default()
    };

    runner.initialize(&client).await?;
    let error = runner
        .evaluate_entry(&client)
        .await
        .expect_err("cap-compliant size below minSz should fail closed");

    assert!(
        error.to_string().contains("below OKX minSz"),
        "min-size failure should be reported: {error}"
    );
    assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
    Ok(())
}

#[tokio::test]
async fn entry_order_rejects_size_that_fees_can_reduce_below_protectable_minimum() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = test_runner(strategy_id);
    runner.quantity = dec!("0.0001");
    runner.max_quote_notional = None;
    let client = MockOkxClient::default();

    runner.initialize(&client).await?;
    let error = runner
        .evaluate_entry(&client)
        .await
        .expect_err("fee-adjusted quantity below minSz should fail closed");

    assert!(
        error.to_string().contains("can deliver only 0"),
        "unprotectable net entry size should be explicit: {error}"
    );
    assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
    Ok(())
}

#[tokio::test]
async fn entry_order_without_quote_cap_uses_configured_quantity() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = test_runner(strategy_id);
    runner.max_quote_notional = None;
    let client = MockOkxClient {
        ticker: ticker_with_bid_last("600000", "1"),
        ..MockOkxClient::default()
    };

    runner.initialize(&client).await?;
    runner.evaluate_entry(&client).await?;

    assert_eq!(
        client.placed_orders(),
        vec![PlacedOrder {
            inst_id: "BTC-USDT".to_owned(),
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: "0.001".to_owned(),
            price: Some(decimal_to_okx(quantize_decimal_down(
                runner
                    .signal
                    .entry_price_from_bid(dec!("600000"))?
                    .expect("entry price should be available"),
                instrument().tick_size()?,
            )?)),
            purpose: Some(OrderPurpose::Entry),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn entry_order_fails_closed_when_limit_amount_exceeds_okx_bound() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = test_runner(strategy_id);
    runner.max_quote_notional = None;
    let client = MockOkxClient {
        ticker: ticker_with_bid_last("600000", "1"),
        ..MockOkxClient::default()
    };

    runner.initialize(&client).await?;
    runner.exchange_mut()?.instrument.max_limit_amount = "500".to_owned();
    let error = runner
        .evaluate_entry(&client)
        .await
        .expect_err("entry above OKX maxLmtAmt should fail before submission");

    assert!(
        error.to_string().contains("maxLmtAmt"),
        "entry notional above OKX limit amount should report maxLmtAmt: {error}"
    );
    assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
    Ok(())
}

#[tokio::test]
async fn entry_conversion_failure_prevents_order_submission() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = test_runner(strategy_id);
    let client = MockOkxClient {
        fail_quote_usd_rate: true,
        ..MockOkxClient::default()
    };

    runner.initialize(&client).await?;
    let error = runner
        .evaluate_entry(&client)
        .await
        .expect_err("missing conversion evidence must prevent place");

    assert!(
        error
            .to_string()
            .contains("mock quote-to-USD evidence unavailable"),
        "conversion failure should remain explicit: {error}"
    );
    assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
    assert!(
        !client
            .calls()
            .contains(&MockOkxCall::PlaceOrder(Some(OrderPurpose::Entry)))
    );
    Ok(())
}
