use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn shutdown_cancels_live_entry_when_no_position_exists() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = runner_with_live_entry(strategy_id);
    let client = MockOkxClient::default();

    runner.shutdown(&client).await?;

    assert_eq!(client.canceled_orders(), vec![entry_id(strategy_id)]);
    assert_eq!(runner.exchange()?.entry_order, None);
    Ok(())
}

#[tokio::test]
async fn shutdown_does_not_cancel_unrelated_orders() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = runner_with_empty_exchange(strategy_id);
    let client = MockOkxClient {
        open_orders: vec![order(OrderFixture {
            client_order_id: "external-order",
            side: OrderSide::Buy,
            kind: OrderKind::Limit,
            state: "live",
            size: "0.001",
            accumulated_fill_size: "0",
            average_price: "",
            updated_at_ms: "10",
        })],
        ..MockOkxClient::default()
    };

    runner.shutdown(&client).await?;

    assert_eq!(client.canceled_orders(), Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn shutdown_fails_closed_when_entry_cancel_remains_live() {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = runner_with_live_entry(strategy_id);
    let client = MockOkxClient {
        open_orders: vec![order(OrderFixture {
            client_order_id: &entry_id(strategy_id),
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            state: "live",
            size: "0.001",
            accumulated_fill_size: "0",
            average_price: "",
            updated_at_ms: "10",
        })],
        ..MockOkxClient::default()
    };

    let error = runner
        .shutdown(&client)
        .await
        .expect_err("live order after cancel should make shutdown ambiguous");

    assert!(
        error
            .to_string()
            .contains("shutdown left live OKX entry order"),
        "ambiguous live entry should fail closed: {error}"
    );
    assert_eq!(client.canceled_orders(), vec![entry_id(strategy_id)]);
}

#[tokio::test]
async fn shutdown_preserves_protection_for_open_position() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
    seed_signal(&mut runner);
    let take_profit_price = decimal_to_okx(quantize_decimal_up(
        runner.take_profit_price(dec!("110"))?,
        instrument().tick_size()?,
    )?);
    let client = MockOkxClient {
        open_orders: vec![order_with_price(
            OrderFixture {
                client_order_id: &take_profit_id(strategy_id),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "10",
            },
            &take_profit_price,
        )],
        open_algo_orders: vec![stop_loss_algo(strategy_id, "live")],
        ..MockOkxClient::default()
    };

    runner.shutdown(&client).await?;

    assert_eq!(
        client.calls(),
        vec![
            MockOkxCall::OrderLookup(Some(OrderPurpose::TakeProfit)),
            MockOkxCall::OpenAlgoOrders,
            MockOkxCall::OrderLookup(Some(OrderPurpose::TakeProfit)),
            MockOkxCall::OpenAlgoOrders,
            MockOkxCall::Ticker,
        ]
    );
    let state = runner.exchange()?;
    assert!(state.position.is_some());
    assert!(state.take_profit_order.is_some());
    assert!(state.stop_loss_order.is_some());
    assert_eq!(client.canceled_orders(), Vec::<String>::new());
    assert_eq!(client.canceled_algo_orders(), Vec::<String>::new());
    assert!(
        client.calls().iter().all(|call| !matches!(
            call,
            MockOkxCall::PlaceOrder(_)
                | MockOkxCall::PlaceTriggerOrder(_)
                | MockOkxCall::CancelOrder(_)
                | MockOkxCall::CancelAlgoOrder
                | MockOkxCall::AmendOrder
        )),
        "shutdown should preserve existing protection without order churn: {:?}",
        client.calls()
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_processes_terminal_entry_fill_before_clearing_tracking() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = runner_with_live_entry(strategy_id);
    let client = MockOkxClient {
        order_history: vec![order(OrderFixture {
            client_order_id: &entry_id(strategy_id),
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            state: "filled",
            size: "0.001",
            accumulated_fill_size: "0.001",
            average_price: "100",
            updated_at_ms: "10",
        })],
        ..MockOkxClient::default()
    };

    runner.shutdown(&client).await?;

    let state = runner.exchange()?;
    assert_eq!(state.entry_order, None);
    assert_eq!(
        state.position,
        Some(OpenPosition {
            quantity: dec!("0.001"),
            average_price: dec!("100"),
            stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
        })
    );
    assert!(state.take_profit_order.is_some());
    assert!(state.stop_loss_order.is_some());
    assert_eq!(client.canceled_orders(), Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn shutdown_pending_stop_loss_fails_while_take_profit_is_still_live() {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = runner_with_pending_stop_and_take_profit(strategy_id);
    seed_signal(&mut runner);
    let take_profit_price = decimal_to_okx(
        quantize_decimal_up(
            runner
                .take_profit_price(dec!("110"))
                .expect("take-profit price should be calculable"),
            instrument().tick_size().expect("tick size should parse"),
        )
        .expect("take-profit price should quantize"),
    );
    let client = MockOkxClient {
        open_orders: vec![order_with_price(
            OrderFixture {
                client_order_id: &take_profit_id(strategy_id),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.0006",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "10",
            },
            &take_profit_price,
        )],
        ..MockOkxClient::default()
    };

    let error = runner
        .shutdown(&client)
        .await
        .expect_err("live take-profit should block stop-loss market exit submission");

    assert!(
        error.to_string().contains("take-profit order") && error.to_string().contains("still live"),
        "pending stop-loss shutdown should fail while take-profit is live: {error}"
    );
    assert_eq!(client.canceled_orders(), vec![take_profit_id(strategy_id)]);
    assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
}

#[tokio::test]
async fn shutdown_pending_stop_loss_submits_market_exit_protection() -> Result<()> {
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = runner_with_pending_stop(strategy_id);
    let client = MockOkxClient {
        balances: vec![balance("BTC", "0.0006")],
        ..MockOkxClient::default()
    };

    runner.shutdown(&client).await?;

    let state = runner.exchange()?;
    assert!(state.stop_loss_exit_order.is_some());
    assert_eq!(
        client.placed_orders(),
        vec![PlacedOrder {
            inst_id: "BTC-USDT".to_owned(),
            side: OrderSide::Sell,
            kind: OrderKind::Market,
            size: "0.0006".to_owned(),
            price: None,
            purpose: Some(OrderPurpose::StopLoss),
        }]
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_pending_stop_loss_submits_exit_after_take_profit_cancel_reconciles() -> Result<()>
{
    let strategy_id = "okx-ema-atr-maker-btc-usdt";
    let mut runner = runner_with_pending_stop_and_take_profit(strategy_id);
    let client = MockOkxClient {
        balances: vec![balance("BTC", "0.0006")],
        order_history: vec![order(OrderFixture {
            client_order_id: &take_profit_id(strategy_id),
            side: OrderSide::Sell,
            kind: OrderKind::Limit,
            state: "canceled",
            size: "0.0006",
            accumulated_fill_size: "0",
            average_price: "",
            updated_at_ms: "10",
        })],
        ..MockOkxClient::default()
    };

    runner.shutdown(&client).await?;

    let state = runner.exchange()?;
    assert_eq!(state.take_profit_order, None);
    assert!(state.stop_loss_exit_order.is_some());
    assert_eq!(
        client.placed_orders(),
        vec![PlacedOrder {
            inst_id: "BTC-USDT".to_owned(),
            side: OrderSide::Sell,
            kind: OrderKind::Market,
            size: "0.0006".to_owned(),
            price: None,
            purpose: Some(OrderPurpose::StopLoss),
        }]
    );
    Ok(())
}
