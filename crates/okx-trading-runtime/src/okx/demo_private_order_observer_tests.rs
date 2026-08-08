use pretty_assertions::assert_eq;

use super::*;

fn order(state: &str, price: &str) -> OkxOrder {
    OkxOrder {
        inst_type: "SPOT".to_owned(),
        inst_id: "ETH-USDT".to_owned(),
        order_id: "order-1".to_owned(),
        client_order_id: "client-1".to_owned(),
        side: "buy".to_owned(),
        order_type: "post_only".to_owned(),
        price: price.to_owned(),
        state: state.to_owned(),
        average_price: String::new(),
        accumulated_fill_size: "0".to_owned(),
        fee: "0".to_owned(),
        fee_currency: "ETH".to_owned(),
        rebate: "0".to_owned(),
        rebate_currency: "USDT".to_owned(),
        sz: "0.00001".to_owned(),
        created_at_ms: "1710000000000".to_owned(),
        updated_at_ms: "1710000000001".to_owned(),
    }
}

fn expectation(
    state: ExpectedPrivateOrderState,
    price: &'static str,
) -> PrivateOrderExpectation<'static> {
    PrivateOrderExpectation {
        stage: "test",
        instrument_id: "ETH-USDT",
        order_id: "order-1",
        client_order_id: "client-1",
        price,
        size: "0.00001",
        state,
        command_started_at: Instant::now(),
        timeout: PRIVATE_EVENT_TIMEOUT,
    }
}

#[test]
fn validates_correlated_live_amended_and_canceled_order_shapes() -> Result<()> {
    for (state, price, observed) in [
        (ExpectedPrivateOrderState::Live, "100", order("live", "100")),
        (
            ExpectedPrivateOrderState::Live,
            "99.9",
            order("live", "99.9"),
        ),
        (
            ExpectedPrivateOrderState::Canceled,
            "99.9",
            order("canceled", "99.9"),
        ),
    ] {
        validate_private_order(expectation(state, price), &observed)?;
    }
    Ok(())
}

#[test]
fn rejects_uncorrelated_or_filled_private_order_shapes() {
    let mut mismatched_id = order("live", "100");
    mismatched_id.order_id = "other-order".to_owned();
    let mut filled = order("live", "100");
    filled.state = "partially_filled".to_owned();
    filled.accumulated_fill_size = "0.00001".to_owned();
    filled.average_price = "100".to_owned();

    let errors = [mismatched_id, filled]
        .iter()
        .map(|observed| {
            validate_private_order(
                expectation(ExpectedPrivateOrderState::Live, "100"),
                observed,
            )
            .expect_err("unsafe private order shape must be rejected")
            .to_string()
        })
        .collect::<Vec<_>>();

    assert_eq!(errors.len(), 2);
    assert!(errors[0].contains("state live"));
    assert!(errors[1].contains("state partially_filled"));
}
