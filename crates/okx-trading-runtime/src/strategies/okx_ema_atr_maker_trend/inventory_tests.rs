use anyhow::Result;
use pretty_assertions::assert_eq;
use rust_decimal::Decimal;

use super::{StrategyBalance, strategy_balance_after_operator_baseline};

fn decimal(value: &str) -> Decimal {
    value.parse().expect("decimal fixture should parse")
}

#[test]
fn subtracts_operator_baseline_before_quantizing_strategy_balance() -> Result<()> {
    assert_eq!(
        strategy_balance_after_operator_baseline(
            decimal("1.0019995"),
            decimal("1"),
            decimal("0.0001"),
            decimal("0.0001"),
            "BTC",
        )?,
        StrategyBalance {
            total: decimal("0.0019995"),
            tradeable_quantity: decimal("0.0019"),
        }
    );
    Ok(())
}

#[test]
fn preserves_sub_lot_residual_without_strategy_inventory() -> Result<()> {
    assert_eq!(
        strategy_balance_after_operator_baseline(
            decimal("1.00000000978"),
            decimal("1"),
            decimal("0.00000001"),
            decimal("0.00001"),
            "BTC",
        )?,
        StrategyBalance {
            total: decimal("0.00000000978"),
            tradeable_quantity: decimal("0"),
        }
    );
    Ok(())
}

#[test]
fn rejects_account_balance_below_operator_baseline() {
    let error = strategy_balance_after_operator_baseline(
        decimal("0.9999"),
        decimal("1"),
        decimal("0.0001"),
        decimal("0.0001"),
        "BTC",
    )
    .expect_err("operator-owned inventory must never be consumed");

    assert!(
        error
            .to_string()
            .contains("below configured operator-owned base balance")
    );
}

#[test]
fn rejects_negative_operator_baseline() {
    let error = strategy_balance_after_operator_baseline(
        decimal("1"),
        decimal("-1"),
        decimal("0.0001"),
        decimal("0.0001"),
        "BTC",
    )
    .expect_err("negative operator baseline must fail closed");

    assert!(error.to_string().contains("must be non-negative"));
}
