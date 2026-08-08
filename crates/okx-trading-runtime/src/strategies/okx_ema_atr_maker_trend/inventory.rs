use anyhow::{Result, ensure};
use rust_decimal::Decimal;

use crate::okx::types::quantize_decimal_down;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct StrategyBalance {
    pub(super) total: Decimal,
    pub(super) tradeable_quantity: Decimal,
}

pub(super) fn strategy_balance_after_operator_baseline(
    account_total: Decimal,
    operator_owned_base_balance: Decimal,
    lot_size: Decimal,
    min_size: Decimal,
    base_currency: &str,
) -> Result<StrategyBalance> {
    ensure!(
        operator_owned_base_balance >= Decimal::ZERO,
        "configured operator-owned {base_currency} balance must be non-negative"
    );
    ensure!(
        account_total >= operator_owned_base_balance,
        "OKX {base_currency} cash balance {account_total} is below configured operator-owned base balance {operator_owned_base_balance}; refusing to consume operator-owned inventory"
    );
    let total = account_total - operator_owned_base_balance;
    if total <= Decimal::ZERO || total < min_size {
        return Ok(StrategyBalance {
            total,
            tradeable_quantity: Decimal::ZERO,
        });
    }
    Ok(StrategyBalance {
        total,
        tradeable_quantity: quantize_decimal_down(total, lot_size)?,
    })
}

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;
