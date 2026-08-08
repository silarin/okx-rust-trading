use anyhow::Result;
use pretty_assertions::assert_eq;
use rust_decimal::Decimal;

use super::smoke_amended_price;

#[test]
fn amended_price_moves_down_by_one_exact_tick() -> Result<()> {
    assert_eq!(
        smoke_amended_price(Decimal::new(995_001, 1), Decimal::new(1, 1))?,
        Decimal::new(99_500, 0)
    );
    Ok(())
}
