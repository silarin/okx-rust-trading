use pretty_assertions::assert_eq;
use rust_decimal::Decimal;

use super::{CurrencyBalance, base_delta_from_baseline, validate_unfrozen_base_baseline};

fn balance(available: Decimal, total: Decimal, frozen: Decimal) -> CurrencyBalance {
    CurrencyBalance {
        available,
        total,
        frozen,
    }
}

#[test]
fn seeded_demo_balance_is_excluded_from_acquired_base() {
    let baseline = balance(Decimal::ONE, Decimal::ONE, Decimal::ZERO);
    let acquired = Decimal::new(1, 4);
    let current = balance(
        baseline.available + acquired,
        baseline.total + acquired,
        Decimal::ZERO,
    );

    assert_eq!(
        base_delta_from_baseline(baseline, current, "ETH")
            .expect("only the increase above the seeded balance should be acquired"),
        acquired
    );
}

#[test]
fn zero_balance_baseline_remains_supported() {
    let baseline = CurrencyBalance::default();
    let acquired = Decimal::new(2, 4);
    let current = balance(acquired, acquired, Decimal::ZERO);

    assert_eq!(
        base_delta_from_baseline(baseline, current, "ETH")
            .expect("an isolated zero balance should still be supported"),
        acquired
    );
}

#[test]
fn preflight_rejects_frozen_or_inconsistent_baseline() {
    let frozen = balance(Decimal::new(9, 1), Decimal::ONE, Decimal::new(1, 1));
    let inconsistent = balance(Decimal::new(9, 1), Decimal::ONE, Decimal::ZERO);

    assert!(validate_unfrozen_base_baseline(frozen, "ETH").is_err());
    assert!(validate_unfrozen_base_baseline(inconsistent, "ETH").is_err());
}

#[test]
fn reconciliation_rejects_any_seeded_balance_decrease() {
    let baseline = balance(Decimal::ONE, Decimal::ONE, Decimal::ZERO);
    let decreased = balance(Decimal::new(9999, 4), Decimal::new(9999, 4), Decimal::ZERO);

    let error = base_delta_from_baseline(baseline, decreased, "ETH")
        .expect_err("the smoke must never sell or claim seeded ETH");

    assert!(error.to_string().contains("refuses to consume"));
}

#[test]
fn reconciliation_rejects_frozen_or_mismatched_deltas() {
    let baseline = balance(Decimal::ONE, Decimal::ONE, Decimal::ZERO);
    let frozen = balance(
        Decimal::ONE,
        Decimal::ONE + Decimal::new(1, 4),
        Decimal::new(1, 4),
    );
    let mismatched = balance(
        Decimal::ONE + Decimal::new(1, 4),
        Decimal::ONE + Decimal::new(2, 4),
        Decimal::ZERO,
    );

    assert!(base_delta_from_baseline(baseline, frozen, "ETH").is_err());
    assert!(base_delta_from_baseline(baseline, mismatched, "ETH").is_err());
}
