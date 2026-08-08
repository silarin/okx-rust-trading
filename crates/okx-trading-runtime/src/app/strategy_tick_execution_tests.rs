use anyhow::Result;
use pretty_assertions::assert_eq;

use super::handle_successful_strategy_tick;
use crate::app::strategy_tick_failure::{
    StrategyTickFailure, StrategyTickFailureTracker, record_strategy_tick_failure,
};

#[test]
fn successful_strategy_tick_handling_resets_failure_tracker() -> Result<()> {
    let mut tracker = StrategyTickFailureTracker::new(/*strategy_count*/ 1);
    assert!(
        record_strategy_tick_failure(
            &mut tracker,
            /*strategy_index*/ 0,
            anyhow::anyhow!("first tick failed"),
        )?
        .is_none()
    );

    assert!(handle_successful_strategy_tick(&mut tracker, /*strategy_index*/ 0)?.is_none());

    assert_eq!(
        tracker.record_failure(/*strategy_index*/ 0)?,
        StrategyTickFailure::Retry {
            consecutive_failures: 1,
        }
    );
    Ok(())
}
