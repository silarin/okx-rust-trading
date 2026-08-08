use super::MAX_CONSECUTIVE_STRATEGY_TICK_FAILURES;
use super::StrategyTickFailure;
use super::StrategyTickFailureTracker;
use super::record_strategy_tick_failure;
use anyhow::Result;
use pretty_assertions::assert_eq;

#[test]
fn tick_failure_tracker_stops_at_threshold() -> Result<()> {
    let mut tracker = StrategyTickFailureTracker::new(/*strategy_count*/ 1);

    assert_eq!(
        tracker.record_failure(/*strategy_index*/ 0)?,
        StrategyTickFailure::Retry {
            consecutive_failures: 1,
        }
    );
    assert_eq!(
        tracker.record_failure(/*strategy_index*/ 0)?,
        StrategyTickFailure::Retry {
            consecutive_failures: 2,
        }
    );
    assert_eq!(
        tracker.record_failure(/*strategy_index*/ 0)?,
        StrategyTickFailure::Stop {
            consecutive_failures: MAX_CONSECUTIVE_STRATEGY_TICK_FAILURES,
        }
    );
    Ok(())
}

#[test]
fn tick_failure_tracker_resets_after_success() -> Result<()> {
    let mut tracker = StrategyTickFailureTracker::new(/*strategy_count*/ 1);

    assert_eq!(
        tracker.record_failure(/*strategy_index*/ 0)?,
        StrategyTickFailure::Retry {
            consecutive_failures: 1,
        }
    );
    tracker.record_success(/*strategy_index*/ 0)?;

    assert_eq!(
        tracker.record_failure(/*strategy_index*/ 0)?,
        StrategyTickFailure::Retry {
            consecutive_failures: 1,
        }
    );
    Ok(())
}

#[test]
fn tick_failure_tracker_tracks_strategy_indexes_independently() -> Result<()> {
    let mut tracker = StrategyTickFailureTracker::new(/*strategy_count*/ 2);

    assert_eq!(
        tracker.record_failure(/*strategy_index*/ 0)?,
        StrategyTickFailure::Retry {
            consecutive_failures: 1,
        }
    );
    assert_eq!(
        tracker.record_failure(/*strategy_index*/ 1)?,
        StrategyTickFailure::Retry {
            consecutive_failures: 1,
        }
    );
    assert_eq!(
        tracker.record_failure(/*strategy_index*/ 0)?,
        StrategyTickFailure::Retry {
            consecutive_failures: 2,
        }
    );
    Ok(())
}

#[test]
fn trading_safety_matrix_strategy_tick_timeout_feeds_fail_closed_threshold() -> Result<()> {
    let mut tracker = StrategyTickFailureTracker::new(/*strategy_count*/ 1);

    assert!(
        record_strategy_tick_failure(
            &mut tracker,
            /*strategy_index*/ 0,
            anyhow::anyhow!("strategy index 0 exceeded runtime.tick_timeout_ms 5000"),
        )?
        .is_none()
    );
    assert!(
        record_strategy_tick_failure(
            &mut tracker,
            /*strategy_index*/ 0,
            anyhow::anyhow!("strategy index 0 exceeded runtime.tick_timeout_ms 5000"),
        )?
        .is_none()
    );
    let error = record_strategy_tick_failure(
        &mut tracker,
        /*strategy_index*/ 0,
        anyhow::anyhow!("strategy index 0 exceeded runtime.tick_timeout_ms 5000"),
    )?
    .expect("third consecutive timeout should stop runtime");

    assert!(
        error
            .to_string()
            .contains("failed 3 consecutive ticks; stopping runtime"),
        "tick timeout should stop through existing failure threshold: {error}"
    );
    Ok(())
}

#[test]
fn tick_failure_tracker_rejects_untracked_strategy_index() {
    let mut tracker = StrategyTickFailureTracker::new(/*strategy_count*/ 1);

    let error = tracker
        .record_failure(/*strategy_index*/ 1)
        .expect_err("untracked strategy index should fail closed");
    assert!(
        error
            .to_string()
            .contains("strategy index 1 is not tracked"),
        "untracked strategy index should be reported clearly: {error}"
    );

    let error = tracker
        .record_success(/*strategy_index*/ 1)
        .expect_err("untracked strategy index should fail closed");
    assert!(
        error
            .to_string()
            .contains("strategy index 1 is not tracked"),
        "untracked strategy index should be reported clearly: {error}"
    );
}
