//! Owns the per-strategy consecutive tick-failure policy that routes persistent
//! runtime errors through the fatal fail-closed path.

use anyhow::{Result, bail};
use tracing::warn;

const MAX_CONSECUTIVE_STRATEGY_TICK_FAILURES: usize = 3;

#[derive(Debug, PartialEq)]
pub(super) enum StrategyTickFailure {
    Retry { consecutive_failures: usize },
    Stop { consecutive_failures: usize },
}

#[derive(Debug)]
pub(super) struct StrategyTickFailureTracker {
    consecutive_failures: Vec<usize>,
}

impl StrategyTickFailureTracker {
    pub(super) fn new(strategy_count: usize) -> Self {
        Self {
            consecutive_failures: vec![0; strategy_count],
        }
    }

    pub(super) fn record_success(&mut self, strategy_index: usize) -> Result<()> {
        let Some(consecutive_failures) = self.consecutive_failures.get_mut(strategy_index) else {
            bail!(
                "strategy index {strategy_index} is not tracked by the tick failure tracker; stopping runtime"
            );
        };
        *consecutive_failures = 0;
        Ok(())
    }

    pub(super) fn record_failure(&mut self, strategy_index: usize) -> Result<StrategyTickFailure> {
        let Some(consecutive_failures) = self.consecutive_failures.get_mut(strategy_index) else {
            bail!(
                "strategy index {strategy_index} is not tracked by the tick failure tracker; stopping runtime"
            );
        };
        *consecutive_failures += 1;
        if *consecutive_failures >= MAX_CONSECUTIVE_STRATEGY_TICK_FAILURES {
            Ok(StrategyTickFailure::Stop {
                consecutive_failures: *consecutive_failures,
            })
        } else {
            Ok(StrategyTickFailure::Retry {
                consecutive_failures: *consecutive_failures,
            })
        }
    }

    pub(super) fn has_failures(&self) -> bool {
        self.consecutive_failures
            .iter()
            .any(|consecutive_failures| *consecutive_failures > 0)
    }
}

pub(super) fn record_strategy_tick_failure(
    tick_failures: &mut StrategyTickFailureTracker,
    strategy_index: usize,
    err: anyhow::Error,
) -> Result<Option<anyhow::Error>> {
    match tick_failures.record_failure(strategy_index)? {
        StrategyTickFailure::Retry {
            consecutive_failures,
        } => {
            warn!(
                safety_event = "strategy_tick_failure",
                strategy_index,
                consecutive_failures,
                max_consecutive_failures = MAX_CONSECUTIVE_STRATEGY_TICK_FAILURES,
                error = %err,
                "strategy tick failed"
            );
            Ok(None)
        }
        StrategyTickFailure::Stop {
            consecutive_failures,
        } => {
            warn!(
                safety_event = "strategy_tick_failure_threshold",
                strategy_index,
                consecutive_failures,
                max_consecutive_failures = MAX_CONSECUTIVE_STRATEGY_TICK_FAILURES,
                error = %err,
                "strategy tick failure threshold reached; routing runtime through fatal exit policy"
            );
            Ok(Some(anyhow::anyhow!(
                "strategy index {strategy_index} failed {consecutive_failures} consecutive ticks; stopping runtime: {err}"
            )))
        }
    }
}

#[cfg(test)]
#[path = "strategy_tick_failure_tests.rs"]
mod tests;
