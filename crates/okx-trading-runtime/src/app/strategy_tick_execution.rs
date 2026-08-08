//! Runs bounded strategy ticks and mandatory REST reconciliation after an
//! interrupted tick without owning tasks or strategy state.

use std::time::Duration;

use anyhow::{Result, bail};
use tokio::time;
use tracing::warn;

use super::strategy_tick_failure::{StrategyTickFailureTracker, record_strategy_tick_failure};
use crate::{
    okx::trading_client::OkxTradingClient,
    strategies::okx_ema_atr_maker_trend::OkxEmaAtrMakerTrendRunner,
};

const STRATEGY_TICK_TIMEOUT_EVENT: &str = "strategy_tick_timeout";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StrategyDispatch<'a> {
    ConfirmedCandle { instrument_id: &'a str },
    PrivateEvent { instrument_id: Option<&'a str> },
    InstrumentUpdated { instrument_id: &'a str },
    ReconcileTimer,
    StreamStateChanged,
}

#[cfg(test)]
pub(super) async fn execute_strategy_ticks(
    strategies: &mut [OkxEmaAtrMakerTrendRunner],
    client: &OkxTradingClient,
    tick_timeout_ms: u64,
    tick_failures: &mut StrategyTickFailureTracker,
) -> Result<Option<anyhow::Error>> {
    let tick_timeout = Duration::from_millis(tick_timeout_ms);
    for (strategy_index, strategy) in strategies.iter_mut().enumerate() {
        let result = match time::timeout(tick_timeout, strategy.tick(client)).await {
            Ok(Ok(())) => handle_successful_strategy_tick(tick_failures, strategy_index),
            Ok(Err(error)) => handle_strategy_tick_error(tick_failures, strategy_index, error),
            Err(_) => {
                handle_strategy_tick_timeout(
                    client,
                    tick_timeout,
                    tick_timeout_ms,
                    strategy_index,
                    strategy,
                    tick_failures,
                )
                .await
            }
        }?;
        if result.is_some() {
            return Ok(result);
        }
    }
    Ok(None)
}

pub(super) async fn execute_strategy_dispatch(
    strategies: &mut [OkxEmaAtrMakerTrendRunner],
    client: &OkxTradingClient,
    tick_timeout_ms: u64,
    tick_failures: &mut StrategyTickFailureTracker,
    dispatch: StrategyDispatch<'_>,
) -> Result<Option<anyhow::Error>> {
    let tick_timeout = Duration::from_millis(tick_timeout_ms);
    for (strategy_index, strategy) in strategies.iter_mut().enumerate() {
        if !dispatch_matches_strategy(dispatch, strategy) {
            continue;
        }
        if let Some(error) = tick_strategy_with_timeout(
            client,
            tick_timeout,
            tick_timeout_ms,
            strategy_index,
            strategy,
            tick_failures,
            dispatch,
        )
        .await?
        {
            return Ok(Some(error));
        }
    }

    Ok(None)
}

fn dispatch_matches_strategy(
    dispatch: StrategyDispatch<'_>,
    strategy: &OkxEmaAtrMakerTrendRunner,
) -> bool {
    match dispatch {
        StrategyDispatch::ConfirmedCandle { instrument_id }
        | StrategyDispatch::InstrumentUpdated { instrument_id } => {
            strategy.instrument_id() == instrument_id
        }
        StrategyDispatch::PrivateEvent {
            instrument_id: Some(instrument_id),
        } => strategy.instrument_id() == instrument_id,
        StrategyDispatch::PrivateEvent {
            instrument_id: None,
        }
        | StrategyDispatch::ReconcileTimer
        | StrategyDispatch::StreamStateChanged => true,
    }
}

async fn tick_strategy_with_timeout(
    client: &OkxTradingClient,
    tick_timeout: Duration,
    tick_timeout_ms: u64,
    strategy_index: usize,
    strategy: &mut OkxEmaAtrMakerTrendRunner,
    tick_failures: &mut StrategyTickFailureTracker,
    dispatch: StrategyDispatch<'_>,
) -> Result<Option<anyhow::Error>> {
    let work = async {
        match dispatch {
            StrategyDispatch::ConfirmedCandle { .. } => strategy.on_confirmed_candle(client).await,
            StrategyDispatch::PrivateEvent { .. } => strategy.on_private_event(client).await,
            StrategyDispatch::InstrumentUpdated { .. } => {
                strategy.on_instrument_update(client).await
            }
            StrategyDispatch::ReconcileTimer => strategy.on_reconcile_timer(client).await,
            StrategyDispatch::StreamStateChanged => {
                strategy.reconcile_after_interrupted_tick(client).await
            }
        }
    };
    match time::timeout(tick_timeout, work).await {
        Ok(Ok(())) => handle_successful_strategy_tick(tick_failures, strategy_index),
        Ok(Err(error)) => handle_strategy_tick_error(tick_failures, strategy_index, error),
        Err(_) => {
            handle_strategy_tick_timeout(
                client,
                tick_timeout,
                tick_timeout_ms,
                strategy_index,
                strategy,
                tick_failures,
            )
            .await
        }
    }
}

fn handle_successful_strategy_tick(
    tick_failures: &mut StrategyTickFailureTracker,
    strategy_index: usize,
) -> Result<Option<anyhow::Error>> {
    tick_failures.record_success(strategy_index)?;
    Ok(None)
}

fn handle_strategy_tick_error(
    tick_failures: &mut StrategyTickFailureTracker,
    strategy_index: usize,
    error: anyhow::Error,
) -> Result<Option<anyhow::Error>> {
    record_strategy_tick_failure(tick_failures, strategy_index, error)
}

async fn handle_strategy_tick_timeout(
    client: &OkxTradingClient,
    tick_timeout: Duration,
    tick_timeout_ms: u64,
    strategy_index: usize,
    strategy: &mut OkxEmaAtrMakerTrendRunner,
    tick_failures: &mut StrategyTickFailureTracker,
) -> Result<Option<anyhow::Error>> {
    // Dropping the tick future cancels in-flight reqwest/tokio work at the
    // runtime boundary. Reconcile through REST before allowing a later tick
    // to make dependent order decisions from potentially stale local state.
    warn!(
        safety_event = STRATEGY_TICK_TIMEOUT_EVENT,
        strategy_index,
        tick_timeout_ms,
        "strategy tick exceeded runtime timeout; reconciling through REST"
    );
    if let Some(error) = record_strategy_tick_failure(
        tick_failures,
        strategy_index,
        anyhow::anyhow!(
            "{STRATEGY_TICK_TIMEOUT_EVENT}: strategy index {strategy_index} exceeded runtime.tick_timeout_ms {tick_timeout_ms}"
        ),
    )? {
        return Ok(Some(error));
    }
    reconcile_after_interrupted_strategy_tick(
        client,
        tick_timeout,
        tick_timeout_ms,
        strategy_index,
        strategy,
    )
    .await?;
    Ok(None)
}

async fn reconcile_after_interrupted_strategy_tick(
    client: &OkxTradingClient,
    tick_timeout: Duration,
    tick_timeout_ms: u64,
    strategy_index: usize,
    strategy: &mut OkxEmaAtrMakerTrendRunner,
) -> Result<()> {
    match time::timeout(
        tick_timeout,
        strategy.reconcile_after_interrupted_tick(client),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            bail!(
                "strategy index {strategy_index} exceeded runtime.tick_timeout_ms {tick_timeout_ms} and REST reconciliation failed: {error}"
            );
        }
        Err(_) => {
            bail!(
                "strategy index {strategy_index} exceeded runtime.tick_timeout_ms {tick_timeout_ms} and bounded REST reconciliation also timed out"
            );
        }
    }
}

#[cfg(test)]
#[path = "strategy_tick_execution_tests.rs"]
mod tests;
