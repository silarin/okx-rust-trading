//! Owns the independent OKX Cancel-All-After refresh task and its bounded
//! failure signal.

use std::{future::Future, future::pending, time::Duration};

use anyhow::{Context, Result};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tracing::info;

use crate::okx::{client::OkxCancelAllAfterTimeout, trading_client::OkxCancelAllAfterClient};

const CANCEL_ALL_AFTER_REFRESH_MULTIPLIER: u64 = 3;
const MILLIS_PER_SECOND: u64 = 1_000;
pub(super) const MAX_CANCEL_ALL_AFTER_POLL_INTERVAL_MS: u64 =
    OkxCancelAllAfterTimeout::MAX_SECONDS / CANCEL_ALL_AFTER_REFRESH_MULTIPLIER * MILLIS_PER_SECOND;

pub(super) async fn next_cancel_all_after_heartbeat_failure(
    failures: &mut Option<mpsc::Receiver<anyhow::Error>>,
) -> Option<anyhow::Error> {
    match failures {
        Some(failures) => failures.recv().await,
        None => pending().await,
    }
}

pub(super) struct CancelAllAfterHeartbeat {
    pub(super) stop: Option<oneshot::Sender<()>>,
    pub(super) handle: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy)]
struct CancelAllAfterHeartbeatTiming {
    period: Duration,
    refresh_deadline: Duration,
}

enum CancelAllAfterHeartbeatRefresh {
    Refreshed,
    Failed(anyhow::Error),
    Stopped,
}

pub(super) struct CancelAllAfterHeartbeatAck {
    trigger_time: String,
    timestamp: String,
}

pub(super) trait CancelAllAfterHeartbeatSource: Clone + Send + Sync + 'static {
    fn refresh_cancel_all_after(
        &self,
        timeout: OkxCancelAllAfterTimeout,
    ) -> impl Future<Output = Result<CancelAllAfterHeartbeatAck>> + Send;
}

impl CancelAllAfterHeartbeatSource for OkxCancelAllAfterClient {
    async fn refresh_cancel_all_after(
        &self,
        timeout: OkxCancelAllAfterTimeout,
    ) -> Result<CancelAllAfterHeartbeatAck> {
        let acknowledgement = self.cancel_all_after(timeout).await?;
        Ok(CancelAllAfterHeartbeatAck {
            trigger_time: acknowledgement.trigger_time,
            timestamp: acknowledgement.ts,
        })
    }
}

impl CancelAllAfterHeartbeat {
    pub(super) fn spawn<C: CancelAllAfterHeartbeatSource>(
        client: C,
        timeout: OkxCancelAllAfterTimeout,
    ) -> (Self, mpsc::Receiver<anyhow::Error>) {
        Self::spawn_with_timing(
            client,
            timeout,
            cancel_all_after_heartbeat_period(timeout),
            cancel_all_after_heartbeat_refresh_deadline(timeout),
        )
    }

    pub(super) fn spawn_with_timing<C: CancelAllAfterHeartbeatSource>(
        client: C,
        timeout: OkxCancelAllAfterTimeout,
        period: Duration,
        refresh_deadline: Duration,
    ) -> (Self, mpsc::Receiver<anyhow::Error>) {
        let (failure_tx, failure_rx) = mpsc::channel(1);
        let (stop_tx, stop_rx) = oneshot::channel();
        let timing = CancelAllAfterHeartbeatTiming {
            period,
            refresh_deadline,
        };
        let handle = tokio::spawn(async move {
            run_cancel_all_after_heartbeat(client, timeout, timing, stop_rx, failure_tx).await;
        });
        (
            Self {
                stop: Some(stop_tx),
                handle: Some(handle),
            },
            failure_rx,
        )
    }

    pub(super) async fn stop(&mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle
                .await
                .context("OKX Cancel-All-After heartbeat task panicked")?;
        }
        Ok(())
    }

    pub(super) fn abort(&mut self) {
        self.stop.take();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for CancelAllAfterHeartbeat {
    fn drop(&mut self) {
        self.abort();
    }
}

async fn run_cancel_all_after_heartbeat<C: CancelAllAfterHeartbeatSource>(
    client: C,
    timeout: OkxCancelAllAfterTimeout,
    timing: CancelAllAfterHeartbeatTiming,
    mut stop_rx: oneshot::Receiver<()>,
    failure_tx: mpsc::Sender<anyhow::Error>,
) {
    let mut interval = time::interval(timing.period);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = &mut stop_rx => break,
            _ = interval.tick() => {}
        }

        match refresh_cancel_all_after_or_stop(&client, timeout, timing, &mut stop_rx).await {
            CancelAllAfterHeartbeatRefresh::Refreshed => {}
            CancelAllAfterHeartbeatRefresh::Stopped => break,
            CancelAllAfterHeartbeatRefresh::Failed(error) => {
                let _ = failure_tx.send(error).await;
                break;
            }
        }
    }
}

async fn refresh_cancel_all_after_or_stop<C: CancelAllAfterHeartbeatSource>(
    client: &C,
    timeout: OkxCancelAllAfterTimeout,
    timing: CancelAllAfterHeartbeatTiming,
    stop_rx: &mut oneshot::Receiver<()>,
) -> CancelAllAfterHeartbeatRefresh {
    match tokio::select! {
        _ = stop_rx => return CancelAllAfterHeartbeatRefresh::Stopped,
        refresh = time::timeout(
            timing.refresh_deadline,
            refresh_cancel_all_after_with_client(client, timeout),
        ) => refresh,
    } {
        Ok(Ok(())) => CancelAllAfterHeartbeatRefresh::Refreshed,
        Ok(Err(error)) => CancelAllAfterHeartbeatRefresh::Failed(error),
        Err(_) => CancelAllAfterHeartbeatRefresh::Failed(anyhow::anyhow!(
            "OKX Cancel-All-After heartbeat refresh exceeded {:?}",
            timing.refresh_deadline
        )),
    }
}

async fn refresh_cancel_all_after_with_client<C: CancelAllAfterHeartbeatSource>(
    client: &C,
    timeout: OkxCancelAllAfterTimeout,
) -> Result<()> {
    let acknowledgement = client.refresh_cancel_all_after(timeout).await?;
    info!(
        safety_event = "caa_heartbeat_refresh",
        timeout_secs = timeout.seconds(),
        trigger_time = %acknowledgement.trigger_time,
        okx_timestamp = %acknowledgement.timestamp,
        "refreshed OKX cancel-all-after dead-man switch"
    );
    Ok(())
}

pub(super) fn cancel_all_after_heartbeat_refresh_deadline(
    timeout: OkxCancelAllAfterTimeout,
) -> Duration {
    cancel_all_after_heartbeat_period(timeout)
}

pub(super) fn cancel_all_after_heartbeat_period(timeout: OkxCancelAllAfterTimeout) -> Duration {
    Duration::from_millis(
        (timeout.seconds().saturating_mul(MILLIS_PER_SECOND) / CANCEL_ALL_AFTER_REFRESH_MULTIPLIER)
            .max(1),
    )
}

pub(super) fn cancel_all_after_timeout(poll_interval_ms: u64) -> Result<OkxCancelAllAfterTimeout> {
    let poll_interval_secs = poll_interval_ms.div_ceil(MILLIS_PER_SECOND);
    let requested_timeout = poll_interval_secs
        .checked_mul(CANCEL_ALL_AFTER_REFRESH_MULTIPLIER)
        .context("runtime.poll_interval_ms is too large for OKX cancel-all-after timeout")?
        .max(OkxCancelAllAfterTimeout::MIN_SECONDS);
    OkxCancelAllAfterTimeout::new(requested_timeout).with_context(|| {
        format!(
            "runtime.poll_interval_ms {poll_interval_ms} is too large for OKX cancel-all-after refresh; maximum supported with {}x refresh margin is {} ms",
            CANCEL_ALL_AFTER_REFRESH_MULTIPLIER,
            MAX_CANCEL_ALL_AFTER_POLL_INTERVAL_MS
        )
    })
}

#[cfg(test)]
#[path = "cancel_all_after_heartbeat_tests.rs"]
mod tests;
