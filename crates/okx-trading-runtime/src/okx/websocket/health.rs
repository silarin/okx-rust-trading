use std::{
    fmt,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::{AbortHandle, JoinError, JoinHandle},
    time,
};
use tracing::warn;

#[cfg(not(test))]
const OKX_WEBSOCKET_HEALTH_CRITICAL_DELIVERY_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(test)]
const OKX_WEBSOCKET_HEALTH_CRITICAL_DELIVERY_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum OkxWebsocketStreamKind {
    Public,
    Private,
    Business,
}

impl OkxWebsocketStreamKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Private => "private",
            Self::Business => "business",
        }
    }
}

impl fmt::Display for OkxWebsocketStreamKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum OkxWebsocketChannelClass {
    PublicMarketData,
    PublicCandles,
    PrivateTrading,
    PrivateAlgoOrders,
}

impl OkxWebsocketChannelClass {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PublicMarketData => "public_market_data",
            Self::PublicCandles => "public_candles",
            Self::PrivateTrading => "private_trading",
            Self::PrivateAlgoOrders => "private_algo_orders",
        }
    }
}

impl fmt::Display for OkxWebsocketChannelClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct OkxWebsocketStreamIdentity {
    kind: OkxWebsocketStreamKind,
    channel_class: OkxWebsocketChannelClass,
    instrument_count: usize,
}

impl OkxWebsocketStreamIdentity {
    pub(crate) const fn new(
        kind: OkxWebsocketStreamKind,
        channel_class: OkxWebsocketChannelClass,
        instrument_count: usize,
    ) -> Self {
        Self {
            kind,
            channel_class,
            instrument_count,
        }
    }

    pub(crate) const fn kind(self) -> OkxWebsocketStreamKind {
        self.kind
    }

    pub(crate) const fn channel_class(self) -> OkxWebsocketChannelClass {
        self.channel_class
    }

    pub(crate) const fn instrument_count(self) -> usize {
        self.instrument_count
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OkxWebsocketHealthEventKind {
    ConnectAttempt,
    Connected,
    LoginAckSucceeded,
    LoginFailed,
    SubscriptionAckSucceeded,
    SubscriptionAckFailed,
    ReconnectScheduled,
    StreamDisconnectedAfterSubscription,
    StreamFailedBeforeSubscription,
    StreamFailedAfterSubscription,
    StreamTaskPanicked,
    StreamTaskExitedUnexpectedly,
}

impl OkxWebsocketHealthEventKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ConnectAttempt => "connect_attempt",
            Self::Connected => "connected",
            Self::LoginAckSucceeded => "login_ack_succeeded",
            Self::LoginFailed => "login_failed",
            Self::SubscriptionAckSucceeded => "subscription_ack_succeeded",
            Self::SubscriptionAckFailed => "subscription_ack_failed",
            Self::ReconnectScheduled => "reconnect_scheduled",
            Self::StreamDisconnectedAfterSubscription => "stream_disconnected_after_subscription",
            Self::StreamFailedBeforeSubscription => "stream_failed_before_subscription",
            Self::StreamFailedAfterSubscription => "stream_failed_after_subscription",
            Self::StreamTaskPanicked => "stream_task_panicked",
            Self::StreamTaskExitedUnexpectedly => "stream_task_exited_unexpectedly",
        }
    }

    pub(crate) const fn is_critical(self) -> bool {
        is_critical_websocket_health_event(self)
    }
}

impl fmt::Display for OkxWebsocketHealthEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub(crate) const fn is_critical_websocket_health_event(kind: OkxWebsocketHealthEventKind) -> bool {
    matches!(
        kind,
        OkxWebsocketHealthEventKind::LoginFailed
            | OkxWebsocketHealthEventKind::SubscriptionAckSucceeded
            | OkxWebsocketHealthEventKind::SubscriptionAckFailed
            | OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription
            | OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription
            | OkxWebsocketHealthEventKind::StreamFailedAfterSubscription
            | OkxWebsocketHealthEventKind::StreamTaskPanicked
            | OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxWebsocketHealthEvent {
    kind: OkxWebsocketHealthEventKind,
    stream: OkxWebsocketStreamIdentity,
    reconnect_attempt: Option<u64>,
    reconnect_backoff: Option<Duration>,
}

impl OkxWebsocketHealthEvent {
    pub(crate) const fn new(
        kind: OkxWebsocketHealthEventKind,
        stream: OkxWebsocketStreamIdentity,
    ) -> Self {
        Self {
            kind,
            stream,
            reconnect_attempt: None,
            reconnect_backoff: None,
        }
    }

    pub(crate) const fn reconnect_scheduled(
        stream: OkxWebsocketStreamIdentity,
        reconnect_attempt: u64,
        reconnect_backoff: Duration,
    ) -> Self {
        Self {
            kind: OkxWebsocketHealthEventKind::ReconnectScheduled,
            stream,
            reconnect_attempt: Some(reconnect_attempt),
            reconnect_backoff: Some(reconnect_backoff),
        }
    }

    pub(crate) const fn kind(&self) -> OkxWebsocketHealthEventKind {
        self.kind
    }

    pub(crate) const fn stream(&self) -> OkxWebsocketStreamIdentity {
        self.stream
    }

    pub(crate) const fn reconnect_attempt(&self) -> Option<u64> {
        self.reconnect_attempt
    }

    pub(crate) const fn reconnect_backoff(&self) -> Option<Duration> {
        self.reconnect_backoff
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OkxWebsocketHealthReporter {
    sender: mpsc::Sender<OkxWebsocketHealthEvent>,
}

pub(crate) type OkxWebsocketHealthReceiver = mpsc::Receiver<OkxWebsocketHealthEvent>;

impl OkxWebsocketHealthReporter {
    pub(crate) fn channel(capacity: usize) -> (Self, OkxWebsocketHealthReceiver) {
        let (sender, receiver) = mpsc::channel(capacity);
        (Self { sender }, receiver)
    }

    pub(crate) async fn report(&self, event: OkxWebsocketHealthEvent) {
        if event.kind().is_critical() {
            self.report_critical(event).await;
        } else {
            self.report_best_effort(event);
        }
    }

    fn report_best_effort(&self, event: OkxWebsocketHealthEvent) {
        // Reader-loop telemetry must stay cancellation-safe and must not block
        // behind a slow runtime receiver. Readiness and failure events use
        // bounded delivery through report_critical.
        match self.sender.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(event)) => {
                log_best_effort_dropped_event(event, "best_effort_channel_full");
            }
            Err(TrySendError::Closed(event)) => {
                log_best_effort_dropped_event(event, "best_effort_channel_closed");
            }
        }
    }

    async fn report_critical(&self, event: OkxWebsocketHealthEvent) {
        let delivery = time::timeout(
            OKX_WEBSOCKET_HEALTH_CRITICAL_DELIVERY_TIMEOUT,
            self.sender.send(event.clone()),
        )
        .await;
        match delivery {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log_critical_delivery_failure(error.0, "channel_closed");
            }
            Err(_) => {
                log_critical_delivery_failure(event, "channel_full_timeout");
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct OkxWebsocketTaskHandle {
    stream_abort: AbortHandle,
    supervisor_handle: JoinHandle<()>,
    intentional_shutdown: Arc<AtomicBool>,
}

impl OkxWebsocketTaskHandle {
    pub(crate) fn spawn<F>(
        stream: OkxWebsocketStreamIdentity,
        health: Option<OkxWebsocketHealthReporter>,
        future: F,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let intentional_shutdown = Arc::new(AtomicBool::new(false));
        let stream_handle = tokio::spawn(future);
        let stream_abort = stream_handle.abort_handle();
        let supervisor_shutdown = Arc::clone(&intentional_shutdown);
        let supervisor_handle = tokio::spawn(async move {
            supervise_websocket_task(stream_handle, stream, health, supervisor_shutdown).await;
        });
        Self {
            stream_abort,
            supervisor_handle,
            intentional_shutdown,
        }
    }

    pub(crate) fn abort(&mut self) {
        self.intentional_shutdown.store(true, Ordering::SeqCst);
        self.stream_abort.abort();
        self.supervisor_handle.abort();
    }
}

impl Drop for OkxWebsocketTaskHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

async fn supervise_websocket_task(
    stream_handle: JoinHandle<()>,
    stream: OkxWebsocketStreamIdentity,
    health: Option<OkxWebsocketHealthReporter>,
    intentional_shutdown: Arc<AtomicBool>,
) {
    match stream_handle.await {
        Ok(()) if intentional_shutdown.load(Ordering::SeqCst) => {}
        Ok(()) => {
            report_task_lifecycle_event(
                health,
                OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly,
                    stream,
                ),
            )
            .await;
        }
        Err(error) if intentional_shutdown.load(Ordering::SeqCst) && error.is_cancelled() => {}
        Err(error) => {
            let kind = task_lifecycle_event_kind(&error);
            report_task_lifecycle_event(health, OkxWebsocketHealthEvent::new(kind, stream)).await;
        }
    }
}

fn task_lifecycle_event_kind(error: &JoinError) -> OkxWebsocketHealthEventKind {
    if error.is_panic() {
        OkxWebsocketHealthEventKind::StreamTaskPanicked
    } else {
        OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly
    }
}

async fn report_task_lifecycle_event(
    health: Option<OkxWebsocketHealthReporter>,
    event: OkxWebsocketHealthEvent,
) {
    if let Some(health) = health {
        health.report(event).await;
    } else {
        let stream = event.stream();
        warn!(
            safety_event = "ws_stream_task_lifecycle_unreported",
            websocket_health_event = %event.kind(),
            stream_kind = %stream.kind(),
            channel_class = %stream.channel_class(),
            instrument_count = stream.instrument_count(),
            "OKX WebSocket task lifecycle failure had no health reporter"
        );
    }
}

fn log_best_effort_dropped_event(event: OkxWebsocketHealthEvent, drop_reason: &str) {
    let stream = event.stream();
    warn!(
        safety_event = "ws_health_event_dropped",
        websocket_health_event = %event.kind(),
        stream_kind = %stream.kind(),
        channel_class = %stream.channel_class(),
        instrument_count = stream.instrument_count(),
        drop_reason,
        "dropped OKX WebSocket health event from bounded channel"
    );
}

fn log_critical_delivery_failure(event: OkxWebsocketHealthEvent, delivery_failure: &str) {
    let stream = event.stream();
    warn!(
        safety_event = "ws_health_critical_event_delivery_failed",
        websocket_health_event = %event.kind(),
        stream_kind = %stream.kind(),
        channel_class = %stream.channel_class(),
        instrument_count = stream.instrument_count(),
        delivery_failure,
        delivery_timeout_ms = OKX_WEBSOCKET_HEALTH_CRITICAL_DELIVERY_TIMEOUT.as_millis(),
        "failed delivering critical OKX WebSocket health event through bounded channel"
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::{Context, Result};
    use pretty_assertions::assert_eq;
    use tokio::time;

    use super::*;
    use crate::test_support::CapturedLogs;

    fn test_stream() -> OkxWebsocketStreamIdentity {
        OkxWebsocketStreamIdentity::new(
            OkxWebsocketStreamKind::Public,
            OkxWebsocketChannelClass::PublicMarketData,
            1,
        )
    }

    fn test_event(kind: OkxWebsocketHealthEventKind) -> OkxWebsocketHealthEvent {
        OkxWebsocketHealthEvent::new(kind, test_stream())
    }

    fn critical_event_kinds() -> [OkxWebsocketHealthEventKind; 8] {
        [
            OkxWebsocketHealthEventKind::LoginFailed,
            OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
            OkxWebsocketHealthEventKind::SubscriptionAckFailed,
            OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
            OkxWebsocketHealthEventKind::StreamTaskPanicked,
            OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly,
            OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription,
            OkxWebsocketHealthEventKind::StreamFailedAfterSubscription,
        ]
    }

    fn best_effort_event_kinds() -> [OkxWebsocketHealthEventKind; 4] {
        [
            OkxWebsocketHealthEventKind::ConnectAttempt,
            OkxWebsocketHealthEventKind::Connected,
            OkxWebsocketHealthEventKind::LoginAckSucceeded,
            OkxWebsocketHealthEventKind::ReconnectScheduled,
        ]
    }

    async fn recv_health_event(
        receiver: &mut OkxWebsocketHealthReceiver,
    ) -> Result<OkxWebsocketHealthEvent> {
        time::timeout(Duration::from_millis(250), receiver.recv())
            .await
            .context("timed out waiting for WebSocket health event")?
            .context("WebSocket health channel closed")
    }

    #[test]
    fn websocket_health_event_classification_matches_delivery_policy() {
        for kind in critical_event_kinds() {
            assert!(
                is_critical_websocket_health_event(kind),
                "{kind} should use bounded critical delivery"
            );
            assert!(kind.is_critical(), "{kind} should classify as critical");
        }

        for kind in best_effort_event_kinds() {
            assert!(
                !is_critical_websocket_health_event(kind),
                "{kind} should remain best-effort telemetry"
            );
            assert!(
                !kind.is_critical(),
                "{kind} should not classify as critical"
            );
        }
    }

    #[tokio::test]
    async fn non_critical_health_event_logs_best_effort_drop_when_full() -> Result<()> {
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(1);
        health
            .report(test_event(OkxWebsocketHealthEventKind::Connected))
            .await;
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        health
            .report(test_event(OkxWebsocketHealthEventKind::ConnectAttempt))
            .await;
        let queued_event = recv_health_event(&mut health_events).await?;
        let logs = logs.contents();

        assert_eq!(
            queued_event,
            test_event(OkxWebsocketHealthEventKind::Connected)
        );
        assert!(logs.contains("ws_health_event_dropped"));
        assert!(logs.contains("best_effort_channel_full"));
        assert!(logs.contains("connect_attempt"));
        Ok(())
    }

    #[tokio::test]
    async fn critical_health_events_wait_for_bounded_capacity() -> Result<()> {
        for kind in critical_event_kinds() {
            let (health, mut health_events) = OkxWebsocketHealthReporter::channel(1);
            health
                .report(test_event(OkxWebsocketHealthEventKind::Connected))
                .await;
            let reporter = health.clone();
            let report = tokio::spawn(async move {
                reporter.report(test_event(kind)).await;
            });

            time::sleep(Duration::from_millis(10)).await;
            let queued_event = recv_health_event(&mut health_events).await?;
            time::timeout(Duration::from_millis(250), report)
                .await
                .context("critical health reporter did not finish after capacity freed")?
                .context("critical health reporter task panicked")?;
            let delivered_event = recv_health_event(&mut health_events).await?;

            assert_eq!(
                queued_event,
                test_event(OkxWebsocketHealthEventKind::Connected)
            );
            assert_eq!(delivered_event, test_event(kind));
        }

        Ok(())
    }

    #[tokio::test]
    async fn critical_health_event_timeout_logs_distinct_safety_event() -> Result<()> {
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(1);
        health
            .report(test_event(OkxWebsocketHealthEventKind::Connected))
            .await;
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        time::timeout(
            Duration::from_millis(250),
            health.report(test_event(
                OkxWebsocketHealthEventKind::SubscriptionAckFailed,
            )),
        )
        .await
        .context("critical health reporter hung behind saturated channel")?;
        let queued_event = recv_health_event(&mut health_events).await?;
        let logs = logs.contents();

        assert_eq!(
            queued_event,
            test_event(OkxWebsocketHealthEventKind::Connected)
        );
        assert!(logs.contains("ws_health_critical_event_delivery_failed"));
        assert!(logs.contains("channel_full_timeout"));
        assert!(logs.contains("subscription_ack_failed"));
        Ok(())
    }

    #[tokio::test]
    async fn critical_health_event_closed_channel_logs_distinct_safety_event() -> Result<()> {
        let (health, health_events) = OkxWebsocketHealthReporter::channel(1);
        drop(health_events);
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        health
            .report(test_event(OkxWebsocketHealthEventKind::LoginFailed))
            .await;
        let logs = logs.contents();

        assert!(logs.contains("ws_health_critical_event_delivery_failed"));
        assert!(logs.contains("channel_closed"));
        assert!(logs.contains("login_failed"));
        Ok(())
    }

    #[tokio::test]
    async fn stream_task_panic_reports_through_critical_bounded_delivery() -> Result<()> {
        let stream = test_stream();
        let (health, mut health_events) = OkxWebsocketHealthReporter::channel(1);
        health
            .report(test_event(OkxWebsocketHealthEventKind::Connected))
            .await;

        let task = OkxWebsocketTaskHandle::spawn(stream, Some(health), async {
            panic!("simulated WebSocket task panic for health reporter test");
        });
        time::sleep(Duration::from_millis(10)).await;
        let queued_event = recv_health_event(&mut health_events).await?;
        let lifecycle_event = recv_health_event(&mut health_events).await?;
        drop(task);

        assert_eq!(
            queued_event,
            test_event(OkxWebsocketHealthEventKind::Connected)
        );
        assert_eq!(
            lifecycle_event,
            OkxWebsocketHealthEvent::new(OkxWebsocketHealthEventKind::StreamTaskPanicked, stream)
        );
        Ok(())
    }
}
