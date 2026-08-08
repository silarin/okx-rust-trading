//! Tracks mandatory OKX WebSocket startup readiness without making stream
//! events authoritative for exchange state.

use std::collections::BTreeSet;

use crate::okx::websocket::{
    OkxWebsocketHealthEvent, OkxWebsocketHealthEventKind, OkxWebsocketStreamIdentity,
};

#[derive(Debug)]
pub(super) struct WebsocketHealthTracker {
    pub(super) expected_streams: BTreeSet<OkxWebsocketStreamIdentity>,
    pub(super) mandatory_streams: BTreeSet<OkxWebsocketStreamIdentity>,
    pub(super) ready_streams: BTreeSet<OkxWebsocketStreamIdentity>,
    pub(super) ever_ready_streams: BTreeSet<OkxWebsocketStreamIdentity>,
    pub(super) failed_before_ready_streams: BTreeSet<OkxWebsocketStreamIdentity>,
}

impl WebsocketHealthTracker {
    pub(super) fn new(streams: impl IntoIterator<Item = OkxWebsocketStreamIdentity>) -> Self {
        let expected_streams: BTreeSet<_> = streams.into_iter().collect();
        // PRD WebSocket hybrid startup policy for strategy-enabled profiles:
        //
        // | Stream class | Mandatory | REST-degraded startup | Pre-ready failure | Proof |
        // | Public market data | yes | no REST-only startup selector | fatal | websocket_health_mandatory_stream_classes_fail_before_readiness_are_fatal |
        // | Public candle/business | yes when expected | no REST-only startup selector | fatal | websocket_health_mandatory_stream_classes_fail_before_readiness_are_fatal |
        // | Private trading | yes | no REST-only startup selector | fatal | websocket_health_partial_readiness_does_not_mask_mandatory_failure |
        // | Private algo/business | yes when expected | no REST-only startup selector | fatal | websocket_health_mandatory_stream_classes_fail_before_readiness_are_fatal |
        //
        // REST remains authoritative for snapshots, recovery, fallback, and final
        // reconciliation after stream readiness. It does not silently downgrade a
        // strategy-enabled profile when a mandatory stream fails before ACK readiness.
        let mandatory_streams = expected_streams.clone();
        Self {
            expected_streams,
            mandatory_streams,
            ready_streams: BTreeSet::new(),
            ever_ready_streams: BTreeSet::new(),
            failed_before_ready_streams: BTreeSet::new(),
        }
    }

    pub(super) fn record(&mut self, event: OkxWebsocketHealthEvent) {
        let stream = event.stream();
        if !self.expected_streams.contains(&stream) {
            return;
        }

        match event.kind() {
            OkxWebsocketHealthEventKind::SubscriptionAckSucceeded => {
                self.ready_streams.insert(stream);
                self.ever_ready_streams.insert(stream);
                self.failed_before_ready_streams.remove(&stream);
            }
            OkxWebsocketHealthEventKind::LoginFailed
            | OkxWebsocketHealthEventKind::SubscriptionAckFailed
            | OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription
            | OkxWebsocketHealthEventKind::StreamTaskPanicked
            | OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly => {
                self.ready_streams.remove(&stream);
                if !self.ever_ready_streams.contains(&stream) {
                    self.failed_before_ready_streams.insert(stream);
                }
            }
            OkxWebsocketHealthEventKind::ConnectAttempt
            | OkxWebsocketHealthEventKind::Connected
            | OkxWebsocketHealthEventKind::LoginAckSucceeded
            | OkxWebsocketHealthEventKind::ReconnectScheduled
            | OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription
            | OkxWebsocketHealthEventKind::StreamFailedAfterSubscription => {
                self.ready_streams.remove(&stream);
            }
        }
    }

    pub(super) fn startup_readiness_error(&self) -> Option<anyhow::Error> {
        let mut failed_mandatory_streams = self.mandatory_streams.iter().filter(|stream| {
            self.failed_before_ready_streams.contains(stream)
                && !self.ready_streams.contains(stream)
        });
        let first_failed = failed_mandatory_streams.next()?;
        let failed_count = 1 + failed_mandatory_streams.count();
        Some(anyhow::anyhow!(
            "{failed_count} mandatory OKX WebSocket stream(s) failed before subscription readiness; first failed stream: OKX {} WebSocket {} with {} instrument(s)",
            first_failed.kind(),
            first_failed.channel_class(),
            first_failed.instrument_count()
        ))
    }

    pub(super) fn all_mandatory_streams_ready(&self) -> bool {
        self.mandatory_streams.is_subset(&self.ready_streams)
    }
}

pub(super) fn websocket_task_lifecycle_error(
    event: &OkxWebsocketHealthEvent,
) -> Option<anyhow::Error> {
    let stream = event.stream();
    match event.kind() {
        OkxWebsocketHealthEventKind::StreamTaskPanicked => Some(anyhow::anyhow!(
            "OKX {} WebSocket {} stream task panicked",
            stream.kind(),
            stream.channel_class()
        )),
        OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly => Some(anyhow::anyhow!(
            "OKX {} WebSocket {} stream task exited unexpectedly",
            stream.kind(),
            stream.channel_class()
        )),
        OkxWebsocketHealthEventKind::ConnectAttempt
        | OkxWebsocketHealthEventKind::Connected
        | OkxWebsocketHealthEventKind::LoginAckSucceeded
        | OkxWebsocketHealthEventKind::LoginFailed
        | OkxWebsocketHealthEventKind::SubscriptionAckSucceeded
        | OkxWebsocketHealthEventKind::SubscriptionAckFailed
        | OkxWebsocketHealthEventKind::ReconnectScheduled
        | OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription
        | OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription
        | OkxWebsocketHealthEventKind::StreamFailedAfterSubscription => None,
    }
}

#[cfg(test)]
#[path = "websocket_health_tracker_tests.rs"]
mod tests;
