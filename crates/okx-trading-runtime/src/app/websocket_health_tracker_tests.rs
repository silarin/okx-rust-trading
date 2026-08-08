use std::{collections::BTreeSet, time::Duration};

use anyhow::Result;
use pretty_assertions::assert_eq;

use super::{WebsocketHealthTracker, websocket_task_lifecycle_error};
use crate::okx::websocket::{
    OkxWebsocketChannelClass, OkxWebsocketHealthEvent, OkxWebsocketHealthEventKind,
    OkxWebsocketStreamIdentity, OkxWebsocketStreamKind,
};

#[test]
fn websocket_health_tracker_marks_all_expected_streams_mandatory() {
    let streams = expected_streams();
    let expected = streams.into_iter().collect::<BTreeSet<_>>();
    let tracker = WebsocketHealthTracker::new(streams);

    assert_eq!(tracker.expected_streams, expected);
    assert_eq!(tracker.mandatory_streams, expected);
}

#[test]
fn websocket_health_all_mandatory_streams_ready_allows_runtime() {
    let streams = expected_streams();
    let expected = streams.into_iter().collect::<BTreeSet<_>>();
    let mut tracker = WebsocketHealthTracker::new(streams);

    for stream in streams {
        tracker.record(OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
            stream,
        ));
    }

    assert!(tracker.startup_readiness_error().is_none());
    assert!(tracker.all_mandatory_streams_ready());
    assert_eq!(tracker.ready_streams, expected);
    assert_eq!(tracker.ever_ready_streams, expected);
}

#[test]
fn websocket_health_pending_mandatory_streams_are_not_ready() {
    let streams = expected_streams();
    let mut tracker = WebsocketHealthTracker::new(streams);

    for stream in &streams[..streams.len() - 1] {
        tracker.record(OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
            *stream,
        ));
    }

    assert!(!tracker.all_mandatory_streams_ready());
    assert!(tracker.startup_readiness_error().is_none());

    tracker.record(OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
        *streams.last().expect("fixture should include streams"),
    ));

    assert!(tracker.all_mandatory_streams_ready());
}

#[test]
fn websocket_health_all_mandatory_streams_fail_before_ready_is_fatal() {
    let streams = expected_streams();
    let mut tracker = WebsocketHealthTracker::new(streams);

    for stream in streams {
        tracker.record(OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
            stream,
        ));
    }

    let error = tracker
        .startup_readiness_error()
        .expect("pre-ready failures should stop runtime");
    assert!(
        error
            .to_string()
            .contains("4 mandatory OKX WebSocket stream(s) failed before subscription readiness"),
        "all pre-ready failures should report the startup invariant: {error}"
    );
}

#[test]
fn websocket_health_partial_readiness_does_not_mask_mandatory_failure() {
    let ready_stream = stream(
        OkxWebsocketStreamKind::Public,
        OkxWebsocketChannelClass::PublicMarketData,
    );
    let failed_stream = stream(
        OkxWebsocketStreamKind::Private,
        OkxWebsocketChannelClass::PrivateTrading,
    );
    let mut tracker = WebsocketHealthTracker::new([ready_stream, failed_stream]);

    tracker.record(OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
        ready_stream,
    ));
    tracker.record(OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
        failed_stream,
    ));

    let error = tracker
        .startup_readiness_error()
        .expect("one ready stream must not mask another mandatory failure");
    assert!(
        error
            .to_string()
            .contains("1 mandatory OKX WebSocket stream(s) failed before subscription readiness"),
        "partial readiness should still fail closed: {error}"
    );
    assert!(
        error
            .to_string()
            .contains("private WebSocket private_trading"),
        "readiness error should identify the failed stream: {error}"
    );
}

#[test]
fn websocket_health_mandatory_stream_classes_fail_before_readiness_are_fatal() {
    for stream in expected_streams() {
        let mut tracker = WebsocketHealthTracker::new([stream]);
        tracker.record(OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
            stream,
        ));

        let error = tracker
            .startup_readiness_error()
            .expect("mandatory stream failure should fail closed");
        assert!(
            error.to_string().contains(&format!(
                "OKX {} WebSocket {}",
                stream.kind(),
                stream.channel_class()
            )),
            "readiness error should identify the failed stream: {error}"
        );
    }
}

#[test]
fn websocket_health_task_lifecycle_before_readiness_is_fatal() -> Result<()> {
    let stream = stream(
        OkxWebsocketStreamKind::Public,
        OkxWebsocketChannelClass::PublicMarketData,
    );
    for (kind, expected_message) in [
        (
            OkxWebsocketHealthEventKind::StreamTaskPanicked,
            "stream task panicked",
        ),
        (
            OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly,
            "stream task exited unexpectedly",
        ),
    ] {
        let event = OkxWebsocketHealthEvent::new(kind, stream);
        let lifecycle_error = websocket_task_lifecycle_error(&event)
            .expect("task lifecycle event should produce a fatal error");
        assert!(lifecycle_error.to_string().contains(expected_message));

        let mut tracker = WebsocketHealthTracker::new([stream]);
        tracker.record(event);
        assert!(tracker.startup_readiness_error().is_some());
    }
    Ok(())
}

#[test]
fn websocket_health_reconnect_before_ack_does_not_mark_startup_ready() {
    let stream = stream(
        OkxWebsocketStreamKind::Public,
        OkxWebsocketChannelClass::PublicMarketData,
    );
    let mut tracker = WebsocketHealthTracker::new([stream]);
    tracker.record(OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
        stream,
    ));
    tracker.record(OkxWebsocketHealthEvent::reconnect_scheduled(
        stream,
        /*reconnect_attempt*/ 1,
        Duration::from_millis(10),
    ));

    assert!(!tracker.ready_streams.contains(&stream));
    assert!(tracker.startup_readiness_error().is_some());

    tracker.record(OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
        stream,
    ));

    assert!(tracker.ready_streams.contains(&stream));
    assert!(tracker.startup_readiness_error().is_none());
}

#[test]
fn websocket_health_post_ready_session_transitions_revoke_only_the_affected_stream() {
    let streams = expected_streams();
    for affected_stream in streams {
        for kind in [
            OkxWebsocketHealthEventKind::ConnectAttempt,
            OkxWebsocketHealthEventKind::Connected,
            OkxWebsocketHealthEventKind::LoginAckSucceeded,
            OkxWebsocketHealthEventKind::LoginFailed,
            OkxWebsocketHealthEventKind::SubscriptionAckFailed,
            OkxWebsocketHealthEventKind::ReconnectScheduled,
            OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription,
            OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
            OkxWebsocketHealthEventKind::StreamFailedAfterSubscription,
            OkxWebsocketHealthEventKind::StreamTaskPanicked,
            OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly,
        ] {
            let mut tracker = WebsocketHealthTracker::new(streams);
            for stream in streams {
                tracker.record(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                    stream,
                ));
            }

            tracker.record(if kind == OkxWebsocketHealthEventKind::ReconnectScheduled {
                OkxWebsocketHealthEvent::reconnect_scheduled(
                    affected_stream,
                    1,
                    Duration::from_millis(10),
                )
            } else {
                OkxWebsocketHealthEvent::new(kind, affected_stream)
            });

            assert!(
                !tracker.all_mandatory_streams_ready(),
                "{kind} must revoke aggregate readiness for {affected_stream:?}"
            );
            assert!(
                !tracker.ready_streams.contains(&affected_stream),
                "{kind} must revoke the affected stream {affected_stream:?}"
            );
            assert!(
                streams
                    .iter()
                    .filter(|stream| **stream != affected_stream)
                    .all(|stream| tracker.ready_streams.contains(stream)),
                "{kind} must not revoke another stream"
            );
            assert!(
                tracker.ever_ready_streams.contains(&affected_stream),
                "{kind} must preserve prior subscription evidence"
            );
            assert!(
                tracker.startup_readiness_error().is_none(),
                "{kind} after prior readiness must not become a startup failure"
            );

            tracker.record(OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                affected_stream,
            ));
            assert!(
                tracker.all_mandatory_streams_ready(),
                "only a fresh subscription acknowledgement should restore {kind}"
            );
        }
    }
}

#[test]
fn websocket_health_connection_and_login_success_do_not_restore_subscription_readiness() {
    let stream = stream(
        OkxWebsocketStreamKind::Private,
        OkxWebsocketChannelClass::PrivateTrading,
    );
    let mut tracker = WebsocketHealthTracker::new([stream]);
    tracker.record(OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
        stream,
    ));
    tracker.record(OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription,
        stream,
    ));

    for kind in [
        OkxWebsocketHealthEventKind::ConnectAttempt,
        OkxWebsocketHealthEventKind::Connected,
        OkxWebsocketHealthEventKind::LoginAckSucceeded,
    ] {
        tracker.record(OkxWebsocketHealthEvent::new(kind, stream));
        assert!(
            !tracker.all_mandatory_streams_ready(),
            "{kind} must not substitute for a subscription acknowledgement"
        );
    }

    tracker.record(OkxWebsocketHealthEvent::new(
        OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
        stream,
    ));
    assert!(tracker.all_mandatory_streams_ready());
}

fn expected_streams() -> [OkxWebsocketStreamIdentity; 4] {
    [
        stream(
            OkxWebsocketStreamKind::Public,
            OkxWebsocketChannelClass::PublicMarketData,
        ),
        stream(
            OkxWebsocketStreamKind::Business,
            OkxWebsocketChannelClass::PublicCandles,
        ),
        stream(
            OkxWebsocketStreamKind::Private,
            OkxWebsocketChannelClass::PrivateTrading,
        ),
        stream(
            OkxWebsocketStreamKind::Business,
            OkxWebsocketChannelClass::PrivateAlgoOrders,
        ),
    ]
}

fn stream(
    kind: OkxWebsocketStreamKind,
    channel_class: OkxWebsocketChannelClass,
) -> OkxWebsocketStreamIdentity {
    OkxWebsocketStreamIdentity::new(kind, channel_class, /*instrument_count*/ 1)
}
