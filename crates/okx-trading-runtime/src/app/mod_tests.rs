use tracing_subscriber::layer::SubscriberExt;

use super::telemetry_filter_from_rust_log_or_default;
use crate::test_support::CapturedLogs;

#[test]
fn empty_rust_log_preserves_default_info_telemetry() {
    let logs = CapturedLogs::default();
    let filter = telemetry_filter_from_rust_log_or_default(Some(""))
        .expect("empty RUST_LOG should use the default telemetry filter");
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(filter).with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(logs.clone()),
        ),
    );
    let _guard = tracing::dispatcher::set_default(&dispatch);

    tracing::info!("empty RUST_LOG keeps info telemetry");
    tracing::warn!("empty RUST_LOG keeps warn telemetry");
    tracing::debug!("empty RUST_LOG suppresses debug telemetry");
    let contents = logs.contents();

    assert!(contents.contains("empty RUST_LOG keeps info telemetry"));
    assert!(contents.contains("empty RUST_LOG keeps warn telemetry"));
    assert!(!contents.contains("empty RUST_LOG suppresses debug telemetry"));
}
