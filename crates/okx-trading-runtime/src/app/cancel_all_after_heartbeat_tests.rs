use std::time::Duration;

use anyhow::Result;
use pretty_assertions::assert_eq;

use super::{
    cancel_all_after_heartbeat_period, cancel_all_after_heartbeat_refresh_deadline,
    next_cancel_all_after_heartbeat_failure,
};
use crate::okx::client::OkxCancelAllAfterTimeout;

#[test]
fn refreshes_inside_safety_window() -> Result<()> {
    let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;

    assert_eq!(
        cancel_all_after_heartbeat_period(timeout),
        Duration::from_millis(3_333)
    );
    assert_eq!(
        cancel_all_after_heartbeat_refresh_deadline(timeout),
        cancel_all_after_heartbeat_period(timeout)
    );
    assert!(
        cancel_all_after_heartbeat_refresh_deadline(timeout)
            < Duration::from_secs(timeout.seconds())
    );
    Ok(())
}

#[tokio::test]
async fn failure_is_observable() -> Result<()> {
    let (failure_tx, failure_rx) = tokio::sync::mpsc::channel(1);
    failure_tx.send(anyhow::anyhow!("heartbeat failed")).await?;
    let mut failures = Some(failure_rx);

    let error = next_cancel_all_after_heartbeat_failure(&mut failures)
        .await
        .expect("heartbeat failure should be observable");

    assert_eq!(error.to_string(), "heartbeat failed");
    Ok(())
}
