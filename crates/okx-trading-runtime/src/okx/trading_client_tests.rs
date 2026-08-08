use super::*;

#[test]
fn websocket_request_ids_are_unique_for_repeated_client_order_ids() {
    let first = websocket_request_id(OKX_WEBSOCKET_PLACE_ORDER_REQUEST_PREFIX, 1, "entry1");
    let second = websocket_request_id(OKX_WEBSOCKET_PLACE_ORDER_REQUEST_PREFIX, 2, "entry1");

    assert_ne!(first, second);
    assert_valid_request_id(&first, OKX_WEBSOCKET_PLACE_ORDER_REQUEST_PREFIX);
    assert_valid_request_id(&second, OKX_WEBSOCKET_PLACE_ORDER_REQUEST_PREFIX);
}

#[test]
fn websocket_request_ids_stay_bounded_after_nonce_wrap() {
    let request_id = websocket_request_id(
        OKX_WEBSOCKET_CANCEL_ORDER_REQUEST_PREFIX,
        u64::MAX,
        "takeprofit1",
    );

    assert_valid_request_id(&request_id, OKX_WEBSOCKET_CANCEL_ORDER_REQUEST_PREFIX);
}

#[test]
fn websocket_order_command_ack_timeout_leaves_tick_budget_for_rest_fallback() {
    assert_eq!(
        websocket_order_command_ack_timeout(5_000),
        Duration::from_nanos(1_666_666_666)
    );
    assert_eq!(
        websocket_order_command_ack_timeout(1),
        OKX_WEBSOCKET_COMMAND_MIN_ACK_TIMEOUT
    );
    assert_eq!(
        websocket_order_command_ack_timeout(60_000),
        DEFAULT_ACK_TIMEOUT
    );
}

#[test]
fn missing_connected_websocket_session_fails_closed() {
    let mut session = None;

    let Err(error) = connected_websocket_order_session(&mut session) else {
        panic!("missing WebSocket order session should fail closed");
    };

    match error {
        OkxWebsocketOrderCommandError::Unavailable(error) => assert!(
            error
                .to_string()
                .contains("WebSocket order session was unavailable"),
            "missing session should be reported clearly: {error}"
        ),
        OkxWebsocketOrderCommandError::PreparationRejected(error) => {
            panic!("missing session should be unavailable, not a preparation rejection: {error}")
        }
        OkxWebsocketOrderCommandError::Ambiguous(error) => {
            panic!("missing session should not be ambiguous: {error}")
        }
    }
}

#[tokio::test]
async fn server_time_refresh_failures_use_bounded_coalescing_delivery() {
    let (failures, mut receiver) = mpsc::channel(1);
    report_server_time_refresh_failure(Some(&failures), anyhow::anyhow!("first refresh failure"));
    report_server_time_refresh_failure(
        Some(&failures),
        anyhow::anyhow!("coalesced refresh failure"),
    );

    let failure = receiver.recv().await.expect("first failure remains queued");
    assert!(failure.to_string().contains("first refresh failure"));
    assert!(receiver.try_recv().is_err());
}

fn assert_valid_request_id(request_id: &str, prefix: char) {
    assert_eq!(request_id.len(), 32);
    assert!(request_id.starts_with(prefix));
    assert!(request_id.bytes().all(|byte| byte.is_ascii_alphanumeric()));
}
