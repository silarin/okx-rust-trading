use anyhow::Result;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

#[test]
fn place_order_command_uses_spot_cash_post_only_shape() -> Result<()> {
    let command = place_order_command_json(OkxWebsocketPlaceOrder {
        td_mode: "cash",
        id: "entryreq1",
        inst_id_code: 123_456,
        exp_time: "1710000005000",
        side: OrderSide::Buy,
        kind: OrderKind::PostOnly,
        size: "0.001",
        price: Some("100.1"),
        trade_quote_currency: "USDT",
        client_order_id: "entry1",
        tag: "okxrusttrading",
    })?;
    let value: serde_json::Value = serde_json::from_str(&command)?;

    assert_eq!(
        value,
        json!({
            "id": "entryreq1",
            "op": "order",
            "expTime": "1710000005000",
            "args": [{
                "instIdCode": 123456,
                "tdMode": "cash",
                "side": "buy",
                "ordType": "post_only",
                "sz": "0.001",
                "px": "100.1",
                "pxAmendType": "0",
                "tradeQuoteCcy": "USDT",
                "tag": "okxrusttrading",
                "clOrdId": "entry1"
            }]
        })
    );
    Ok(())
}

#[test]
fn amend_order_command_uses_client_order_id_and_request_id() -> Result<()> {
    let command = amend_order_command_json(OkxWebsocketAmendOrder {
        id: "amendreq1",
        inst_id_code: 123_456,
        exp_time: "1710000005000",
        client_order_id: "takeprofit1",
        request_id: "takeprofitamend1",
        new_size: Some("0.002"),
        new_price: Some("105.2"),
    })?;
    let value: serde_json::Value = serde_json::from_str(&command)?;

    assert_eq!(
        value,
        json!({
            "id": "amendreq1",
            "op": "amend-order",
            "expTime": "1710000005000",
            "args": [{
                "instIdCode": 123456,
                "clOrdId": "takeprofit1",
                "reqId": "takeprofitamend1",
                "newSz": "0.002",
                "newPx": "105.2",
                "pxAmendType": "0"
            }]
        })
    );
    Ok(())
}

#[test]
fn cancel_order_command_uses_client_order_id() -> Result<()> {
    let command = cancel_order_command_json(OkxWebsocketCancelOrder {
        id: "cancelreq1",
        inst_id_code: 123_456,
        client_order_id: "entry1",
    })?;
    let value: serde_json::Value = serde_json::from_str(&command)?;

    assert_eq!(
        value,
        json!({
            "id": "cancelreq1",
            "op": "cancel-order",
            "args": [{
                "instIdCode": 123456,
                "clOrdId": "entry1"
            }]
        })
    );
    Ok(())
}

#[test]
fn parses_correlated_successful_order_ack() -> Result<()> {
    let acknowledgement = parse_order_command_ack(
        r#"{
            "id": "entryreq1",
            "op": "order",
            "code": "0",
            "msg": "",
            "data": [{
                "ordId": "ord-1",
                "clOrdId": "entry1",
                "sCode": "0",
                "sMsg": ""
            }],
            "inTime": "1710000000000",
            "outTime": "1710000000001"
        }"#,
        OkxWebsocketExpectedOrderAck {
            id: "entryreq1",
            operation: OkxWebsocketOrderOperation::PlaceOrder,
            client_order_id: "entry1",
            request_id: None,
        },
    )?;

    assert_eq!(
        acknowledgement,
        OkxOrderAck {
            order_id: "ord-1".to_owned(),
            client_order_id: "entry1".to_owned(),
            status_code: "0".to_owned(),
            status_message: String::new(),
            status_sub_code: String::new(),
            timestamp: String::new(),
        }
    );
    Ok(())
}

#[test]
fn parses_successful_order_ack_with_empty_client_order_id() -> Result<()> {
    let acknowledgement = parse_order_command_ack(
        r#"{
            "id": "entryreq1",
            "op": "order",
            "code": "0",
            "msg": "",
            "data": [{
                "ordId": "ord-1",
                "clOrdId": "",
                "sCode": "0",
                "sMsg": ""
            }]
        }"#,
        OkxWebsocketExpectedOrderAck {
            id: "entryreq1",
            operation: OkxWebsocketOrderOperation::PlaceOrder,
            client_order_id: "entry1",
            request_id: None,
        },
    )?;

    assert_eq!(
        acknowledgement,
        OkxOrderAck {
            order_id: "ord-1".to_owned(),
            client_order_id: "entry1".to_owned(),
            status_code: "0".to_owned(),
            status_message: String::new(),
            status_sub_code: String::new(),
            timestamp: String::new(),
        }
    );
    Ok(())
}

#[test]
fn parses_correlated_successful_amend_ack_with_request_id() -> Result<()> {
    let acknowledgement = parse_order_command_ack(
        r#"{
            "id": "takeprofitamend1",
            "op": "amend-order",
            "code": "0",
            "msg": "",
            "data": [{
                "ordId": "ord-1",
                "clOrdId": "takeprofit1",
                "reqId": "takeprofitamend1",
                "sCode": "0",
                "sMsg": ""
            }]
        }"#,
        OkxWebsocketExpectedOrderAck {
            id: "takeprofitamend1",
            operation: OkxWebsocketOrderOperation::AmendOrder,
            client_order_id: "takeprofit1",
            request_id: Some("takeprofitamend1"),
        },
    )?;

    assert_eq!(
        acknowledgement,
        OkxOrderAck {
            order_id: "ord-1".to_owned(),
            client_order_id: "takeprofit1".to_owned(),
            status_code: "0".to_owned(),
            status_message: String::new(),
            status_sub_code: String::new(),
            timestamp: String::new(),
        }
    );
    Ok(())
}

#[test]
fn rejects_amend_acknowledgement_with_wrong_request_id() {
    let error = parse_order_command_ack(
        r#"{
            "id": "takeprofitamend1",
            "op": "amend-order",
            "code": "0",
            "msg": "",
            "data": [{
                "ordId": "ord-1",
                "clOrdId": "takeprofit1",
                "reqId": "staleamend1",
                "sCode": "0",
                "sMsg": ""
            }]
        }"#,
        OkxWebsocketExpectedOrderAck {
            id: "takeprofitamend1",
            operation: OkxWebsocketOrderOperation::AmendOrder,
            client_order_id: "takeprofit1",
            request_id: Some("takeprofitamend1"),
        },
    )
    .unwrap_err();

    assert!(
        error.to_string().contains(
            "OKX WebSocket amend-order acknowledgement returned reqId staleamend1 for requested takeprofitamend1"
        ),
        "mismatched amend request id should fail: {error}"
    );
}

#[test]
fn rejects_stale_or_foreign_acknowledgement_id() {
    let error = parse_order_command_ack(
        r#"{
            "id": "entryold",
            "op": "order",
            "code": "0",
            "data": [{
                "ordId": "ord-1",
                "clOrdId": "entry1",
                "sCode": "0",
                "sMsg": ""
            }]
        }"#,
        OkxWebsocketExpectedOrderAck {
            id: "entrynew",
            operation: OkxWebsocketOrderOperation::PlaceOrder,
            client_order_id: "entry1",
            request_id: None,
        },
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("did not match requested"),
        "stale request id should fail: {error}"
    );
}

#[test]
fn rejects_websocket_level_error_acknowledgement() {
    let error = parse_order_command_ack(
        r#"{
            "id": "entryreq1",
            "op": "order",
            "code": "60013",
            "msg": "Invalid args",
            "data": []
        }"#,
        OkxWebsocketExpectedOrderAck {
            id: "entryreq1",
            operation: OkxWebsocketOrderOperation::PlaceOrder,
            client_order_id: "entry1",
            request_id: None,
        },
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("OKX WebSocket order entryreq1 rejected: 60013 Invalid args"),
        "WebSocket-level order error should fail: {error}"
    );
}

#[test]
fn rejects_order_row_rejection_acknowledgement() {
    let error = parse_order_command_ack(
        r#"{
            "id": "entryreq1",
            "op": "order",
            "code": "0",
            "msg": "",
            "data": [{
                "ordId": "ord-1",
                "clOrdId": "entry1",
                "sCode": "51000",
                "sMsg": "Parameter error",
                "subCode": "51131"
            }]
        }"#,
        OkxWebsocketExpectedOrderAck {
            id: "entryreq1",
            operation: OkxWebsocketOrderOperation::PlaceOrder,
            client_order_id: "entry1",
            request_id: None,
        },
    )
    .unwrap_err();

    assert!(
        error.to_string().contains(
            r#"OKX WebSocket order entry1 rejected: 51000 Parameter error subCode="51131""#
        ),
        "row-level order rejection should preserve subCode: {error}"
    );
}

#[test]
fn rejects_order_row_rejection_acknowledgement_with_empty_order_id() {
    let error = parse_order_command_ack(
        r#"{
            "id": "entryreq1",
            "op": "order",
            "code": "0",
            "msg": "",
            "data": [{
                "ordId": "",
                "clOrdId": "entry1",
                "sCode": "51000",
                "sMsg": "Parameter error"
            }]
        }"#,
        OkxWebsocketExpectedOrderAck {
            id: "entryreq1",
            operation: OkxWebsocketOrderOperation::PlaceOrder,
            client_order_id: "entry1",
            request_id: None,
        },
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("OKX WebSocket order entry1 rejected: 51000 Parameter error"),
        "row-level rejection should not be masked by empty ordId: {error}"
    );
}

#[test]
fn rejects_top_level_error_with_order_row_status() {
    let error = parse_order_command_ack(
        r#"{
            "id": "cancelreq1",
            "op": "cancel-order",
            "code": "1",
            "msg": "",
            "data": [{
                "ordId": "",
                "clOrdId": "entry1",
                "sCode": "51400",
                "sMsg": "Order not found"
            }]
        }"#,
        OkxWebsocketExpectedOrderAck {
            id: "cancelreq1",
            operation: OkxWebsocketOrderOperation::CancelOrder,
            client_order_id: "entry1",
            request_id: None,
        },
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("OKX WebSocket cancel-order entry1 rejected: 51400 Order not found"),
        "top-level error with row status should surface the per-row OKX status: {error}"
    );
}

#[test]
fn rejects_empty_or_multiple_acknowledgement_rows() {
    for payload in [
        r#"{"id":"entryreq1","op":"order","code":"0","msg":"","data":[]}"#,
        r#"{
            "id": "entryreq1",
            "op": "order",
            "code": "0",
            "msg": "",
            "data": [
                {"ordId":"ord-1","clOrdId":"entry1","sCode":"0","sMsg":""},
                {"ordId":"ord-2","clOrdId":"entry1","sCode":"0","sMsg":""}
            ]
        }"#,
    ] {
        let error = parse_order_command_ack(
            payload,
            OkxWebsocketExpectedOrderAck {
                id: "entryreq1",
                operation: OkxWebsocketOrderOperation::PlaceOrder,
                client_order_id: "entry1",
                request_id: None,
            },
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("acknowledgement rows"),
            "ambiguous row count should fail: {error}"
        );
    }
}

#[test]
fn ack_tracker_marks_duplicate_request_ids() -> Result<()> {
    let mut tracker = OkxWebsocketAckTracker::default();

    assert_eq!(
        tracker.record("entryreq1")?,
        OkxWebsocketAckRecord::FirstSeen
    );
    assert_eq!(
        tracker.record("entryreq1")?,
        OkxWebsocketAckRecord::Duplicate
    );
    assert_eq!(
        tracker.record("entryreq2")?,
        OkxWebsocketAckRecord::FirstSeen
    );
    Ok(())
}

#[test]
fn ack_tracker_bounds_recent_request_ids_and_eviction_allows_old_ids_again() -> Result<()> {
    let mut tracker = OkxWebsocketAckTracker::default();

    assert_eq!(
        tracker.record("entryreq0")?,
        OkxWebsocketAckRecord::FirstSeen
    );
    for index in 1..=OKX_WEBSOCKET_ACK_TRACKER_MAX_IDS {
        assert_eq!(
            tracker.record(&format!("entryreq{index}"))?,
            OkxWebsocketAckRecord::FirstSeen
        );
    }

    assert_eq!(
        tracker.seen_request_ids.len(),
        OKX_WEBSOCKET_ACK_TRACKER_MAX_IDS
    );
    assert_eq!(
        tracker.insertion_order.len(),
        OKX_WEBSOCKET_ACK_TRACKER_MAX_IDS
    );
    assert_eq!(
        tracker.record("entryreq0")?,
        OkxWebsocketAckRecord::FirstSeen
    );
    assert_eq!(
        tracker.record("entryreq0")?,
        OkxWebsocketAckRecord::Duplicate
    );
    Ok(())
}

#[test]
fn ack_tracker_rejects_invalid_request_ids() {
    let mut tracker = OkxWebsocketAckTracker::default();

    for request_id in ["", "entry-req-1"] {
        let error = tracker
            .record(request_id)
            .expect_err("invalid request id should fail");
        let error = error.to_string();

        assert!(
            error.contains("must not be empty") || error.contains("ASCII alphanumeric"),
            "invalid acknowledgement id should fail clearly: {error}"
        );
    }
}

#[test]
fn rejects_invalid_command_fields() {
    for request in [
        OkxWebsocketPlaceOrder {
            td_mode: "cash",
            id: "",
            inst_id_code: 123_456,
            exp_time: "1710000005000",
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: "0.001",
            price: Some("100.1"),
            trade_quote_currency: "USDT",
            client_order_id: "entry-1",
            tag: "okxrusttrading",
        },
        OkxWebsocketPlaceOrder {
            td_mode: "cash",
            id: "entry-req-1",
            inst_id_code: 123_456,
            exp_time: "1710000005000",
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: "0.001",
            price: Some("100.1"),
            trade_quote_currency: "USDT",
            client_order_id: "entry1",
            tag: "okxrusttrading",
        },
        OkxWebsocketPlaceOrder {
            td_mode: "cash",
            id: "entryreq1",
            inst_id_code: 123_456,
            exp_time: "1710000005000",
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: "0.001",
            price: Some("100.1"),
            trade_quote_currency: "USDT",
            client_order_id: "entry-1",
            tag: "okxrusttrading",
        },
        OkxWebsocketPlaceOrder {
            td_mode: "cash",
            id: "entryreq1",
            inst_id_code: 0,
            exp_time: "1710000005000",
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: "0.001",
            price: Some("100.1"),
            trade_quote_currency: "USDT",
            client_order_id: "entry1",
            tag: "okxrusttrading",
        },
        OkxWebsocketPlaceOrder {
            td_mode: "cash",
            id: "entryreq1",
            inst_id_code: 123_456,
            exp_time: "",
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: "0.001",
            price: Some("100.1"),
            trade_quote_currency: "USDT",
            client_order_id: "entry1",
            tag: "okxrusttrading",
        },
        OkxWebsocketPlaceOrder {
            td_mode: "cash",
            id: "entryreq1",
            inst_id_code: 123_456,
            exp_time: "notatime",
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: "0.001",
            price: Some("100.1"),
            trade_quote_currency: "USDT",
            client_order_id: "entry1",
            tag: "okxrusttrading",
        },
        OkxWebsocketPlaceOrder {
            td_mode: "cash",
            id: "entryreq1",
            inst_id_code: 123_456,
            exp_time: "1710000005000",
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: "0.001",
            price: Some("100.1"),
            trade_quote_currency: "",
            client_order_id: "entry1",
            tag: "okxrusttrading",
        },
        OkxWebsocketPlaceOrder {
            td_mode: "cash",
            id: "entryreq1",
            inst_id_code: 123_456,
            exp_time: "1710000005000",
            side: OrderSide::Sell,
            kind: OrderKind::Market,
            size: "0.001",
            price: None,
            trade_quote_currency: "USDT",
            client_order_id: "stopexit1",
            tag: "okxrusttrading",
        },
    ] {
        place_order_command_json(request)
            .expect_err("invalid WebSocket place order command should fail");
    }
}

#[test]
fn rejects_amend_without_new_size_or_price() {
    let error = amend_order_command_json(OkxWebsocketAmendOrder {
        id: "amendreq1",
        inst_id_code: 123_456,
        exp_time: "1710000005000",
        client_order_id: "takeprofit1",
        request_id: "takeprofitamend1",
        new_size: None,
        new_price: None,
    })
    .unwrap_err();

    assert!(
        error.to_string().contains("requires newSz or newPx"),
        "amend without new fields should fail: {error}"
    );
}
