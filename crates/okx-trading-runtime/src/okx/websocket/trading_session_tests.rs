use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use pretty_assertions::assert_eq;
use serde_json::json;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use super::*;
use crate::okx::{
    types::{OrderKind, OrderSide},
    websocket::trading::{OkxWebsocketCancelOrder, OkxWebsocketPlaceOrder},
};

const TEST_WEBSOCKET_TIMEOUT: Duration = Duration::from_secs(1);
const TEST_WEBSOCKET_LOGIN_TIMESTAMP: &str = "1538054050";

type TestWebSocket = tokio_tungstenite::WebSocketStream<TcpStream>;

#[test]
fn trading_command_credentials_reject_surrounding_whitespace() {
    for (api_key, api_secret, api_passphrase) in [
        (" api-key", "secret", "passphrase"),
        ("api-key ", "secret", "passphrase"),
        ("api-key", " secret", "passphrase"),
        ("api-key", "secret ", "passphrase"),
        ("api-key", "secret", " passphrase"),
        ("api-key", "secret", "passphrase "),
    ] {
        let error = OkxWebsocketTradingCommandCredentials::new(
            api_key.to_owned(),
            api_secret.to_owned(),
            api_passphrase.to_owned(),
        )
        .expect_err("padded WebSocket trading credentials should fail");

        assert!(
            error.to_string().contains("leading or trailing whitespace"),
            "padded WebSocket trading credentials should report whitespace: {error}"
        );
    }
}

#[tokio::test]
async fn command_session_logs_in_and_returns_correlated_ack() -> Result<()> {
    let (url, server) =
        spawn_command_server(vec![expected_order_ack("entryreq1", "entry1")]).await?;
    let mut session =
        OkxWebsocketTradingCommandSession::connect(config(url), TEST_WEBSOCKET_LOGIN_TIMESTAMP)
            .await?;

    let acknowledgement = session
        .place_order(OkxWebsocketPlaceOrder {
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
        })
        .await?;
    let received = await_command_server(server).await?;

    assert_eq!(acknowledgement.order_id, "ord-entry1");
    assert_eq!(acknowledgement.client_order_id, "entry1");
    assert_eq!(received.len(), 2);
    assert_eq!(
        json_value(&received[0])?["args"][0]["timestamp"],
        TEST_WEBSOCKET_LOGIN_TIMESTAMP
    );
    assert_eq!(json_value(&received[1])?["op"], json!("order"));
    Ok(())
}

#[tokio::test]
async fn command_session_ignores_private_events_and_stale_acks() -> Result<()> {
    let (url, server) = spawn_command_server(vec![
        r#"{"arg":{"channel":"orders","instType":"SPOT","instId":"BTC-USDT"},"data":[]}"#
            .to_owned(),
        expected_order_ack("oldreq", "entryold"),
        expected_cancel_ack("cancelreq1", "entry1"),
    ])
    .await?;
    let mut session =
        OkxWebsocketTradingCommandSession::connect(config(url), TEST_WEBSOCKET_LOGIN_TIMESTAMP)
            .await?;

    let acknowledgement = session
        .cancel_order(OkxWebsocketCancelOrder {
            id: "cancelreq1",
            inst_id_code: 123_456,
            client_order_id: "entry1",
        })
        .await?;
    let received = await_command_server(server).await?;

    assert_eq!(acknowledgement.order_id, "ord-entry1");
    assert_eq!(received.len(), 2);
    assert_eq!(json_value(&received[1])?["op"], json!("cancel-order"));
    Ok(())
}

#[tokio::test]
async fn command_session_sends_idle_ping_before_command() -> Result<()> {
    let (url, server) =
        spawn_command_server_with_idle_pong(vec![expected_order_ack("entryreq1", "entry1")])
            .await?;
    let mut session =
        OkxWebsocketTradingCommandSession::connect(config(url), TEST_WEBSOCKET_LOGIN_TIMESTAMP)
            .await?;
    session.last_received_at = Instant::now() - (DEFAULT_IDLE_PING_AFTER + Duration::from_secs(1));

    let acknowledgement = session
        .place_order(OkxWebsocketPlaceOrder {
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
        })
        .await?;
    let received = await_command_server(server).await?;

    assert_eq!(acknowledgement.order_id, "ord-entry1");
    assert_eq!(received.len(), 3);
    assert_eq!(received[1], "ping");
    assert_eq!(json_value(&received[2])?["op"], json!("order"));
    Ok(())
}

#[tokio::test]
async fn command_session_rejects_correlated_error_ack() -> Result<()> {
    let (url, server) = spawn_command_server(vec![
        r#"{
        "id": "entryreq1",
        "op": "order",
        "code": "60013",
        "msg": "Invalid args",
        "data": []
    }"#
        .to_owned(),
    ])
    .await?;
    let mut session =
        OkxWebsocketTradingCommandSession::connect(config(url), TEST_WEBSOCKET_LOGIN_TIMESTAMP)
            .await?;

    let error = session
        .place_order(OkxWebsocketPlaceOrder {
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
        })
        .await
        .unwrap_err();
    let received = await_command_server(server).await?;

    assert!(
        error
            .to_string()
            .contains("OKX WebSocket order entryreq1 rejected: 60013 Invalid args"),
        "correlated error acknowledgement should fail: {error}"
    );
    assert_eq!(received.len(), 2);
    Ok(())
}

#[tokio::test]
async fn command_session_preserves_correlated_row_rejection_subcode() -> Result<()> {
    let (url, server) = spawn_command_server(vec![
        r#"{
        "id": "entryreq1",
        "op": "order",
        "code": "0",
        "msg": "",
        "data": [{
            "ordId": "",
            "clOrdId": "entry1",
            "sCode": "51000",
            "sMsg": "Parameter error",
            "subCode": "51131"
        }]
    }"#
        .to_owned(),
    ])
    .await?;
    let mut session =
        OkxWebsocketTradingCommandSession::connect(config(url), TEST_WEBSOCKET_LOGIN_TIMESTAMP)
            .await?;

    let error = session
        .place_order(OkxWebsocketPlaceOrder {
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
        })
        .await
        .unwrap_err();
    let received = await_command_server(server).await?;

    assert!(
        error.to_string().contains(
            r#"OKX WebSocket order entry1 rejected: 51000 Parameter error subCode="51131""#
        ),
        "correlated row rejection should preserve subCode: {error}"
    );
    assert_eq!(received.len(), 2);
    Ok(())
}

async fn spawn_command_server(
    command_responses: Vec<String>,
) -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();

        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        for response in command_responses {
            websocket.send(Message::Text(response.into())).await?;
        }

        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_command_server_with_idle_pong(
    command_responses: Vec<String>,
) -> Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(listener).await?;
        let mut received = Vec::new();

        received.push(next_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_text(&mut websocket).await?);
        websocket.send(Message::Text("pong".into())).await?;
        received.push(next_text(&mut websocket).await?);
        for response in command_responses {
            websocket.send(Message::Text(response.into())).await?;
        }

        Ok(received)
    });
    Ok((url, handle))
}

async fn accept_test_websocket(listener: TcpListener) -> Result<TestWebSocket> {
    let (stream, _) = time::timeout(TEST_WEBSOCKET_TIMEOUT, listener.accept())
        .await
        .context("timed out accepting test WebSocket TCP connection")??;
    time::timeout(TEST_WEBSOCKET_TIMEOUT, accept_async(stream))
        .await
        .context("timed out accepting test WebSocket handshake")?
        .context("failed accepting test WebSocket handshake")
}

async fn await_command_server(handle: JoinHandle<Result<Vec<String>>>) -> Result<Vec<String>> {
    time::timeout(TEST_WEBSOCKET_TIMEOUT, handle)
        .await
        .context("timed out waiting for test WebSocket server task")?
        .context("test WebSocket server task panicked")?
}

async fn next_test_websocket_message(websocket: &mut TestWebSocket) -> Result<Message> {
    time::timeout(TEST_WEBSOCKET_TIMEOUT, websocket.next())
        .await
        .context("timed out waiting for test WebSocket client text frame")?
        .context("test WebSocket closed before text frame")?
        .context("failed reading test WebSocket client frame")
}

async fn next_text(websocket: &mut TestWebSocket) -> Result<String> {
    loop {
        let message = next_test_websocket_message(websocket).await?;
        if let Message::Text(payload) = message {
            return Ok(payload.to_string());
        }
    }
}

fn config(url: String) -> OkxWebsocketTradingCommandConfig {
    OkxWebsocketTradingCommandConfig::with_ack_timeout(
        url,
        OkxWebsocketTradingCommandCredentials::new(
            "api-key".to_owned(),
            "secret".to_owned(),
            "passphrase".to_owned(),
        )
        .expect("credentials should be valid"),
        Duration::from_secs(1),
    )
    .expect("config should be valid")
}

fn expected_order_ack(id: &str, client_order_id: &str) -> String {
    expected_ack(id, "order", client_order_id)
}

fn expected_cancel_ack(id: &str, client_order_id: &str) -> String {
    expected_ack(id, "cancel-order", client_order_id)
}

fn expected_ack(id: &str, op: &str, client_order_id: &str) -> String {
    format!(
        r#"{{
            "id": "{id}",
            "op": "{op}",
            "code": "0",
            "msg": "",
            "data": [{{
                "ordId": "ord-{client_order_id}",
                "clOrdId": "{client_order_id}",
                "sCode": "0",
                "sMsg": ""
            }}]
        }}"#
    )
}

fn json_value(payload: &str) -> Result<serde_json::Value> {
    serde_json::from_str(payload).context("payload should be JSON")
}
