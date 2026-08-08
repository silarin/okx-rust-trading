use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle, time};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use super::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Copy)]
enum ProbeServerMode {
    Public,
    Private,
    TradingSession,
}

struct ProbeServer {
    url: String,
    frames: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<Result<bool>>,
}

impl ProbeServer {
    async fn spawn(mode: ProbeServerMode) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let frames = Arc::new(Mutex::new(Vec::new()));
        let recorded = frames.clone();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut websocket = accept_async(stream).await?;
            let mut clean_close = false;
            while let Some(message) = websocket.next().await {
                match message? {
                    Message::Text(payload) => {
                        let payload = payload.to_string();
                        recorded.lock().await.push(payload.clone());
                        let value: Value = serde_json::from_str(&payload)?;
                        match value.get("op").and_then(Value::as_str) {
                            Some("login") => {
                                websocket
                                    .send(Message::Text(
                                        r#"{"event":"login","code":"0","msg":""}"#.into(),
                                    ))
                                    .await?
                            }
                            Some("subscribe") => {
                                let arg = value
                                    .get("args")
                                    .and_then(Value::as_array)
                                    .and_then(|args| args.first())
                                    .cloned()
                                    .context("subscription arg")?;
                                websocket
                                    .send(Message::Text(
                                        serde_json::json!({"event": "subscribe", "arg": arg})
                                            .to_string()
                                            .into(),
                                    ))
                                    .await?;
                            }
                            Some(operation) => {
                                anyhow::bail!("unexpected outbound operation {operation}")
                            }
                            None => anyhow::bail!("outbound text frame omitted op"),
                        }
                    }
                    Message::Close(_) => {
                        clean_close = true;
                        websocket.flush().await?;
                        time::sleep(Duration::from_millis(10)).await;
                        break;
                    }
                    Message::Ping(payload) => websocket.send(Message::Pong(payload)).await?,
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
            let expected_text_frames = match mode {
                ProbeServerMode::Public | ProbeServerMode::TradingSession => 1,
                ProbeServerMode::Private => 2,
            };
            ensure!(
                recorded.lock().await.len() == expected_text_frames,
                "unexpected outbound text-frame count"
            );
            Ok(clean_close)
        });
        Ok(Self { url, frames, task })
    }

    async fn finish(self) -> Result<Vec<String>> {
        let clean_close = time::timeout(TEST_TIMEOUT, self.task)
            .await
            .context("probe server task lingered")??
            .context("probe server task failed")?;
        ensure!(
            clean_close,
            "client did not complete a clean close handshake"
        );
        Ok(self.frames.lock().await.clone())
    }
}

fn credentials() -> OkxEconomicsWebsocketCredentials {
    OkxEconomicsWebsocketCredentials::new(
        "test-key".to_owned(),
        "test-secret".to_owned(),
        "test-passphrase".to_owned(),
    )
    .expect("test credentials")
}

#[test]
fn probe_inputs_accept_an_exact_canonical_spot_instrument() -> Result<()> {
    validate_probe_inputs("ws://127.0.0.1:1", "ETH-USDT", Duration::from_millis(1))
}

#[tokio::test]
async fn public_probe_subscribes_then_closes_cleanly() -> Result<()> {
    let server = ProbeServer::spawn(ProbeServerMode::Public).await?;
    probe_public_websocket(&server.url, "ETH-USDT", TEST_TIMEOUT).await?;
    let frames = server.finish().await?;

    assert_eq!(frame_operations(&frames)?, ["subscribe"]);
    let subscription: Value = serde_json::from_str(&frames[0])?;
    assert_eq!(subscription["args"][0]["instId"], "ETH-USDT");
    assert_no_order_operation(&frames)?;
    Ok(())
}

#[tokio::test]
async fn private_probe_logs_in_subscribes_to_spot_orders_and_closes() -> Result<()> {
    let server = ProbeServer::spawn(ProbeServerMode::Private).await?;
    probe_private_websocket(
        &server.url,
        &credentials(),
        "1700000000",
        "ETH-USDT",
        TEST_TIMEOUT,
    )
    .await?;
    let frames = server.finish().await?;

    assert_eq!(frame_operations(&frames)?, ["login", "subscribe"]);
    let subscription: Value = serde_json::from_str(&frames[1])?;
    assert_eq!(subscription["args"][0]["channel"], "orders");
    assert_eq!(subscription["args"][0]["instType"], "SPOT");
    assert_eq!(subscription["args"][0]["instId"], "ETH-USDT");
    assert_no_order_operation(&frames)?;
    Ok(())
}

#[tokio::test]
async fn trading_session_probe_only_logs_in_and_closes() -> Result<()> {
    let server = ProbeServer::spawn(ProbeServerMode::TradingSession).await?;
    probe_trading_session(&server.url, &credentials(), "1700000000", TEST_TIMEOUT).await?;
    let frames = server.finish().await?;

    assert_eq!(frame_operations(&frames)?, ["login"]);
    assert_no_order_operation(&frames)?;
    Ok(())
}

#[tokio::test]
async fn websocket_probe_times_out_without_ack_and_leaves_no_client_task() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut websocket = accept_async(stream).await?;
        let _ = websocket.next().await;
        time::sleep(Duration::from_secs(5)).await;
        Result::<()>::Ok(())
    });

    let error = probe_public_websocket(&url, "BTC-USDT", Duration::from_millis(10))
        .await
        .expect_err("missing subscription acknowledgement should time out");
    assert!(error.to_string().contains("timed out"));
    server.abort();
    let _ = server.await;
    Ok(())
}

#[tokio::test]
async fn websocket_probe_rejects_wrong_ack_and_noncanonical_instrument() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut websocket = accept_async(stream).await?;
        let _ = websocket.next().await;
        websocket
            .send(Message::Text(
                r#"{"event":"subscribe","arg":{"channel":"tickers","instId":"ETH-USDT"}}"#.into(),
            ))
            .await?;
        Result::<()>::Ok(())
    });

    probe_public_websocket(&url, "BTC-USDT", TEST_TIMEOUT)
        .await
        .expect_err("wrong acknowledgement should fail");
    server.await??;

    probe_public_websocket("ws://127.0.0.1:1", "eth-usdt", TEST_TIMEOUT)
        .await
        .expect_err("noncanonical instrument should fail before connecting");
    Ok(())
}

fn frame_operations(frames: &[String]) -> Result<Vec<String>> {
    frames
        .iter()
        .map(|frame| {
            let value: Value = serde_json::from_str(frame)?;
            value
                .get("op")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("outbound frame operation")
        })
        .collect()
}

fn assert_no_order_operation(frames: &[String]) -> Result<()> {
    for operation in frame_operations(frames)? {
        ensure!(
            !matches!(
                operation.as_str(),
                "order"
                    | "amend-order"
                    | "cancel-order"
                    | "batch-orders"
                    | "batch-amend-orders"
                    | "batch-cancel-orders"
            ),
            "preflight emitted prohibited order operation {operation}"
        );
    }
    Ok(())
}
