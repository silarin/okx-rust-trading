use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use okx_public_protocol::OkxSpotInstrumentId;
use serde_json::json;
use tokio::{net::TcpStream, time};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use zeroize::Zeroizing;

use super::{
    auth::{
        OkxWebsocketLoginCredentials, login_request_at, parse_login_ack,
        validate_websocket_login_credential,
    },
    subscription::{
        OkxWebsocketSubscriptionAck, OkxWebsocketSubscriptionEvent, acknowledge_subscription,
        parse_subscription_event,
    },
};

const PUBLIC_TICKERS_CHANNEL: &str = "tickers";
const PRIVATE_ORDERS_CHANNEL: &str = "orders";
const SPOT_INSTRUMENT_TYPE: &str = "SPOT";

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct OkxEconomicsWebsocketCredentials {
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    api_passphrase: Zeroizing<String>,
}

impl OkxEconomicsWebsocketCredentials {
    pub(crate) fn new(
        api_key: impl Into<Zeroizing<String>>,
        api_secret: impl Into<Zeroizing<String>>,
        api_passphrase: impl Into<Zeroizing<String>>,
    ) -> Result<Self> {
        let credentials = Self {
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            api_passphrase: api_passphrase.into(),
        };
        validate_websocket_login_credential(
            "OKX economics preflight WebSocket api_key",
            &credentials.api_key,
        )?;
        validate_websocket_login_credential(
            "OKX economics preflight WebSocket api_secret",
            &credentials.api_secret,
        )?;
        validate_websocket_login_credential(
            "OKX economics preflight WebSocket api_passphrase",
            &credentials.api_passphrase,
        )?;
        Ok(credentials)
    }

    fn login_credentials(&self) -> OkxWebsocketLoginCredentials<'_> {
        OkxWebsocketLoginCredentials {
            api_key: self.api_key.as_str(),
            api_secret: self.api_secret.as_str(),
            api_passphrase: self.api_passphrase.as_str(),
        }
    }
}

impl std::fmt::Debug for OkxEconomicsWebsocketCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OkxEconomicsWebsocketCredentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("api_passphrase", &"<redacted>")
            .finish()
    }
}

pub(crate) async fn probe_public_websocket(
    url: &str,
    instrument_id: &str,
    timeout: Duration,
) -> Result<()> {
    validate_probe_inputs(url, instrument_id, timeout)?;
    let mut stream = connect(url, timeout, "public").await?;
    let request = json!({
        "op": "subscribe",
        "args": [{"channel": PUBLIC_TICKERS_CHANNEL, "instId": instrument_id}],
    });
    stream
        .send(Message::Text(request.to_string().into()))
        .await
        .context("failed sending OKX economics preflight public subscription")?;
    wait_for_subscription_ack(
        &mut stream,
        OkxWebsocketSubscriptionAck {
            channel: PUBLIC_TICKERS_CHANNEL.to_owned(),
            inst_id: Some(instrument_id.to_owned()),
            inst_type: None,
        },
        timeout,
        "economics preflight public",
    )
    .await?;
    close_cleanly(&mut stream, timeout, "public").await
}

pub(crate) async fn probe_private_websocket(
    url: &str,
    credentials: &OkxEconomicsWebsocketCredentials,
    login_timestamp: &str,
    instrument_id: &str,
    timeout: Duration,
) -> Result<()> {
    validate_probe_inputs(url, instrument_id, timeout)?;
    let mut stream = connect(url, timeout, "private").await?;
    login(
        &mut stream,
        credentials,
        login_timestamp,
        timeout,
        "private",
    )
    .await?;
    let request = json!({
        "op": "subscribe",
        "args": [{
            "channel": PRIVATE_ORDERS_CHANNEL,
            "instType": SPOT_INSTRUMENT_TYPE,
            "instId": instrument_id,
        }],
    });
    stream
        .send(Message::Text(request.to_string().into()))
        .await
        .context("failed sending OKX economics preflight private subscription")?;
    wait_for_subscription_ack(
        &mut stream,
        OkxWebsocketSubscriptionAck {
            channel: PRIVATE_ORDERS_CHANNEL.to_owned(),
            inst_id: Some(instrument_id.to_owned()),
            inst_type: Some(SPOT_INSTRUMENT_TYPE.to_owned()),
        },
        timeout,
        "economics preflight private",
    )
    .await?;
    close_cleanly(&mut stream, timeout, "private").await
}

pub(crate) async fn probe_trading_session(
    url: &str,
    credentials: &OkxEconomicsWebsocketCredentials,
    login_timestamp: &str,
    timeout: Duration,
) -> Result<()> {
    ensure!(
        !url.trim().is_empty(),
        "OKX WebSocket URL must not be empty"
    );
    ensure!(!timeout.is_zero(), "OKX WebSocket timeout must be non-zero");
    let mut stream = connect(url, timeout, "trading-session").await?;
    login(
        &mut stream,
        credentials,
        login_timestamp,
        timeout,
        "trading-session",
    )
    .await?;
    close_cleanly(&mut stream, timeout, "trading-session").await
}

fn validate_probe_inputs(url: &str, instrument_id: &str, timeout: Duration) -> Result<()> {
    ensure!(
        !url.trim().is_empty(),
        "OKX WebSocket URL must not be empty"
    );
    OkxSpotInstrumentId::try_from(instrument_id)
        .context("OKX economics preflight WebSocket instrument must be canonical SPOT")?;
    ensure!(!timeout.is_zero(), "OKX WebSocket timeout must be non-zero");
    Ok(())
}

async fn connect(
    url: &str,
    timeout: Duration,
    context: &'static str,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    time::timeout(timeout, connect_async(url))
        .await
        .with_context(|| {
            format!("timed out connecting to OKX economics preflight {context} WebSocket")
        })?
        .with_context(|| {
            format!("failed connecting to OKX economics preflight {context} WebSocket")
        })
        .map(|(stream, _)| stream)
}

async fn login(
    stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    credentials: &OkxEconomicsWebsocketCredentials,
    login_timestamp: &str,
    timeout: Duration,
    context: &'static str,
) -> Result<()> {
    let request = login_request_at(&credentials.login_credentials(), login_timestamp)?;
    stream
        .send(Message::Text(request.into()))
        .await
        .with_context(|| format!("failed sending OKX economics preflight {context} login"))?;
    time::timeout(timeout, async {
        loop {
            match next_message(stream, context).await? {
                Message::Text(payload) => {
                    if parse_login_ack(payload.as_ref(), context)? {
                        return Ok(());
                    }
                }
                Message::Ping(payload) => stream
                    .send(Message::Pong(payload))
                    .await
                    .with_context(|| format!("failed replying to OKX {context} login ping"))?,
                Message::Close(_) => {
                    bail!("OKX {context} WebSocket closed before login acknowledgement")
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    })
    .await
    .with_context(|| format!("timed out waiting for OKX {context} login acknowledgement"))?
}

async fn wait_for_subscription_ack(
    stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    expected: OkxWebsocketSubscriptionAck,
    timeout: Duration,
    context: &'static str,
) -> Result<()> {
    let mut pending = [expected].into_iter().collect();
    time::timeout(timeout, async {
        loop {
            match next_message(stream, context).await? {
                Message::Text(payload) => match parse_subscription_event(&payload, context)? {
                    OkxWebsocketSubscriptionEvent::Acknowledged(ack) => {
                        if acknowledge_subscription(&mut pending, ack, context)? {
                            return Ok(());
                        }
                    }
                    OkxWebsocketSubscriptionEvent::Control
                    | OkxWebsocketSubscriptionEvent::Other => {}
                    OkxWebsocketSubscriptionEvent::Data(_) => {
                        bail!("OKX {context} WebSocket sent data before subscription readiness")
                    }
                    OkxWebsocketSubscriptionEvent::Error { code, msg, .. } => {
                        bail!(
                            "OKX {context} WebSocket subscription rejected with code {code}: {msg}"
                        )
                    }
                },
                Message::Ping(payload) => {
                    stream.send(Message::Pong(payload)).await.with_context(|| {
                        format!("failed replying to OKX {context} subscription ping")
                    })?
                }
                Message::Close(_) => {
                    bail!("OKX {context} WebSocket closed before subscription acknowledgement")
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    })
    .await
    .with_context(|| format!("timed out waiting for OKX {context} subscription acknowledgement"))?
}

async fn close_cleanly(
    stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    timeout: Duration,
    context: &'static str,
) -> Result<()> {
    stream
        .send(Message::Close(None))
        .await
        .with_context(|| format!("failed initiating OKX economics preflight {context} close"))?;
    time::timeout(timeout, async {
        loop {
            match next_message(stream, context).await? {
                Message::Close(_) => return Ok(()),
                Message::Ping(payload) => stream
                    .send(Message::Pong(payload))
                    .await
                    .with_context(|| format!("failed replying to OKX {context} shutdown ping"))?,
                Message::Text(_) | Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    })
    .await
    .with_context(|| format!("timed out closing OKX economics preflight {context} WebSocket"))?
}

async fn next_message(
    stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    context: &'static str,
) -> Result<Message> {
    stream
        .next()
        .await
        .with_context(|| {
            format!("OKX economics preflight {context} WebSocket ended without a clean close")
        })?
        .with_context(|| format!("failed reading OKX economics preflight {context} WebSocket"))
}

#[cfg(test)]
#[path = "economics_preflight_tests.rs"]
mod tests;
