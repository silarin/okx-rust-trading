use std::{
    error::Error,
    fmt,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{net::TcpStream, time};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use zeroize::Zeroizing;

use super::{
    auth::{
        OkxWebsocketLoginCredentials, login_request_at, parse_login_ack,
        validate_websocket_login_credential,
    },
    protocol_error::OkxWebsocketProtocolError,
    trading::{
        OkxWebsocketAckRecord, OkxWebsocketAckTracker, OkxWebsocketAmendOrder,
        OkxWebsocketCancelOrder, OkxWebsocketExpectedOrderAck, OkxWebsocketOrderCommandResponse,
        OkxWebsocketOrderOperation, OkxWebsocketPlaceOrder, amend_order_command_json,
        cancel_order_command_json, parse_order_command_ack, place_order_command_json,
    },
};
use crate::okx::types::OkxOrderAck;

pub(crate) const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_IDLE_PING_AFTER: Duration = Duration::from_secs(25);
const OKX_WEBSOCKET_TEXT_PING: &str = "ping";
const OKX_WEBSOCKET_TEXT_PONG: &str = "pong";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxWebsocketTradingCommandConfig {
    pub url: String,
    pub credentials: OkxWebsocketTradingCommandCredentials,
    pub ack_timeout: Duration,
}

impl OkxWebsocketTradingCommandConfig {
    pub fn with_ack_timeout(
        url: String,
        credentials: OkxWebsocketTradingCommandCredentials,
        ack_timeout: Duration,
    ) -> Result<Self> {
        ensure!(
            !url.trim().is_empty(),
            "OKX WebSocket trading command URL must not be empty"
        );
        ensure!(
            !ack_timeout.is_zero(),
            "OKX WebSocket trading command acknowledgement timeout must be non-zero"
        );
        Ok(Self {
            url,
            credentials,
            ack_timeout,
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct OkxWebsocketTradingCommandCredentials {
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    api_passphrase: Zeroizing<String>,
}

impl OkxWebsocketTradingCommandCredentials {
    pub fn new(
        api_key: impl Into<Zeroizing<String>>,
        api_secret: impl Into<Zeroizing<String>>,
        api_passphrase: impl Into<Zeroizing<String>>,
    ) -> Result<Self> {
        let api_key = api_key.into();
        let api_secret = api_secret.into();
        let api_passphrase = api_passphrase.into();
        validate_websocket_login_credential("OKX WebSocket trading command api_key", &api_key)?;
        validate_websocket_login_credential(
            "OKX WebSocket trading command api_secret",
            &api_secret,
        )?;
        validate_websocket_login_credential(
            "OKX WebSocket trading command api_passphrase",
            &api_passphrase,
        )?;
        Ok(Self {
            api_key,
            api_secret,
            api_passphrase,
        })
    }

    fn login_credentials(&self) -> OkxWebsocketLoginCredentials<'_> {
        OkxWebsocketLoginCredentials {
            api_key: self.api_key.as_str(),
            api_secret: self.api_secret.as_str(),
            api_passphrase: self.api_passphrase.as_str(),
        }
    }
}

impl fmt::Debug for OkxWebsocketTradingCommandCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OkxWebsocketTradingCommandCredentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("api_passphrase", &"<redacted>")
            .finish()
    }
}

#[derive(Debug)]
pub struct OkxWebsocketTradingCommandSession {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ack_tracker: OkxWebsocketAckTracker,
    ack_timeout: Duration,
    idle_ping_after: Duration,
    last_received_at: Instant,
}

impl OkxWebsocketTradingCommandSession {
    pub async fn connect(
        config: OkxWebsocketTradingCommandConfig,
        login_timestamp: &str,
    ) -> Result<Self> {
        let (mut stream, _) = time::timeout(config.ack_timeout, connect_async(config.url.as_str()))
            .await
            .with_context(|| {
                format!(
                    "timed out connecting to OKX WebSocket trading command endpoint {}",
                    config.url
                )
            })?
            .with_context(|| {
                format!(
                    "failed connecting to OKX WebSocket trading command endpoint {}",
                    config.url
                )
            })?;
        let login = login_request_at(&config.credentials.login_credentials(), login_timestamp)?;
        stream
            .send(Message::Text(login.into()))
            .await
            .context("failed sending OKX WebSocket trading command login")?;
        wait_for_login_ack(&mut stream, config.ack_timeout).await?;
        Ok(Self {
            stream,
            ack_tracker: OkxWebsocketAckTracker::default(),
            ack_timeout: config.ack_timeout,
            idle_ping_after: DEFAULT_IDLE_PING_AFTER,
            last_received_at: Instant::now(),
        })
    }

    pub async fn place_order(
        &mut self,
        request: OkxWebsocketPlaceOrder<'_>,
    ) -> std::result::Result<OkxOrderAck, OkxWebsocketCommandError> {
        let expected = OkxWebsocketExpectedOrderAck {
            id: request.id,
            operation: OkxWebsocketOrderOperation::PlaceOrder,
            client_order_id: request.client_order_id,
            request_id: None,
        };
        let command =
            place_order_command_json(request).map_err(OkxWebsocketCommandError::NotSent)?;
        self.send_command(command, expected).await
    }

    pub async fn amend_order(
        &mut self,
        request: OkxWebsocketAmendOrder<'_>,
    ) -> std::result::Result<OkxOrderAck, OkxWebsocketCommandError> {
        let expected = OkxWebsocketExpectedOrderAck {
            id: request.id,
            operation: OkxWebsocketOrderOperation::AmendOrder,
            client_order_id: request.client_order_id,
            request_id: Some(request.request_id),
        };
        let command =
            amend_order_command_json(request).map_err(OkxWebsocketCommandError::NotSent)?;
        self.send_command(command, expected).await
    }

    pub async fn cancel_order(
        &mut self,
        request: OkxWebsocketCancelOrder<'_>,
    ) -> std::result::Result<OkxOrderAck, OkxWebsocketCommandError> {
        let expected = OkxWebsocketExpectedOrderAck {
            id: request.id,
            operation: OkxWebsocketOrderOperation::CancelOrder,
            client_order_id: request.client_order_id,
            request_id: None,
        };
        let command =
            cancel_order_command_json(request).map_err(OkxWebsocketCommandError::NotSent)?;
        self.send_command(command, expected).await
    }

    async fn send_command(
        &mut self,
        command: String,
        expected: OkxWebsocketExpectedOrderAck<'_>,
    ) -> std::result::Result<OkxOrderAck, OkxWebsocketCommandError> {
        self.ensure_idle_connection()
            .await
            .map_err(OkxWebsocketCommandError::NotSent)?;
        self.stream
            .send(Message::Text(command.into()))
            .await
            .with_context(|| {
                format!(
                    "failed sending OKX WebSocket {} command {}",
                    expected.operation.as_okx(),
                    expected.id
                )
            })
            .map_err(OkxWebsocketCommandError::NotSent)?;
        time::timeout(self.ack_timeout, self.read_ack(expected))
            .await
            .with_context(|| {
                format!(
                    "timed out waiting for OKX WebSocket {} acknowledgement {}",
                    expected.operation.as_okx(),
                    expected.id
                )
            })
            .map_err(OkxWebsocketCommandError::Ambiguous)?
            .map_err(OkxWebsocketCommandError::Ambiguous)
    }

    async fn ensure_idle_connection(&mut self) -> Result<()> {
        if self.last_received_at.elapsed() < self.idle_ping_after {
            return Ok(());
        }
        self.stream
            .send(Message::Text(OKX_WEBSOCKET_TEXT_PING.into()))
            .await
            .context("failed sending OKX WebSocket trading command idle ping")?;
        time::timeout(self.ack_timeout, self.wait_for_text_pong())
            .await
            .context("timed out waiting for OKX WebSocket trading command idle pong")?
    }

    async fn wait_for_text_pong(&mut self) -> Result<()> {
        loop {
            let message = self.next_message().await?;
            match message {
                Message::Text(payload) if payload.as_str() == OKX_WEBSOCKET_TEXT_PONG => {
                    return Ok(());
                }
                Message::Ping(payload) => {
                    self.stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed replying to OKX WebSocket trading command ping")?;
                }
                Message::Pong(_) => return Ok(()),
                Message::Close(_) => {
                    bail!("OKX WebSocket trading command stream closed before idle pong");
                }
                Message::Text(_) | Message::Binary(_) | Message::Frame(_) => {}
            }
        }
    }

    async fn read_ack(
        &mut self,
        expected: OkxWebsocketExpectedOrderAck<'_>,
    ) -> Result<OkxOrderAck> {
        loop {
            let message = self.next_message().await?;
            match message {
                Message::Text(payload) if payload.as_str() == OKX_WEBSOCKET_TEXT_PONG => {}
                Message::Text(payload) => {
                    if let Some(acknowledgement) =
                        self.handle_text_ack(payload.as_ref(), expected)?
                    {
                        return Ok(acknowledgement);
                    }
                }
                Message::Ping(payload) => {
                    self.stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed replying to OKX WebSocket trading command ping")?;
                }
                Message::Close(_) => {
                    bail!("OKX WebSocket trading command stream closed before acknowledgement");
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }

    async fn next_message(&mut self) -> Result<Message> {
        let message = self
            .stream
            .next()
            .await
            .context("OKX WebSocket trading command stream closed before acknowledgement")?
            .context("failed reading OKX WebSocket trading command message")?;
        self.last_received_at = Instant::now();
        Ok(message)
    }

    fn handle_text_ack(
        &mut self,
        payload: &str,
        expected: OkxWebsocketExpectedOrderAck<'_>,
    ) -> Result<Option<OkxOrderAck>> {
        let Some(response) = parse_command_response(payload)? else {
            return Ok(None);
        };
        if response.id != expected.id {
            let _ = self.ack_tracker.record(&response.id)?;
            return Ok(None);
        }
        if self.ack_tracker.record(&response.id)? == OkxWebsocketAckRecord::Duplicate {
            return Ok(None);
        }
        parse_order_command_ack(payload, expected).map(Some)
    }
}

#[derive(Debug)]
pub(crate) enum OkxWebsocketCommandError {
    NotSent(anyhow::Error),
    Ambiguous(anyhow::Error),
}

impl fmt::Display for OkxWebsocketCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotSent(err) | Self::Ambiguous(err) => err.fmt(formatter),
        }
    }
}

impl Error for OkxWebsocketCommandError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NotSent(err) | Self::Ambiguous(err) => err.source(),
        }
    }
}

async fn wait_for_login_ack(
    stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    ack_timeout: Duration,
) -> Result<()> {
    time::timeout(ack_timeout, async {
        loop {
            let message = stream
                .next()
                .await
                .context("OKX WebSocket trading command stream closed before login")?
                .context("failed reading OKX WebSocket trading command login message")?;
            match message {
                Message::Text(payload) => {
                    if parse_login_ack(payload.as_ref(), "trading command")? {
                        return Ok(());
                    }
                }
                Message::Ping(payload) => {
                    stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed replying to OKX WebSocket trading command login ping")?;
                }
                Message::Close(_) => {
                    return Err(OkxWebsocketProtocolError::ClosedBeforeLoginAck {
                        context: "trading command".to_owned(),
                    }
                    .into());
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    })
    .await
    .map_err(|_| OkxWebsocketProtocolError::TimedOutWaitingForLoginAck {
        context: "trading command".to_owned(),
    })?
}

fn parse_command_response(payload: &str) -> Result<Option<OkxWebsocketOrderCommandResponse>> {
    let value: Value = serde_json::from_str(payload)
        .context("failed parsing OKX WebSocket trading command text frame")?;
    let op = value.get("op").and_then(Value::as_str);
    if !matches!(op, Some("order" | "amend-order" | "cancel-order")) {
        return Ok(None);
    }
    serde_json::from_value(value)
        .map(Some)
        .context("failed parsing OKX WebSocket trading command acknowledgement")
}

#[cfg(test)]
#[path = "trading_session_tests.rs"]
mod tests;
