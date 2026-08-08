use std::{error::Error, fmt};

use super::subscription::OkxWebsocketSubscriptionAck;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum OkxWebsocketProtocolError {
    MalformedJson {
        context: String,
        parser_error: String,
    },
    MissingSubscribeArg {
        context: String,
    },
    MissingDataArg {
        context: String,
    },
    UnexpectedSubscriptionAck {
        context: String,
        ack: Box<OkxWebsocketSubscriptionAck>,
    },
    SubscriptionErrorEvent {
        context: String,
        code: String,
        msg: String,
        ack: Option<Box<OkxWebsocketSubscriptionAck>>,
    },
    ServiceUpgradeNotice {
        context: String,
        code: String,
    },
    UnsupportedNotice {
        context: String,
        code: String,
    },
    MalformedNotice {
        context: String,
    },
    DataBeforeSubscriptionAck {
        context: String,
        ack: Box<OkxWebsocketSubscriptionAck>,
    },
    NonAckTextBeforeSubscriptionAck {
        context: String,
    },
    ClosedBeforeLoginAck {
        context: String,
    },
    ClosedBeforeSubscriptionAck {
        context: String,
    },
    LoginRejected {
        context: String,
        code: String,
        msg: String,
    },
    TimedOutWaitingForLoginAck {
        context: String,
    },
    TimedOutWaitingForSubscriptionAck {
        context: String,
    },
}

impl fmt::Display for OkxWebsocketProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedJson {
                context,
                parser_error,
            } => write!(
                formatter,
                "failed parsing OKX {context} WebSocket protocol JSON: {parser_error}"
            ),
            Self::MissingSubscribeArg { context } => write!(
                formatter,
                "OKX {context} WebSocket subscribe acknowledgement omitted arg"
            ),
            Self::MissingDataArg { context } => write!(
                formatter,
                "OKX {context} WebSocket data frame omitted arg during subscription ACK wait"
            ),
            Self::UnexpectedSubscriptionAck { context, ack } => write!(
                formatter,
                "OKX {context} WebSocket acknowledged unexpected subscription {ack}"
            ),
            Self::SubscriptionErrorEvent {
                context,
                code,
                msg,
                ack,
            } => match ack {
                Some(ack) => write!(
                    formatter,
                    "OKX {context} WebSocket error {code}: {msg}; arg {ack}"
                ),
                None => write!(formatter, "OKX {context} WebSocket error {code}: {msg}"),
            },
            Self::ServiceUpgradeNotice { context, code } => write!(
                formatter,
                "OKX {context} WebSocket service upgrade notice {code} requires reconnect"
            ),
            Self::UnsupportedNotice { context, code } => write!(
                formatter,
                "OKX {context} WebSocket received unsupported notice code {code}"
            ),
            Self::MalformedNotice { context } => write!(
                formatter,
                "OKX {context} WebSocket notice omitted its required code"
            ),
            Self::DataBeforeSubscriptionAck { context, ack } => write!(
                formatter,
                "OKX {context} WebSocket received data before subscription ACK for {ack}"
            ),
            Self::NonAckTextBeforeSubscriptionAck { context } => write!(
                formatter,
                "OKX {context} WebSocket received non-ACK text before subscription ACK"
            ),
            Self::ClosedBeforeLoginAck { context } => {
                write!(
                    formatter,
                    "OKX {context} WebSocket stream closed before login ACK"
                )
            }
            Self::ClosedBeforeSubscriptionAck { context } => write!(
                formatter,
                "OKX {context} WebSocket stream closed before subscription ACK"
            ),
            Self::LoginRejected { context, code, msg } => {
                write!(
                    formatter,
                    "OKX {context} WebSocket login failed {code}: {msg}"
                )
            }
            Self::TimedOutWaitingForLoginAck { context } => write!(
                formatter,
                "timed out waiting for OKX {context} WebSocket login ACK"
            ),
            Self::TimedOutWaitingForSubscriptionAck { context } => write!(
                formatter,
                "timed out waiting for OKX {context} WebSocket subscription ACK"
            ),
        }
    }
}

impl Error for OkxWebsocketProtocolError {}
