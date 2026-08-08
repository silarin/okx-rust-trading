use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::Value;

use super::{notice::reject_websocket_notice, protocol_error::OkxWebsocketProtocolError};

const OKX_WEBSOCKET_SUBSCRIBE_EVENT: &str = "subscribe";
const OKX_WEBSOCKET_ERROR_EVENT: &str = "error";
const OKX_WEBSOCKET_CHANNEL_CONN_COUNT_EVENT: &str = "channel-conn-count";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct OkxWebsocketSubscriptionAck {
    pub(super) channel: String,
    pub(super) inst_id: Option<String>,
    pub(super) inst_type: Option<String>,
}

#[derive(Debug)]
pub(super) enum OkxWebsocketSubscriptionEvent {
    Acknowledged(OkxWebsocketSubscriptionAck),
    Control,
    Data(OkxWebsocketSubscriptionAck),
    Error {
        code: String,
        msg: String,
        arg: Option<OkxWebsocketSubscriptionAck>,
    },
    Other,
}

pub(super) fn parse_subscription_event(
    payload: &str,
    context: &str,
) -> Result<OkxWebsocketSubscriptionEvent, OkxWebsocketProtocolError> {
    let message: OkxWebsocketSubscriptionMessage =
        serde_json::from_str(payload).map_err(|error| {
            OkxWebsocketProtocolError::MalformedJson {
                context: context.to_owned(),
                parser_error: error.to_string(),
            }
        })?;
    reject_websocket_notice(message.event.as_deref(), message.code.as_deref(), context)?;
    match message.event.as_deref() {
        Some(OKX_WEBSOCKET_SUBSCRIBE_EVENT) => {
            let arg =
                message
                    .arg
                    .ok_or_else(|| OkxWebsocketProtocolError::MissingSubscribeArg {
                        context: context.to_owned(),
                    })?;
            Ok(OkxWebsocketSubscriptionEvent::Acknowledged(arg.into()))
        }
        Some(OKX_WEBSOCKET_ERROR_EVENT) => Ok(OkxWebsocketSubscriptionEvent::Error {
            code: message.code.unwrap_or_default(),
            msg: message.msg.unwrap_or_default(),
            arg: message.arg.map(Into::into),
        }),
        Some(OKX_WEBSOCKET_CHANNEL_CONN_COUNT_EVENT) => Ok(OkxWebsocketSubscriptionEvent::Control),
        None if message.data.is_some() => {
            let arg = message
                .arg
                .ok_or_else(|| OkxWebsocketProtocolError::MissingDataArg {
                    context: context.to_owned(),
                })?;
            Ok(OkxWebsocketSubscriptionEvent::Data(arg.into()))
        }
        Some(_) | None => Ok(OkxWebsocketSubscriptionEvent::Other),
    }
}

pub(super) fn acknowledge_subscription(
    pending: &mut BTreeSet<OkxWebsocketSubscriptionAck>,
    ack: OkxWebsocketSubscriptionAck,
    context: &str,
) -> Result<bool, OkxWebsocketProtocolError> {
    if !pending.remove(&ack) {
        return Err(OkxWebsocketProtocolError::UnexpectedSubscriptionAck {
            context: context.to_owned(),
            ack: Box::new(ack),
        });
    }
    Ok(pending.is_empty())
}

impl std::fmt::Display for OkxWebsocketSubscriptionAck {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "channel={}", self.channel)?;
        if let Some(inst_id) = &self.inst_id {
            write!(formatter, ", instId={inst_id}")?;
        }
        if let Some(inst_type) = &self.inst_type {
            write!(formatter, ", instType={inst_type}")?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct OkxWebsocketSubscriptionMessage {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    arg: Option<OkxWebsocketSubscriptionArg>,
    #[serde(default)]
    data: Option<Value>,
}

#[derive(Deserialize)]
struct OkxWebsocketSubscriptionArg {
    channel: String,
    #[serde(rename = "instId", default)]
    inst_id: Option<String>,
    #[serde(rename = "instType", default)]
    inst_type: Option<String>,
}

impl From<OkxWebsocketSubscriptionArg> for OkxWebsocketSubscriptionAck {
    fn from(arg: OkxWebsocketSubscriptionArg) -> Self {
        Self {
            channel: arg.channel,
            inst_id: arg.inst_id,
            inst_type: arg.inst_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use pretty_assertions::assert_eq;

    use super::*;

    fn ticker_ack(inst_id: &str) -> OkxWebsocketSubscriptionAck {
        OkxWebsocketSubscriptionAck {
            channel: "tickers".to_owned(),
            inst_id: Some(inst_id.to_owned()),
            inst_type: None,
        }
    }

    #[test]
    fn subscription_event_reports_malformed_json_without_raw_payload() {
        let error =
            parse_subscription_event(r#"{"arg":{"apiKey":"secret-key"}"#, "private").unwrap_err();

        assert!(matches!(
            error,
            OkxWebsocketProtocolError::MalformedJson { .. }
        ));
        assert!(!error.to_string().contains("secret-key"));
    }

    #[test]
    fn subscription_event_reports_missing_subscribe_arg() {
        let error = parse_subscription_event(r#"{"event":"subscribe"}"#, "public").unwrap_err();

        assert_eq!(
            error,
            OkxWebsocketProtocolError::MissingSubscribeArg {
                context: "public".to_owned()
            }
        );
    }

    #[test]
    fn subscription_event_reports_missing_data_arg() {
        let error =
            parse_subscription_event(r#"{"data":[{"instId":"BTC-USDT"}]}"#, "public").unwrap_err();

        assert_eq!(
            error,
            OkxWebsocketProtocolError::MissingDataArg {
                context: "public".to_owned()
            }
        );
    }

    #[test]
    fn subscription_event_reports_okx_error_event_metadata() {
        let event = parse_subscription_event(
            r#"{
                "event": "error",
                "code": "60012",
                "msg": "Invalid request",
                "arg": {"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"}
            }"#,
            "private",
        )
        .expect("OKX error event should parse as a typed subscription event");

        let OkxWebsocketSubscriptionEvent::Error { code, msg, arg } = event else {
            panic!("expected OKX subscription error event");
        };

        assert_eq!(code, "60012");
        assert_eq!(msg, "Invalid request");
        assert_eq!(
            arg,
            Some(OkxWebsocketSubscriptionAck {
                channel: "orders".to_owned(),
                inst_id: Some("BTC-USDT".to_owned()),
                inst_type: Some("SPOT".to_owned()),
            })
        );
    }

    #[test]
    fn acknowledge_subscription_reports_wrong_ack_metadata() {
        let mut pending = BTreeSet::from([ticker_ack("BTC-USDT")]);
        let error =
            acknowledge_subscription(&mut pending, ticker_ack("ETH-USDT"), "public").unwrap_err();

        assert_eq!(
            error,
            OkxWebsocketProtocolError::UnexpectedSubscriptionAck {
                context: "public".to_owned(),
                ack: Box::new(ticker_ack("ETH-USDT")),
            }
        );
        assert!(error.to_string().contains("instId=ETH-USDT"));
    }
}
