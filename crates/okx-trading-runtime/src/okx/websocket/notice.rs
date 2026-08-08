use super::protocol_error::OkxWebsocketProtocolError;

const OKX_WEBSOCKET_NOTICE_EVENT: &str = "notice";
const OKX_WEBSOCKET_SERVICE_UPGRADE_NOTICE_CODE: &str = "64008";

pub(super) fn reject_websocket_notice(
    event: Option<&str>,
    code: Option<&str>,
    context: &str,
) -> Result<(), OkxWebsocketProtocolError> {
    if event != Some(OKX_WEBSOCKET_NOTICE_EVENT) {
        return Ok(());
    }

    match code {
        Some(OKX_WEBSOCKET_SERVICE_UPGRADE_NOTICE_CODE) => {
            Err(OkxWebsocketProtocolError::ServiceUpgradeNotice {
                context: context.to_owned(),
                code: OKX_WEBSOCKET_SERVICE_UPGRADE_NOTICE_CODE.to_owned(),
            })
        }
        Some(code) if !code.is_empty() => Err(OkxWebsocketProtocolError::UnsupportedNotice {
            context: context.to_owned(),
            code: code.to_owned(),
        }),
        Some(_) | None => Err(OkxWebsocketProtocolError::MalformedNotice {
            context: context.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn exact_service_upgrade_notice_is_a_sanitized_stream_failure() {
        for _ in 0..2 {
            let error = reject_websocket_notice(Some("notice"), Some("64008"), "business")
                .expect_err("every service-upgrade notice must terminate its stream generation");

            assert_eq!(
                error,
                OkxWebsocketProtocolError::ServiceUpgradeNotice {
                    context: "business".to_owned(),
                    code: "64008".to_owned(),
                }
            );
            assert_eq!(
                error.to_string(),
                "OKX business WebSocket service upgrade notice 64008 requires reconnect"
            );
        }
    }

    #[test]
    fn malformed_or_unsupported_notice_is_a_stream_failure() {
        for (code, expected) in [
            (
                None,
                OkxWebsocketProtocolError::MalformedNotice {
                    context: "private".to_owned(),
                },
            ),
            (
                Some(""),
                OkxWebsocketProtocolError::MalformedNotice {
                    context: "private".to_owned(),
                },
            ),
            (
                Some("64009"),
                OkxWebsocketProtocolError::UnsupportedNotice {
                    context: "private".to_owned(),
                    code: "64009".to_owned(),
                },
            ),
        ] {
            assert_eq!(
                reject_websocket_notice(Some("notice"), code, "private")
                    .expect_err("unsupported notice must fail closed"),
                expected
            );
        }
    }

    #[test]
    fn unrelated_control_events_are_not_reclassified() {
        for event in [None, Some("login"), Some("subscribe"), Some("error")] {
            reject_websocket_notice(event, Some("64008"), "private")
                .expect("only event=notice may be classified as a maintenance notice");
        }
    }
}
