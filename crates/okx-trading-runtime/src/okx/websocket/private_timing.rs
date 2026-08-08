use std::time::Duration;

#[cfg(test)]
use anyhow::{Result, ensure};

use super::{
    OKX_WEBSOCKET_IDLE_PING_AFTER, OKX_WEBSOCKET_IDLE_PONG_TIMEOUT,
    OKX_WEBSOCKET_SUBSCRIPTION_ACK_TIMEOUT,
};

#[cfg(not(test))]
const OKX_PRIVATE_LOGIN_ACK_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const OKX_PRIVATE_LOGIN_ACK_TIMEOUT: Duration = Duration::from_millis(75);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OkxPrivateStreamTiming {
    idle_ping_after: Duration,
    idle_pong_timeout: Duration,
    login_ack_timeout: Duration,
    subscription_ack_timeout: Duration,
}

impl OkxPrivateStreamTiming {
    #[cfg(test)]
    pub(crate) fn new(
        idle_ping_after: Duration,
        idle_pong_timeout: Duration,
        login_ack_timeout: Duration,
        subscription_ack_timeout: Duration,
    ) -> Result<Self> {
        ensure!(
            !idle_ping_after.is_zero()
                && !idle_pong_timeout.is_zero()
                && !login_ack_timeout.is_zero()
                && !subscription_ack_timeout.is_zero(),
            "OKX private WebSocket timing durations must be positive"
        );
        Ok(Self {
            idle_ping_after,
            idle_pong_timeout,
            login_ack_timeout,
            subscription_ack_timeout,
        })
    }

    pub(super) const fn idle_ping_after(self) -> Duration {
        self.idle_ping_after
    }

    pub(super) const fn idle_pong_timeout(self) -> Duration {
        self.idle_pong_timeout
    }

    pub(super) const fn login_ack_timeout(self) -> Duration {
        self.login_ack_timeout
    }

    pub(super) const fn subscription_ack_timeout(self) -> Duration {
        self.subscription_ack_timeout
    }
}

impl Default for OkxPrivateStreamTiming {
    fn default() -> Self {
        Self {
            idle_ping_after: OKX_WEBSOCKET_IDLE_PING_AFTER,
            idle_pong_timeout: OKX_WEBSOCKET_IDLE_PONG_TIMEOUT,
            login_ack_timeout: OKX_PRIVATE_LOGIN_ACK_TIMEOUT,
            subscription_ack_timeout: OKX_WEBSOCKET_SUBSCRIPTION_ACK_TIMEOUT,
        }
    }
}
