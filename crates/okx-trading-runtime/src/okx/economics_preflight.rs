use std::{future::Future, time::Duration};

use anyhow::{Context, Result};

use crate::config::{types::BotConfig, validation::okx_simulated_trading_from_routing};

use super::{
    client::OkxRestClient,
    types::{OkxAccountConfig, OkxInstrument, OkxTicker, OkxTradeFeeRate},
    websocket::economics_preflight::{
        OkxEconomicsWebsocketCredentials, probe_private_websocket, probe_public_websocket,
        probe_trading_session,
    },
};

pub(crate) trait OkxEconomicsPreflightSource {
    fn server_time(&self) -> impl Future<Output = Result<()>> + Send;

    fn account_config(&self) -> impl Future<Output = Result<OkxAccountConfig>> + Send;

    fn spot_trade_fee(
        &self,
        instrument_id: &str,
        fee_group_id: &str,
    ) -> impl Future<Output = Result<OkxTradeFeeRate>> + Send;

    fn instrument(&self, instrument_id: &str)
    -> impl Future<Output = Result<OkxInstrument>> + Send;

    fn ticker(&self, instrument_id: &str) -> impl Future<Output = Result<OkxTicker>> + Send;

    fn probe_public_websocket(
        &self,
        instrument_id: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    fn probe_private_websocket(
        &self,
        instrument_id: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    fn probe_trading_session(&self) -> impl Future<Output = Result<()>> + Send;
}

pub(crate) struct OkxEconomicsPreflightClient {
    rest: OkxRestClient,
    public_websocket_url: String,
    private_websocket_url: String,
    websocket_credentials: OkxEconomicsWebsocketCredentials,
    timeout: Duration,
}

impl OkxEconomicsPreflightClient {
    pub(crate) fn new(config: &BotConfig, timeout: Duration) -> Result<Self> {
        let okx = config.okx.as_ref().context("OKX config is required")?;
        let public_websocket_url = okx
            .base_url_ws_public
            .clone()
            .context("OKX public WebSocket URL is required")?;
        let private_websocket_url = okx
            .base_url_ws_private
            .clone()
            .context("OKX private WebSocket URL is required")?;
        let websocket_credentials = OkxEconomicsWebsocketCredentials::new(
            okx.api_key.clone(),
            okx.api_secret.clone(),
            okx.api_passphrase.clone(),
        )?;
        let rest =
            OkxRestClient::new_with_timeout(okx, okx_simulated_trading_from_routing(okx), timeout)?;
        Ok(Self {
            rest,
            public_websocket_url,
            private_websocket_url,
            websocket_credentials,
            timeout,
        })
    }
}

impl OkxEconomicsPreflightSource for OkxEconomicsPreflightClient {
    async fn server_time(&self) -> Result<()> {
        self.rest.economics_preflight_server_time().await
    }

    async fn account_config(&self) -> Result<OkxAccountConfig> {
        self.rest.account_config().await
    }

    async fn spot_trade_fee(
        &self,
        instrument_id: &str,
        fee_group_id: &str,
    ) -> Result<OkxTradeFeeRate> {
        self.rest
            .spot_trade_fee_for_group(instrument_id, fee_group_id)
            .await
    }

    async fn instrument(&self, instrument_id: &str) -> Result<OkxInstrument> {
        self.rest.instruments(instrument_id).await
    }

    async fn ticker(&self, instrument_id: &str) -> Result<OkxTicker> {
        self.rest.ticker(instrument_id).await
    }

    async fn probe_public_websocket(&self, instrument_id: &str) -> Result<()> {
        probe_public_websocket(&self.public_websocket_url, instrument_id, self.timeout).await
    }

    async fn probe_private_websocket(&self, instrument_id: &str) -> Result<()> {
        let login_timestamp = self.rest.websocket_login_timestamp().await?;
        probe_private_websocket(
            &self.private_websocket_url,
            &self.websocket_credentials,
            &login_timestamp,
            instrument_id,
            self.timeout,
        )
        .await
    }

    async fn probe_trading_session(&self) -> Result<()> {
        let login_timestamp = self.rest.websocket_login_timestamp().await?;
        probe_trading_session(
            &self.private_websocket_url,
            &self.websocket_credentials,
            &login_timestamp,
            self.timeout,
        )
        .await
    }
}
