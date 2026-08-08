use std::{future::Future, time::Duration};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::config::{types::BotConfig, validation::okx_simulated_trading_from_routing};

use super::{client::OkxRestClient, types::OkxAccountConfig};

pub trait OkxZeroFeePairSelectionSource {
    fn server_time(&self) -> impl Future<Output = Result<()>> + Send;

    fn account_spot_instruments(
        &self,
    ) -> impl Future<Output = Result<Vec<OkxSelectionInstrument>>> + Send;

    fn account_config(&self) -> impl Future<Output = Result<OkxAccountConfig>> + Send;

    fn ticker(
        &self,
        instrument_id: &str,
    ) -> impl Future<Output = Result<OkxSelectionTicker>> + Send;

    fn order_book(
        &self,
        instrument_id: &str,
        depth: usize,
    ) -> impl Future<Output = Result<OkxSelectionOrderBook>> + Send;
}

pub struct OkxZeroFeePairSelectionClient {
    rest: OkxRestClient,
}

impl OkxZeroFeePairSelectionClient {
    pub fn new(config: &BotConfig, timeout: Duration) -> Result<Self> {
        let okx = config.okx.as_ref().context("OKX config is required")?;
        let rest =
            OkxRestClient::new_with_timeout(okx, okx_simulated_trading_from_routing(okx), timeout)?;
        Ok(Self { rest })
    }
}

impl OkxZeroFeePairSelectionSource for OkxZeroFeePairSelectionClient {
    async fn server_time(&self) -> Result<()> {
        self.rest.economics_preflight_server_time().await
    }

    async fn account_spot_instruments(&self) -> Result<Vec<OkxSelectionInstrument>> {
        self.rest.selection_account_spot_instruments().await
    }

    async fn account_config(&self) -> Result<OkxAccountConfig> {
        self.rest.account_config().await
    }

    async fn ticker(&self, instrument_id: &str) -> Result<OkxSelectionTicker> {
        self.rest.selection_ticker(instrument_id).await
    }

    async fn order_book(&self, instrument_id: &str, depth: usize) -> Result<OkxSelectionOrderBook> {
        self.rest.selection_order_book(instrument_id, depth).await
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OkxSelectionInstrument {
    #[serde(rename = "instType", default)]
    pub inst_type: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "baseCcy")]
    pub base_ccy: String,
    #[serde(rename = "quoteCcy")]
    pub quote_ccy: String,
    #[serde(rename = "tradeQuoteCcyList", default)]
    pub trade_quote_currencies: Vec<String>,
    #[serde(rename = "groupId", default)]
    pub group_id: String,
    #[serde(default)]
    pub state: String,
    #[serde(rename = "ruleType", default)]
    pub rule_type: String,
    #[serde(rename = "openType", default)]
    pub open_type: String,
    #[serde(rename = "tickSz")]
    pub tick_size: String,
    #[serde(rename = "lotSz")]
    pub lot_size: String,
    #[serde(rename = "minSz")]
    pub min_size: String,
    #[serde(rename = "upcChg", default)]
    pub upcoming_changes: Vec<OkxSelectionInstrumentChange>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OkxSelectionInstrumentChange {
    #[serde(default)]
    pub param: String,
    #[serde(rename = "newValue", default)]
    pub new_value: String,
    #[serde(rename = "effTime", default)]
    pub effective_time: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OkxSelectionTicker {
    #[serde(rename = "instType", default)]
    pub inst_type: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "bidPx")]
    pub bid_px: String,
    #[serde(rename = "askPx")]
    pub ask_px: String,
    #[serde(rename = "bidSz")]
    pub bid_size: String,
    #[serde(rename = "askSz")]
    pub ask_size: String,
    pub last: String,
    #[serde(rename = "vol24h")]
    pub base_volume_24h: String,
    #[serde(rename = "volCcy24h")]
    pub quote_volume_24h: String,
    pub ts: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OkxSelectionOrderBook {
    pub asks: Vec<Vec<String>>,
    pub bids: Vec<Vec<String>>,
    pub ts: String,
}
