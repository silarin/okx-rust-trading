use std::collections::BTreeSet;

use anyhow::{Context, Result, bail, ensure};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Deserializer, de};

use crate::okx::capability::AccountLevelDiagnosticSnapshot;

/// OKX candle data used for signal indicators. OHLC values are `f64` because
/// EMA/ATR signal math is not order-facing; order submission uses OKX string
/// fields parsed into `Decimal` before sizing, pricing, or reconciliation.
#[derive(Clone, Debug, PartialEq)]
pub struct MarketBar {
    pub ts_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub confirm: bool,
}

impl MarketBar {
    pub(crate) fn validate(&self, context: &str) -> Result<()> {
        ensure!(self.ts_ms > 0, "{context} ts must be positive");
        ensure_positive_finite_candle_value(context, "open", self.open)?;
        ensure_positive_finite_candle_value(context, "high", self.high)?;
        ensure_positive_finite_candle_value(context, "low", self.low)?;
        ensure_positive_finite_candle_value(context, "close", self.close)?;
        ensure!(
            self.high >= self.open,
            "{context} high must be at least open"
        );
        ensure!(
            self.high >= self.close,
            "{context} high must be at least close"
        );
        ensure!(self.high >= self.low, "{context} high must be at least low");
        ensure!(self.low <= self.open, "{context} low must be at most open");
        ensure!(
            self.low <= self.close,
            "{context} low must be at most close"
        );
        ensure!(self.low <= self.high, "{context} low must be at most high");
        Ok(())
    }
}

impl<'de> Deserialize<'de> for MarketBar {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<String>::deserialize(deserializer)?;
        if values.len() < 9 {
            return Err(serde::de::Error::custom(
                "OKX candle payload must contain at least 9 fields",
            ));
        }
        let bar = Self {
            ts_ms: parse_field(&values, 0, "ts").map_err(serde::de::Error::custom)?,
            open: parse_field(&values, 1, "open").map_err(serde::de::Error::custom)?,
            high: parse_field(&values, 2, "high").map_err(serde::de::Error::custom)?,
            low: parse_field(&values, 3, "low").map_err(serde::de::Error::custom)?,
            close: parse_field(&values, 4, "close").map_err(serde::de::Error::custom)?,
            confirm: parse_confirm_flag(&values).map_err(serde::de::Error::custom)?,
        };
        bar.validate("OKX candle")
            .map_err(serde::de::Error::custom)?;
        Ok(bar)
    }
}

fn ensure_positive_finite_candle_value(context: &str, name: &str, value: f64) -> Result<()> {
    ensure!(
        value.is_finite() && value > 0.0,
        "{context} {name} must be finite and positive"
    );
    Ok(())
}

fn parse_confirm_flag(values: &[String]) -> std::result::Result<bool, String> {
    match values.get(8).map(String::as_str) {
        Some("0") => Ok(false),
        Some("1") => Ok(true),
        Some(_) => Err("OKX candle confirm flag must be 0 or 1".to_owned()),
        None => Err("missing OKX candle confirm field".to_owned()),
    }
}

fn parse_field<T>(values: &[String], index: usize, name: &str) -> std::result::Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    values
        .get(index)
        .ok_or_else(|| format!("missing OKX candle {name} field"))?
        .parse::<T>()
        .map_err(|err| format!("invalid OKX candle {name} field: {err}"))
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxTicker {
    #[serde(rename = "instType", default)]
    pub inst_type: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "bidPx")]
    pub bid_px: String,
    #[serde(rename = "askPx")]
    pub ask_px: String,
    pub last: String,
}

impl OkxTicker {
    pub fn validate_prices(&self) -> Result<()> {
        self.bid_decimal()?;
        self.ask_decimal()?;
        self.last_decimal()?;
        Ok(())
    }

    pub fn bid_decimal(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX ticker bidPx", &self.bid_px)
    }

    pub fn ask_decimal(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX ticker askPx", &self.ask_px)
    }

    pub fn last_decimal(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX ticker last", &self.last)
    }

    /// Returns the bid as `f64` for signal or threshold checks only. Use
    /// `bid_decimal` when a value can affect OKX order price or size.
    pub fn bid(&self) -> Result<f64> {
        self.bid_decimal()?
            .to_f64()
            .context("OKX ticker bidPx cannot be represented as f64")
    }

    /// Returns the last price as `f64` for signal or threshold checks only. Use
    /// `last_decimal` when a value can affect OKX order price or size.
    pub fn last(&self) -> Result<f64> {
        self.last_decimal()?
            .to_f64()
            .context("OKX ticker last cannot be represented as f64")
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxInstrument {
    #[serde(rename = "instType", default)]
    pub inst_type: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "groupId")]
    pub group_id: String,
    #[serde(
        rename = "instIdCode",
        default,
        deserialize_with = "deserialize_optional_u64_string"
    )]
    pub inst_id_code: Option<u64>,
    #[serde(default)]
    pub state: String,
    #[serde(rename = "baseCcy")]
    pub base_ccy: String,
    #[serde(rename = "quoteCcy")]
    pub quote_ccy: String,
    #[serde(rename = "tradeQuoteCcyList", default)]
    pub trade_quote_currencies: Vec<String>,
    #[serde(rename = "tickSz")]
    pub tick_size: String,
    #[serde(rename = "lotSz")]
    pub lot_size: String,
    #[serde(rename = "minSz")]
    pub min_size: String,
    #[serde(rename = "maxLmtSz", default)]
    pub max_limit_size: String,
    #[serde(rename = "maxLmtAmt", default)]
    pub max_limit_amount: String,
    #[serde(rename = "maxMktSz", default)]
    pub max_market_size: String,
    #[serde(rename = "maxMktAmt", default)]
    pub max_market_amount: String,
    #[serde(rename = "maxTriggerSz", default)]
    pub max_trigger_size: String,
    #[serde(rename = "initPxLmtPct")]
    pub initial_price_limit_pct: String,
    #[serde(rename = "floatPxLmtPct")]
    pub float_price_limit_pct: String,
    #[serde(rename = "maxPxLmtPct")]
    pub maximum_price_limit_pct: String,
}

impl OkxInstrument {
    pub fn fee_group_id(&self) -> Result<&str> {
        validate_fee_group_id("OKX instrument groupId", &self.group_id)?;
        Ok(&self.group_id)
    }

    pub fn websocket_inst_id_code(&self) -> Result<Option<u64>> {
        let Some(inst_id_code) = self.inst_id_code else {
            return Ok(None);
        };
        let inst_id = &self.inst_id;
        ensure!(
            inst_id_code > 0,
            "OKX instrument {inst_id} instIdCode must be positive"
        );
        Ok(Some(inst_id_code))
    }

    pub fn ensure_live(&self) -> Result<()> {
        ensure!(
            self.state == "live",
            "OKX instrument {} state {} is not live",
            self.inst_id,
            self.state
        );
        Ok(())
    }

    pub fn validate_order_limits(&self) -> Result<()> {
        self.max_limit_size()?;
        self.max_limit_amount()?;
        self.max_market_size_usdt()?;
        self.max_market_amount()?;
        self.max_trigger_size()?;
        self.price_limit_percentages()?;
        Ok(())
    }

    pub(crate) fn price_limit_percentages(&self) -> Result<(Option<Decimal>, Decimal, Decimal)> {
        let initial = if self.initial_price_limit_pct.is_empty() {
            None
        } else {
            Some(parse_positive_decimal(
                "OKX instrument initPxLmtPct",
                &self.initial_price_limit_pct,
            )?)
        };
        Ok((
            initial,
            parse_positive_decimal("OKX instrument floatPxLmtPct", &self.float_price_limit_pct)?,
            parse_positive_decimal("OKX instrument maxPxLmtPct", &self.maximum_price_limit_pct)?,
        ))
    }

    pub fn ensure_trade_quote_currency(&self, quote: &str) -> Result<()> {
        let trade_quote_currencies = self.trade_quote_currency_set()?;
        ensure!(
            trade_quote_currencies.contains(quote),
            "OKX instrument {} tradeQuoteCcyList {:?} does not include {quote}",
            self.inst_id,
            self.trade_quote_currencies
        );
        Ok(())
    }

    pub(crate) fn trade_quote_currency_set(&self) -> Result<BTreeSet<&str>> {
        ensure!(
            !self.trade_quote_currencies.is_empty(),
            "OKX instrument {} omitted tradeQuoteCcyList",
            self.inst_id
        );
        let mut currencies = BTreeSet::new();
        for currency in &self.trade_quote_currencies {
            ensure!(
                is_okx_asset_code(currency),
                "OKX instrument {} tradeQuoteCcyList entry {currency:?} must use an uppercase OKX asset code",
                self.inst_id
            );
            ensure!(
                currencies.insert(currency.as_str()),
                "OKX instrument {} tradeQuoteCcyList contains duplicate currency {currency}",
                self.inst_id
            );
        }
        Ok(currencies)
    }

    pub(crate) fn has_usd_order_amount_limit(&self) -> Result<bool> {
        Ok(self.max_limit_amount()?.is_some() || self.max_market_amount()?.is_some())
    }

    pub fn tick_size(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX instrument tickSz", &self.tick_size)
    }

    pub fn lot_size(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX instrument lotSz", &self.lot_size)
    }

    pub fn min_size(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX instrument minSz", &self.min_size)
    }

    pub fn max_limit_size(&self) -> Result<Option<Decimal>> {
        parse_optional_positive_decimal("OKX instrument maxLmtSz", &self.max_limit_size)
    }

    pub fn max_limit_amount(&self) -> Result<Option<Decimal>> {
        parse_optional_positive_decimal("OKX instrument maxLmtAmt", &self.max_limit_amount)
    }

    /// OKX documents `maxMktSz` for SPOT as a quantity in USDT.
    pub fn max_market_size_usdt(&self) -> Result<Option<Decimal>> {
        parse_optional_positive_decimal("OKX instrument maxMktSz", &self.max_market_size)
    }

    pub fn max_market_amount(&self) -> Result<Option<Decimal>> {
        parse_optional_positive_decimal("OKX instrument maxMktAmt", &self.max_market_amount)
    }

    pub fn max_trigger_size(&self) -> Result<Option<Decimal>> {
        parse_optional_positive_decimal("OKX instrument maxTriggerSz", &self.max_trigger_size)
    }

    pub fn ensure_limit_size(&self, size: Decimal, context: &str) -> Result<()> {
        ensure_max_size(context, size, self.max_limit_size()?, "maxLmtSz")
    }

    pub fn ensure_limit_quote_amount(&self, amount: Decimal, context: &str) -> Result<()> {
        let Some(max_limit_amount) = self.max_limit_amount()? else {
            return Ok(());
        };
        ensure!(
            self.quote_ccy == "USD",
            "OKX instrument {} maxLmtAmt is USD-denominated but quoteCcy {} lacks validated USD conversion evidence for {context}",
            self.inst_id,
            self.quote_ccy
        );
        ensure_max_size(context, amount, Some(max_limit_amount), "maxLmtAmt")
    }

    /// Checks a base-currency SPOT market sell against the USDT-denominated
    /// `maxMktSz` using exact Decimal reference-price arithmetic.
    pub fn ensure_spot_market_sell_size(
        &self,
        base_size: Decimal,
        reference_price: Decimal,
        context: &str,
    ) -> Result<()> {
        ensure!(
            self.inst_type == "SPOT",
            "{context} maxMktSz validation requires a SPOT instrument, got {}",
            self.inst_type
        );
        ensure!(base_size > Decimal::ZERO, "{context} must be positive");
        ensure!(
            reference_price > Decimal::ZERO,
            "{context} reference price must be positive"
        );
        let Some(max_market_size_usdt) = self.max_market_size_usdt()? else {
            return Ok(());
        };
        ensure!(
            self.quote_ccy == "USDT",
            "OKX instrument {} maxMktSz is USDT-denominated but quoteCcy {} cannot be converted without authoritative current FX evidence",
            self.inst_id,
            self.quote_ccy
        );
        let usdt_notional = base_size
            .checked_mul(reference_price)
            .with_context(|| format!("{context} USDT notional overflowed Decimal"))?;
        ensure_max_size(
            &format!("{context} USDT notional"),
            usdt_notional,
            Some(max_market_size_usdt),
            "maxMktSz",
        )
    }

    pub fn ensure_market_buy_quote_amount(&self, amount: Decimal, context: &str) -> Result<()> {
        let Some(max_market_amount) = self.max_market_amount()? else {
            return Ok(());
        };
        ensure!(
            self.quote_ccy == "USD",
            "OKX instrument {} maxMktAmt is USD-denominated but quoteCcy {} lacks validated USD conversion evidence for {context}",
            self.inst_id,
            self.quote_ccy
        );
        ensure_max_size(context, amount, Some(max_market_amount), "maxMktAmt")
    }

    pub fn ensure_trigger_size(&self, size: Decimal, context: &str) -> Result<()> {
        ensure_max_size(context, size, self.max_trigger_size()?, "maxTriggerSz")
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub(super) struct OkxIndexTicker {
    #[serde(rename = "instId")]
    pub(super) inst_id: String,
    #[serde(rename = "idxPx")]
    pub(super) index_price: String,
    #[serde(rename = "ts")]
    pub(super) timestamp_ms: String,
}

impl OkxIndexTicker {
    pub(crate) fn price(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX index ticker idxPx", &self.index_price)
    }

    pub(crate) fn timestamp_ms(&self) -> Result<i128> {
        let timestamp_ms = self
            .timestamp_ms
            .parse::<i128>()
            .context("OKX index ticker ts must be Unix milliseconds")?;
        ensure!(
            timestamp_ms > 0,
            "OKX index ticker ts must be positive Unix milliseconds"
        );
        Ok(timestamp_ms)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub(crate) struct OkxPriceLimit {
    #[serde(rename = "instType")]
    pub(crate) inst_type: String,
    #[serde(rename = "instId")]
    pub(crate) inst_id: String,
    #[serde(rename = "buyLmt")]
    pub(crate) buy_limit: String,
    #[serde(rename = "sellLmt")]
    pub(crate) sell_limit: String,
    #[serde(rename = "ts")]
    pub(crate) timestamp_ms: String,
    pub(crate) enabled: bool,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OptionalU64String {
    Integer(u64),
    String(String),
}

fn deserialize_optional_u64_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(value) = Option::<OptionalU64String>::deserialize(deserializer)? else {
        return Ok(None);
    };
    match value {
        OptionalU64String::Integer(value) => Ok(Some(value)),
        OptionalU64String::String(value) => {
            let value = value.trim();
            if value.is_empty() {
                return Ok(None);
            }
            value
                .parse::<u64>()
                .map(Some)
                .map_err(|err| de::Error::custom(format!("instIdCode must be an integer: {err}")))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub const fn as_okx(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderKind {
    Limit,
    Market,
    PostOnly,
}

impl OrderKind {
    pub const fn as_okx(self) -> &'static str {
        match self {
            Self::Limit => "limit",
            Self::Market => "market",
            Self::PostOnly => "post_only",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxOrderAck {
    #[serde(rename = "ordId")]
    pub order_id: String,
    #[serde(rename = "clOrdId")]
    pub client_order_id: String,
    #[serde(rename = "sCode")]
    pub status_code: String,
    #[serde(rename = "sMsg")]
    pub status_message: String,
    #[serde(rename = "subCode", default)]
    pub status_sub_code: String,
    #[serde(rename = "ts", default)]
    pub timestamp: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxAlgoOrderAck {
    #[serde(rename = "algoId")]
    pub algo_id: String,
    #[serde(rename = "algoClOrdId", default)]
    pub client_order_id: String,
    #[serde(rename = "sCode")]
    pub status_code: String,
    #[serde(rename = "sMsg")]
    pub status_message: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxMaximumOrderSize {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(default)]
    pub ccy: String,
    #[serde(rename = "maxBuy")]
    pub max_buy: String,
    #[serde(rename = "maxSell")]
    pub max_sell: String,
}

impl OkxMaximumOrderSize {
    pub(crate) fn ensure_cash_spot_margin_currency(&self, expected_base_ccy: &str) -> Result<()> {
        ensure!(
            self.ccy.is_empty() || self.ccy == expected_base_ccy,
            "OKX cash-SPOT max-size margin currency {:?} must be empty or match validated base currency {expected_base_ccy}",
            self.ccy
        );
        Ok(())
    }

    pub fn max_buy_base(&self) -> Result<Decimal> {
        parse_non_negative_decimal("OKX maximum order size maxBuy", &self.max_buy)
    }

    pub fn max_sell_quote(&self) -> Result<Decimal> {
        parse_non_negative_decimal("OKX maximum order size maxSell", &self.max_sell)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxMaximumAvailableSize {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "availBuy")]
    pub available_buy: String,
    #[serde(rename = "availSell")]
    pub available_sell: String,
}

impl OkxMaximumAvailableSize {
    pub fn available_buy_quote(&self) -> Result<Decimal> {
        parse_non_negative_decimal(
            "OKX maximum available tradable amount availBuy",
            &self.available_buy,
        )
    }

    pub fn available_sell_base(&self) -> Result<Decimal> {
        parse_non_negative_decimal(
            "OKX maximum available tradable amount availSell",
            &self.available_sell,
        )
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxOrder {
    #[serde(rename = "instType", default)]
    pub inst_type: String,
    #[serde(rename = "instId", default)]
    pub inst_id: String,
    #[serde(rename = "ordId")]
    pub order_id: String,
    #[serde(rename = "clOrdId")]
    pub client_order_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(rename = "ordType", default)]
    pub order_type: String,
    #[serde(rename = "px", default)]
    pub price: String,
    pub state: String,
    #[serde(rename = "avgPx")]
    pub average_price: String,
    #[serde(rename = "accFillSz")]
    pub accumulated_fill_size: String,
    #[serde(default)]
    pub fee: String,
    #[serde(rename = "feeCcy", default)]
    pub fee_currency: String,
    #[serde(default)]
    pub rebate: String,
    #[serde(rename = "rebateCcy", default)]
    pub rebate_currency: String,
    pub sz: String,
    #[serde(rename = "cTime", default)]
    pub created_at_ms: String,
    #[serde(rename = "uTime", default)]
    pub updated_at_ms: String,
}

impl OkxOrder {
    pub fn fill_size(&self) -> Result<Decimal> {
        parse_non_negative_decimal("OKX order accFillSz", &self.accumulated_fill_size)
    }

    pub fn average_fill_price(&self) -> Result<Option<Decimal>> {
        if self.average_price.trim().is_empty() {
            return Ok(None);
        }
        let average_price = parse_non_negative_decimal("OKX order avgPx", &self.average_price)?;
        if average_price == Decimal::ZERO {
            ensure!(
                self.fill_size()? == Decimal::ZERO,
                "OKX order avgPx must be positive when accFillSz is positive"
            );
            return Ok(None);
        }
        Ok(Some(average_price))
    }

    pub fn cumulative_spot_accounting(
        &self,
        base_currency: &str,
        quote_currency: &str,
    ) -> Result<OkxSpotFillAccounting> {
        let fill_size = self.fill_size()?;
        if fill_size == Decimal::ZERO {
            return Ok(OkxSpotFillAccounting::default());
        }
        let fill_price = self
            .average_fill_price()?
            .context("OKX filled order is missing avgPx")?;
        let mut accounting = spot_fill_accounting(
            "OKX order",
            self.parsed_side(),
            fill_size,
            fill_price,
            (&self.fee, &self.fee_currency),
            base_currency,
            quote_currency,
        )?;
        apply_spot_rebate(
            "OKX order",
            &mut accounting,
            &self.rebate,
            &self.rebate_currency,
            base_currency,
            quote_currency,
        )?;
        ensure_spot_accounting_direction("OKX order", self.parsed_side(), accounting)?;
        Ok(accounting)
    }

    pub fn created_at_ms(&self) -> Result<i64> {
        parse_timestamp_ms("OKX order cTime", &self.created_at_ms)
    }

    pub fn is_live(&self) -> bool {
        matches!(self.state.as_str(), "live" | "partially_filled")
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self.state.as_str(), "filled" | "canceled" | "mmp_canceled")
    }

    pub fn is_filled(&self) -> bool {
        self.state == "filled"
    }

    pub fn is_terminal_without_fill(&self) -> bool {
        matches!(self.state.as_str(), "canceled" | "mmp_canceled")
    }

    pub fn ensure_documented_state(&self, context: &str) -> Result<()> {
        ensure!(
            self.is_live() || self.is_terminal(),
            "OKX {context} returned order {} with undocumented state {:?}",
            self.order_id,
            self.state
        );
        Ok(())
    }

    pub fn requested_size(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX order sz", &self.sz)
    }

    pub fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
            .parse::<i64>()
            .or_else(|_| self.created_at_ms.parse::<i64>())
            .unwrap_or_default()
    }

    pub fn parsed_side(&self) -> Option<OrderSide> {
        match self.side.as_str() {
            "buy" => Some(OrderSide::Buy),
            "sell" => Some(OrderSide::Sell),
            _ => None,
        }
    }

    pub fn parsed_kind(&self) -> Option<OrderKind> {
        match self.order_type.as_str() {
            "limit" => Some(OrderKind::Limit),
            "market" => Some(OrderKind::Market),
            "post_only" => Some(OrderKind::PostOnly),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxFill {
    #[serde(rename = "instType", default)]
    pub inst_type: String,
    #[serde(rename = "instId", default)]
    pub inst_id: String,
    #[serde(rename = "ordId", default)]
    pub order_id: String,
    #[serde(rename = "clOrdId", default)]
    pub client_order_id: String,
    #[serde(rename = "billId", default)]
    pub bill_id: String,
    #[serde(rename = "tradeId", default)]
    pub trade_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(rename = "fillSz")]
    pub fill_size: String,
    #[serde(rename = "fillPx")]
    pub fill_price: String,
    #[serde(default, alias = "fillFee")]
    pub fee: String,
    #[serde(rename = "feeCcy", alias = "fillFeeCcy", default)]
    pub fee_currency: String,
    #[serde(rename = "feeRate", default)]
    pub fee_rate: String,
    #[serde(rename = "execType", default)]
    pub execution_type: String,
    #[serde(rename = "fillTime", default)]
    pub fill_time_ms: String,
    #[serde(rename = "ts", default)]
    pub event_time_ms: String,
}

impl OkxFill {
    pub fn fill_size(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX fill fillSz", &self.fill_size)
    }

    pub fn fill_price(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX fill fillPx", &self.fill_price)
    }

    pub fn spot_accounting(
        &self,
        base_currency: &str,
        quote_currency: &str,
    ) -> Result<OkxSpotFillAccounting> {
        spot_fill_accounting(
            "OKX fill",
            self.parsed_side(),
            self.fill_size()?,
            self.fill_price()?,
            (&self.fee, &self.fee_currency),
            base_currency,
            quote_currency,
        )
    }

    pub fn fill_time_ms(&self) -> i64 {
        self.preferred_fill_time_ms()
            .parse::<i64>()
            .unwrap_or_default()
    }

    pub fn parsed_side(&self) -> Option<OrderSide> {
        match self.side.as_str() {
            "buy" => Some(OrderSide::Buy),
            "sell" => Some(OrderSide::Sell),
            _ => None,
        }
    }

    pub fn dedupe_key(&self) -> String {
        if !self.bill_id.trim().is_empty() {
            return self.bill_id.clone();
        }
        if !self.trade_id.trim().is_empty() {
            return self.trade_id.clone();
        }
        if self.order_id.trim().is_empty()
            && self.client_order_id.trim().is_empty()
            && self.side.trim().is_empty()
            && self.preferred_fill_time_ms().trim().is_empty()
            && self.fill_size.trim().is_empty()
        {
            return String::new();
        }
        format!(
            "{}:{}:{}:{}:{}",
            self.order_id,
            self.client_order_id,
            self.side,
            self.preferred_fill_time_ms(),
            self.fill_size
        )
    }

    fn preferred_fill_time_ms(&self) -> &str {
        if self.fill_time_ms.trim().is_empty() {
            &self.event_time_ms
        } else {
            &self.fill_time_ms
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxAlgoOrder {
    #[serde(rename = "instType", default)]
    pub inst_type: String,
    #[serde(rename = "instId", default)]
    pub inst_id: String,
    #[serde(rename = "tdMode", default)]
    pub td_mode: String,
    #[serde(rename = "algoId")]
    pub algo_id: String,
    #[serde(rename = "algoClOrdId", default)]
    pub client_order_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(rename = "ordType", default)]
    pub order_type: String,
    #[serde(rename = "triggerPx", default)]
    pub trigger_price: String,
    #[serde(rename = "orderPx", alias = "ordPx", default)]
    pub order_price: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub sz: String,
    #[serde(rename = "cTime", default)]
    pub created_at_ms: String,
    #[serde(rename = "uTime", default)]
    pub updated_at_ms: String,
}

impl OkxAlgoOrder {
    pub fn is_live(&self) -> bool {
        matches!(self.state.as_str(), "live" | "pause")
    }

    pub fn is_effective(&self) -> bool {
        self.state == "effective"
    }

    pub fn is_terminal_without_execution(&self) -> bool {
        matches!(self.state.as_str(), "canceled" | "order_failed")
    }

    pub fn ensure_documented_state(&self, context: &str) -> Result<()> {
        ensure!(
            matches!(
                self.state.as_str(),
                "live"
                    | "pause"
                    | "effective"
                    | "canceled"
                    | "order_failed"
                    | "partially_effective"
                    | "partially_failed"
            ),
            "OKX {context} returned algo {} with undocumented state {:?}",
            self.algo_id,
            self.state
        );
        Ok(())
    }

    pub fn trigger_price(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX algo order triggerPx", &self.trigger_price)
    }

    pub fn requested_size(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX algo order sz", &self.sz)
    }

    pub fn parsed_side(&self) -> Option<OrderSide> {
        match self.side.as_str() {
            "buy" => Some(OrderSide::Buy),
            "sell" => Some(OrderSide::Sell),
            _ => None,
        }
    }

    pub fn is_trigger_market_order(&self) -> bool {
        self.order_type == "trigger" && self.order_price == "-1"
    }
}

/// REST-authoritative representation of the subset of a standalone SPOT OCO
/// needed by the bounded Demo contract test. The trading strategy does not use
/// this type.
#[cfg(test)]
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub(crate) struct OkxOcoOrder {
    #[serde(rename = "instType", default)]
    pub(crate) inst_type: String,
    #[serde(rename = "instId", default)]
    pub(crate) inst_id: String,
    #[serde(rename = "algoId")]
    pub(crate) algo_id: String,
    #[serde(rename = "algoClOrdId", default)]
    pub(crate) client_order_id: String,
    #[serde(rename = "ordId", default)]
    pub(crate) order_id: String,
    #[serde(default)]
    pub(crate) side: String,
    #[serde(rename = "ordType", default)]
    pub(crate) order_type: String,
    #[serde(default)]
    pub(crate) state: String,
    #[serde(default)]
    pub(crate) sz: String,
    #[serde(rename = "tpTriggerPx", default)]
    pub(crate) take_profit_trigger_price: String,
    #[serde(rename = "tpTriggerPxType", default)]
    pub(crate) take_profit_trigger_price_type: String,
    #[serde(rename = "tpOrdPx", default)]
    pub(crate) take_profit_order_price: String,
    #[serde(rename = "slTriggerPx", default)]
    pub(crate) stop_loss_trigger_price: String,
    #[serde(rename = "slTriggerPxType", default)]
    pub(crate) stop_loss_trigger_price_type: String,
    #[serde(rename = "slOrdPx", default)]
    pub(crate) stop_loss_order_price: String,
    #[serde(rename = "actualSide", default)]
    pub(crate) actual_side: String,
    #[serde(rename = "actualSz", default)]
    pub(crate) actual_size: String,
    #[serde(rename = "actualPx", default)]
    pub(crate) actual_price: String,
    #[serde(default)]
    pub(crate) tag: String,
    #[serde(rename = "cTime", default)]
    pub(crate) created_at_ms: String,
    #[serde(rename = "uTime", default)]
    pub(crate) updated_at_ms: String,
}

#[cfg(test)]
impl OkxOcoOrder {
    pub(crate) fn ensure_contract(&self, context: &str) -> Result<()> {
        ensure!(
            self.inst_type == "SPOT",
            "OKX {context} returned OCO {} with instType {:?}; expected SPOT",
            self.algo_id,
            self.inst_type
        );
        ensure!(
            self.order_type == "oco" && self.side == "sell",
            "OKX {context} returned algo {} with ordType {:?} and side {:?}; expected a sell OCO",
            self.algo_id,
            self.order_type,
            self.side
        );
        ensure!(
            !self.algo_id.trim().is_empty()
                && !self.client_order_id.trim().is_empty()
                && !self.inst_id.trim().is_empty(),
            "OKX {context} returned OCO without stable instrument, algo, and client identifiers"
        );
        ensure!(
            matches!(
                self.state.as_str(),
                "live"
                    | "pause"
                    | "effective"
                    | "canceled"
                    | "order_failed"
                    | "partially_effective"
                    | "partially_failed"
            ),
            "OKX {context} returned OCO {} with undocumented state {:?}",
            self.algo_id,
            self.state
        );
        self.requested_size()?;
        let take_profit = self.take_profit_trigger_price()?;
        let stop_loss = self.stop_loss_trigger_price()?;
        ensure!(
            take_profit > stop_loss,
            "OKX {context} returned sell OCO {} with take-profit trigger {take_profit} not above stop-loss trigger {stop_loss}",
            self.algo_id
        );
        ensure!(
            self.take_profit_trigger_price_type == "last"
                && self.stop_loss_trigger_price_type == "last",
            "OKX {context} returned OCO {} with trigger types {:?}/{:?}; expected last/last",
            self.algo_id,
            self.take_profit_trigger_price_type,
            self.stop_loss_trigger_price_type
        );
        ensure!(
            self.take_profit_order_price == "-1" && self.stop_loss_order_price == "-1",
            "OKX {context} returned OCO {} with order prices {:?}/{:?}; the bounded Demo contract requires market execution on both legs",
            self.algo_id,
            self.take_profit_order_price,
            self.stop_loss_order_price
        );
        Ok(())
    }

    pub(crate) fn requested_size(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX OCO sz", &self.sz)
    }

    pub(crate) fn take_profit_trigger_price(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX OCO tpTriggerPx", &self.take_profit_trigger_price)
    }

    pub(crate) fn stop_loss_trigger_price(&self) -> Result<Decimal> {
        parse_positive_decimal("OKX OCO slTriggerPx", &self.stop_loss_trigger_price)
    }

    pub(crate) fn is_pending(&self) -> bool {
        matches!(self.state.as_str(), "live" | "pause")
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self.state.as_str(),
            "effective" | "canceled" | "order_failed" | "partially_effective" | "partially_failed"
        )
    }

    pub(crate) fn ensure_clean_execution(&self, expected_side: &str) -> Result<()> {
        ensure!(
            self.state == "effective",
            "OKX OCO {} ended in state {:?}; expected effective",
            self.algo_id,
            self.state
        );
        ensure!(
            self.actual_side == expected_side,
            "OKX OCO {} executed side {:?}; expected {expected_side}",
            self.algo_id,
            self.actual_side
        );
        let actual_size = parse_positive_decimal("OKX OCO actualSz", &self.actual_size)?;
        ensure!(
            actual_size == self.requested_size()?,
            "OKX OCO {} executed size {actual_size} but protected size was {}; partial execution is not sufficient evidence for the proposed offload",
            self.algo_id,
            self.sz
        );
        parse_positive_decimal("OKX OCO actualPx", &self.actual_price)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxBalance {
    pub details: Vec<OkxBalanceDetail>,
}

impl OkxBalance {
    pub fn validate(&self) -> Result<()> {
        for detail in &self.details {
            ensure!(
                !detail.ccy.trim().is_empty(),
                "OKX balance ccy must be provided"
            );
            detail.available()?;
            detail.total()?;
            detail.frozen()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxBalanceDetail {
    pub ccy: String,
    #[serde(rename = "availBal")]
    pub available_balance: String,
    #[serde(rename = "cashBal", default)]
    pub cash_balance: String,
    #[serde(rename = "frozenBal", default)]
    pub frozen_balance: String,
}

impl OkxBalanceDetail {
    pub fn total(&self) -> Result<Decimal> {
        ensure!(
            !self.cash_balance.trim().is_empty(),
            "OKX balance cashBal must be provided"
        );
        parse_non_negative_decimal("OKX balance cashBal", &self.cash_balance)
    }

    pub fn available(&self) -> Result<Decimal> {
        parse_non_negative_decimal("OKX balance availBal", &self.available_balance)
    }

    pub fn frozen(&self) -> Result<Decimal> {
        ensure!(
            !self.frozen_balance.trim().is_empty(),
            "OKX balance frozenBal must be provided"
        );
        parse_non_negative_decimal("OKX balance frozenBal", &self.frozen_balance)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxAccountConfig {
    pub uid: String,
    #[serde(rename = "mainUid")]
    pub main_uid: String,
    #[serde(rename = "acctLv")]
    pub account_level: String,
    pub perm: String,
    #[serde(rename = "autoLoan")]
    pub auto_loan: bool,
    #[serde(rename = "enableSpotBorrow")]
    pub enable_spot_borrow: bool,
    #[serde(rename = "spotBorrowAutoRepay")]
    pub spot_borrow_auto_repay: bool,
    #[serde(rename = "feeType")]
    pub fee_type: String,
    #[serde(rename = "kycLv", default)]
    pub kyc_level: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedLiveKycLevel {
    LevelTwo,
    LevelThree,
}

impl ValidatedLiveKycLevel {
    pub(crate) const fn as_okx(self) -> &'static str {
        match self {
            Self::LevelTwo => "2",
            Self::LevelThree => "3",
        }
    }
}

impl OkxAccountConfig {
    pub fn ensure_spot_trading_enabled(&self) -> Result<()> {
        AccountLevelDiagnosticSnapshot::observe(self)?;
        ensure!(
            self.has_trade_permission(),
            "OKX API key permissions {:?} do not include trade",
            self.perm
        );
        ensure!(
            !self.auto_loan && !self.enable_spot_borrow && !self.spot_borrow_auto_repay,
            "OKX account borrow/auto-loan settings are enabled; this SPOT-only runtime requires cash trading without borrowing"
        );
        self.spot_fee_type()?;
        Ok(())
    }

    pub fn ensure_spot_economics_safe(&self) -> Result<()> {
        AccountLevelDiagnosticSnapshot::observe(self)?;
        ensure!(
            !self.auto_loan && !self.enable_spot_borrow && !self.spot_borrow_auto_repay,
            "OKX account borrow/auto-loan settings are enabled; this SPOT-only runtime requires cash trading without borrowing"
        );
        self.spot_fee_type()?;
        Ok(())
    }

    pub(crate) fn validated_live_kyc_level(&self) -> Result<ValidatedLiveKycLevel> {
        match self.kyc_level.as_str() {
            "2" => Ok(ValidatedLiveKycLevel::LevelTwo),
            "3" => Ok(ValidatedLiveKycLevel::LevelThree),
            _ => bail!(
                "Production order placement requires OKX kycLv 2 or 3; missing, malformed, unverified, or level-1 evidence is ineligible"
            ),
        }
    }

    pub fn spot_fee_type(&self) -> Result<OkxSpotFeeType> {
        match self.fee_type.as_str() {
            "0" => Ok(OkxSpotFeeType::ReceivedCurrency),
            "1" => Ok(OkxSpotFeeType::QuoteCurrency),
            _ => bail!(
                "OKX account feeType {:?} is unsupported; expected 0 (received currency) or 1 (quote currency)",
                self.fee_type
            ),
        }
    }

    fn has_trade_permission(&self) -> bool {
        self.perm
            .split(',')
            .map(str::trim)
            .any(|permission| permission.eq_ignore_ascii_case("trade"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxSpotFeeType {
    ReceivedCurrency,
    QuoteCurrency,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct OkxSpotFillAccounting {
    pub base_change: Decimal,
    pub quote_change: Decimal,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub(crate) struct OkxTradeFeeResponse {
    #[serde(rename = "instType")]
    inst_type: String,
    level: String,
    #[serde(rename = "feeGroup")]
    fee_groups: Vec<OkxTradeFeeGroup>,
    #[serde(default)]
    ts: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct OkxTradeFeeGroup {
    #[serde(rename = "groupId")]
    group_id: String,
    maker: String,
    taker: String,
}

impl OkxTradeFeeResponse {
    pub(crate) fn into_spot_group_rate(
        self,
        inst_id: &str,
        expected_group_id: &str,
    ) -> Result<OkxTradeFeeRate> {
        ensure!(
            self.inst_type == "SPOT",
            "OKX fee-rate response returned instType {} for {inst_id}; expected SPOT",
            self.inst_type
        );
        validate_fee_group_id("validated OKX instrument groupId", expected_group_id)?;

        let mut matching = self
            .fee_groups
            .into_iter()
            .filter(|group| group.group_id == expected_group_id)
            .collect::<Vec<_>>();
        ensure!(
            matching.len() == 1,
            "OKX returned {} feeGroup rows matching groupId {expected_group_id} for SPOT {inst_id}",
            matching.len()
        );
        let group = matching.remove(0);
        let rate = OkxTradeFeeRate {
            inst_type: self.inst_type,
            level: self.level,
            group_id: group.group_id,
            maker: group.maker,
            taker: group.taker,
            ts: self.ts,
        };
        rate.normalized_maker_cost_rate()?;
        rate.normalized_taker_cost_rate()?;
        Ok(rate)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct OkxTradeFeeRate {
    pub inst_type: String,
    pub level: String,
    pub group_id: String,
    pub maker: String,
    pub taker: String,
    pub ts: String,
}

impl OkxTradeFeeRate {
    pub fn ensure_spot(&self, inst_id: &str) -> Result<()> {
        ensure!(
            self.inst_type == "SPOT",
            "OKX fee-rate response returned instType {} for {inst_id}; expected SPOT",
            self.inst_type
        );
        validate_fee_group_id("OKX fee rate groupId", &self.group_id)?;
        Ok(())
    }

    pub fn round_trip_commission_rate(&self) -> Result<Decimal> {
        Ok(commission_rate("OKX fee maker", &self.maker)?
            + commission_rate("OKX fee taker", &self.taker)?)
    }

    /// Converts OKX's sign convention into positive cost and negative rebate.
    pub fn normalized_maker_cost_rate(&self) -> Result<Decimal> {
        normalized_cost_rate("OKX fee maker", &self.maker)
    }

    /// Converts OKX's sign convention into positive cost and negative rebate.
    pub fn normalized_taker_cost_rate(&self) -> Result<Decimal> {
        normalized_cost_rate("OKX fee taker", &self.taker)
    }

    pub fn ensure_round_trip_commission_at_most(
        &self,
        inst_id: &str,
        max_round_trip_fee_rate: Decimal,
    ) -> Result<()> {
        let round_trip_commission_rate = self.round_trip_commission_rate()?;
        ensure!(
            round_trip_commission_rate <= max_round_trip_fee_rate,
            "OKX SPOT {inst_id} round-trip commission rate {round_trip_commission_rate} exceeds strategy fee assumption {max_round_trip_fee_rate}"
        );
        Ok(())
    }

    pub fn ensure_commissions_at_most(
        &self,
        inst_id: &str,
        max_maker_fee_rate: Decimal,
        max_taker_fee_rate: Decimal,
    ) -> Result<()> {
        let maker_commission_rate = commission_rate("OKX fee maker", &self.maker)?;
        let taker_commission_rate = commission_rate("OKX fee taker", &self.taker)?;
        ensure!(
            maker_commission_rate <= max_maker_fee_rate,
            "OKX SPOT {inst_id} maker commission rate {maker_commission_rate} exceeds strategy maker fee assumption {max_maker_fee_rate}"
        );
        ensure!(
            taker_commission_rate <= max_taker_fee_rate,
            "OKX SPOT {inst_id} taker commission rate {taker_commission_rate} exceeds strategy taker fee assumption {max_taker_fee_rate}"
        );
        Ok(())
    }
}

pub fn quantize_decimal_down(value: Decimal, step: Decimal) -> Result<Decimal> {
    ensure!(value > Decimal::ZERO, "value must be positive");
    Ok((value / step).floor() * step)
}

pub fn quantize_decimal_up(value: Decimal, step: Decimal) -> Result<Decimal> {
    ensure!(value > Decimal::ZERO, "value must be positive");
    Ok((value / step).ceil() * step)
}

pub fn decimal_to_okx(value: Decimal) -> String {
    value.normalize().to_string()
}

fn parse_positive_decimal(context: &str, value: &str) -> Result<Decimal> {
    let parsed = value
        .parse::<Decimal>()
        .with_context(|| format!("{context} must be a decimal"))?;
    ensure!(parsed > Decimal::ZERO, "{context} must be positive");
    Ok(parsed)
}

fn validate_fee_group_id(context: &str, value: &str) -> Result<()> {
    ensure!(!value.is_empty(), "{context} must be provided");
    ensure!(
        value == value.trim(),
        "{context} must not contain surrounding whitespace"
    );
    ensure!(
        value.bytes().all(|byte| byte.is_ascii_digit()),
        "{context} must contain only ASCII decimal digits"
    );
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{context} must fit in an unsigned 64-bit integer"))?;
    ensure!(parsed > 0, "{context} must be positive");
    Ok(())
}

fn is_okx_asset_code(value: &str) -> bool {
    matches!(value.len(), 2..=12)
        && value
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
}

fn parse_non_negative_decimal(context: &str, value: &str) -> Result<Decimal> {
    let parsed = parse_decimal(context, value)?;
    ensure!(parsed >= Decimal::ZERO, "{context} must be non-negative");
    Ok(parsed)
}

fn parse_decimal(context: &str, value: &str) -> Result<Decimal> {
    value
        .parse::<Decimal>()
        .with_context(|| format!("{context} must be a decimal"))
}

fn parse_timestamp_ms(context: &str, value: &str) -> Result<i64> {
    let timestamp = value
        .parse::<i64>()
        .with_context(|| format!("{context} must be an integer timestamp"))?;
    ensure!(timestamp > 0, "{context} must be positive");
    Ok(timestamp)
}

fn spot_fill_accounting(
    context: &str,
    side: Option<OrderSide>,
    fill_size: Decimal,
    fill_price: Decimal,
    fee: (&str, &str),
    base_currency: &str,
    quote_currency: &str,
) -> Result<OkxSpotFillAccounting> {
    ensure!(
        !base_currency.trim().is_empty() && !quote_currency.trim().is_empty(),
        "{context} SPOT accounting requires base and quote currencies"
    );
    ensure!(
        base_currency != quote_currency,
        "{context} SPOT accounting requires distinct base and quote currencies"
    );
    let side = side.with_context(|| format!("{context} has undocumented side"))?;
    let fee_amount = parse_decimal(&format!("{context} fee"), fee.0)?;

    let notional = fill_size * fill_price;
    let (base_change, quote_change) = match side {
        OrderSide::Buy => (fill_size, -notional),
        OrderSide::Sell => (-fill_size, notional),
    };
    let mut accounting = OkxSpotFillAccounting {
        base_change,
        quote_change,
    };
    apply_spot_currency_adjustment(
        context,
        "feeCcy",
        &mut accounting,
        fee_amount,
        fee.1,
        base_currency,
        quote_currency,
    )?;
    ensure_spot_accounting_direction(context, Some(side), accounting)?;
    Ok(accounting)
}

fn apply_spot_rebate(
    context: &str,
    accounting: &mut OkxSpotFillAccounting,
    rebate: &str,
    rebate_currency: &str,
    base_currency: &str,
    quote_currency: &str,
) -> Result<()> {
    let rebate = if rebate.trim().is_empty() {
        Decimal::ZERO
    } else {
        parse_non_negative_decimal(&format!("{context} rebate"), rebate)?
    };
    if rebate == Decimal::ZERO && rebate_currency.trim().is_empty() {
        return Ok(());
    }
    apply_spot_currency_adjustment(
        context,
        "rebateCcy",
        accounting,
        rebate,
        rebate_currency,
        base_currency,
        quote_currency,
    )
}

fn apply_spot_currency_adjustment(
    context: &str,
    field: &str,
    accounting: &mut OkxSpotFillAccounting,
    amount: Decimal,
    currency: &str,
    base_currency: &str,
    quote_currency: &str,
) -> Result<()> {
    ensure!(
        currency == base_currency || currency == quote_currency,
        "{context} {field} {currency:?} does not match base {base_currency} or quote {quote_currency}"
    );
    if currency == base_currency {
        accounting.base_change += amount;
    } else {
        accounting.quote_change += amount;
    }
    Ok(())
}

fn ensure_spot_accounting_direction(
    context: &str,
    side: Option<OrderSide>,
    accounting: OkxSpotFillAccounting,
) -> Result<()> {
    match side.with_context(|| format!("{context} has undocumented side"))? {
        OrderSide::Buy => ensure!(
            accounting.base_change > Decimal::ZERO && accounting.quote_change < Decimal::ZERO,
            "{context} buy accounting must receive positive base and spend positive quote"
        ),
        OrderSide::Sell => ensure!(
            accounting.base_change < Decimal::ZERO && accounting.quote_change > Decimal::ZERO,
            "{context} sell accounting must spend positive base and receive positive quote"
        ),
    }
    Ok(())
}

fn commission_rate(context: &str, value: &str) -> Result<Decimal> {
    let fee_rate = parse_decimal(context, value)?;
    if fee_rate < Decimal::ZERO {
        Ok(-fee_rate)
    } else {
        Ok(Decimal::ZERO)
    }
}

fn normalized_cost_rate(context: &str, value: &str) -> Result<Decimal> {
    Ok(-parse_decimal(context, value)?)
}

fn parse_optional_positive_decimal(context: &str, value: &str) -> Result<Option<Decimal>> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_positive_decimal(context, value)?))
}

fn ensure_max_size(
    context: &str,
    size: Decimal,
    max_size: Option<Decimal>,
    okx_field: &str,
) -> Result<()> {
    let Some(max_size) = max_size else {
        return Ok(());
    };
    ensure!(
        size <= max_size,
        "{context} {size} exceeds OKX {okx_field} {max_size}"
    );
    Ok(())
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
