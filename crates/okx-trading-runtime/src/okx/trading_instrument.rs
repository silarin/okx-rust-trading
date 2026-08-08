use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;

use crate::{
    config::{
        types::{RequestedInstrumentType, RequestedTradeMode, RequestedTradingInstrument},
        validation::validate_requested_trading_instrument,
    },
    okx::types::{
        OkxAccountConfig, OkxBalance, OkxIndexTicker, OkxInstrument, OkxMaximumAvailableSize,
        OkxMaximumOrderSize, OkxPriceLimit, OrderSide,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidatedInstrumentType {
    Spot,
}

impl ValidatedInstrumentType {
    pub const fn as_okx(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidatedTradeMode {
    Cash,
}

impl ValidatedTradeMode {
    pub const fn as_okx(self) -> &'static str {
        match self {
            Self::Cash => "cash",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ValidatedQuoteUsdSource {
    Identity,
    Index(String),
}

#[derive(Clone, Debug)]
pub struct ValidatedQuoteUsdRate {
    quote_ccy: String,
    source: ValidatedQuoteUsdSource,
    usd_per_quote: Decimal,
    source_timestamp_ms: Option<i128>,
}

/// Fresh, exact OKX public price-limit evidence for one validated SPOT
/// instrument. Only the exchange-validation workflow can construct it.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedSpotPriceLimit {
    inst_id: String,
    buy_limit: Option<Decimal>,
    sell_limit: Option<Decimal>,
    source_timestamp_ms: i128,
}

impl ValidatedSpotPriceLimit {
    pub(super) fn from_response(
        expected_inst_id: &str,
        row: OkxPriceLimit,
        server_now_ms: i128,
        max_staleness: Duration,
    ) -> Result<Self> {
        ensure!(
            matches!(row.inst_type.as_str(), "SPOT" | "MARGIN"),
            "OKX price-limit response for {expected_inst_id} returned unsupported instType {}",
            row.inst_type
        );
        ensure!(
            row.inst_id == expected_inst_id,
            "OKX price-limit response returned {} for requested {expected_inst_id}",
            row.inst_id
        );
        ensure!(
            server_now_ms > 0,
            "synchronized OKX server time must be positive"
        );
        let source_timestamp_ms = row
            .timestamp_ms
            .parse::<i128>()
            .context("OKX price-limit ts must be Unix milliseconds")?;
        ensure!(
            source_timestamp_ms > 0,
            "OKX price-limit ts must be positive"
        );
        ensure!(
            source_timestamp_ms <= server_now_ms,
            "OKX price-limit ts is in the future by {} ms",
            source_timestamp_ms - server_now_ms
        );
        let age_ms = server_now_ms
            .checked_sub(source_timestamp_ms)
            .context("OKX price-limit timestamp age overflowed")?;
        let max_staleness_ms = i128::try_from(max_staleness.as_millis())
            .context("OKX price-limit maximum staleness is out of range")?;
        ensure!(
            age_ms <= max_staleness_ms,
            "OKX price-limit ts is stale by {age_ms} ms; maximum age is {max_staleness_ms} ms"
        );

        let (buy_limit, sell_limit) = if row.enabled {
            (
                Some(parse_positive_price_limit("buyLmt", &row.buy_limit)?),
                Some(parse_positive_price_limit("sellLmt", &row.sell_limit)?),
            )
        } else {
            ensure!(
                row.buy_limit.is_empty() && row.sell_limit.is_empty(),
                "disabled OKX price-limit evidence for {expected_inst_id} must return empty buyLmt and sellLmt"
            );
            (None, None)
        };

        Ok(Self {
            inst_id: row.inst_id,
            buy_limit,
            sell_limit,
            source_timestamp_ms,
        })
    }

    pub(crate) fn ensure_price(
        &self,
        side: OrderSide,
        price: Decimal,
        context: &str,
    ) -> Result<()> {
        ensure!(price > Decimal::ZERO, "{context} price must be positive");
        match side {
            OrderSide::Buy => {
                if let Some(buy_limit) = self.buy_limit {
                    ensure!(
                        price <= buy_limit,
                        "{context} buy price {price} exceeds fresh OKX buyLmt {buy_limit} for {}",
                        self.inst_id
                    );
                }
            }
            OrderSide::Sell => {
                if let Some(sell_limit) = self.sell_limit {
                    ensure!(
                        price >= sell_limit,
                        "{context} sell price {price} is below fresh OKX sellLmt {sell_limit} for {}",
                        self.inst_id
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) const fn source_timestamp_ms(&self) -> i128 {
        self.source_timestamp_ms
    }

    #[cfg(test)]
    pub(crate) fn disabled_for_unvalidated_test_route(inst_id: &str) -> Self {
        Self {
            inst_id: inst_id.to_owned(),
            buy_limit: None,
            sell_limit: None,
            source_timestamp_ms: 1,
        }
    }
}

fn parse_positive_price_limit(field: &str, value: &str) -> Result<Decimal> {
    ensure!(
        !value.trim().is_empty() && value == value.trim(),
        "OKX price-limit {field} must be non-empty and trimmed when enabled"
    );
    let value = value
        .parse::<Decimal>()
        .with_context(|| format!("OKX price-limit {field} must be a decimal value"))?;
    ensure!(
        value > Decimal::ZERO,
        "OKX price-limit {field} must be positive"
    );
    Ok(value)
}

impl ValidatedQuoteUsdRate {
    pub(crate) fn identity(quote_ccy: &str) -> Result<Self> {
        ensure!(
            quote_ccy == "USD",
            "USD identity conversion requires exact quote currency USD"
        );
        Ok(Self {
            quote_ccy: quote_ccy.to_owned(),
            source: ValidatedQuoteUsdSource::Identity,
            usd_per_quote: Decimal::ONE,
            source_timestamp_ms: None,
        })
    }

    pub(super) fn from_index_ticker(quote_ccy: &str, ticker: &OkxIndexTicker) -> Result<Self> {
        ensure!(
            quote_ccy != "USD",
            "USD quote currency must use identity conversion"
        );
        let expected_inst_id = format!("{quote_ccy}-USD");
        ensure!(
            ticker.inst_id == expected_inst_id,
            "OKX index ticker returned {} for validated quote currency {quote_ccy}; expected {expected_inst_id}",
            ticker.inst_id
        );
        Ok(Self {
            quote_ccy: quote_ccy.to_owned(),
            source: ValidatedQuoteUsdSource::Index(expected_inst_id),
            usd_per_quote: ticker.price()?,
            source_timestamp_ms: Some(ticker.timestamp_ms()?),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_test_index(quote_ccy: &str, index_price: Decimal) -> Result<Self> {
        Self::from_index_ticker(
            quote_ccy,
            &OkxIndexTicker {
                inst_id: format!("{quote_ccy}-USD"),
                index_price: index_price.to_string(),
                timestamp_ms: "1".to_owned(),
            },
        )
    }

    pub(crate) const fn source_timestamp_ms(&self) -> Option<i128> {
        self.source_timestamp_ms
    }

    #[cfg(test)]
    pub(crate) const fn usd_per_quote(&self) -> Decimal {
        self.usd_per_quote
    }
}

/// Immutable product authority produced only after the requested tuple, public
/// metadata, account-visible metadata, permissions, loan state, sizing, and
/// cash balances agree.
#[derive(Clone, Debug)]
pub struct ValidatedTradingInstrument {
    instrument: OkxInstrument,
    trade_quote_ccy: String,
    inst_type: ValidatedInstrumentType,
    td_mode: ValidatedTradeMode,
    quote_usd_source: Option<ValidatedQuoteUsdSource>,
}

pub(super) struct TradingInstrumentExchangeEvidence<'a> {
    pub public: OkxInstrument,
    pub account: OkxInstrument,
    pub account_config: &'a OkxAccountConfig,
    pub price: Decimal,
    pub maximum: &'a OkxMaximumOrderSize,
    pub available: &'a OkxMaximumAvailableSize,
    pub balances: &'a [OkxBalance],
    pub quote_usd_rate: Option<&'a ValidatedQuoteUsdRate>,
}

impl ValidatedTradingInstrument {
    pub fn inst_id(&self) -> &str {
        &self.instrument.inst_id
    }

    pub fn inst_id_code(&self) -> Result<Option<u64>> {
        self.instrument.websocket_inst_id_code()
    }

    pub const fn inst_type(&self) -> ValidatedInstrumentType {
        self.inst_type
    }

    pub const fn td_mode(&self) -> ValidatedTradeMode {
        self.td_mode
    }

    pub fn base_ccy(&self) -> &str {
        &self.instrument.base_ccy
    }

    pub fn quote_ccy(&self) -> &str {
        &self.instrument.quote_ccy
    }

    pub fn trade_quote_ccy(&self) -> &str {
        &self.trade_quote_ccy
    }

    pub fn fee_group_id(&self) -> Result<&str> {
        self.instrument.fee_group_id()
    }

    pub fn instrument(&self) -> &OkxInstrument {
        &self.instrument
    }

    pub fn price_limit_percentages(&self) -> Result<(Option<Decimal>, Decimal, Decimal)> {
        self.instrument.price_limit_percentages()
    }

    pub fn has_usd_order_amount_limit(&self) -> Result<bool> {
        self.instrument.has_usd_order_amount_limit()
    }

    pub fn ensure_limit_quote_amount(
        &self,
        quote_amount: Decimal,
        quote_usd_rate: &ValidatedQuoteUsdRate,
        context: &str,
    ) -> Result<()> {
        self.ensure_quote_amount_within_usd_limit(
            quote_amount,
            quote_usd_rate,
            self.instrument.max_limit_amount()?,
            "maxLmtAmt",
            context,
        )
    }

    pub fn ensure_market_buy_quote_amount(
        &self,
        quote_amount: Decimal,
        quote_usd_rate: &ValidatedQuoteUsdRate,
        context: &str,
    ) -> Result<()> {
        self.ensure_quote_amount_within_usd_limit(
            quote_amount,
            quote_usd_rate,
            self.instrument.max_market_amount()?,
            "maxMktAmt",
            context,
        )
    }

    fn ensure_quote_amount_within_usd_limit(
        &self,
        quote_amount: Decimal,
        quote_usd_rate: &ValidatedQuoteUsdRate,
        maximum_usd_amount: Option<Decimal>,
        okx_field: &str,
        context: &str,
    ) -> Result<()> {
        let Some(maximum_usd_amount) = maximum_usd_amount else {
            return Ok(());
        };
        ensure!(quote_amount > Decimal::ZERO, "{context} must be positive");
        let expected_source = self
            .quote_usd_source
            .as_ref()
            .context("validated trading instrument omitted required USD conversion source")?;
        ensure!(
            quote_usd_rate.quote_ccy == self.quote_ccy()
                && &quote_usd_rate.source == expected_source,
            "fresh USD conversion evidence contradicts validated quote currency {}",
            self.quote_ccy()
        );
        let usd_amount = quote_amount
            .checked_mul(quote_usd_rate.usd_per_quote)
            .with_context(|| format!("{context} USD conversion overflowed Decimal"))?;
        ensure!(
            usd_amount <= maximum_usd_amount,
            "{context} USD amount {usd_amount} exceeds OKX {okx_field} {maximum_usd_amount}"
        );
        Ok(())
    }

    pub fn ensure_public_refresh_matches(&self, current: &OkxInstrument) -> Result<()> {
        current.ensure_live()?;
        validate_overlapping_metadata(&self.instrument, current)
            .context("fresh OKX public instrument metadata contradicts validated startup context")
    }

    pub(super) fn from_exchange_evidence(
        requested: &RequestedTradingInstrument,
        evidence: TradingInstrumentExchangeEvidence<'_>,
    ) -> Result<Self> {
        let TradingInstrumentExchangeEvidence {
            public,
            account,
            account_config,
            price,
            maximum,
            available,
            balances,
            quote_usd_rate,
        } = evidence;
        validate_requested_trading_instrument(requested)?;
        ensure!(
            requested.inst_type == RequestedInstrumentType::Spot
                && requested.td_mode == RequestedTradeMode::Cash,
            "only a requested SPOT + cash tuple can enter OKX runtime validation"
        );
        account_config.ensure_spot_trading_enabled()?;

        validate_instrument_row(requested, &public, "public")?;
        validate_instrument_row(requested, &account, "account")?;
        validate_overlapping_metadata(&public, &account)?;

        let trade_quote_ccy = public.quote_ccy.clone();
        public.ensure_trade_quote_currency(&trade_quote_ccy)?;
        account.ensure_trade_quote_currency(&trade_quote_ccy)?;
        let quote_usd_source = validate_quote_usd_rate(&public, quote_usd_rate)?;
        validate_sizing_and_balance_evidence(&public, price, maximum, available, balances)?;

        Ok(Self {
            instrument: public,
            trade_quote_ccy,
            inst_type: ValidatedInstrumentType::Spot,
            td_mode: ValidatedTradeMode::Cash,
            quote_usd_source,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_test_instrument(instrument: OkxInstrument) -> Result<Self> {
        instrument.ensure_live()?;
        instrument.ensure_trade_quote_currency(&instrument.quote_ccy)?;
        instrument.price_limit_percentages()?;
        let quote_usd_source = if instrument.has_usd_order_amount_limit()? {
            Some(if instrument.quote_ccy == "USD" {
                ValidatedQuoteUsdSource::Identity
            } else {
                ValidatedQuoteUsdSource::Index(format!("{}-USD", instrument.quote_ccy))
            })
        } else {
            None
        };
        Ok(Self {
            trade_quote_ccy: instrument.quote_ccy.clone(),
            instrument,
            inst_type: ValidatedInstrumentType::Spot,
            td_mode: ValidatedTradeMode::Cash,
            quote_usd_source,
        })
    }
}

fn validate_instrument_row(
    requested: &RequestedTradingInstrument,
    instrument: &OkxInstrument,
    source: &str,
) -> Result<()> {
    let requested_id = requested.instrument.as_str();
    ensure!(
        instrument.inst_id == requested_id,
        "OKX {source} instrument returned {} for requested {requested_id}",
        instrument.inst_id
    );
    ensure!(
        instrument.inst_type == requested.inst_type.as_okx(),
        "OKX {source} instrument {} returned instType {} for requested {}",
        instrument.inst_id,
        instrument.inst_type,
        requested.inst_type
    );
    instrument.ensure_live()?;
    instrument.fee_group_id()?;
    let (base, quote) = requested_id
        .split_once('-')
        .context("validated OKX SPOT instrument must use BASE-QUOTE")?;
    ensure!(
        instrument.base_ccy == base && instrument.quote_ccy == quote,
        "OKX {source} instrument {requested_id} currencies {}/{} contradict identifier {base}/{quote}",
        instrument.base_ccy,
        instrument.quote_ccy
    );
    instrument.tick_size()?;
    instrument.lot_size()?;
    instrument.min_size()?;
    instrument.validate_order_limits()?;
    instrument.websocket_inst_id_code()?;
    Ok(())
}

fn validate_overlapping_metadata(public: &OkxInstrument, account: &OkxInstrument) -> Result<()> {
    ensure!(
        public.inst_id == account.inst_id
            && public.inst_type == account.inst_type
            && public.state == account.state
            && public.base_ccy == account.base_ccy
            && public.quote_ccy == account.quote_ccy
            && public.fee_group_id()? == account.fee_group_id()?,
        "OKX public/account instrument identity metadata disagrees for {}",
        public.inst_id
    );
    ensure!(
        public.trade_quote_currency_set()? == account.trade_quote_currency_set()?,
        "OKX public/account tradeQuoteCcyList metadata disagrees for {}",
        public.inst_id
    );
    ensure!(
        public.tick_size()? == account.tick_size()?
            && public.lot_size()? == account.lot_size()?
            && public.min_size()? == account.min_size()?
            && public.max_limit_size()? == account.max_limit_size()?
            && public.max_limit_amount()? == account.max_limit_amount()?
            && public.max_market_size_usdt()? == account.max_market_size_usdt()?
            && public.max_market_amount()? == account.max_market_amount()?
            && public.max_trigger_size()? == account.max_trigger_size()?,
        "OKX public/account precision or order-limit metadata disagrees for {}",
        public.inst_id
    );
    ensure!(
        public.price_limit_percentages()? == account.price_limit_percentages()?,
        "OKX public/account price-limit percentage metadata disagrees for {}",
        public.inst_id
    );
    ensure!(
        public.websocket_inst_id_code()? == account.websocket_inst_id_code()?,
        "OKX public/account instIdCode metadata disagrees for {}",
        public.inst_id
    );
    Ok(())
}

fn validate_quote_usd_rate(
    instrument: &OkxInstrument,
    quote_usd_rate: Option<&ValidatedQuoteUsdRate>,
) -> Result<Option<ValidatedQuoteUsdSource>> {
    if !instrument.has_usd_order_amount_limit()? {
        ensure!(
            quote_usd_rate.is_none(),
            "OKX USD conversion evidence was supplied without a USD-denominated order limit"
        );
        return Ok(None);
    }
    let quote_usd_rate =
        quote_usd_rate.context("OKX USD-denominated order limits require quote-to-USD evidence")?;
    ensure!(
        quote_usd_rate.quote_ccy == instrument.quote_ccy,
        "OKX USD conversion evidence was for quote currency {} instead of {}",
        quote_usd_rate.quote_ccy,
        instrument.quote_ccy
    );
    let expected_source = if instrument.quote_ccy == "USD" {
        ValidatedQuoteUsdSource::Identity
    } else {
        ValidatedQuoteUsdSource::Index(format!("{}-USD", instrument.quote_ccy))
    };
    ensure!(
        quote_usd_rate.source == expected_source,
        "OKX USD conversion evidence source contradicted quote currency {}",
        instrument.quote_ccy
    );
    Ok(Some(expected_source))
}

fn validate_sizing_and_balance_evidence(
    instrument: &OkxInstrument,
    price: Decimal,
    maximum: &OkxMaximumOrderSize,
    available: &OkxMaximumAvailableSize,
    balances: &[OkxBalance],
) -> Result<()> {
    ensure!(
        price > Decimal::ZERO,
        "OKX sizing evidence price must be positive"
    );
    ensure!(
        maximum.inst_id == instrument.inst_id && available.inst_id == instrument.inst_id,
        "OKX sizing evidence returned a contradictory instrument identity for {}",
        instrument.inst_id
    );
    maximum.ensure_cash_spot_margin_currency(&instrument.base_ccy)?;
    let max_buy_base = maximum.max_buy_base()?;
    let max_sell_quote = maximum.max_sell_quote()?;
    let available_buy_quote = available.available_buy_quote()?;
    let available_sell_base = available.available_sell_base()?;

    let quote_available = available_balance(balances, &instrument.quote_ccy)?;
    let base_available = available_balance(balances, &instrument.base_ccy)?;
    ensure!(
        available_buy_quote <= quote_available,
        "OKX max-avail-size availBuy {available_buy_quote} {} exceeds cash available balance {quote_available}",
        instrument.quote_ccy
    );
    ensure!(
        available_sell_base <= base_available,
        "OKX max-avail-size availSell {available_sell_base} {} exceeds cash available balance {base_available}",
        instrument.base_ccy
    );
    let max_buy_quote = max_buy_base
        .checked_mul(price)
        .context("OKX max-size maxBuy times price overflowed Decimal")?;
    ensure!(
        max_buy_quote <= available_buy_quote,
        "OKX max-size maxBuy {max_buy_base} base at price {price} contradicts max-avail-size availBuy {available_buy_quote} quote"
    );
    let max_sell_base = max_sell_quote
        .checked_div(price)
        .context("OKX max-size maxSell divided by price overflowed Decimal")?;
    ensure!(
        max_sell_base <= available_sell_base,
        "OKX max-size maxSell {max_sell_quote} quote at price {price} contradicts max-avail-size availSell {available_sell_base} base"
    );
    Ok(())
}

fn available_balance(balances: &[OkxBalance], currency: &str) -> Result<Decimal> {
    let mut matching = balances
        .iter()
        .flat_map(|balance| &balance.details)
        .filter(|detail| detail.ccy == currency);
    let value = matching
        .next()
        .map(|detail| detail.available())
        .transpose()?
        .unwrap_or(Decimal::ZERO);
    ensure!(
        matching.next().is_none(),
        "OKX account balance returned duplicate {currency} rows"
    );
    Ok(value)
}

#[cfg(test)]
#[path = "trading_instrument_tests.rs"]
mod tests;
