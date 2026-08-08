//! Exact sizing and submission of new OKX SPOT entry orders.

use std::time::Instant;

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use tracing::info;

use super::{OkxEmaAtrMakerTrendRunner, TrackedOrder};
use crate::okx::{
    client::OkxClient,
    types::{OrderKind, OrderSide, decimal_to_okx, quantize_decimal_down},
};

use super::order_id::{OrderPurpose, client_order_id};

impl OkxEmaAtrMakerTrendRunner {
    pub(super) async fn evaluate_entry(&mut self, client: &impl OkxClient) -> Result<()> {
        if !self.entry_preconditions_met()? {
            return Ok(());
        }

        let ticker = client.ticker(&self.instrument_id).await?;
        let bid = ticker.bid_decimal()?;
        let Some(raw_price) = self.signal.entry_price_from_bid(bid)? else {
            return Ok(());
        };
        let state = self.exchange()?;
        let price = quantize_decimal_down(raw_price, state.instrument.tick_size()?)?;
        let quantity = self.entry_quantity(price)?;
        let size = quantize_decimal_down(quantity, state.instrument.lot_size()?)?;
        ensure!(
            size >= state.instrument.min_size()?,
            "OkxEmaAtrMakerTrend entry size {size} is below OKX minSz {}",
            state.instrument.min_size()?
        );
        let entry_fee_cost_rate = self.entry_fee_cost_rate.ok_or_else(|| {
            anyhow::anyhow!("OkxEmaAtrMakerTrend entry fee rate is not initialized")
        })?;
        let net_protectable_size = quantize_decimal_down(
            size * (Decimal::ONE - entry_fee_cost_rate),
            state.instrument.lot_size()?,
        )?;
        ensure!(
            net_protectable_size >= state.instrument.min_size()?,
            "OkxEmaAtrMakerTrend entry size {size} can deliver only {net_protectable_size} after maker fee rate {entry_fee_cost_rate}, below OKX minSz {}",
            state.instrument.min_size()?
        );
        state
            .instrument
            .ensure_limit_size(size, "OkxEmaAtrMakerTrend entry size")?;
        self.ensure_entry_notional_within_cap(size, price)?;
        let quote_notional = size
            .checked_mul(price)
            .context("OkxEmaAtrMakerTrend entry quote notional overflowed Decimal")?;
        self.ensure_limit_quote_amount(
            client,
            quote_notional,
            "OkxEmaAtrMakerTrend entry quote notional",
        )
        .await?;
        let price = decimal_to_okx(price);
        let size = decimal_to_okx(size);
        let client_order_id = client_order_id(&self.instance_id, OrderPurpose::Entry);
        client.record_order_decision(Instant::now());
        client
            .place_order(
                &self.instrument_id,
                OrderSide::Buy,
                OrderKind::PostOnly,
                &size,
                Some(&price),
                &client_order_id,
            )
            .await?;
        self.exchange_mut()?.entry_order = Some(TrackedOrder {
            client_order_id: client_order_id.clone(),
            last_fill_size: Decimal::ZERO,
            last_average_fill_price: None,
            last_accounted_base_change: Decimal::ZERO,
            last_accounted_quote_change: Decimal::ZERO,
            cancel_requested: false,
        });
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            client_order_id,
            price,
            size,
            "submitted OKX post-only entry"
        );
        Ok(())
    }

    pub fn entry_quantity(&self, price: Decimal) -> Result<Decimal> {
        ensure!(
            price > Decimal::ZERO,
            "OkxEmaAtrMakerTrend entry price must be positive"
        );
        let mut quantity = self.quantity;
        let entry_fee_cost_rate = self.entry_fee_cost_rate.ok_or_else(|| {
            anyhow::anyhow!("OkxEmaAtrMakerTrend entry fee rate is not initialized")
        })?;
        ensure!(
            entry_fee_cost_rate >= Decimal::ZERO && entry_fee_cost_rate < Decimal::ONE,
            "OkxEmaAtrMakerTrend entry fee cost rate must be non-negative and below one"
        );
        if let Some(max_quote_notional) = self.max_quote_notional {
            quantity =
                quantity.min(max_quote_notional / (price * (Decimal::ONE + entry_fee_cost_rate)));
        }
        ensure!(
            quantity > Decimal::ZERO,
            "OkxEmaAtrMakerTrend entry quantity must be positive"
        );
        Ok(quantity)
    }

    fn ensure_entry_notional_within_cap(&self, size: Decimal, price: Decimal) -> Result<()> {
        let Some(max_quote_notional) = self.max_quote_notional else {
            return Ok(());
        };
        let entry_fee_cost_rate = self.entry_fee_cost_rate.ok_or_else(|| {
            anyhow::anyhow!("OkxEmaAtrMakerTrend entry fee rate is not initialized")
        })?;
        let notional = size
            .checked_mul(price)
            .and_then(|value| value.checked_mul(Decimal::ONE + entry_fee_cost_rate))
            .context("OkxEmaAtrMakerTrend capped entry notional overflowed Decimal")?;
        ensure!(
            notional <= max_quote_notional,
            "OkxEmaAtrMakerTrend entry notional {notional} exceeds max_quote_notional {max_quote_notional}"
        );
        Ok(())
    }

    fn entry_preconditions_met(&self) -> Result<bool> {
        let state = self.exchange()?;
        Ok(self.signal.entry_allowed()
            && state.entry_order.is_none()
            && state.position.is_none()
            && state.take_profit_order.is_none()
            && state.stop_loss_exit_order.is_none()
            && state.stop_loss_pending.is_none())
    }
}
