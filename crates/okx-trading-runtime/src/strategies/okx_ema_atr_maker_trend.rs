use std::{collections::HashSet, sync::Arc, time::Instant};

use anyhow::{Context, Result, bail, ensure};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use time::OffsetDateTime;
use tracing::{debug, error, info, warn};

mod entry_submission;
mod inventory;
mod order_id;
mod shutdown;
mod signal;

use inventory::{StrategyBalance, strategy_balance_after_operator_baseline};
use order_id::{
    ORDER_ID_PREFIX, OrderPurpose, client_order_id, legacy_strategy_tag,
    parse_legacy_strategy_client_order_id, parse_strategy_client_order_id, strategy_tag,
};
use signal::{SignalState, confirmed_bars_chronological};

#[cfg(test)]
use order_id::{OKX_CLIENT_ORDER_ID_MAX_LEN, base36};

use crate::{
    config::types::{BotConfig, StrategyInstanceConfig, StrategyKind},
    okx::{
        client::{OkxClient, OkxOrderAmend},
        trading_instrument::ValidatedTradingInstrument,
        types::{
            OkxAlgoOrder, OkxBalance, OkxFill, OkxInstrument, OkxOrder, OkxSpotFillAccounting,
            OkxTradeFeeRate, OrderKind, OrderSide, decimal_to_okx, quantize_decimal_down,
            quantize_decimal_up,
        },
    },
};

pub(crate) const MAX_MAKER_FEE_RATE: &str = "0.001";
pub(crate) const MAX_TAKER_FEE_RATE: &str = "0.002";
pub(crate) const OKX_EMA_ATR_MAKER_TREND_BAR: &str = "1m";
const HISTORICAL_WARMUP_BARS: usize = 120;

pub(crate) fn strategy_ownership_tag_for_config(strategy_id: &str) -> String {
    order_id::strategy_ownership_tag_for_config(strategy_id)
}

fn configured_strategy_tags_for_instrument(config: &BotConfig, instrument_id: &str) -> Vec<String> {
    let mut tags = config
        .strategies
        .instances
        .iter()
        .filter(|instance| instance.kind == StrategyKind::OkxEmaAtrMakerTrend)
        .filter(|instance| instance.instrument_id() == instrument_id)
        .map(|instance| strategy_tag(&instance.id))
        .collect::<Vec<_>>();
    tags.sort();
    tags.dedup();
    tags
}

#[derive(Debug)]
pub struct OkxEmaAtrMakerTrendRunner {
    instance_id: String,
    configured_strategy_tags: Vec<String>,
    instrument_id: String,
    validated_instrument: Option<Arc<ValidatedTradingInstrument>>,
    quantity: Decimal,
    operator_owned_base_balance: Decimal,
    max_entry_order_age_ms: u64,
    max_quote_notional: Option<Decimal>,
    take_profit_atr_multiple: Decimal,
    stop_loss_atr_multiple: Decimal,
    entry_fee_cost_rate: Option<Decimal>,
    exit_fee_cost_rate: Option<Decimal>,
    signal: SignalState,
    exchange: Option<ExchangeState>,
}

#[derive(Clone, Debug)]
struct ExchangeState {
    instrument: OkxInstrument,
    last_bar_ts_ms: Option<i64>,
    entry_order: Option<TrackedOrder>,
    take_profit_order: Option<TrackedOrder>,
    stop_loss_order: Option<TrackedAlgoOrder>,
    stop_loss_exit_order: Option<TrackedOrder>,
    position: Option<OpenPosition>,
    stop_loss_pending: Option<StopLossPendingReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopLossPendingReason {
    LocalThreshold,
    ExitReconciliation,
}

#[derive(Clone, Debug, PartialEq)]
struct TrackedOrder {
    client_order_id: String,
    last_fill_size: Decimal,
    last_average_fill_price: Option<Decimal>,
    last_accounted_base_change: Decimal,
    last_accounted_quote_change: Decimal,
    cancel_requested: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct TrackedAlgoOrder {
    algo_id: String,
    client_order_id: String,
    size: Decimal,
    trigger_price: Decimal,
    cancel_requested: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct OpenPosition {
    quantity: Decimal,
    average_price: Decimal,
    stop_loss_trigger: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TakeProfitOrderShape {
    size: Decimal,
    price: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PositionReconstruction {
    NoPosition,
    Reconstructed(OpenPosition),
    NeedsMoreEvidence(PositionEvidenceGap),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PositionEvidenceGap {
    MissingCostBasis,
    InventoryMismatch { strategy_owned_quantity: Decimal },
}

#[derive(Clone, Copy)]
enum PositionEvidenceAttempt {
    Initial,
    Final,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum EntryAveragePriceStatus {
    NoEntryFill,
    MissingPrice,
    Reconstructed(Decimal),
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct WarmupSummary {
    confirmed_bar_count: usize,
    applied_bars: usize,
    last_bar_ts_ms: Option<i64>,
}

impl OkxEmaAtrMakerTrendRunner {
    pub fn from_validated_instance(
        config: &BotConfig,
        instance: &StrategyInstanceConfig,
        validated_instrument: Arc<ValidatedTradingInstrument>,
    ) -> Result<Self> {
        ensure!(
            instance.instrument_id() == validated_instrument.inst_id(),
            "strategy {} requested instrument {} but received validated context for {}",
            instance.id,
            instance.instrument_id(),
            validated_instrument.inst_id()
        );
        Self::from_instance_with_context(config, instance, Some(validated_instrument))
    }

    #[cfg(test)]
    pub fn from_instance(config: &BotConfig, instance: &StrategyInstanceConfig) -> Result<Self> {
        Self::from_instance_with_context(config, instance, None)
    }

    fn from_instance_with_context(
        config: &BotConfig,
        instance: &StrategyInstanceConfig,
        validated_instrument: Option<Arc<ValidatedTradingInstrument>>,
    ) -> Result<Self> {
        ensure!(
            instance.kind == StrategyKind::OkxEmaAtrMakerTrend,
            "OkxEmaAtrMakerTrend received unsupported strategy kind"
        );
        let params = instance.params.okx_ema_atr_maker_trend();
        let instrument_id = instance.instrument_id().to_owned();
        let configured_strategy_tags =
            configured_strategy_tags_for_instrument(config, &instrument_id);
        let max_quote_notional = params
            .max_quote_notional_by_instrument
            .get(&instrument_id)
            .copied()
            .or(params.max_quote_notional);
        let signal = SignalState::new(
            params.fast_ema_period,
            params.slow_ema_period,
            params.atr_period,
            params.entry_offset_atr_multiple,
            params.min_entry_offset_bps,
            params.max_entry_offset_bps,
        );

        Ok(Self {
            instance_id: instance.id.clone(),
            configured_strategy_tags,
            instrument_id,
            validated_instrument,
            quantity: params.quantity,
            operator_owned_base_balance: params.operator_owned_base_balance,
            max_entry_order_age_ms: params.max_entry_order_age_ms,
            max_quote_notional,
            take_profit_atr_multiple: params.take_profit_atr_multiple,
            stop_loss_atr_multiple: params.stop_loss_atr_multiple,
            entry_fee_cost_rate: Some(
                MAX_MAKER_FEE_RATE
                    .parse::<Decimal>()
                    .context("OkxEmaAtrMakerTrend MAX_MAKER_FEE_RATE must be a decimal")?,
            ),
            exit_fee_cost_rate: Some(
                MAX_TAKER_FEE_RATE
                    .parse::<Decimal>()
                    .context("OkxEmaAtrMakerTrend MAX_TAKER_FEE_RATE must be a decimal")?,
            ),
            signal,
            exchange: None,
        })
    }

    pub async fn initialize(&mut self, client: &impl OkxClient) -> Result<()> {
        let warmup = self.prepare_startup_exchange_state(client).await?;
        self.protect_reconstructed_startup_state(client).await?;
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            confirmed_bars = warmup.confirmed_bar_count,
            applied_bars = warmup.applied_bars,
            ready = self.signal.ready(),
            "initialized OKX 1m maker trend strategy"
        );
        Ok(())
    }

    pub async fn tick(&mut self, client: &impl OkxClient) -> Result<()> {
        self.on_confirmed_candle(client).await
    }

    pub async fn on_confirmed_candle(&mut self, client: &impl OkxClient) -> Result<()> {
        self.ensure_initialized(client).await?;
        self.refresh_bars(client).await?;
        self.refresh_tracked_orders(client).await?;
        self.enforce_tick_exit_protection(client).await?;
        self.evaluate_entry(client).await
    }

    pub async fn on_private_event(&mut self, client: &impl OkxClient) -> Result<()> {
        self.ensure_initialized(client).await?;
        self.refresh_tracked_orders(client).await?;
        self.enforce_tick_exit_protection(client).await
    }

    pub async fn on_reconcile_timer(&mut self, client: &impl OkxClient) -> Result<()> {
        self.ensure_initialized(client).await?;
        // REST/cached candle refresh preserves recovery if a public event was
        // coalesced or the stream reconnected, but timer work never evaluates
        // a new entry without a newly delivered confirmed-candle event.
        self.refresh_bars(client).await?;
        self.refresh_tracked_orders(client).await?;
        self.enforce_tick_exit_protection(client).await
    }

    pub async fn on_instrument_update(&mut self, client: &impl OkxClient) -> Result<()> {
        self.ensure_initialized(client).await?;
        let instrument = client.instruments(&self.instrument_id).await?;
        if let Some(validated) = &self.validated_instrument {
            validated.ensure_public_refresh_matches(&instrument)?;
            self.exchange_mut()?.instrument = validated.instrument().clone();
        } else {
            #[cfg(test)]
            {
                self.exchange_mut()?.instrument = instrument;
            }
            #[cfg(not(test))]
            bail!(
                "strategy {} is missing validated instrument context",
                self.instance_id
            );
        }
        self.reconstruct_exchange_state(client).await?;
        self.ensure_position_protection(client).await
    }

    pub fn instrument_id(&self) -> &str {
        &self.instrument_id
    }

    fn validated_instrument_for_order(&self) -> Result<Arc<ValidatedTradingInstrument>> {
        if let Some(validated) = &self.validated_instrument {
            return Ok(Arc::clone(validated));
        }
        #[cfg(test)]
        {
            return Ok(Arc::new(ValidatedTradingInstrument::from_test_instrument(
                self.exchange()?.instrument.clone(),
            )?));
        }
        #[cfg(not(test))]
        bail!(
            "strategy {} is missing validated instrument context",
            self.instance_id
        );
    }

    async fn ensure_limit_quote_amount(
        &self,
        client: &impl OkxClient,
        quote_amount: Decimal,
        context: &str,
    ) -> Result<()> {
        let validated = self.validated_instrument_for_order()?;
        if validated.instrument().max_limit_amount()?.is_none() {
            return Ok(());
        }
        let quote_usd_rate = client.fresh_quote_usd_rate(&validated).await?;
        validated.ensure_limit_quote_amount(quote_amount, &quote_usd_rate, context)
    }

    async fn prepare_startup_exchange_state(
        &mut self,
        client: &impl OkxClient,
    ) -> Result<WarmupSummary> {
        let instrument = if let Some(validated) = &self.validated_instrument {
            validated.instrument().clone()
        } else {
            #[cfg(test)]
            {
                client.instruments(&self.instrument_id).await?
            }
            #[cfg(not(test))]
            bail!(
                "strategy {} is missing validated instrument context",
                self.instance_id
            );
        };
        let fee = client.spot_trade_fee(&self.instrument_id).await?;
        self.configure_fee_schedule(&fee)?;
        let warmup = self.apply_startup_warmup(client).await?;
        self.exchange = Some(ExchangeState {
            instrument,
            last_bar_ts_ms: warmup.last_bar_ts_ms,
            entry_order: None,
            take_profit_order: None,
            stop_loss_order: None,
            stop_loss_exit_order: None,
            position: None,
            stop_loss_pending: None,
        });
        Ok(warmup)
    }

    fn configure_fee_schedule(&mut self, fee: &OkxTradeFeeRate) -> Result<()> {
        fee.ensure_spot(&self.instrument_id)?;
        let maker_cost_rate = fee.normalized_maker_cost_rate()?.max(Decimal::ZERO);
        let taker_cost_rate = fee.normalized_taker_cost_rate()?.max(Decimal::ZERO);
        ensure!(
            maker_cost_rate < Decimal::ONE && taker_cost_rate < Decimal::ONE,
            "OKX SPOT fee costs must be below one"
        );
        self.signal
            .set_round_trip_cost_rate(maker_cost_rate + taker_cost_rate)?;
        self.entry_fee_cost_rate = Some(maker_cost_rate);
        self.exit_fee_cost_rate = Some(taker_cost_rate);
        Ok(())
    }

    async fn protect_reconstructed_startup_state(&mut self, client: &impl OkxClient) -> Result<()> {
        self.reconstruct_exchange_state(client).await?;
        self.ensure_position_protection(client).await
    }

    async fn enforce_tick_exit_protection(&mut self, client: &impl OkxClient) -> Result<()> {
        self.ensure_stop_loss_order(client).await?;
        self.evaluate_stop_loss(client).await?;
        self.ensure_position_protection(client).await
    }

    async fn ensure_initialized(&mut self, client: &impl OkxClient) -> Result<()> {
        if self.exchange.is_none() {
            self.initialize(client).await?;
        }
        Ok(())
    }

    async fn apply_startup_warmup(&mut self, client: &impl OkxClient) -> Result<WarmupSummary> {
        let bars = client
            .candles(
                &self.instrument_id,
                OKX_EMA_ATR_MAKER_TREND_BAR,
                HISTORICAL_WARMUP_BARS,
            )
            .await?;
        let confirmed_bars = confirmed_bars_chronological(&bars);
        let confirmed_bar_count = confirmed_bars.len();
        let mut applied_bars = 0usize;
        let mut last_bar_ts_ms = None;
        for bar in confirmed_bars {
            if self.signal.update_from_bar(bar) {
                applied_bars += 1;
            }
            last_bar_ts_ms = Some(bar.ts_ms);
        }
        ensure!(
            self.signal.ready(),
            "OkxEmaAtrMakerTrend requires initialized EMA/ATR before live startup; only {confirmed_bar_count} confirmed warmup bars available from {HISTORICAL_WARMUP_BARS} requested"
        );
        Ok(WarmupSummary {
            confirmed_bar_count,
            applied_bars,
            last_bar_ts_ms,
        })
    }

    async fn refresh_tracked_orders(&mut self, client: &impl OkxClient) -> Result<()> {
        self.refresh_entry_order(client).await?;
        self.refresh_take_profit_order(client).await?;
        self.refresh_stop_loss_order(client).await?;
        self.refresh_stop_loss_exit_order(client).await
    }

    async fn ensure_position_protection(&mut self, client: &impl OkxClient) -> Result<()> {
        self.ensure_take_profit_order(client).await?;
        self.ensure_stop_loss_order(client).await
    }

    pub async fn reconcile_after_interrupted_tick(
        &mut self,
        client: &impl OkxClient,
    ) -> Result<()> {
        if self.exchange.is_none() {
            self.initialize(client).await?;
            return Ok(());
        }
        self.reconstruct_exchange_state(client).await?;
        self.refresh_entry_order(client).await?;
        self.refresh_take_profit_order(client).await?;
        self.refresh_stop_loss_order(client).await?;
        self.refresh_stop_loss_exit_order(client).await?;
        self.evaluate_stop_loss(client).await?;
        self.ensure_take_profit_order(client).await?;
        self.ensure_stop_loss_order(client).await
    }

    async fn reconstruct_exchange_state(&mut self, client: &impl OkxClient) -> Result<()> {
        let strategy_tag = strategy_tag(&self.instance_id);
        let open_orders = client.open_orders(&self.instrument_id).await?;
        let open_algo_orders = client.open_algo_orders(&self.instrument_id).await?;
        reject_live_legacy_strategy_ids(
            &open_orders,
            &open_algo_orders,
            &self.instance_id,
            &strategy_tag,
            &self.configured_strategy_tags,
        )?;
        let entry_order = find_live_strategy_order(
            &open_orders,
            &strategy_tag,
            OrderPurpose::Entry,
            OrderSide::Buy,
            &self.exchange()?.instrument,
        )?;
        let take_profit_order = find_live_strategy_order(
            &open_orders,
            &strategy_tag,
            OrderPurpose::TakeProfit,
            OrderSide::Sell,
            &self.exchange()?.instrument,
        )?;
        let stop_loss_exit_order = find_live_strategy_order(
            &open_orders,
            &strategy_tag,
            OrderPurpose::StopLoss,
            OrderSide::Sell,
            &self.exchange()?.instrument,
        )?;
        let stop_loss_order = find_live_strategy_stop_loss_algo(&open_algo_orders, &strategy_tag)?;
        reject_unknown_live_strategy_orders(&open_orders, &strategy_tag)?;
        reject_unknown_live_strategy_algo_orders(&open_algo_orders, &strategy_tag)?;

        let balances = client.balances().await?;
        let strategy_balance = self.reconstruct_strategy_balance(&balances)?;
        let mut order_evidence = strategy_order_evidence_from_orders(&open_orders, &strategy_tag);
        if strategy_balance.total >= self.exchange()?.instrument.lot_size()? {
            self.append_targeted_strategy_order_evidence(
                client,
                &mut order_evidence,
                &strategy_tag,
            )
            .await?;
        }
        let mut fill_evidence = strategy_fill_evidence(
            order_evidence.iter(),
            &[],
            &strategy_tag,
            &self.exchange()?.instrument,
        )?;
        let mut position_reconstruction = self.reconstruct_position_from_evidence(
            strategy_balance,
            &fill_evidence,
            PositionEvidenceAttempt::Initial,
        )?;
        let mut used_broad_history_fallback = false;
        if let PositionReconstruction::NeedsMoreEvidence(gap) = position_reconstruction {
            used_broad_history_fallback = true;
            debug!(
                strategy_id = %self.instance_id,
                instrument = %self.instrument_id,
                ?gap,
                "falling back to broad OKX order history for strategy reconstruction"
            );
            let order_fills = self
                .append_broad_history_fallback_evidence(client, &mut order_evidence, &strategy_tag)
                .await?;
            fill_evidence = strategy_fill_evidence(
                order_evidence.iter(),
                &order_fills,
                &strategy_tag,
                &self.exchange()?.instrument,
            )?;
            position_reconstruction = self.reconstruct_position_from_evidence(
                strategy_balance,
                &fill_evidence,
                PositionEvidenceAttempt::Final,
            )?;
        }
        let position =
            self.finalize_reconstructed_position(strategy_balance, position_reconstruction)?;

        if position.is_none() && take_profit_order.is_some() {
            bail!(
                "live strategy take-profit order exists for {} but no strategy-owned {} balance was reconstructed",
                self.instrument_id,
                self.exchange()?.instrument.base_ccy
            );
        }
        if position.is_none() && stop_loss_order.is_some() {
            bail!(
                "live strategy stop-loss algo exists for {} but no strategy-owned {} balance was reconstructed",
                self.instrument_id,
                self.exchange()?.instrument.base_ccy
            );
        }
        if position.is_none() && stop_loss_exit_order.is_some() {
            bail!(
                "live strategy stop-loss market exit exists for {} but no strategy-owned {} balance was reconstructed",
                self.instrument_id,
                self.exchange()?.instrument.base_ccy
            );
        }
        self.validate_reconstructed_live_protection(
            &open_orders,
            stop_loss_order.as_ref(),
            position,
            &strategy_tag,
        )?;

        let state = self.exchange_mut()?;
        state.entry_order = entry_order;
        state.take_profit_order = take_profit_order;
        state.stop_loss_order = stop_loss_order;
        state.stop_loss_exit_order = stop_loss_exit_order;
        state.position = position;
        state.stop_loss_pending = state
            .stop_loss_exit_order
            .is_some()
            .then_some(StopLossPendingReason::ExitReconciliation);
        let has_entry_order = state.entry_order.is_some();
        let has_take_profit_order = state.take_profit_order.is_some();
        let has_stop_loss_order = state.stop_loss_order.is_some();
        let has_stop_loss_exit_order = state.stop_loss_exit_order.is_some();
        let position_quantity = state.position.map(|position| position.quantity);
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            has_entry_order,
            has_take_profit_order,
            has_stop_loss_order,
            has_stop_loss_exit_order,
            ?position_quantity,
            used_broad_history_fallback,
            "reconstructed OKX strategy state"
        );
        Ok(())
    }

    async fn append_targeted_strategy_order_evidence(
        &self,
        client: &impl OkxClient,
        order_evidence: &mut Vec<OkxOrder>,
        strategy_tag: &str,
    ) -> Result<()> {
        let Some(state) = self.exchange.as_ref() else {
            return Ok(());
        };
        let mut seen_client_order_ids = strategy_order_evidence_ids(order_evidence);
        let tracked_order_ids = [
            state
                .entry_order
                .as_ref()
                .map(|order| order.client_order_id.as_str()),
            state
                .take_profit_order
                .as_ref()
                .map(|order| order.client_order_id.as_str()),
            state
                .stop_loss_exit_order
                .as_ref()
                .map(|order| order.client_order_id.as_str()),
        ];
        for client_order_id in tracked_order_ids.into_iter().flatten() {
            if seen_client_order_ids.contains(client_order_id)
                || parse_strategy_client_order_id(client_order_id, strategy_tag).is_none()
            {
                continue;
            }
            let Some(order) = client.order(&self.instrument_id, client_order_id).await? else {
                continue;
            };
            append_strategy_order_evidence(
                order_evidence,
                &mut seen_client_order_ids,
                order,
                strategy_tag,
            );
        }
        Ok(())
    }

    async fn append_broad_history_fallback_evidence(
        &self,
        client: &impl OkxClient,
        order_evidence: &mut Vec<OkxOrder>,
        strategy_tag: &str,
    ) -> Result<Vec<OkxFill>> {
        let order_history = client.order_history(&self.instrument_id).await?;
        let mut seen_client_order_ids = strategy_order_evidence_ids(order_evidence);
        for order in order_history {
            append_strategy_order_evidence(
                order_evidence,
                &mut seen_client_order_ids,
                order,
                strategy_tag,
            );
        }
        client.order_fills(&self.instrument_id).await
    }

    fn reconstruct_position_from_evidence(
        &self,
        strategy_balance: StrategyBalance,
        fill_evidence: &[StrategyFillEvidence],
        attempt: PositionEvidenceAttempt,
    ) -> Result<PositionReconstruction> {
        let strategy_owned_quantity = strategy_net_filled_position(fill_evidence)?;
        let lot_size = self.exchange()?.instrument.lot_size()?;
        let inventory_distance = if strategy_balance.total >= strategy_owned_quantity {
            strategy_balance.total - strategy_owned_quantity
        } else {
            strategy_owned_quantity - strategy_balance.total
        };
        if strategy_balance.tradeable_quantity <= Decimal::ZERO {
            return if inventory_distance >= lot_size {
                Ok(PositionReconstruction::NeedsMoreEvidence(
                    PositionEvidenceGap::InventoryMismatch {
                        strategy_owned_quantity,
                    },
                ))
            } else {
                Ok(PositionReconstruction::NoPosition)
            };
        }
        let average_price = match (attempt, strategy_entry_average_price(fill_evidence)?) {
            (PositionEvidenceAttempt::Initial, EntryAveragePriceStatus::NoEntryFill)
            | (PositionEvidenceAttempt::Final, EntryAveragePriceStatus::NoEntryFill) => {
                return Ok(PositionReconstruction::NeedsMoreEvidence(
                    PositionEvidenceGap::MissingCostBasis,
                ));
            }
            (PositionEvidenceAttempt::Initial, EntryAveragePriceStatus::MissingPrice) => {
                return Ok(PositionReconstruction::NeedsMoreEvidence(
                    PositionEvidenceGap::MissingCostBasis,
                ));
            }
            (PositionEvidenceAttempt::Final, EntryAveragePriceStatus::MissingPrice) => {
                bail!("OKX entry fill is missing avgPx");
            }
            (
                PositionEvidenceAttempt::Initial,
                EntryAveragePriceStatus::Reconstructed(average_price),
            )
            | (
                PositionEvidenceAttempt::Final,
                EntryAveragePriceStatus::Reconstructed(average_price),
            ) => average_price,
        };
        if inventory_distance >= lot_size {
            return Ok(PositionReconstruction::NeedsMoreEvidence(
                PositionEvidenceGap::InventoryMismatch {
                    strategy_owned_quantity,
                },
            ));
        }
        Ok(PositionReconstruction::Reconstructed(OpenPosition {
            quantity: strategy_balance.tradeable_quantity,
            average_price,
            stop_loss_trigger: self.stop_loss_trigger(average_price)?,
        }))
    }

    fn finalize_reconstructed_position(
        &self,
        strategy_balance: StrategyBalance,
        position_reconstruction: PositionReconstruction,
    ) -> Result<Option<OpenPosition>> {
        match position_reconstruction {
            PositionReconstruction::NoPosition => Ok(None),
            PositionReconstruction::Reconstructed(position) => Ok(Some(position)),
            PositionReconstruction::NeedsMoreEvidence(PositionEvidenceGap::MissingCostBasis) => {
                bail!(
                    "cannot reconstruct OKX strategy position cost basis for {}; refusing to manage reconstructed {} {} balance",
                    self.instrument_id,
                    strategy_balance.tradeable_quantity,
                    self.exchange()
                        .map(|state| state.instrument.base_ccy.as_str())
                        .unwrap_or("base")
                );
            }
            PositionReconstruction::NeedsMoreEvidence(PositionEvidenceGap::InventoryMismatch {
                strategy_owned_quantity,
            }) => {
                let state = self.exchange()?;
                if strategy_balance.total > strategy_owned_quantity {
                    bail!(
                        "reconstructed OKX {} strategy balance {} {} exceeds strategy-tagged net filled quantity {}; refusing to manage possible non-strategy balance",
                        self.instrument_id,
                        strategy_balance.total,
                        state.instrument.base_ccy,
                        strategy_owned_quantity.max(Decimal::ZERO)
                    );
                }
                bail!(
                    "strategy-tagged net filled quantity {} {} exceeds reconstructed OKX {} strategy balance {}; refusing to hide missing strategy-owned inventory",
                    strategy_owned_quantity,
                    state.instrument.base_ccy,
                    self.instrument_id,
                    strategy_balance.total
                );
            }
        }
    }

    fn validate_reconstructed_live_protection(
        &self,
        open_orders: &[OkxOrder],
        stop_loss_order: Option<&TrackedAlgoOrder>,
        position: Option<OpenPosition>,
        strategy_tag: &str,
    ) -> Result<()> {
        let Some(position) = position else {
            return Ok(());
        };
        if let Some(order) = open_orders.iter().find(|order| {
            order.is_live()
                && parse_strategy_client_order_id(&order.client_order_id, strategy_tag)
                    == Some(OrderPurpose::TakeProfit)
                && order.parsed_side() == Some(OrderSide::Sell)
        }) {
            ensure!(
                self.take_profit_order_matches_position(order, position)?,
                "live OKX take-profit order {} does not match reconstructed strategy position for {}",
                order.client_order_id,
                self.instrument_id
            );
        }
        if let Some(stop_loss_order) = stop_loss_order {
            ensure!(
                self.stop_loss_order_matches_position(stop_loss_order, position)?,
                "live OKX stop-loss algo {} does not match reconstructed strategy position for {}",
                stop_loss_order.algo_id,
                self.instrument_id
            );
        }
        Ok(())
    }

    fn reconstruct_strategy_balance(&self, balances: &[OkxBalance]) -> Result<StrategyBalance> {
        let state = self.exchange()?;
        let base_ccy = &state.instrument.base_ccy;
        let total = balances
            .iter()
            .flat_map(|balance| balance.details.iter())
            .filter(|detail| detail.ccy == *base_ccy)
            .try_fold(Decimal::ZERO, |total, detail| {
                Ok::<_, anyhow::Error>(total + detail.total()?)
            })?;
        strategy_balance_after_operator_baseline(
            total,
            self.operator_owned_base_balance,
            state.instrument.lot_size()?,
            state.instrument.min_size()?,
            base_ccy,
        )
    }

    async fn refresh_bars(&mut self, client: &impl OkxClient) -> Result<()> {
        let bars = client
            .live_candles(&self.instrument_id, OKX_EMA_ATR_MAKER_TREND_BAR, 3)
            .await?;
        for bar in confirmed_bars_chronological(&bars) {
            let state = self.exchange_mut()?;
            if state
                .last_bar_ts_ms
                .is_some_and(|last_ts_ms| bar.ts_ms <= last_ts_ms)
            {
                continue;
            }
            state.last_bar_ts_ms = Some(bar.ts_ms);
            if self.signal.update_from_bar(bar) {
                debug!(
                    strategy_id = %self.instance_id,
                    instrument = %self.instrument_id,
                    close = bar.close,
                    "updated OKX 1m signal state"
                );
            }
        }
        Ok(())
    }

    async fn refresh_entry_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(tracked_order) = self
            .exchange()
            .ok()
            .and_then(|state| state.entry_order.clone())
        else {
            return Ok(());
        };
        let client_order_id = tracked_order.client_order_id.clone();
        let Some(order) = client.order(&self.instrument_id, &client_order_id).await? else {
            return Ok(());
        };
        let fill_size = order.fill_size()?;
        let fill_delta =
            cumulative_fill_delta("OKX entry order", fill_size, tracked_order.last_fill_size)?;
        let average_fill_price = order.average_fill_price()?;
        if fill_delta > Decimal::ZERO {
            let state = self.exchange()?;
            let accounting = order.cumulative_spot_accounting(
                &state.instrument.base_ccy,
                &state.instrument.quote_ccy,
            )?;
            let (net_fill_delta, effective_fill_price) =
                entry_accounting_delta(accounting, &tracked_order)?;
            let (had_position, position) =
                self.next_open_position(net_fill_delta, effective_fill_price)?;
            self.update_entry_tracking(
                &client_order_id,
                fill_size,
                average_fill_price,
                accounting,
                tracked_order.cancel_requested,
            )?;
            self.open_position(client, had_position, position).await?;
        }

        if order.is_live() {
            let entry_expired = entry_order_is_expired(
                order.created_at_ms()?,
                unix_time_ms()?,
                self.max_entry_order_age_ms,
            )?;
            if fill_size > Decimal::ZERO || !self.signal.entry_allowed() || entry_expired {
                self.cancel_tracked_entry_order(client, &client_order_id)
                    .await?;
            }
            return Ok(());
        }

        if fill_size > Decimal::ZERO {
            self.exchange_mut()?.entry_order = None;
        } else if order.is_terminal_without_fill() {
            self.exchange_mut()?.entry_order = None;
            warn!(
                strategy_id = %self.instance_id,
                instrument = %self.instrument_id,
                client_order_id,
                state = %order.state,
                "OKX entry order closed without fill"
            );
        } else if order.is_terminal() {
            self.exchange_mut()?.entry_order = None;
        }
        Ok(())
    }

    fn update_entry_tracking(
        &mut self,
        client_order_id: &str,
        last_fill_size: Decimal,
        last_average_fill_price: Option<Decimal>,
        accounting: OkxSpotFillAccounting,
        cancel_requested: bool,
    ) -> Result<()> {
        if let Some(entry_order) = self.exchange_mut()?.entry_order.as_mut()
            && entry_order.client_order_id == client_order_id
        {
            entry_order.last_fill_size = last_fill_size;
            entry_order.last_average_fill_price = last_average_fill_price;
            entry_order.last_accounted_base_change = accounting.base_change;
            entry_order.last_accounted_quote_change = accounting.quote_change;
            entry_order.cancel_requested = cancel_requested;
        }
        Ok(())
    }

    async fn cancel_tracked_entry_order(
        &mut self,
        client: &impl OkxClient,
        client_order_id: &str,
    ) -> Result<()> {
        let Some(entry_order) = self.exchange()?.entry_order.as_ref() else {
            return Ok(());
        };
        if entry_order.client_order_id != client_order_id || entry_order.cancel_requested {
            return Ok(());
        }
        client.record_order_decision(Instant::now());
        client
            .cancel_order(&self.instrument_id, client_order_id)
            .await?;
        if let Some(entry_order) = self.exchange_mut()?.entry_order.as_mut()
            && entry_order.client_order_id == client_order_id
        {
            entry_order.cancel_requested = true;
        }
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            client_order_id,
            "requested OKX entry order cancel"
        );
        Ok(())
    }

    async fn open_position(
        &mut self,
        client: &impl OkxClient,
        had_position: bool,
        position: OpenPosition,
    ) -> Result<()> {
        self.exchange_mut()?.position = Some(position);
        self.ensure_stop_loss_order(client).await?;
        if had_position {
            self.refresh_take_profit_order(client).await?;
        }
        self.ensure_take_profit_order(client).await?;
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            quantity = %position.quantity,
            average_price = %position.average_price,
            stop_loss_trigger = %position.stop_loss_trigger,
            "opened OKX strategy position"
        );
        Ok(())
    }

    fn next_open_position(
        &self,
        quantity: Decimal,
        average_price: Decimal,
    ) -> Result<(bool, OpenPosition)> {
        let had_position = self.exchange()?.position.is_some();
        let position = match self.exchange()?.position {
            Some(position) => {
                let new_quantity = position.quantity + quantity;
                let average_price = ((position.average_price * position.quantity)
                    + (average_price * quantity))
                    / new_quantity;
                OpenPosition {
                    quantity: new_quantity,
                    average_price,
                    stop_loss_trigger: self.stop_loss_trigger(average_price)?,
                }
            }
            None => OpenPosition {
                quantity,
                average_price,
                stop_loss_trigger: self.stop_loss_trigger(average_price)?,
            },
        };
        Ok((had_position, position))
    }

    async fn amend_take_profit_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(state) = self.exchange().ok() else {
            return Ok(());
        };
        let Some(position) = state.position else {
            return Ok(());
        };
        let Some(take_profit_order) = state.take_profit_order.clone() else {
            return Ok(());
        };
        if take_profit_order.cancel_requested {
            return Ok(());
        }
        if take_profit_order.last_fill_size > Decimal::ZERO {
            self.cancel_take_profit_order(client).await?;
            return Ok(());
        }

        let target = self.take_profit_order_shape(position.quantity, position.average_price)?;
        state
            .instrument
            .ensure_limit_size(target.size, "OkxEmaAtrMakerTrend take-profit size")?;
        let quote_notional = target
            .size
            .checked_mul(target.price)
            .context("OkxEmaAtrMakerTrend take-profit quote notional overflowed Decimal")?;
        self.ensure_limit_quote_amount(
            client,
            quote_notional,
            "OkxEmaAtrMakerTrend take-profit quote notional",
        )
        .await?;
        let size = decimal_to_okx(target.size);
        let price = decimal_to_okx(target.price);
        client.record_order_decision(Instant::now());
        client
            .amend_order(OkxOrderAmend {
                inst_id: &self.instrument_id,
                side: OrderSide::Sell,
                client_order_id: &take_profit_order.client_order_id,
                new_size: Some(&size),
                new_price: Some(&price),
            })
            .await?;
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            client_order_id = %take_profit_order.client_order_id,
            price,
            size,
            "amended OKX take-profit order"
        );
        Ok(())
    }

    async fn cancel_take_profit_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(take_profit_order) = self
            .exchange()
            .ok()
            .and_then(|state| state.take_profit_order.clone())
        else {
            return Ok(());
        };
        if take_profit_order.cancel_requested {
            return Ok(());
        }
        client.record_order_decision(Instant::now());
        client
            .cancel_order(&self.instrument_id, &take_profit_order.client_order_id)
            .await?;
        if let Some(current_order) = self.exchange_mut()?.take_profit_order.as_mut()
            && current_order.client_order_id == take_profit_order.client_order_id
        {
            current_order.cancel_requested = true;
        }
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            client_order_id = %take_profit_order.client_order_id,
            "requested OKX take-profit cancel"
        );
        Ok(())
    }

    async fn place_take_profit(
        &mut self,
        client: &impl OkxClient,
        quantity: Decimal,
        average_price: Decimal,
    ) -> Result<()> {
        let target = self.take_profit_order_shape(quantity, average_price)?;
        let state = self.exchange()?;
        let min_size = state.instrument.min_size()?;
        let size = target.size;
        ensure!(
            size >= min_size,
            "OkxEmaAtrMakerTrend take-profit size {size} is below OKX minSz {min_size}"
        );
        state
            .instrument
            .ensure_limit_size(target.size, "OkxEmaAtrMakerTrend take-profit size")?;
        let quote_notional = target
            .size
            .checked_mul(target.price)
            .context("OkxEmaAtrMakerTrend take-profit quote notional overflowed Decimal")?;
        self.ensure_limit_quote_amount(
            client,
            quote_notional,
            "OkxEmaAtrMakerTrend take-profit quote notional",
        )
        .await?;
        let price = decimal_to_okx(target.price);
        let size = decimal_to_okx(target.size);
        let client_order_id = client_order_id(&self.instance_id, OrderPurpose::TakeProfit);
        client.record_order_decision(Instant::now());
        client
            .place_order(
                &self.instrument_id,
                OrderSide::Sell,
                OrderKind::Limit,
                &size,
                Some(&price),
                &client_order_id,
            )
            .await?;
        self.exchange_mut()?.take_profit_order = Some(TrackedOrder {
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
            "submitted OKX take-profit order"
        );
        Ok(())
    }

    async fn refresh_take_profit_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(tracked_order) = self
            .exchange()
            .ok()
            .and_then(|state| state.take_profit_order.clone())
        else {
            return Ok(());
        };
        let client_order_id = tracked_order.client_order_id.clone();
        let Some(order) = client.order(&self.instrument_id, &client_order_id).await? else {
            self.exchange_mut()?.take_profit_order = None;
            if self.exchange()?.position.is_some() {
                warn!(
                    strategy_id = %self.instance_id,
                    instrument = %self.instrument_id,
                    client_order_id,
                    cancel_requested = tracked_order.cancel_requested,
                    "tracked OKX take-profit is missing; resubmitting if position still needs protection"
                );
                self.ensure_take_profit_order(client).await?;
            }
            return Ok(());
        };
        let fill_size = order.fill_size()?;
        let fill_delta = cumulative_fill_delta(
            "OKX take-profit order",
            fill_size,
            tracked_order.last_fill_size,
        )?;
        let accounting = order.cumulative_spot_accounting(
            &self.exchange()?.instrument.base_ccy,
            &self.exchange()?.instrument.quote_ccy,
        )?;
        if fill_delta > Decimal::ZERO {
            let base_reduction = exit_accounting_delta(accounting, &tracked_order)?;
            let position_changed = self.apply_position_reducing_fill(base_reduction)?;
            self.update_take_profit_tracking(&client_order_id, fill_size, accounting)?;
            if position_changed {
                self.cancel_stop_loss_order(client).await?;
                self.ensure_stop_loss_order(client).await?;
            }
        }

        if order.is_live() {
            if tracked_order.cancel_requested {
                self.update_take_profit_tracking(&client_order_id, fill_size, accounting)?;
                return Ok(());
            }
            let Some(position) = self.exchange()?.position else {
                warn!(
                    strategy_id = %self.instance_id,
                    instrument = %self.instrument_id,
                    client_order_id,
                    "OKX take-profit is live without a tracked position; canceling"
                );
                self.cancel_take_profit_order(client).await?;
                return Ok(());
            };
            if !self.take_profit_order_matches_position(&order, position)? {
                warn!(
                    strategy_id = %self.instance_id,
                    instrument = %self.instrument_id,
                    client_order_id,
                    "OKX take-profit no longer matches position; amending"
                );
                self.amend_take_profit_order(client).await?;
                return Ok(());
            }
            self.update_take_profit_tracking(&client_order_id, fill_size, accounting)?;
            return Ok(());
        }

        if order.is_terminal() {
            self.exchange_mut()?.take_profit_order = None;
        }

        if self.exchange()?.position.is_none() {
            self.cancel_stop_loss_order(client).await?;
            self.clear_position_state();
            info!(
                strategy_id = %self.instance_id,
                instrument = %self.instrument_id,
                client_order_id,
                "OKX take-profit filled"
            );
        } else if order.is_terminal_without_fill() || fill_delta > Decimal::ZERO {
            if self.exchange()?.position.is_none() {
                return Ok(());
            }
            warn!(
                strategy_id = %self.instance_id,
                instrument = %self.instrument_id,
                client_order_id,
                state = %order.state,
                fill_delta = %fill_delta,
                "OKX take-profit closed with remaining position; resubmitting"
            );
            self.ensure_take_profit_order(client).await?;
            self.ensure_stop_loss_order(client).await?;
        }
        Ok(())
    }

    fn take_profit_order_matches_position(
        &self,
        order: &OkxOrder,
        position: OpenPosition,
    ) -> Result<bool> {
        let state = self.exchange()?;
        let lot_size = state.instrument.lot_size()?;
        let tick_size = state.instrument.tick_size()?;
        let expected_shape =
            self.take_profit_order_shape(position.quantity, position.average_price)?;
        let requested_size = order.requested_size()?;
        if !decimal_within_half_step(requested_size, expected_shape.size, lot_size) {
            return Ok(false);
        }
        if order.price.trim().is_empty() {
            return Ok(false);
        }
        let requested_price = order.price.parse::<Decimal>().with_context(|| {
            format!(
                "OKX take-profit order {} has invalid px {}",
                order.client_order_id, order.price
            )
        })?;
        self.ensure_take_profit_clears_exit_fee(position.average_price, requested_price)?;
        Ok(decimal_within_half_step(
            requested_price,
            expected_shape.price,
            tick_size,
        ))
    }

    fn take_profit_order_shape(
        &self,
        quantity: Decimal,
        average_price: Decimal,
    ) -> Result<TakeProfitOrderShape> {
        let state = self.exchange()?;
        let price = quantize_decimal_up(
            self.take_profit_price(average_price)?,
            state.instrument.tick_size()?,
        )?;
        self.ensure_take_profit_clears_exit_fee(average_price, price)?;
        Ok(TakeProfitOrderShape {
            size: quantize_decimal_down(quantity, state.instrument.lot_size()?)?,
            price,
        })
    }

    fn ensure_take_profit_clears_exit_fee(
        &self,
        average_price: Decimal,
        take_profit_price: Decimal,
    ) -> Result<()> {
        ensure!(
            average_price > Decimal::ZERO,
            "OkxEmaAtrMakerTrend average price must be positive"
        );
        ensure!(
            take_profit_price > Decimal::ZERO,
            "OkxEmaAtrMakerTrend take-profit price must be positive"
        );
        let exit_fee_cost_rate = self
            .exit_fee_cost_rate
            .context("OkxEmaAtrMakerTrend exit fee rate is not initialized")?;
        ensure!(
            exit_fee_cost_rate >= Decimal::ZERO && exit_fee_cost_rate < Decimal::ONE,
            "OkxEmaAtrMakerTrend exit fee cost rate must be non-negative and below one"
        );
        let net_quote_proceeds = take_profit_price * (Decimal::ONE - exit_fee_cost_rate);
        ensure!(
            net_quote_proceeds >= average_price,
            "OkxEmaAtrMakerTrend take-profit price {take_profit_price} does not recover fee-adjusted average cost {average_price} after exit fee rate {exit_fee_cost_rate}; net quote proceeds per base are {net_quote_proceeds}"
        );
        Ok(())
    }

    fn apply_position_reducing_fill(&mut self, fill_delta: Decimal) -> Result<bool> {
        let min_size = self.exchange()?.instrument.min_size()?;
        let Some(position) = self.exchange_mut()?.position.as_mut() else {
            return Ok(false);
        };
        position.quantity = position
            .quantity
            .checked_sub(fill_delta)
            .filter(|remaining| *remaining > Decimal::ZERO)
            .unwrap_or(Decimal::ZERO);
        if position.quantity < min_size {
            self.exchange_mut()?.position = None;
            return Ok(true);
        }
        Ok(true)
    }

    fn update_take_profit_tracking(
        &mut self,
        client_order_id: &str,
        last_fill_size: Decimal,
        accounting: OkxSpotFillAccounting,
    ) -> Result<()> {
        if let Some(take_profit_order) = self.exchange_mut()?.take_profit_order.as_mut()
            && take_profit_order.client_order_id == client_order_id
        {
            take_profit_order.last_fill_size = last_fill_size;
            take_profit_order.last_accounted_base_change = accounting.base_change;
            take_profit_order.last_accounted_quote_change = accounting.quote_change;
        }
        Ok(())
    }

    async fn ensure_take_profit_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(state) = self.exchange().ok() else {
            return Ok(());
        };
        let Some(position) = state.position else {
            return Ok(());
        };
        if state.take_profit_order.is_some()
            || state.stop_loss_exit_order.is_some()
            || state.stop_loss_pending.is_some()
        {
            return Ok(());
        }
        warn!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            "OKX position has no active take-profit; resubmitting"
        );
        self.place_take_profit(client, position.quantity, position.average_price)
            .await
    }

    async fn ensure_stop_loss_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(state) = self.exchange().ok() else {
            return Ok(());
        };
        let Some(position) = state.position else {
            return Ok(());
        };
        if state.stop_loss_pending.is_some() || state.stop_loss_exit_order.is_some() {
            return Ok(());
        }
        if let Some(stop_loss_order) = state.stop_loss_order.clone() {
            if stop_loss_order.cancel_requested {
                return Ok(());
            }
            if self.stop_loss_order_matches_position(&stop_loss_order, position)? {
                return Ok(());
            }
            warn!(
                strategy_id = %self.instance_id,
                instrument = %self.instrument_id,
                algo_id = %stop_loss_order.algo_id,
                client_order_id = %stop_loss_order.client_order_id,
                "OKX stop-loss trigger algo no longer matches position; replacing"
            );
            self.cancel_stop_loss_order(client).await?;
            return Ok(());
        }
        self.place_stop_loss_order(client, position).await
    }

    fn stop_loss_order_matches_position(
        &self,
        stop_loss_order: &TrackedAlgoOrder,
        position: OpenPosition,
    ) -> Result<bool> {
        let state = self.exchange()?;
        let lot_size = state.instrument.lot_size()?;
        let tick_size = state.instrument.tick_size()?;
        let expected_size = quantize_decimal_down(position.quantity, lot_size)?;
        let expected_trigger = quantize_decimal_down(position.stop_loss_trigger, tick_size)?;
        Ok(
            decimal_within_half_step(stop_loss_order.size, expected_size, lot_size)
                && decimal_within_half_step(
                    stop_loss_order.trigger_price,
                    expected_trigger,
                    tick_size,
                ),
        )
    }

    async fn refresh_stop_loss_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(tracked_order) = self
            .exchange()
            .ok()
            .and_then(|state| state.stop_loss_order.clone())
        else {
            return Ok(());
        };
        let open_algo_orders = client.open_algo_orders(&self.instrument_id).await?;
        if let Some(order) = open_algo_orders
            .iter()
            .find(|order| order.algo_id == tracked_order.algo_id)
        {
            self.exchange_mut()?.stop_loss_order = Some(tracked_stop_loss_algo(
                order,
                tracked_order.cancel_requested,
            )?);
            let Some(position) = self.exchange()?.position else {
                return Ok(());
            };
            let state = self.exchange()?;
            if state.stop_loss_pending == Some(StopLossPendingReason::LocalThreshold)
                && state.stop_loss_exit_order.is_none()
                && state.take_profit_order.is_none()
            {
                let last = client.ticker(&self.instrument_id).await?.last_decimal()?;
                if last > position.stop_loss_trigger {
                    self.exchange_mut()?.stop_loss_pending = None;
                    info!(
                        strategy_id = %self.instance_id,
                        instrument = %self.instrument_id,
                        algo_id = %tracked_order.algo_id,
                        client_order_id = %tracked_order.client_order_id,
                        last = %last,
                        stop_loss_trigger = %position.stop_loss_trigger,
                        "OKX stop-loss trigger algo is still live and price recovered; restoring take-profit management"
                    );
                    self.ensure_take_profit_order(client).await?;
                }
            }
            return Ok(());
        }

        let Some(position) = self.exchange()?.position else {
            self.exchange_mut()?.stop_loss_order = None;
            return Ok(());
        };
        let algo_history = client.algo_order_history(&self.instrument_id).await?;
        let mut finished_without_execution = false;
        if let Some(history_order) = algo_history.iter().find(|order| {
            order.algo_id == tracked_order.algo_id
                || order.client_order_id == tracked_order.client_order_id
        }) {
            ensure!(
                history_order.parsed_side() == Some(OrderSide::Sell),
                "historical OKX stop-loss algo {} has unexpected side {}",
                tracked_order.client_order_id,
                history_order.side
            );
            if history_order.is_effective() {
                let last = client.ticker(&self.instrument_id).await?.last_decimal()?;
                self.reconcile_triggered_stop_loss_order(client, &tracked_order, position, last)
                    .await?;
                return Ok(());
            }
            if history_order.is_terminal_without_execution() {
                self.exchange_mut()?.stop_loss_order = None;
                finished_without_execution = true;
                warn!(
                    strategy_id = %self.instance_id,
                    instrument = %self.instrument_id,
                    algo_id = %tracked_order.algo_id,
                    client_order_id = %tracked_order.client_order_id,
                    state = %history_order.state,
                    "OKX stop-loss trigger algo finished without execution; will resubmit"
                );
            }
        }
        let stop_loss_pending = self.exchange()?.stop_loss_pending;
        let last = client.ticker(&self.instrument_id).await?.last_decimal()?;
        if stop_loss_pending.is_some() || last <= position.stop_loss_trigger {
            self.reconcile_triggered_stop_loss_order(client, &tracked_order, position, last)
                .await?;
        } else {
            self.exchange_mut()?.stop_loss_order = None;
            if !finished_without_execution {
                warn!(
                    strategy_id = %self.instance_id,
                    instrument = %self.instrument_id,
                    algo_id = %tracked_order.algo_id,
                    client_order_id = %tracked_order.client_order_id,
                    "OKX stop-loss trigger algo is missing before trigger; will resubmit"
                );
            }
        }
        Ok(())
    }

    async fn reconcile_triggered_stop_loss_order(
        &mut self,
        client: &impl OkxClient,
        tracked_order: &TrackedAlgoOrder,
        position: OpenPosition,
        last: Decimal,
    ) -> Result<()> {
        if let Some(take_profit_order) = self
            .exchange()
            .ok()
            .and_then(|state| state.take_profit_order.clone())
            && cancel_if_live(
                client,
                &self.instrument_id,
                &take_profit_order.client_order_id,
            )
            .await
        {
            self.mark_take_profit_cancel_requested(&take_profit_order.client_order_id)?;
        }

        if let Some(reconciled_position) = self
            .reconcile_position_quantity_from_balance(client)
            .await?
        {
            let state = self.exchange_mut()?;
            state.stop_loss_order = None;
            state.stop_loss_pending = Some(StopLossPendingReason::ExitReconciliation);
            warn!(
                strategy_id = %self.instance_id,
                instrument = %self.instrument_id,
                algo_id = %tracked_order.algo_id,
                client_order_id = %tracked_order.client_order_id,
                remaining_quantity = %reconciled_position.quantity,
                last = %last,
                stop_loss_trigger = %position.stop_loss_trigger,
                "OKX stop-loss trigger algo is no longer pending but base balance remains; preserving position for fallback exit"
            );
            return Ok(());
        }
        self.clear_position_state();
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            algo_id = %tracked_order.algo_id,
            client_order_id = %tracked_order.client_order_id,
            last = %last,
            stop_loss_trigger = %position.stop_loss_trigger,
            "OKX stop-loss trigger algo is no longer pending and no base balance remains; treating position as exited"
        );
        Ok(())
    }

    async fn reconcile_position_quantity_from_balance(
        &mut self,
        client: &impl OkxClient,
    ) -> Result<Option<OpenPosition>> {
        let Some(position) = self.exchange()?.position else {
            return Ok(None);
        };
        let balances = client.balances().await?;
        let account_quantity = self
            .reconstruct_strategy_balance(&balances)?
            .tradeable_quantity;
        let quantity = account_quantity.min(position.quantity);
        if quantity <= Decimal::ZERO {
            self.clear_position_state();
            return Ok(None);
        }
        let position = OpenPosition {
            quantity,
            ..position
        };
        self.exchange_mut()?.position = Some(position);
        Ok(Some(position))
    }

    async fn place_stop_loss_order(
        &mut self,
        client: &impl OkxClient,
        position: OpenPosition,
    ) -> Result<()> {
        let state = self.exchange()?;
        let size = quantize_decimal_down(position.quantity, state.instrument.lot_size()?)?;
        ensure!(
            size >= state.instrument.min_size()?,
            "OkxEmaAtrMakerTrend stop size {size} is below OKX minSz {}",
            state.instrument.min_size()?
        );
        state
            .instrument
            .ensure_trigger_size(size, "OkxEmaAtrMakerTrend stop size")?;
        let trigger_price =
            quantize_decimal_down(position.stop_loss_trigger, state.instrument.tick_size()?)?;
        let tracked_size = size;
        let tracked_trigger_price = trigger_price;
        let size = decimal_to_okx(size);
        let trigger_price = decimal_to_okx(trigger_price);
        let client_order_id = client_order_id(&self.instance_id, OrderPurpose::StopLoss);
        client.record_order_decision(Instant::now());
        let acknowledgement = client
            .place_trigger_order(
                &self.instrument_id,
                OrderSide::Sell,
                &size,
                &trigger_price,
                &client_order_id,
            )
            .await?;
        self.exchange_mut()?.stop_loss_order = Some(TrackedAlgoOrder {
            algo_id: acknowledgement.algo_id.clone(),
            client_order_id: client_order_id.clone(),
            size: tracked_size,
            trigger_price: tracked_trigger_price,
            cancel_requested: false,
        });
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            algo_id = %acknowledgement.algo_id,
            client_order_id,
            trigger_price,
            size,
            "submitted OKX stop-loss trigger algo"
        );
        Ok(())
    }

    async fn cancel_stop_loss_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(stop_loss_order) = self
            .exchange()
            .ok()
            .and_then(|state| state.stop_loss_order.clone())
        else {
            return Ok(());
        };
        if stop_loss_order.cancel_requested {
            return Ok(());
        }
        client.record_order_decision(Instant::now());
        client
            .cancel_algo_order(&self.instrument_id, &stop_loss_order.algo_id)
            .await?;
        if let Some(current_order) = self.exchange_mut()?.stop_loss_order.as_mut()
            && current_order.algo_id == stop_loss_order.algo_id
        {
            current_order.cancel_requested = true;
        }
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            algo_id = %stop_loss_order.algo_id,
            client_order_id = %stop_loss_order.client_order_id,
            "requested OKX stop-loss trigger algo cancel"
        );
        Ok(())
    }

    async fn refresh_stop_loss_exit_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(tracked_order) = self
            .exchange()
            .ok()
            .and_then(|state| state.stop_loss_exit_order.clone())
        else {
            return Ok(());
        };
        let client_order_id = tracked_order.client_order_id.clone();
        let Some(order) = client.order(&self.instrument_id, &client_order_id).await? else {
            self.exchange_mut()?.stop_loss_exit_order = None;
            if let Some(reconciled_position) = self
                .reconcile_position_quantity_from_balance(client)
                .await?
            {
                self.exchange_mut()?.stop_loss_pending =
                    Some(StopLossPendingReason::ExitReconciliation);
                warn!(
                    strategy_id = %self.instance_id,
                    instrument = %self.instrument_id,
                    client_order_id,
                    remaining_quantity = %reconciled_position.quantity,
                    "tracked OKX stop-loss market exit is missing while base balance remains; retrying"
                );
            } else {
                info!(
                    strategy_id = %self.instance_id,
                    instrument = %self.instrument_id,
                    client_order_id,
                    "tracked OKX stop-loss market exit is missing and no base balance remains; treating position as exited"
                );
            }
            return Ok(());
        };
        let fill_size = order.fill_size()?;
        let fill_delta = cumulative_fill_delta(
            "OKX stop-loss market exit",
            fill_size,
            tracked_order.last_fill_size,
        )?;
        let accounting = order.cumulative_spot_accounting(
            &self.exchange()?.instrument.base_ccy,
            &self.exchange()?.instrument.quote_ccy,
        )?;
        if fill_delta > Decimal::ZERO {
            self.apply_position_reducing_fill(exit_accounting_delta(accounting, &tracked_order)?)?;
        }

        if order.is_live() {
            self.update_stop_loss_exit_tracking(&client_order_id, fill_size, accounting)?;
            return Ok(());
        }

        self.exchange_mut()?.stop_loss_exit_order = None;
        if self.exchange()?.position.is_none() {
            self.clear_position_state();
            info!(
                strategy_id = %self.instance_id,
                instrument = %self.instrument_id,
                client_order_id,
                "OKX stop-loss market exit filled"
            );
        } else if order.is_terminal_without_fill() || order.is_terminal() {
            self.exchange_mut()?.stop_loss_pending =
                Some(StopLossPendingReason::ExitReconciliation);
            warn!(
                strategy_id = %self.instance_id,
                instrument = %self.instrument_id,
                client_order_id,
                state = %order.state,
                fill_delta = %fill_delta,
                "OKX stop-loss market exit closed with remaining position; retrying"
            );
        }
        Ok(())
    }

    fn update_stop_loss_exit_tracking(
        &mut self,
        client_order_id: &str,
        last_fill_size: Decimal,
        accounting: OkxSpotFillAccounting,
    ) -> Result<()> {
        if let Some(stop_loss_exit_order) = self.exchange_mut()?.stop_loss_exit_order.as_mut()
            && stop_loss_exit_order.client_order_id == client_order_id
        {
            stop_loss_exit_order.last_fill_size = last_fill_size;
            stop_loss_exit_order.last_accounted_base_change = accounting.base_change;
            stop_loss_exit_order.last_accounted_quote_change = accounting.quote_change;
        }
        Ok(())
    }

    async fn cancel_stop_loss_exit_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(stop_loss_exit_order) = self
            .exchange()
            .ok()
            .and_then(|state| state.stop_loss_exit_order.clone())
        else {
            return Ok(());
        };
        if stop_loss_exit_order.cancel_requested {
            return Ok(());
        }
        client.record_order_decision(Instant::now());
        client
            .cancel_order(&self.instrument_id, &stop_loss_exit_order.client_order_id)
            .await?;
        if let Some(current_order) = self.exchange_mut()?.stop_loss_exit_order.as_mut()
            && current_order.client_order_id == stop_loss_exit_order.client_order_id
        {
            current_order.cancel_requested = true;
        }
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            client_order_id = %stop_loss_exit_order.client_order_id,
            "requested OKX stop-loss market exit cancel during shutdown"
        );
        Ok(())
    }

    async fn evaluate_stop_loss(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(position) = self.exchange().ok().and_then(|state| state.position) else {
            return Ok(());
        };
        let stop_loss_pending = self
            .exchange()
            .map(|state| state.stop_loss_pending)
            .unwrap_or(None);
        let (last, market_reference_price) = if stop_loss_pending.is_some() {
            (position.stop_loss_trigger, None)
        } else {
            let ticker = client.ticker(&self.instrument_id).await?;
            (ticker.last_decimal()?, Some(ticker.ask_decimal()?))
        };
        if stop_loss_pending.is_none() && last > position.stop_loss_trigger {
            return Ok(());
        }
        if stop_loss_pending.is_none() {
            self.exchange_mut()?.stop_loss_pending = Some(StopLossPendingReason::LocalThreshold);
        }

        if let Some(tracked_order) = self
            .exchange()
            .ok()
            .and_then(|state| state.take_profit_order.clone())
        {
            let client_order_id = tracked_order.client_order_id.clone();
            let Some(order) = client.order(&self.instrument_id, &client_order_id).await? else {
                if cancel_if_live(client, &self.instrument_id, &client_order_id).await {
                    self.mark_take_profit_cancel_requested(&client_order_id)?;
                }
                return Ok(());
            };
            if order.is_live() {
                if !tracked_order.cancel_requested {
                    client.record_order_decision(Instant::now());
                    client
                        .cancel_order(&self.instrument_id, &client_order_id)
                        .await?;
                    self.mark_take_profit_cancel_requested(&client_order_id)?;
                    info!(
                        strategy_id = %self.instance_id,
                        instrument = %self.instrument_id,
                        client_order_id,
                        "requested OKX take-profit cancel before stop-loss exit"
                    );
                }
                return Ok(());
            }
            let fill_size = order.fill_size()?;
            let fill_delta = cumulative_fill_delta(
                "OKX take-profit order",
                fill_size,
                tracked_order.last_fill_size,
            )?;
            if fill_delta > Decimal::ZERO && stop_loss_pending.is_none() {
                let accounting = order.cumulative_spot_accounting(
                    &self.exchange()?.instrument.base_ccy,
                    &self.exchange()?.instrument.quote_ccy,
                )?;
                self.apply_position_reducing_fill(exit_accounting_delta(
                    accounting,
                    &tracked_order,
                )?)?;
            }
            self.exchange_mut()?.take_profit_order = None;
            let Some(position) = self.exchange()?.position else {
                return Ok(());
            };
            self.exchange_mut()?.position = Some(position);
        }
        if let Some(stop_loss_order) = self.exchange()?.stop_loss_order.as_ref() {
            warn!(
                strategy_id = %self.instance_id,
                instrument = %self.instrument_id,
                algo_id = %stop_loss_order.algo_id,
                client_order_id = %stop_loss_order.client_order_id,
                last = %last,
                stop_loss_trigger = %position.stop_loss_trigger,
                "OKX stop-loss trigger algo is active; local market exit suppressed"
            );
            return Ok(());
        }
        if self.exchange()?.stop_loss_exit_order.is_some() {
            return Ok(());
        }
        if stop_loss_pending.is_some()
            && self
                .reconcile_position_quantity_from_balance(client)
                .await?
                .is_none()
        {
            return Ok(());
        }
        let Some(position) = self.exchange()?.position else {
            return Ok(());
        };
        let state = self.exchange()?;
        let size = quantize_decimal_down(position.quantity, state.instrument.lot_size()?)?;
        ensure!(
            size >= state.instrument.min_size()?,
            "OkxEmaAtrMakerTrend stop size {size} is below OKX minSz {}",
            state.instrument.min_size()?
        );
        let market_reference_price = match market_reference_price {
            Some(price) => price,
            None => client.ticker(&self.instrument_id).await?.ask_decimal()?,
        };
        state.instrument.ensure_spot_market_sell_size(
            size,
            market_reference_price,
            "OkxEmaAtrMakerTrend stop market size",
        )?;
        let size = decimal_to_okx(size);
        let client_order_id = client_order_id(&self.instance_id, OrderPurpose::StopLoss);
        client.record_order_decision(Instant::now());
        client
            .place_order(
                &self.instrument_id,
                OrderSide::Sell,
                OrderKind::Market,
                &size,
                None,
                &client_order_id,
            )
            .await?;
        {
            let state = self.exchange_mut()?;
            state.stop_loss_pending = Some(StopLossPendingReason::ExitReconciliation);
            state.stop_loss_exit_order = Some(TrackedOrder {
                client_order_id: client_order_id.clone(),
                last_fill_size: Decimal::ZERO,
                last_average_fill_price: None,
                last_accounted_base_change: Decimal::ZERO,
                last_accounted_quote_change: Decimal::ZERO,
                cancel_requested: false,
            });
        }
        warn!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            client_order_id,
            last = %last,
            stop_loss_trigger = %position.stop_loss_trigger,
            size,
            "submitted OKX stop-loss market exit; awaiting fill confirmation"
        );
        Ok(())
    }

    fn mark_take_profit_cancel_requested(&mut self, client_order_id: &str) -> Result<()> {
        if let Some(take_profit_order) = self.exchange_mut()?.take_profit_order.as_mut()
            && take_profit_order.client_order_id == client_order_id
        {
            take_profit_order.cancel_requested = true;
        }
        Ok(())
    }

    fn take_profit_price(&self, average_price: Decimal) -> Result<Decimal> {
        ensure!(
            average_price > Decimal::ZERO,
            "OkxEmaAtrMakerTrend average price must be positive"
        );
        let atr = self.signal_atr_decimal()?;
        let atr_price = average_price + self.take_profit_atr_multiple * atr;
        let exit_fee_cost_rate = self
            .exit_fee_cost_rate
            .context("OkxEmaAtrMakerTrend exit fee rate is not initialized")?;
        ensure!(
            exit_fee_cost_rate >= Decimal::ZERO && exit_fee_cost_rate < Decimal::ONE,
            "OkxEmaAtrMakerTrend exit fee cost rate must be non-negative and below one"
        );
        let fee_break_even_price = average_price / (Decimal::ONE - exit_fee_cost_rate);
        let price = atr_price.max(fee_break_even_price);
        ensure!(
            price > Decimal::ZERO,
            "OkxEmaAtrMakerTrend take-profit price must be positive"
        );
        Ok(price)
    }

    fn stop_loss_trigger(&self, average_price: Decimal) -> Result<Decimal> {
        ensure!(
            average_price > Decimal::ZERO,
            "OkxEmaAtrMakerTrend average price must be positive"
        );
        let atr = self.signal_atr_decimal()?;
        let trigger = average_price - self.stop_loss_atr_multiple * atr;
        ensure!(
            trigger > Decimal::ZERO,
            "OkxEmaAtrMakerTrend stop-loss trigger must be positive"
        );
        Ok(trigger)
    }

    fn signal_atr_decimal(&self) -> Result<Decimal> {
        let atr = self
            .signal
            .last_atr()
            .context("OkxEmaAtrMakerTrend ATR is not initialized")?;
        ensure!(
            atr.is_finite() && atr > 0.0 && atr.is_normal(),
            "OkxEmaAtrMakerTrend ATR must be finite, positive, and normal before Decimal order math"
        );
        // This is the f64-to-Decimal order boundary for ATR-derived protection
        // prices. OKX prices and sizes must be quantized from Decimal and
        // serialized with decimal_to_okx before submission.
        Decimal::from_f64(atr).context("OKX ATR cannot be represented as Decimal")
    }

    fn exchange(&self) -> Result<&ExchangeState> {
        self.exchange
            .as_ref()
            .context("OkxEmaAtrMakerTrend exchange state is not initialized")
    }

    fn exchange_mut(&mut self) -> Result<&mut ExchangeState> {
        self.exchange
            .as_mut()
            .context("OkxEmaAtrMakerTrend exchange state is not initialized")
    }

    fn clear_position_state(&mut self) {
        if let Some(state) = self.exchange.as_mut() {
            state.position = None;
            state.entry_order = None;
            state.take_profit_order = None;
            state.stop_loss_order = None;
            state.stop_loss_exit_order = None;
            state.stop_loss_pending = None;
        }
    }
}

fn find_live_strategy_order(
    open_orders: &[OkxOrder],
    strategy_tag: &str,
    purpose: OrderPurpose,
    side: OrderSide,
    instrument: &OkxInstrument,
) -> Result<Option<TrackedOrder>> {
    let mut matching_orders = open_orders
        .iter()
        .filter(|order| order.is_live())
        .filter(|order| {
            parse_strategy_client_order_id(&order.client_order_id, strategy_tag) == Some(purpose)
                && order.parsed_side() == Some(side)
        });
    let Some(order) = matching_orders.next() else {
        return Ok(None);
    };
    if let Some(duplicate) = matching_orders.next() {
        bail!(
            "multiple live OKX strategy orders found for tag {strategy_tag}: {} and {}",
            order.client_order_id,
            duplicate.client_order_id
        );
    }
    let accounting =
        order.cumulative_spot_accounting(&instrument.base_ccy, &instrument.quote_ccy)?;
    Ok(Some(TrackedOrder {
        client_order_id: order.client_order_id.clone(),
        last_fill_size: order.fill_size()?,
        last_average_fill_price: order.average_fill_price()?,
        last_accounted_base_change: accounting.base_change,
        last_accounted_quote_change: accounting.quote_change,
        cancel_requested: false,
    }))
}

fn entry_accounting_delta(
    accounting: OkxSpotFillAccounting,
    tracked_order: &TrackedOrder,
) -> Result<(Decimal, Decimal)> {
    let base_delta = accounting.base_change - tracked_order.last_accounted_base_change;
    let quote_spent = -accounting.quote_change;
    let previous_quote_spent = -tracked_order.last_accounted_quote_change;
    let quote_spent_delta = quote_spent - previous_quote_spent;
    ensure!(
        base_delta > Decimal::ZERO,
        "OKX entry net base fill delta must be positive"
    );
    ensure!(
        quote_spent_delta > Decimal::ZERO,
        "OKX entry quote cost delta must be positive"
    );
    let delta_price = quote_spent_delta / base_delta;
    ensure!(
        delta_price > Decimal::ZERO,
        "OKX fee-adjusted entry fill delta price must be positive"
    );
    Ok((base_delta, delta_price))
}

fn exit_accounting_delta(
    accounting: OkxSpotFillAccounting,
    tracked_order: &TrackedOrder,
) -> Result<Decimal> {
    let base_delta = accounting.base_change - tracked_order.last_accounted_base_change;
    ensure!(
        base_delta < Decimal::ZERO,
        "OKX exit net base fill delta must be negative"
    );
    let quote_delta = accounting.quote_change - tracked_order.last_accounted_quote_change;
    ensure!(
        quote_delta > Decimal::ZERO,
        "OKX exit net quote proceeds delta must be positive"
    );
    Ok(-base_delta)
}

fn entry_order_is_expired(created_at_ms: i64, now_ms: i64, max_age_ms: u64) -> Result<bool> {
    ensure!(max_age_ms > 0, "OKX entry maximum age must be positive");
    ensure!(
        now_ms >= created_at_ms,
        "OKX entry order cTime {created_at_ms} is later than local time {now_ms}; refusing stale-order decision"
    );
    let age_ms = now_ms - created_at_ms;
    let max_age_ms = i64::try_from(max_age_ms).context("OKX entry maximum age exceeds i64")?;
    Ok(age_ms >= max_age_ms)
}

fn unix_time_ms() -> Result<i64> {
    let milliseconds = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    i64::try_from(milliseconds).context("current UTC timestamp milliseconds exceed i64")
}

fn cumulative_fill_delta(
    context: &str,
    fill_size: Decimal,
    last_fill_size: Decimal,
) -> Result<Decimal> {
    ensure!(
        fill_size >= Decimal::ZERO,
        "{context} cumulative fill size must be non-negative"
    );
    ensure!(
        last_fill_size >= Decimal::ZERO,
        "{context} tracked fill size must be non-negative"
    );
    ensure!(
        fill_size >= last_fill_size,
        "{context} cumulative fill size decreased from {last_fill_size} to {fill_size}; refusing to reconcile inconsistent OKX fill state"
    );
    Ok(fill_size - last_fill_size)
}

fn half(value: Decimal) -> Decimal {
    value / Decimal::from(2)
}

fn decimal_within_half_step(value: Decimal, expected: Decimal, step: Decimal) -> bool {
    let distance = if value >= expected {
        value - expected
    } else {
        expected - value
    };
    distance <= half(step)
}

fn find_live_strategy_stop_loss_algo(
    open_algo_orders: &[OkxAlgoOrder],
    strategy_tag: &str,
) -> Result<Option<TrackedAlgoOrder>> {
    let mut matching_orders = open_algo_orders
        .iter()
        .filter(|order| order.is_live())
        .filter(|order| {
            parse_strategy_client_order_id(&order.client_order_id, strategy_tag)
                == Some(OrderPurpose::StopLoss)
                && order.parsed_side() == Some(OrderSide::Sell)
        });
    let Some(order) = matching_orders.next() else {
        return Ok(None);
    };
    if let Some(duplicate) = matching_orders.next() {
        bail!(
            "multiple live OKX strategy stop-loss algos found for tag {strategy_tag}: {} and {}",
            order.client_order_id,
            duplicate.client_order_id
        );
    }
    Ok(Some(tracked_stop_loss_algo(
        order, /*cancel_requested*/ false,
    )?))
}

fn tracked_stop_loss_algo(
    order: &OkxAlgoOrder,
    cancel_requested: bool,
) -> Result<TrackedAlgoOrder> {
    ensure!(
        order.is_live(),
        "live OKX stop-loss algo {} has unexpected state {:?}; expected explicit live or pause",
        order.client_order_id,
        order.state
    );
    ensure!(
        order.parsed_side() == Some(OrderSide::Sell),
        "live OKX stop-loss algo {} has unexpected side {}",
        order.client_order_id,
        order.side
    );
    ensure!(
        order.is_trigger_market_order(),
        "live OKX stop-loss algo {} has unexpected type {} and orderPx {}",
        order.client_order_id,
        order.order_type,
        order.order_price
    );
    Ok(TrackedAlgoOrder {
        algo_id: order.algo_id.clone(),
        client_order_id: order.client_order_id.clone(),
        size: order.requested_size()?,
        trigger_price: order.trigger_price()?,
        cancel_requested,
    })
}

fn reject_live_legacy_strategy_ids(
    open_orders: &[OkxOrder],
    open_algo_orders: &[OkxAlgoOrder],
    strategy_id: &str,
    strategy_tag: &str,
    configured_strategy_tags: &[String],
) -> Result<()> {
    let legacy_tag = legacy_strategy_tag(strategy_id);
    for order in open_orders.iter().filter(|order| order.is_live()) {
        if live_legacy_strategy_id(
            &order.client_order_id,
            strategy_tag,
            &legacy_tag,
            configured_strategy_tags,
        ) {
            bail!(
                "live OKX legacy strategy order id {} for strategy instance {}; close or reconcile legacy orders before using current ownership tag",
                order.client_order_id,
                strategy_id
            );
        }
    }
    for order in open_algo_orders.iter().filter(|order| order.is_live()) {
        if live_legacy_strategy_id(
            &order.client_order_id,
            strategy_tag,
            &legacy_tag,
            configured_strategy_tags,
        ) {
            bail!(
                "live OKX legacy strategy algo id {} for strategy instance {}; close or reconcile legacy algos before using current ownership tag",
                order.client_order_id,
                strategy_id
            );
        }
    }
    Ok(())
}

fn live_legacy_strategy_id(
    client_order_id: &str,
    strategy_tag: &str,
    legacy_strategy_tag: &str,
    configured_strategy_tags: &[String],
) -> bool {
    parse_strategy_client_order_id(client_order_id, strategy_tag).is_none()
        && !configured_strategy_tags
            .iter()
            .any(|tag| parse_strategy_client_order_id(client_order_id, tag).is_some())
        && parse_legacy_strategy_client_order_id(client_order_id, legacy_strategy_tag).is_some()
}

fn reject_unknown_live_strategy_orders(open_orders: &[OkxOrder], strategy_tag: &str) -> Result<()> {
    for order in open_orders.iter().filter(|order| order.is_live()) {
        if !order
            .client_order_id
            .starts_with(&format!("{ORDER_ID_PREFIX}{strategy_tag}"))
        {
            continue;
        }
        let Some(purpose) = parse_strategy_client_order_id(&order.client_order_id, strategy_tag)
        else {
            bail!(
                "unknown live OKX strategy order id {} for tag {strategy_tag}",
                order.client_order_id
            );
        };
        let expected_side = match purpose {
            OrderPurpose::Entry => OrderSide::Buy,
            OrderPurpose::TakeProfit => OrderSide::Sell,
            OrderPurpose::StopLoss => OrderSide::Sell,
        };
        if order.parsed_side() != Some(expected_side) {
            bail!(
                "live OKX strategy order {} has unexpected side {}",
                order.client_order_id,
                order.side
            );
        }
        if purpose == OrderPurpose::Entry && order.parsed_kind() != Some(OrderKind::PostOnly) {
            bail!(
                "live OKX entry order {} has unexpected type {}",
                order.client_order_id,
                order.order_type
            );
        }
        if purpose == OrderPurpose::TakeProfit && order.parsed_kind() != Some(OrderKind::Limit) {
            bail!(
                "live OKX take-profit order {} has unexpected type {}",
                order.client_order_id,
                order.order_type
            );
        }
        if purpose == OrderPurpose::StopLoss && order.parsed_kind() != Some(OrderKind::Market) {
            bail!(
                "live OKX stop-loss market exit {} has unexpected type {}",
                order.client_order_id,
                order.order_type
            );
        }
    }
    Ok(())
}

fn reject_unknown_live_strategy_algo_orders(
    open_algo_orders: &[OkxAlgoOrder],
    strategy_tag: &str,
) -> Result<()> {
    for order in open_algo_orders {
        if !order
            .client_order_id
            .starts_with(&format!("{ORDER_ID_PREFIX}{strategy_tag}"))
        {
            continue;
        }
        let Some(purpose) = parse_strategy_client_order_id(&order.client_order_id, strategy_tag)
        else {
            bail!(
                "unknown live OKX strategy algo id {} for tag {strategy_tag}",
                order.client_order_id
            );
        };
        ensure!(
            order.is_live(),
            "live OKX strategy algo {} has unexpected state {:?}; expected explicit live or pause",
            order.client_order_id,
            order.state
        );
        ensure!(
            purpose == OrderPurpose::StopLoss,
            "live OKX algo {} has unexpected strategy purpose {:?}",
            order.client_order_id,
            purpose
        );
        ensure!(
            order.parsed_side() == Some(OrderSide::Sell),
            "live OKX stop-loss algo {} has unexpected side {}",
            order.client_order_id,
            order.side
        );
        ensure!(
            order.is_trigger_market_order(),
            "live OKX stop-loss algo {} has unexpected type {} and orderPx {}",
            order.client_order_id,
            order.order_type,
            order.order_price
        );
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct StrategyFillEvidence {
    purpose: OrderPurpose,
    side: OrderSide,
    accounting: Option<OkxSpotFillAccounting>,
}

fn strategy_order_evidence_from_orders(orders: &[OkxOrder], strategy_tag: &str) -> Vec<OkxOrder> {
    let mut evidence = Vec::new();
    let mut seen_client_order_ids = HashSet::new();
    for order in orders {
        append_strategy_order_evidence(
            &mut evidence,
            &mut seen_client_order_ids,
            order.clone(),
            strategy_tag,
        );
    }
    evidence
}

fn strategy_order_evidence_ids(orders: &[OkxOrder]) -> HashSet<String> {
    orders
        .iter()
        .map(|order| order.client_order_id.clone())
        .collect()
}

fn append_strategy_order_evidence(
    orders: &mut Vec<OkxOrder>,
    seen_client_order_ids: &mut HashSet<String>,
    order: OkxOrder,
    strategy_tag: &str,
) {
    if parse_strategy_client_order_id(&order.client_order_id, strategy_tag).is_none() {
        return;
    }
    if seen_client_order_ids.insert(order.client_order_id.clone()) {
        orders.push(order);
    }
}

fn strategy_fill_evidence<'a>(
    orders: impl IntoIterator<Item = &'a OkxOrder>,
    fills: &[OkxFill],
    strategy_tag: &str,
    instrument: &OkxInstrument,
) -> Result<Vec<StrategyFillEvidence>> {
    let mut evidence = Vec::new();
    let mut seen_fill_ids = HashSet::new();
    let mut fill_client_order_ids = HashSet::new();

    for fill in fills {
        let Some(purpose) = parse_strategy_client_order_id(&fill.client_order_id, strategy_tag)
        else {
            continue;
        };
        let fill_id = fill.dedupe_key();
        ensure!(
            !fill_id.is_empty(),
            "strategy-tagged OKX fill for {} has no stable identity",
            fill.client_order_id
        );
        if !seen_fill_ids.insert(fill_id) {
            continue;
        }
        let Some(side) = fill.parsed_side() else {
            bail!(
                "strategy-tagged OKX fill {} has unexpected side {} for purpose {:?}",
                fill.dedupe_key(),
                fill.side,
                purpose
            );
        };
        ensure_expected_fill_side(purpose, side, &fill.client_order_id)?;
        fill_client_order_ids.insert(fill.client_order_id.clone());
        evidence.push(StrategyFillEvidence {
            purpose,
            side,
            accounting: Some(fill.spot_accounting(&instrument.base_ccy, &instrument.quote_ccy)?),
        });
    }

    for order in orders {
        let Some(purpose) = parse_strategy_client_order_id(&order.client_order_id, strategy_tag)
        else {
            continue;
        };
        if fill_client_order_ids.contains(&order.client_order_id) {
            continue;
        }
        let fill_size = order.fill_size()?;
        if fill_size <= Decimal::ZERO {
            continue;
        }
        let Some(side) = order.parsed_side() else {
            bail!(
                "strategy-tagged OKX order {} has unexpected side {} for purpose {:?}",
                order.client_order_id,
                order.side,
                purpose
            );
        };
        ensure_expected_fill_side(purpose, side, &order.client_order_id)?;
        let accounting = if order.average_fill_price()?.is_some() {
            Some(order.cumulative_spot_accounting(&instrument.base_ccy, &instrument.quote_ccy)?)
        } else {
            None
        };
        evidence.push(StrategyFillEvidence {
            purpose,
            side,
            accounting,
        });
    }

    Ok(evidence)
}

fn ensure_expected_fill_side(
    purpose: OrderPurpose,
    side: OrderSide,
    client_order_id: &str,
) -> Result<()> {
    let expected_side = match purpose {
        OrderPurpose::Entry => OrderSide::Buy,
        OrderPurpose::TakeProfit | OrderPurpose::StopLoss => OrderSide::Sell,
    };
    ensure!(
        side == expected_side,
        "strategy-tagged OKX order {client_order_id} has unexpected side {:?} for purpose {:?}",
        side,
        purpose
    );
    Ok(())
}

fn strategy_entry_average_price(
    evidence: &[StrategyFillEvidence],
) -> Result<EntryAveragePriceStatus> {
    let mut quantity = Decimal::ZERO;
    let mut quote_cost = Decimal::ZERO;
    for fill in evidence
        .iter()
        .filter(|fill| fill.purpose == OrderPurpose::Entry && fill.side == OrderSide::Buy)
    {
        let Some(accounting) = fill.accounting else {
            return Ok(EntryAveragePriceStatus::MissingPrice);
        };
        quantity += accounting.base_change;
        quote_cost -= accounting.quote_change;
    }
    if quantity <= Decimal::ZERO {
        return Ok(EntryAveragePriceStatus::NoEntryFill);
    }
    ensure!(
        quote_cost > Decimal::ZERO,
        "OKX strategy entry quote cost must be positive"
    );
    let average_price = quote_cost / quantity;
    ensure!(
        average_price > Decimal::ZERO,
        "OKX strategy entry average price must be positive"
    );
    Ok(EntryAveragePriceStatus::Reconstructed(average_price))
}

fn strategy_net_filled_position(evidence: &[StrategyFillEvidence]) -> Result<Decimal> {
    let mut quantity = Decimal::ZERO;
    for fill in evidence {
        let accounting = fill
            .accounting
            .context("strategy-tagged OKX fill is missing fee-adjusted accounting")?;
        match (fill.purpose, fill.side) {
            (OrderPurpose::Entry, OrderSide::Buy) => quantity += accounting.base_change,
            (OrderPurpose::TakeProfit | OrderPurpose::StopLoss, OrderSide::Sell) => {
                quantity += accounting.base_change;
            }
            (OrderPurpose::Entry, OrderSide::Sell)
            | (OrderPurpose::TakeProfit | OrderPurpose::StopLoss, OrderSide::Buy) => {
                bail!(
                    "strategy-tagged OKX fill has unexpected side {:?} for purpose {:?}",
                    fill.side,
                    fill.purpose
                );
            }
        }
    }
    Ok(quantity.max(Decimal::ZERO))
}

async fn cancel_if_live(
    client: &impl OkxClient,
    instrument_id: &str,
    client_order_id: &str,
) -> bool {
    client.record_order_decision(Instant::now());
    if let Err(err) = client.cancel_order(instrument_id, client_order_id).await {
        error!(
            error = %err,
            instrument = %instrument_id,
            client_order_id,
            "failed to cancel OKX order"
        );
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use anyhow::{Context, Result, bail};
    use pretty_assertions::assert_eq;
    use rust_decimal::Decimal;

    use crate::config::{loader::load_config_from_str_with_secret_resolver, types::StrategyKind};
    use crate::okx::client::OkxOrderAmend;

    use super::signal::{SignalState, entry_offset_distance, volatility_clears_fee_threshold};
    use super::{
        ExchangeState, OKX_CLIENT_ORDER_ID_MAX_LEN, ORDER_ID_PREFIX, OkxEmaAtrMakerTrendRunner,
        OpenPosition, OrderPurpose, StopLossPendingReason, TakeProfitOrderShape, TrackedAlgoOrder,
        TrackedOrder, base36, client_order_id, decimal_to_okx, legacy_strategy_tag,
        parse_legacy_strategy_client_order_id, parse_strategy_client_order_id, strategy_tag,
    };
    use crate::okx::{
        client::OkxClient,
        trading_instrument::{ValidatedQuoteUsdRate, ValidatedTradingInstrument},
        types::{
            MarketBar, OkxAlgoOrder, OkxAlgoOrderAck, OkxBalance, OkxBalanceDetail, OkxFill,
            OkxInstrument, OkxOrder, OkxOrderAck, OkxTicker, OkxTradeFeeRate, OrderKind, OrderSide,
            quantize_decimal_down, quantize_decimal_up,
        },
    };

    macro_rules! dec {
        ($value:literal) => {
            $value
                .parse::<Decimal>()
                .expect("decimal literal should parse")
        };
    }

    #[path = "entry_submission_tests.rs"]
    mod entry_submission_tests;

    #[path = "shutdown_tests.rs"]
    mod shutdown_tests;

    #[test]
    fn entry_offset_is_clamped_by_bps_bounds() {
        assert_eq!(
            entry_offset_distance(100.0, 1.0, dec!("0.1"), dec!("20.0"), dec!("30.0")),
            Some(dec!("0.2"))
        );
        assert_eq!(
            entry_offset_distance(100.0, 100.0, dec!("0.1"), dec!("1.0"), dec!("30.0")),
            Some(dec!("0.3"))
        );
    }

    #[test]
    fn numeric_boundary_precision_entry_offset_converts_atr_to_decimal_safely() -> Result<()> {
        assert_eq!(
            entry_offset_distance(100.0, 1.25, dec!("0.2"), dec!("1.0"), dec!("50.0")),
            Some(dec!("0.25"))
        );
        assert_eq!(
            entry_offset_distance(100.0, 0.3, dec!("0.1"), dec!("1.0"), dec!("50.0")),
            Some(dec!("0.03"))
        );
        assert_eq!(
            entry_offset_distance(100.0, 0.0000001, dec!("1.0"), dec!("1.0"), dec!("50.0")),
            Some(dec!("0.01"))
        );
        assert_eq!(
            entry_offset_distance(100.0, 0.001, dec!("1.0"), dec!("10.0"), dec!("100.0")),
            Some(dec!("0.1"))
        );
        assert_eq!(
            entry_offset_distance(100.0, 100.0, dec!("1.0"), dec!("1.0"), dec!("25.0")),
            Some(dec!("0.25"))
        );

        let small_offset_price = dec!("100.005")
            - entry_offset_distance(100.0, 0.0000001, dec!("1.0"), dec!("1.0"), dec!("50.0"))
                .expect("valid tiny ATR should clamp to the minimum Decimal offset");
        let quantized = quantize_decimal_down(small_offset_price, dec!("0.01"))?;

        assert_eq!(small_offset_price, dec!("99.995"));
        assert_eq!(quantized, dec!("99.99"));
        assert_eq!(decimal_to_okx(quantized), "99.99");
        Ok(())
    }

    #[test]
    fn numeric_boundary_precision_rejects_invalid_entry_offset_inputs() {
        let invalid_values = [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -1.0,
            0.0,
            f64::from_bits(1),
        ];
        for value in invalid_values {
            assert_eq!(
                entry_offset_distance(value, 1.0, dec!("1.0"), dec!("1.0"), dec!("50.0")),
                None,
                "invalid close {value:?} must not produce an order offset"
            );
            assert_eq!(
                entry_offset_distance(100.0, value, dec!("1.0"), dec!("1.0"), dec!("50.0")),
                None,
                "invalid ATR {value:?} must not produce an order offset"
            );
            assert_eq!(
                volatility_clears_fee_threshold(100.0, value, dec!("0.002")),
                false,
                "invalid ATR {value:?} must not clear fee threshold"
            );
        }
    }

    #[test]
    fn volatility_fee_threshold_uses_exact_decimal_comparison() {
        assert_eq!(
            volatility_clears_fee_threshold(100.0, 0.5, dec!("0.002")),
            true
        );
        assert_eq!(
            volatility_clears_fee_threshold(100.0, 0.4999, dec!("0.002")),
            false
        );
        assert_eq!(
            volatility_clears_fee_threshold(100.0, -0.5, dec!("0.002")),
            false
        );
    }

    #[test]
    fn entry_order_age_uses_inclusive_bounded_deadline() -> Result<()> {
        assert_eq!(super::entry_order_is_expired(1_000, 15_999, 15_000)?, false);
        assert_eq!(super::entry_order_is_expired(1_000, 16_000, 15_000)?, true);

        let error = super::entry_order_is_expired(2_000, 1_999, 15_000)
            .expect_err("future exchange timestamp should fail closed");
        assert!(
            error.to_string().contains("later than local time"),
            "clock ambiguity should be explicit: {error}"
        );
        Ok(())
    }

    #[test]
    fn signal_state_becomes_ready_after_warmup() {
        let mut state = SignalState::new(2, 3, 3, dec!("0.1"), dec!("1.0"), dec!("15.0"));
        state
            .set_round_trip_cost_rate(dec!("0.003"))
            .expect("test fee schedule should be valid");
        let bars = [
            bar(1, 100.0, 101.0, 99.0, 100.0),
            bar(2, 101.0, 103.0, 100.0, 102.0),
            bar(3, 102.0, 106.0, 101.0, 105.0),
            bar(4, 105.0, 111.0, 104.0, 110.0),
        ];

        for bar in &bars {
            state.update_from_bar(bar);
        }

        assert_eq!(state.ready(), true);
        assert_eq!(state.entry_allowed(), true);
        assert_eq!(
            state
                .entry_price_from_bid(Decimal::from(110))
                .expect("entry price should be representable")
                .is_some(),
            true
        );
    }

    #[test]
    fn signal_state_with_invalid_internal_periods_stays_unready() {
        let mut state = SignalState::new(0, 0, 0, dec!("0.1"), dec!("1.0"), dec!("15.0"));
        let bars = [
            bar(1, 100.0, 101.0, 99.0, 100.0),
            bar(2, 101.0, 103.0, 100.0, 102.0),
            bar(3, 102.0, 106.0, 101.0, 105.0),
        ];

        for bar in &bars {
            assert_eq!(state.update_from_bar(bar), false);
        }

        assert_eq!(state.ready(), false);
        assert_eq!(state.entry_allowed(), false);
        assert_eq!(state.last_atr(), None);
    }

    #[tokio::test]
    async fn checked_in_one_minute_demo_params_reject_low_atr_entry() -> Result<()> {
        let mut runner = checked_in_demo_runner()?;
        let client = MockOkxClient {
            candles: vec![
                bar(1, 100.0, 100.05, 99.95, 100.0),
                bar(2, 100.0, 100.07, 99.97, 100.02),
                bar(3, 100.02, 100.09, 99.99, 100.04),
                bar(4, 100.04, 100.11, 100.01, 100.06),
                bar(5, 100.06, 100.13, 100.03, 100.08),
            ],
            ticker: ticker_with_bid_last("100.1", "100.1"),
            balances: vec![balance("BTC", "1")],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(runner.signal.ready(), true);
        assert_eq!(
            runner
                .signal
                .last_atr()
                .is_some_and(|atr| atr < 100.08 * 0.005),
            true,
            "low-ATR 1m fixture should stay below the configured fee threshold"
        );
        assert_eq!(runner.signal.entry_allowed(), false);

        runner.evaluate_entry(&client).await?;

        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn checked_in_one_minute_demo_params_place_fee_clearing_entry_and_take_profit()
    -> Result<()> {
        let mut runner = checked_in_demo_runner()?;
        let client = MockOkxClient {
            candles: vec![
                bar(1, 100.0, 100.5, 99.5, 100.0),
                bar(2, 100.0, 100.8, 99.8, 100.2),
                bar(3, 100.2, 101.0, 100.0, 100.4),
                bar(4, 100.4, 101.2, 100.2, 100.6),
                bar(5, 100.6, 101.4, 100.4, 100.8),
            ],
            ticker: ticker_with_bid_last("100.9", "100.9"),
            balances: vec![balance("BTC", "1")],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(runner.signal.ready(), true);
        assert_eq!(runner.signal.entry_allowed(), true);
        let tick_size = runner.exchange()?.instrument.tick_size()?;
        let lot_size = runner.exchange()?.instrument.lot_size()?;
        let entry_price = quantize_decimal_down(
            runner
                .signal
                .entry_price_from_bid(dec!("100.9"))?
                .context("checked-in 1m economics fixture should produce entry price")?,
            tick_size,
        )?;
        let entry_size = quantize_decimal_down(runner.entry_quantity(entry_price)?, lot_size)?;

        assert_eq!(entry_price, dec!("100.8"));
        assert_eq!(entry_size, dec!("0.001"));
        assert_eq!(quantize_decimal_down(entry_price, tick_size)?, entry_price);
        assert_eq!(quantize_decimal_down(entry_size, lot_size)?, entry_size);
        assert!(
            entry_size * entry_price <= dec!("500"),
            "checked-in max_quote_notional must hold after final tick/lot rounding"
        );

        runner.evaluate_entry(&client).await?;

        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                size: "0.001".to_owned(),
                price: Some("100.8".to_owned()),
                purpose: Some(OrderPurpose::Entry),
            }]
        );

        let entry_client_order_id = runner
            .exchange()?
            .entry_order
            .as_ref()
            .context("entry should be tracked after placement")?
            .client_order_id
            .clone();
        let fill_client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &entry_client_order_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100.8",
                updated_at_ms: "6",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_entry_order(&fill_client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("100.8"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100.8"))?,
            })
        );
        let take_profit_price =
            quantize_decimal_up(runner.take_profit_price(dec!("100.8"))?, tick_size)?;
        assert_eq!(take_profit_price, dec!("102.3"));
        assert_eq!(
            quantize_decimal_up(take_profit_price, tick_size)?,
            take_profit_price
        );
        assert!(
            take_profit_price * (Decimal::ONE - dec!("0.002")) >= dec!("100.8"),
            "final rounded take-profit must recover fee-adjusted cost after the exit fee"
        );
        assert_eq!(
            fill_client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                size: "0.001".to_owned(),
                price: Some("102.3".to_owned()),
                purpose: Some(OrderPurpose::TakeProfit),
            }]
        );
        assert_eq!(
            fill_client.placed_algo_orders(),
            vec![PlacedAlgoOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                size: "0.001".to_owned(),
                trigger_price: "99.8".to_owned(),
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        Ok(())
    }

    #[test]
    fn numeric_boundary_precision_protection_prices_quantize_and_serialize_exactly() -> Result<()> {
        let mut runner = runner_with_empty_exchange("okx-ema-atr-maker-btc-usdt");
        runner.exchange.as_mut().expect("exchange state").instrument = precision_instrument();

        runner.signal.last_atr = Some(0.3);
        let take_profit_raw = runner.take_profit_price(dec!("100.005"))?;
        let stop_loss_raw = runner.stop_loss_trigger(dec!("100.005"))?;
        let take_profit = quantize_decimal_up(take_profit_raw, dec!("0.01"))?;
        let stop_loss = quantize_decimal_down(stop_loss_raw, dec!("0.01"))?;

        assert_eq!(take_profit_raw, dec!("100.455"));
        assert_eq!(take_profit, dec!("100.46"));
        assert_eq!(decimal_to_okx(take_profit), "100.46");
        assert_eq!(stop_loss_raw, dec!("99.705"));
        assert_eq!(stop_loss, dec!("99.7"));
        assert_eq!(decimal_to_okx(stop_loss), "99.7");

        runner.signal.last_atr = Some(0.4);
        let tick_boundary_take_profit =
            quantize_decimal_up(runner.take_profit_price(dec!("100"))?, dec!("0.01"))?;
        let tick_boundary_stop_loss =
            quantize_decimal_down(runner.stop_loss_trigger(dec!("100"))?, dec!("0.01"))?;

        assert_eq!(tick_boundary_take_profit, dec!("100.6"));
        assert_eq!(decimal_to_okx(tick_boundary_take_profit), "100.6");
        assert_eq!(tick_boundary_stop_loss, dec!("99.6"));
        assert_eq!(decimal_to_okx(tick_boundary_stop_loss), "99.6");
        Ok(())
    }

    #[test]
    fn take_profit_shape_uses_rounded_price_for_fee_coverage() -> Result<()> {
        let mut runner = runner_with_empty_exchange("okx-ema-atr-maker-btc-usdt");

        runner.signal.last_atr = Some(0.2);
        let target = runner.take_profit_order_shape(dec!("0.001"), dec!("100"))?;

        assert_eq!(
            target,
            TakeProfitOrderShape {
                size: dec!("0.001"),
                price: dec!("100.3"),
            }
        );

        runner.signal.last_atr = Some(0.1);
        assert_eq!(
            runner.take_profit_order_shape(dec!("0.001"), dec!("100"))?,
            TakeProfitOrderShape {
                size: dec!("0.001"),
                price: dec!("100.3"),
            }
        );
        Ok(())
    }

    #[test]
    fn numeric_boundary_precision_rejects_invalid_protection_atr_values() {
        for value in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.3,
            0.0,
            f64::from_bits(1),
        ] {
            let mut runner = test_runner("okx-ema-atr-maker-btc-usdt");
            runner.signal.last_atr = Some(value);

            let take_profit_error = runner
                .take_profit_price(dec!("100"))
                .expect_err("invalid ATR must not produce take-profit price");
            let stop_loss_error = runner
                .stop_loss_trigger(dec!("100"))
                .expect_err("invalid ATR must not produce stop-loss trigger");

            assert!(
                take_profit_error.to_string().contains("finite, positive"),
                "invalid take-profit ATR should report Decimal boundary failure: {take_profit_error}"
            );
            assert!(
                stop_loss_error.to_string().contains("finite, positive"),
                "invalid stop-loss ATR should report Decimal boundary failure: {stop_loss_error}"
            );
        }
    }

    #[test]
    fn strategy_order_ids_are_tagged_unique_and_parseable() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let tag = strategy_tag(strategy_id);
        let legacy_tag = legacy_strategy_tag(strategy_id);
        let legacy_id = format!("{ORDER_ID_PREFIX}{legacy_tag}B00000001");
        let first = client_order_id(strategy_id, OrderPurpose::Entry);
        let second = client_order_id(strategy_id, OrderPurpose::Entry);

        assert_ne!(first, second);
        assert_eq!(tag.len(), 11);
        assert!(tag.starts_with("OKXE"));
        assert!(first.len() <= OKX_CLIENT_ORDER_ID_MAX_LEN);
        assert!(first.chars().all(|ch| ch.is_ascii_alphanumeric()));
        assert!(first.starts_with(&format!("{ORDER_ID_PREFIX}{tag}B")));
        assert_eq!(
            parse_strategy_client_order_id(&first, &tag),
            Some(OrderPurpose::Entry)
        );
        assert_eq!(parse_strategy_client_order_id(&legacy_id, &tag), None);
        assert_eq!(
            parse_legacy_strategy_client_order_id(&legacy_id, &legacy_tag),
            Some(OrderPurpose::Entry)
        );
    }

    #[test]
    fn strategy_tags_include_full_id_hash_to_avoid_legacy_prefix_collision() {
        let btc_strategy_id = "okx-ema-atr-maker-btc-usdt";
        let eth_strategy_id = "okx-ema-atr-maker-eth-usdt";
        let btc_tag = strategy_tag(btc_strategy_id);
        let eth_tag = strategy_tag(eth_strategy_id);
        let btc_order_id = entry_id(btc_strategy_id);

        assert_ne!(btc_tag, eth_tag);
        assert_eq!(
            parse_strategy_client_order_id(&btc_order_id, &btc_tag),
            Some(OrderPurpose::Entry)
        );
        assert_eq!(
            parse_strategy_client_order_id(&btc_order_id, &eth_tag),
            None
        );
    }

    #[test]
    fn strategy_order_id_parser_rejects_malformed_or_ambiguous_ids() {
        let tag = strategy_tag("okx-ema-atr-maker-btc-usdt");

        for malformed in [
            format!("{ORDER_ID_PREFIX}{tag}"),
            format!("{ORDER_ID_PREFIX}{tag}X00000001"),
            format!("{ORDER_ID_PREFIX}{tag}B"),
            format!("{ORDER_ID_PREFIX}{tag}B0000"),
            format!("{ORDER_ID_PREFIX}{tag}B00000001!"),
            format!("{ORDER_ID_PREFIX}{tag}B123456789012345678"),
        ] {
            assert_eq!(
                parse_strategy_client_order_id(&malformed, &tag),
                None,
                "{malformed} should not parse as strategy-owned"
            );
        }
        assert_eq!(
            parse_strategy_client_order_id("ROXSHORTTAGB00000001", "SHORTTAG"),
            None
        );
    }

    #[test]
    fn strategy_order_id_format_stays_within_okx_limit_at_u64_time_boundary() {
        let tag = strategy_tag("okx-ema-atr-maker-btc-usdt-with-a-very-long-id");
        let worst_case = format!(
            "{ORDER_ID_PREFIX}{tag}{}{time}{sequence:04}",
            OrderPurpose::StopLoss.as_code(),
            time = base36(u64::MAX),
            sequence = 9_999,
        );

        assert_eq!(tag.len(), 11);
        assert!(worst_case.len() <= OKX_CLIENT_ORDER_ID_MAX_LEN);
        assert_eq!(
            parse_strategy_client_order_id(&worst_case, &tag),
            Some(OrderPurpose::StopLoss)
        );
    }

    #[tokio::test]
    async fn initialize_reconstructs_base_balance_and_submits_take_profit() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let tag = strategy_tag(strategy_id);
        let entry_id = entry_id(strategy_id);
        let client = MockOkxClient {
            balances: vec![OkxBalance {
                details: vec![OkxBalanceDetail {
                    ccy: "BTC".to_owned(),
                    available_balance: "0.001".to_owned(),
                    cash_balance: "0.001".to_owned(),
                    frozen_balance: "0".to_owned(),
                }],
            }],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        let position = runner.exchange()?.position;
        assert_eq!(
            position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("100"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
            })
        );
        let placed_orders = client.placed_orders();
        let placed_algo_orders = client.placed_algo_orders();
        let expected_price = decimal_to_okx(quantize_decimal_up(
            runner.take_profit_price(dec!("100"))?,
            instrument().tick_size()?,
        )?);
        let expected_trigger_price = decimal_to_okx(quantize_decimal_down(
            runner.stop_loss_trigger(dec!("100"))?,
            instrument().tick_size()?,
        )?);
        assert_eq!(
            placed_orders,
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                size: "0.001".to_owned(),
                price: Some(expected_price),
                purpose: Some(OrderPurpose::TakeProfit),
            }]
        );
        assert_eq!(
            placed_algo_orders,
            vec![PlacedAlgoOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                size: "0.001".to_owned(),
                trigger_price: expected_trigger_price,
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        assert_eq!(
            parse_strategy_client_order_id(
                &runner
                    .exchange()?
                    .take_profit_order
                    .as_ref()
                    .expect("take-profit order should be tracked")
                    .client_order_id,
                &tag
            ),
            Some(OrderPurpose::TakeProfit)
        );
        assert_eq!(
            parse_strategy_client_order_id(
                &runner
                    .exchange()?
                    .stop_loss_order
                    .as_ref()
                    .expect("stop-loss algo should be tracked")
                    .client_order_id,
                &tag
            ),
            Some(OrderPurpose::StopLoss)
        );
        assert_eq!(client.broad_history_call_counts(), (1, 1));
        Ok(())
    }

    #[tokio::test]
    async fn initialize_reconstructs_open_entry_fill_without_broad_history() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.0005")],
            open_orders: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "partially_filled",
                size: "0.001",
                accumulated_fill_size: "0.0005",
                average_price: "100",
                updated_at_ms: "5",
            })],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.0005"),
                average_price: dec!("100"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
            })
        );
        assert_eq!(
            runner
                .exchange()?
                .entry_order
                .as_ref()
                .map(|order| order.last_fill_size),
            Some(dec!("0.0005"))
        );
        assert_eq!(client.broad_history_call_counts(), (0, 0));
        assert_eq!(client.order_lookup_client_order_ids(), Vec::<String>::new());
        Ok(())
    }

    #[tokio::test]
    async fn numeric_boundary_precision_position_orders_use_quantized_decimal_strings() -> Result<()>
    {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let entry_id = entry_id(strategy_id);
        let client = MockOkxClient {
            instrument: precision_instrument(),
            candles: numeric_boundary_precision_bars(),
            balances: vec![balance("BTC", "0.001")],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100.015",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("100.015"),
                stop_loss_trigger: dec!("99.015"),
            })
        );
        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                size: "0.001".to_owned(),
                price: Some("101.52".to_owned()),
                purpose: Some(OrderPurpose::TakeProfit),
            }]
        );
        assert_eq!(
            client.placed_algo_orders(),
            vec![PlacedAlgoOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                size: "0.001".to_owned(),
                trigger_price: "99.01".to_owned(),
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn initialize_fails_with_insufficient_confirmed_warmup_bars() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let mut unconfirmed_bar = bar(3, 102.0, 106.0, 101.0, 105.0);
        unconfirmed_bar.confirm = false;
        let client = MockOkxClient {
            candles: vec![
                bar(1, 100.0, 101.0, 99.0, 100.0),
                bar(2, 101.0, 103.0, 100.0, 102.0),
                unconfirmed_bar,
            ],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("insufficient warmup should fail closed");

        assert!(err.to_string().contains("requires initialized EMA/ATR"));
        assert!(err.to_string().contains("only 2 confirmed warmup bars"));
        assert!(runner.exchange.is_none());
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        assert_eq!(
            client.calls(),
            vec![MockOkxCall::Instruments, MockOkxCall::Candles]
        );
    }

    #[tokio::test]
    async fn initialize_reconstructs_before_ensuring_protection() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id(strategy_id),
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        let calls = client.calls();
        assert_eq!(
            calls,
            vec![
                MockOkxCall::Instruments,
                MockOkxCall::Candles,
                MockOkxCall::OpenOrders,
                MockOkxCall::OpenAlgoOrders,
                MockOkxCall::Balances,
                MockOkxCall::OrderHistory,
                MockOkxCall::OrderFills,
                MockOkxCall::QuoteUsdRate,
                MockOkxCall::PlaceOrder(Some(OrderPurpose::TakeProfit)),
                MockOkxCall::PlaceTriggerOrder(Some(OrderPurpose::StopLoss)),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn tick_refreshes_tracked_orders_before_entry_evaluation() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_empty_exchange(strategy_id);
        seed_signal(&mut runner);
        runner.exchange_mut()?.take_profit_order = Some(TrackedOrder {
            client_order_id: take_profit_id(strategy_id),
            last_fill_size: Decimal::ZERO,
            last_average_fill_price: None,
            last_accounted_base_change: Decimal::ZERO,
            last_accounted_quote_change: Decimal::ZERO,
            cancel_requested: false,
        });
        let client = MockOkxClient::default();

        runner.tick(&client).await?;

        let calls = client.calls();
        assert_eq!(
            calls,
            vec![
                MockOkxCall::LiveCandles,
                MockOkxCall::OrderLookup(Some(OrderPurpose::TakeProfit)),
                MockOkxCall::Ticker,
                MockOkxCall::QuoteUsdRate,
                MockOkxCall::PlaceOrder(Some(OrderPurpose::Entry)),
            ]
        );
        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                size: "0.001".to_owned(),
                price: Some("109.8".to_owned()),
                purpose: Some(OrderPurpose::Entry),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn trading_safety_matrix_rejects_legacy_live_entry_order_ids() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let legacy_entry_id = legacy_entry_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &legacy_entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "live",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "5",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("legacy live entry order should fail closed");

        assert!(
            err.to_string().contains("legacy strategy order id"),
            "legacy live order should report explicit legacy ownership failure: {err}"
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn trading_safety_matrix_rejects_legacy_live_stop_loss_algo_ids() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut legacy_stop_loss = stop_loss_algo(strategy_id, "live");
        legacy_stop_loss.client_order_id = legacy_stop_loss_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            open_algo_orders: vec![legacy_stop_loss],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("legacy live stop-loss algo should fail closed");

        assert!(
            err.to_string().contains("legacy strategy algo id"),
            "legacy live algo should report explicit legacy ownership failure: {err}"
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_on_duplicate_live_strategy_orders_without_broad_history() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let duplicate_entry_id = format!("ROX{}B00000002", strategy_tag(strategy_id));
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            open_orders: vec![
                order(OrderFixture {
                    client_order_id: &entry_id,
                    side: OrderSide::Buy,
                    kind: OrderKind::PostOnly,
                    state: "live",
                    size: "0.001",
                    accumulated_fill_size: "0",
                    average_price: "",
                    updated_at_ms: "5",
                }),
                order(OrderFixture {
                    client_order_id: &duplicate_entry_id,
                    side: OrderSide::Buy,
                    kind: OrderKind::PostOnly,
                    state: "live",
                    size: "0.001",
                    accumulated_fill_size: "0",
                    average_price: "",
                    updated_at_ms: "6",
                }),
            ],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("duplicate live strategy orders should fail closed");

        assert!(
            err.to_string()
                .contains("multiple live OKX strategy orders"),
            "duplicate live orders should report explicit ownership failure: {err}"
        );
        assert_eq!(client.broad_history_call_counts(), (0, 0));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_on_unknown_live_strategy_order_without_broad_history() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let unknown_id = format!("ROX{}X00000001", strategy_tag(strategy_id));
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &unknown_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "live",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "5",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("unknown live strategy order should fail closed");

        assert!(
            err.to_string()
                .contains("unknown live OKX strategy order id"),
            "unknown live order should report explicit ownership failure: {err}"
        );
        assert_eq!(client.broad_history_call_counts(), (0, 0));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_does_not_treat_sibling_current_tag_as_legacy() -> Result<()> {
        let strategy_id = "ABCD0";
        let sibling_strategy_id = "ABCD-X43";
        let sibling_entry_id = entry_id(sibling_strategy_id);
        let runner_tags = vec![strategy_tag(strategy_id), strategy_tag(sibling_strategy_id)];
        let mut runner = test_runner_with_configured_tags(strategy_id, runner_tags);
        let legacy_prefix = format!("{ORDER_ID_PREFIX}{}", legacy_strategy_tag(strategy_id));
        assert!(sibling_entry_id.starts_with(&format!("{legacy_prefix}S")));
        assert_eq!(
            parse_legacy_strategy_client_order_id(
                &sibling_entry_id,
                &legacy_strategy_tag(strategy_id)
            ),
            Some(OrderPurpose::StopLoss)
        );
        assert_eq!(
            parse_strategy_client_order_id(&sibling_entry_id, &strategy_tag(sibling_strategy_id)),
            Some(OrderPurpose::Entry)
        );
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &sibling_entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "live",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "5",
            })],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn initialize_applies_confirmed_warmup_bars_chronologically() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let mut current_bar = bar(5, 111.0, 113.0, 110.0, 112.0);
        current_bar.confirm = false;
        let client = MockOkxClient {
            candles: vec![
                current_bar,
                bar(4, 105.0, 111.0, 104.0, 110.0),
                bar(3, 102.0, 106.0, 101.0, 105.0),
                bar(2, 101.0, 103.0, 100.0, 102.0),
                bar(1, 100.0, 101.0, 99.0, 100.0),
            ],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(runner.exchange()?.last_bar_ts_ms, Some(4));
        assert_eq!(runner.signal.ready(), true);
        assert_eq!(runner.signal.last_close, Some(110.0));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        assert_eq!(client.broad_history_call_counts(), (0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn refresh_bars_applies_missed_confirmed_bars_chronologically() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        seed_signal(&mut runner);
        runner.exchange = Some(ExchangeState {
            instrument: instrument(),
            last_bar_ts_ms: Some(4),
            entry_order: None,
            take_profit_order: None,
            stop_loss_order: None,
            stop_loss_exit_order: None,
            position: None,
            stop_loss_pending: None,
        });
        let older_new_bar = bar(5, 110.0, 116.0, 109.0, 115.0);
        let newer_new_bar = bar(6, 115.0, 122.0, 114.0, 120.0);
        let mut current_bar = bar(7, 120.0, 123.0, 119.0, 121.0);
        current_bar.confirm = false;
        let client = MockOkxClient {
            candles: vec![
                current_bar,
                newer_new_bar.clone(),
                older_new_bar.clone(),
                bar(4, 105.0, 111.0, 104.0, 110.0),
            ],
            ..MockOkxClient::default()
        };
        let mut expected_signal = runner.signal.clone();
        expected_signal.update_from_bar(&older_new_bar);
        expected_signal.update_from_bar(&newer_new_bar);

        runner.refresh_bars(&client).await?;

        assert_eq!(runner.exchange()?.last_bar_ts_ms, Some(6));
        assert_eq!(runner.signal.last_close, expected_signal.last_close);
        assert_eq!(runner.signal.last_atr, expected_signal.last_atr);
        assert_eq!(
            runner.signal.current_atr_offset,
            expected_signal.current_atr_offset
        );
        Ok(())
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_position_cost_basis_is_missing() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("startup should fail without strategy entry cost basis");

        assert!(
            err.to_string()
                .contains("cannot reconstruct OKX strategy position cost basis")
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        assert_eq!(client.broad_history_call_counts(), (1, 1));
    }

    #[tokio::test]
    async fn checked_in_demo_operator_balance_starts_with_zero_strategy_inventory() -> Result<()> {
        let mut runner = checked_in_demo_runner()?;
        let client = MockOkxClient {
            candles: vec![
                bar(1, 100.0, 101.0, 99.0, 100.0),
                bar(2, 101.0, 103.0, 100.0, 102.0),
                bar(3, 102.0, 106.0, 101.0, 105.0),
                bar(4, 105.0, 111.0, 104.0, 110.0),
                bar(5, 110.0, 113.0, 109.0, 112.0),
            ],
            balances: vec![balance("BTC", "1.00000001")],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(runner.exchange()?.position, None);
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        assert_eq!(client.broad_history_call_counts(), (0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_account_is_below_operator_baseline() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner_with_operator_baseline(strategy_id, dec!("1"));
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.9999")],
            ..MockOkxClient::default()
        };

        let error = runner
            .initialize(&client)
            .await
            .expect_err("account below protected operator balance must fail closed");

        assert!(
            error
                .to_string()
                .contains("below configured operator-owned base balance")
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        assert_eq!(client.broad_history_call_counts(), (0, 0));
    }

    #[tokio::test]
    async fn initialize_reconstructs_only_tagged_inventory_above_operator_baseline() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = test_runner_with_operator_baseline(strategy_id, dec!("1"));
        let client = MockOkxClient {
            balances: vec![balance("BTC", "1.001")],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("100"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
            })
        );
        assert_eq!(client.placed_orders()[0].size, "0.001");
        assert_eq!(client.placed_algo_orders()[0].size, "0.001");
        Ok(())
    }

    #[tokio::test]
    async fn initialize_tagged_exit_reduces_only_inventory_above_operator_baseline() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = test_runner_with_operator_baseline(strategy_id, dec!("1"));
        let client = MockOkxClient {
            balances: vec![balance("BTC", "1.001")],
            order_history: vec![
                order(OrderFixture {
                    client_order_id: &entry_id,
                    side: OrderSide::Buy,
                    kind: OrderKind::PostOnly,
                    state: "filled",
                    size: "0.002",
                    accumulated_fill_size: "0.002",
                    average_price: "100",
                    updated_at_ms: "4",
                }),
                order(OrderFixture {
                    client_order_id: &take_profit_id,
                    side: OrderSide::Sell,
                    kind: OrderKind::Limit,
                    state: "filled",
                    size: "0.002",
                    accumulated_fill_size: "0.001",
                    average_price: "120",
                    updated_at_ms: "5",
                }),
            ],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("100"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
            })
        );
        assert_eq!(client.placed_orders()[0].size, "0.001");
        assert_eq!(client.placed_algo_orders()[0].size, "0.001");
        Ok(())
    }

    #[tokio::test]
    async fn initialize_deduplicates_partial_fill_history_above_operator_baseline() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let first = fill(FillFixture {
            client_order_id: &entry_id,
            side: OrderSide::Buy,
            fill_size: "0.0004",
            fill_price: "100",
            fill_time_ms: "4",
            bill_id: "bill-entry-1",
        });
        let second = fill(FillFixture {
            client_order_id: &entry_id,
            side: OrderSide::Buy,
            fill_size: "0.0006",
            fill_price: "100",
            fill_time_ms: "5",
            bill_id: "bill-entry-2",
        });
        let mut runner = test_runner_with_operator_baseline(strategy_id, dec!("1"));
        let client = MockOkxClient {
            balances: vec![balance("BTC", "1.001")],
            order_fills: vec![first.clone(), first, second],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("100"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn initialize_fails_when_tagged_inventory_exceeds_account_delta() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = test_runner_with_operator_baseline(strategy_id, dec!("1"));
        let client = MockOkxClient {
            balances: vec![balance("BTC", "1.0005")],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        let error = runner
            .initialize(&client)
            .await
            .expect_err("missing strategy-owned inventory must fail closed");

        assert!(
            error
                .to_string()
                .contains("exceeds reconstructed OKX BTC-USDT strategy balance")
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_propagates_incomplete_history_failure() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner_with_operator_baseline(strategy_id, dec!("1"));
        let client = MockOkxClient {
            balances: vec![balance("BTC", "1.001")],
            fail_order_history: true,
            ..MockOkxClient::default()
        };

        let error = runner
            .initialize(&client)
            .await
            .expect_err("incomplete tagged history must fail closed");

        assert!(error.to_string().contains("refusing partial history"));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        assert_eq!(client.broad_history_call_counts(), (1, 0));
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_balance_cash_balance_is_missing() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let mut btc_balance = balance("BTC", "0.001");
        btc_balance.details[0].cash_balance.clear();
        let client = MockOkxClient {
            balances: vec![btc_balance],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("startup should reject missing OKX cashBal evidence");

        assert!(
            err.to_string()
                .contains("OKX balance cashBal must be provided"),
            "missing cashBal should fail closed before reconstruction: {err}"
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        assert_eq!(client.broad_history_call_counts(), (0, 0));
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_balance_exceeds_strategy_fills() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.002")],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("startup should reject non-strategy balance");

        assert!(
            err.to_string()
                .contains("exceeds strategy-tagged net filled quantity")
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        assert_eq!(client.broad_history_call_counts(), (1, 1));
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_balance_exceeds_strategy_net_fills() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.0011")],
            order_history: vec![
                order(OrderFixture {
                    client_order_id: &entry_id,
                    side: OrderSide::Buy,
                    kind: OrderKind::PostOnly,
                    state: "filled",
                    size: "0.002",
                    accumulated_fill_size: "0.002",
                    average_price: "100",
                    updated_at_ms: "4",
                }),
                order(OrderFixture {
                    client_order_id: &take_profit_id,
                    side: OrderSide::Sell,
                    kind: OrderKind::Limit,
                    state: "filled",
                    size: "0.002",
                    accumulated_fill_size: "0.001",
                    average_price: "120",
                    updated_at_ms: "5",
                }),
            ],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("startup should reject balance above strategy net fills");

        assert!(
            err.to_string()
                .contains("exceeds strategy-tagged net filled quantity")
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_live_take_profit_mismatches_position() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            open_orders: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.002",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "5",
            })],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("mismatched live take-profit should fail closed");

        assert!(
            err.to_string().contains("take-profit order")
                && err.to_string().contains("does not match"),
            "mismatched live take-profit should report final invariant failure: {err}"
        );
        assert_eq!(client.broad_history_call_counts(), (1, 1));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_live_take_profit_has_empty_price() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            open_orders: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "5",
            })],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("empty-px live take-profit should fail closed");

        assert!(
            err.to_string().contains("take-profit order")
                && err.to_string().contains("does not match"),
            "empty-px live take-profit should report final invariant failure: {err}"
        );
        assert_eq!(client.broad_history_call_counts(), (1, 1));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_live_take_profit_has_whitespace_price() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            open_orders: vec![order_with_price(
                OrderFixture {
                    client_order_id: &take_profit_id,
                    side: OrderSide::Sell,
                    kind: OrderKind::Limit,
                    state: "live",
                    size: "0.001",
                    accumulated_fill_size: "0",
                    average_price: "",
                    updated_at_ms: "5",
                },
                "   ",
            )],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("whitespace-px live take-profit should fail closed");

        assert!(
            err.to_string().contains("take-profit order")
                && err.to_string().contains("does not match"),
            "whitespace-px live take-profit should report final invariant failure: {err}"
        );
        assert_eq!(client.broad_history_call_counts(), (1, 1));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_live_take_profit_does_not_clear_exit_fee() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            candles: vec![
                bar(1, 100.0, 100.03, 99.97, 100.0),
                bar(2, 100.0, 100.03, 99.97, 100.0),
                bar(3, 100.0, 100.03, 99.97, 100.0),
                bar(4, 100.0, 100.03, 99.97, 100.0),
            ],
            balances: vec![balance("BTC", "0.001")],
            open_orders: vec![order_with_price(
                OrderFixture {
                    client_order_id: &take_profit_id,
                    side: OrderSide::Sell,
                    kind: OrderKind::Limit,
                    state: "live",
                    size: "0.001",
                    accumulated_fill_size: "0",
                    average_price: "",
                    updated_at_ms: "5",
                },
                "100.1",
            )],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        let error = runner
            .initialize(&client)
            .await
            .expect_err("underpriced live take-profit should fail closed");

        assert!(
            error
                .to_string()
                .contains("does not recover fee-adjusted average cost"),
            "underpriced live take-profit should report fee coverage failure: {error}"
        );
        assert_eq!(client.broad_history_call_counts(), (1, 1));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_live_stop_loss_mismatches_position() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut stop_loss = stop_loss_algo(strategy_id, "live");
        stop_loss.sz = "0.002".to_owned();
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            open_algo_orders: vec![stop_loss],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .initialize(&client)
            .await
            .expect_err("mismatched live stop-loss should fail closed");

        assert!(
            err.to_string().contains("stop-loss algo")
                && err.to_string().contains("does not match"),
            "mismatched live stop-loss should report final invariant failure: {err}"
        );
        assert_eq!(client.broad_history_call_counts(), (1, 1));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_live_stop_loss_has_empty_state() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let client =
            reconstruction_client_with_stop_loss_algo(strategy_id, stop_loss_algo(strategy_id, ""));

        let err = runner
            .initialize(&client)
            .await
            .expect_err("empty-state live stop-loss algo should fail closed");

        assert!(
            err.to_string().contains("unexpected state")
                && err.to_string().contains("explicit live or pause"),
            "empty-state stop-loss algo should report explicit state validation failure: {err}"
        );
        assert_eq!(client.broad_history_call_counts(), (0, 0));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_live_stop_loss_has_whitespace_state() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let client = reconstruction_client_with_stop_loss_algo(
            strategy_id,
            stop_loss_algo(strategy_id, " "),
        );

        let err = runner
            .initialize(&client)
            .await
            .expect_err("whitespace-state live stop-loss algo should fail closed");

        assert!(
            err.to_string().contains("unexpected state")
                && err.to_string().contains("explicit live or pause"),
            "whitespace-state stop-loss algo should report explicit state validation failure: {err}"
        );
        assert_eq!(client.broad_history_call_counts(), (0, 0));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_fails_closed_when_live_stop_loss_has_unknown_state() {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let client = reconstruction_client_with_stop_loss_algo(
            strategy_id,
            stop_loss_algo(strategy_id, "pending"),
        );

        let err = runner
            .initialize(&client)
            .await
            .expect_err("unknown-state live stop-loss algo should fail closed");

        assert!(
            err.to_string().contains("unexpected state")
                && err.to_string().contains("explicit live or pause"),
            "unknown-state stop-loss algo should report explicit state validation failure: {err}"
        );
        assert_eq!(client.broad_history_call_counts(), (0, 0));
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
    }

    #[tokio::test]
    async fn initialize_accepts_live_stop_loss_with_explicit_live_state() -> Result<()> {
        assert_initialize_accepts_stop_loss_algo_state("live").await
    }

    #[tokio::test]
    async fn initialize_accepts_paused_stop_loss_as_live_protection() -> Result<()> {
        assert_initialize_accepts_stop_loss_algo_state("pause").await
    }

    #[tokio::test]
    async fn initialize_reconstructs_balance_after_tagged_take_profit_fill() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            order_history: vec![
                order(OrderFixture {
                    client_order_id: &entry_id,
                    side: OrderSide::Buy,
                    kind: OrderKind::PostOnly,
                    state: "filled",
                    size: "0.002",
                    accumulated_fill_size: "0.002",
                    average_price: "100",
                    updated_at_ms: "4",
                }),
                order(OrderFixture {
                    client_order_id: &take_profit_id,
                    side: OrderSide::Sell,
                    kind: OrderKind::Limit,
                    state: "filled",
                    size: "0.002",
                    accumulated_fill_size: "0.001",
                    average_price: "120",
                    updated_at_ms: "5",
                }),
            ],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("100"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn initialize_reconstructs_balance_from_order_fills_history() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.000999")],
            order_fills: vec![fill_with_fee(
                FillFixture {
                    client_order_id: &entry_id,
                    side: OrderSide::Buy,
                    fill_size: "0.001",
                    fill_price: "100",
                    fill_time_ms: "4",
                    bill_id: "bill-entry",
                },
                "-0.000001",
                "BTC",
            )],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.0009"),
                average_price: dec!("0.1") / dec!("0.000999"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("0.1") / dec!("0.000999"))?,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn initialize_reconstructs_live_stop_loss_market_exit() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let stop_loss_exit_id = stop_loss_exit_id(strategy_id);
        let mut runner = test_runner(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            open_orders: vec![order(OrderFixture {
                client_order_id: &stop_loss_exit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Market,
                state: "live",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "5",
            })],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        };

        runner.initialize(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state
                .stop_loss_exit_order
                .as_ref()
                .map(|order| order.client_order_id.as_str()),
            Some(stop_loss_exit_id.as_str())
        );
        assert_eq!(
            state.stop_loss_pending,
            Some(StopLossPendingReason::ExitReconciliation)
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn live_partial_entry_fill_opens_protected_position_and_cancels_remainder() -> Result<()>
    {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = runner_with_live_entry(strategy_id);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "partially_filled",
                size: "0.001",
                accumulated_fill_size: "0.0005",
                average_price: "100",
                updated_at_ms: "6",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_entry_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state.position,
            Some(OpenPosition {
                quantity: dec!("0.0005"),
                average_price: dec!("100"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
            })
        );
        assert_eq!(
            state.entry_order.as_ref().map(|order| order.last_fill_size),
            Some(dec!("0.0005"))
        );
        assert_eq!(
            state
                .entry_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(true)
        );
        assert_eq!(client.canceled_orders(), vec![entry_id]);
        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                size: "0.0005".to_owned(),
                price: Some(decimal_to_okx(quantize_decimal_up(
                    runner.take_profit_price(dec!("100"))?,
                    instrument().tick_size()?,
                )?)),
                purpose: Some(OrderPurpose::TakeProfit),
            }]
        );
        assert_eq!(
            client.placed_algo_orders(),
            vec![PlacedAlgoOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                size: "0.0005".to_owned(),
                trigger_price: decimal_to_okx(quantize_decimal_down(
                    runner.stop_loss_trigger(dec!("100"))?,
                    instrument().tick_size()?,
                )?),
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn received_currency_entry_fee_reduces_owned_and_protected_base() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = runner_with_live_entry(strategy_id);
        let mut filled_order = order(OrderFixture {
            client_order_id: &entry_id,
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            state: "partially_filled",
            size: "0.001",
            accumulated_fill_size: "0.0005",
            average_price: "100",
            updated_at_ms: "6",
        });
        filled_order.fee = "-0.0000005".to_owned();
        filled_order.fee_currency = "BTC".to_owned();
        let client = MockOkxClient {
            open_orders: vec![filled_order],
            ..MockOkxClient::default()
        };

        runner.refresh_entry_order(&client).await?;

        let effective_cost = dec!("0.05") / dec!("0.0004995");
        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.0004995"),
                average_price: effective_cost,
                stop_loss_trigger: runner.stop_loss_trigger(effective_cost)?,
            })
        );
        assert_eq!(client.placed_orders()[0].size, "0.0004");
        assert_eq!(client.placed_algo_orders()[0].size, "0.0004");
        Ok(())
    }

    #[tokio::test]
    async fn quote_currency_entry_fee_increases_effective_cost_basis() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = runner_with_live_entry(strategy_id);
        let mut filled_order = order(OrderFixture {
            client_order_id: &entry_id,
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            state: "partially_filled",
            size: "0.001",
            accumulated_fill_size: "0.0005",
            average_price: "100",
            updated_at_ms: "6",
        });
        filled_order.fee = "-0.00005".to_owned();
        filled_order.fee_currency = "USDT".to_owned();
        let client = MockOkxClient {
            open_orders: vec![filled_order],
            ..MockOkxClient::default()
        };

        runner.refresh_entry_order(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.0005"),
                average_price: dec!("100.1"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100.1"))?,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn entry_fill_floors_take_profit_at_exact_exit_fee_break_even() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = runner_with_live_entry(strategy_id);
        runner.signal.last_atr = Some(0.1);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "partially_filled",
                size: "0.001",
                accumulated_fill_size: "0.0005",
                average_price: "1000",
                updated_at_ms: "6",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_entry_order(&client).await?;

        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                size: "0.0005".to_owned(),
                price: Some("1002.1".to_owned()),
                purpose: Some(OrderPurpose::TakeProfit),
            }]
        );
        assert_eq!(
            client.placed_algo_orders(),
            vec![PlacedAlgoOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                size: "0.0005".to_owned(),
                trigger_price: decimal_to_okx(quantize_decimal_down(
                    runner.stop_loss_trigger(dec!("1000"))?,
                    instrument().tick_size()?,
                )?),
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.0005"),
                average_price: dec!("1000"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("1000"))?,
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn numeric_boundary_invalid_atr_entry_fill_preserves_tracking_for_reconciliation()
    -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = runner_with_live_entry(strategy_id);
        runner.signal.last_atr = Some(0.0);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "partially_filled",
                size: "0.001",
                accumulated_fill_size: "0.0005",
                average_price: "100",
                updated_at_ms: "6",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .refresh_entry_order(&client)
            .await
            .expect_err("invalid ATR should fail before advancing entry fill tracking");

        assert!(
            err.to_string().contains("finite, positive"),
            "invalid ATR should report the Decimal boundary failure: {err}"
        );
        let state = runner.exchange()?;
        assert_eq!(
            state.entry_order,
            Some(TrackedOrder {
                client_order_id: entry_id,
                last_fill_size: Decimal::ZERO,
                last_average_fill_price: None,
                last_accounted_base_change: Decimal::ZERO,
                last_accounted_quote_change: Decimal::ZERO,
                cancel_requested: false,
            })
        );
        assert_eq!(state.position, None);
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn terminal_entry_after_partial_fill_amends_take_profit_size() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = runner_with_partially_tracked_entry(strategy_id);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &take_profit_id(strategy_id),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.0005",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "6",
            })],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "105",
                updated_at_ms: "7",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_entry_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("105"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("105"))?,
            })
        );
        assert_eq!(state.entry_order, None);
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        assert_eq!(
            client.amended_orders(),
            vec![AmendedOrder {
                inst_id: "BTC-USDT".to_owned(),
                client_order_id: take_profit_id(strategy_id),
                new_size: Some("0.001".to_owned()),
                new_price: Some(decimal_to_okx(quantize_decimal_up(
                    runner.take_profit_price(dec!("105"))?,
                    instrument().tick_size()?,
                )?)),
            }]
        );
        assert_eq!(client.canceled_algo_orders(), vec!["algo-stop".to_owned()]);
        assert_eq!(
            state
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(false)
        );
        assert_eq!(
            state
                .stop_loss_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(true)
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn decreasing_entry_fill_size_fails_closed_without_clearing_tracking() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = runner_with_partially_tracked_entry(strategy_id);
        let client = MockOkxClient {
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.0004",
                average_price: "100",
                updated_at_ms: "7",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .refresh_entry_order(&client)
            .await
            .expect_err("decreasing cumulative fill should fail closed");

        assert!(
            err.to_string().contains("cumulative fill size decreased"),
            "inconsistent entry fill should report monotonicity failure: {err}"
        );
        let state = runner.exchange()?;
        assert_eq!(
            state.entry_order,
            Some(TrackedOrder {
                client_order_id: entry_id,
                last_fill_size: dec!("0.0005"),
                last_average_fill_price: Some(dec!("100")),
                last_accounted_base_change: dec!("0.0005"),
                last_accounted_quote_change: dec!("-0.05"),
                cancel_requested: true,
            })
        );
        assert_eq!(
            state.position,
            Some(OpenPosition {
                quantity: dec!("0.0005"),
                average_price: dec!("100"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
            })
        );
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        assert_eq!(client.canceled_algo_orders(), Vec::<String>::new());
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn canceled_take_profit_is_replaced_after_okx_confirms_closure() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_resized_position_canceling_take_profit(strategy_id);
        let client = MockOkxClient {
            order_history: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "canceled",
                size: "0.0005",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "8",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_take_profit_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(false)
        );
        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                size: "0.001".to_owned(),
                price: Some(decimal_to_okx(quantize_decimal_up(
                    runner.take_profit_price(dec!("105"))?,
                    instrument().tick_size()?,
                )?)),
                purpose: Some(OrderPurpose::TakeProfit),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_pending_take_profit_is_not_replaced_while_still_live() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_resized_position_canceling_take_profit(strategy_id);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.0005",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "8",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_take_profit_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(true)
        );
        assert_eq!(
            state
                .take_profit_order
                .as_ref()
                .map(|order| order.last_fill_size),
            Some(Decimal::ZERO)
        );
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn live_stale_take_profit_is_amended_without_cancel() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.0005",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "8",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_take_profit_order(&client).await?;

        assert_eq!(
            runner
                .exchange()?
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(false)
        );
        assert_eq!(
            client.amended_orders(),
            vec![AmendedOrder {
                inst_id: "BTC-USDT".to_owned(),
                client_order_id: take_profit_id,
                new_size: Some("0.001".to_owned()),
                new_price: Some(decimal_to_okx(quantize_decimal_up(
                    runner.take_profit_price(dec!("110"))?,
                    instrument().tick_size()?,
                )?)),
            }]
        );
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn live_take_profit_with_empty_price_is_amended_without_cancel() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "8",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_take_profit_order(&client).await?;

        assert_eq!(
            runner
                .exchange()?
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(false)
        );
        assert_eq!(
            client.amended_orders(),
            vec![AmendedOrder {
                inst_id: "BTC-USDT".to_owned(),
                client_order_id: take_profit_id,
                new_size: Some("0.001".to_owned()),
                new_price: Some(decimal_to_okx(quantize_decimal_up(
                    runner.take_profit_price(dec!("110"))?,
                    instrument().tick_size()?,
                )?)),
            }]
        );
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn live_take_profit_with_whitespace_price_is_amended_without_cancel() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let client = MockOkxClient {
            open_orders: vec![order_with_price(
                OrderFixture {
                    client_order_id: &take_profit_id,
                    side: OrderSide::Sell,
                    kind: OrderKind::Limit,
                    state: "live",
                    size: "0.001",
                    accumulated_fill_size: "0",
                    average_price: "",
                    updated_at_ms: "8",
                },
                "   ",
            )],
            ..MockOkxClient::default()
        };

        runner.refresh_take_profit_order(&client).await?;

        assert_eq!(
            runner
                .exchange()?
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(false)
        );
        assert_eq!(
            client.amended_orders(),
            vec![AmendedOrder {
                inst_id: "BTC-USDT".to_owned(),
                client_order_id: take_profit_id,
                new_size: Some("0.001".to_owned()),
                new_price: Some(decimal_to_okx(quantize_decimal_up(
                    runner.take_profit_price(dec!("110"))?,
                    instrument().tick_size()?,
                )?)),
            }]
        );
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn oversized_stale_take_profit_amend_fails_before_submission() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        runner.exchange_mut()?.instrument.max_limit_size = "0.0008".to_owned();
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.0005",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "8",
            })],
            ..MockOkxClient::default()
        };

        let error = runner
            .refresh_take_profit_order(&client)
            .await
            .expect_err("oversized take-profit amend should fail before submission");

        assert!(
            error.to_string().contains("maxLmtSz"),
            "oversized take-profit amend should report the OKX limit size bound: {error}"
        );
        assert_eq!(client.amended_orders(), Vec::<AmendedOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn over_amount_stale_take_profit_amend_fails_before_submission() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        runner.exchange_mut()?.instrument.max_limit_amount = "0.1".to_owned();
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.0005",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "8",
            })],
            ..MockOkxClient::default()
        };

        let error = runner
            .refresh_take_profit_order(&client)
            .await
            .expect_err("over-amount take-profit amend should fail before submission");

        assert!(
            error.to_string().contains("maxLmtAmt"),
            "over-amount take-profit amend should report the OKX limit amount bound: {error}"
        );
        assert_eq!(client.amended_orders(), Vec::<AmendedOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn take_profit_amend_conversion_failure_prevents_submission() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let client = MockOkxClient {
            fail_quote_usd_rate: true,
            open_orders: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.0005",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "8",
            })],
            ..MockOkxClient::default()
        };

        let error = runner
            .refresh_take_profit_order(&client)
            .await
            .expect_err("missing conversion evidence must prevent amend");

        assert!(
            error
                .to_string()
                .contains("mock quote-to-USD evidence unavailable"),
            "conversion failure should remain explicit: {error}"
        );
        assert_eq!(client.amended_orders(), Vec::<AmendedOrder>::new());
        assert!(!client.calls().contains(&MockOkxCall::AmendOrder));
        Ok(())
    }

    #[tokio::test]
    async fn live_take_profit_with_invalid_price_fails_closed() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let client = MockOkxClient {
            open_orders: vec![order_with_price(
                OrderFixture {
                    client_order_id: &take_profit_id,
                    side: OrderSide::Sell,
                    kind: OrderKind::Limit,
                    state: "live",
                    size: "0.001",
                    accumulated_fill_size: "0",
                    average_price: "",
                    updated_at_ms: "8",
                },
                "not-a-decimal",
            )],
            ..MockOkxClient::default()
        };

        let error = runner
            .refresh_take_profit_order(&client)
            .await
            .expect_err("invalid take-profit px should fail closed");

        assert!(
            error.to_string().contains("invalid px"),
            "invalid take-profit px should report parse failure: {error}"
        );
        assert_eq!(client.amended_orders(), Vec::<AmendedOrder>::new());
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn take_profit_amend_uses_exact_exit_fee_break_even_floor() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        runner.signal.last_atr = Some(0.1);
        let client = MockOkxClient::default();

        runner.amend_take_profit_order(&client).await?;

        assert_eq!(
            client.amended_orders(),
            vec![AmendedOrder {
                inst_id: "BTC-USDT".to_owned(),
                client_order_id: take_profit_id(strategy_id),
                new_size: Some("0.001".to_owned()),
                new_price: Some("110.3".to_owned()),
            }]
        );
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        Ok(())
    }

    #[tokio::test]
    async fn partially_filled_take_profit_accounts_for_base_rebate_before_cancel() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let mut take_profit = order(OrderFixture {
            client_order_id: &take_profit_id,
            side: OrderSide::Sell,
            kind: OrderKind::Limit,
            state: "partially_filled",
            size: "0.001",
            accumulated_fill_size: "0.0004",
            average_price: "120",
            updated_at_ms: "8",
        });
        take_profit.rebate = "0.00001".to_owned();
        take_profit.rebate_currency = "BTC".to_owned();
        let client = MockOkxClient {
            open_orders: vec![take_profit],
            ..MockOkxClient::default()
        };

        runner.refresh_take_profit_order(&client).await?;

        assert_eq!(client.amended_orders(), Vec::<AmendedOrder>::new());
        assert_eq!(client.canceled_orders(), vec![take_profit_id]);
        assert_eq!(client.canceled_algo_orders(), vec!["algo-stop".to_owned()]);
        assert_eq!(
            runner
                .exchange()?
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(true)
        );
        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.00061"),
                average_price: dec!("110"),
                stop_loss_trigger: dec!("100"),
            })
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn decreasing_take_profit_fill_size_fails_closed_without_replacement() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        let state = runner
            .exchange
            .as_mut()
            .expect("exchange state should be initialized");
        state.take_profit_order = Some(TrackedOrder {
            client_order_id: take_profit_id.clone(),
            last_fill_size: dec!("0.0005"),
            last_average_fill_price: None,
            last_accounted_base_change: Decimal::ZERO,
            last_accounted_quote_change: Decimal::ZERO,
            cancel_requested: true,
        });
        state.position = Some(OpenPosition {
            quantity: dec!("0.0005"),
            average_price: dec!("110"),
            stop_loss_trigger: dec!("100"),
        });
        let client = MockOkxClient {
            order_history: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "canceled",
                size: "0.001",
                accumulated_fill_size: "0.0004",
                average_price: "120",
                updated_at_ms: "8",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .refresh_take_profit_order(&client)
            .await
            .expect_err("decreasing take-profit cumulative fill should fail closed");

        assert!(
            err.to_string().contains("cumulative fill size decreased"),
            "inconsistent take-profit fill should report monotonicity failure: {err}"
        );
        let state = runner.exchange()?;
        assert_eq!(
            state.take_profit_order,
            Some(TrackedOrder {
                client_order_id: take_profit_id,
                last_fill_size: dec!("0.0005"),
                last_average_fill_price: None,
                last_accounted_base_change: Decimal::ZERO,
                last_accounted_quote_change: Decimal::ZERO,
                cancel_requested: true,
            })
        );
        assert_eq!(
            state.position,
            Some(OpenPosition {
                quantity: dec!("0.0005"),
                average_price: dec!("110"),
                stop_loss_trigger: dec!("100"),
            })
        );
        assert_eq!(client.canceled_orders(), Vec::<String>::new());
        assert_eq!(client.canceled_algo_orders(), Vec::<String>::new());
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn canceled_stop_loss_is_replaced_after_okx_confirms_closure() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_resized_position_canceling_take_profit(strategy_id);
        let stale_trigger = runner.stop_loss_trigger(dec!("100"))?;
        let stale_stop_loss_order = runner
            .exchange
            .as_mut()
            .and_then(|state| state.stop_loss_order.as_mut())
            .expect("stop-loss order should be tracked");
        stale_stop_loss_order.size = dec!("0.0005");
        stale_stop_loss_order.trigger_price = stale_trigger;
        let cancel_client = MockOkxClient::default();

        runner.ensure_stop_loss_order(&cancel_client).await?;
        runner.ensure_stop_loss_order(&cancel_client).await?;

        assert_eq!(
            cancel_client.canceled_algo_orders(),
            vec!["algo-stop".to_owned()]
        );
        assert_eq!(
            cancel_client.placed_algo_orders(),
            Vec::<PlacedAlgoOrder>::new()
        );
        assert_eq!(
            runner
                .exchange()?
                .stop_loss_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(true)
        );

        let replace_client = MockOkxClient {
            ticker: ticker_with_last("110"),
            algo_order_history: vec![stop_loss_algo(strategy_id, "canceled")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&replace_client).await?;
        runner.ensure_stop_loss_order(&replace_client).await?;

        assert_eq!(
            replace_client.placed_algo_orders(),
            vec![PlacedAlgoOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                size: "0.001".to_owned(),
                trigger_price: decimal_to_okx(quantize_decimal_down(
                    runner.stop_loss_trigger(dec!("105"))?,
                    instrument().tick_size()?,
                )?),
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        assert_eq!(
            runner
                .exchange()?
                .stop_loss_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(false)
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_pending_stop_loss_is_not_replaced_while_still_live() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_resized_position_canceling_take_profit(strategy_id);
        let stale_trigger = runner.stop_loss_trigger(dec!("100"))?;
        let stale_stop_loss_order = runner
            .exchange
            .as_mut()
            .and_then(|state| state.stop_loss_order.as_mut())
            .expect("stop-loss order should be tracked");
        stale_stop_loss_order.size = dec!("0.0005");
        stale_stop_loss_order.trigger_price = stale_trigger;
        let cancel_client = MockOkxClient::default();

        runner.ensure_stop_loss_order(&cancel_client).await?;
        runner.ensure_stop_loss_order(&cancel_client).await?;

        assert_eq!(
            cancel_client.canceled_algo_orders(),
            vec!["algo-stop".to_owned()]
        );
        assert_eq!(
            cancel_client.placed_algo_orders(),
            Vec::<PlacedAlgoOrder>::new()
        );

        let live_client = MockOkxClient {
            open_algo_orders: vec![stop_loss_algo(strategy_id, "live")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&live_client).await?;
        runner.ensure_stop_loss_order(&live_client).await?;

        assert_eq!(
            runner.exchange()?.stop_loss_order,
            Some(TrackedAlgoOrder {
                algo_id: "algo-stop".to_owned(),
                client_order_id: stop_loss_id(strategy_id),
                size: dec!("0.001"),
                trigger_price: dec!("100"),
                cancel_requested: true,
            })
        );
        assert_eq!(
            live_client.placed_algo_orders(),
            Vec::<PlacedAlgoOrder>::new()
        );
        assert_eq!(live_client.canceled_algo_orders(), Vec::<String>::new());
        Ok(())
    }

    #[tokio::test]
    async fn tracked_stop_loss_refresh_rejects_non_trigger_market_algo() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        let mut invalid_stop_loss = stop_loss_algo(strategy_id, "live");
        invalid_stop_loss.order_type = "conditional".to_owned();
        invalid_stop_loss.order_price = "100".to_owned();
        let client = MockOkxClient {
            open_algo_orders: vec![invalid_stop_loss],
            ..MockOkxClient::default()
        };

        let err = runner
            .refresh_stop_loss_order(&client)
            .await
            .expect_err("non-trigger-market stop-loss algo should fail closed");

        assert!(
            err.to_string().contains("unexpected type"),
            "invalid stop-loss algo shape should report type validation failure: {err}"
        );
        assert_eq!(
            runner.exchange()?.stop_loss_order,
            Some(TrackedAlgoOrder {
                algo_id: "algo-stop".to_owned(),
                client_order_id: stop_loss_id(strategy_id),
                size: dec!("0.001"),
                trigger_price: dec!("100"),
                cancel_requested: false,
            })
        );
        assert_eq!(client.canceled_algo_orders(), Vec::<String>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn tracked_stop_loss_refresh_rejects_empty_state_algo() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        let client = MockOkxClient {
            open_algo_orders: vec![stop_loss_algo(strategy_id, "")],
            ..MockOkxClient::default()
        };

        let err = runner
            .refresh_stop_loss_order(&client)
            .await
            .expect_err("empty-state tracked stop-loss algo should fail closed");

        assert!(
            err.to_string().contains("unexpected state")
                && err.to_string().contains("explicit live or pause"),
            "empty-state tracked stop-loss algo should report explicit state validation failure: {err}"
        );
        assert_eq!(
            runner.exchange()?.stop_loss_order,
            Some(TrackedAlgoOrder {
                algo_id: "algo-stop".to_owned(),
                client_order_id: stop_loss_id(strategy_id),
                size: dec!("0.001"),
                trigger_price: dec!("100"),
                cancel_requested: false,
            })
        );
        assert_eq!(client.canceled_algo_orders(), Vec::<String>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn local_stop_pending_restores_take_profit_after_price_recovers() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let take_profit_id = take_profit_id(strategy_id);
        let take_profit_price = decimal_to_okx(quantize_decimal_up(
            runner.take_profit_price(dec!("110"))?,
            instrument().tick_size()?,
        )?);
        let live_take_profit = order_with_price(
            OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "live",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "10",
            },
            &take_profit_price,
        );
        let threshold_client = MockOkxClient {
            ticker: ticker_with_last("99"),
            open_orders: vec![live_take_profit],
            ..MockOkxClient::default()
        };

        runner.evaluate_stop_loss(&threshold_client).await?;

        {
            let state = runner.exchange()?;
            assert_eq!(
                state.stop_loss_pending,
                Some(StopLossPendingReason::LocalThreshold)
            );
            assert_eq!(
                state
                    .take_profit_order
                    .as_ref()
                    .map(|order| order.cancel_requested),
                Some(true)
            );
        }

        let canceled_take_profit = order_with_price(
            OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "canceled",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "11",
            },
            &take_profit_price,
        );
        let cancel_settled_client = MockOkxClient {
            order_history: vec![canceled_take_profit],
            ..MockOkxClient::default()
        };

        runner
            .refresh_take_profit_order(&cancel_settled_client)
            .await?;

        assert_eq!(runner.exchange()?.take_profit_order, None);

        let recovery_client = MockOkxClient {
            ticker: ticker_with_last("110"),
            open_algo_orders: vec![stop_loss_algo(strategy_id, "live")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&recovery_client).await?;

        let state = runner.exchange()?;
        assert_eq!(state.stop_loss_pending, None);
        assert_eq!(
            state.take_profit_order.as_ref().map(|order| {
                parse_strategy_client_order_id(&order.client_order_id, &strategy_tag(strategy_id))
            }),
            Some(Some(OrderPurpose::TakeProfit))
        );
        assert_eq!(
            recovery_client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                size: "0.001".to_owned(),
                price: Some(take_profit_price),
                purpose: Some(OrderPurpose::TakeProfit),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn local_stop_pending_does_not_restore_take_profit_while_cancel_is_ambiguous()
    -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let take_profit_id = take_profit_id(strategy_id);
        let take_profit_price = decimal_to_okx(quantize_decimal_up(
            runner.take_profit_price(dec!("110"))?,
            instrument().tick_size()?,
        )?);
        let threshold_client = MockOkxClient {
            ticker: ticker_with_last("99"),
            open_orders: vec![order_with_price(
                OrderFixture {
                    client_order_id: &take_profit_id,
                    side: OrderSide::Sell,
                    kind: OrderKind::Limit,
                    state: "live",
                    size: "0.001",
                    accumulated_fill_size: "0",
                    average_price: "",
                    updated_at_ms: "10",
                },
                &take_profit_price,
            )],
            ..MockOkxClient::default()
        };

        runner.evaluate_stop_loss(&threshold_client).await?;

        let recovery_client = MockOkxClient {
            ticker: ticker_with_last("110"),
            open_algo_orders: vec![stop_loss_algo(strategy_id, "live")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&recovery_client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state.stop_loss_pending,
            Some(StopLossPendingReason::LocalThreshold)
        );
        assert_eq!(
            state
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(true)
        );
        assert_eq!(recovery_client.placed_orders(), Vec::<PlacedOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn local_stop_pending_does_not_restore_take_profit_with_market_exit_order() -> Result<()>
    {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        seed_signal(&mut runner);
        {
            let state = runner
                .exchange
                .as_mut()
                .expect("exchange state should be initialized");
            state.stop_loss_pending = Some(StopLossPendingReason::LocalThreshold);
            state.stop_loss_exit_order = Some(TrackedOrder {
                client_order_id: stop_loss_exit_id(strategy_id),
                last_fill_size: Decimal::ZERO,
                last_average_fill_price: None,
                last_accounted_base_change: Decimal::ZERO,
                last_accounted_quote_change: Decimal::ZERO,
                cancel_requested: false,
            });
        }
        let recovery_client = MockOkxClient {
            ticker: ticker_with_last("110"),
            open_algo_orders: vec![stop_loss_algo(strategy_id, "live")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&recovery_client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state.stop_loss_pending,
            Some(StopLossPendingReason::LocalThreshold)
        );
        assert_eq!(state.take_profit_order, None);
        assert_eq!(recovery_client.placed_orders(), Vec::<PlacedOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn effective_stop_history_does_not_restore_take_profit_after_price_recovers() -> Result<()>
    {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let take_profit_id = take_profit_id(strategy_id);
        let take_profit_price = decimal_to_okx(quantize_decimal_up(
            runner.take_profit_price(dec!("110"))?,
            instrument().tick_size()?,
        )?);
        let client = MockOkxClient {
            ticker: ticker_with_last("110"),
            balances: vec![balance("BTC", "0.0005")],
            algo_order_history: vec![stop_loss_algo(strategy_id, "effective")],
            order_history: vec![order_with_price(
                OrderFixture {
                    client_order_id: &take_profit_id,
                    side: OrderSide::Sell,
                    kind: OrderKind::Limit,
                    state: "canceled",
                    size: "0.001",
                    accumulated_fill_size: "0",
                    average_price: "",
                    updated_at_ms: "11",
                },
                &take_profit_price,
            )],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&client).await?;
        runner.refresh_take_profit_order(&client).await?;
        runner.ensure_take_profit_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state.stop_loss_pending,
            Some(StopLossPendingReason::ExitReconciliation)
        );
        assert_eq!(state.take_profit_order, None);
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn missing_tracked_take_profit_is_resubmitted_for_open_position() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        let client = MockOkxClient::default();

        runner.refresh_take_profit_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(false)
        );
        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                size: "0.001".to_owned(),
                price: Some(decimal_to_okx(quantize_decimal_up(
                    runner.take_profit_price(dec!("110"))?,
                    instrument().tick_size()?,
                )?)),
                purpose: Some(OrderPurpose::TakeProfit),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_tracked_take_profit_fails_closed_when_limit_amount_exceeds_okx_bound()
    -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        seed_signal(&mut runner);
        runner.exchange_mut()?.instrument.max_limit_amount = "0.1".to_owned();
        let client = MockOkxClient::default();

        let error = runner
            .refresh_take_profit_order(&client)
            .await
            .expect_err("over-amount take-profit replacement should fail before submission");

        assert!(
            error.to_string().contains("maxLmtAmt"),
            "over-amount take-profit replacement should report maxLmtAmt: {error}"
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn missing_triggered_stop_preserves_remaining_balance_for_fallback_exit() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        let client = MockOkxClient {
            ticker: ticker_with_last("99"),
            balances: vec![balance("BTC", "0.0005")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&client).await?;

        {
            let state = runner.exchange()?;
            assert_eq!(
                state.position,
                Some(OpenPosition {
                    quantity: dec!("0.0005"),
                    average_price: dec!("110"),
                    stop_loss_trigger: dec!("100"),
                })
            );
            assert_eq!(state.stop_loss_order, None);
            assert_eq!(
                state.stop_loss_pending,
                Some(StopLossPendingReason::ExitReconciliation)
            );
        }

        runner.ensure_stop_loss_order(&client).await?;

        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn effective_stop_history_preserves_remaining_balance_after_price_recovers() -> Result<()>
    {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        let client = MockOkxClient {
            ticker: ticker_with_last("110"),
            balances: vec![balance("BTC", "0.0005")],
            algo_order_history: vec![stop_loss_algo(strategy_id, "effective")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&client).await?;

        {
            let state = runner.exchange()?;
            assert_eq!(
                state.position,
                Some(OpenPosition {
                    quantity: dec!("0.0005"),
                    average_price: dec!("110"),
                    stop_loss_trigger: dec!("100"),
                })
            );
            assert_eq!(state.stop_loss_order, None);
            assert_eq!(
                state.stop_loss_pending,
                Some(StopLossPendingReason::ExitReconciliation)
            );
        }

        runner.ensure_stop_loss_order(&client).await?;

        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn canceled_stop_history_resubmits_without_marking_pending() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        let client = MockOkxClient {
            ticker: ticker_with_last("110"),
            algo_order_history: vec![stop_loss_algo(strategy_id, "canceled")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&client).await?;

        {
            let state = runner.exchange()?;
            assert_eq!(
                state.position,
                Some(OpenPosition {
                    quantity: dec!("0.001"),
                    average_price: dec!("110"),
                    stop_loss_trigger: dec!("100"),
                })
            );
            assert_eq!(state.stop_loss_order, None);
            assert_eq!(state.stop_loss_pending, None);
        }

        runner.ensure_stop_loss_order(&client).await?;

        assert_eq!(
            client.placed_algo_orders(),
            vec![PlacedAlgoOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                size: "0.001".to_owned(),
                trigger_price: "100".to_owned(),
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_stop_history_resubmits_without_marking_pending() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        let client = MockOkxClient {
            ticker: ticker_with_last("110"),
            algo_order_history: vec![stop_loss_algo(strategy_id, "order_failed")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&client).await?;

        {
            let state = runner.exchange()?;
            assert_eq!(
                state.position,
                Some(OpenPosition {
                    quantity: dec!("0.001"),
                    average_price: dec!("110"),
                    stop_loss_trigger: dec!("100"),
                })
            );
            assert_eq!(state.stop_loss_order, None);
            assert_eq!(state.stop_loss_pending, None);
        }

        runner.ensure_stop_loss_order(&client).await?;

        assert_eq!(
            client.placed_algo_orders(),
            vec![PlacedAlgoOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                size: "0.001".to_owned(),
                trigger_price: "100".to_owned(),
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn canceled_stop_history_below_trigger_preserves_balance_for_fallback_exit() -> Result<()>
    {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        let client = MockOkxClient {
            ticker: ticker_with_last("99"),
            balances: vec![balance("BTC", "0.0005")],
            algo_order_history: vec![stop_loss_algo(strategy_id, "canceled")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state.position,
            Some(OpenPosition {
                quantity: dec!("0.0005"),
                average_price: dec!("110"),
                stop_loss_trigger: dec!("100"),
            })
        );
        assert_eq!(state.stop_loss_order, None);
        assert_eq!(
            state.stop_loss_pending,
            Some(StopLossPendingReason::ExitReconciliation)
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_triggered_stop_caps_reconciled_balance_to_tracked_position() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        let client = MockOkxClient {
            ticker: ticker_with_last("99"),
            balances: vec![balance("BTC", "0.005")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("110"),
                stop_loss_trigger: dec!("100"),
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_take_profit_cancel_remains_retryable_after_missing_stop() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        let client = MockOkxClient {
            ticker: ticker_with_last("99"),
            balances: vec![balance("BTC", "0.0005")],
            fail_order_cancels: true,
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state
                .take_profit_order
                .as_ref()
                .map(|order| order.cancel_requested),
            Some(false)
        );
        assert_eq!(client.canceled_orders(), vec![take_profit_id(strategy_id)]);
        Ok(())
    }

    #[tokio::test]
    async fn pending_stop_uses_reconciled_balance_without_double_counting_take_profit_fill()
    -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let take_profit_id = take_profit_id(strategy_id);
        let mut runner = runner_with_pending_stop_and_take_profit(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.0006")],
            order_history: vec![order(OrderFixture {
                client_order_id: &take_profit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Limit,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.0004",
                average_price: "120",
                updated_at_ms: "5",
            })],
            ..MockOkxClient::default()
        };

        runner.evaluate_stop_loss(&client).await?;

        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Market,
                size: "0.0006".to_owned(),
                price: None,
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn pending_stop_tracks_market_exit_until_fill_confirmation() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_pending_stop(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.0006")],
            ..MockOkxClient::default()
        };

        runner.evaluate_stop_loss(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state.position,
            Some(OpenPosition {
                quantity: dec!("0.0006"),
                average_price: dec!("110"),
                stop_loss_trigger: dec!("100"),
            })
        );
        assert_eq!(
            state.stop_loss_exit_order.as_ref().map(|order| {
                parse_strategy_client_order_id(&order.client_order_id, &strategy_tag(strategy_id))
            }),
            Some(Some(OrderPurpose::StopLoss))
        );
        assert_eq!(
            state.stop_loss_pending,
            Some(StopLossPendingReason::ExitReconciliation)
        );
        Ok(())
    }

    #[tokio::test]
    async fn stop_loss_market_exit_fill_clears_position() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let stop_loss_exit_id = stop_loss_exit_id(strategy_id);
        let mut runner = runner_with_stop_loss_exit_order(strategy_id);
        let client = MockOkxClient {
            order_history: vec![order(OrderFixture {
                client_order_id: &stop_loss_exit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Market,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "99",
                updated_at_ms: "6",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_exit_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(state.position, None);
        assert_eq!(state.stop_loss_exit_order, None);
        assert_eq!(state.stop_loss_pending, None);
        Ok(())
    }

    #[tokio::test]
    async fn partial_stop_loss_market_exit_keeps_remaining_position_pending() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let stop_loss_exit_id = stop_loss_exit_id(strategy_id);
        let mut runner = runner_with_stop_loss_exit_order(strategy_id);
        let client = MockOkxClient {
            order_history: vec![order(OrderFixture {
                client_order_id: &stop_loss_exit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Market,
                state: "canceled",
                size: "0.001",
                accumulated_fill_size: "0.0005",
                average_price: "99",
                updated_at_ms: "6",
            })],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_exit_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(
            state.position,
            Some(OpenPosition {
                quantity: dec!("0.0005"),
                average_price: dec!("110"),
                stop_loss_trigger: dec!("100"),
            })
        );
        assert_eq!(state.stop_loss_exit_order, None);
        assert_eq!(
            state.stop_loss_pending,
            Some(StopLossPendingReason::ExitReconciliation)
        );
        Ok(())
    }

    #[tokio::test]
    async fn decreasing_stop_loss_market_exit_fill_fails_closed_without_retry() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let stop_loss_exit_id = stop_loss_exit_id(strategy_id);
        let mut runner = runner_with_stop_loss_exit_order(strategy_id);
        let state = runner
            .exchange
            .as_mut()
            .expect("exchange state should be initialized");
        state.stop_loss_exit_order = Some(TrackedOrder {
            client_order_id: stop_loss_exit_id.clone(),
            last_fill_size: dec!("0.0005"),
            last_average_fill_price: None,
            last_accounted_base_change: Decimal::ZERO,
            last_accounted_quote_change: Decimal::ZERO,
            cancel_requested: false,
        });
        let client = MockOkxClient {
            order_history: vec![order(OrderFixture {
                client_order_id: &stop_loss_exit_id,
                side: OrderSide::Sell,
                kind: OrderKind::Market,
                state: "partially_filled",
                size: "0.001",
                accumulated_fill_size: "0.0004",
                average_price: "99",
                updated_at_ms: "6",
            })],
            ..MockOkxClient::default()
        };

        let err = runner
            .refresh_stop_loss_exit_order(&client)
            .await
            .expect_err("decreasing stop-loss market exit fill should fail closed");

        assert!(
            err.to_string().contains("cumulative fill size decreased"),
            "inconsistent stop-loss market exit fill should report monotonicity failure: {err}"
        );
        let state = runner.exchange()?;
        assert_eq!(
            state.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("110"),
                stop_loss_trigger: dec!("100"),
            })
        );
        assert_eq!(
            state.stop_loss_exit_order,
            Some(TrackedOrder {
                client_order_id: stop_loss_exit_id,
                last_fill_size: dec!("0.0005"),
                last_average_fill_price: None,
                last_accounted_base_change: Decimal::ZERO,
                last_accounted_quote_change: Decimal::ZERO,
                cancel_requested: false,
            })
        );
        assert_eq!(
            state.stop_loss_pending,
            Some(StopLossPendingReason::ExitReconciliation)
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn missing_stop_loss_market_exit_retries_reconciled_balance() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_stop_loss_exit_order(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.0006")],
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_exit_order(&client).await?;
        {
            let state = runner.exchange()?;
            assert_eq!(
                state.position,
                Some(OpenPosition {
                    quantity: dec!("0.0006"),
                    average_price: dec!("110"),
                    stop_loss_trigger: dec!("100"),
                })
            );
            assert_eq!(state.stop_loss_exit_order, None);
            assert_eq!(
                state.stop_loss_pending,
                Some(StopLossPendingReason::ExitReconciliation)
            );
        }

        runner.evaluate_stop_loss(&client).await?;
        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Market,
                size: "0.0006".to_owned(),
                price: None,
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        let state = runner.exchange()?;
        assert_eq!(
            state.position,
            Some(OpenPosition {
                quantity: dec!("0.0006"),
                average_price: dec!("110"),
                stop_loss_trigger: dec!("100"),
            })
        );
        assert_eq!(
            state.stop_loss_exit_order.as_ref().map(|order| {
                parse_strategy_client_order_id(&order.client_order_id, &strategy_tag(strategy_id))
            }),
            Some(Some(OrderPurpose::StopLoss))
        );
        assert_eq!(
            state.stop_loss_pending,
            Some(StopLossPendingReason::ExitReconciliation)
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_triggered_stop_clears_position_after_balance_is_gone() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_position_and_stop(strategy_id);
        let client = MockOkxClient {
            ticker: ticker_with_last("99"),
            ..MockOkxClient::default()
        };

        runner.refresh_stop_loss_order(&client).await?;

        let state = runner.exchange()?;
        assert_eq!(state.position, None);
        assert_eq!(state.stop_loss_order, None);
        assert_eq!(state.stop_loss_pending, None);
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_tick_reconciles_live_entry_from_rest() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_empty_exchange(strategy_id);
        seed_signal(&mut runner);
        let client = MockOkxClient {
            open_orders: vec![order(OrderFixture {
                client_order_id: &entry_id(strategy_id),
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "live",
                size: "0.001",
                accumulated_fill_size: "0",
                average_price: "",
                updated_at_ms: "10",
            })],
            ..MockOkxClient::default()
        };

        runner.reconcile_after_interrupted_tick(&client).await?;

        assert_eq!(
            runner
                .exchange()?
                .entry_order
                .as_ref()
                .map(|order| order.client_order_id.clone()),
            Some(entry_id(strategy_id))
        );
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_tick_uses_targeted_order_lookup_without_broad_history() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let entry_id = entry_id(strategy_id);
        let mut runner = runner_with_live_entry(strategy_id);
        let client = MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "10",
            })],
            ..MockOkxClient::default()
        };

        runner.reconcile_after_interrupted_tick(&client).await?;

        assert_eq!(
            runner.exchange()?.position,
            Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("100"),
                stop_loss_trigger: runner.stop_loss_trigger(dec!("100"))?,
            })
        );
        assert_eq!(client.order_lookup_client_order_ids(), vec![entry_id]);
        assert_eq!(client.broad_history_call_counts(), (0, 0));
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_tick_below_stop_submits_market_exit() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_empty_exchange(strategy_id);
        seed_signal(&mut runner);
        let client = MockOkxClient {
            ticker: ticker_with_last("99"),
            balances: vec![balance("BTC", "0.001")],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id(strategy_id),
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "110",
                updated_at_ms: "10",
            })],
            ..MockOkxClient::default()
        };

        runner.reconcile_after_interrupted_tick(&client).await?;

        let state = runner.exchange()?;
        assert!(state.stop_loss_exit_order.is_some());
        assert_eq!(
            client.placed_orders(),
            vec![PlacedOrder {
                inst_id: "BTC-USDT".to_owned(),
                side: OrderSide::Sell,
                kind: OrderKind::Market,
                size: "0.001".to_owned(),
                price: None,
                purpose: Some(OrderPurpose::StopLoss),
            }]
        );
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_tick_rejects_stop_market_exit_above_usdt_max_market_size() -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = runner_with_empty_exchange(strategy_id);
        runner.exchange_mut()?.instrument.max_market_size = "0.05".to_owned();
        seed_signal(&mut runner);
        let client = MockOkxClient {
            ticker: ticker_with_last("99"),
            balances: vec![balance("BTC", "0.001")],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id(strategy_id),
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "110",
                updated_at_ms: "10",
            })],
            ..MockOkxClient::default()
        };

        let error = runner
            .reconcile_after_interrupted_tick(&client)
            .await
            .expect_err("USDT-denominated maxMktSz must block the oversized market exit");

        assert!(
            error.to_string().contains("USDT notional 0.099")
                && error.to_string().contains("exceeds OKX maxMktSz 0.05"),
            "market-size rejection should report the documented USDT unit: {error}"
        );
        assert_eq!(client.placed_orders(), Vec::<PlacedOrder>::new());
        Ok(())
    }

    fn bar(ts_ms: i64, open: f64, high: f64, low: f64, close: f64) -> MarketBar {
        MarketBar {
            ts_ms,
            open,
            high,
            low,
            close,
            confirm: true,
        }
    }

    fn checked_in_demo_runner() -> Result<OkxEmaAtrMakerTrendRunner> {
        let contents = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml"),
        )
        .context("checked-in demo profile should be readable")?;
        let config = load_config_from_str_with_secret_resolver(&contents, |name| {
            Some(format!("test-{name}"))
        })?;
        let instance = config
            .strategies
            .instances
            .iter()
            .find(|instance| instance.enabled && instance.kind == StrategyKind::OkxEmaAtrMakerTrend)
            .context("checked-in demo profile should include enabled OkxEmaAtrMakerTrend")?;
        OkxEmaAtrMakerTrendRunner::from_instance(&config, instance)
    }

    fn test_runner(strategy_id: &str) -> OkxEmaAtrMakerTrendRunner {
        test_runner_with_configured_tags(strategy_id, vec![strategy_tag(strategy_id)])
    }

    fn test_runner_with_operator_baseline(
        strategy_id: &str,
        operator_owned_base_balance: Decimal,
    ) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = test_runner(strategy_id);
        runner.operator_owned_base_balance = operator_owned_base_balance;
        runner
    }

    fn test_runner_with_configured_tags(
        strategy_id: &str,
        configured_strategy_tags: Vec<String>,
    ) -> OkxEmaAtrMakerTrendRunner {
        let mut signal = SignalState::new(2, 3, 3, dec!("0.1"), dec!("1.0"), dec!("15.0"));
        signal
            .set_round_trip_cost_rate(dec!("0.003"))
            .expect("test fee schedule should be valid");
        OkxEmaAtrMakerTrendRunner {
            instance_id: strategy_id.to_owned(),
            configured_strategy_tags,
            instrument_id: "BTC-USDT".to_owned(),
            validated_instrument: None,
            quantity: dec!("0.001"),
            operator_owned_base_balance: Decimal::ZERO,
            max_entry_order_age_ms: 15_000,
            max_quote_notional: Some(dec!("500")),
            take_profit_atr_multiple: dec!("1.5"),
            stop_loss_atr_multiple: dec!("1.0"),
            entry_fee_cost_rate: Some(dec!("0.001")),
            exit_fee_cost_rate: Some(dec!("0.002")),
            signal,
            exchange: None,
        }
    }

    fn seed_signal(runner: &mut OkxEmaAtrMakerTrendRunner) {
        for bar in warmup_bars() {
            runner.signal.update_from_bar(&bar);
        }
    }

    fn warmup_bars() -> Vec<MarketBar> {
        vec![
            bar(1, 100.0, 101.0, 99.0, 100.0),
            bar(2, 101.0, 103.0, 100.0, 102.0),
            bar(3, 102.0, 106.0, 101.0, 105.0),
            bar(4, 105.0, 111.0, 104.0, 110.0),
        ]
    }

    fn numeric_boundary_precision_bars() -> Vec<MarketBar> {
        vec![
            bar(1, 100.0, 100.6, 99.6, 100.1),
            bar(2, 100.1, 100.7, 99.7, 100.2),
            bar(3, 100.2, 100.8, 99.8, 100.3),
            bar(4, 100.3, 100.9, 99.9, 100.4),
        ]
    }

    fn instrument() -> OkxInstrument {
        OkxInstrument {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            group_id: "12".to_owned(),
            inst_id_code: Some(123_456),
            state: "live".to_owned(),
            base_ccy: "BTC".to_owned(),
            quote_ccy: "USDT".to_owned(),
            trade_quote_currencies: vec!["USDT".to_owned()],
            tick_size: "0.1".to_owned(),
            lot_size: "0.0001".to_owned(),
            min_size: "0.0001".to_owned(),
            max_limit_size: "999".to_owned(),
            max_limit_amount: "100000".to_owned(),
            max_market_size: "100".to_owned(),
            max_market_amount: "100000".to_owned(),
            max_trigger_size: "999".to_owned(),
            initial_price_limit_pct: "0.05".to_owned(),
            float_price_limit_pct: "0.03".to_owned(),
            maximum_price_limit_pct: "0.15".to_owned(),
        }
    }

    fn precision_instrument() -> OkxInstrument {
        OkxInstrument {
            tick_size: "0.01".to_owned(),
            lot_size: "0.000001".to_owned(),
            min_size: "0.000001".to_owned(),
            ..instrument()
        }
    }

    fn runner_with_position_and_stop(strategy_id: &str) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = test_runner(strategy_id);
        runner.exchange = Some(ExchangeState {
            instrument: instrument(),
            last_bar_ts_ms: None,
            entry_order: None,
            take_profit_order: None,
            stop_loss_order: Some(TrackedAlgoOrder {
                algo_id: "algo-stop".to_owned(),
                client_order_id: format!("ROX{}S00000001", strategy_tag(strategy_id)),
                size: dec!("0.001"),
                trigger_price: dec!("100"),
                cancel_requested: false,
            }),
            stop_loss_exit_order: None,
            position: Some(OpenPosition {
                quantity: dec!("0.001"),
                average_price: dec!("110"),
                stop_loss_trigger: dec!("100"),
            }),
            stop_loss_pending: None,
        });
        runner
    }

    fn runner_with_live_entry(strategy_id: &str) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = test_runner(strategy_id);
        seed_signal(&mut runner);
        runner.exchange = Some(ExchangeState {
            instrument: instrument(),
            last_bar_ts_ms: None,
            entry_order: Some(TrackedOrder {
                client_order_id: entry_id(strategy_id),
                last_fill_size: Decimal::ZERO,
                last_average_fill_price: None,
                last_accounted_base_change: Decimal::ZERO,
                last_accounted_quote_change: Decimal::ZERO,
                cancel_requested: false,
            }),
            take_profit_order: None,
            stop_loss_order: None,
            stop_loss_exit_order: None,
            position: None,
            stop_loss_pending: None,
        });
        runner
    }

    fn runner_with_empty_exchange(strategy_id: &str) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = test_runner(strategy_id);
        runner.exchange = Some(ExchangeState {
            instrument: instrument(),
            last_bar_ts_ms: None,
            entry_order: None,
            take_profit_order: None,
            stop_loss_order: None,
            stop_loss_exit_order: None,
            position: None,
            stop_loss_pending: None,
        });
        runner
    }

    fn runner_with_partially_tracked_entry(strategy_id: &str) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = runner_with_live_entry(strategy_id);
        let stop_loss_trigger = runner
            .stop_loss_trigger(dec!("100"))
            .expect("stop-loss trigger should be calculable");
        let state = runner
            .exchange
            .as_mut()
            .expect("exchange state should be initialized");
        state.entry_order = Some(TrackedOrder {
            client_order_id: entry_id(strategy_id),
            last_fill_size: dec!("0.0005"),
            last_average_fill_price: Some(dec!("100")),
            last_accounted_base_change: dec!("0.0005"),
            last_accounted_quote_change: dec!("-0.05"),
            cancel_requested: true,
        });
        state.take_profit_order = Some(TrackedOrder {
            client_order_id: take_profit_id(strategy_id),
            last_fill_size: Decimal::ZERO,
            last_average_fill_price: None,
            last_accounted_base_change: Decimal::ZERO,
            last_accounted_quote_change: Decimal::ZERO,
            cancel_requested: false,
        });
        state.stop_loss_order = Some(TrackedAlgoOrder {
            algo_id: "algo-stop".to_owned(),
            client_order_id: stop_loss_id(strategy_id),
            size: dec!("0.0005"),
            trigger_price: stop_loss_trigger,
            cancel_requested: false,
        });
        state.position = Some(OpenPosition {
            quantity: dec!("0.0005"),
            average_price: dec!("100"),
            stop_loss_trigger,
        });
        runner
    }

    fn runner_with_resized_position_canceling_take_profit(
        strategy_id: &str,
    ) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = runner_with_partially_tracked_entry(strategy_id);
        let stop_loss_trigger = runner
            .stop_loss_trigger(dec!("105"))
            .expect("stop-loss trigger should be calculable");
        let state = runner
            .exchange
            .as_mut()
            .expect("exchange state should be initialized");
        state.entry_order = None;
        state.take_profit_order = Some(TrackedOrder {
            client_order_id: take_profit_id(strategy_id),
            last_fill_size: Decimal::ZERO,
            last_average_fill_price: None,
            last_accounted_base_change: Decimal::ZERO,
            last_accounted_quote_change: Decimal::ZERO,
            cancel_requested: true,
        });
        state.stop_loss_order = Some(TrackedAlgoOrder {
            algo_id: "algo-stop".to_owned(),
            client_order_id: stop_loss_id(strategy_id),
            size: dec!("0.001"),
            trigger_price: stop_loss_trigger,
            cancel_requested: false,
        });
        state.position = Some(OpenPosition {
            quantity: dec!("0.001"),
            average_price: dec!("105"),
            stop_loss_trigger,
        });
        runner
    }

    fn runner_with_position_stop_and_take_profit(strategy_id: &str) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = runner_with_position_and_stop(strategy_id);
        let state = runner
            .exchange
            .as_mut()
            .expect("exchange state should be initialized");
        state.take_profit_order = Some(TrackedOrder {
            client_order_id: take_profit_id(strategy_id),
            last_fill_size: Decimal::ZERO,
            last_average_fill_price: None,
            last_accounted_base_change: Decimal::ZERO,
            last_accounted_quote_change: Decimal::ZERO,
            cancel_requested: false,
        });
        runner
    }

    fn runner_with_pending_stop_and_take_profit(strategy_id: &str) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = runner_with_position_stop_and_take_profit(strategy_id);
        let state = runner
            .exchange
            .as_mut()
            .expect("exchange state should be initialized");
        state.position = Some(OpenPosition {
            quantity: dec!("0.0006"),
            average_price: dec!("110"),
            stop_loss_trigger: dec!("100"),
        });
        state.stop_loss_order = None;
        state.stop_loss_pending = Some(StopLossPendingReason::ExitReconciliation);
        runner
    }

    fn runner_with_pending_stop(strategy_id: &str) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = runner_with_position_and_stop(strategy_id);
        let state = runner
            .exchange
            .as_mut()
            .expect("exchange state should be initialized");
        state.position = Some(OpenPosition {
            quantity: dec!("0.0006"),
            average_price: dec!("110"),
            stop_loss_trigger: dec!("100"),
        });
        state.stop_loss_order = None;
        state.stop_loss_pending = Some(StopLossPendingReason::ExitReconciliation);
        runner
    }

    fn runner_with_stop_loss_exit_order(strategy_id: &str) -> OkxEmaAtrMakerTrendRunner {
        let mut runner = runner_with_position_and_stop(strategy_id);
        let state = runner
            .exchange
            .as_mut()
            .expect("exchange state should be initialized");
        state.stop_loss_order = None;
        state.stop_loss_exit_order = Some(TrackedOrder {
            client_order_id: stop_loss_exit_id(strategy_id),
            last_fill_size: Decimal::ZERO,
            last_average_fill_price: None,
            last_accounted_base_change: Decimal::ZERO,
            last_accounted_quote_change: Decimal::ZERO,
            cancel_requested: false,
        });
        state.stop_loss_pending = Some(StopLossPendingReason::ExitReconciliation);
        runner
    }

    fn entry_id(strategy_id: &str) -> String {
        format!("ROX{}B00000001", strategy_tag(strategy_id))
    }

    fn legacy_entry_id(strategy_id: &str) -> String {
        format!("ROX{}B00000001", legacy_strategy_tag(strategy_id))
    }

    fn take_profit_id(strategy_id: &str) -> String {
        format!("ROX{}T00000001", strategy_tag(strategy_id))
    }

    fn stop_loss_id(strategy_id: &str) -> String {
        format!("ROX{}S00000001", strategy_tag(strategy_id))
    }

    fn legacy_stop_loss_id(strategy_id: &str) -> String {
        format!("ROX{}S00000001", legacy_strategy_tag(strategy_id))
    }

    fn stop_loss_exit_id(strategy_id: &str) -> String {
        format!("ROX{}S00000002", strategy_tag(strategy_id))
    }

    async fn assert_initialize_accepts_stop_loss_algo_state(state: &str) -> Result<()> {
        let strategy_id = "okx-ema-atr-maker-btc-usdt";
        let mut runner = test_runner(strategy_id);
        let trigger_price = reconstructed_stop_loss_trigger(strategy_id)?;
        let client = reconstruction_client_with_stop_loss_algo(
            strategy_id,
            stop_loss_algo_matching_reconstructed_position(strategy_id, state)?,
        );

        runner.initialize(&client).await?;

        assert_eq!(
            runner.exchange()?.stop_loss_order,
            Some(TrackedAlgoOrder {
                algo_id: "algo-stop".to_owned(),
                client_order_id: stop_loss_id(strategy_id),
                size: dec!("0.001"),
                trigger_price,
                cancel_requested: false,
            })
        );
        assert_eq!(client.placed_algo_orders(), Vec::<PlacedAlgoOrder>::new());
        Ok(())
    }

    fn reconstruction_client_with_stop_loss_algo(
        strategy_id: &str,
        stop_loss: OkxAlgoOrder,
    ) -> MockOkxClient {
        MockOkxClient {
            balances: vec![balance("BTC", "0.001")],
            open_algo_orders: vec![stop_loss],
            order_history: vec![order(OrderFixture {
                client_order_id: &entry_id(strategy_id),
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                state: "filled",
                size: "0.001",
                accumulated_fill_size: "0.001",
                average_price: "100",
                updated_at_ms: "4",
            })],
            ..MockOkxClient::default()
        }
    }

    fn stop_loss_algo_matching_reconstructed_position(
        strategy_id: &str,
        state: &str,
    ) -> Result<OkxAlgoOrder> {
        let mut stop_loss = stop_loss_algo(strategy_id, state);
        stop_loss.trigger_price = decimal_to_okx(reconstructed_stop_loss_trigger(strategy_id)?);
        Ok(stop_loss)
    }

    fn reconstructed_stop_loss_trigger(strategy_id: &str) -> Result<Decimal> {
        let mut runner = test_runner(strategy_id);
        seed_signal(&mut runner);
        quantize_decimal_down(
            runner.stop_loss_trigger(dec!("100"))?,
            instrument().tick_size()?,
        )
    }

    fn stop_loss_algo(strategy_id: &str, state: &str) -> OkxAlgoOrder {
        OkxAlgoOrder {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            td_mode: "cash".to_owned(),
            algo_id: "algo-stop".to_owned(),
            client_order_id: stop_loss_id(strategy_id),
            side: OrderSide::Sell.as_okx().to_owned(),
            order_type: "trigger".to_owned(),
            trigger_price: "100".to_owned(),
            order_price: "-1".to_owned(),
            state: state.to_owned(),
            sz: "0.001".to_owned(),
            created_at_ms: "10".to_owned(),
            updated_at_ms: "10".to_owned(),
        }
    }

    fn balance(ccy: &str, cash_balance: &str) -> OkxBalance {
        OkxBalance {
            details: vec![OkxBalanceDetail {
                ccy: ccy.to_owned(),
                available_balance: cash_balance.to_owned(),
                cash_balance: cash_balance.to_owned(),
                frozen_balance: "0".to_owned(),
            }],
        }
    }

    fn ticker() -> OkxTicker {
        ticker_with_last("110")
    }

    fn ticker_with_last(last: &str) -> OkxTicker {
        ticker_with_bid_last(last, last)
    }

    fn ticker_with_bid_last(bid: &str, last: &str) -> OkxTicker {
        OkxTicker {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            bid_px: bid.to_owned(),
            ask_px: last.to_owned(),
            last: last.to_owned(),
        }
    }

    struct OrderFixture<'a> {
        client_order_id: &'a str,
        side: OrderSide,
        kind: OrderKind,
        state: &'a str,
        size: &'a str,
        accumulated_fill_size: &'a str,
        average_price: &'a str,
        updated_at_ms: &'a str,
    }

    fn order(fixture: OrderFixture<'_>) -> OkxOrder {
        let fee_currency = match fixture.side {
            OrderSide::Buy => "BTC",
            OrderSide::Sell => "USDT",
        };
        OkxOrder {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            order_id: fixture.client_order_id.replace("ROX", "OKX"),
            client_order_id: fixture.client_order_id.to_owned(),
            side: fixture.side.as_okx().to_owned(),
            order_type: fixture.kind.as_okx().to_owned(),
            price: String::new(),
            state: fixture.state.to_owned(),
            average_price: fixture.average_price.to_owned(),
            accumulated_fill_size: fixture.accumulated_fill_size.to_owned(),
            fee: "0".to_owned(),
            fee_currency: fee_currency.to_owned(),
            rebate: "0".to_owned(),
            rebate_currency: fee_currency.to_owned(),
            sz: fixture.size.to_owned(),
            created_at_ms: fixture.updated_at_ms.to_owned(),
            updated_at_ms: fixture.updated_at_ms.to_owned(),
        }
    }

    fn order_with_price(fixture: OrderFixture<'_>, price: &str) -> OkxOrder {
        let mut order = order(fixture);
        order.price = price.to_owned();
        order
    }

    struct FillFixture<'a> {
        client_order_id: &'a str,
        side: OrderSide,
        fill_size: &'a str,
        fill_price: &'a str,
        fill_time_ms: &'a str,
        bill_id: &'a str,
    }

    fn fill(fixture: FillFixture<'_>) -> OkxFill {
        let fee_currency = match fixture.side {
            OrderSide::Buy => "BTC",
            OrderSide::Sell => "USDT",
        };
        OkxFill {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            order_id: fixture.client_order_id.replace("ROX", "OKX"),
            client_order_id: fixture.client_order_id.to_owned(),
            bill_id: fixture.bill_id.to_owned(),
            trade_id: String::new(),
            side: fixture.side.as_okx().to_owned(),
            fill_size: fixture.fill_size.to_owned(),
            fill_price: fixture.fill_price.to_owned(),
            fee: "0".to_owned(),
            fee_currency: fee_currency.to_owned(),
            fee_rate: "0".to_owned(),
            execution_type: "M".to_owned(),
            fill_time_ms: fixture.fill_time_ms.to_owned(),
            event_time_ms: String::new(),
        }
    }

    fn fill_with_fee(fixture: FillFixture<'_>, fee: &str, fee_currency: &str) -> OkxFill {
        let mut fill = fill(fixture);
        fill.fee = fee.to_owned();
        fill.fee_currency = fee_currency.to_owned();
        fill
    }

    #[derive(Clone, Debug, PartialEq)]
    struct PlacedOrder {
        inst_id: String,
        side: OrderSide,
        kind: OrderKind,
        size: String,
        price: Option<String>,
        purpose: Option<OrderPurpose>,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct AmendedOrder {
        inst_id: String,
        client_order_id: String,
        new_size: Option<String>,
        new_price: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct PlacedAlgoOrder {
        inst_id: String,
        side: OrderSide,
        size: String,
        trigger_price: String,
        purpose: Option<OrderPurpose>,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum MockOkxCall {
        Instruments,
        Candles,
        LiveCandles,
        Ticker,
        QuoteUsdRate,
        Balances,
        OpenOrders,
        OpenAlgoOrders,
        OrderHistory,
        OrderFills,
        AlgoOrderHistory,
        OrderLookup(Option<OrderPurpose>),
        PlaceOrder(Option<OrderPurpose>),
        PlaceTriggerOrder(Option<OrderPurpose>),
        CancelOrder(Option<OrderPurpose>),
        CancelAlgoOrder,
        AmendOrder,
    }

    struct MockOkxClient {
        instrument: OkxInstrument,
        candles: Vec<MarketBar>,
        ticker: OkxTicker,
        quote_usd_rate: Decimal,
        fail_quote_usd_rate: bool,
        balances: Vec<OkxBalance>,
        open_orders: Vec<OkxOrder>,
        open_algo_orders: Vec<OkxAlgoOrder>,
        algo_order_history: Vec<OkxAlgoOrder>,
        order_history: Vec<OkxOrder>,
        order_fills: Vec<OkxFill>,
        fail_order_history: bool,
        fail_order_cancels: bool,
        order_history_calls: Mutex<usize>,
        order_fills_calls: Mutex<usize>,
        order_lookup_client_order_ids: Mutex<Vec<String>>,
        placed_orders: Mutex<Vec<PlacedOrder>>,
        amended_orders: Mutex<Vec<AmendedOrder>>,
        placed_algo_orders: Mutex<Vec<PlacedAlgoOrder>>,
        canceled_orders: Mutex<Vec<String>>,
        canceled_algo_orders: Mutex<Vec<String>>,
        calls: Mutex<Vec<MockOkxCall>>,
    }

    impl Default for MockOkxClient {
        fn default() -> Self {
            Self {
                instrument: instrument(),
                candles: warmup_bars(),
                ticker: ticker(),
                quote_usd_rate: Decimal::ONE,
                fail_quote_usd_rate: false,
                balances: Vec::new(),
                open_orders: Vec::new(),
                open_algo_orders: Vec::new(),
                algo_order_history: Vec::new(),
                order_history: Vec::new(),
                order_fills: Vec::new(),
                fail_order_history: false,
                fail_order_cancels: false,
                order_history_calls: Mutex::new(0),
                order_fills_calls: Mutex::new(0),
                order_lookup_client_order_ids: Mutex::new(Vec::new()),
                placed_orders: Mutex::new(Vec::new()),
                amended_orders: Mutex::new(Vec::new()),
                placed_algo_orders: Mutex::new(Vec::new()),
                canceled_orders: Mutex::new(Vec::new()),
                canceled_algo_orders: Mutex::new(Vec::new()),
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl MockOkxClient {
        fn placed_orders(&self) -> Vec<PlacedOrder> {
            lock(&self.placed_orders).clone()
        }

        fn amended_orders(&self) -> Vec<AmendedOrder> {
            lock(&self.amended_orders).clone()
        }

        fn placed_algo_orders(&self) -> Vec<PlacedAlgoOrder> {
            lock(&self.placed_algo_orders).clone()
        }

        fn canceled_orders(&self) -> Vec<String> {
            lock(&self.canceled_orders).clone()
        }

        fn canceled_algo_orders(&self) -> Vec<String> {
            lock(&self.canceled_algo_orders).clone()
        }

        fn broad_history_call_counts(&self) -> (usize, usize) {
            (
                *lock(&self.order_history_calls),
                *lock(&self.order_fills_calls),
            )
        }

        fn order_lookup_client_order_ids(&self) -> Vec<String> {
            lock(&self.order_lookup_client_order_ids).clone()
        }

        fn calls(&self) -> Vec<MockOkxCall> {
            lock(&self.calls).clone()
        }
    }

    impl OkxClient for MockOkxClient {
        async fn instruments(&self, _inst_id: &str) -> Result<OkxInstrument> {
            lock(&self.calls).push(MockOkxCall::Instruments);
            Ok(self.instrument.clone())
        }

        async fn candles(
            &self,
            _inst_id: &str,
            _bar: &str,
            _limit: usize,
        ) -> Result<Vec<MarketBar>> {
            lock(&self.calls).push(MockOkxCall::Candles);
            Ok(self.candles.clone())
        }

        async fn live_candles(
            &self,
            _inst_id: &str,
            _bar: &str,
            _limit: usize,
        ) -> Result<Vec<MarketBar>> {
            lock(&self.calls).push(MockOkxCall::LiveCandles);
            Ok(self.candles.clone())
        }

        async fn ticker(&self, _inst_id: &str) -> Result<OkxTicker> {
            lock(&self.calls).push(MockOkxCall::Ticker);
            Ok(self.ticker.clone())
        }

        async fn fresh_quote_usd_rate(
            &self,
            instrument: &ValidatedTradingInstrument,
        ) -> Result<ValidatedQuoteUsdRate> {
            lock(&self.calls).push(MockOkxCall::QuoteUsdRate);
            if self.fail_quote_usd_rate {
                bail!("mock quote-to-USD evidence unavailable");
            }
            if instrument.quote_ccy() == "USD" {
                return ValidatedQuoteUsdRate::identity("USD");
            }
            ValidatedQuoteUsdRate::from_test_index(instrument.quote_ccy(), self.quote_usd_rate)
        }

        async fn balances(&self) -> Result<Vec<OkxBalance>> {
            lock(&self.calls).push(MockOkxCall::Balances);
            Ok(self.balances.clone())
        }

        async fn spot_trade_fee(&self, _inst_id: &str) -> Result<OkxTradeFeeRate> {
            Ok(OkxTradeFeeRate {
                inst_type: "SPOT".to_owned(),
                level: "Lv1".to_owned(),
                group_id: "12".to_owned(),
                maker: "-0.001".to_owned(),
                taker: "-0.002".to_owned(),
                ts: "1".to_owned(),
            })
        }

        async fn open_orders(&self, _inst_id: &str) -> Result<Vec<OkxOrder>> {
            lock(&self.calls).push(MockOkxCall::OpenOrders);
            Ok(self.open_orders.clone())
        }

        async fn order_history(&self, _inst_id: &str) -> Result<Vec<OkxOrder>> {
            lock(&self.calls).push(MockOkxCall::OrderHistory);
            *lock(&self.order_history_calls) += 1;
            if self.fail_order_history {
                bail!("mock order history pagination exceeded; refusing partial history");
            }
            Ok(self.order_history.clone())
        }

        async fn order_fills(&self, _inst_id: &str) -> Result<Vec<OkxFill>> {
            lock(&self.calls).push(MockOkxCall::OrderFills);
            *lock(&self.order_fills_calls) += 1;
            Ok(self.order_fills.clone())
        }

        async fn open_algo_orders(&self, _inst_id: &str) -> Result<Vec<OkxAlgoOrder>> {
            lock(&self.calls).push(MockOkxCall::OpenAlgoOrders);
            Ok(self.open_algo_orders.clone())
        }

        async fn algo_order_history(&self, _inst_id: &str) -> Result<Vec<OkxAlgoOrder>> {
            lock(&self.calls).push(MockOkxCall::AlgoOrderHistory);
            Ok(self.algo_order_history.clone())
        }

        async fn place_order(
            &self,
            inst_id: &str,
            side: OrderSide,
            kind: OrderKind,
            size: &str,
            price: Option<&str>,
            client_order_id: &str,
        ) -> Result<OkxOrderAck> {
            let purpose = parse_strategy_client_order_id(
                client_order_id,
                &strategy_tag("okx-ema-atr-maker-btc-usdt"),
            );
            lock(&self.calls).push(MockOkxCall::PlaceOrder(purpose));
            let placed_order = PlacedOrder {
                inst_id: inst_id.to_owned(),
                side,
                kind,
                size: size.to_owned(),
                price: price.map(str::to_owned),
                purpose,
            };
            lock(&self.placed_orders).push(placed_order);
            Ok(OkxOrderAck {
                order_id: "okx-order-id".to_owned(),
                client_order_id: client_order_id.to_owned(),
                status_code: "0".to_owned(),
                status_message: String::new(),
                status_sub_code: String::new(),
                timestamp: String::new(),
            })
        }

        async fn cancel_order(&self, _inst_id: &str, client_order_id: &str) -> Result<()> {
            lock(&self.calls).push(MockOkxCall::CancelOrder(parse_strategy_client_order_id(
                client_order_id,
                &strategy_tag("okx-ema-atr-maker-btc-usdt"),
            )));
            let fail_order_cancels = self.fail_order_cancels;
            lock(&self.canceled_orders).push(client_order_id.to_owned());
            if fail_order_cancels {
                anyhow::bail!("mock cancel failed");
            }
            Ok(())
        }

        async fn amend_order(&self, request: OkxOrderAmend<'_>) -> Result<OkxOrderAck> {
            lock(&self.calls).push(MockOkxCall::AmendOrder);
            let amended_order = AmendedOrder {
                inst_id: request.inst_id.to_owned(),
                client_order_id: request.client_order_id.to_owned(),
                new_size: request.new_size.map(str::to_owned),
                new_price: request.new_price.map(str::to_owned),
            };
            lock(&self.amended_orders).push(amended_order);
            Ok(OkxOrderAck {
                order_id: "okx-order-id".to_owned(),
                client_order_id: request.client_order_id.to_owned(),
                status_code: "0".to_owned(),
                status_message: String::new(),
                status_sub_code: String::new(),
                timestamp: String::new(),
            })
        }

        async fn place_trigger_order(
            &self,
            inst_id: &str,
            side: OrderSide,
            size: &str,
            trigger_price: &str,
            client_order_id: &str,
        ) -> Result<OkxAlgoOrderAck> {
            let purpose = parse_strategy_client_order_id(
                client_order_id,
                &strategy_tag("okx-ema-atr-maker-btc-usdt"),
            );
            lock(&self.calls).push(MockOkxCall::PlaceTriggerOrder(purpose));
            let placed_order = PlacedAlgoOrder {
                inst_id: inst_id.to_owned(),
                side,
                size: size.to_owned(),
                trigger_price: trigger_price.to_owned(),
                purpose,
            };
            lock(&self.placed_algo_orders).push(placed_order);
            Ok(OkxAlgoOrderAck {
                algo_id: format!("algo-{client_order_id}"),
                client_order_id: client_order_id.to_owned(),
                status_code: "0".to_owned(),
                status_message: String::new(),
            })
        }

        async fn cancel_algo_order(&self, _inst_id: &str, algo_id: &str) -> Result<()> {
            lock(&self.calls).push(MockOkxCall::CancelAlgoOrder);
            lock(&self.canceled_algo_orders).push(algo_id.to_owned());
            Ok(())
        }

        async fn order(&self, _inst_id: &str, client_order_id: &str) -> Result<Option<OkxOrder>> {
            lock(&self.calls).push(MockOkxCall::OrderLookup(parse_strategy_client_order_id(
                client_order_id,
                &strategy_tag("okx-ema-atr-maker-btc-usdt"),
            )));
            lock(&self.order_lookup_client_order_ids).push(client_order_id.to_owned());
            Ok(self
                .open_orders
                .iter()
                .chain(self.order_history.iter())
                .find(|order| order.client_order_id == client_order_id)
                .cloned())
        }
    }

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().expect("mock mutex should not be poisoned")
    }
}
