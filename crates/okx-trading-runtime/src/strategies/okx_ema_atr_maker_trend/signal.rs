use anyhow::{Result, ensure};
use rust_decimal::{Decimal, prelude::FromPrimitive};

use crate::okx::types::MarketBar;

/// EMA/ATR state intentionally uses `f64` for indicator math only. Any value
/// that can affect OKX order placement must cross into `Decimal` before
/// quantization and serialization.
#[derive(Clone, Debug)]
pub(super) struct SignalState {
    entry_offset_atr_multiple: Decimal,
    min_entry_offset_bps: Decimal,
    max_entry_offset_bps: Decimal,
    ema_fast: Ema,
    ema_slow: Ema,
    atr: Atr,
    round_trip_cost_rate: Option<Decimal>,
    macro_trend_bullish: bool,
    is_volatility_sufficient: bool,
    pub(super) current_atr_offset: Option<Decimal>,
    pub(super) last_close: Option<f64>,
    pub(super) last_atr: Option<f64>,
}

#[derive(Clone, Debug)]
struct Ema {
    period: usize,
    multiplier: f64,
    samples: usize,
    seed_sum: f64,
    value: Option<f64>,
}

#[derive(Clone, Debug)]
struct Atr {
    period: usize,
    samples: usize,
    seed_sum: f64,
    value: Option<f64>,
    previous_close: Option<f64>,
}

impl SignalState {
    pub(super) fn new(
        fast_ema_period: usize,
        slow_ema_period: usize,
        atr_period: usize,
        entry_offset_atr_multiple: Decimal,
        min_entry_offset_bps: Decimal,
        max_entry_offset_bps: Decimal,
    ) -> Self {
        Self {
            entry_offset_atr_multiple,
            min_entry_offset_bps,
            max_entry_offset_bps,
            ema_fast: Ema::new(fast_ema_period),
            ema_slow: Ema::new(slow_ema_period),
            atr: Atr::new(atr_period),
            round_trip_cost_rate: None,
            macro_trend_bullish: false,
            is_volatility_sufficient: false,
            current_atr_offset: None,
            last_close: None,
            last_atr: None,
        }
    }

    pub(super) fn update_from_bar(&mut self, bar: &MarketBar) -> bool {
        if !bar.close.is_finite()
            || bar.close <= 0.0
            || !bar.high.is_finite()
            || !bar.low.is_finite()
            || bar.high < bar.low
        {
            self.clear_macro_signal();
            return false;
        }

        self.ema_fast.update(bar.close);
        self.ema_slow.update(bar.close);
        self.atr.update(bar.high, bar.low, bar.close);

        let Some(fast) = self.ema_fast.value() else {
            self.clear_macro_signal();
            return false;
        };
        let Some(slow) = self.ema_slow.value() else {
            self.clear_macro_signal();
            return false;
        };
        let Some(atr) = self.atr.value() else {
            self.clear_macro_signal();
            return false;
        };

        self.last_close = Some(bar.close);
        self.last_atr = Some(atr);
        self.macro_trend_bullish = fast > slow && bar.close > slow;
        self.is_volatility_sufficient = self
            .round_trip_cost_rate
            .is_some_and(|cost_rate| volatility_clears_fee_threshold(bar.close, atr, cost_rate));
        self.current_atr_offset = if self.entry_allowed_without_offset() {
            entry_offset_distance(
                bar.close,
                atr,
                self.entry_offset_atr_multiple,
                self.min_entry_offset_bps,
                self.max_entry_offset_bps,
            )
        } else {
            None
        };
        true
    }

    pub(super) const fn last_atr(&self) -> Option<f64> {
        self.last_atr
    }

    pub(super) fn set_round_trip_cost_rate(&mut self, cost_rate: Decimal) -> Result<()> {
        ensure!(
            cost_rate >= Decimal::ZERO && cost_rate < Decimal::ONE,
            "OkxEmaAtrMakerTrend round-trip cost rate must be non-negative and below one"
        );
        self.round_trip_cost_rate = Some(cost_rate);
        Ok(())
    }

    pub(super) fn ready(&self) -> bool {
        self.last_close.is_some() && self.last_atr.is_some()
    }

    pub(super) fn entry_allowed(&self) -> bool {
        self.entry_allowed_without_offset() && self.current_atr_offset.is_some()
    }

    fn entry_allowed_without_offset(&self) -> bool {
        self.macro_trend_bullish && self.is_volatility_sufficient
    }

    pub(super) fn entry_price_from_bid(&self, bid: Decimal) -> Result<Option<Decimal>> {
        let Some(offset) = self.current_atr_offset else {
            return Ok(None);
        };
        ensure!(
            offset > Decimal::ZERO,
            "OkxEmaAtrMakerTrend entry offset must be positive"
        );
        let price = bid - offset;
        Ok((price > Decimal::ZERO).then_some(price))
    }

    fn clear_macro_signal(&mut self) {
        self.macro_trend_bullish = false;
        self.is_volatility_sufficient = false;
        self.current_atr_offset = None;
    }
}

pub(super) fn confirmed_bars_chronological(bars: &[MarketBar]) -> Vec<&MarketBar> {
    let mut confirmed_bars = bars.iter().filter(|bar| bar.confirm).collect::<Vec<_>>();
    confirmed_bars.sort_by_key(|bar| bar.ts_ms);
    confirmed_bars
}

impl Ema {
    fn new(period: usize) -> Self {
        Self {
            period,
            multiplier: 2.0 / (period as f64 + 1.0),
            samples: 0,
            seed_sum: 0.0,
            value: None,
        }
    }

    fn update(&mut self, close: f64) {
        self.samples += 1;
        if self.samples < self.period {
            self.seed_sum += close;
            return;
        }
        if self.samples == self.period {
            self.seed_sum += close;
            self.value = Some(self.seed_sum / self.period as f64);
            return;
        }
        let Some(previous) = self.value else {
            return;
        };
        self.value = Some((close - previous) * self.multiplier + previous);
    }

    fn value(&self) -> Option<f64> {
        self.value
    }
}

impl Atr {
    fn new(period: usize) -> Self {
        Self {
            period,
            samples: 0,
            seed_sum: 0.0,
            value: None,
            previous_close: None,
        }
    }

    fn update(&mut self, high: f64, low: f64, close: f64) {
        let true_range = match self.previous_close {
            Some(previous_close) => (high - low)
                .max((high - previous_close).abs())
                .max((low - previous_close).abs()),
            None => high - low,
        };
        self.previous_close = Some(close);
        self.samples += 1;
        if self.samples < self.period {
            self.seed_sum += true_range;
            return;
        }
        if self.samples == self.period {
            self.seed_sum += true_range;
            self.value = Some(self.seed_sum / self.period as f64);
            return;
        }
        let Some(previous) = self.value else {
            return;
        };
        self.value =
            Some((previous * (self.period as f64 - 1.0) + true_range) / self.period as f64);
    }

    fn value(&self) -> Option<f64> {
        self.value
    }
}

pub(super) fn entry_offset_distance(
    close: f64,
    atr: f64,
    entry_offset_atr_multiple: Decimal,
    min_entry_offset_bps: Decimal,
    max_entry_offset_bps: Decimal,
) -> Option<Decimal> {
    if !close.is_finite()
        || close <= 0.0
        || !close.is_normal()
        || !atr.is_finite()
        || atr <= 0.0
        || !atr.is_normal()
    {
        return None;
    }

    // This is the f64-to-Decimal order boundary for ATR-derived entry prices.
    // Downstream OKX price strings must be quantized from this Decimal value.
    let close = Decimal::from_f64(close)?;
    let atr = Decimal::from_f64(atr)?;
    let raw_offset = atr * entry_offset_atr_multiple;
    let floor_offset = close * min_entry_offset_bps / Decimal::new(10_000, 0);
    let ceiling_offset = close * max_entry_offset_bps / Decimal::new(10_000, 0);
    if floor_offset <= Decimal::ZERO || ceiling_offset < floor_offset {
        return None;
    }

    Some(raw_offset.max(floor_offset).min(ceiling_offset))
}

pub(super) fn volatility_clears_fee_threshold(
    close: f64,
    atr: f64,
    round_trip_cost_rate: Decimal,
) -> bool {
    if !close.is_finite()
        || close <= 0.0
        || !close.is_normal()
        || !atr.is_finite()
        || atr <= 0.0
        || !atr.is_normal()
    {
        return false;
    }

    let Some(close) = Decimal::from_f64(close) else {
        return false;
    };
    let Some(atr) = Decimal::from_f64(atr) else {
        return false;
    };
    if round_trip_cost_rate < Decimal::ZERO || round_trip_cost_rate >= Decimal::ONE {
        return false;
    }
    let minimum_volatility = Decimal::new(25, 1) * round_trip_cost_rate;
    atr / close >= minimum_volatility
}
