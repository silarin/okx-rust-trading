//! Bounded monotonic latency telemetry for the direct OKX runtime.

use std::{
    array,
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use tracing::warn;

const LATENCY_STAGE_COUNT: usize = 8;
const LATENCY_SAMPLE_WINDOW: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OkxLatencyStage {
    FrameReceivedToParsed,
    ParsedToBookApplied,
    BookAppliedToFeaturesReady,
    FeaturesReadyToStrategyDispatch,
    StrategyDispatchToDecisionComplete,
    DecisionCompleteToCommandStart,
    CommandStartToAcknowledgement,
    AmbiguousCommandToRestReconciliation,
}

impl OkxLatencyStage {
    const ALL: [Self; LATENCY_STAGE_COUNT] = [
        Self::FrameReceivedToParsed,
        Self::ParsedToBookApplied,
        Self::BookAppliedToFeaturesReady,
        Self::FeaturesReadyToStrategyDispatch,
        Self::StrategyDispatchToDecisionComplete,
        Self::DecisionCompleteToCommandStart,
        Self::CommandStartToAcknowledgement,
        Self::AmbiguousCommandToRestReconciliation,
    ];

    const fn index(self) -> usize {
        match self {
            Self::FrameReceivedToParsed => 0,
            Self::ParsedToBookApplied => 1,
            Self::BookAppliedToFeaturesReady => 2,
            Self::FeaturesReadyToStrategyDispatch => 3,
            Self::StrategyDispatchToDecisionComplete => 4,
            Self::DecisionCompleteToCommandStart => 5,
            Self::CommandStartToAcknowledgement => 6,
            Self::AmbiguousCommandToRestReconciliation => 7,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FrameReceivedToParsed => "frame_received_to_parsed",
            Self::ParsedToBookApplied => "parsed_to_book_applied",
            Self::BookAppliedToFeaturesReady => "book_applied_to_features_ready",
            Self::FeaturesReadyToStrategyDispatch => "features_ready_to_strategy_dispatch",
            Self::StrategyDispatchToDecisionComplete => "strategy_dispatch_to_decision_complete",
            Self::DecisionCompleteToCommandStart => "decision_complete_to_command_start",
            Self::CommandStartToAcknowledgement => "command_start_to_acknowledgement",
            Self::AmbiguousCommandToRestReconciliation => {
                "ambiguous_command_to_rest_reconciliation"
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct OkxLatencyMetrics {
    samples: Arc<Mutex<LatencySamples>>,
    counters: Arc<LatencyCounters>,
}

impl OkxLatencyMetrics {
    pub fn record(&self, stage: OkxLatencyStage, duration: Duration) {
        let micros = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        lock_samples(&self.samples).stages[stage.index()].record(micros);
    }

    pub fn record_elapsed(&self, stage: OkxLatencyStage, started_at: Instant) {
        self.record(stage, started_at.elapsed());
    }

    pub(crate) fn record_market_event_produced(&self) -> u64 {
        self.counters
            .market_events_produced
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    pub(crate) fn record_market_event_dispatched(&self, generation: u64) {
        let previous = self
            .counters
            .last_market_generation_dispatched
            .swap(generation, Ordering::Relaxed);
        if generation > previous.saturating_add(1) {
            self.counters.market_events_coalesced.fetch_add(
                generation.saturating_sub(previous).saturating_sub(1),
                Ordering::Relaxed,
            );
        }
    }

    pub(crate) fn record_market_event_dropped(&self) {
        self.counters
            .market_events_dropped
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_backpressure_incident(&self) {
        self.counters
            .backpressure_incidents
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_stale_event_rejection(&self) {
        self.counters
            .stale_event_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_sequence_gap_invalidation(&self) {
        self.counters
            .sequence_gap_invalidations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_reconnect(&self) {
        self.counters.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_command_fallback(&self) {
        self.counters
            .command_fallbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_ambiguous_command_reconciliation(&self) {
        self.counters
            .ambiguous_command_reconciliations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> OkxLatencySnapshot {
        let samples = lock_samples(&self.samples);
        let stages = OkxLatencyStage::ALL
            .into_iter()
            .map(|stage| (stage, samples.stages[stage.index()].summary()))
            .collect();
        OkxLatencySnapshot {
            stages,
            counters: self.counters.snapshot(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxLatencySnapshot {
    pub stages: BTreeMap<OkxLatencyStage, OkxLatencySummary>,
    pub counters: OkxLatencyCountersSnapshot,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OkxLatencySummary {
    pub count: u64,
    pub min_micros: u64,
    pub p50_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
    pub sample_window: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OkxLatencyCountersSnapshot {
    pub market_events_produced: u64,
    pub market_events_dropped: u64,
    pub market_events_coalesced: u64,
    pub backpressure_incidents: u64,
    pub stale_event_rejections: u64,
    pub sequence_gap_invalidations: u64,
    pub reconnects: u64,
    pub command_fallbacks: u64,
    pub ambiguous_command_reconciliations: u64,
}

#[derive(Debug)]
struct LatencySamples {
    stages: [LatencyStageSamples; LATENCY_STAGE_COUNT],
}

impl Default for LatencySamples {
    fn default() -> Self {
        Self {
            stages: array::from_fn(|_| LatencyStageSamples::default()),
        }
    }
}

#[derive(Debug, Default)]
struct LatencyStageSamples {
    count: u64,
    min_micros: Option<u64>,
    max_micros: u64,
    recent_micros: VecDeque<u64>,
}

impl LatencyStageSamples {
    fn record(&mut self, micros: u64) {
        self.count = self.count.saturating_add(1);
        self.min_micros = Some(
            self.min_micros
                .map_or(micros, |current| current.min(micros)),
        );
        self.max_micros = self.max_micros.max(micros);
        if self.recent_micros.len() == LATENCY_SAMPLE_WINDOW {
            self.recent_micros.pop_front();
        }
        self.recent_micros.push_back(micros);
    }

    fn summary(&self) -> OkxLatencySummary {
        if self.recent_micros.is_empty() {
            return OkxLatencySummary::default();
        }
        let mut sorted = self.recent_micros.iter().copied().collect::<Vec<_>>();
        sorted.sort_unstable();
        OkxLatencySummary {
            count: self.count,
            min_micros: self.min_micros.unwrap_or_default(),
            p50_micros: percentile(&sorted, 50),
            p95_micros: percentile(&sorted, 95),
            p99_micros: percentile(&sorted, 99),
            max_micros: self.max_micros,
            sample_window: sorted.len(),
        }
    }
}

#[derive(Debug, Default)]
struct LatencyCounters {
    market_events_produced: AtomicU64,
    market_events_dropped: AtomicU64,
    market_events_coalesced: AtomicU64,
    last_market_generation_dispatched: AtomicU64,
    backpressure_incidents: AtomicU64,
    stale_event_rejections: AtomicU64,
    sequence_gap_invalidations: AtomicU64,
    reconnects: AtomicU64,
    command_fallbacks: AtomicU64,
    ambiguous_command_reconciliations: AtomicU64,
}

impl LatencyCounters {
    fn snapshot(&self) -> OkxLatencyCountersSnapshot {
        OkxLatencyCountersSnapshot {
            market_events_produced: self.market_events_produced.load(Ordering::Relaxed),
            market_events_dropped: self.market_events_dropped.load(Ordering::Relaxed),
            market_events_coalesced: self.market_events_coalesced.load(Ordering::Relaxed),
            backpressure_incidents: self.backpressure_incidents.load(Ordering::Relaxed),
            stale_event_rejections: self.stale_event_rejections.load(Ordering::Relaxed),
            sequence_gap_invalidations: self.sequence_gap_invalidations.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            command_fallbacks: self.command_fallbacks.load(Ordering::Relaxed),
            ambiguous_command_reconciliations: self
                .ambiguous_command_reconciliations
                .load(Ordering::Relaxed),
        }
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len().saturating_sub(1))]
}

fn lock_samples(mutex: &Mutex<LatencySamples>) -> MutexGuard<'_, LatencySamples> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                safety_event = "okx_latency_metrics_poisoned",
                "OKX latency telemetry mutex poisoned; resetting bounded samples"
            );
            let mut guard = poisoned.into_inner();
            *guard = LatencySamples::default();
            mutex.clear_poison();
            guard
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_latency_summary_reports_percentiles_and_all_time_extrema() {
        let metrics = OkxLatencyMetrics::default();
        for micros in 1..=100 {
            metrics.record(
                OkxLatencyStage::FrameReceivedToParsed,
                Duration::from_micros(micros),
            );
        }
        let summary = metrics.snapshot().stages[&OkxLatencyStage::FrameReceivedToParsed];
        assert_eq!(summary.count, 100);
        assert_eq!(summary.min_micros, 1);
        assert_eq!(summary.p50_micros, 50);
        assert_eq!(summary.p95_micros, 95);
        assert_eq!(summary.p99_micros, 99);
        assert_eq!(summary.max_micros, 100);
        assert_eq!(summary.sample_window, 100);
    }

    #[test]
    fn market_generation_gaps_count_coalesced_updates() {
        let metrics = OkxLatencyMetrics::default();
        for _ in 0..4 {
            metrics.record_market_event_produced();
        }
        metrics.record_market_event_dispatched(1);
        metrics.record_market_event_dispatched(4);
        assert_eq!(metrics.snapshot().counters.market_events_coalesced, 2);
    }
}
