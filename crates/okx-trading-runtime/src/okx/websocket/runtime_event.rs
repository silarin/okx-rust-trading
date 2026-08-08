//! Bounded OKX WebSocket-to-runtime event delivery.

use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, bail};
use tokio::{
    sync::{mpsc, watch},
    time,
};

use okx_market_model::OkxLevel2FeatureSnapshot;

use crate::okx::latency::OkxLatencyMetrics;

#[cfg(not(test))]
const NON_LOSSY_EVENT_DELIVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);
#[cfg(test)]
const NON_LOSSY_EVENT_DELIVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(25);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OkxPublicRuntimeEventKind {
    ConfirmedCandle {
        instrument_id: String,
        bar_ts_ms: i64,
    },
    InstrumentUpdated {
        instrument_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxPublicRuntimeEvent {
    pub(crate) kind: OkxPublicRuntimeEventKind,
    pub(crate) received_at: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OkxPrivateRuntimeEventKind {
    Order {
        instrument_id: String,
        client_order_id: String,
    },
    Fill {
        instrument_id: String,
        client_order_id: String,
    },
    AlgoOrder {
        instrument_id: String,
    },
    Account,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxPrivateRuntimeEvent {
    pub(crate) kind: OkxPrivateRuntimeEventKind,
    pub(crate) received_at: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OkxLevel2RuntimeEvent {
    pub(crate) event_generation: u64,
    pub(crate) features: Arc<OkxLevel2FeatureSnapshot>,
}

#[derive(Clone, Debug)]
pub(crate) struct OkxRuntimeEventReporter {
    public: mpsc::Sender<OkxPublicRuntimeEvent>,
    private: mpsc::Sender<OkxPrivateRuntimeEvent>,
    level2: watch::Sender<Option<OkxLevel2RuntimeEvent>>,
    latency: OkxLatencyMetrics,
}

pub(crate) struct OkxRuntimeEventReceivers {
    pub(crate) public: mpsc::Receiver<OkxPublicRuntimeEvent>,
    pub(crate) private: mpsc::Receiver<OkxPrivateRuntimeEvent>,
    pub(crate) level2: watch::Receiver<Option<OkxLevel2RuntimeEvent>>,
}

impl OkxRuntimeEventReporter {
    pub(crate) fn channel(
        public_capacity: usize,
        private_capacity: usize,
        latency: OkxLatencyMetrics,
    ) -> (Self, OkxRuntimeEventReceivers) {
        let (public, public_receiver) = mpsc::channel(public_capacity);
        let (private, private_receiver) = mpsc::channel(private_capacity);
        let (level2, level2_receiver) = watch::channel(None);
        (
            Self {
                public,
                private,
                level2,
                latency,
            },
            OkxRuntimeEventReceivers {
                public: public_receiver,
                private: private_receiver,
                level2: level2_receiver,
            },
        )
    }

    pub(crate) async fn report_public(&self, event: OkxPublicRuntimeEvent) -> Result<()> {
        self.send_non_lossy(&self.public, event, "public market event")
            .await
    }

    pub(crate) async fn report_private(&self, event: OkxPrivateRuntimeEvent) -> Result<()> {
        self.send_non_lossy(&self.private, event, "private account/order event")
            .await
    }

    pub(crate) fn report_level2(&self, features: Arc<OkxLevel2FeatureSnapshot>) {
        let event_generation = self.latency.record_market_event_produced();
        let event = OkxLevel2RuntimeEvent {
            event_generation,
            features,
        };
        if self.level2.send(Some(event)).is_err() {
            self.latency.record_market_event_dropped();
        }
    }

    async fn send_non_lossy<T>(
        &self,
        sender: &mpsc::Sender<T>,
        event: T,
        context: &'static str,
    ) -> Result<()>
    where
        T: Send,
    {
        match time::timeout(NON_LOSSY_EVENT_DELIVERY_TIMEOUT, sender.send(event)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => bail!("bounded OKX {context} channel closed"),
            Err(_) => {
                self.latency.record_backpressure_incident();
                Err(anyhow::anyhow!(
                    "bounded OKX {context} delivery exceeded {} ms",
                    NON_LOSSY_EVENT_DELIVERY_TIMEOUT.as_millis()
                ))
                .with_context(
                    || "non-lossy OKX event delivery failed; runtime must reconcile or fail closed",
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rust_decimal::Decimal;

    use super::*;
    use okx_market_model::{OkxDepthMove, OkxDepthSlope, OkxNearBookDepth};
    use okx_public_protocol::OkxSpotInstrumentId;

    fn level2_features(generation: u64) -> Arc<OkxLevel2FeatureSnapshot> {
        let now = Instant::now();
        Arc::new(OkxLevel2FeatureSnapshot {
            instrument_id: OkxSpotInstrumentId::try_from("BTC-USDT")
                .expect("canonical test instrument"),
            epoch: 1,
            generation,
            sequence_id: generation as i64,
            exchange_ts_ms: 1_700_000_000_000,
            received_at: now,
            parsed_at: now,
            book_applied_at: now,
            features_ready_at: now,
            bid_level_count: 1,
            ask_level_count: 1,
            best_bid: Decimal::ONE,
            best_ask: Decimal::TWO,
            mid: Decimal::new(15, 1),
            spread: Decimal::ONE,
            classic_microprice: Decimal::new(15, 1),
            multi_level_microprice: Decimal::new(15, 1),
            microprice_displacement: Decimal::ZERO,
            imbalance_l1: Decimal::ZERO,
            imbalance_l3: Decimal::ZERO,
            imbalance_l5: Decimal::ZERO,
            imbalance_l10: Decimal::ZERO,
            imbalance_l25: Decimal::ZERO,
            imbalance_l50: Decimal::ZERO,
            imbalance_l100: Decimal::ZERO,
            imbalance_l200: Decimal::ZERO,
            imbalance_l400: Decimal::ZERO,
            distance_weighted_imbalance: Decimal::ZERO,
            notional_imbalance: Decimal::ZERO,
            near_book_depth: OkxNearBookDepth::default(),
            depth_move_1bps: OkxDepthMove::default(),
            depth_move_2bps: OkxDepthMove::default(),
            depth_move_5bps: OkxDepthMove::default(),
            depth_move_10bps: OkxDepthMove::default(),
            depth_slope: OkxDepthSlope::default(),
            liquidity_vacuum: Decimal::ZERO,
            imbalance_velocity_per_second: Decimal::ZERO,
            imbalance_persistence: Decimal::ZERO,
            short_horizon_book_volatility_bps: Decimal::ZERO,
        })
    }

    #[tokio::test]
    async fn private_delivery_is_bounded_and_backpressure_is_visible() {
        let latency = OkxLatencyMetrics::default();
        let (reporter, _receivers) = OkxRuntimeEventReporter::channel(1, 1, latency.clone());
        reporter
            .report_private(OkxPrivateRuntimeEvent {
                kind: OkxPrivateRuntimeEventKind::Account,
                received_at: Instant::now(),
            })
            .await
            .expect("first event");
        let error = reporter
            .report_private(OkxPrivateRuntimeEvent {
                kind: OkxPrivateRuntimeEventKind::Account,
                received_at: Instant::now(),
            })
            .await
            .expect_err("full queue must be bounded");
        assert!(error.to_string().contains("must reconcile or fail closed"));
        assert_eq!(latency.snapshot().counters.backpressure_incidents, 1);
    }

    #[tokio::test]
    async fn private_queue_is_independent_from_coalesced_market_updates() {
        let latency = OkxLatencyMetrics::default();
        let (reporter, mut receivers) = OkxRuntimeEventReporter::channel(1, 1, latency);
        reporter
            .report_private(OkxPrivateRuntimeEvent {
                kind: OkxPrivateRuntimeEventKind::Account,
                received_at: Instant::now(),
            })
            .await
            .expect("private event");
        let private = time::timeout(Duration::from_millis(10), receivers.private.recv())
            .await
            .expect("private event was not blocked")
            .expect("private channel open");
        assert_eq!(private.kind, OkxPrivateRuntimeEventKind::Account);
    }

    #[test]
    fn level2_coalescing_retains_newest_generation_and_counts_superseded_update() {
        let latency = OkxLatencyMetrics::default();
        let (reporter, mut receivers) = OkxRuntimeEventReporter::channel(1, 1, latency.clone());
        reporter.report_level2(level2_features(10));
        reporter.report_level2(level2_features(11));

        let latest = receivers
            .level2
            .borrow_and_update()
            .clone()
            .expect("latest Level-2 event");
        assert_eq!(latest.features.generation, 11);
        assert_eq!(latest.event_generation, 2);
        latency.record_market_event_dispatched(latest.event_generation);
        assert_eq!(latency.snapshot().counters.market_events_coalesced, 1);
    }
}
