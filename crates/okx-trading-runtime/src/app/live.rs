use std::{
    collections::{BTreeMap, HashMap, btree_map::Entry},
    future::{Future, pending},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use tokio::{
    sync::{mpsc, watch},
    time,
};
use tracing::{debug, info, warn};

#[cfg(test)]
use super::cancel_all_after_heartbeat::MAX_CANCEL_ALL_AFTER_POLL_INTERVAL_MS;
use super::cancel_all_after_heartbeat::{
    CancelAllAfterHeartbeat, cancel_all_after_timeout, next_cancel_all_after_heartbeat_failure,
};
use super::okx_startup_preflight::preflight_strategy_enabled_account;
use super::okx_stream_config::{
    build_market_stream_configs, build_private_stream_configs, required_okx_config,
};
#[cfg(test)]
use super::strategy_tick_execution::execute_strategy_ticks;
use super::strategy_tick_execution::{StrategyDispatch, execute_strategy_dispatch};
use super::strategy_tick_failure::StrategyTickFailureTracker;
use super::websocket_health_tracker::{WebsocketHealthTracker, websocket_task_lifecycle_error};

use crate::{
    config::{
        runtime::{masked_okx_account_id, masked_okx_api_key},
        types::{BotConfig, OkxTradingService, StrategyKind},
    },
    okx::latency::{OkxLatencyMetrics, OkxLatencyStage},
    okx::websocket::{
        OkxLevel2RuntimeEvent, OkxPrivateRuntimeEvent, OkxPrivateRuntimeEventKind,
        OkxPrivateStream, OkxPrivateStreamConfig, OkxPublicMarketStream,
        OkxPublicMarketStreamConfig, OkxPublicRuntimeEvent, OkxPublicRuntimeEventKind,
        OkxRuntimeEventReporter, OkxWebsocketHealthEvent, OkxWebsocketHealthEventKind,
        OkxWebsocketHealthReceiver, OkxWebsocketHealthReporter,
    },
    okx::{
        client::OkxCancelAllAfterTimeout,
        trading_client::{
            OkxAccountConfigObservationClient, OkxServerTimeRefresher, OkxTradingClient,
        },
        trading_instrument::ValidatedTradingInstrument,
        types::OkxAccountConfig,
    },
    strategies::okx_ema_atr_maker_trend::OkxEmaAtrMakerTrendRunner,
};

const WEBSOCKET_HEALTH_CHANNEL_CAPACITY: usize = 64;
const PUBLIC_RUNTIME_EVENT_CHANNEL_CAPACITY: usize = 64;
const PRIVATE_RUNTIME_EVENT_CHANNEL_CAPACITY: usize = 64;
const SAFETY_EVENT_CAA_ARM_ATTEMPT: &str = "caa_arm_attempt";
const SAFETY_EVENT_CAA_ARM_AMBIGUOUS: &str = "caa_arm_ambiguous";
const ACCOUNT_LEVEL_DIAGNOSTIC_OBSERVATION_INTERVAL: Duration = Duration::from_secs(15);

type AccountLevelDiagnosticObservation =
    Pin<Box<dyn Future<Output = Result<OkxAccountConfig>> + Send>>;

enum AccountLevelDiagnosticMonitorEvent {
    StartObservation,
    ObservationCompleted(Result<OkxAccountConfig>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeExitReason {
    OperatorShutdown,
    CancelAllAfterHeartbeatFailure,
    StrategyFailure,
    FatalRuntimeError,
}

impl RuntimeExitReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::OperatorShutdown => "operator_shutdown",
            Self::CancelAllAfterHeartbeatFailure => "cancel_all_after_heartbeat_failure",
            Self::StrategyFailure => "strategy_failure",
            Self::FatalRuntimeError => "fatal_runtime_error",
        }
    }
}

impl std::fmt::Display for RuntimeExitReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
enum RuntimeLoopDecision {
    Continue,
    OperatorShutdown,
    Fatal {
        reason: RuntimeExitReason,
        error: anyhow::Error,
    },
}

pub async fn run(config: BotConfig) -> Result<()> {
    let mut engine = build_trading_engine(config)?;
    engine.run().await
}

fn build_trading_engine(config: BotConfig) -> Result<TradingEngine> {
    let client = OkxTradingClient::from_config(&config)?;
    let latency = client.latency_metrics();
    let (runtime_event_reporter, runtime_events) = OkxRuntimeEventReporter::channel(
        PUBLIC_RUNTIME_EVENT_CHANNEL_CAPACITY,
        PRIVATE_RUNTIME_EVENT_CHANNEL_CAPACITY,
        latency.clone(),
    );
    client.configure_runtime_events(runtime_event_reporter);
    let has_enabled_strategies = config
        .strategies
        .instances
        .iter()
        .any(|instance| instance.enabled);
    #[cfg(not(test))]
    let strategies = Vec::new();
    #[cfg(test)]
    let strategies = build_strategies(&config)?;
    #[cfg(not(test))]
    let market_stream_configs = Vec::new();
    #[cfg(test)]
    let market_stream_configs = build_market_stream_configs(&config, !strategies.is_empty())?;
    #[cfg(not(test))]
    let private_stream_configs = Vec::new();
    #[cfg(test)]
    let private_stream_configs = build_private_stream_configs(&config, !strategies.is_empty())?;
    let websocket_health_tracker = WebsocketHealthTracker::new(
        market_stream_configs
            .iter()
            .map(OkxPublicMarketStreamConfig::health_identity)
            .chain(
                private_stream_configs
                    .iter()
                    .map(OkxPrivateStreamConfig::health_identity),
            ),
    );
    let (websocket_health_reporter, websocket_health_events) =
        OkxWebsocketHealthReporter::channel(WEBSOCKET_HEALTH_CHANNEL_CAPACITY);
    let cancel_all_after_timeout = if !has_enabled_strategies {
        None
    } else {
        Some(cancel_all_after_timeout(config.runtime.poll_interval_ms)?)
    };
    Ok(TradingEngine {
        config,
        client,
        strategies,
        cancel_all_after_timeout,
        cancel_all_after_armed: false,
        cancel_all_after_arm_attempted: false,
        cancel_all_after_heartbeat: None,
        cancel_all_after_heartbeat_failures: None,
        server_time_refresher: None,
        server_time_refresh_failures: None,
        market_stream_configs,
        market_streams: Vec::new(),
        private_stream_configs,
        private_streams: Vec::new(),
        websocket_health_reporter,
        websocket_health_events,
        websocket_health_tracker,
        websocket_reconcile_requested: false,
        pending_confirmed_candles: BTreeMap::new(),
        public_runtime_events: runtime_events.public,
        private_runtime_events: runtime_events.private,
        level2_runtime_events: runtime_events.level2,
        latency,
        latency_report_ticks: 0,
    })
}

fn build_validated_strategies(
    config: &BotConfig,
    validated_instruments: &HashMap<String, Arc<ValidatedTradingInstrument>>,
) -> Result<Vec<OkxEmaAtrMakerTrendRunner>> {
    let mut strategies = Vec::new();
    for instance in config
        .strategies
        .instances
        .iter()
        .filter(|instance| instance.enabled)
    {
        match instance.kind {
            StrategyKind::OkxEmaAtrMakerTrend => {
                let validated = validated_instruments
                    .get(instance.instrument_id())
                    .with_context(|| {
                        format!(
                            "strategy {} is missing validated trading context for {}",
                            instance.id,
                            instance.instrument_id()
                        )
                    })?;
                strategies.push(OkxEmaAtrMakerTrendRunner::from_validated_instance(
                    config,
                    instance,
                    Arc::clone(validated),
                )?);
            }
        }
    }
    Ok(strategies)
}

#[cfg(test)]
fn build_strategies(config: &BotConfig) -> Result<Vec<OkxEmaAtrMakerTrendRunner>> {
    config
        .strategies
        .instances
        .iter()
        .filter(|instance| instance.enabled)
        .map(|instance| match instance.kind {
            StrategyKind::OkxEmaAtrMakerTrend => {
                OkxEmaAtrMakerTrendRunner::from_instance(config, instance)
            }
        })
        .collect()
}

struct TradingEngine {
    config: BotConfig,
    client: OkxTradingClient,
    strategies: Vec<OkxEmaAtrMakerTrendRunner>,
    cancel_all_after_timeout: Option<OkxCancelAllAfterTimeout>,
    cancel_all_after_armed: bool,
    cancel_all_after_arm_attempted: bool,
    cancel_all_after_heartbeat: Option<CancelAllAfterHeartbeat>,
    cancel_all_after_heartbeat_failures: Option<mpsc::Receiver<anyhow::Error>>,
    server_time_refresher: Option<OkxServerTimeRefresher>,
    server_time_refresh_failures: Option<mpsc::Receiver<anyhow::Error>>,
    market_stream_configs: Vec<OkxPublicMarketStreamConfig>,
    market_streams: Vec<OkxPublicMarketStream>,
    private_stream_configs: Vec<OkxPrivateStreamConfig>,
    private_streams: Vec<OkxPrivateStream>,
    websocket_health_reporter: OkxWebsocketHealthReporter,
    websocket_health_events: OkxWebsocketHealthReceiver,
    websocket_health_tracker: WebsocketHealthTracker,
    websocket_reconcile_requested: bool,
    pending_confirmed_candles: BTreeMap<String, OkxPublicRuntimeEvent>,
    public_runtime_events: mpsc::Receiver<OkxPublicRuntimeEvent>,
    private_runtime_events: mpsc::Receiver<OkxPrivateRuntimeEvent>,
    level2_runtime_events: watch::Receiver<Option<OkxLevel2RuntimeEvent>>,
    latency: OkxLatencyMetrics,
    latency_report_ticks: u64,
}

async fn next_server_time_refresh_failure(
    failures: &mut Option<mpsc::Receiver<anyhow::Error>>,
) -> Option<anyhow::Error> {
    match failures {
        Some(failures) => failures.recv().await,
        None => pending().await,
    }
}

fn begin_account_level_diagnostic_observation(
    client: OkxAccountConfigObservationClient,
    timeout: Duration,
) -> AccountLevelDiagnosticObservation {
    Box::pin(async move {
        time::timeout(timeout, client.account_config())
            .await
            .context(
                "recurring OKX account-level diagnostic observation exceeded the configured strategy tick timeout",
            )?
    })
}

async fn next_account_level_diagnostic_monitor_event(
    interval: &mut Option<time::Interval>,
    observation: &mut Option<AccountLevelDiagnosticObservation>,
) -> AccountLevelDiagnosticMonitorEvent {
    if let Some(observation) = observation {
        return AccountLevelDiagnosticMonitorEvent::ObservationCompleted(observation.await);
    }
    match interval {
        Some(interval) => {
            interval.tick().await;
            AccountLevelDiagnosticMonitorEvent::StartObservation
        }
        None => pending().await,
    }
}

impl TradingEngine {
    async fn run(&mut self) -> Result<()> {
        if let Err(error) = self.startup().await {
            warn!(
                safety_event = "runtime_fatal_startup_exit",
                exit_reason = %RuntimeExitReason::FatalRuntimeError,
                error = %error,
                cancel_all_after_configured = self.cancel_all_after_timeout.is_some(),
                cancel_all_after_armed = self.cancel_all_after_armed,
                cancel_all_after_arm_attempted = self.cancel_all_after_arm_attempted,
                "fatal startup exit; routing through fatal runtime exit policy"
            );
            return self
                .fail_closed_for_fatal_error(
                    RuntimeExitReason::FatalRuntimeError,
                    error.context("fatal startup exit"),
                )
                .await;
        }

        self.run_loop().await
    }

    async fn run_loop(&mut self) -> Result<()> {
        let mut tick_failures = StrategyTickFailureTracker::new(self.strategies.len());
        let mut reconciliation_interval =
            time::interval(Duration::from_millis(self.config.runtime.poll_interval_ms));
        reconciliation_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut heartbeat_failures = self.cancel_all_after_heartbeat_failures.take();
        let mut server_time_failures = self.server_time_refresh_failures.take();
        let mut account_level_diagnostic_interval = (!self.strategies.is_empty()).then(|| {
            let first_observation =
                time::Instant::now() + ACCOUNT_LEVEL_DIAGNOSTIC_OBSERVATION_INTERVAL;
            let mut interval = time::interval_at(
                first_observation,
                ACCOUNT_LEVEL_DIAGNOSTIC_OBSERVATION_INTERVAL,
            );
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            interval
        });
        let account_config_observer =
            (!self.strategies.is_empty()).then(|| self.client.account_config_observation_client());
        let mut account_level_diagnostic_observation = None;
        loop {
            let pending_confirmed_candle_ready = self.websocket_strategy_dispatch_ready()
                && !self.pending_confirmed_candles.is_empty();
            let decision = tokio::select! {
                biased;
                failure = next_cancel_all_after_heartbeat_failure(&mut heartbeat_failures) => {
                    Self::cancel_all_after_heartbeat_failure_decision(failure)
                }
                failure = next_server_time_refresh_failure(&mut server_time_failures) => {
                    self.server_time_refresh_failure_decision(failure, &mut tick_failures).await
                }
                _ = tokio::signal::ctrl_c() => {
                    Ok(Self::operator_shutdown_decision())
                }
                event = next_account_level_diagnostic_monitor_event(
                    &mut account_level_diagnostic_interval,
                    &mut account_level_diagnostic_observation,
                ) => {
                    match event {
                        AccountLevelDiagnosticMonitorEvent::StartObservation => {
                            let observer = account_config_observer
                                .as_ref()
                                .context("strategy-enabled runtime is missing its account configuration observer")?
                                .clone();
                            account_level_diagnostic_observation = Some(
                                begin_account_level_diagnostic_observation(
                                    observer,
                                    Duration::from_millis(self.config.runtime.tick_timeout_ms),
                                ),
                            );
                            Ok(RuntimeLoopDecision::Continue)
                        }
                        AccountLevelDiagnosticMonitorEvent::ObservationCompleted(result) => {
                            account_level_diagnostic_observation = None;
                            self.account_level_diagnostic_observation_decision(result)
                        }
                    }
                }
                private_event = self.private_runtime_events.recv() => {
                    self.private_runtime_event_decision(private_event, &mut tick_failures).await
                }
                _ = reconciliation_interval.tick() => {
                    self.reconciliation_timer_decision(&mut tick_failures).await
                }
                websocket_health = self.websocket_health_events.recv() => {
                    self.websocket_health_decision(websocket_health, &mut tick_failures).await
                }
                _ = std::future::ready(()), if pending_confirmed_candle_ready => {
                    self.pending_confirmed_candle_decision(&mut tick_failures).await
                }
                public_event = self.public_runtime_events.recv() => {
                    self.public_runtime_event_decision(public_event, &mut tick_failures).await
                }
                changed = self.level2_runtime_events.changed() => {
                    self.level2_runtime_event_decision(changed)
                }
            }?;

            match decision {
                RuntimeLoopDecision::Continue => {}
                RuntimeLoopDecision::OperatorShutdown => {
                    return self.shutdown_for_operator().await;
                }
                RuntimeLoopDecision::Fatal { reason, error } => {
                    return self.fail_closed_for_fatal_error(reason, error).await;
                }
            }
        }
    }

    fn operator_shutdown_decision() -> RuntimeLoopDecision {
        info!(
            safety_event = "operator_ctrl_c_received",
            exit_reason = %RuntimeExitReason::OperatorShutdown,
            "received shutdown signal"
        );
        RuntimeLoopDecision::OperatorShutdown
    }

    fn cancel_all_after_heartbeat_failure_decision(
        failure: Option<anyhow::Error>,
    ) -> Result<RuntimeLoopDecision> {
        let error = failure
            .context("OKX Cancel-All-After heartbeat stopped without reporting a failure")?;
        Ok(RuntimeLoopDecision::Fatal {
            reason: RuntimeExitReason::CancelAllAfterHeartbeatFailure,
            error,
        })
    }

    fn account_level_diagnostic_observation_decision(
        &self,
        observation: Result<OkxAccountConfig>,
    ) -> Result<RuntimeLoopDecision> {
        match observation {
            Ok(account_config) => match self.validate_strategy_account_config(&account_config) {
                Ok(()) => Ok(RuntimeLoopDecision::Continue),
                Err(error) => Ok(RuntimeLoopDecision::Fatal {
                    reason: RuntimeExitReason::FatalRuntimeError,
                    error: error.context(
                        "recurring OKX account configuration no longer satisfies SPOT cash safety",
                    ),
                }),
            },
            Err(error) => Ok(RuntimeLoopDecision::Fatal {
                reason: RuntimeExitReason::FatalRuntimeError,
                error: error
                    .context("recurring OKX account-level diagnostic observation failed closed"),
            }),
        }
    }

    fn validate_strategy_account_config(&self, account_config: &OkxAccountConfig) -> Result<()> {
        account_config.ensure_spot_trading_enabled()?;
        let trading_service = self
            .config
            .okx
            .as_ref()
            .context("strategy-enabled account validation requires OKX configuration")?
            .trading_service;
        if trading_service == OkxTradingService::Production {
            account_config.validated_live_kyc_level()?;
        }
        Ok(())
    }

    async fn websocket_health_decision(
        &mut self,
        websocket_health: Option<OkxWebsocketHealthEvent>,
        tick_failures: &mut StrategyTickFailureTracker,
    ) -> Result<RuntimeLoopDecision> {
        let Some(event) = websocket_health else {
            return Ok(RuntimeLoopDecision::Fatal {
                reason: RuntimeExitReason::FatalRuntimeError,
                error: anyhow::anyhow!("OKX WebSocket health channel closed unexpectedly"),
            });
        };
        if let Some(error) = self.process_websocket_health_events(event) {
            return Ok(RuntimeLoopDecision::Fatal {
                reason: RuntimeExitReason::FatalRuntimeError,
                error,
            });
        }
        if self.websocket_reconcile_requested
            && self.websocket_health_tracker.all_mandatory_streams_ready()
        {
            return self.websocket_reconciliation_decision(tick_failures).await;
        }
        Ok(RuntimeLoopDecision::Continue)
    }

    async fn reconciliation_timer_decision(
        &mut self,
        tick_failures: &mut StrategyTickFailureTracker,
    ) -> Result<RuntimeLoopDecision> {
        self.latency_report_ticks = self.latency_report_ticks.saturating_add(1);
        if self.latency_report_ticks.is_multiple_of(60) {
            self.log_latency_snapshot();
        }
        if self.websocket_reconcile_requested
            && self.websocket_health_tracker.all_mandatory_streams_ready()
        {
            return self.websocket_reconciliation_decision(tick_failures).await;
        }
        self.strategy_dispatch_decision(
            tick_failures,
            StrategyDispatch::ReconcileTimer,
            Instant::now(),
        )
        .await
    }

    async fn server_time_refresh_failure_decision(
        &mut self,
        failure: Option<anyhow::Error>,
        tick_failures: &mut StrategyTickFailureTracker,
    ) -> Result<RuntimeLoopDecision> {
        let Some(error) = failure else {
            return Ok(RuntimeLoopDecision::Fatal {
                reason: RuntimeExitReason::FatalRuntimeError,
                error: anyhow::anyhow!(
                    "OKX server-time refresh failure channel closed unexpectedly"
                ),
            });
        };
        warn!(
            safety_event = "server_time_refresh_failure_reconcile",
            error = %error,
            "proactive server-time refresh failed; reconciling strategy state while lazy order-path refresh remains authoritative"
        );
        self.strategy_dispatch_decision(
            tick_failures,
            StrategyDispatch::StreamStateChanged,
            Instant::now(),
        )
        .await
    }

    async fn public_runtime_event_decision(
        &mut self,
        event: Option<OkxPublicRuntimeEvent>,
        tick_failures: &mut StrategyTickFailureTracker,
    ) -> Result<RuntimeLoopDecision> {
        let Some(event) = event else {
            return Ok(RuntimeLoopDecision::Fatal {
                reason: RuntimeExitReason::FatalRuntimeError,
                error: anyhow::anyhow!("OKX public runtime event channel closed unexpectedly"),
            });
        };
        if matches!(
            event.kind,
            OkxPublicRuntimeEventKind::ConfirmedCandle { .. }
        ) && !self.websocket_strategy_dispatch_ready()
        {
            self.defer_confirmed_candle(event);
            return Ok(RuntimeLoopDecision::Continue);
        }
        let dispatch = match &event.kind {
            OkxPublicRuntimeEventKind::ConfirmedCandle { instrument_id, .. } => {
                StrategyDispatch::ConfirmedCandle { instrument_id }
            }
            OkxPublicRuntimeEventKind::InstrumentUpdated { instrument_id } => {
                StrategyDispatch::InstrumentUpdated { instrument_id }
            }
        };
        self.strategy_dispatch_decision(tick_failures, dispatch, event.received_at)
            .await
    }

    fn defer_confirmed_candle(&mut self, event: OkxPublicRuntimeEvent) {
        let OkxPublicRuntimeEventKind::ConfirmedCandle {
            instrument_id,
            bar_ts_ms,
        } = &event.kind
        else {
            return;
        };
        if !self
            .strategies
            .iter()
            .any(|strategy| strategy.instrument_id() == instrument_id)
        {
            return;
        }
        match self.pending_confirmed_candles.entry(instrument_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(event);
            }
            Entry::Occupied(mut entry) => {
                let OkxPublicRuntimeEventKind::ConfirmedCandle {
                    bar_ts_ms: pending_bar_ts_ms,
                    ..
                } = &entry.get().kind
                else {
                    unreachable!("pending confirmed-candle map contains a different event kind");
                };
                if bar_ts_ms >= pending_bar_ts_ms {
                    entry.insert(event);
                }
            }
        }
    }

    fn take_ready_confirmed_candle(&mut self) -> Option<OkxPublicRuntimeEvent> {
        self.websocket_strategy_dispatch_ready()
            .then(|| self.pending_confirmed_candles.pop_first())
            .flatten()
            .map(|(_, event)| event)
    }

    async fn pending_confirmed_candle_decision(
        &mut self,
        tick_failures: &mut StrategyTickFailureTracker,
    ) -> Result<RuntimeLoopDecision> {
        let event = self
            .take_ready_confirmed_candle()
            .context("ready confirmed-candle dispatch was selected without a pending event")?;
        self.public_runtime_event_decision(Some(event), tick_failures)
            .await
    }

    async fn private_runtime_event_decision(
        &mut self,
        event: Option<OkxPrivateRuntimeEvent>,
        tick_failures: &mut StrategyTickFailureTracker,
    ) -> Result<RuntimeLoopDecision> {
        let Some(event) = event else {
            return Ok(RuntimeLoopDecision::Fatal {
                reason: RuntimeExitReason::FatalRuntimeError,
                error: anyhow::anyhow!("OKX private runtime event channel closed unexpectedly"),
            });
        };
        let instrument_id = match &event.kind {
            OkxPrivateRuntimeEventKind::Order { instrument_id, .. }
            | OkxPrivateRuntimeEventKind::Fill { instrument_id, .. }
            | OkxPrivateRuntimeEventKind::AlgoOrder { instrument_id } => {
                Some(instrument_id.as_str())
            }
            OkxPrivateRuntimeEventKind::Account => None,
        };
        self.strategy_dispatch_decision(
            tick_failures,
            StrategyDispatch::PrivateEvent { instrument_id },
            event.received_at,
        )
        .await
    }

    fn level2_runtime_event_decision(
        &mut self,
        changed: Result<(), watch::error::RecvError>,
    ) -> Result<RuntimeLoopDecision> {
        changed.context("OKX Level-2 runtime event channel closed unexpectedly")?;
        let event = self.level2_runtime_events.borrow_and_update().clone();
        let Some(event) = event else {
            return Ok(RuntimeLoopDecision::Continue);
        };
        self.latency
            .record_market_event_dispatched(event.event_generation);
        self.latency.record(
            OkxLatencyStage::FeaturesReadyToStrategyDispatch,
            event.features.features_ready_at.elapsed(),
        );
        let decision_started_at = Instant::now();
        self.observe_level2_shadow(&event);
        self.latency.record_elapsed(
            OkxLatencyStage::StrategyDispatchToDecisionComplete,
            decision_started_at,
        );
        Ok(RuntimeLoopDecision::Continue)
    }

    async fn strategy_dispatch_decision(
        &mut self,
        tick_failures: &mut StrategyTickFailureTracker,
        dispatch: StrategyDispatch<'_>,
        dispatch_ready_at: Instant,
    ) -> Result<RuntimeLoopDecision> {
        if !self.websocket_strategy_dispatch_ready() {
            return Ok(RuntimeLoopDecision::Continue);
        }
        let decision_started_at = Instant::now();
        let result = execute_strategy_dispatch(
            &mut self.strategies,
            &self.client,
            self.config.runtime.tick_timeout_ms,
            tick_failures,
            dispatch,
        )
        .await;
        self.latency.record(
            OkxLatencyStage::FeaturesReadyToStrategyDispatch,
            decision_started_at.saturating_duration_since(dispatch_ready_at),
        );
        self.latency.record_elapsed(
            OkxLatencyStage::StrategyDispatchToDecisionComplete,
            decision_started_at,
        );
        match result {
            Ok(None) => Ok(RuntimeLoopDecision::Continue),
            Ok(Some(error)) => Ok(RuntimeLoopDecision::Fatal {
                reason: RuntimeExitReason::StrategyFailure,
                error,
            }),
            Err(error) => Ok(RuntimeLoopDecision::Fatal {
                reason: RuntimeExitReason::FatalRuntimeError,
                error,
            }),
        }
    }

    fn websocket_strategy_dispatch_ready(&self) -> bool {
        self.websocket_health_tracker.all_mandatory_streams_ready()
            && !self.websocket_reconcile_requested
    }

    async fn websocket_reconciliation_decision(
        &mut self,
        tick_failures: &mut StrategyTickFailureTracker,
    ) -> Result<RuntimeLoopDecision> {
        self.websocket_reconcile_requested = false;
        let decision = self
            .strategy_dispatch_decision(
                tick_failures,
                StrategyDispatch::StreamStateChanged,
                Instant::now(),
            )
            .await?;
        if matches!(decision, RuntimeLoopDecision::Continue) && tick_failures.has_failures() {
            self.websocket_reconcile_requested = true;
        }
        Ok(decision)
    }

    fn observe_level2_shadow(&mut self, event: &OkxLevel2RuntimeEvent) {
        let features = &event.features;
        let max_staleness = Duration::from_millis(
            self.config
                .okx
                .as_ref()
                .map(|okx| okx.websocket.max_staleness_ms)
                .unwrap_or_default(),
        );
        if features.received_at.elapsed() > max_staleness {
            self.latency.record_stale_event_rejection();
            warn!(
                safety_event = "level2_shadow_stale_rejected",
                epoch = features.epoch,
                generation = features.generation,
                sequence_id = features.sequence_id,
                "rejected stale OKX Level-2 shadow feature snapshot"
            );
            return;
        }
        debug!(
            shadow_only = true,
            instrument_id = %features.instrument_id,
            epoch = features.epoch,
            generation = features.generation,
            sequence_id = features.sequence_id,
            exchange_ts_ms = features.exchange_ts_ms,
            feature_age_micros = features.received_at.elapsed().as_micros(),
            imbalance_l10 = %features.imbalance_l10,
            microprice_displacement = %features.microprice_displacement,
            "observed non-trading OKX Level-2 shadow features"
        );
    }

    fn log_latency_snapshot(&self) {
        let snapshot = self.latency.snapshot();
        for (stage, summary) in snapshot.stages {
            debug!(
                latency_stage = stage.as_str(),
                count = summary.count,
                min_micros = summary.min_micros,
                p50_micros = summary.p50_micros,
                p95_micros = summary.p95_micros,
                p99_micros = summary.p99_micros,
                max_micros = summary.max_micros,
                sample_window = summary.sample_window,
                "OKX bounded latency summary"
            );
        }
        let counters = snapshot.counters;
        info!(
            market_events_produced = counters.market_events_produced,
            market_events_dropped = counters.market_events_dropped,
            market_events_coalesced = counters.market_events_coalesced,
            backpressure_incidents = counters.backpressure_incidents,
            stale_event_rejections = counters.stale_event_rejections,
            sequence_gap_invalidations = counters.sequence_gap_invalidations,
            reconnects = counters.reconnects,
            command_fallbacks = counters.command_fallbacks,
            ambiguous_command_reconciliations = counters.ambiguous_command_reconciliations,
            "OKX event-driven runtime counters"
        );
    }

    async fn shutdown_for_operator(&mut self) -> Result<()> {
        let exit_reason = RuntimeExitReason::OperatorShutdown;
        info!(
            safety_event = "operator_shutdown_start",
            exit_reason = %exit_reason,
            strategy_count = self.strategies.len(),
            strategy_cleanup_attempted = true,
            "starting OKX strategy shutdown cleanup"
        );
        let mut shutdown_error = None;
        for (strategy_index, strategy) in self.strategies.iter_mut().enumerate() {
            if let Err(err) = strategy.shutdown(&self.client).await {
                warn!(
                    safety_event = "operator_shutdown_strategy_cleanup_failed",
                    strategy_index,
                    error = %err,
                    "strategy shutdown cleanup failed"
                );
                if shutdown_error.is_none() {
                    shutdown_error = Some(err);
                }
            }
        }
        match self.stop_server_time_refresher().await {
            Ok(refresher_stopped) => {
                info!(
                    server_time_refresher_stopped = refresher_stopped,
                    "stopped OKX server time refresher for operator shutdown"
                );
            }
            Err(err) => {
                warn!(
                    error = %err,
                    server_time_refresher_stopped = false,
                    "OKX server time refresher did not stop cleanly"
                );
                if shutdown_error.is_none() {
                    shutdown_error = Some(err);
                }
            }
        }
        match self.stop_cancel_all_after_heartbeat().await {
            Ok(heartbeat_stopped) => {
                info!(
                    safety_event = "operator_shutdown_heartbeat_stopped",
                    exit_reason = %exit_reason,
                    cancel_all_after_heartbeat_stopped = heartbeat_stopped,
                    "stopped OKX Cancel-All-After heartbeat for operator shutdown"
                );
            }
            Err(err) => {
                warn!(
                    safety_event = "operator_shutdown_heartbeat_stop_failed",
                    exit_reason = %exit_reason,
                    error = %err,
                    cancel_all_after_heartbeat_stopped = false,
                    "OKX Cancel-All-After heartbeat did not stop cleanly"
                );
                if shutdown_error.is_none() {
                    shutdown_error = Some(err);
                }
            }
        }
        if shutdown_error.is_none() {
            if let Err(err) = self.disarm_cancel_all_after().await {
                warn!(
                    safety_event = "operator_shutdown_caa_disarm_failed",
                    error = %err,
                    "OKX Cancel-All-After disarm failed during shutdown"
                );
                shutdown_error = Some(err);
            }
        } else {
            warn!(
                safety_event = "operator_shutdown_caa_disarm_skipped",
                exit_reason = %exit_reason,
                cancel_all_after_disarmed = false,
                cancel_all_after_armed = self.cancel_all_after_armed,
                cancel_all_after_arm_attempted = self.cancel_all_after_arm_attempted,
                cancel_all_after_left_armed = self.cancel_all_after_may_be_armed(),
                "skipping OKX Cancel-All-After disarm because strategy shutdown cleanup is ambiguous"
            );
        }
        if let Some(err) = shutdown_error {
            warn!(
                safety_event = "operator_shutdown_ambiguous",
                exit_reason = %exit_reason,
                cancel_all_after_disarmed = false,
                cancel_all_after_armed = self.cancel_all_after_armed,
                cancel_all_after_arm_attempted = self.cancel_all_after_arm_attempted,
                cancel_all_after_left_armed = self.cancel_all_after_may_be_armed(),
                error = %err,
                "operator shutdown cleanup finished with ambiguous state"
            );
            bail!("OKX strategy shutdown cleanup finished with ambiguous state: {err}");
        }
        info!(
            safety_event = "operator_shutdown_complete",
            exit_reason = %exit_reason,
            cancel_all_after_disarmed = self.cancel_all_after_timeout.is_some(),
            cancel_all_after_left_armed = false,
            "finished OKX strategy shutdown cleanup"
        );
        Ok(())
    }

    async fn fail_closed_for_fatal_error(
        &mut self,
        reason: RuntimeExitReason,
        error: anyhow::Error,
    ) -> Result<()> {
        let server_time_refresher_aborted = self.abort_server_time_refresher_for_fatal_exit();
        let heartbeat_aborted = self.abort_cancel_all_after_heartbeat_for_fatal_exit();
        let cancel_all_after_configured = self.cancel_all_after_timeout.is_some();
        let cancel_all_after_armed = self.cancel_all_after_armed;
        let cancel_all_after_arm_attempted = self.cancel_all_after_arm_attempted;
        let cancel_all_after_left_armed = self.cancel_all_after_may_be_armed();
        warn!(
            safety_event = "runtime_fatal_fail_closed",
            exit_reason = %reason,
            error = %error,
            strategy_cleanup_attempted = false,
            cancel_all_after_configured,
            cancel_all_after_armed,
            cancel_all_after_arm_attempted,
            cancel_all_after_heartbeat_aborted = heartbeat_aborted,
            server_time_refresher_aborted,
            cancel_all_after_disarmed = false,
            cancel_all_after_left_armed,
            "fatal runtime exit; applying OKX Cancel-All-After fail-closed policy"
        );
        if cancel_all_after_left_armed {
            warn!(
                safety_event = "runtime_fatal_caa_left_armed",
                exit_reason = %reason,
                "leaving OKX Cancel-All-After armed or possibly armed for exchange-side fail-closed protection"
            );
            bail!(
                "fatal runtime exit ({reason}); OKX Cancel-All-After left armed or possibly armed for fail-closed protection: {error:#}"
            );
        }
        bail!("fatal runtime exit ({reason}): {error:#}");
    }

    fn cancel_all_after_may_be_armed(&self) -> bool {
        self.cancel_all_after_armed || self.cancel_all_after_arm_attempted
    }

    async fn stop_cancel_all_after_heartbeat(&mut self) -> Result<bool> {
        let stopped = if let Some(heartbeat) = self.cancel_all_after_heartbeat.as_mut() {
            heartbeat.stop().await?;
            true
        } else {
            false
        };
        self.cancel_all_after_heartbeat = None;
        self.cancel_all_after_heartbeat_failures = None;
        Ok(stopped)
    }

    async fn stop_server_time_refresher(&mut self) -> Result<bool> {
        let stopped = if let Some(refresher) = self.server_time_refresher.as_mut() {
            refresher.stop().await?;
            true
        } else {
            false
        };
        self.server_time_refresher = None;
        self.server_time_refresh_failures = None;
        Ok(stopped)
    }

    fn abort_cancel_all_after_heartbeat_for_fatal_exit(&mut self) -> bool {
        if let Some(heartbeat) = self.cancel_all_after_heartbeat.as_mut() {
            heartbeat.abort();
            self.cancel_all_after_heartbeat = None;
            self.cancel_all_after_heartbeat_failures = None;
            return true;
        }
        false
    }

    fn abort_server_time_refresher_for_fatal_exit(&mut self) -> bool {
        if let Some(refresher) = self.server_time_refresher.as_mut() {
            refresher.abort();
            self.server_time_refresher = None;
            self.server_time_refresh_failures = None;
            return true;
        }
        false
    }

    async fn start_cancel_all_after_heartbeat(
        &mut self,
        timeout: OkxCancelAllAfterTimeout,
    ) -> Result<()> {
        ensure!(
            self.cancel_all_after_heartbeat.is_none(),
            "OKX Cancel-All-After heartbeat is already running"
        );
        let (heartbeat, failures) =
            CancelAllAfterHeartbeat::spawn(self.client.cancel_all_after_client(), timeout);
        self.cancel_all_after_heartbeat = Some(heartbeat);
        self.cancel_all_after_heartbeat_failures = Some(failures);
        Ok(())
    }

    fn start_server_time_refresher(&mut self) -> Result<()> {
        ensure!(
            self.server_time_refresher.is_none(),
            "OKX server time refresher is already running"
        );
        let (refresher, failures) = OkxServerTimeRefresher::spawn_with_failure_reporting(
            self.client.server_time_refresh_client(),
        );
        self.server_time_refresher = Some(refresher);
        self.server_time_refresh_failures = Some(failures);
        Ok(())
    }

    #[cfg(test)]
    async fn tick_once(
        &mut self,
        tick_failures: &mut StrategyTickFailureTracker,
    ) -> Result<Option<anyhow::Error>> {
        execute_strategy_ticks(
            &mut self.strategies,
            &self.client,
            self.config.runtime.tick_timeout_ms,
            tick_failures,
        )
        .await
    }

    async fn startup(&mut self) -> Result<()> {
        let okx = required_okx_config(&self.config)?;
        let trading_service = okx.trading_service;
        let configured_strategy_count = self
            .config
            .strategies
            .instances
            .iter()
            .filter(|instance| instance.enabled)
            .count();
        info!(
            safety_event = "runtime_startup_begin",
            base_url = %okx.base_url,
            okx_api_key = %masked_okx_api_key(&self.config),
            okx_account_id = %masked_okx_account_id(&self.config),
            strategy_count = configured_strategy_count,
            "starting OKX REST/WebSocket hybrid trading runtime"
        );

        let has_enabled_strategies = self.cancel_all_after_timeout.is_some();
        if !has_enabled_strategies {
            warn!("no enabled strategies configured; runtime will wait for shutdown");
        } else {
            self.start_strategy_enabled_order_path(trading_service)
                .await?;
        }

        self.start_market_streams();
        self.start_private_streams();

        for strategy_index in 0..self.strategies.len() {
            let account_config = begin_account_level_diagnostic_observation(
                self.client.account_config_observation_client(),
                Duration::from_millis(self.config.runtime.tick_timeout_ms),
            )
            .await
            .context(
                "fresh OKX account configuration is required before strategy initialization",
            )?;
            self.validate_strategy_account_config(&account_config)
                .context("OKX account configuration changed before strategy initialization")?;
            self.strategies[strategy_index]
                .initialize(&self.client)
                .await?;
        }

        Ok(())
    }

    async fn start_strategy_enabled_order_path(
        &mut self,
        trading_service: OkxTradingService,
    ) -> Result<()> {
        info!(
            safety_event = "runtime_order_intent_validated",
            order_intent = ?self.config.runtime.order_intent,
            strategy_count = self.config.strategies.instances.iter().filter(|instance| instance.enabled).count(),
            trading_service = ?trading_service,
            "validated strategy-enabled startup order intent"
        );

        // Account and fee preflight must finish before CAA, and CAA must be
        // armed before WebSocket streams or strategy recovery/initialization.
        let validated_instruments =
            preflight_strategy_enabled_account(&self.client, &self.config).await?;
        let validated_inst_type = validated_instruments
            .values()
            .next()
            .context("strategy-enabled startup produced no validated instrument contexts")?
            .inst_type()
            .as_okx();
        #[cfg(test)]
        let test_enable_market_streams = !self.market_stream_configs.is_empty();
        #[cfg(test)]
        let test_enable_private_streams = !self.private_stream_configs.is_empty();
        self.strategies = build_validated_strategies(&self.config, &validated_instruments)?;
        self.market_stream_configs =
            build_market_stream_configs(&self.config, !self.strategies.is_empty())?
                .into_iter()
                .map(|config| config.with_validated_instrument_type(validated_inst_type))
                .collect::<Result<Vec<_>>>()?;
        #[cfg(test)]
        if !test_enable_market_streams {
            self.market_stream_configs.clear();
        }
        self.private_stream_configs =
            build_private_stream_configs(&self.config, !self.strategies.is_empty())?
                .into_iter()
                .map(|config| config.with_validated_instrument_type(validated_inst_type))
                .collect::<Result<Vec<_>>>()?;
        #[cfg(test)]
        if !test_enable_private_streams {
            self.private_stream_configs.clear();
        }
        self.websocket_health_tracker = WebsocketHealthTracker::new(
            self.market_stream_configs
                .iter()
                .map(OkxPublicMarketStreamConfig::health_identity)
                .chain(
                    self.private_stream_configs
                        .iter()
                        .map(OkxPrivateStreamConfig::health_identity),
                ),
        );
        let timeout = self
            .cancel_all_after_timeout
            .context("strategy-enabled startup is missing OKX Cancel-All-After timeout")?;
        self.refresh_cancel_all_after(timeout).await?;
        self.start_cancel_all_after_heartbeat(timeout).await?;
        self.start_server_time_refresher()?;

        if !self.private_stream_configs.is_empty() {
            match self.client.prepare_order_command_path().await {
                Ok(()) => info!(
                    safety_event = "ws_order_command_prewarm_success",
                    "prepared OKX WebSocket order command path before strategy initialization"
                ),
                Err(err) => warn!(
                    safety_event = "ws_order_command_prewarm_unavailable",
                    error = %err,
                    "OKX WebSocket order command path unavailable at startup; strategy orders will use REST fallback until the command path is prepared"
                ),
            }
        }
        Ok(())
    }

    fn start_market_streams(&mut self) {
        for config in self.market_stream_configs.drain(..) {
            let stream_kind = if config.subscribe_tickers || config.subscribe_instruments {
                "public"
            } else {
                "business"
            };
            info!(
                instrument_count = config.instrument_ids.len(),
                stream_kind,
                subscribe_tickers = config.subscribe_tickers,
                subscribe_instruments = config.subscribe_instruments,
                candle_channel_count = config.candle_channels.len(),
                "starting OKX market WebSocket stream"
            );
            self.market_streams
                .push(OkxPublicMarketStream::spawn_with_health(
                    config,
                    self.client.market_data_cache(),
                    Some(self.websocket_health_reporter.clone()),
                ));
        }
    }

    fn start_private_streams(&mut self) {
        let login_timestamp_provider = self.client.websocket_login_timestamp_provider();
        for config in self.private_stream_configs.drain(..) {
            info!(
                instrument_count = config.instrument_ids.len(),
                stream_kind = ?config.kind,
                "starting OKX private WebSocket stream"
            );
            self.private_streams
                .push(OkxPrivateStream::spawn_with_health(
                    config,
                    self.client.private_event_cache(),
                    login_timestamp_provider.clone(),
                    Some(self.websocket_health_reporter.clone()),
                ));
        }
    }

    fn handle_websocket_health_event(&mut self, event: OkxWebsocketHealthEvent) {
        let stream = event.stream();
        if matches!(
            event.kind(),
            OkxWebsocketHealthEventKind::SubscriptionAckSucceeded
                | OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription
                | OkxWebsocketHealthEventKind::StreamFailedAfterSubscription
        ) {
            self.websocket_reconcile_requested = true;
        }
        match event.kind() {
            OkxWebsocketHealthEventKind::LoginFailed
            | OkxWebsocketHealthEventKind::SubscriptionAckFailed
            | OkxWebsocketHealthEventKind::ReconnectScheduled
            | OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription
            | OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription
            | OkxWebsocketHealthEventKind::StreamFailedAfterSubscription
            | OkxWebsocketHealthEventKind::StreamTaskPanicked
            | OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly => {
                warn!(
                    safety_event = "ws_health_event",
                    websocket_health_event = %event.kind(),
                    stream_kind = %stream.kind(),
                    channel_class = %stream.channel_class(),
                    instrument_count = stream.instrument_count(),
                    reconnect_attempt = event.reconnect_attempt(),
                    reconnect_backoff_ms = event.reconnect_backoff().map(|duration| duration.as_millis()),
                    "received OKX WebSocket health event"
                );
            }
            OkxWebsocketHealthEventKind::ConnectAttempt
            | OkxWebsocketHealthEventKind::Connected
            | OkxWebsocketHealthEventKind::LoginAckSucceeded
            | OkxWebsocketHealthEventKind::SubscriptionAckSucceeded => {
                info!(
                    safety_event = "ws_health_event",
                    websocket_health_event = %event.kind(),
                    stream_kind = %stream.kind(),
                    channel_class = %stream.channel_class(),
                    instrument_count = stream.instrument_count(),
                    reconnect_attempt = event.reconnect_attempt(),
                    reconnect_backoff_ms = event.reconnect_backoff().map(|duration| duration.as_millis()),
                    "received OKX WebSocket health event"
                );
            }
        }
        self.websocket_health_tracker.record(event);
    }

    fn process_websocket_health_events(
        &mut self,
        first_event: OkxWebsocketHealthEvent,
    ) -> Option<anyhow::Error> {
        // Drain the currently queued bounded health events before evaluating
        // fatal startup readiness. This lets queued subscription readiness
        // clear stale pre-ready failures without letting health polling starve
        // Ctrl-C, Cancel-All-After heartbeat failures, or strategy dispatch.
        let mut fatal_task_lifecycle_error = websocket_task_lifecycle_error(&first_event);
        self.handle_websocket_health_event(first_event);
        for _ in 0..WEBSOCKET_HEALTH_CHANNEL_CAPACITY {
            match self.websocket_health_events.try_recv() {
                Ok(event) => {
                    if fatal_task_lifecycle_error.is_none() {
                        fatal_task_lifecycle_error = websocket_task_lifecycle_error(&event);
                    }
                    self.handle_websocket_health_event(event);
                }
                Err(mpsc::error::TryRecvError::Empty)
                | Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        fatal_task_lifecycle_error
            .or_else(|| self.websocket_health_tracker.startup_readiness_error())
    }

    async fn refresh_cancel_all_after(&mut self, timeout: OkxCancelAllAfterTimeout) -> Result<()> {
        self.cancel_all_after_arm_attempted = true;
        info!(
            safety_event = SAFETY_EVENT_CAA_ARM_ATTEMPT,
            timeout_secs = timeout.seconds(),
            "arming OKX cancel-all-after dead-man switch"
        );
        let acknowledgement = match self.client.cancel_all_after(timeout).await {
            Ok(acknowledgement) => acknowledgement,
            Err(error) => {
                warn!(
                    safety_event = SAFETY_EVENT_CAA_ARM_AMBIGUOUS,
                    timeout_secs = timeout.seconds(),
                    cancel_all_after_arm_attempted = self.cancel_all_after_arm_attempted,
                    "OKX cancel-all-after arm outcome is ambiguous"
                );
                return Err(error);
            }
        };
        self.cancel_all_after_armed = true;
        info!(
            safety_event = "caa_arm_success",
            timeout_secs = timeout.seconds(),
            trigger_time = %acknowledgement.trigger_time,
            okx_timestamp = %acknowledgement.ts,
            "refreshed OKX cancel-all-after dead-man switch"
        );
        Ok(())
    }

    async fn disarm_cancel_all_after(&mut self) -> Result<()> {
        if self.cancel_all_after_timeout.is_none() {
            self.cancel_all_after_armed = false;
            self.cancel_all_after_arm_attempted = false;
            return Ok(());
        }
        let acknowledgement = self.client.disarm_cancel_all_after().await?;
        self.cancel_all_after_armed = false;
        self.cancel_all_after_arm_attempted = false;
        info!(
            safety_event = "caa_disarm_success",
            okx_timestamp = %acknowledgement.ts,
            "disarmed OKX cancel-all-after dead-man switch"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        path::Path,
        time::{Duration, Instant},
    };

    use anyhow::{Context, Result};
    use pretty_assertions::assert_eq;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        task::JoinHandle,
        time,
    };

    use crate::{
        config::{
            loader::{
                load_config_from_str_with_secret_resolver, load_config_path_with_secret_resolver,
            },
            types::{OkxTradingService, RuntimeOrderIntent},
        },
        okx::{
            client::{OKX_CANCEL_ALL_AFTER_TAG, OkxCancelAllAfterTimeout},
            trading_client::OkxServerTimeRefresher,
            websocket::{
                OkxPublicRuntimeEvent, OkxPublicRuntimeEventKind, OkxWebsocketChannelClass,
                OkxWebsocketHealthEvent, OkxWebsocketHealthEventKind, OkxWebsocketStreamIdentity,
                OkxWebsocketStreamKind,
            },
        },
        test_support::{CapturedLogs, HttpTestServer as TestServer},
    };

    use super::{
        CancelAllAfterHeartbeat, MAX_CANCEL_ALL_AFTER_POLL_INTERVAL_MS, RuntimeExitReason,
        RuntimeLoopDecision, SAFETY_EVENT_CAA_ARM_AMBIGUOUS, SAFETY_EVENT_CAA_ARM_ATTEMPT,
        StrategyTickFailureTracker, TradingEngine, WebsocketHealthTracker, build_strategies,
        build_trading_engine, cancel_all_after_timeout,
    };

    const TEST_HTTP_TIMEOUT: Duration = Duration::from_secs(1);
    const TEST_HTTP_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

    const DISABLED_STRATEGY_PROFILE: &str = r#"
[product]
name = "okx-rust-trading"

[runtime]
trader_id = "PUBLIC-DEMO-OPERATOR"
poll_interval_ms = 2000

[okx]
api_key = "${OKX_API_KEY}"
api_secret = "${OKX_API_SECRET}"
api_passphrase = "${OKX_API_PASSPHRASE}"
account_id = "OKX-PUBLIC-DEMO"
api_domain = "EEA"
account_jurisdiction = "EEA"
base_url = "https://eea.okx.com"
base_url_ws_public = "wss://wseea.okx.com:8443/ws/v5/public"
base_url_ws_private = "wss://wseea.okx.com:8443/ws/v5/private"
base_url_ws_business = "wss://wseea.okx.com:8443/ws/v5/business"
request_timeout_ms = 60000

[[instruments]]
instrument_id = "BTC-USDT"
base_currency = "BTC"
quote_currency = "USDT"
enabled = true

[[strategies.instances]]
kind = "okx_ema_atr_maker_trend"
id = "okx-ema-atr-maker-btc-usdt"
enabled = false
instrument = "BTC-USDT"
inst_type = "SPOT"
td_mode = "cash"
bar = "1m"

[strategies.instances.params]
fast_ema_period = 2
slow_ema_period = 5
atr_period = 3
quantity = "0.001"
max_quote_notional = "500"
entry_offset_atr_multiple = "0.1"
min_entry_offset_bps = "1.0"
max_entry_offset_bps = "15.0"
take_profit_atr_multiple = "1.5"
stop_loss_atr_multiple = "1.0"
"#;

    #[tokio::test]
    async fn trading_safety_matrix_operator_ctrl_c_shutdown_disarms_after_cleanup() -> Result<()> {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            cancel_all_after_ack_body("0", "4102444810123"),
        ])
        .await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;

        engine.shutdown_for_operator().await?;
        let requests = server.await_requests().await?;

        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "POST /api/v5/trade/cancel-all-after ");
        assert_request_json(
            &requests[1],
            serde_json::json!({
                "timeOut": "0",
                "tag": "okxrusttrading",
            }),
        );
        assert!(!engine.cancel_all_after_armed);
        Ok(())
    }

    #[tokio::test]
    async fn trading_safety_matrix_fatal_heartbeat_failure_leaves_cancel_all_after_armed()
    -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config("config/live.toml"))?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;

        let error = engine
            .fail_closed_for_fatal_error(
                RuntimeExitReason::CancelAllAfterHeartbeatFailure,
                anyhow::anyhow!("heartbeat refresh failed"),
            )
            .await
            .expect_err("fatal heartbeat failure should stop runtime");

        assert!(
            error
                .to_string()
                .contains("OKX Cancel-All-After left armed"),
            "fatal heartbeat exit should leave dead-man switch armed: {error}"
        );
        assert!(engine.cancel_all_after_heartbeat.is_none());
        assert!(engine.cancel_all_after_armed);
        Ok(())
    }

    #[test]
    fn runtime_loop_ctrl_c_decision_routes_to_operator_shutdown() {
        match TradingEngine::operator_shutdown_decision() {
            RuntimeLoopDecision::OperatorShutdown => {}
            decision => panic!("Ctrl-C should route to operator shutdown: {decision:?}"),
        }
    }

    #[test]
    fn runtime_loop_heartbeat_failure_decision_routes_to_fatal_fail_closed() -> Result<()> {
        let decision = TradingEngine::cancel_all_after_heartbeat_failure_decision(Some(
            anyhow::anyhow!("heartbeat failed"),
        ))?;

        match decision {
            RuntimeLoopDecision::Fatal { reason, error } => {
                assert_eq!(reason, RuntimeExitReason::CancelAllAfterHeartbeatFailure);
                assert_eq!(error.to_string(), "heartbeat failed");
            }
            decision => panic!("heartbeat failure should route to fatal fail-closed: {decision:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn trading_safety_matrix_strategy_failure_exit_leaves_cancel_all_after_armed()
    -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config("config/live.toml"))?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;

        let error = engine
            .fail_closed_for_fatal_error(
                RuntimeExitReason::StrategyFailure,
                anyhow::anyhow!("strategy index 0 failed 3 consecutive ticks"),
            )
            .await
            .expect_err("strategy failure threshold should stop runtime");

        assert!(
            error.to_string().contains("fatal runtime exit"),
            "strategy fatal exit should remain an error: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("OKX Cancel-All-After left armed"),
            "strategy fatal exit should preserve exchange fail-closed protection: {error}"
        );
        assert!(engine.cancel_all_after_armed);
        Ok(())
    }

    #[tokio::test]
    async fn trading_safety_matrix_unexpected_runtime_failure_leaves_cancel_all_after_armed()
    -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config("config/live.toml"))?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;

        let error = engine
            .fail_closed_for_fatal_error(
                RuntimeExitReason::FatalRuntimeError,
                anyhow::anyhow!("unexpected runtime failure"),
            )
            .await
            .expect_err("unexpected runtime failure should stop runtime");

        assert!(
            error.to_string().contains("fatal runtime exit"),
            "unexpected runtime failure should remain fatal: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("OKX Cancel-All-After left armed"),
            "unexpected fatal exit should preserve exchange fail-closed protection: {error}"
        );
        assert!(engine.cancel_all_after_armed);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_exit_policy_fatal_runtime_error_does_not_disarm_cancel_all_after() -> Result<()>
    {
        let mut engine = build_trading_engine(load_profile_config("config/live.toml"))?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;

        let error = engine
            .fail_closed_for_fatal_error(
                RuntimeExitReason::FatalRuntimeError,
                anyhow::anyhow!("fatal path"),
            )
            .await
            .expect_err("fatal runtime path should not disarm CAA");

        assert!(
            error.to_string().contains("fatal runtime exit"),
            "fatal runtime path should remain an error: {error}"
        );
        assert!(engine.cancel_all_after_armed);
        assert!(engine.cancel_all_after_heartbeat.is_none());
        Ok(())
    }

    #[test]
    fn build_strategies_skips_disabled_instances() {
        let config = load_config_from_str_with_secret_resolver(
            DISABLED_STRATEGY_PROFILE,
            test_secret_resolver,
        )
        .expect("disabled strategy profile should load");

        let strategies = build_strategies(&config).expect("strategy builder should succeed");

        assert_eq!(strategies.len(), 0);
    }

    #[test]
    fn trading_engine_builder_fails_closed_without_okx_config() {
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config.okx = None;

        expect_missing_okx_config(build_trading_engine(config), "trading engine builder");
    }

    #[test]
    fn checked_in_demo_profile_builds_hybrid_trading_engine() {
        let engine = build_trading_engine(load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        ))
        .expect("checked-in demo profile should build the hybrid runtime");

        assert_eq!(engine.strategies.len(), 1);
        assert_eq!(
            engine.cancel_all_after_timeout,
            Some(
                OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)
                    .expect("minimum OKX cancel-all-after timeout should be valid")
            )
        );
        assert!(!engine.cancel_all_after_armed);
    }

    #[test]
    fn checked_in_live_profile_builds_strategy_empty_hybrid_trading_engine() {
        let engine = build_trading_engine(load_profile_config("config/live.toml"))
            .expect("checked-in live profile should build the hybrid runtime");

        assert_eq!(engine.strategies.len(), 0);
        assert_eq!(engine.cancel_all_after_timeout, None);
    }

    #[test]
    fn strategy_empty_profile_skips_public_market_stream() -> Result<()> {
        let config = load_profile_config("config/live.toml");
        let engine = build_trading_engine(config)?;

        assert_eq!(engine.strategies.len(), 0);
        assert!(engine.market_stream_configs.is_empty());
        Ok(())
    }

    #[test]
    fn strategy_empty_profile_skips_private_streams() -> Result<()> {
        let config = load_profile_config("config/live.toml");
        let engine = build_trading_engine(config)?;

        assert_eq!(engine.strategies.len(), 0);
        assert!(engine.market_stream_configs.is_empty());
        assert!(engine.private_stream_configs.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn strategy_empty_startup_skips_order_capable_path() -> Result<()> {
        let server = TestServer::spawn(Vec::new()).await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;

        engine.startup().await?;
        let requests = server.await_requests().await?;

        assert_eq!(requests, Vec::<String>::new());
        assert!(!engine.cancel_all_after_armed);
        assert!(!engine.cancel_all_after_arm_attempted);
        assert!(engine.cancel_all_after_heartbeat.is_none());
        assert!(engine.server_time_refresher.is_none());
        assert!(engine.market_streams.is_empty());
        assert!(engine.private_streams.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn recurring_account_level_change_routes_runtime_to_fatal_exit() -> Result<()> {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
            account_config_body("2", "read_only,trade", /*auto_loan*/ false),
        ])
        .await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let engine = build_trading_engine(config)?;

        engine.client.account_config().await?;
        let observation = engine
            .client
            .account_config_observation_client()
            .account_config()
            .await;
        let decision = engine.account_level_diagnostic_observation_decision(observation)?;

        match decision {
            RuntimeLoopDecision::Fatal { reason, error } => {
                assert_eq!(reason, RuntimeExitReason::FatalRuntimeError);
                assert!(
                    format!("{error:#}").contains("account-level diagnostic changed"),
                    "unexpected recurring diagnostic failure: {error:#}"
                );
            }
            decision => panic!("diagnostic change should fail closed: {decision:?}"),
        }
        let requests = server.await_requests().await?;
        assert_eq!(requests.len(), 3);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "GET /api/v5/account/config ");
        assert_request_target(&requests[2], "GET /api/v5/account/config ");
        Ok(())
    }

    #[tokio::test]
    async fn recurring_production_account_check_reapplies_kyc_gate() -> Result<()> {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            account_config_body_with_kyc("1", "read_only,trade", /*auto_loan*/ false, "2"),
            account_config_body_with_kyc("1", "read_only,trade", /*auto_loan*/ false, "1"),
        ])
        .await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let engine = build_trading_engine(config)?;

        engine.client.account_config().await?;
        let observation = engine
            .client
            .account_config_observation_client()
            .account_config()
            .await;
        let decision = engine.account_level_diagnostic_observation_decision(observation)?;

        match decision {
            RuntimeLoopDecision::Fatal { reason, error } => {
                assert_eq!(reason, RuntimeExitReason::FatalRuntimeError);
                assert!(
                    format!("{error:#}").contains("kycLv 2 or 3"),
                    "unexpected recurring KYC failure: {error:#}"
                );
            }
            decision => panic!("KYC eligibility change should fail closed: {decision:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn observability_startup_safety_events_exclude_sensitive_material() -> Result<()> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.extend([
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            cancel_all_after_ack_body("4102444820123", "4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            candles_body([
                candle_json(1_000, "110"),
                candle_json(2_000, "108"),
                candle_json(3_000, "106"),
                candle_json(4_000, "104"),
                candle_json(5_000, "102"),
            ]),
            empty_okx_data_body(),
            empty_okx_data_body(),
            balance_body("BTC", "1"),
        ]);
        let server = TestServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        engine.market_stream_configs.clear();
        engine.private_stream_configs.clear();
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        engine.startup().await?;
        engine.stop_cancel_all_after_heartbeat().await?;
        let requests = server.await_requests().await?;
        let logs = logs.contents();

        assert!(logs.contains("runtime_startup_begin"));
        assert!(logs.contains("runtime_order_intent_validated"));
        assert!(logs.contains("runtime_account_preflight_ok"));
        assert!(logs.contains("runtime_trading_tuple_preflight_ok"));
        assert!(logs.contains("runtime_spot_fee_preflight_ok"));
        assert!(logs.contains("caa_arm_attempt"));
        assert!(logs.contains("caa_arm_success"));
        assert_logs_exclude_sensitive_material(&logs);
        assert_eq!(requests.len(), 17);
        Ok(())
    }

    #[tokio::test]
    async fn strategy_enabled_startup_keeps_running_when_websocket_order_prewarm_is_unavailable()
    -> Result<()> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.extend([
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            cancel_all_after_ack_body("4102444820123", "4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            candles_body([
                candle_json(1_000, "110"),
                candle_json(2_000, "108"),
                candle_json(3_000, "106"),
                candle_json(4_000, "104"),
                candle_json(5_000, "102"),
            ]),
            empty_okx_data_body(),
            empty_okx_data_body(),
            balance_body("BTC", "1"),
        ]);
        let server = TestServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        let okx = config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX");
        okx.base_url = format!("http://{}", server.addr());
        okx.base_url_ws_private = Some("ws://127.0.0.1:9/ws/v5/private".to_owned());
        okx.base_url_ws_public = Some("ws://127.0.0.1:9/ws/v5/public".to_owned());
        okx.base_url_ws_business = Some("ws://127.0.0.1:9/ws/v5/business".to_owned());
        let mut engine = build_trading_engine(config)?;
        engine.market_stream_configs.clear();
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        engine.startup().await?;
        engine.stop_cancel_all_after_heartbeat().await?;
        let requests = server.await_requests().await?;
        let logs = logs.contents();

        assert!(logs.contains("ws_order_command_prewarm_unavailable"));
        assert_eq!(requests.len(), 17);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[10], "POST /api/v5/trade/cancel-all-after ");
        assert_request_target(
            &requests[2],
            "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_all_after_ambiguous_failure_omits_raw_body_and_preserves_event_names()
    -> Result<()> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.extend([
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            "not-json".to_owned(),
        ]);
        let server = TestServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;

        let error = engine
            .run()
            .await
            .expect_err("cancel-all-after arm failure should stop runtime");
        let requests = server.await_requests().await?;
        let error = error.to_string();

        assert!(
            error.contains("fatal runtime exit"),
            "startup CAA arm failure should route through fatal runtime policy: {error}"
        );
        assert_eq!(SAFETY_EVENT_CAA_ARM_ATTEMPT, "caa_arm_attempt");
        assert_eq!(SAFETY_EVENT_CAA_ARM_AMBIGUOUS, "caa_arm_ambiguous");
        assert!(engine.cancel_all_after_arm_attempted);
        assert!(!engine.cancel_all_after_armed);
        assert!(
            !error.contains("not-json"),
            "startup CAA arm failure should not expose the raw OKX response body: {error}"
        );
        assert_eq!(requests.len(), 11);
        Ok(())
    }

    #[tokio::test]
    async fn hybrid_runtime_startup_and_single_tick_preserve_okx_rest_sequence() -> Result<()> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.extend([
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            cancel_all_after_ack_body("4102444820123", "4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            candles_body([
                candle_json(1_000, "110"),
                candle_json(2_000, "108"),
                candle_json(3_000, "106"),
                candle_json(4_000, "104"),
                candle_json(5_000, "102"),
            ]),
            empty_okx_data_body(),
            empty_okx_data_body(),
            balance_body("BTC", "1"),
            candles_body([
                candle_json(6_000, "101"),
                candle_json(7_000, "100"),
                candle_json(8_000, "99"),
            ]),
        ]);
        let server = TestServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        engine.market_stream_configs.clear();
        engine.private_stream_configs.clear();
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());

        engine.startup().await?;
        assert!(engine.cancel_all_after_heartbeat.is_some());
        assert!(engine.cancel_all_after_armed);
        assert!(engine.tick_once(&mut tick_failures).await?.is_none());
        engine.stop_cancel_all_after_heartbeat().await?;
        let requests = server.await_requests().await?;

        assert_eq!(requests.len(), 18);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "GET /api/v5/account/config ");
        assert_request_target(
            &requests[2],
            "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[3],
            "GET /api/v5/account/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[5],
            "GET /api/v5/market/index-tickers?instId=USDT-USD ",
        );
        assert_request_target(
            &requests[9],
            "GET /api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(&requests[10], "POST /api/v5/trade/cancel-all-after ");
        assert_request_json(
            &requests[10],
            serde_json::json!({
                "timeOut": OkxCancelAllAfterTimeout::MIN_SECONDS.to_string(),
                "tag": "okxrusttrading",
            }),
        );
        assert_request_target(&requests[11], "GET /api/v5/account/config ");
        assert_request_target(
            &requests[12],
            "GET /api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[13],
            "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=120 ",
        );
        assert_request_target(
            &requests[14],
            "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100 ",
        );
        assert_request_target(
            &requests[15],
            "GET /api/v5/trade/orders-algo-pending?instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100 ",
        );
        assert_request_target(&requests[16], "GET /api/v5/account/balance ");
        assert_request_target(
            &requests[17],
            "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=3 ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn trading_safety_matrix_strategy_initialization_failure_after_cancel_all_after_arm_leaves_it_armed()
    -> Result<()> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.extend([
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            cancel_all_after_ack_body("4102444820123", "4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
            okx_error_body("51000", "instrument unavailable"),
        ]);
        let server = TestServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        let okx = config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX");
        okx.base_url = format!("http://{}", server.addr());
        okx.base_url_ws_public = Some("ws://127.0.0.1:9/ws/v5/public".to_owned());
        okx.base_url_ws_private = Some("ws://127.0.0.1:9/ws/v5/private".to_owned());
        okx.base_url_ws_business = Some("ws://127.0.0.1:9/ws/v5/business".to_owned());
        let mut engine = build_trading_engine(config)?;

        let error = engine
            .run()
            .await
            .expect_err("strategy initialization failure should stop runtime");
        let requests = server.await_requests().await?;

        assert!(
            error.to_string().contains("fatal runtime exit"),
            "startup failure should route through fatal runtime policy: {error}"
        );
        assert!(
            error.to_string().contains("fatal startup exit"),
            "startup failure should be identified clearly: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("OKX Cancel-All-After left armed or possibly armed"),
            "startup failure after arming should preserve exchange fail-closed protection: {error}"
        );
        assert!(engine.cancel_all_after_armed);
        assert!(engine.cancel_all_after_arm_attempted);
        assert!(engine.cancel_all_after_heartbeat.is_none());
        assert!(engine.market_stream_configs.is_empty());
        assert!(engine.private_stream_configs.is_empty());
        assert_eq!(engine.market_streams.len(), 2);
        assert_eq!(engine.private_streams.len(), 2);
        assert_eq!(requests.len(), 13);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "GET /api/v5/account/config ");
        assert_request_target(
            &requests[2],
            "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[3],
            "GET /api/v5/account/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[9],
            "GET /api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(&requests[10], "POST /api/v5/trade/cancel-all-after ");
        assert_request_json(
            &requests[10],
            serde_json::json!({
                "timeOut": OkxCancelAllAfterTimeout::MIN_SECONDS.to_string(),
                "tag": "okxrusttrading",
            }),
        );
        assert_request_target(&requests[11], "GET /api/v5/account/config ");
        assert_request_target(
            &requests[12],
            "GET /api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT ",
        );
        assert_no_cancel_all_after_disarm_request(&requests);
        Ok(())
    }

    #[tokio::test]
    async fn account_change_before_strategy_initialization_fails_with_caa_armed() -> Result<()> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.extend([
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            cancel_all_after_ack_body("4102444820123", "4102444810123"),
            account_config_body("2", "read_only,trade", /*auto_loan*/ false),
        ]);
        let server = TestServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        let okx = config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX");
        okx.base_url = format!("http://{}", server.addr());
        okx.base_url_ws_public = Some("ws://127.0.0.1:9/ws/v5/public".to_owned());
        okx.base_url_ws_private = Some("ws://127.0.0.1:9/ws/v5/private".to_owned());
        okx.base_url_ws_business = Some("ws://127.0.0.1:9/ws/v5/business".to_owned());
        let mut engine = build_trading_engine(config)?;

        let error = engine
            .run()
            .await
            .expect_err("account change before strategy initialization should fail closed");
        let requests = server.await_requests().await?;

        assert!(
            format!("{error:#}").contains("account-level diagnostic changed"),
            "unexpected startup account change failure: {error:#}"
        );
        assert!(
            error
                .to_string()
                .contains("OKX Cancel-All-After left armed or possibly armed"),
            "startup account change should preserve exchange protection: {error}"
        );
        assert_eq!(requests.len(), 12);
        assert_request_target(&requests[10], "POST /api/v5/trade/cancel-all-after ");
        assert_request_target(&requests[11], "GET /api/v5/account/config ");
        assert_no_cancel_all_after_disarm_request(&requests);
        Ok(())
    }

    #[tokio::test]
    async fn trading_safety_matrix_account_preflight_failure_does_not_arm_cancel_all_after()
    -> Result<()> {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            okx_error_body("51000", "account config unavailable"),
        ])
        .await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;

        let error = engine
            .run()
            .await
            .expect_err("startup account preflight failure should stop runtime");
        let requests = server.await_requests().await?;

        assert!(
            error.to_string().contains("fatal runtime exit"),
            "startup account preflight failure should route through fatal runtime policy: {error}"
        );
        assert!(
            error.to_string().contains("fatal startup exit"),
            "startup account preflight failure should be identified clearly: {error}"
        );
        assert!(
            !error
                .to_string()
                .contains("OKX Cancel-All-After left armed"),
            "startup failure before CAA request must not claim CAA remains armed: {error}"
        );
        assert!(!engine.cancel_all_after_armed);
        assert!(!engine.cancel_all_after_arm_attempted);
        assert!(engine.cancel_all_after_heartbeat.is_none());
        assert!(engine.market_streams.is_empty());
        assert!(engine.private_streams.is_empty());
        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "GET /api/v5/account/config ");
        assert_no_cancel_all_after_request(&requests);
        Ok(())
    }

    #[tokio::test]
    async fn trading_safety_matrix_ineligible_production_kyc_does_not_arm_cancel_all_after()
    -> Result<()> {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            account_config_body_with_kyc("1", "read_only,trade", /*auto_loan*/ false, "1"),
        ])
        .await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        let okx = config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX");
        okx.trading_service = OkxTradingService::Production;
        okx.base_url = format!("http://{}", server.addr());
        config.runtime.order_intent = Some(RuntimeOrderIntent::LiveOkxSpotConfirmed);
        let mut engine = build_trading_engine(config)?;

        let error = engine
            .run()
            .await
            .expect_err("ineligible Production KYC should stop runtime before CAA");
        let requests = server.await_requests().await?;

        assert!(
            format!("{error:#}").contains("Production order placement requires OKX kycLv 2 or 3"),
            "Production KYC rejection should remain in the fatal startup chain: {error:#}"
        );
        assert!(!engine.cancel_all_after_armed);
        assert!(!engine.cancel_all_after_arm_attempted);
        assert!(engine.cancel_all_after_heartbeat.is_none());
        assert!(engine.market_streams.is_empty());
        assert!(engine.private_streams.is_empty());
        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "GET /api/v5/account/config ");
        assert_no_cancel_all_after_request(&requests);
        Ok(())
    }

    #[tokio::test]
    async fn trading_safety_matrix_fee_preflight_failure_does_not_arm_cancel_all_after()
    -> Result<()> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.push(okx_error_body("51000", "fee unavailable"));
        let server = TestServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;

        let error = engine
            .run()
            .await
            .expect_err("startup fee preflight failure should stop runtime");
        let requests = server.await_requests().await?;

        assert!(
            error.to_string().contains("fatal runtime exit"),
            "startup fee preflight failure should route through fatal runtime policy: {error}"
        );
        assert!(
            error.to_string().contains("fatal startup exit"),
            "startup fee preflight failure should be identified clearly: {error}"
        );
        assert!(
            !error
                .to_string()
                .contains("OKX Cancel-All-After left armed"),
            "startup fee failure before CAA request must not claim CAA remains armed: {error}"
        );
        assert!(!engine.cancel_all_after_armed);
        assert!(!engine.cancel_all_after_arm_attempted);
        assert!(engine.cancel_all_after_heartbeat.is_none());
        assert!(engine.market_streams.is_empty());
        assert!(engine.private_streams.is_empty());
        assert_eq!(requests.len(), 10);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "GET /api/v5/account/config ");
        assert_request_target(
            &requests[2],
            "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[3],
            "GET /api/v5/account/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[9],
            "GET /api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT ",
        );
        assert_no_cancel_all_after_request(&requests);
        Ok(())
    }

    #[tokio::test]
    async fn trading_safety_matrix_cancel_all_after_arm_failure_fails_closed() -> Result<()> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.extend([
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            "not-json".to_owned(),
        ]);
        let server = TestServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;

        let error = engine
            .run()
            .await
            .expect_err("cancel-all-after arm failure should stop runtime");
        let requests = server.await_requests().await?;

        assert!(
            error.to_string().contains("fatal runtime exit"),
            "startup CAA arm failure should route through fatal runtime policy: {error}"
        );
        assert!(
            error.to_string().contains("fatal startup exit"),
            "startup CAA arm failure should be identified clearly: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("OKX Cancel-All-After left armed or possibly armed"),
            "startup CAA arm failure should treat the POST outcome as ambiguous: {error}"
        );
        assert!(!engine.cancel_all_after_armed);
        assert!(engine.cancel_all_after_arm_attempted);
        assert!(engine.cancel_all_after_heartbeat.is_none());
        assert!(engine.market_streams.is_empty());
        assert!(engine.private_streams.is_empty());
        assert_eq!(requests.len(), 11);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "GET /api/v5/account/config ");
        assert_request_target(
            &requests[2],
            "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[3],
            "GET /api/v5/account/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[9],
            "GET /api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(&requests[10], "POST /api/v5/trade/cancel-all-after ");
        assert_no_cancel_all_after_disarm_request(&requests);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_exit_policy_startup_cancel_all_after_failure_stops_before_order_runtime()
    -> Result<()> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.extend([
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            "not-json".to_owned(),
        ]);
        let server = TestServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;

        let error = engine
            .run()
            .await
            .expect_err("startup CAA arm failure should stop runtime");
        let requests = server.await_requests().await?;

        assert!(
            error.to_string().contains("fatal startup exit"),
            "startup CAA arm failure should be reported before order-capable runtime starts: {error}"
        );
        assert!(engine.market_streams.is_empty());
        assert!(engine.private_streams.is_empty());
        assert!(engine.cancel_all_after_heartbeat.is_none());
        assert_eq!(requests.len(), 11);
        assert_request_target(&requests[10], "POST /api/v5/trade/cancel-all-after ");
        assert_no_cancel_all_after_disarm_request(&requests);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_exit_policy_operator_ctrl_c_uses_cleanup_and_disarm_path() -> Result<()> {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            cancel_all_after_ack_body("0", "4102444810123"),
        ])
        .await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;

        engine.shutdown_for_operator().await?;
        let requests = server.await_requests().await?;

        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[1], "POST /api/v5/trade/cancel-all-after ");
        assert_request_json(
            &requests[1],
            serde_json::json!({
                "timeOut": "0",
                "tag": "okxrusttrading",
            }),
        );
        assert!(!engine.cancel_all_after_armed);
        Ok(())
    }

    #[tokio::test]
    async fn observability_fatal_fail_closed_events_are_emitted() -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config("config/live.toml"))?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let error = engine
            .fail_closed_for_fatal_error(
                RuntimeExitReason::FatalRuntimeError,
                anyhow::anyhow!("fatal path"),
            )
            .await
            .expect_err("fatal runtime path should not disarm CAA");
        let logs = logs.contents();

        assert!(
            error.to_string().contains("fatal runtime exit"),
            "fatal runtime path should remain an error: {error}"
        );
        assert!(logs.contains("runtime_fatal_fail_closed"));
        assert!(logs.contains("runtime_fatal_caa_left_armed"));
        assert_logs_exclude_sensitive_material(&logs);
        Ok(())
    }

    #[tokio::test]
    async fn observability_operator_shutdown_events_are_emitted() -> Result<()> {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            cancel_all_after_ack_body("0", "4102444810123"),
        ])
        .await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        engine.shutdown_for_operator().await?;
        let requests = server.await_requests().await?;
        let logs = logs.contents();

        assert!(logs.contains("operator_shutdown_start"));
        assert!(logs.contains("operator_shutdown_heartbeat_stopped"));
        assert!(logs.contains("caa_disarm_success"));
        assert!(logs.contains("operator_shutdown_complete"));
        assert_logs_exclude_sensitive_material(&logs);
        assert_eq!(requests.len(), 2);
        Ok(())
    }

    #[test]
    fn cancel_all_after_timeout_uses_okx_minimum_for_fast_polling() -> Result<()> {
        assert_eq!(
            cancel_all_after_timeout(/*poll_interval_ms*/ 2_000)?,
            OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?
        );
        Ok(())
    }

    #[test]
    fn cancel_all_after_timeout_scales_with_poll_interval() -> Result<()> {
        assert_eq!(
            cancel_all_after_timeout(/*poll_interval_ms*/ 20_000)?,
            OkxCancelAllAfterTimeout::new(/*seconds*/ 60)?
        );
        Ok(())
    }

    #[test]
    fn cancel_all_after_timeout_respects_okx_maximum_refresh_margin() -> Result<()> {
        assert_eq!(
            cancel_all_after_timeout(MAX_CANCEL_ALL_AFTER_POLL_INTERVAL_MS)?,
            OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MAX_SECONDS)?
        );

        let error = cancel_all_after_timeout(MAX_CANCEL_ALL_AFTER_POLL_INTERVAL_MS + 1)
            .expect_err("poll interval should exceed the OKX cancel-all-after safety margin");

        assert!(
            error.to_string().contains("runtime.poll_interval_ms"),
            "rejected poll interval should be reported: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_all_after_heartbeat_stop_joins_task() -> Result<()> {
        let engine = build_trading_engine(load_profile_config("config/live.toml"))?;
        let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
        let (mut heartbeat, mut failures) = CancelAllAfterHeartbeat::spawn_with_timing(
            engine.client.cancel_all_after_client(),
            timeout,
            Duration::from_secs(60),
            Duration::from_secs(1),
        );
        let abort_handle = heartbeat
            .handle
            .as_ref()
            .expect("heartbeat task should be running")
            .abort_handle();

        time::timeout(Duration::from_millis(200), heartbeat.stop())
            .await
            .context("heartbeat stop did not join task promptly")??;

        assert!(abort_handle.is_finished());
        assert!(heartbeat.stop.is_none());
        assert!(heartbeat.handle.is_none());
        assert!(
            matches!(
                failures.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                    | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
            ),
            "clean heartbeat stop should not report a refresh failure"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancel_all_after_heartbeat_stop_interrupts_in_flight_refresh() -> Result<()> {
        let HangingCancelAllAfterPostServer {
            addr,
            post_received,
            release_post,
            requests,
        } = HangingCancelAllAfterPostServer::spawn().await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{addr}");
        let engine = build_trading_engine(config)?;
        let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
        let (mut heartbeat, mut failures) = CancelAllAfterHeartbeat::spawn_with_timing(
            engine.client.cancel_all_after_client(),
            timeout,
            Duration::from_millis(10),
            Duration::from_secs(5),
        );

        time::timeout(Duration::from_secs(1), post_received)
            .await
            .context("heartbeat refresh did not reach the in-flight Cancel-All-After POST")?
            .context("heartbeat server stopped before receiving Cancel-All-After POST")?;
        let stop_started = Instant::now();
        time::timeout(Duration::from_millis(200), heartbeat.stop())
            .await
            .context("heartbeat stop waited for the in-flight refresh deadline")??;

        assert!(
            stop_started.elapsed() < Duration::from_secs(1),
            "heartbeat stop should not wait for the full in-flight refresh deadline"
        );
        assert!(
            matches!(
                failures.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                    | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
            ),
            "operator stop should not report the interrupted refresh as a heartbeat failure"
        );
        let _ = release_post.send(());
        let requests = await_runtime_test_requests(requests).await?;

        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "POST /api/v5/trade/cancel-all-after ");
        Ok(())
    }

    #[tokio::test]
    async fn cancel_all_after_heartbeat_drop_aborts_remaining_task() -> Result<()> {
        let HangingCancelAllAfterPostServer {
            addr,
            post_received,
            release_post,
            requests,
        } = HangingCancelAllAfterPostServer::spawn().await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{addr}");
        let engine = build_trading_engine(config)?;
        let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
        let (heartbeat, mut failures) = CancelAllAfterHeartbeat::spawn_with_timing(
            engine.client.cancel_all_after_client(),
            timeout,
            Duration::from_millis(10),
            Duration::from_secs(5),
        );
        let abort_handle = heartbeat
            .handle
            .as_ref()
            .expect("heartbeat task should be running")
            .abort_handle();

        time::timeout(Duration::from_secs(1), post_received)
            .await
            .context("heartbeat refresh did not reach the in-flight Cancel-All-After POST")?
            .context("heartbeat server stopped before receiving Cancel-All-After POST")?;
        drop(heartbeat);
        for _ in 0..20 {
            if abort_handle.is_finished() {
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }

        assert!(abort_handle.is_finished());
        assert!(
            matches!(
                failures.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
            ),
            "dropping heartbeat should abort without reporting a refresh failure"
        );
        let _ = release_post.send(());
        let requests = await_runtime_test_requests(requests).await?;

        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "POST /api/v5/trade/cancel-all-after ");
        Ok(())
    }

    #[tokio::test]
    async fn cancel_all_after_heartbeat_reports_refresh_failure_and_stops() -> Result<()> {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            okx_error_body("50000", "heartbeat failed"),
        ])
        .await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let engine = build_trading_engine(config)?;
        let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
        let (mut heartbeat, mut failures) = CancelAllAfterHeartbeat::spawn_with_timing(
            engine.client.cancel_all_after_client(),
            timeout,
            Duration::from_millis(10),
            Duration::from_secs(1),
        );

        let error = time::timeout(Duration::from_secs(1), failures.recv())
            .await
            .context("heartbeat refresh error was not reported")?
            .context("heartbeat failure channel closed without an error")?;
        let no_second_failure = time::timeout(Duration::from_millis(200), failures.recv())
            .await
            .context("heartbeat did not stop after reporting the first refresh failure")?;
        heartbeat.stop().await?;
        let requests = server.await_requests().await?;

        assert!(
            error
                .to_string()
                .contains("OKX API error 50000: heartbeat failed"),
            "heartbeat refresh error should remain observable: {error}"
        );
        assert!(no_second_failure.is_none());
        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "POST /api/v5/trade/cancel-all-after ");
        Ok(())
    }

    #[tokio::test]
    async fn cancel_all_after_heartbeat_reports_refresh_timeout_and_stops() -> Result<()> {
        let HangingCancelAllAfterPostServer {
            addr,
            post_received,
            release_post,
            requests,
        } = HangingCancelAllAfterPostServer::spawn().await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{addr}");
        let engine = build_trading_engine(config)?;
        let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
        let (mut heartbeat, mut failures) = CancelAllAfterHeartbeat::spawn_with_timing(
            engine.client.cancel_all_after_client(),
            timeout,
            Duration::from_millis(10),
            Duration::from_millis(50),
        );

        time::timeout(Duration::from_secs(1), post_received)
            .await
            .context("heartbeat refresh did not reach the in-flight Cancel-All-After POST")?
            .context("heartbeat server stopped before receiving Cancel-All-After POST")?;
        let error = time::timeout(Duration::from_secs(1), failures.recv())
            .await
            .context("heartbeat refresh timeout was not reported")?
            .context("heartbeat failure channel closed without an error")?;
        let no_second_failure = time::timeout(Duration::from_millis(200), failures.recv())
            .await
            .context("heartbeat did not stop after reporting the first refresh timeout")?;
        heartbeat.stop().await?;
        let _ = release_post.send(());
        let requests = await_runtime_test_requests(requests).await?;

        assert!(
            error
                .to_string()
                .contains("OKX Cancel-All-After heartbeat refresh exceeded"),
            "heartbeat timeout should remain observable: {error}"
        );
        assert!(no_second_failure.is_none());
        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "POST /api/v5/trade/cancel-all-after ");
        Ok(())
    }

    #[tokio::test]
    async fn operator_shutdown_stops_server_time_refresher_during_in_flight_refresh() -> Result<()>
    {
        let HangingServerTimeServer {
            addr,
            request_received,
            release_response,
            requests,
        } = HangingServerTimeServer::spawn().await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{addr}");
        let mut engine = build_trading_engine(config)?;
        engine.server_time_refresher = Some(OkxServerTimeRefresher::spawn_with_timing(
            engine.client.server_time_refresh_client(),
            Duration::from_secs(60),
            Duration::from_secs(5),
        ));

        time::timeout(Duration::from_secs(1), request_received)
            .await
            .context("server time refresher did not start the in-flight refresh")?
            .context("server time server stopped before receiving refresh request")?;
        time::timeout(Duration::from_millis(200), engine.shutdown_for_operator())
            .await
            .context("operator shutdown waited for the in-flight server time refresh")??;
        let _ = release_response.send(());
        let requests = await_runtime_test_requests(requests).await?;

        assert!(engine.server_time_refresher.is_none());
        assert_eq!(requests.len(), 1);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        Ok(())
    }

    #[tokio::test]
    async fn fatal_runtime_exit_aborts_server_time_refresher_during_in_flight_refresh() -> Result<()>
    {
        let HangingServerTimeServer {
            addr,
            request_received,
            release_response,
            requests,
        } = HangingServerTimeServer::spawn().await?;
        let mut config = load_profile_config("config/live.toml");
        config
            .okx
            .as_mut()
            .expect("live profile should configure OKX")
            .base_url = format!("http://{addr}");
        let mut engine = build_trading_engine(config)?;
        engine.server_time_refresher = Some(OkxServerTimeRefresher::spawn_with_timing(
            engine.client.server_time_refresh_client(),
            Duration::from_secs(60),
            Duration::from_secs(5),
        ));

        time::timeout(Duration::from_secs(1), request_received)
            .await
            .context("server time refresher did not start the in-flight refresh")?
            .context("server time server stopped before receiving refresh request")?;
        let error = time::timeout(
            Duration::from_millis(200),
            engine.fail_closed_for_fatal_error(
                RuntimeExitReason::FatalRuntimeError,
                anyhow::anyhow!("fatal path"),
            ),
        )
        .await
        .context("fatal runtime exit waited for the in-flight server time refresh")?
        .expect_err("fatal runtime path should remain an error");
        let _ = release_response.send(());
        let requests = await_runtime_test_requests(requests).await?;

        assert!(
            error.to_string().contains("fatal runtime exit"),
            "fatal runtime path should remain fail closed: {error}"
        );
        assert!(engine.server_time_refresher.is_none());
        assert_eq!(requests.len(), 1);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        Ok(())
    }

    #[test]
    fn websocket_health_startup_policy_marks_all_expected_streams_mandatory() -> Result<()> {
        let engine = build_trading_engine(load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        ))?;

        assert_eq!(engine.websocket_health_tracker.expected_streams.len(), 4);
        assert_eq!(
            engine.websocket_health_tracker.mandatory_streams,
            engine.websocket_health_tracker.expected_streams
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_pending_mandatory_streams_gate_strategy_ticks() -> Result<()> {
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config.runtime.poll_interval_ms = 1;
        if let Some(okx) = config.okx.as_mut() {
            okx.base_url = "http://127.0.0.1:1".to_owned();
            okx.request_timeout_ms = 1;
        }
        let mut engine = build_trading_engine(config)?;
        let ready_stream = first_expected_websocket_stream(&engine);
        engine
            .websocket_health_reporter
            .report(OkxWebsocketHealthEvent::new(
                OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                ready_stream,
            ))
            .await;

        let outcome = time::timeout(Duration::from_millis(50), engine.run_loop()).await;

        assert!(
            outcome.is_err(),
            "runtime should keep waiting instead of ticking with pending mandatory stream ACKs"
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_reconciliation_request_stays_latched_while_stream_is_unready() -> Result<()>
    {
        let mut engine = build_trading_engine(load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        ))?;
        let stream = first_expected_websocket_stream(&engine);
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());

        assert!(matches!(
            engine
                .websocket_health_decision(
                    Some(OkxWebsocketHealthEvent::new(
                        OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                        stream,
                    )),
                    &mut tick_failures,
                )
                .await?,
            RuntimeLoopDecision::Continue
        ));
        assert!(engine.websocket_reconcile_requested);
        assert!(
            !engine
                .websocket_health_tracker
                .all_mandatory_streams_ready()
        );

        assert!(matches!(
            engine
                .websocket_health_decision(
                    Some(OkxWebsocketHealthEvent::reconnect_scheduled(
                        stream,
                        1,
                        Duration::from_millis(10),
                    )),
                    &mut tick_failures,
                )
                .await?,
            RuntimeLoopDecision::Continue
        ));
        assert!(
            engine.websocket_reconcile_requested,
            "readiness loss must not consume the pending REST reconciliation"
        );
        assert!(!engine.websocket_strategy_dispatch_ready());
        Ok(())
    }

    #[tokio::test]
    async fn confirmed_candle_before_readiness_is_retained_until_all_streams_are_ready()
    -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        ))?;
        let event = OkxPublicRuntimeEvent {
            kind: OkxPublicRuntimeEventKind::ConfirmedCandle {
                instrument_id: "BTC-USDT".to_owned(),
                bar_ts_ms: 1_700_000_000_000,
            },
            received_at: Instant::now(),
        };
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());

        assert!(matches!(
            engine
                .public_runtime_event_decision(Some(event.clone()), &mut tick_failures)
                .await?,
            RuntimeLoopDecision::Continue
        ));
        assert_eq!(
            engine.pending_confirmed_candles.get("BTC-USDT"),
            Some(&event)
        );
        assert!(engine.take_ready_confirmed_candle().is_none());

        let expected_streams = engine
            .websocket_health_tracker
            .expected_streams
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for stream in expected_streams {
            engine
                .websocket_health_tracker
                .record(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                    stream,
                ));
        }

        assert_eq!(engine.take_ready_confirmed_candle(), Some(event));
        assert!(engine.pending_confirmed_candles.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn pre_readiness_confirmed_candles_coalesce_to_latest_per_instrument() -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        ))?;
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());
        let older = OkxPublicRuntimeEvent {
            kind: OkxPublicRuntimeEventKind::ConfirmedCandle {
                instrument_id: "BTC-USDT".to_owned(),
                bar_ts_ms: 1_700_000_000_000,
            },
            received_at: Instant::now(),
        };
        let newer = OkxPublicRuntimeEvent {
            kind: OkxPublicRuntimeEventKind::ConfirmedCandle {
                instrument_id: "BTC-USDT".to_owned(),
                bar_ts_ms: 1_700_000_060_000,
            },
            received_at: Instant::now(),
        };

        engine
            .public_runtime_event_decision(Some(older), &mut tick_failures)
            .await?;
        engine
            .public_runtime_event_decision(Some(newer.clone()), &mut tick_failures)
            .await?;

        assert_eq!(engine.pending_confirmed_candles.len(), 1);
        assert_eq!(
            engine.pending_confirmed_candles.get("BTC-USDT"),
            Some(&newer)
        );
        Ok(())
    }

    #[tokio::test]
    async fn post_ready_disconnect_reconciles_before_dispatching_retained_candle() -> Result<()> {
        let mut responses = strategy_startup_script();
        responses.extend([
            RuntimeHttpResponse::ok(empty_okx_data_body()),
            RuntimeHttpResponse::ok(empty_okx_data_body()),
            RuntimeHttpResponse::ok(balance_body("BTC", "1")),
            RuntimeHttpResponse::ok(candles_body([
                candle_json(6_000, "101"),
                candle_json(7_000, "100"),
                candle_json(8_000, "99"),
            ])),
        ]);
        let server = ConcurrentRuntimeHttpServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        let expected_streams = engine
            .websocket_health_tracker
            .expected_streams
            .iter()
            .copied()
            .collect::<Vec<_>>();
        engine.market_stream_configs.clear();
        engine.private_stream_configs.clear();
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());

        engine.startup().await?;
        engine.stop_cancel_all_after_heartbeat().await?;
        engine.stop_server_time_refresher().await?;
        engine.websocket_health_tracker =
            WebsocketHealthTracker::new(expected_streams.iter().copied());
        for stream in &expected_streams {
            engine
                .websocket_health_tracker
                .record(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                    *stream,
                ));
        }
        assert!(engine.websocket_strategy_dispatch_ready());

        let disconnected_stream = expected_streams[0];
        engine.handle_websocket_health_event(OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription,
            disconnected_stream,
        ));
        assert!(!engine.websocket_strategy_dispatch_ready());
        assert!(engine.websocket_reconcile_requested);

        let candle = OkxPublicRuntimeEvent {
            kind: OkxPublicRuntimeEventKind::ConfirmedCandle {
                instrument_id: "BTC-USDT".to_owned(),
                bar_ts_ms: 1_700_000_000_000,
            },
            received_at: Instant::now(),
        };
        assert!(matches!(
            engine
                .public_runtime_event_decision(Some(candle.clone()), &mut tick_failures)
                .await?,
            RuntimeLoopDecision::Continue
        ));
        assert_eq!(
            engine.pending_confirmed_candles.get("BTC-USDT"),
            Some(&candle)
        );

        for kind in [
            OkxWebsocketHealthEventKind::ConnectAttempt,
            OkxWebsocketHealthEventKind::Connected,
            OkxWebsocketHealthEventKind::LoginAckSucceeded,
        ] {
            engine.handle_websocket_health_event(OkxWebsocketHealthEvent::new(
                kind,
                disconnected_stream,
            ));
            assert!(!engine.websocket_strategy_dispatch_ready());
        }
        engine.handle_websocket_health_event(OkxWebsocketHealthEvent::new(
            OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
            disconnected_stream,
        ));
        assert!(
            engine
                .websocket_health_tracker
                .all_mandatory_streams_ready()
        );
        assert!(
            !engine.websocket_strategy_dispatch_ready(),
            "subscription readiness must not bypass recovery reconciliation"
        );
        assert!(engine.take_ready_confirmed_candle().is_none());

        assert!(matches!(
            engine
                .websocket_reconciliation_decision(&mut tick_failures)
                .await?,
            RuntimeLoopDecision::Continue
        ));
        assert!(!engine.websocket_reconcile_requested);
        assert!(!tick_failures.has_failures());
        assert!(engine.websocket_strategy_dispatch_ready());
        assert!(matches!(
            engine
                .pending_confirmed_candle_decision(&mut tick_failures)
                .await?,
            RuntimeLoopDecision::Continue
        ));
        assert!(engine.pending_confirmed_candles.is_empty());

        let requests = server.await_requests().await?;
        assert_eq!(requests.len(), 21);
        assert_request_target(
            &requests[17],
            "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100 ",
        );
        assert_request_target(
            &requests[18],
            "GET /api/v5/trade/orders-algo-pending?instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100 ",
        );
        assert_request_target(&requests[19], "GET /api/v5/account/balance ");
        assert_request_target(
            &requests[20],
            "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=3 ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_post_ready_reconciliation_keeps_candle_gated_until_timer_retry_succeeds()
    -> Result<()> {
        let mut responses = strategy_startup_script();
        responses.extend([
            RuntimeHttpResponse::ok(okx_error_body("51000", "open orders failed")),
            RuntimeHttpResponse::ok(empty_okx_data_body()),
            RuntimeHttpResponse::ok(empty_okx_data_body()),
            RuntimeHttpResponse::ok(balance_body("BTC", "1")),
        ]);
        let server = ConcurrentRuntimeHttpServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        let expected_streams = engine
            .websocket_health_tracker
            .expected_streams
            .iter()
            .copied()
            .collect::<Vec<_>>();
        engine.market_stream_configs.clear();
        engine.private_stream_configs.clear();
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());

        engine.startup().await?;
        engine.stop_cancel_all_after_heartbeat().await?;
        engine.stop_server_time_refresher().await?;
        engine.websocket_health_tracker =
            WebsocketHealthTracker::new(expected_streams.iter().copied());
        for stream in expected_streams {
            engine
                .websocket_health_tracker
                .record(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                    stream,
                ));
        }
        engine.websocket_reconcile_requested = true;
        let candle = OkxPublicRuntimeEvent {
            kind: OkxPublicRuntimeEventKind::ConfirmedCandle {
                instrument_id: "BTC-USDT".to_owned(),
                bar_ts_ms: 1_700_000_000_000,
            },
            received_at: Instant::now(),
        };
        engine.defer_confirmed_candle(candle.clone());

        assert!(matches!(
            engine
                .websocket_reconciliation_decision(&mut tick_failures)
                .await?,
            RuntimeLoopDecision::Continue
        ));
        assert!(tick_failures.has_failures());
        assert!(
            engine.websocket_reconcile_requested,
            "non-terminal reconciliation failure must remain retryable"
        );
        assert!(!engine.websocket_strategy_dispatch_ready());
        assert!(engine.take_ready_confirmed_candle().is_none());
        assert_eq!(
            engine.pending_confirmed_candles.get("BTC-USDT"),
            Some(&candle)
        );

        assert!(matches!(
            engine
                .reconciliation_timer_decision(&mut tick_failures)
                .await?,
            RuntimeLoopDecision::Continue
        ));
        assert!(!tick_failures.has_failures());
        assert!(!engine.websocket_reconcile_requested);
        assert_eq!(engine.take_ready_confirmed_candle(), Some(candle));

        let requests = server.await_requests().await?;
        assert_eq!(requests.len(), 21);
        assert_request_target(
            &requests[17],
            "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100 ",
        );
        assert_request_target(
            &requests[18],
            "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100 ",
        );
        assert_request_target(
            &requests[19],
            "GET /api/v5/trade/orders-algo-pending?instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100 ",
        );
        assert_request_target(&requests[20], "GET /api/v5/account/balance ");
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_queued_readiness_clears_stale_pre_ready_failures() -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        ))?;
        let expected_streams = engine
            .websocket_health_tracker
            .expected_streams
            .iter()
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(expected_streams.len(), 4);

        for stream in &expected_streams {
            engine
                .websocket_health_reporter
                .report(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription,
                    *stream,
                ))
                .await;
        }
        for stream in &expected_streams {
            engine
                .websocket_health_reporter
                .report(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                    *stream,
                ))
                .await;
        }

        let first_event = engine
            .websocket_health_events
            .try_recv()
            .expect("queued WebSocket health event should be available");
        let fatal_error = engine.process_websocket_health_events(first_event);

        assert!(fatal_error.is_none());
        assert_eq!(engine.websocket_health_tracker.ready_streams.len(), 4);
        assert!(
            engine
                .websocket_health_tracker
                .failed_before_ready_streams
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_task_panic_routes_to_fatal_runtime_error() -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config("config/live.toml"))?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;
        engine
            .websocket_health_reporter
            .report(test_websocket_health_event(
                OkxWebsocketHealthEventKind::StreamTaskPanicked,
            ))
            .await;

        let error = time::timeout(Duration::from_millis(250), engine.run_loop())
            .await
            .context("runtime loop did not consume WebSocket task panic health event")?
            .expect_err("WebSocket task panic should fail closed");

        assert!(
            error.to_string().contains("stream task panicked"),
            "task panic should be surfaced in the fatal runtime error: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("OKX Cancel-All-After left armed or possibly armed"),
            "fatal WebSocket task panic should preserve CAA fail-closed protection: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_fatal_decision_routes_to_fatal_fail_closed() -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        ))?;
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());
        let decision = engine
            .websocket_health_decision(
                Some(test_websocket_health_event(
                    OkxWebsocketHealthEventKind::StreamTaskPanicked,
                )),
                &mut tick_failures,
            )
            .await?;

        match decision {
            RuntimeLoopDecision::Fatal { reason, error } => {
                assert_eq!(reason, RuntimeExitReason::FatalRuntimeError);
                assert!(
                    error.to_string().contains("stream task panicked"),
                    "fatal WebSocket health decision should preserve cause: {error}"
                );
            }
            decision => {
                panic!("fatal WebSocket health event should route to fail-closed: {decision:?}")
            }
        }
        Ok(())
    }

    #[test]
    fn websocket_health_unexpected_task_completion_is_fatal() -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        ))?;
        let error = engine
            .process_websocket_health_events(test_websocket_health_event(
                OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly,
            ))
            .expect("unexpected WebSocket task completion should be fatal");

        assert!(
            error
                .to_string()
                .contains("stream task exited unexpectedly"),
            "unexpected task completion should be explicit: {error}"
        );
        Ok(())
    }

    #[test]
    fn websocket_health_post_ready_transitions_request_reconciliation() -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        ))?;
        let stream = first_expected_websocket_stream(&engine);

        assert!(
            engine
                .process_websocket_health_events(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                    stream,
                ))
                .is_none()
        );
        assert!(engine.websocket_reconcile_requested);
        engine.websocket_reconcile_requested = false;
        assert!(
            engine
                .process_websocket_health_events(OkxWebsocketHealthEvent::reconnect_scheduled(
                    stream,
                    1,
                    Duration::from_millis(10),
                ))
                .is_none()
        );
        assert!(!engine.websocket_reconcile_requested);
        assert!(
            engine
                .process_websocket_health_events(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription,
                    stream,
                ))
                .is_none()
        );
        assert!(engine.websocket_reconcile_requested);
        engine.websocket_reconcile_requested = false;
        assert!(
            engine
                .process_websocket_health_events(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::StreamFailedAfterSubscription,
                    stream,
                ))
                .is_none()
        );
        assert!(engine.websocket_reconcile_requested);
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_events_do_not_starve_caa_heartbeat_failure() -> Result<()> {
        let mut engine = build_trading_engine(load_profile_config("config/live.toml"))?;
        engine.cancel_all_after_timeout = Some(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?);
        engine.cancel_all_after_armed = true;
        let (failure_tx, failure_rx) = tokio::sync::mpsc::channel(1);
        engine.cancel_all_after_heartbeat_failures = Some(failure_rx);
        engine
            .websocket_health_reporter
            .report(test_websocket_health_event(
                OkxWebsocketHealthEventKind::Connected,
            ))
            .await;
        let sender = tokio::spawn(async move {
            time::sleep(Duration::from_millis(10)).await;
            failure_tx.send(anyhow::anyhow!("heartbeat failed")).await
        });

        let error = time::timeout(Duration::from_millis(250), engine.run_loop())
            .await
            .context("runtime loop starved heartbeat failure behind WebSocket health events")?
            .expect_err("heartbeat failure should still stop the runtime");
        sender
            .await
            .expect("heartbeat sender task should not panic")
            .expect("heartbeat receiver should remain open");

        assert!(
            error.to_string().contains("heartbeat failed"),
            "heartbeat failure should remain visible: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn websocket_health_events_do_not_starve_strategy_tick_handling() -> Result<()> {
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config.runtime.poll_interval_ms = 1;
        if let Some(okx) = config.okx.as_mut() {
            okx.base_url = "http://127.0.0.1:1".to_owned();
            okx.request_timeout_ms = 1;
        }
        let mut engine = build_trading_engine(config)?;
        let expected_streams = engine
            .websocket_health_tracker
            .expected_streams
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for stream in expected_streams {
            engine
                .websocket_health_reporter
                .report(OkxWebsocketHealthEvent::new(
                    OkxWebsocketHealthEventKind::SubscriptionAckSucceeded,
                    stream,
                ))
                .await;
        }
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let error = time::timeout(Duration::from_millis(500), engine.run_loop())
            .await
            .context("runtime loop starved strategy ticks behind WebSocket health events")?
            .expect_err("strategy tick failures should still stop the runtime");
        let logs = logs.contents();

        assert!(
            error.to_string().contains("strategy index 0 failed"),
            "strategy tick failure should remain visible: {error}"
        );
        assert!(
            logs.contains("strategy_tick_failure"),
            "queued WebSocket health events should not starve strategy ticks: {logs}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn timed_out_tick_triggers_bounded_rest_reconciliation() -> Result<()> {
        let mut responses = strategy_startup_script();
        responses.extend([
            RuntimeHttpResponse::delayed_ok(
                candles_body([candle_json(6_000, "101")]),
                Duration::from_millis(100),
            ),
            RuntimeHttpResponse::ok(empty_okx_data_body()),
            RuntimeHttpResponse::ok(empty_okx_data_body()),
            RuntimeHttpResponse::ok(balance_body("BTC", "1")),
        ]);
        let server = ConcurrentRuntimeHttpServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config.runtime.tick_timeout_ms = 25;
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        engine.market_stream_configs.clear();
        engine.private_stream_configs.clear();
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());

        engine.startup().await?;
        assert!(engine.tick_once(&mut tick_failures).await?.is_none());
        engine.stop_cancel_all_after_heartbeat().await?;
        engine.stop_server_time_refresher().await?;
        let requests = server.await_requests().await?;

        assert_eq!(requests.len(), 21);
        assert_request_target(
            &requests[17],
            "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=3 ",
        );
        assert_request_target(
            &requests[18],
            "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100 ",
        );
        assert_request_target(
            &requests[19],
            "GET /api/v5/trade/orders-algo-pending?instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100 ",
        );
        assert_request_target(&requests[20], "GET /api/v5/account/balance ");
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_tick_reconciliation_failure_fails_closed() -> Result<()> {
        let mut responses = strategy_startup_script();
        responses.extend([
            RuntimeHttpResponse::delayed_ok(
                candles_body([candle_json(6_000, "101")]),
                Duration::from_millis(100),
            ),
            RuntimeHttpResponse::ok(okx_error_body("51000", "open orders failed")),
        ]);
        let server = ConcurrentRuntimeHttpServer::spawn(responses).await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config.runtime.tick_timeout_ms = 25;
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        engine.market_stream_configs.clear();
        engine.private_stream_configs.clear();
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());

        engine.startup().await?;
        let error = engine
            .tick_once(&mut tick_failures)
            .await
            .expect_err("interrupted tick reconciliation failure should fail closed");
        engine.stop_cancel_all_after_heartbeat().await?;
        engine.stop_server_time_refresher().await?;
        let requests = server.await_requests().await?;

        assert!(
            error.to_string().contains("REST reconciliation failed"),
            "reconciliation failure should identify the fail-closed path: {error}"
        );
        assert!(
            error.to_string().contains("open orders failed"),
            "reconciliation failure should preserve the OKX error cause: {error}"
        );
        assert_eq!(requests.len(), 19);
        assert_request_target(
            &requests[17],
            "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=3 ",
        );
        assert_request_target(
            &requests[18],
            "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100 ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_tick_reconciliation_timeout_fails_closed_and_logs() -> Result<()> {
        let server = TestServer::spawn_with_response_delay(
            vec![
                instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
                instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
            ],
            Duration::from_millis(25),
        )
        .await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config.runtime.tick_timeout_ms = 1;
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let mut engine = build_trading_engine(config)?;
        let mut tick_failures = StrategyTickFailureTracker::new(engine.strategies.len());
        let logs = CapturedLogs::default();
        let dispatch = logs.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let error = engine
            .tick_once(&mut tick_failures)
            .await
            .expect_err("interrupted tick reconciliation should remain bounded");
        let logs = logs.contents();

        assert!(
            error
                .to_string()
                .contains("bounded REST reconciliation also timed out"),
            "interrupted tick should fail closed when reconciliation also times out: {error}"
        );
        assert!(logs.contains("strategy_tick_timeout"));
        assert!(logs.contains("strategy_tick_failure"));
        assert_logs_exclude_sensitive_material(&logs);
        let _ = server.await_requests().await;
        Ok(())
    }

    fn test_websocket_health_event(kind: OkxWebsocketHealthEventKind) -> OkxWebsocketHealthEvent {
        OkxWebsocketHealthEvent::new(
            kind,
            OkxWebsocketStreamIdentity::new(
                OkxWebsocketStreamKind::Public,
                OkxWebsocketChannelClass::PublicMarketData,
                1,
            ),
        )
    }

    fn first_expected_websocket_stream(engine: &TradingEngine) -> OkxWebsocketStreamIdentity {
        *engine
            .websocket_health_tracker
            .expected_streams
            .iter()
            .next()
            .expect("strategy-enabled runtime should track WebSocket streams")
    }

    fn load_profile_config(path: &str) -> crate::config::types::BotConfig {
        let source = if path == "config/live.toml" {
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml"
        } else {
            path
        };
        let mut config =
            load_config_path_with_secret_resolver(Path::new(source), test_secret_resolver)
                .expect("test OKX profile should load");
        if path == "config/live.toml" {
            config.runtime.order_intent = None;
            config.strategies.instances.clear();
            let okx = config.okx.as_mut().expect("test profile configures OKX");
            okx.trading_service = OkxTradingService::Production;
            okx.base_url = "https://eea.okx.com".to_owned();
            okx.base_url_ws_public = Some("wss://wseea.okx.com:8443/ws/v5/public".to_owned());
            okx.base_url_ws_private = Some("wss://wseea.okx.com:8443/ws/v5/private".to_owned());
            okx.base_url_ws_business = Some("wss://wseea.okx.com:8443/ws/v5/business".to_owned());
        }
        config
    }

    fn strategy_startup_script() -> Vec<RuntimeHttpResponse> {
        let mut responses = vec![
            okx_server_time_body("4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        ];
        responses.extend(validated_tuple_preflight_bodies());
        responses.extend([
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            cancel_all_after_ack_body("4102444820123", "4102444810123"),
            account_config_body("1", "read_only,trade", /*auto_loan*/ false),
            trade_fee_body("SPOT", "-0.0008", "-0.001"),
            candles_body([
                candle_json(1_000, "110"),
                candle_json(2_000, "108"),
                candle_json(3_000, "106"),
                candle_json(4_000, "104"),
                candle_json(5_000, "102"),
            ]),
            empty_okx_data_body(),
            empty_okx_data_body(),
            balance_body("BTC", "1"),
        ]);
        responses.into_iter().map(RuntimeHttpResponse::ok).collect()
    }

    struct RuntimeHttpResponse {
        body: String,
        response_delay: Option<Duration>,
    }

    impl RuntimeHttpResponse {
        fn ok(body: String) -> Self {
            Self {
                body,
                response_delay: None,
            }
        }

        fn delayed_ok(body: String, response_delay: Duration) -> Self {
            Self {
                body,
                response_delay: Some(response_delay),
            }
        }
    }

    struct ConcurrentRuntimeHttpServer {
        addr: SocketAddr,
        requests: JoinHandle<Result<Vec<String>>>,
    }

    impl ConcurrentRuntimeHttpServer {
        async fn spawn(responses: Vec<RuntimeHttpResponse>) -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let requests = tokio::spawn(async move {
                let mut requests = Vec::new();
                let mut response_tasks: Vec<JoinHandle<Result<()>>> = Vec::new();
                for response in responses {
                    let mut stream = accept_test_http_connection(&listener).await?;
                    requests.push(read_test_http_request(&mut stream).await?);
                    response_tasks.push(tokio::spawn(async move {
                        if let Some(response_delay) = response.response_delay {
                            time::sleep(response_delay).await;
                            let _ = write_test_http_response(&mut stream, &response.body).await;
                        } else {
                            write_test_http_response(&mut stream, &response.body).await?;
                        }
                        Ok(())
                    }));
                }
                for response_task in response_tasks {
                    response_task
                        .await
                        .context("runtime scripted HTTP response task panicked")??;
                }
                Ok(requests)
            });

            Ok(Self { addr, requests })
        }

        const fn addr(&self) -> SocketAddr {
            self.addr
        }

        async fn await_requests(self) -> Result<Vec<String>> {
            await_runtime_test_requests(self.requests).await
        }
    }

    struct HangingCancelAllAfterPostServer {
        addr: SocketAddr,
        post_received: tokio::sync::oneshot::Receiver<()>,
        release_post: tokio::sync::oneshot::Sender<()>,
        requests: JoinHandle<Result<Vec<String>>>,
    }

    impl HangingCancelAllAfterPostServer {
        async fn spawn() -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let (post_received_tx, post_received) = tokio::sync::oneshot::channel();
            let (release_post, release_post_rx) = tokio::sync::oneshot::channel();
            let requests = tokio::spawn(async move {
                let mut requests = Vec::new();

                let mut server_time_stream = accept_test_http_connection(&listener).await?;
                requests.push(read_test_http_request(&mut server_time_stream).await?);
                write_test_http_response(
                    &mut server_time_stream,
                    &okx_server_time_body("4102444810123"),
                )
                .await?;

                let mut cancel_all_after_stream = accept_test_http_connection(&listener).await?;
                requests.push(read_test_http_request(&mut cancel_all_after_stream).await?);
                let _ = post_received_tx.send(());
                let _ = release_post_rx.await;

                Ok(requests)
            });

            Ok(Self {
                addr,
                post_received,
                release_post,
                requests,
            })
        }
    }

    struct HangingServerTimeServer {
        addr: SocketAddr,
        request_received: tokio::sync::oneshot::Receiver<()>,
        release_response: tokio::sync::oneshot::Sender<()>,
        requests: JoinHandle<Result<Vec<String>>>,
    }

    impl HangingServerTimeServer {
        async fn spawn() -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let (request_received_tx, request_received) = tokio::sync::oneshot::channel();
            let (release_response, release_response_rx) = tokio::sync::oneshot::channel();
            let requests = tokio::spawn(async move {
                let mut requests = Vec::new();

                let mut stream = accept_test_http_connection(&listener).await?;
                requests.push(read_test_http_request(&mut stream).await?);
                let _ = request_received_tx.send(());
                let _ = release_response_rx.await;

                Ok(requests)
            });

            Ok(Self {
                addr,
                request_received,
                release_response,
                requests,
            })
        }
    }

    async fn read_test_http_request(stream: &mut TcpStream) -> Result<String> {
        time::timeout(TEST_HTTP_TIMEOUT, read_test_http_request_inner(stream))
            .await
            .context("timed out reading runtime test HTTP request")?
    }

    async fn read_test_http_request_inner(stream: &mut TcpStream) -> Result<String> {
        let mut request = Vec::new();
        let mut header_end = None;
        loop {
            let mut buffer = [0; 1024];
            let bytes_read = stream.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..bytes_read]);
            if header_end.is_none() {
                header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|index| index + 4);
            }
            let Some(header_end) = header_end else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if request.len() >= header_end + content_length {
                break;
            }
        }
        Ok(String::from_utf8_lossy(&request).to_string())
    }

    async fn write_test_http_response(stream: &mut TcpStream, body: &str) -> Result<()> {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        time::timeout(TEST_HTTP_TIMEOUT, stream.write_all(response.as_bytes()))
            .await
            .context("timed out writing runtime test HTTP response")??;
        Ok(())
    }

    async fn accept_test_http_connection(listener: &TcpListener) -> Result<TcpStream> {
        let (stream, _) = time::timeout(TEST_HTTP_TIMEOUT, listener.accept())
            .await
            .context("timed out accepting runtime test HTTP connection")??;
        Ok(stream)
    }

    async fn await_runtime_test_requests(
        handle: JoinHandle<Result<Vec<String>>>,
    ) -> Result<Vec<String>> {
        let mut handle = handle;
        let join_result = match time::timeout(TEST_HTTP_JOIN_TIMEOUT, &mut handle).await {
            Ok(join_result) => join_result,
            Err(error) => {
                handle.abort();
                let _ = handle.await;
                return Err(error).context("timed out waiting for runtime test HTTP server task");
            }
        };
        join_result.context("runtime test HTTP server task panicked")?
    }

    fn expect_missing_okx_config<T>(result: Result<T>, context: &str) {
        let error = match result {
            Ok(_) => panic!("{context} should fail closed without OKX config"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains("OKX config is required")
                || message.contains("missing required [okx] config"),
            "{context} should report missing OKX config: {error}"
        );
    }

    fn test_secret_resolver(name: &str) -> Option<String> {
        match name {
            "OKX_API_KEY" => Some("demo-key".to_owned()),
            "OKX_API_SECRET" => Some("demo-secret".to_owned()),
            "OKX_API_PASSPHRASE" => Some("demo-passphrase".to_owned()),
            _ => None,
        }
    }

    fn okx_server_time_body(timestamp: &str) -> String {
        okx_data_body(&format!(r#"[{{"ts":"{timestamp}"}}]"#))
    }

    fn empty_okx_data_body() -> String {
        r#"{"code":"0","msg":"","data":[]}"#.to_owned()
    }

    fn balance_body(currency: &str, cash_balance: &str) -> String {
        okx_data_body(&format!(
            r#"[{{"details":[{{"ccy":"{currency}","availBal":"{cash_balance}","cashBal":"{cash_balance}","frozenBal":"0"}}]}}]"#
        ))
    }

    fn account_config_body(account_level: &str, permissions: &str, auto_loan: bool) -> String {
        account_config_body_with_kyc(account_level, permissions, auto_loan, "2")
    }

    fn account_config_body_with_kyc(
        account_level: &str,
        permissions: &str,
        auto_loan: bool,
        kyc_level: &str,
    ) -> String {
        okx_data_body(&format!(
            r#"[{{"uid":"1001","mainUid":"1001","acctLv":"{account_level}","perm":"{permissions}","autoLoan":{auto_loan},"enableSpotBorrow":false,"spotBorrowAutoRepay":false,"feeType":"0","kycLv":"{kyc_level}"}}]"#
        ))
    }

    fn trade_fee_body(inst_type: &str, maker: &str, taker: &str) -> String {
        okx_data_body(&format!(
            r#"[{{"instType":"{inst_type}","level":"Lv1","maker":"{maker}","taker":"{taker}","feeGroup":[{{"groupId":"12","maker":"{maker}","taker":"{taker}"}}],"ts":"1763979985847"}}]"#
        ))
    }

    fn cancel_all_after_ack_body(trigger_time: &str, ts: &str) -> String {
        okx_data_body(&format!(
            r#"[{{"triggerTime":"{trigger_time}","tag":"{OKX_CANCEL_ALL_AFTER_TAG}","ts":"{ts}"}}]"#
        ))
    }

    fn instrument_body(
        inst_id: &str,
        base_ccy: &str,
        quote_ccy: &str,
        tick_size: &str,
        lot_size: &str,
        min_size: &str,
    ) -> String {
        okx_data_body(&format!(
            r#"[{{"instType":"SPOT","instId":"{inst_id}","instIdCode":"123456","groupId":"12","state":"live","baseCcy":"{base_ccy}","quoteCcy":"{quote_ccy}","tradeQuoteCcyList":["{quote_ccy}"],"tickSz":"{tick_size}","lotSz":"{lot_size}","minSz":"{min_size}","maxLmtSz":"999","maxLmtAmt":"100000","maxMktSz":"100","maxMktAmt":"100000","maxTriggerSz":"999","initPxLmtPct":"0.05","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}}]"#
        ))
    }

    fn validated_tuple_preflight_bodies() -> Vec<String> {
        vec![
            instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
            instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
            okx_data_body(
                r#"[{"instType":"SPOT","instId":"BTC-USDT","last":"100000","lastSz":"0.001","askPx":"100001","askSz":"1","bidPx":"99999","bidSz":"1","open24h":"99000","high24h":"101000","low24h":"98000","volCcy24h":"1000000","vol24h":"10","sodUtc0":"99000","sodUtc8":"99500","ts":"4102444810123"}]"#,
            ),
            okx_data_body(r#"[{"instId":"USDT-USD","idxPx":"1","ts":"4102444810123"}]"#),
            okx_data_body(
                r#"[{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"0.001","maxSell":"100"}]"#,
            ),
            okx_data_body(r#"[{"instId":"BTC-USDT","availBuy":"100","availSell":"0.001"}]"#),
            okx_data_body(
                r#"[{"details":[{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"},{"ccy":"USDT","availBal":"100","cashBal":"100","frozenBal":"0"}]}]"#,
            ),
        ]
    }

    fn candles_body(candles: impl IntoIterator<Item = String>) -> String {
        let candles = candles.into_iter().collect::<Vec<_>>().join(",");
        okx_data_body(&format!("[{candles}]"))
    }

    fn candle_json(ts_ms: i64, close: &str) -> String {
        format!(r#"["{ts_ms}","100","120","95","{close}","1","1","1","1"]"#)
    }

    fn okx_data_body(data: &str) -> String {
        format!(r#"{{"code":"0","msg":"","data":{data}}}"#)
    }

    fn okx_error_body(code: &str, message: &str) -> String {
        format!(r#"{{"code":"{code}","msg":"{message}","data":[]}}"#)
    }

    fn assert_request_target(request: &str, expected_prefix: &str) {
        assert!(
            request.starts_with(expected_prefix),
            "request used unexpected target; expected prefix {expected_prefix:?}: {request}"
        );
    }

    fn assert_request_json(request: &str, expected: serde_json::Value) {
        let (_, body) = request
            .split_once("\r\n\r\n")
            .expect("request should include headers and body");
        let actual: serde_json::Value =
            serde_json::from_str(body).expect("request body should be JSON");
        assert_eq!(actual, expected);
    }

    fn assert_no_cancel_all_after_disarm_request(requests: &[String]) {
        assert!(
            requests
                .iter()
                .all(|request| !request.contains(r#""timeOut":"0""#)),
            "startup fatal exit must not disarm OKX Cancel-All-After: {requests:#?}"
        );
    }

    fn assert_no_cancel_all_after_request(requests: &[String]) {
        assert!(
            requests
                .iter()
                .all(|request| !request.contains("trade/cancel-all-after")),
            "startup account preflight failure must not arm OKX Cancel-All-After: {requests:#?}"
        );
    }

    fn assert_logs_exclude_sensitive_material(logs: &str) {
        for forbidden in [
            "demo-key",
            "demo-secret",
            "demo-passphrase",
            "OKX-PUBLIC-DEMO",
            "\"uid\":\"1001\"",
            "\"mainUid\":\"1001\"",
            "not-json",
        ] {
            assert!(
                !logs.contains(forbidden),
                "captured safety logs should not contain {forbidden:?}: {logs}"
            );
        }
    }
}
