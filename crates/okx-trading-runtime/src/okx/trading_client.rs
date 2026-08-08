use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tracing::{debug, warn};

use crate::{
    config::types::{BotConfig, RequestedTradingInstrument},
    okx::{
        capability::ValidatedCapabilityGeneration,
        client::{
            OKX_CANCEL_ALL_AFTER_TAG, OKX_SERVER_TIME_REFRESH_MARGIN, OkxCancelAllAfterAck,
            OkxCancelAllAfterTimeout, OkxOrderSubmitReconciliation,
        },
        latency::{OkxLatencyMetrics, OkxLatencyStage},
        trading_instrument::{ValidatedQuoteUsdRate, ValidatedTradingInstrument},
        types::{
            MarketBar, OkxAccountConfig, OkxAlgoOrder, OkxAlgoOrderAck, OkxBalance, OkxFill,
            OkxInstrument, OkxOrder, OkxOrderAck, OkxTicker, OkxTradeFeeRate, OrderKind, OrderSide,
        },
        websocket::{
            OkxMarketDataCache, OkxPrivateEventCache, OkxRuntimeEventReporter,
            trading::{OkxWebsocketAmendOrder, OkxWebsocketCancelOrder, OkxWebsocketPlaceOrder},
            trading_session::{
                DEFAULT_ACK_TIMEOUT, OkxWebsocketCommandError, OkxWebsocketTradingCommandConfig,
                OkxWebsocketTradingCommandCredentials, OkxWebsocketTradingCommandSession,
            },
        },
    },
};

use super::client::{OkxClient, OkxOrderAmend, OkxRestClient, OkxWebsocketLoginTimestampProvider};

const OKX_WEBSOCKET_PLACE_ORDER_REQUEST_PREFIX: char = 'p';
const OKX_WEBSOCKET_CANCEL_ORDER_REQUEST_PREFIX: char = 'c';
const OKX_WEBSOCKET_AMEND_ORDER_REQUEST_PREFIX: char = 'a';
const OKX_WEBSOCKET_REQUEST_NONCE_MASK: u64 = 0x0fff_ffff_ffff_ffff;
const OKX_WEBSOCKET_COMMAND_TICK_TIMEOUT_DIVISOR: u32 = 3;
const OKX_WEBSOCKET_COMMAND_MIN_ACK_TIMEOUT: Duration = Duration::from_millis(1);

pub(crate) struct OkxTradingClient {
    rest: OkxRestClient,
    websocket_order_commands: Option<OkxWebsocketOrderCommands>,
    websocket_inst_id_codes: Mutex<HashMap<String, u64>>,
    websocket_request_nonce: AtomicU64,
    latency: OkxLatencyMetrics,
    pending_order_decision_at: Mutex<Option<Instant>>,
}

#[derive(Clone)]
pub(crate) struct OkxCancelAllAfterClient {
    rest: OkxRestClient,
}

#[derive(Clone)]
pub(crate) struct OkxServerTimeRefreshClient {
    rest: OkxRestClient,
}

#[derive(Clone)]
pub(crate) struct OkxAccountConfigObservationClient {
    rest: OkxRestClient,
}

pub(crate) struct OkxServerTimeRefresher {
    stop: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

struct OkxWebsocketOrderCommands {
    config: OkxWebsocketTradingCommandConfig,
    session: AsyncMutex<Option<OkxWebsocketTradingCommandSession>>,
    available: AtomicBool,
    prepare_attempted: AtomicBool,
}

enum OkxWebsocketOrderCommandError {
    Unavailable(anyhow::Error),
    PreparationRejected(anyhow::Error),
    Ambiguous(anyhow::Error),
}

#[derive(Clone, Copy)]
struct OkxRegularOrderCommand<'a> {
    inst_id: &'a str,
    side: OrderSide,
    kind: OrderKind,
    size: &'a str,
    price: Option<&'a str>,
    client_order_id: &'a str,
}

impl OkxTradingClient {
    pub(crate) fn from_config(config: &BotConfig) -> Result<Self> {
        let rest = OkxRestClient::from_config(config)?;
        Ok(Self::new(rest, websocket_order_command_config(config)?))
    }

    pub(crate) fn new(
        rest: OkxRestClient,
        websocket_order_command_config: Option<OkxWebsocketTradingCommandConfig>,
    ) -> Self {
        let websocket_order_commands =
            websocket_order_command_config.map(|config| OkxWebsocketOrderCommands {
                config,
                session: AsyncMutex::new(None),
                available: AtomicBool::new(false),
                prepare_attempted: AtomicBool::new(false),
            });
        Self {
            rest,
            websocket_order_commands,
            websocket_inst_id_codes: Mutex::new(HashMap::new()),
            websocket_request_nonce: AtomicU64::new(1),
            latency: OkxLatencyMetrics::default(),
            pending_order_decision_at: Mutex::new(None),
        }
    }

    pub(crate) fn latency_metrics(&self) -> OkxLatencyMetrics {
        self.latency.clone()
    }

    pub(crate) fn configure_runtime_events(&self, reporter: OkxRuntimeEventReporter) {
        self.rest
            .market_data_cache()
            .configure_runtime_observers(reporter.clone(), self.latency.clone());
        self.rest
            .private_event_cache()
            .configure_runtime_observer(reporter);
    }

    fn record_order_decision(&self, decided_at: Instant) {
        if let Ok(mut pending) = self.pending_order_decision_at.lock() {
            *pending = Some(decided_at);
        }
    }

    fn record_command_start(&self, command_started_at: Instant) {
        if let Ok(mut pending) = self.pending_order_decision_at.lock()
            && let Some(decided_at) = pending.take()
        {
            self.latency.record(
                OkxLatencyStage::DecisionCompleteToCommandStart,
                command_started_at.saturating_duration_since(decided_at),
            );
        }
    }

    pub(crate) fn market_data_cache(&self) -> OkxMarketDataCache {
        self.rest.market_data_cache()
    }

    pub(crate) fn private_event_cache(&self) -> OkxPrivateEventCache {
        self.rest.private_event_cache()
    }

    pub(crate) fn cancel_all_after_client(&self) -> OkxCancelAllAfterClient {
        OkxCancelAllAfterClient {
            rest: self.rest.clone(),
        }
    }

    pub(crate) fn server_time_refresh_client(&self) -> OkxServerTimeRefreshClient {
        OkxServerTimeRefreshClient {
            rest: self.rest.clone(),
        }
    }

    pub(crate) fn account_config_observation_client(&self) -> OkxAccountConfigObservationClient {
        OkxAccountConfigObservationClient {
            rest: self.rest.clone(),
        }
    }

    pub(crate) fn websocket_login_timestamp_provider(&self) -> OkxWebsocketLoginTimestampProvider {
        self.rest.websocket_login_timestamp_provider()
    }

    pub(crate) async fn prepare_order_command_path(&self) -> Result<()> {
        let Some(commands) = &self.websocket_order_commands else {
            return Ok(());
        };
        commands.prepare_attempted.store(true, Ordering::Release);
        if let Err(err) = self.prepare_websocket_order_commands(commands).await {
            self.mark_websocket_order_commands_unavailable(commands)
                .await;
            return Err(err);
        }
        Ok(())
    }

    pub(crate) async fn instruments(&self, inst_id: &str) -> Result<OkxInstrument> {
        let instrument = if let Ok(validated) = self.rest.validated_trading_instrument(inst_id) {
            self.rest
                .instrument_for_type(validated.inst_type().as_okx(), inst_id)
                .await?
        } else {
            #[cfg(test)]
            {
                self.rest.instruments(inst_id).await?
            }
            #[cfg(not(test))]
            anyhow::bail!(
                "OKX trading tuple for {inst_id} was not validated before metadata refresh"
            );
        };
        if self.websocket_order_commands.is_some()
            && let Some(inst_id_code) = instrument.websocket_inst_id_code()?
        {
            self.websocket_inst_id_codes
                .lock()
                .map_err(|_| anyhow::anyhow!("OKX WebSocket instIdCode cache lock poisoned"))?
                .insert(instrument.inst_id.clone(), inst_id_code);
        }
        Ok(instrument)
    }

    pub(crate) async fn account_config(&self) -> Result<OkxAccountConfig> {
        self.rest.account_config().await
    }

    pub(crate) async fn validate_trading_instrument(
        &self,
        requested: &RequestedTradingInstrument,
        account_config: &OkxAccountConfig,
    ) -> Result<Arc<ValidatedCapabilityGeneration>> {
        let validated = self
            .rest
            .validate_trading_instrument(requested, account_config)
            .await?;
        if self.websocket_order_commands.is_some()
            && let Some(inst_id_code) = validated.inst_id_code()?
        {
            self.websocket_inst_id_codes
                .lock()
                .map_err(|_| anyhow::anyhow!("OKX WebSocket instIdCode cache lock poisoned"))?
                .insert(validated.inst_id().to_owned(), inst_id_code);
        }
        Ok(validated)
    }

    pub(crate) async fn cancel_all_after(
        &self,
        timeout: OkxCancelAllAfterTimeout,
    ) -> Result<OkxCancelAllAfterAck> {
        self.rest.cancel_all_after(timeout).await
    }

    pub(crate) async fn disarm_cancel_all_after(&self) -> Result<OkxCancelAllAfterAck> {
        self.rest
            .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
            .await
    }

    pub(crate) async fn place_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        kind: OrderKind,
        size: &str,
        price: Option<&str>,
        client_order_id: &str,
    ) -> Result<OkxOrderAck> {
        let command_started_at = Instant::now();
        self.record_command_start(command_started_at);
        let Some(commands) = &self.websocket_order_commands else {
            let result = self
                .rest
                .place_order(inst_id, side, kind, size, price, client_order_id)
                .await;
            self.latency.record_elapsed(
                OkxLatencyStage::CommandStartToAcknowledgement,
                command_started_at,
            );
            return result;
        };
        match kind {
            OrderKind::Market => {
                let result = self
                    .rest
                    .place_order(inst_id, side, kind, size, price, client_order_id)
                    .await;
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                return result;
            }
            OrderKind::Limit | OrderKind::PostOnly => {}
        }

        let command = OkxRegularOrderCommand {
            inst_id,
            side,
            kind,
            size,
            price,
            client_order_id,
        };
        match self.place_order_with_websocket(commands, command).await {
            Ok(acknowledgement) => {
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                Ok(acknowledgement)
            }
            Err(OkxWebsocketOrderCommandError::Unavailable(err)) => {
                self.latency.record_command_fallback();
                warn!(
                    instrument_id = command.inst_id,
                    client_order_id = command.client_order_id,
                    error = %err,
                    "OKX WebSocket order command unavailable; falling back to REST order submit"
                );
                let result = self
                    .rest
                    .place_order(
                        command.inst_id,
                        command.side,
                        command.kind,
                        command.size,
                        command.price,
                        command.client_order_id,
                    )
                    .await;
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                result
            }
            Err(OkxWebsocketOrderCommandError::PreparationRejected(err)) => {
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                Err(err).context(
                    "OKX WebSocket order preparation failed before command submission; refusing REST fallback",
                )
            }
            Err(OkxWebsocketOrderCommandError::Ambiguous(err)) => {
                self.latency.record_ambiguous_command_reconciliation();
                let reconciliation_started_at = Instant::now();
                warn!(
                    instrument_id = command.inst_id,
                    client_order_id = command.client_order_id,
                    error = %err,
                    "OKX WebSocket order command acknowledgement ambiguous; reconciling through REST"
                );
                let result = self
                    .rest
                    .reconcile_order_submit_failure(
                        OkxOrderSubmitReconciliation {
                            inst_id: command.inst_id,
                            side: command.side,
                            kind: command.kind,
                            size: command.size,
                            price: command.price,
                            client_order_id: command.client_order_id,
                        },
                        err,
                    )
                    .await;
                self.latency.record_elapsed(
                    OkxLatencyStage::AmbiguousCommandToRestReconciliation,
                    reconciliation_started_at,
                );
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                result
            }
        }
    }

    pub(crate) async fn cancel_order(&self, inst_id: &str, client_order_id: &str) -> Result<()> {
        let command_started_at = Instant::now();
        self.record_command_start(command_started_at);
        let Some(commands) = &self.websocket_order_commands else {
            let result = self.rest.cancel_order(inst_id, client_order_id).await;
            self.latency.record_elapsed(
                OkxLatencyStage::CommandStartToAcknowledgement,
                command_started_at,
            );
            return result;
        };

        match self
            .cancel_order_with_websocket(commands, inst_id, client_order_id)
            .await
        {
            Ok(()) => {
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                Ok(())
            }
            Err(OkxWebsocketOrderCommandError::Unavailable(err)) => {
                self.latency.record_command_fallback();
                warn!(
                    instrument_id = inst_id,
                    client_order_id,
                    error = %err,
                    "OKX WebSocket cancel command unavailable; falling back to REST cancel"
                );
                let result = self.rest.cancel_order(inst_id, client_order_id).await;
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                result
            }
            Err(OkxWebsocketOrderCommandError::PreparationRejected(err)) => {
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                Err(err).context(
                    "OKX WebSocket cancel preparation failed before command submission; refusing REST fallback",
                )
            }
            Err(OkxWebsocketOrderCommandError::Ambiguous(err)) => {
                self.latency.record_ambiguous_command_reconciliation();
                let reconciliation_started_at = Instant::now();
                warn!(
                    instrument_id = inst_id,
                    client_order_id,
                    error = %err,
                    "OKX WebSocket cancel command acknowledgement ambiguous; reconciling through REST cancel"
                );
                let result = self.rest.cancel_order(inst_id, client_order_id).await;
                self.latency.record_elapsed(
                    OkxLatencyStage::AmbiguousCommandToRestReconciliation,
                    reconciliation_started_at,
                );
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                result
            }
        }
    }

    pub(crate) async fn amend_order(&self, amend: OkxOrderAmend<'_>) -> Result<OkxOrderAck> {
        amend.validate()?;
        let command_started_at = Instant::now();
        self.record_command_start(command_started_at);
        let Some(commands) = &self.websocket_order_commands else {
            let result = self.rest.amend_order(amend).await;
            self.latency.record_elapsed(
                OkxLatencyStage::CommandStartToAcknowledgement,
                command_started_at,
            );
            return result;
        };

        match self.amend_order_with_websocket(commands, amend).await {
            Ok(acknowledgement) => {
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                Ok(acknowledgement)
            }
            Err(OkxWebsocketOrderCommandError::Unavailable(err)) => {
                self.latency.record_command_fallback();
                warn!(
                    instrument_id = amend.inst_id,
                    client_order_id = amend.client_order_id,
                    error = %err,
                    "OKX WebSocket amend command unavailable; falling back to REST amend"
                );
                let result = self.rest.amend_order(amend).await;
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                result
            }
            Err(OkxWebsocketOrderCommandError::PreparationRejected(err)) => {
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                Err(err).context(
                    "OKX WebSocket amend preparation failed before command submission; refusing REST fallback",
                )
            }
            Err(OkxWebsocketOrderCommandError::Ambiguous(err)) => {
                self.latency.record_ambiguous_command_reconciliation();
                let reconciliation_started_at = Instant::now();
                warn!(
                    instrument_id = amend.inst_id,
                    client_order_id = amend.client_order_id,
                    error = %err,
                    "OKX WebSocket amend command acknowledgement ambiguous; reconciling through REST order lookup"
                );
                let result = self.rest.reconcile_order_amend_failure(amend, err).await;
                self.latency.record_elapsed(
                    OkxLatencyStage::AmbiguousCommandToRestReconciliation,
                    reconciliation_started_at,
                );
                self.latency.record_elapsed(
                    OkxLatencyStage::CommandStartToAcknowledgement,
                    command_started_at,
                );
                result
            }
        }
    }

    async fn place_order_with_websocket(
        &self,
        commands: &OkxWebsocketOrderCommands,
        command: OkxRegularOrderCommand<'_>,
    ) -> std::result::Result<OkxOrderAck, OkxWebsocketOrderCommandError> {
        self.ensure_websocket_order_session_available(commands)
            .await?;
        let inst_id_code = self.websocket_inst_id_code(command.inst_id).await?;
        let exp_time = self
            .rest
            .prepare_websocket_place_order(
                command.inst_id,
                command.side,
                command.kind,
                command.price,
            )
            .await
            .map_err(OkxWebsocketOrderCommandError::PreparationRejected)?;
        let (td_mode, trade_quote_currency) = self
            .rest
            .websocket_order_route(command.inst_id)
            .map_err(OkxWebsocketOrderCommandError::PreparationRejected)?;
        let mut session = self.websocket_order_session(commands).await?;
        let request_id = self.next_websocket_request_id(
            OKX_WEBSOCKET_PLACE_ORDER_REQUEST_PREFIX,
            command.client_order_id,
        );
        let command_result = connected_websocket_order_session(&mut session)?
            .place_order(OkxWebsocketPlaceOrder {
                id: &request_id,
                inst_id_code,
                exp_time: &exp_time,
                side: command.side,
                kind: command.kind,
                size: command.size,
                price: command.price,
                td_mode,
                trade_quote_currency: &trade_quote_currency,
                client_order_id: command.client_order_id,
                tag: OKX_CANCEL_ALL_AFTER_TAG,
            })
            .await;
        match command_result {
            Ok(acknowledgement) => Ok(acknowledgement),
            Err(err) => {
                *session = None;
                commands.available.store(false, Ordering::Release);
                Err(map_websocket_command_error(err))
            }
        }
    }

    async fn amend_order_with_websocket(
        &self,
        commands: &OkxWebsocketOrderCommands,
        amend: OkxOrderAmend<'_>,
    ) -> std::result::Result<OkxOrderAck, OkxWebsocketOrderCommandError> {
        self.ensure_websocket_order_session_available(commands)
            .await?;
        let inst_id_code = self.websocket_inst_id_code(amend.inst_id).await?;
        let exp_time = self
            .rest
            .prepare_websocket_amend_order(amend.inst_id, amend.side, amend.new_price)
            .await
            .map_err(OkxWebsocketOrderCommandError::PreparationRejected)?;
        let mut session = self.websocket_order_session(commands).await?;
        let request_id = self.next_websocket_request_id(
            OKX_WEBSOCKET_AMEND_ORDER_REQUEST_PREFIX,
            amend.client_order_id,
        );
        let command_result = connected_websocket_order_session(&mut session)?
            .amend_order(OkxWebsocketAmendOrder {
                id: &request_id,
                inst_id_code,
                exp_time: &exp_time,
                client_order_id: amend.client_order_id,
                request_id: &request_id,
                new_size: amend.new_size,
                new_price: amend.new_price,
            })
            .await;
        match command_result {
            Ok(acknowledgement) => Ok(acknowledgement),
            Err(err) => {
                *session = None;
                commands.available.store(false, Ordering::Release);
                Err(map_websocket_command_error(err))
            }
        }
    }

    async fn cancel_order_with_websocket(
        &self,
        commands: &OkxWebsocketOrderCommands,
        inst_id: &str,
        client_order_id: &str,
    ) -> std::result::Result<(), OkxWebsocketOrderCommandError> {
        self.ensure_websocket_order_session_available(commands)
            .await?;
        let inst_id_code = self.websocket_inst_id_code(inst_id).await?;
        self.rest
            .prepare_websocket_cancel_order(inst_id)
            .await
            .map_err(OkxWebsocketOrderCommandError::Unavailable)?;
        let mut session = self.websocket_order_session(commands).await?;
        let request_id = self
            .next_websocket_request_id(OKX_WEBSOCKET_CANCEL_ORDER_REQUEST_PREFIX, client_order_id);
        let command_result = connected_websocket_order_session(&mut session)?
            .cancel_order(OkxWebsocketCancelOrder {
                id: &request_id,
                inst_id_code,
                client_order_id,
            })
            .await;
        match command_result {
            Ok(_) => Ok(()),
            Err(err) => {
                *session = None;
                commands.available.store(false, Ordering::Release);
                Err(map_websocket_command_error(err))
            }
        }
    }

    async fn prepare_websocket_order_commands(
        &self,
        commands: &OkxWebsocketOrderCommands,
    ) -> Result<()> {
        self.rest.prepare_websocket_order_command_timing().await?;
        self.connect_websocket_order_commands(commands).await
    }

    async fn connect_websocket_order_commands(
        &self,
        commands: &OkxWebsocketOrderCommands,
    ) -> Result<()> {
        let login_timestamp = self.rest.websocket_login_timestamp().await.context(
            "failed obtaining OKX server-time-backed WebSocket login timestamp for order command session",
        )?;
        let connected =
            OkxWebsocketTradingCommandSession::connect(commands.config.clone(), &login_timestamp)
                .await?;
        let mut session = commands.session.lock().await;
        *session = Some(connected);
        commands.available.store(true, Ordering::Release);
        Ok(())
    }

    async fn mark_websocket_order_commands_unavailable(
        &self,
        commands: &OkxWebsocketOrderCommands,
    ) {
        commands.available.store(false, Ordering::Release);
        *commands.session.lock().await = None;
    }

    async fn ensure_websocket_order_session_available(
        &self,
        commands: &OkxWebsocketOrderCommands,
    ) -> std::result::Result<(), OkxWebsocketOrderCommandError> {
        if commands.available.load(Ordering::Acquire) {
            return Ok(());
        }
        if !commands.prepare_attempted.load(Ordering::Acquire) {
            return Err(OkxWebsocketOrderCommandError::Unavailable(anyhow::anyhow!(
                "OKX WebSocket order command path is not prepared; using REST fallback"
            )));
        }
        if let Err(err) = self.connect_websocket_order_commands(commands).await {
            self.mark_websocket_order_commands_unavailable(commands)
                .await;
            return Err(OkxWebsocketOrderCommandError::Unavailable(err.context(
                "failed reconnecting OKX WebSocket order command session; using REST fallback",
            )));
        }
        Ok(())
    }

    async fn websocket_order_session<'a>(
        &self,
        commands: &'a OkxWebsocketOrderCommands,
    ) -> std::result::Result<
        tokio::sync::MutexGuard<'a, Option<OkxWebsocketTradingCommandSession>>,
        OkxWebsocketOrderCommandError,
    > {
        let session = commands.session.lock().await;
        if session.is_none() {
            commands.available.store(false, Ordering::Release);
            return Err(OkxWebsocketOrderCommandError::Unavailable(anyhow::anyhow!(
                "OKX WebSocket order session is not connected; using REST fallback"
            )));
        }
        Ok(session)
    }

    async fn websocket_inst_id_code(
        &self,
        inst_id: &str,
    ) -> std::result::Result<u64, OkxWebsocketOrderCommandError> {
        if let Some(inst_id_code) = self
            .websocket_inst_id_codes
            .lock()
            .map_err(|_| {
                OkxWebsocketOrderCommandError::Unavailable(anyhow::anyhow!(
                    "OKX WebSocket instIdCode cache lock poisoned"
                ))
            })?
            .get(inst_id)
            .copied()
        {
            return Ok(inst_id_code);
        }

        #[cfg(not(test))]
        let instrument = self
            .rest
            .validated_trading_instrument(inst_id)
            .map_err(OkxWebsocketOrderCommandError::Unavailable)?;
        #[cfg(not(test))]
        let inst_id_code = instrument
            .inst_id_code()
            .map_err(OkxWebsocketOrderCommandError::Unavailable)?
            .with_context(|| {
                format!(
                    "OKX instrument {inst_id} omitted instIdCode required for WebSocket order commands"
                )
            })
            .map_err(OkxWebsocketOrderCommandError::Unavailable)?;
        #[cfg(test)]
        let inst_id_code = self
            .rest
            .instruments(inst_id)
            .await
            .map_err(OkxWebsocketOrderCommandError::Unavailable)?
            .websocket_inst_id_code()
            .map_err(OkxWebsocketOrderCommandError::Unavailable)?
            .with_context(|| {
                format!(
                    "OKX instrument {inst_id} omitted instIdCode required for WebSocket order commands"
                )
            })
            .map_err(OkxWebsocketOrderCommandError::Unavailable)?;
        self.websocket_inst_id_codes
            .lock()
            .map_err(|_| {
                OkxWebsocketOrderCommandError::Unavailable(anyhow::anyhow!(
                    "OKX WebSocket instIdCode cache lock poisoned"
                ))
            })?
            .insert(inst_id.to_owned(), inst_id_code);
        Ok(inst_id_code)
    }

    fn next_websocket_request_id(&self, prefix: char, client_order_id: &str) -> String {
        let nonce = self.websocket_request_nonce.fetch_add(1, Ordering::Relaxed)
            & OKX_WEBSOCKET_REQUEST_NONCE_MASK;
        websocket_request_id(prefix, nonce, client_order_id)
    }
}

fn connected_websocket_order_session(
    session: &mut Option<OkxWebsocketTradingCommandSession>,
) -> std::result::Result<&mut OkxWebsocketTradingCommandSession, OkxWebsocketOrderCommandError> {
    let Some(session) = session.as_mut() else {
        return Err(OkxWebsocketOrderCommandError::Unavailable(anyhow::anyhow!(
            "OKX WebSocket order session was unavailable after connection setup"
        )));
    };
    Ok(session)
}

fn map_websocket_command_error(err: OkxWebsocketCommandError) -> OkxWebsocketOrderCommandError {
    match err {
        OkxWebsocketCommandError::NotSent(err) => OkxWebsocketOrderCommandError::Unavailable(err),
        OkxWebsocketCommandError::Ambiguous(err) => OkxWebsocketOrderCommandError::Ambiguous(err),
    }
}

impl OkxCancelAllAfterClient {
    pub(crate) async fn cancel_all_after(
        &self,
        timeout: OkxCancelAllAfterTimeout,
    ) -> Result<OkxCancelAllAfterAck> {
        self.rest.cancel_all_after(timeout).await
    }
}

impl OkxServerTimeRefreshClient {
    pub(crate) async fn refresh_if_expiring(&self) -> Result<bool> {
        self.rest.refresh_server_time_if_expiring().await
    }
}

impl OkxAccountConfigObservationClient {
    pub(crate) async fn account_config(&self) -> Result<OkxAccountConfig> {
        self.rest.account_config().await
    }
}

impl OkxServerTimeRefresher {
    #[cfg(test)]
    pub(crate) fn spawn_with_timing(
        client: OkxServerTimeRefreshClient,
        period: Duration,
        refresh_deadline: Duration,
    ) -> Self {
        Self::spawn_with_timing_and_failures(client, period, refresh_deadline, None)
    }

    pub(crate) fn spawn_with_failure_reporting(
        client: OkxServerTimeRefreshClient,
    ) -> (Self, mpsc::Receiver<anyhow::Error>) {
        let (failures, receiver) = mpsc::channel(1);
        (
            Self::spawn_with_timing_and_failures(
                client,
                OKX_SERVER_TIME_REFRESH_MARGIN,
                OKX_SERVER_TIME_REFRESH_MARGIN,
                Some(failures),
            ),
            receiver,
        )
    }

    fn spawn_with_timing_and_failures(
        client: OkxServerTimeRefreshClient,
        period: Duration,
        refresh_deadline: Duration,
        failures: Option<mpsc::Sender<anyhow::Error>>,
    ) -> Self {
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            if !refresh_server_time_until_stop(
                &client,
                refresh_deadline,
                &mut stop_rx,
                failures.as_ref(),
            )
            .await
            {
                return;
            }
            let mut interval = time::interval(period);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = interval.tick() => {
                        if !refresh_server_time_until_stop(
                            &client,
                            refresh_deadline,
                            &mut stop_rx,
                            failures.as_ref(),
                        )
                        .await
                        {
                            break;
                        }
                    }
                }
            }
        });
        Self {
            stop: Some(stop_tx),
            handle: Some(handle),
        }
    }

    pub(crate) async fn stop(&mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle
                .await
                .context("OKX server time refresher task panicked")?;
        }
        Ok(())
    }

    pub(crate) fn abort(&mut self) {
        self.stop.take();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for OkxServerTimeRefresher {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

async fn refresh_server_time_until_stop(
    client: &OkxServerTimeRefreshClient,
    refresh_deadline: Duration,
    stop_rx: &mut oneshot::Receiver<()>,
    failures: Option<&mpsc::Sender<anyhow::Error>>,
) -> bool {
    tokio::select! {
        _ = stop_rx => false,
        refresh = time::timeout(refresh_deadline, client.refresh_if_expiring()) => {
            match refresh {
                Ok(Ok(true)) => {
                    debug!("proactively refreshed OKX server time cache");
                }
                Ok(Ok(false)) => {}
                Ok(Err(err)) => {
                    warn!(
                        error = %err,
                        "OKX server time proactive refresh failed; lazy order-path refresh remains available"
                    );
                    report_server_time_refresh_failure(failures, err);
                }
                Err(_) => {
                    warn!(
                        refresh_deadline_ms = refresh_deadline.as_millis(),
                        "OKX server time proactive refresh exceeded deadline; lazy order-path refresh remains available"
                    );
                    report_server_time_refresh_failure(
                        failures,
                        anyhow::anyhow!(
                            "OKX server time proactive refresh exceeded {} ms",
                            refresh_deadline.as_millis()
                        ),
                    );
                }
            }
            true
        }
    }
}

fn report_server_time_refresh_failure(
    failures: Option<&mpsc::Sender<anyhow::Error>>,
    error: anyhow::Error,
) {
    let Some(failures) = failures else {
        return;
    };
    match failures.try_send(error) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => warn!(
            safety_event = "server_time_refresh_failure_coalesced",
            "server-time refresh failure already awaits runtime reconciliation"
        ),
        Err(mpsc::error::TrySendError::Closed(_)) => warn!(
            safety_event = "server_time_refresh_failure_delivery_closed",
            "server-time refresh failure receiver closed"
        ),
    }
}

fn websocket_order_command_config(
    config: &BotConfig,
) -> Result<Option<OkxWebsocketTradingCommandConfig>> {
    let okx = config.okx.as_ref().context("OKX config is required")?;
    let url = okx
        .base_url_ws_private
        .clone()
        .context("OKX base_url_ws_private is required for WebSocket order commands")?;
    let credentials = OkxWebsocketTradingCommandCredentials::new(
        okx.api_key.clone(),
        okx.api_secret.clone(),
        okx.api_passphrase.clone(),
    )?;
    Ok(Some(OkxWebsocketTradingCommandConfig::with_ack_timeout(
        url,
        credentials,
        websocket_order_command_ack_timeout(config.runtime.tick_timeout_ms),
    )?))
}

fn websocket_order_command_ack_timeout(tick_timeout_ms: u64) -> Duration {
    let tick_budget =
        Duration::from_millis(tick_timeout_ms) / OKX_WEBSOCKET_COMMAND_TICK_TIMEOUT_DIVISOR;
    tick_budget.clamp(OKX_WEBSOCKET_COMMAND_MIN_ACK_TIMEOUT, DEFAULT_ACK_TIMEOUT)
}

fn websocket_request_id(prefix: char, nonce: u64, client_order_id: &str) -> String {
    let nonce = nonce & OKX_WEBSOCKET_REQUEST_NONCE_MASK;
    format!(
        "{prefix}{nonce:015x}{:016x}",
        fnv1a64(client_order_id.as_bytes())
    )
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

impl OkxClient for OkxTradingClient {
    fn record_order_decision(&self, decided_at: Instant) {
        OkxTradingClient::record_order_decision(self, decided_at);
    }

    async fn instruments(&self, inst_id: &str) -> Result<OkxInstrument> {
        OkxTradingClient::instruments(self, inst_id).await
    }

    async fn candles(&self, inst_id: &str, bar: &str, limit: usize) -> Result<Vec<MarketBar>> {
        self.rest.candles(inst_id, bar, limit).await
    }

    async fn live_candles(&self, inst_id: &str, bar: &str, limit: usize) -> Result<Vec<MarketBar>> {
        self.rest.live_candles(inst_id, bar, limit).await
    }

    async fn ticker(&self, inst_id: &str) -> Result<OkxTicker> {
        self.rest.ticker(inst_id).await
    }

    async fn fresh_quote_usd_rate(
        &self,
        instrument: &ValidatedTradingInstrument,
    ) -> Result<ValidatedQuoteUsdRate> {
        self.rest.fresh_quote_usd_rate(instrument).await
    }

    async fn balances(&self) -> Result<Vec<OkxBalance>> {
        self.rest.balances().await
    }

    async fn spot_trade_fee(&self, inst_id: &str) -> Result<OkxTradeFeeRate> {
        self.rest.spot_trade_fee(inst_id).await
    }

    async fn open_orders(&self, inst_id: &str) -> Result<Vec<OkxOrder>> {
        self.rest.open_orders(inst_id).await
    }

    async fn order_history(&self, inst_id: &str) -> Result<Vec<OkxOrder>> {
        self.rest.order_history(inst_id).await
    }

    async fn order_fills(&self, inst_id: &str) -> Result<Vec<OkxFill>> {
        self.rest.order_fills(inst_id).await
    }

    async fn open_algo_orders(&self, inst_id: &str) -> Result<Vec<OkxAlgoOrder>> {
        self.rest.open_algo_orders(inst_id).await
    }

    async fn algo_order_history(&self, inst_id: &str) -> Result<Vec<OkxAlgoOrder>> {
        self.rest.algo_order_history(inst_id).await
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
        OkxTradingClient::place_order(self, inst_id, side, kind, size, price, client_order_id).await
    }

    async fn cancel_order(&self, inst_id: &str, client_order_id: &str) -> Result<()> {
        OkxTradingClient::cancel_order(self, inst_id, client_order_id).await
    }

    async fn amend_order(&self, request: OkxOrderAmend<'_>) -> Result<OkxOrderAck> {
        OkxTradingClient::amend_order(self, request).await
    }

    async fn place_trigger_order(
        &self,
        inst_id: &str,
        side: OrderSide,
        size: &str,
        trigger_price: &str,
        client_order_id: &str,
    ) -> Result<OkxAlgoOrderAck> {
        let command_started_at = Instant::now();
        self.record_command_start(command_started_at);
        let result = self
            .rest
            .place_trigger_order(inst_id, side, size, trigger_price, client_order_id)
            .await;
        self.latency.record_elapsed(
            OkxLatencyStage::CommandStartToAcknowledgement,
            command_started_at,
        );
        result
    }

    async fn cancel_algo_order(&self, inst_id: &str, algo_id: &str) -> Result<()> {
        let command_started_at = Instant::now();
        self.record_command_start(command_started_at);
        let result = self.rest.cancel_algo_order(inst_id, algo_id).await;
        self.latency.record_elapsed(
            OkxLatencyStage::CommandStartToAcknowledgement,
            command_started_at,
        );
        result
    }

    async fn order(&self, inst_id: &str, client_order_id: &str) -> Result<Option<OkxOrder>> {
        self.rest.order(inst_id, client_order_id).await
    }
}

#[cfg(test)]
#[path = "trading_client_tests.rs"]
mod tests;
