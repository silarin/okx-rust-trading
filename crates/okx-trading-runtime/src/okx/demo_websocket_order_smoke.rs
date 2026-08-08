use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use rust_decimal::Decimal;

use super::demo_order_smoke::{SmokeOrderLifecycle, finish_order_lifecycle};
use super::demo_private_order_observer::{
    DemoPrivateOrderObserver, ExpectedPrivateOrderState, PrivateOrderExpectation,
};
use crate::{
    config::types::BotConfig,
    okx::{
        client::{OKX_CANCEL_ALL_AFTER_TAG, OkxRestClient},
        types::{OkxInstrument, OrderKind, OrderSide},
        websocket::{
            trading::{OkxWebsocketAmendOrder, OkxWebsocketCancelOrder, OkxWebsocketPlaceOrder},
            trading_session::{
                OkxWebsocketTradingCommandConfig, OkxWebsocketTradingCommandCredentials,
                OkxWebsocketTradingCommandSession,
            },
        },
    },
};

const WEBSOCKET_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const WEBSOCKET_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const AMEND_RECONCILE_ATTEMPTS: usize = 4;
const AMEND_RECONCILE_DELAY: Duration = Duration::from_millis(100);

pub(super) struct PreparedWebsocketOrderTransport {
    pub(super) session: OkxWebsocketTradingCommandSession,
    pub(super) inst_id_code: u64,
}

#[derive(Clone, Copy)]
pub(super) enum WebsocketOrderLifecycle<'a> {
    PlaceCancel,
    PlaceAmendCancel { amended_price: &'a str },
}

#[derive(Clone, Copy)]
pub(super) enum WebsocketOrderObservation<'a> {
    Disabled,
    PrivateOrders(&'a DemoPrivateOrderObserver),
}

impl WebsocketOrderObservation<'_> {
    async fn wait_for_order(self, expectation: PrivateOrderExpectation<'_>) -> Result<()> {
        match self {
            Self::Disabled => Ok(()),
            Self::PrivateOrders(observer) => observer.wait_for_order(expectation).await,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct WebsocketSmokeOrder<'a> {
    pub(super) instrument_id: &'a str,
    pub(super) size: &'a str,
    pub(super) price: &'a str,
    pub(super) client_order_id: &'a str,
    pub(super) lifecycle: WebsocketOrderLifecycle<'a>,
    pub(super) observation: WebsocketOrderObservation<'a>,
}

pub(super) async fn connect(
    client: &OkxRestClient,
    config: &BotConfig,
    instrument: &OkxInstrument,
) -> Result<PreparedWebsocketOrderTransport> {
    let inst_id_code = instrument
        .websocket_inst_id_code()?
        .context("OKX demo WebSocket order smoke requires instrument instIdCode")?;
    let okx = config.okx.as_ref().context("OKX config is required")?;
    let url = okx
        .base_url_ws_private
        .clone()
        .context("OKX base_url_ws_private is required for WebSocket order smoke")?;
    let credentials = OkxWebsocketTradingCommandCredentials::new(
        okx.api_key.clone(),
        okx.api_secret.clone(),
        okx.api_passphrase.clone(),
    )?;
    let command_config = OkxWebsocketTradingCommandConfig::with_ack_timeout(
        url,
        credentials,
        WEBSOCKET_ACK_TIMEOUT,
    )?;
    let login_timestamp = client
        .websocket_login_timestamp()
        .await
        .context("OKX demo WebSocket order login timestamp sync failed")?;
    let connect_started = Instant::now();
    let session = tokio::time::timeout(
        WEBSOCKET_CONNECT_TIMEOUT,
        OkxWebsocketTradingCommandSession::connect(command_config, &login_timestamp),
    )
    .await
    .context("timed out connecting the OKX demo WebSocket order command session")?
    .context("OKX demo WebSocket order command login failed")?;
    eprintln!(
        "OKX demo WebSocket order command connection completed in {} ms",
        connect_started.elapsed().as_millis()
    );
    Ok(PreparedWebsocketOrderTransport {
        session,
        inst_id_code,
    })
}

pub(super) async fn run_order_lifecycle(
    client: &OkxRestClient,
    transport: &mut PreparedWebsocketOrderTransport,
    order: WebsocketSmokeOrder<'_>,
) -> SmokeOrderLifecycle {
    let mut failures = Vec::new();

    let place_started = Instant::now();
    let place_acknowledgement = match async {
        let validated = client
            .validated_trading_instrument(order.instrument_id)
            .context("WebSocket place request is missing validated trading context")?;
        let exp_time = client
            .prepare_websocket_place_order(
                order.instrument_id,
                OrderSide::Buy,
                OrderKind::PostOnly,
                Some(order.price),
            )
            .await
            .context("WebSocket post-only place preparation failed")?;
        let request_id =
            websocket_request_id("WSP").context("WebSocket place request id generation failed")?;
        transport
            .session
            .place_order(OkxWebsocketPlaceOrder {
                td_mode: validated.td_mode().as_okx(),
                id: &request_id,
                inst_id_code: transport.inst_id_code,
                exp_time: &exp_time,
                side: OrderSide::Buy,
                kind: OrderKind::PostOnly,
                size: order.size,
                price: Some(order.price),
                trade_quote_currency: validated.trade_quote_ccy(),
                client_order_id: order.client_order_id,
                tag: OKX_CANCEL_ALL_AFTER_TAG,
            })
            .await
            .context("WebSocket post-only place acknowledgement failed or was ambiguous")
    }
    .await
    {
        Ok(acknowledgement) => Some(acknowledgement),
        Err(error) => {
            failures.push(format!("WebSocket post-only place failed: {error:#}"));
            None
        }
    };
    eprintln!(
        "OKX demo WebSocket post-only place acknowledgement completed in {} ms",
        place_started.elapsed().as_millis()
    );

    let place_event_observed = if let Some(acknowledgement) = &place_acknowledgement {
        match order
            .observation
            .wait_for_order(PrivateOrderExpectation {
                stage: "place",
                instrument_id: order.instrument_id,
                order_id: &acknowledgement.order_id,
                client_order_id: order.client_order_id,
                price: order.price,
                size: order.size,
                state: ExpectedPrivateOrderState::Live,
                command_started_at: place_started,
                timeout: super::demo_private_order_observer::PRIVATE_EVENT_TIMEOUT,
            })
            .await
        {
            Ok(()) => true,
            Err(error) => {
                failures.push(format!(
                    "private orders stream did not correlate the placed order: {error:#}"
                ));
                false
            }
        }
    } else {
        false
    };

    let command_session_reliable = match (order.lifecycle, &place_acknowledgement) {
        (WebsocketOrderLifecycle::PlaceCancel, Some(_)) => true,
        (WebsocketOrderLifecycle::PlaceAmendCancel { amended_price }, Some(acknowledgement))
            if place_event_observed =>
        {
            run_amend_order(
                client,
                transport,
                order,
                &acknowledgement.order_id,
                amended_price,
                &mut failures,
            )
            .await
        }
        (WebsocketOrderLifecycle::PlaceCancel, None) => false,
        (WebsocketOrderLifecycle::PlaceAmendCancel { .. }, None) => {
            failures.push(
                "WebSocket amend was not attempted without a confirmed place acknowledgement"
                    .to_owned(),
            );
            false
        }
        (WebsocketOrderLifecycle::PlaceAmendCancel { .. }, Some(_)) => {
            failures.push(
                "WebSocket amend was not attempted without a correlated private live-order event"
                    .to_owned(),
            );
            false
        }
    };

    let cancel_started = Instant::now();
    let cancel_acknowledgement = if command_session_reliable {
        match client
            .prepare_websocket_cancel_order(order.instrument_id)
            .await
        {
            Ok(()) => match websocket_request_id("WSC") {
                Ok(request_id) => match transport
                    .session
                    .cancel_order(OkxWebsocketCancelOrder {
                        id: &request_id,
                        inst_id_code: transport.inst_id_code,
                        client_order_id: order.client_order_id,
                    })
                    .await
                {
                    Ok(acknowledgement)
                        if place_acknowledgement
                            .as_ref()
                            .is_some_and(|placed| placed.order_id == acknowledgement.order_id) =>
                    {
                        Some(acknowledgement)
                    }
                    Ok(acknowledgement) => {
                        failures.push(format!(
                            "WebSocket cancel acknowledgement returned order id {} instead of the placed order id",
                            acknowledgement.order_id
                        ));
                        None
                    }
                    Err(error) => {
                        failures.push(format!(
                            "WebSocket cancel acknowledgement failed or was ambiguous: {error:#}"
                        ));
                        None
                    }
                },
                Err(error) => {
                    failures.push(format!(
                        "WebSocket cancel request id generation failed: {error:#}"
                    ));
                    None
                }
            },
            Err(error) => {
                failures.push(format!("WebSocket cancel preparation failed: {error:#}"));
                None
            }
        }
    } else {
        failures.push(
            "WebSocket cancel was not attempted after an unconfirmed order-command outcome"
                .to_owned(),
        );
        None
    };
    eprintln!(
        "OKX demo WebSocket cancel acknowledgement completed in {} ms",
        cancel_started.elapsed().as_millis()
    );

    if let Some(acknowledgement) = &cancel_acknowledgement
        && let Err(error) = order
            .observation
            .wait_for_order(PrivateOrderExpectation {
                stage: "cancel",
                instrument_id: order.instrument_id,
                order_id: &acknowledgement.order_id,
                client_order_id: order.client_order_id,
                price: match order.lifecycle {
                    WebsocketOrderLifecycle::PlaceCancel => order.price,
                    WebsocketOrderLifecycle::PlaceAmendCancel { amended_price } => amended_price,
                },
                size: order.size,
                state: ExpectedPrivateOrderState::Canceled,
                command_started_at: cancel_started,
                timeout: super::demo_private_order_observer::PRIVATE_EVENT_TIMEOUT,
            })
            .await
    {
        failures.push(format!(
            "private orders stream did not correlate the canceled order: {error:#}"
        ));
    }

    if cancel_acknowledgement.is_none() {
        let fallback_started = Instant::now();
        if let Err(error) = client
            .cancel_order(order.instrument_id, order.client_order_id)
            .await
        {
            failures.push(format!("REST cancel cleanup fallback failed: {error:#}"));
        }
        eprintln!(
            "OKX demo REST cancel cleanup fallback completed in {} ms",
            fallback_started.elapsed().as_millis()
        );
    }

    finish_order_lifecycle(client, order.instrument_id, order.client_order_id, failures).await
}

async fn run_amend_order(
    client: &OkxRestClient,
    transport: &mut PreparedWebsocketOrderTransport,
    order: WebsocketSmokeOrder<'_>,
    placed_order_id: &str,
    amended_price: &str,
    failures: &mut Vec<String>,
) -> bool {
    let amend_started = Instant::now();
    let amend_acknowledgement = match client
        .prepare_websocket_amend_order(order.instrument_id, OrderSide::Buy, Some(amended_price))
        .await
    {
        Ok(exp_time) => match websocket_request_id("WSA") {
            Ok(request_id) => match transport
                .session
                .amend_order(OkxWebsocketAmendOrder {
                    id: &request_id,
                    inst_id_code: transport.inst_id_code,
                    exp_time: &exp_time,
                    client_order_id: order.client_order_id,
                    request_id: &request_id,
                    new_size: None,
                    new_price: Some(amended_price),
                })
                .await
            {
                Ok(acknowledgement) if acknowledgement.order_id == placed_order_id => {
                    Some(acknowledgement)
                }
                Ok(acknowledgement) => {
                    failures.push(format!(
                        "WebSocket amend acknowledgement returned order id {} instead of the placed order id",
                        acknowledgement.order_id
                    ));
                    None
                }
                Err(error) => {
                    failures.push(format!(
                        "WebSocket amend acknowledgement failed or was ambiguous: {error:#}"
                    ));
                    None
                }
            },
            Err(error) => {
                failures.push(format!(
                    "WebSocket amend request id generation failed: {error:#}"
                ));
                None
            }
        },
        Err(error) => {
            failures.push(format!("WebSocket amend preparation failed: {error:#}"));
            None
        }
    };
    eprintln!(
        "OKX demo WebSocket amend acknowledgement completed in {} ms",
        amend_started.elapsed().as_millis()
    );

    let amend_event_observed = if let Some(acknowledgement) = &amend_acknowledgement {
        match order
            .observation
            .wait_for_order(PrivateOrderExpectation {
                stage: "amend",
                instrument_id: order.instrument_id,
                order_id: &acknowledgement.order_id,
                client_order_id: order.client_order_id,
                price: amended_price,
                size: order.size,
                state: ExpectedPrivateOrderState::Live,
                command_started_at: amend_started,
                timeout: super::demo_private_order_observer::PRIVATE_EVENT_TIMEOUT,
            })
            .await
        {
            Ok(()) => true,
            Err(error) => {
                failures.push(format!(
                    "private orders stream did not correlate the amended order: {error:#}"
                ));
                false
            }
        }
    } else {
        false
    };

    if amend_acknowledgement.is_some() {
        let reconcile_started = Instant::now();
        if let Err(error) = confirm_order_amend(client, order, amended_price).await {
            failures.push(format!(
                "REST did not confirm the accepted WebSocket amendment: {error:#}"
            ));
        }
        eprintln!(
            "OKX demo amended-order REST reconciliation completed in {} ms",
            reconcile_started.elapsed().as_millis()
        );
    }
    amend_acknowledgement.is_some() && amend_event_observed
}

async fn confirm_order_amend(
    client: &OkxRestClient,
    order: WebsocketSmokeOrder<'_>,
    amended_price: &str,
) -> Result<()> {
    let expected_price = amended_price
        .parse::<Decimal>()
        .context("failed parsing expected OKX demo amended price")?;
    let expected_size = order
        .size
        .parse::<Decimal>()
        .context("failed parsing expected OKX demo order size")?;
    let mut last_shape = "missing".to_owned();

    for attempt in 0..AMEND_RECONCILE_ATTEMPTS {
        if let Some(reconciled) = client
            .order(order.instrument_id, order.client_order_id)
            .await
            .context("OKX demo amended-order REST lookup failed")?
        {
            reconciled.ensure_documented_state("demo amended-order smoke lookup")?;
            let actual_price = reconciled
                .price
                .parse::<Decimal>()
                .context("failed parsing REST-reconciled OKX demo amended price")?;
            let actual_size = reconciled.requested_size()?;
            let fill_size = reconciled.fill_size()?;
            if reconciled.is_live()
                && actual_price == expected_price
                && actual_size == expected_size
                && fill_size == Decimal::ZERO
            {
                return Ok(());
            }
            last_shape = format!(
                "state {}, price {actual_price}, size {actual_size}, fill {fill_size}",
                reconciled.state
            );
        }
        if attempt + 1 < AMEND_RECONCILE_ATTEMPTS {
            tokio::time::sleep(AMEND_RECONCILE_DELAY).await;
        }
    }
    bail!(
        "OKX demo order did not reach the requested live amended shape after {AMEND_RECONCILE_ATTEMPTS} attempts; last shape was {last_shape}"
    )
}

pub(super) fn websocket_request_id(prefix: &str) -> Result<String> {
    let unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before the Unix epoch")?
        .as_millis();
    let request_id = format!("{prefix}{unix_millis}");
    ensure!(
        request_id.len() <= 32,
        "OKX demo WebSocket request id exceeds 32 characters"
    );
    Ok(request_id)
}
