use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use rust_decimal::Decimal;

use crate::{
    config::types::BotConfig,
    okx::{
        client::{OkxCancelAllAfterTimeout, OkxRestClient},
        trading_instrument::ValidatedTradingInstrument,
        types::{
            OkxInstrument, OkxOrder, OrderKind, OrderSide, decimal_to_okx, quantize_decimal_down,
            quantize_decimal_up,
        },
    },
};

const RECONCILE_ATTEMPTS: usize = 12;
const RECONCILE_DELAY: Duration = Duration::from_millis(250);

pub(super) async fn run_post_only_place_cancel_smoke(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
) -> Result<()> {
    run_post_only_place_cancel_smoke_with_transport(client, validated, SmokeOrderTransport::Rest)
        .await
}

pub(super) async fn run_websocket_post_only_place_cancel_smoke(
    client: &OkxRestClient,
    config: &BotConfig,
    validated: &ValidatedTradingInstrument,
) -> Result<()> {
    run_post_only_place_cancel_smoke_with_transport(
        client,
        validated,
        SmokeOrderTransport::WebSocketPlaceCancel(config),
    )
    .await
}

pub(super) async fn run_websocket_post_only_place_amend_cancel_smoke(
    client: &OkxRestClient,
    config: &BotConfig,
    validated: &ValidatedTradingInstrument,
) -> Result<()> {
    run_post_only_place_cancel_smoke_with_transport(
        client,
        validated,
        SmokeOrderTransport::WebSocketPlaceAmendCancel(config),
    )
    .await
}

enum SmokeOrderTransport<'a> {
    Rest,
    WebSocketPlaceCancel(&'a BotConfig),
    WebSocketPlaceAmendCancel(&'a BotConfig),
}

async fn run_post_only_place_cancel_smoke_with_transport(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
    transport: SmokeOrderTransport<'_>,
) -> Result<()> {
    let instrument = validated.instrument();
    let instrument_id = &instrument.inst_id;
    let prepared = prepare_post_only_order(client, validated, PostOnlyPrice::DeeplyPassive).await?;

    let amended_price = match &transport {
        SmokeOrderTransport::WebSocketPlaceAmendCancel(_) => {
            let amended_price = smoke_amended_price(prepared.price, instrument.tick_size()?)?;
            ensure!(
                amended_price < prepared.price && amended_price < prepared.bid,
                "OKX demo order smoke amended buy price {amended_price} must remain below initial price {} and bid {}",
                prepared.price,
                prepared.bid
            );
            let amended_notional = amended_price
                .checked_mul(prepared.size)
                .context("OKX demo amended order smoke notional overflowed Decimal")?;
            if instrument.max_limit_amount()?.is_some() {
                let quote_usd_rate = client.fresh_quote_usd_rate(validated).await?;
                validated.ensure_limit_quote_amount(
                    amended_notional,
                    &quote_usd_rate,
                    "OKX demo amended order smoke notional",
                )?;
            }
            Some(decimal_to_okx(amended_price))
        }
        SmokeOrderTransport::Rest | SmokeOrderTransport::WebSocketPlaceCancel(_) => None,
    };
    let client_order_id = smoke_client_order_id()?;
    let size = decimal_to_okx(prepared.size);
    let price = decimal_to_okx(prepared.price);
    let mut private_order_observer = match &transport {
        SmokeOrderTransport::WebSocketPlaceAmendCancel(config) => Some(
            super::demo_private_order_observer::DemoPrivateOrderObserver::connect(
                client,
                config,
                instrument_id,
            )
            .await?,
        ),
        SmokeOrderTransport::Rest | SmokeOrderTransport::WebSocketPlaceCancel(_) => None,
    };
    let websocket_transport = match &transport {
        SmokeOrderTransport::Rest => None,
        SmokeOrderTransport::WebSocketPlaceCancel(config)
        | SmokeOrderTransport::WebSocketPlaceAmendCancel(config) => {
            Some(super::demo_websocket_order_smoke::connect(client, config, instrument).await?)
        }
    };
    let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
    client
        .cancel_all_after(timeout)
        .await
        .context("OKX demo order smoke refused to place because Cancel-All-After arm failed")?;

    let mut lifecycle = match (transport, websocket_transport) {
        (SmokeOrderTransport::Rest, None) => {
            run_rest_order_lifecycle(client, instrument_id, &size, &price, &client_order_id).await
        }
        (SmokeOrderTransport::WebSocketPlaceCancel(_), Some(mut websocket_transport)) => {
            super::demo_websocket_order_smoke::run_order_lifecycle(
                client,
                &mut websocket_transport,
                super::demo_websocket_order_smoke::WebsocketSmokeOrder {
                    instrument_id,
                    size: &size,
                    price: &price,
                    client_order_id: &client_order_id,
                    lifecycle:
                        super::demo_websocket_order_smoke::WebsocketOrderLifecycle::PlaceCancel,
                    observation:
                        super::demo_websocket_order_smoke::WebsocketOrderObservation::Disabled,
                },
            )
            .await
        }
        (SmokeOrderTransport::WebSocketPlaceAmendCancel(_), Some(mut websocket_transport)) => {
            let Some(amended_price) = amended_price.as_deref() else {
                bail!("OKX demo WebSocket amend smoke omitted its validated amended price");
            };
            let Some(observer) = private_order_observer.as_ref() else {
                bail!("OKX demo WebSocket amend smoke omitted its private order observer");
            };
            super::demo_websocket_order_smoke::run_order_lifecycle(
                client,
                &mut websocket_transport,
                super::demo_websocket_order_smoke::WebsocketSmokeOrder {
                    instrument_id,
                    size: &size,
                    price: &price,
                    client_order_id: &client_order_id,
                    lifecycle: super::demo_websocket_order_smoke::WebsocketOrderLifecycle::PlaceAmendCancel {
                        amended_price,
                    },
                    observation: super::demo_websocket_order_smoke::WebsocketOrderObservation::PrivateOrders(observer),
                },
            )
            .await
        }
        (SmokeOrderTransport::Rest, Some(_))
        | (SmokeOrderTransport::WebSocketPlaceCancel(_), None)
        | (SmokeOrderTransport::WebSocketPlaceAmendCancel(_), None) => {
            bail!("OKX demo order smoke constructed an inconsistent transport plan");
        }
    };
    if !lifecycle.cleanup_verified {
        return lifecycle.result.context(
            "OKX demo order cleanup was not authoritatively verified; Cancel-All-After remains armed",
        );
    }

    if let Some(observer) = private_order_observer.as_mut()
        && let Err(reconnect_error) = observer.reconnect().await
    {
        lifecycle.result = match lifecycle.result {
            Ok(()) => Err(reconnect_error),
            Err(lifecycle_error) => Err(anyhow!(
                "OKX demo order lifecycle failed: {lifecycle_error:#}; cleanup was verified but private order observer recovery also failed: {reconnect_error:#}"
            )),
        };
    }

    finalize_order_lifecycle(client, lifecycle).await
}

pub(super) struct PreparedPostOnlyOrder {
    pub(super) bid: Decimal,
    pub(super) price: Decimal,
    pub(super) size: Decimal,
}

#[derive(Clone, Copy)]
pub(super) enum PostOnlyPrice {
    DeeplyPassive,
    Crossing,
}

pub(super) async fn prepare_post_only_order(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
    price_kind: PostOnlyPrice,
) -> Result<PreparedPostOnlyOrder> {
    let instrument = validated.instrument();
    let instrument_id = &instrument.inst_id;
    let preexisting_orders = client
        .open_orders(instrument_id)
        .await
        .context("OKX demo order smoke preflight open-order reconciliation failed")?;
    ensure!(
        preexisting_orders.is_empty(),
        "OKX demo order smoke requires no pre-existing open {instrument_id} orders; found {} and made no mutation",
        preexisting_orders.len()
    );

    let ticker = client
        .ticker(instrument_id)
        .await
        .context("OKX demo order smoke ticker preflight failed")?;
    ensure!(
        ticker.inst_id == *instrument_id,
        "OKX demo order smoke received ticker {} for requested {instrument_id}",
        ticker.inst_id
    );
    ticker.validate_prices()?;

    let size = quantize_decimal_up(instrument.min_size()?, instrument.lot_size()?)?;
    ensure!(
        size >= instrument.min_size()?,
        "OKX demo order smoke quantized size {size} below minSz {}",
        instrument.min_size()?
    );
    instrument.ensure_limit_size(size, "OKX demo order smoke size")?;

    let bid = ticker.bid_decimal()?;
    let price = match price_kind {
        PostOnlyPrice::DeeplyPassive => {
            let price = quantize_decimal_down(bid * Decimal::new(995, 3), instrument.tick_size()?)?;
            ensure!(
                price < bid,
                "OKX demo order smoke passive buy price {price} must be below bid {bid}"
            );
            price
        }
        PostOnlyPrice::Crossing => {
            let ask = ticker.ask_decimal()?;
            let price = quantize_decimal_up(ask * Decimal::new(1005, 3), instrument.tick_size()?)?;
            ensure!(
                price > ask,
                "OKX demo crossing post-only buy price {price} must be above ask {ask}"
            );
            price
        }
    };
    let quote_notional = price
        .checked_mul(size)
        .context("OKX demo order smoke quote notional overflowed Decimal")?;
    ensure!(
        quote_notional <= Decimal::new(20, 0),
        "OKX demo order smoke minimum-size quote notional {quote_notional} {} exceeds hard cap 20",
        instrument.quote_ccy
    );
    if instrument.max_limit_amount()?.is_some() {
        let quote_usd_rate = client.fresh_quote_usd_rate(validated).await?;
        validated.ensure_limit_quote_amount(
            quote_notional,
            &quote_usd_rate,
            "OKX demo order smoke notional",
        )?;
    }
    ensure_quote_balance(client, instrument, quote_notional).await?;
    Ok(PreparedPostOnlyOrder { bid, price, size })
}

fn smoke_amended_price(price: Decimal, tick_size: Decimal) -> Result<Decimal> {
    ensure!(
        price > tick_size,
        "OKX demo order smoke cannot amend price {price} down by tick size {tick_size}"
    );
    let amended_price = quantize_decimal_down(price - tick_size, tick_size)?;
    ensure!(
        amended_price > Decimal::ZERO && amended_price < price,
        "OKX demo order smoke amended price {amended_price} must be positive and below {price}"
    );
    Ok(amended_price)
}

async fn ensure_quote_balance(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    quote_notional: Decimal,
) -> Result<()> {
    let balances = client
        .balances()
        .await
        .context("OKX demo order smoke quote-balance preflight failed")?;
    let matching_details = balances
        .iter()
        .flat_map(|balance| &balance.details)
        .filter(|detail| detail.ccy == instrument.quote_ccy)
        .collect::<Vec<_>>();
    ensure!(
        matching_details.len() == 1,
        "OKX demo order smoke expected one {} balance row, received {}",
        instrument.quote_ccy,
        matching_details.len()
    );
    let available = matching_details[0].available()?;
    ensure!(
        available >= quote_notional,
        "OKX demo order smoke requires {quote_notional} {} but only {available} is available",
        instrument.quote_ccy
    );
    Ok(())
}

pub(super) fn smoke_client_order_id() -> Result<String> {
    let unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before the Unix epoch")?
        .as_millis();
    let client_order_id = format!("OKXSMOKE{unix_millis}");
    ensure!(
        client_order_id.len() <= 32,
        "OKX demo smoke client order id exceeds 32 characters"
    );
    Ok(client_order_id)
}

pub(super) struct SmokeOrderLifecycle {
    pub(super) cleanup_verified: bool,
    pub(super) result: Result<()>,
}

pub(super) async fn finalize_order_lifecycle(
    client: &OkxRestClient,
    lifecycle: SmokeOrderLifecycle,
) -> Result<()> {
    if !lifecycle.cleanup_verified {
        return lifecycle.result.context(
            "OKX demo order cleanup was not authoritatively verified; Cancel-All-After remains armed",
        );
    }

    let disarm_result = client
        .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
        .await
        .context("OKX demo order cleanup passed but Cancel-All-After disarm failed");
    match (lifecycle.result, disarm_result) {
        (Ok(()), Ok(_)) => Ok(()),
        (Err(lifecycle_error), Ok(_)) => Err(lifecycle_error),
        (Ok(()), Err(disarm_error)) => Err(disarm_error),
        (Err(lifecycle_error), Err(disarm_error)) => Err(anyhow!(
            "OKX demo order lifecycle failed: {lifecycle_error:#}; cleanup was verified but Cancel-All-After disarm also failed: {disarm_error:#}"
        )),
    }
}

async fn run_rest_order_lifecycle(
    client: &OkxRestClient,
    instrument_id: &str,
    size: &str,
    price: &str,
    client_order_id: &str,
) -> SmokeOrderLifecycle {
    let mut failures = Vec::new();

    let place_started = Instant::now();
    let place_result = client
        .place_order(
            instrument_id,
            OrderSide::Buy,
            OrderKind::PostOnly,
            size,
            Some(price),
            client_order_id,
        )
        .await;
    eprintln!(
        "OKX demo post-only place acknowledgement completed in {} ms",
        place_started.elapsed().as_millis()
    );
    if let Err(error) = place_result {
        failures.push(format!("post-only place failed: {error:#}"));
    }

    let cancel_started = Instant::now();
    if let Err(error) = client.cancel_order(instrument_id, client_order_id).await {
        failures.push(format!("immediate cancel failed: {error:#}"));
    }
    eprintln!(
        "OKX demo cancel acknowledgement completed in {} ms",
        cancel_started.elapsed().as_millis()
    );

    finish_order_lifecycle(client, instrument_id, client_order_id, failures).await
}

pub(super) async fn finish_order_lifecycle(
    client: &OkxRestClient,
    instrument_id: &str,
    client_order_id: &str,
    mut failures: Vec<String>,
) -> SmokeOrderLifecycle {
    let reconcile_started = Instant::now();
    let terminal_order = wait_for_terminal_order(client, instrument_id, client_order_id).await;
    eprintln!(
        "OKX demo terminal REST reconciliation completed in {} ms",
        reconcile_started.elapsed().as_millis()
    );

    let terminal_without_fill = match terminal_order {
        Ok(order) => match order.fill_size() {
            Ok(fill_size) if order.is_terminal_without_fill() && fill_size == Decimal::ZERO => true,
            Ok(fill_size) => {
                failures.push(format!(
                    "REST reconciliation returned state {} with accumulated fill size {fill_size}",
                    order.state
                ));
                false
            }
            Err(error) => {
                failures.push(format!(
                    "REST terminal order fill validation failed: {error:#}"
                ));
                false
            }
        },
        Err(error) => {
            failures.push(format!(
                "REST terminal order reconciliation failed: {error:#}"
            ));
            false
        }
    };

    let no_open_orders = match client.open_orders(instrument_id).await {
        Ok(open_orders) if open_orders.is_empty() => true,
        Ok(open_orders) => {
            failures.push(format!(
                "post-cancel REST reconciliation found {} open {instrument_id} orders",
                open_orders.len()
            ));
            false
        }
        Err(error) => {
            failures.push(format!(
                "post-cancel open-order reconciliation failed: {error:#}"
            ));
            false
        }
    };

    let cleanup_verified = terminal_without_fill && no_open_orders;
    let result = if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join("; ")))
    };
    SmokeOrderLifecycle {
        cleanup_verified,
        result,
    }
}

pub(super) async fn wait_for_terminal_order(
    client: &OkxRestClient,
    instrument_id: &str,
    client_order_id: &str,
) -> Result<OkxOrder> {
    let mut last_state = "missing".to_owned();
    for attempt in 0..RECONCILE_ATTEMPTS {
        if let Some(order) = client
            .order(instrument_id, client_order_id)
            .await
            .context("OKX demo order REST lookup failed")?
        {
            order.ensure_documented_state("demo order smoke lookup")?;
            if order.is_terminal() {
                return Ok(order);
            }
            last_state = order.state;
        }
        if attempt + 1 < RECONCILE_ATTEMPTS {
            tokio::time::sleep(RECONCILE_DELAY).await;
        }
    }
    bail!(
        "OKX demo order did not reach a terminal REST state after {RECONCILE_ATTEMPTS} attempts; last state was {last_state}"
    )
}

#[cfg(test)]
#[path = "demo_order_smoke_tests.rs"]
mod tests;
