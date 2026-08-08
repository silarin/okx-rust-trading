use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use super::{
    demo_order_smoke::{
        PostOnlyPrice, finalize_order_lifecycle, finish_order_lifecycle, prepare_post_only_order,
        smoke_client_order_id,
    },
    demo_private_order_observer::{
        DemoPrivateOrderObserver, ExpectedPrivateOrderState, PRIVATE_EVENT_TIMEOUT,
        PrivateOrderExpectation,
    },
};
use crate::{
    config::types::BotConfig,
    okx::{
        client::{OkxCancelAllAfterTimeout, OkxRestClient},
        trading_instrument::ValidatedTradingInstrument,
        types::{OrderKind, OrderSide, decimal_to_okx},
    },
};

const CAA_EXPIRY_EVENT_TIMEOUT: Duration = Duration::from_secs(20);

pub(super) async fn run_cancel_all_after_expiry_smoke(
    client: &OkxRestClient,
    config: &BotConfig,
    validated: &ValidatedTradingInstrument,
) -> Result<()> {
    let prepared = prepare_post_only_order(client, validated, PostOnlyPrice::DeeplyPassive).await?;
    let instrument = validated.instrument();
    let instrument_id = &instrument.inst_id;
    let size = decimal_to_okx(prepared.size);
    let price = decimal_to_okx(prepared.price);
    let client_order_id = smoke_client_order_id()?;
    let observer = DemoPrivateOrderObserver::connect(client, config, instrument_id).await?;

    let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
    let arm_started = Instant::now();
    client
        .cancel_all_after(timeout)
        .await
        .context("OKX Demo CAA expiry smoke refused to place because CAA arm failed")?;
    eprintln!(
        "OKX Demo CAA expiry arm acknowledgement completed in {} ms",
        arm_started.elapsed().as_millis()
    );

    let place_started = Instant::now();
    let acknowledgement = client
        .place_order(
            instrument_id,
            OrderSide::Buy,
            OrderKind::PostOnly,
            &size,
            Some(&price),
            &client_order_id,
        )
        .await;
    eprintln!(
        "OKX Demo CAA expiry post-only place acknowledgement completed in {} ms",
        place_started.elapsed().as_millis()
    );

    let mut failures = Vec::new();
    match acknowledgement {
        Ok(acknowledgement) if acknowledgement.client_order_id == client_order_id => {
            let live_result = observer
                .wait_for_order(PrivateOrderExpectation {
                    stage: "CAA expiry place",
                    instrument_id,
                    order_id: &acknowledgement.order_id,
                    client_order_id: &client_order_id,
                    price: &price,
                    size: &size,
                    state: ExpectedPrivateOrderState::Live,
                    command_started_at: place_started,
                    timeout: PRIVATE_EVENT_TIMEOUT,
                })
                .await;
            if let Err(error) = live_result {
                failures.push(format!(
                    "private orders stream did not correlate the CAA-protected live order: {error:#}"
                ));
                cancel_fallback(client, instrument_id, &client_order_id, &mut failures).await;
            } else if let Err(error) = observer
                .wait_for_order(PrivateOrderExpectation {
                    stage: "CAA expiry cancellation",
                    instrument_id,
                    order_id: &acknowledgement.order_id,
                    client_order_id: &client_order_id,
                    price: &price,
                    size: &size,
                    state: ExpectedPrivateOrderState::Canceled,
                    command_started_at: arm_started,
                    timeout: CAA_EXPIRY_EVENT_TIMEOUT,
                })
                .await
            {
                failures.push(format!(
                    "private orders stream did not observe CAA expiry cancellation: {error:#}"
                ));
                cancel_fallback(client, instrument_id, &client_order_id, &mut failures).await;
            } else {
                eprintln!(
                    "OKX Demo CAA expiry cancellation observed after {} ms",
                    arm_started.elapsed().as_millis()
                );
            }
        }
        Ok(_) => {
            failures.push(
                "CAA expiry place acknowledgement returned an unexpected client order id"
                    .to_owned(),
            );
            cancel_fallback(client, instrument_id, &client_order_id, &mut failures).await;
        }
        Err(error) => {
            failures.push(format!("CAA-protected post-only place failed: {error:#}"));
            cancel_fallback(client, instrument_id, &client_order_id, &mut failures).await;
        }
    }

    let lifecycle = finish_order_lifecycle(client, instrument_id, &client_order_id, failures).await;
    finalize_order_lifecycle(client, lifecycle).await
}

async fn cancel_fallback(
    client: &OkxRestClient,
    instrument_id: &str,
    client_order_id: &str,
    failures: &mut Vec<String>,
) {
    let cancel_started = Instant::now();
    if let Err(error) = client.cancel_order(instrument_id, client_order_id).await {
        failures.push(format!("REST cancel cleanup fallback failed: {error:#}"));
    }
    eprintln!(
        "OKX Demo CAA expiry REST cancel fallback completed in {} ms",
        cancel_started.elapsed().as_millis()
    );
}
