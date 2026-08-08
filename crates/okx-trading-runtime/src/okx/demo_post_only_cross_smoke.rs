use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use super::{
    demo_order_smoke::{
        PostOnlyPrice, finalize_order_lifecycle, finish_order_lifecycle, prepare_post_only_order,
        smoke_client_order_id,
    },
    demo_private_order_observer::{
        DemoPrivateOrderObserver, ExpectedPrivateOrderState, PrivateOrderExpectation,
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

const POST_ONLY_CANCEL_TIMEOUT: Duration = Duration::from_secs(3);

pub(super) async fn run_crossing_post_only_smoke(
    client: &OkxRestClient,
    config: &BotConfig,
    validated: &ValidatedTradingInstrument,
) -> Result<()> {
    let prepared = prepare_post_only_order(client, validated, PostOnlyPrice::Crossing).await?;
    let instrument = validated.instrument();
    let instrument_id = &instrument.inst_id;
    let size = decimal_to_okx(prepared.size);
    let price = decimal_to_okx(prepared.price);
    let client_order_id = smoke_client_order_id()?;
    let observer = DemoPrivateOrderObserver::connect(client, config, instrument_id).await?;

    let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
    client
        .cancel_all_after(timeout)
        .await
        .context("OKX Demo crossing post-only smoke refused to place because CAA arm failed")?;

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
        "OKX Demo crossing post-only place acknowledgement completed in {} ms",
        place_started.elapsed().as_millis()
    );

    let mut failures = Vec::new();
    match acknowledgement {
        Ok(acknowledgement) if acknowledgement.client_order_id == client_order_id => {
            if let Err(error) = observer
                .wait_for_order(PrivateOrderExpectation {
                    stage: "crossing post-only cancellation",
                    instrument_id,
                    order_id: &acknowledgement.order_id,
                    client_order_id: &client_order_id,
                    price: &price,
                    size: &size,
                    state: ExpectedPrivateOrderState::Canceled,
                    command_started_at: place_started,
                    timeout: POST_ONLY_CANCEL_TIMEOUT,
                })
                .await
            {
                failures.push(format!(
                    "crossing post-only order was not canceled immediately without a fill: {error:#}"
                ));
                cancel_fallback(client, instrument_id, &client_order_id, &mut failures).await;
            }
        }
        Ok(_) => {
            failures.push(
                "crossing post-only acknowledgement returned an unexpected client order id"
                    .to_owned(),
            );
            cancel_fallback(client, instrument_id, &client_order_id, &mut failures).await;
        }
        Err(error) => {
            failures.push(format!("crossing post-only place failed: {error:#}"));
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
    if let Err(error) = client.cancel_order(instrument_id, client_order_id).await {
        failures.push(format!("REST cancel cleanup fallback failed: {error:#}"));
    }
}
