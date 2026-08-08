use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};

use super::{
    demo_order_smoke::{
        PostOnlyPrice, SmokeOrderLifecycle, finalize_order_lifecycle, finish_order_lifecycle,
        prepare_post_only_order, smoke_client_order_id,
    },
    demo_websocket_order_smoke::{connect, websocket_request_id},
};
use crate::{
    config::types::BotConfig,
    okx::{
        client::{OKX_CANCEL_ALL_AFTER_TAG, OkxCancelAllAfterTimeout, OkxRestClient},
        trading_instrument::ValidatedTradingInstrument,
        types::{OrderKind, OrderSide, decimal_to_okx},
        websocket::trading::OkxWebsocketPlaceOrder,
    },
};

const EXPIRY_WAIT: Duration = Duration::from_secs(3);

pub(super) async fn run_expired_websocket_place_smoke(
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
    let mut transport = connect(client, config, instrument).await?;

    let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
    client
        .cancel_all_after(timeout)
        .await
        .context("OKX Demo expired WebSocket smoke refused to send because CAA arm failed")?;
    let exp_time = client
        .prepare_websocket_place_order(
            instrument_id,
            OrderSide::Buy,
            OrderKind::PostOnly,
            Some(&price),
        )
        .await
        .context("OKX Demo expired WebSocket smoke could not prepare expTime")?;
    let validated = client
        .validated_trading_instrument(instrument_id)
        .context("OKX Demo expired WebSocket smoke missing validated trading context")?;
    tokio::time::sleep(EXPIRY_WAIT).await;
    let request_id = websocket_request_id("WSE")?;

    let send_started = Instant::now();
    let result = transport
        .session
        .place_order(OkxWebsocketPlaceOrder {
            td_mode: validated.td_mode().as_okx(),
            id: &request_id,
            inst_id_code: transport.inst_id_code,
            exp_time: &exp_time,
            side: OrderSide::Buy,
            kind: OrderKind::PostOnly,
            size: &size,
            price: Some(&price),
            trade_quote_currency: validated.trade_quote_ccy(),
            client_order_id: &client_order_id,
            tag: OKX_CANCEL_ALL_AFTER_TAG,
        })
        .await;
    eprintln!(
        "OKX Demo expired WebSocket place acknowledgement completed in {} ms",
        send_started.elapsed().as_millis()
    );

    match result {
        Ok(_) => {
            let failures = vec![
                "OKX accepted a WebSocket place request after its expTime deadline".to_owned(),
            ];
            if let Err(error) = client.cancel_order(instrument_id, &client_order_id).await {
                let mut failures = failures;
                failures.push(format!("REST cancel cleanup fallback failed: {error:#}"));
                let lifecycle =
                    finish_order_lifecycle(client, instrument_id, &client_order_id, failures).await;
                return finalize_order_lifecycle(client, lifecycle).await;
            }
            let lifecycle =
                finish_order_lifecycle(client, instrument_id, &client_order_id, failures).await;
            finalize_order_lifecycle(client, lifecycle).await
        }
        Err(error) => {
            let rejection = error.to_string();
            let lowercase = rejection.to_ascii_lowercase();
            let mut failures = Vec::new();
            if !lowercase.contains("rejected") || !lowercase.contains("exp") {
                failures.push(format!(
                    "WebSocket request failed without an explicit expiry rejection: {rejection}"
                ));
            }
            let lifecycle =
                verify_rejected_order_absent(client, instrument_id, &client_order_id, failures)
                    .await;
            finalize_order_lifecycle(client, lifecycle).await
        }
    }
}

async fn verify_rejected_order_absent(
    client: &OkxRestClient,
    instrument_id: &str,
    client_order_id: &str,
    mut failures: Vec<String>,
) -> SmokeOrderLifecycle {
    let no_order = match client.order(instrument_id, client_order_id).await {
        Ok(None) => true,
        Ok(Some(order)) => {
            failures.push(format!(
                "expired WebSocket request created order state {}",
                order.state
            ));
            false
        }
        Err(error) => {
            failures.push(format!(
                "expired WebSocket order REST lookup failed: {error:#}"
            ));
            false
        }
    };
    let no_open_orders = match client.open_orders(instrument_id).await {
        Ok(open_orders) if open_orders.is_empty() => true,
        Ok(open_orders) => {
            failures.push(format!(
                "expired WebSocket request left {} open {instrument_id} orders",
                open_orders.len()
            ));
            false
        }
        Err(error) => {
            failures.push(format!(
                "expired WebSocket open-order reconciliation failed: {error:#}"
            ));
            false
        }
    };
    let result = if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join("; ")))
    };
    SmokeOrderLifecycle {
        cleanup_verified: no_order && no_open_orders,
        result,
    }
}
