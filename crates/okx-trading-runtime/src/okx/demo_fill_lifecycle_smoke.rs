use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, ensure};
use rust_decimal::Decimal;

use super::demo_order_smoke::wait_for_terminal_order;
use crate::okx::{
    client::{OkxCancelAllAfterTimeout, OkxRestClient},
    trading_instrument::ValidatedTradingInstrument,
    types::{
        OkxBalance, OkxInstrument, OkxOrder, OkxSpotFeeType, OkxSpotFillAccounting, OrderKind,
        OrderSide, decimal_to_okx, quantize_decimal_down, quantize_decimal_up,
    },
};

const BALANCE_RECONCILE_ATTEMPTS: usize = 12;
const BALANCE_RECONCILE_DELAY: Duration = Duration::from_millis(250);
const HARD_QUOTE_NOTIONAL_CAP: Decimal = Decimal::from_parts(20, 0, 0, false, 0);
const CROSSING_PRICE_MULTIPLIER: Decimal = Decimal::from_parts(1005, 0, 0, false, 3);

pub(super) async fn run_fill_lifecycle_smoke(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
) -> Result<()> {
    let instrument = validated.instrument();
    let instrument_id = &instrument.inst_id;
    ensure_no_open_orders(client, instrument_id, "preflight").await?;
    let initial_balances = client
        .balances()
        .await
        .context("OKX Demo fill smoke balance preflight failed")?;
    let initial_base = currency_balance(&initial_balances, &instrument.base_ccy)?;
    let initial_quote = currency_balance(&initial_balances, &instrument.quote_ccy)?;
    validate_unfrozen_base_baseline(initial_base, &instrument.base_ccy)?;

    let account_config = client
        .account_config()
        .await
        .context("OKX Demo fill smoke account-config preflight failed")?;
    account_config.ensure_spot_trading_enabled()?;
    let fee_type = account_config.spot_fee_type()?;
    let fee_rate = client
        .spot_trade_fee(instrument_id)
        .await
        .context("OKX Demo fill smoke fee preflight failed")?;
    let taker_cost_rate = fee_rate.normalized_taker_cost_rate()?.max(Decimal::ZERO);
    ensure!(
        taker_cost_rate < Decimal::ONE,
        "OKX Demo fill smoke taker cost rate must be below one"
    );

    let ticker = client
        .ticker(instrument_id)
        .await
        .context("OKX Demo fill smoke ticker preflight failed")?;
    ticker.validate_prices()?;
    let price = quantize_decimal_up(
        ticker.ask_decimal()? * CROSSING_PRICE_MULTIPLIER,
        instrument.tick_size()?,
    )?;
    let minimum_base = instrument.min_size()?;
    let size_before_lot_rounding = match fee_type {
        OkxSpotFeeType::ReceivedCurrency => minimum_base / (Decimal::ONE - taker_cost_rate),
        OkxSpotFeeType::QuoteCurrency => minimum_base,
    };
    let size = quantize_decimal_up(size_before_lot_rounding, instrument.lot_size()?)?;
    instrument.ensure_limit_size(size, "OKX Demo fill smoke crossing buy size")?;
    let notional = price
        .checked_mul(size)
        .context("OKX Demo fill smoke quote notional overflowed Decimal")?;
    ensure!(
        notional <= HARD_QUOTE_NOTIONAL_CAP,
        "OKX Demo fill smoke quote notional {notional} {} exceeds hard cap {HARD_QUOTE_NOTIONAL_CAP}",
        instrument.quote_ccy
    );
    if instrument.max_limit_amount()?.is_some() {
        let quote_usd_rate = client.fresh_quote_usd_rate(validated).await?;
        validated.ensure_limit_quote_amount(
            notional,
            &quote_usd_rate,
            "OKX Demo fill smoke crossing buy notional",
        )?;
    }
    let required_quote = notional
        .checked_mul(Decimal::ONE + taker_cost_rate)
        .context("OKX Demo fill smoke fee-adjusted quote requirement overflowed Decimal")?;
    ensure!(
        initial_quote.available >= required_quote,
        "OKX Demo fill smoke requires sufficient available {} for notional and taker cost",
        instrument.quote_ccy
    );

    let (buy_client_order_id, sell_client_order_id) = fill_client_order_ids()?;
    client
        .cancel_all_after(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?)
        .await
        .context("OKX Demo fill smoke refused to trade because Cancel-All-After arm failed")?;

    let mut failures = Vec::new();
    let buy_result = client
        .place_order(
            instrument_id,
            OrderSide::Buy,
            OrderKind::Limit,
            &decimal_to_okx(size),
            Some(&decimal_to_okx(price)),
            &buy_client_order_id,
        )
        .await;
    if let Err(error) = buy_result {
        failures.push(format!(
            "crossing limit buy failed or remained ambiguous: {error:#}"
        ));
    }
    if let Err(error) = client
        .cancel_order(instrument_id, &buy_client_order_id)
        .await
    {
        failures.push(format!("crossing buy remainder cancel failed: {error:#}"));
    }
    let buy_order = match wait_for_terminal_order(client, instrument_id, &buy_client_order_id).await
    {
        Ok(order) => Some(order),
        Err(error) => {
            failures.push(format!(
                "crossing buy terminal reconciliation failed: {error:#}"
            ));
            None
        }
    };

    let acquired_base = wait_for_acquired_base(client, instrument, initial_base).await;
    let sell_order = match acquired_base {
        Ok(acquired_base) if acquired_base >= minimum_base => {
            let sell_size = quantize_decimal_down(acquired_base, instrument.lot_size()?)?;
            let market_reference_price = client
                .ticker(instrument_id)
                .await
                .context("OKX Demo fill smoke cleanup ticker lookup failed")?
                .ask_decimal()?;
            instrument.ensure_spot_market_sell_size(
                sell_size,
                market_reference_price,
                "OKX Demo fill smoke market sell size",
            )?;
            if let Err(error) = client
                .place_order(
                    instrument_id,
                    OrderSide::Sell,
                    OrderKind::Market,
                    &decimal_to_okx(sell_size),
                    None,
                    &sell_client_order_id,
                )
                .await
            {
                failures.push(format!(
                    "market sell cleanup failed or remained ambiguous: {error:#}"
                ));
            }
            match wait_for_terminal_order(client, instrument_id, &sell_client_order_id).await {
                Ok(order) => Some(order),
                Err(error) => {
                    failures.push(format!(
                        "market sell terminal reconciliation failed: {error:#}"
                    ));
                    None
                }
            }
        }
        Ok(acquired_base) => {
            failures.push(format!(
                "crossing buy produced only {acquired_base} {}, below minSz {minimum_base}",
                instrument.base_ccy
            ));
            None
        }
        Err(error) => {
            failures.push(format!("post-buy balance reconciliation failed: {error:#}"));
            None
        }
    };

    validate_fill_evidence(
        client,
        instrument,
        &buy_client_order_id,
        &sell_client_order_id,
        buy_order.as_ref(),
        sell_order.as_ref(),
        &mut failures,
    )
    .await;
    let cleanup_result = verify_cleanup(client, instrument, initial_base).await;
    let cleanup_verified = cleanup_result.is_ok();
    match cleanup_result {
        Ok((base_residual, final_quote_total)) => {
            let quote_change = final_quote_total - initial_quote.total;
            if quote_change >= Decimal::ZERO {
                failures.push(format!(
                    "taker round trip did not produce the expected negative {} balance change; observed {quote_change}",
                    instrument.quote_ccy
                ));
            }
            eprintln!(
                "OKX Demo fill smoke preserved the initial {} balance with acquired-base residual {base_residual} and quote balance change {quote_change} {}",
                instrument.base_ccy, instrument.quote_ccy
            );
        }
        Err(error) => {
            failures.push(format!(
                "final fill-smoke cleanup verification failed: {error:#}"
            ));
        }
    }

    if cleanup_verified
        && let Err(error) = client
            .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
            .await
    {
        failures.push(format!(
            "cleanup passed but Cancel-All-After disarm failed: {error:#}"
        ));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join("; "))).context(if cleanup_verified {
            "OKX Demo fill lifecycle failed after cleanup was verified"
        } else {
            "OKX Demo fill lifecycle cleanup is ambiguous; Cancel-All-After remains armed"
        })
    }
}

async fn validate_fill_evidence(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    buy_client_order_id: &str,
    sell_client_order_id: &str,
    buy_order: Option<&OkxOrder>,
    sell_order: Option<&OkxOrder>,
    failures: &mut Vec<String>,
) {
    for (side, order) in [(OrderSide::Buy, buy_order), (OrderSide::Sell, sell_order)] {
        let Some(order) = order else { continue };
        match order.cumulative_spot_accounting(&instrument.base_ccy, &instrument.quote_ccy) {
            Ok(accounting) if fill_direction_matches(side, accounting) => {}
            Ok(accounting) => failures.push(format!(
                "{side:?} order returned inconsistent accounting {accounting:?}"
            )),
            Err(error) => failures.push(format!("{side:?} order fee accounting failed: {error:#}")),
        }
    }
    match client.order_fills(&instrument.inst_id).await {
        Ok(fills) => {
            for (side, client_order_id) in [
                (OrderSide::Buy, buy_client_order_id),
                (OrderSide::Sell, sell_client_order_id),
            ] {
                let matching = fills
                    .iter()
                    .filter(|fill| fill.client_order_id == client_order_id)
                    .collect::<Vec<_>>();
                if matching.is_empty() {
                    failures.push(format!("REST fills history omitted expected {side:?} fill"));
                    continue;
                }
                for fill in matching {
                    if fill.execution_type != "T" {
                        failures.push(format!(
                            "expected {side:?} fill to classify as taker, received {:?}",
                            fill.execution_type
                        ));
                    }
                    if let Err(error) =
                        fill.spot_accounting(&instrument.base_ccy, &instrument.quote_ccy)
                    {
                        failures.push(format!("{side:?} REST fill accounting failed: {error:#}"));
                    }
                }
            }
        }
        Err(error) => failures.push(format!(
            "REST fill-history reconciliation failed: {error:#}"
        )),
    }
}

fn fill_direction_matches(side: OrderSide, accounting: OkxSpotFillAccounting) -> bool {
    match side {
        OrderSide::Buy => {
            accounting.base_change > Decimal::ZERO && accounting.quote_change < Decimal::ZERO
        }
        OrderSide::Sell => {
            accounting.base_change < Decimal::ZERO && accounting.quote_change > Decimal::ZERO
        }
    }
}

async fn wait_for_acquired_base(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    initial_base: CurrencyBalance,
) -> Result<Decimal> {
    let mut last = Decimal::ZERO;
    for attempt in 0..BALANCE_RECONCILE_ATTEMPTS {
        let balances = client.balances().await?;
        let base = currency_balance(&balances, &instrument.base_ccy)?;
        last = base_delta_from_baseline(initial_base, base, &instrument.base_ccy)?;
        if last >= instrument.min_size()? {
            return Ok(last);
        }
        if attempt + 1 < BALANCE_RECONCILE_ATTEMPTS {
            tokio::time::sleep(BALANCE_RECONCILE_DELAY).await;
        }
    }
    Ok(last)
}

async fn verify_cleanup(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    initial_base: CurrencyBalance,
) -> Result<(Decimal, Decimal)> {
    ensure_no_open_orders(client, &instrument.inst_id, "cleanup").await?;
    let mut last_base = CurrencyBalance::default();
    let mut last_delta_error = None;
    for attempt in 0..BALANCE_RECONCILE_ATTEMPTS {
        let balances = client.balances().await?;
        let base = currency_balance(&balances, &instrument.base_ccy)?;
        let quote = currency_balance(&balances, &instrument.quote_ccy)?;
        match base_delta_from_baseline(initial_base, base, &instrument.base_ccy) {
            Ok(base_delta) if base_delta < instrument.lot_size()? => {
                return Ok((base_delta, quote.total));
            }
            Ok(_) => last_delta_error = None,
            Err(error) => last_delta_error = Some(error),
        }
        last_base = base;
        if attempt + 1 < BALANCE_RECONCILE_ATTEMPTS {
            tokio::time::sleep(BALANCE_RECONCILE_DELAY).await;
        }
    }
    if let Some(error) = last_delta_error {
        return Err(error)
            .context("OKX Demo fill smoke could not verify base baseline preservation");
    }
    let last_delta = last_base.total - initial_base.total;
    Err(anyhow!(
        "OKX Demo fill smoke left acquired-base delta {last_delta} {} at or above one lot; initial total {}, final total {}, and final frozen {}",
        instrument.base_ccy,
        initial_base.total,
        last_base.total,
        last_base.frozen
    ))
}

async fn ensure_no_open_orders(
    client: &OkxRestClient,
    instrument_id: &str,
    stage: &str,
) -> Result<()> {
    let regular = client.open_orders(instrument_id).await?;
    let algo = client.open_algo_orders(instrument_id).await?;
    ensure!(
        regular.is_empty() && algo.is_empty(),
        "OKX Demo fill smoke {stage} requires no open {instrument_id} orders; found {} regular and {} algo orders",
        regular.len(),
        algo.len()
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CurrencyBalance {
    available: Decimal,
    total: Decimal,
    frozen: Decimal,
}

fn validate_unfrozen_base_baseline(baseline: CurrencyBalance, currency: &str) -> Result<()> {
    ensure!(
        baseline.frozen == Decimal::ZERO && baseline.available == baseline.total,
        "OKX Demo fill smoke requires an unfrozen {currency} balance baseline; found available {}, total {}, and frozen {} and made no mutation",
        baseline.available,
        baseline.total,
        baseline.frozen
    );
    Ok(())
}

fn base_delta_from_baseline(
    baseline: CurrencyBalance,
    current: CurrencyBalance,
    currency: &str,
) -> Result<Decimal> {
    validate_unfrozen_base_baseline(baseline, currency)?;
    ensure!(
        current.total >= baseline.total && current.available >= baseline.available,
        "OKX Demo fill smoke refuses to consume the initial {currency} balance; baseline available/total were {}/{}, current available/total are {}/{}",
        baseline.available,
        baseline.total,
        current.available,
        current.total
    );
    ensure!(
        current.frozen == baseline.frozen,
        "OKX Demo fill smoke cannot reconcile {currency} above its baseline while frozen balance changed from {} to {}",
        baseline.frozen,
        current.frozen
    );
    let available_delta = current.available - baseline.available;
    let total_delta = current.total - baseline.total;
    ensure!(
        available_delta == total_delta,
        "OKX Demo fill smoke {currency} balance deltas disagree: available delta {available_delta}, total delta {total_delta}"
    );
    Ok(total_delta)
}

fn currency_balance(balances: &[OkxBalance], currency: &str) -> Result<CurrencyBalance> {
    let matching = balances
        .iter()
        .flat_map(|balance| &balance.details)
        .filter(|detail| detail.ccy == currency)
        .collect::<Vec<_>>();
    ensure!(
        matching.len() == 1,
        "OKX Demo fill smoke expected exactly one {currency} balance row, received {}",
        matching.len()
    );
    Ok(CurrencyBalance {
        available: matching[0].available()?,
        total: matching[0].total()?,
        frozen: matching[0].frozen()?,
    })
}

fn fill_client_order_ids() -> Result<(String, String)> {
    let unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before the Unix epoch")?
        .as_millis();
    let buy = format!("OKXFILLB{unix_millis}");
    let sell = format!("OKXFILLS{unix_millis}");
    ensure!(
        buy.len() <= 32 && sell.len() <= 32,
        "OKX Demo fill smoke client order id exceeds 32 characters"
    );
    Ok((buy, sell))
}

#[cfg(test)]
#[path = "demo_fill_lifecycle_smoke_tests.rs"]
mod tests;
