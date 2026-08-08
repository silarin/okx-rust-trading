use std::{collections::HashSet, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use rust_decimal::Decimal;

use super::demo_order_smoke::wait_for_terminal_order;
use crate::{
    config::types::BotConfig,
    okx::{
        client::{
            OKX_CANCEL_ALL_AFTER_TAG, OkxCancelAllAfterTimeout, OkxOcoAmend, OkxOcoProtection,
            OkxRestClient,
        },
        trading_instrument::ValidatedTradingInstrument,
        types::{
            OkxBalance, OkxFill, OkxInstrument, OkxMaximumAvailableSize, OkxMaximumOrderSize,
            OkxOcoOrder, OkxOrder, OkxSpotFeeType, OkxSpotFillAccounting, OrderKind, OrderSide,
            decimal_to_okx, quantize_decimal_down, quantize_decimal_up,
        },
    },
};

const RECONCILE_ATTEMPTS: usize = 20;
const RECONCILE_DELAY: Duration = Duration::from_millis(250);
const HARD_QUOTE_NOTIONAL_CAP: Decimal = Decimal::from_parts(20, 0, 0, false, 0);
const CROSSING_PRICE_MULTIPLIER: Decimal = Decimal::from_parts(1005, 0, 0, false, 3);
const OCO_CLIENT_ID_PREFIX: &str = "OKXOCO";

pub(super) async fn run_acquisition_probe(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
) -> Result<()> {
    let instrument = validated.instrument();
    ensure_exact_demo_contract(client, instrument).await?;
    let operator_baseline = base_balance(client, instrument).await?;
    ensure_unfrozen_baseline(operator_baseline, &instrument.base_ccy)?;
    let plan = preflight_acquisition_capacity(client, validated).await?;
    let run_identity = AcquisitionRunIdentity::from_client(client).await?;

    client
        .cancel_all_after(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?)
        .await
        .context(
            "OKX Demo acquisition probe refused mutation because Cancel-All-After arm failed",
        )?;

    let acquisition = acquire_disposable_delta_with_identity(
        client,
        instrument,
        operator_baseline,
        &run_identity.acquisition_client_order_id,
        plan,
    )
    .await;
    let cleanup =
        cleanup_acquisition_probe(client, instrument, operator_baseline, &run_identity, plan).await;
    match (acquisition, cleanup) {
        (Ok(acquired), Ok(())) => {
            client
                .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
                .await
                .context(
                    "OKX Demo acquisition cleanup passed but Cancel-All-After disarm failed",
                )?;
            eprintln!(
                "OKX Demo acquisition probe passed with a fee-adjusted disposable delta of {} planned lots; protected baseline preserved; regular, trigger, and OCO order lists empty; Cancel-All-After disarmed",
                acquired.protected_size / instrument.lot_size()?
            );
            Ok(())
        }
        (Err(acquisition_error), Ok(())) => {
            let disarm = client
                .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
                .await;
            match disarm {
                Ok(_) => Err(acquisition_error).context(
                    "OKX Demo acquisition probe failed after deterministic cleanup; Cancel-All-After disarmed",
                ),
                Err(disarm_error) => Err(anyhow!(
                    "OKX Demo acquisition probe failed: {acquisition_error:#}; cleanup passed but Cancel-All-After disarm failed: {disarm_error:#}"
                )),
            }
        }
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error).context(
            "OKX Demo acquisition probe cleanup is ambiguous; Cancel-All-After remains armed",
        ),
        (Err(acquisition_error), Err(cleanup_error)) => Err(anyhow!(
            "OKX Demo acquisition probe failed: {acquisition_error:#}; documented REST cleanup also failed: {cleanup_error:#}; Cancel-All-After remains armed"
        )),
    }
}

pub(super) async fn run_spot_oco_lifecycle_smoke(
    client: &OkxRestClient,
    _config: &BotConfig,
    validated: &ValidatedTradingInstrument,
) -> Result<()> {
    let instrument = validated.instrument();
    ensure_exact_demo_contract(client, instrument).await?;
    let operator_baseline = base_balance(client, instrument).await?;
    ensure_unfrozen_baseline(operator_baseline, &instrument.base_ccy)?;

    client
        .cancel_all_after(OkxCancelAllAfterTimeout::new(
            OkxCancelAllAfterTimeout::MIN_SECONDS,
        )?)
        .await
        .context("OKX Demo OCO smoke refused mutation because Cancel-All-After arm failed")?;

    let result = run_all_scenarios(client, validated, operator_baseline).await;
    let cleanup = cleanup_test_owned_state(client, instrument, operator_baseline).await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => client
            .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
            .await
            .context("OKX Demo OCO cleanup passed but Cancel-All-After disarm failed")
            .map(|_| ()),
        (Err(scenario_error), Ok(())) => {
            let disarm = client
                .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
                .await;
            match disarm {
                Ok(_) => Err(scenario_error)
                    .context("OKX Demo OCO scenario failed after deterministic cleanup"),
                Err(disarm_error) => Err(anyhow!(
                    "OKX Demo OCO scenario failed: {scenario_error:#}; cleanup passed but Cancel-All-After disarm failed: {disarm_error:#}"
                )),
            }
        }
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error).context(
            "OKX Demo OCO cleanup is ambiguous; Cancel-All-After remains armed for regular orders",
        ),
        (Err(scenario_error), Err(cleanup_error)) => Err(anyhow!(
            "OKX Demo OCO scenario failed: {scenario_error:#}; documented REST cleanup also failed: {cleanup_error:#}; Cancel-All-After remains armed for regular orders"
        )),
    }
}

async fn run_all_scenarios(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
    operator_baseline: CurrencyBalance,
) -> Result<()> {
    run_cancel_scenario(client, validated, operator_baseline).await?;
    run_execution_scenario(
        client,
        validated,
        operator_baseline,
        ExecutionSide::TakeProfit,
    )
    .await?;
    run_execution_scenario(
        client,
        validated,
        operator_baseline,
        ExecutionSide::StopLoss,
    )
    .await?;
    run_restart_amend_scenario(client, validated, operator_baseline).await
}

async fn run_cancel_scenario(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
    operator_baseline: CurrencyBalance,
) -> Result<()> {
    let instrument = validated.instrument();
    ensure_clean_state(client, &instrument.inst_id, "placement/cancel preflight").await?;
    let scenario_baseline = base_balance(client, instrument).await?;
    ensure_operator_baseline(operator_baseline, scenario_baseline, &instrument.base_ccy)?;
    let acquired = acquire_disposable_delta(client, validated, scenario_baseline, 'C').await?;
    let prices = passive_oco_prices(client, instrument).await?;
    let client_order_id = scenario_client_id(client, 'C', "O").await?;
    let ack = client
        .place_spot_oco(OkxOcoProtection {
            inst_id: &instrument.inst_id,
            size: &decimal_to_okx(acquired.protected_size),
            take_profit_trigger_price: &decimal_to_okx(prices.take_profit),
            stop_loss_trigger_price: &decimal_to_okx(prices.stop_loss),
            client_order_id: &client_order_id,
        })
        .await
        .context("OKX Demo placement/cancel OCO placement failed or remained ambiguous")?;
    let placed = wait_for_oco(
        client,
        instrument,
        &client_order_id,
        ExpectedOcoState::Pending,
    )
    .await?;
    ensure!(
        placed.algo_id == ack.algo_id && placed.tag == OKX_CANCEL_ALL_AFTER_TAG,
        "OKX Demo placement/cancel OCO REST detail did not preserve stable algo identity and tag"
    );
    ensure_protected_quantity(&placed, acquired.protected_size)?;
    ensure_baseline_is_not_protected(
        client,
        instrument,
        scenario_baseline,
        acquired.protected_size,
    )
    .await?;

    client
        .cancel_spot_oco(&instrument.inst_id, &placed.algo_id)
        .await
        .context("OKX Demo placement/cancel OCO cancellation failed")?;
    let canceled = wait_for_oco(
        client,
        instrument,
        &client_order_id,
        ExpectedOcoState::Canceled,
    )
    .await?;
    ensure_history_matches(client, instrument, &canceled).await?;
    ensure_no_pending_oco(client, instrument, &canceled.algo_id).await?;
    liquidate_disposable_delta(client, instrument, scenario_baseline, 'C').await?;
    ensure_clean_state(client, &instrument.inst_id, "placement/cancel cleanup").await
}

async fn run_execution_scenario(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
    operator_baseline: CurrencyBalance,
    side: ExecutionSide,
) -> Result<()> {
    let instrument = validated.instrument();
    let marker = side.marker();
    ensure_clean_state(client, &instrument.inst_id, side.preflight_label()).await?;
    let scenario_baseline = base_balance(client, instrument).await?;
    ensure_operator_baseline(operator_baseline, scenario_baseline, &instrument.base_ccy)?;
    let acquired = acquire_disposable_delta(client, validated, scenario_baseline, marker).await?;
    let prices = immediate_oco_prices(client, instrument, side).await?;
    let client_order_id = scenario_client_id(client, marker, "O").await?;
    let ack = client
        .place_spot_oco(OkxOcoProtection {
            inst_id: &instrument.inst_id,
            size: &decimal_to_okx(acquired.protected_size),
            take_profit_trigger_price: &decimal_to_okx(prices.take_profit),
            stop_loss_trigger_price: &decimal_to_okx(prices.stop_loss),
            client_order_id: &client_order_id,
        })
        .await
        .with_context(|| format!("OKX Demo {} OCO placement failed", side.label()))?;
    let terminal = wait_for_oco(
        client,
        instrument,
        &client_order_id,
        ExpectedOcoState::Executed(side.actual_side()),
    )
    .await?;
    ensure!(terminal.algo_id == ack.algo_id, "OCO algo identity changed");
    terminal.ensure_clean_execution(side.actual_side())?;
    ensure_history_matches(client, instrument, &terminal).await?;
    ensure_execution_fill_evidence(client, instrument, &terminal).await?;
    ensure_no_pending_oco(client, instrument, &terminal.algo_id).await?;
    wait_for_delta_below_lot(client, instrument, scenario_baseline).await?;
    ensure_clean_state(client, &instrument.inst_id, side.cleanup_label()).await
}

async fn run_restart_amend_scenario(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
    operator_baseline: CurrencyBalance,
) -> Result<()> {
    let instrument = validated.instrument();
    ensure_clean_state(client, &instrument.inst_id, "restart/amend preflight").await?;
    let scenario_baseline = base_balance(client, instrument).await?;
    ensure_operator_baseline(operator_baseline, scenario_baseline, &instrument.base_ccy)?;
    let acquired = acquire_disposable_delta(client, validated, scenario_baseline, 'R').await?;
    let prices = passive_oco_prices(client, instrument).await?;
    let client_order_id = scenario_client_id(client, 'R', "O").await?;
    let ack = client
        .place_spot_oco(OkxOcoProtection {
            inst_id: &instrument.inst_id,
            size: &decimal_to_okx(acquired.protected_size),
            take_profit_trigger_price: &decimal_to_okx(prices.take_profit),
            stop_loss_trigger_price: &decimal_to_okx(prices.stop_loss),
            client_order_id: &client_order_id,
        })
        .await?;

    let stable_algo_id = ack.algo_id.clone();
    drop(ack);
    let rediscovered = wait_for_oco(
        client,
        instrument,
        &client_order_id,
        ExpectedOcoState::Pending,
    )
    .await
    .context("OKX Demo OCO restart discovery failed")?;
    ensure!(
        rediscovered.algo_id == stable_algo_id && rediscovered.client_order_id == client_order_id,
        "OKX Demo OCO restart discovery could not correlate stable identifiers"
    );

    let lot = instrument.lot_size()?;
    let new_size = acquired.protected_size - lot;
    ensure!(
        new_size >= instrument.min_size()?,
        "OKX Demo OCO acquisition did not leave enough quantity for a one-lot resize"
    );
    let tick = instrument.tick_size()?;
    let new_take_profit = prices.take_profit - tick;
    let new_stop_loss = prices.stop_loss + tick;
    ensure!(
        new_take_profit > new_stop_loss,
        "OKX Demo OCO one-tick amendment would invert triggers"
    );
    client
        .amend_spot_oco(OkxOcoAmend {
            inst_id: &instrument.inst_id,
            algo_id: &rediscovered.algo_id,
            client_order_id: &client_order_id,
            new_size: &decimal_to_okx(new_size),
            new_take_profit_trigger_price: &decimal_to_okx(new_take_profit),
            new_stop_loss_trigger_price: &decimal_to_okx(new_stop_loss),
        })
        .await
        .context("OKX Demo OCO quantity/price amendment failed")?;
    let amended = wait_for_oco(
        client,
        instrument,
        &client_order_id,
        ExpectedOcoState::Pending,
    )
    .await?;
    ensure!(
        amended.requested_size()? == new_size
            && amended.take_profit_trigger_price()? == new_take_profit
            && amended.stop_loss_trigger_price()? == new_stop_loss,
        "OKX Demo OCO REST detail did not confirm the exact amended quantity and triggers"
    );
    client
        .cancel_spot_oco(&instrument.inst_id, &amended.algo_id)
        .await?;
    let canceled = wait_for_oco(
        client,
        instrument,
        &client_order_id,
        ExpectedOcoState::Canceled,
    )
    .await?;
    ensure_history_matches(client, instrument, &canceled).await?;
    liquidate_disposable_delta(client, instrument, scenario_baseline, 'R').await?;
    ensure_clean_state(client, &instrument.inst_id, "restart/amend cleanup").await
}

async fn ensure_exact_demo_contract(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
) -> Result<()> {
    let validated = client
        .validated_trading_instrument(&instrument.inst_id)
        .context("OKX Demo OCO requires the immutable validated functional-test tuple")?;
    ensure!(
        validated.inst_id() == instrument.inst_id
            && validated.inst_type().as_okx() == instrument.inst_type
            && validated.base_ccy() == instrument.base_ccy
            && validated.quote_ccy() == instrument.quote_ccy
            && validated.trade_quote_ccy() == instrument.quote_ccy,
        "OKX Demo OCO instrument contradicts the immutable validated functional-test tuple"
    );
    instrument.ensure_live()?;
    instrument.validate_order_limits()?;
    let account = client.account_config().await?;
    account.ensure_spot_trading_enabled()?;
    client
        .spot_trade_fee(&instrument.inst_id)
        .await?
        .ensure_spot(&instrument.inst_id)?;
    ensure_clean_state(client, &instrument.inst_id, "initial preflight").await
}

async fn ensure_clean_state(client: &OkxRestClient, inst_id: &str, stage: &str) -> Result<()> {
    let regular = client.open_orders(inst_id).await?;
    let triggers = client.open_algo_orders(inst_id).await?;
    let oco = client.open_spot_oco_orders(inst_id).await?;
    ensure!(
        regular.is_empty() && triggers.is_empty() && oco.is_empty(),
        "OKX Demo OCO {stage} requires no open {inst_id} state; found {} regular, {} trigger, and {} OCO orders",
        regular.len(),
        triggers.len(),
        oco.len()
    );
    Ok(())
}

async fn preflight_acquisition_capacity(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
) -> Result<AcquisitionPlan> {
    let instrument = validated.instrument();
    let account = client.account_config().await?;
    account.ensure_spot_trading_enabled()?;
    let fee_type = account.spot_fee_type()?;
    let fee = client.spot_trade_fee(&instrument.inst_id).await?;
    fee.ensure_spot(&instrument.inst_id)?;
    let taker_cost_rate = fee.normalized_taker_cost_rate()?.max(Decimal::ZERO);
    ensure!(
        taker_cost_rate < Decimal::ONE,
        "Demo taker cost must be below one"
    );

    let ticker = client.ticker(&instrument.inst_id).await?;
    ticker.validate_prices()?;
    let plan = acquisition_plan(instrument, fee_type, taker_cost_rate, ticker.ask_decimal()?)?;
    if instrument.max_limit_amount()?.is_some() {
        let quote_usd_rate = client.fresh_quote_usd_rate(validated).await?;
        let notional = plan
            .price
            .checked_mul(plan.size)
            .context("OKX Demo acquisition notional overflowed Decimal")?;
        validated.ensure_limit_quote_amount(
            notional,
            &quote_usd_rate,
            "OKX Demo acquisition notional",
        )?;
    }
    let price = decimal_to_okx(plan.price);
    let maximum = client
        .maximum_order_size(
            &instrument.inst_id,
            validated.td_mode().as_okx(),
            &price,
            validated.trade_quote_ccy(),
        )
        .await
        .context("OKX Demo acquisition max-size preflight failed")?;
    let available = client
        .maximum_available_size(
            &instrument.inst_id,
            validated.td_mode().as_okx(),
            validated.trade_quote_ccy(),
        )
        .await
        .context("OKX Demo acquisition max-avail-size preflight failed")?;
    let balances = client
        .balances()
        .await
        .context("OKX Demo acquisition balance capacity preflight failed")?;
    let base = currency_balance(&balances, &instrument.base_ccy)?;
    let quote = currency_balance(&balances, &instrument.quote_ccy)?;
    validate_acquisition_capacity(instrument, plan, &maximum, &available, base, quote)?;
    eprintln!(
        "OKX Demo acquisition capacity preflight passed: instId={} tdMode={}; price=current ask plus 0.5%, rounded up to tickSz; size=fee-adjusted (minSz + one lot), rounded up to lotSz; maxBuy admits planned base size; availBuy admits exact required quote; maxSell and availSell units validated; acquisition cost remains within 20 {}",
        validated.inst_id(),
        validated.td_mode().as_okx(),
        validated.trade_quote_ccy(),
    );
    Ok(plan)
}

fn acquisition_plan(
    instrument: &OkxInstrument,
    fee_type: OkxSpotFeeType,
    taker_cost_rate: Decimal,
    current_ask: Decimal,
) -> Result<AcquisitionPlan> {
    ensure!(
        taker_cost_rate >= Decimal::ZERO && taker_cost_rate < Decimal::ONE,
        "OKX Demo acquisition taker cost rate must be in [0, 1)"
    );
    let price = quantize_decimal_up(
        current_ask * CROSSING_PRICE_MULTIPLIER,
        instrument.tick_size()?,
    )?;
    let lot = instrument.lot_size()?;
    let target_net = instrument.min_size()? + lot;
    let gross = match fee_type {
        OkxSpotFeeType::ReceivedCurrency => target_net / (Decimal::ONE - taker_cost_rate),
        OkxSpotFeeType::QuoteCurrency => target_net,
    };
    let size = quantize_decimal_up(gross, lot)?;
    instrument.ensure_limit_size(size, "OKX Demo acquisition size")?;
    let notional = price
        .checked_mul(size)
        .context("OKX Demo acquisition notional overflowed Decimal")?;
    let required_quote = match fee_type {
        OkxSpotFeeType::ReceivedCurrency => notional,
        OkxSpotFeeType::QuoteCurrency => notional
            .checked_mul(Decimal::ONE + taker_cost_rate)
            .context("OKX Demo acquisition fee-adjusted quote requirement overflowed Decimal")?,
    };
    ensure!(
        required_quote <= HARD_QUOTE_NOTIONAL_CAP,
        "OKX Demo acquisition cost exceeds the {} {} hard cap",
        HARD_QUOTE_NOTIONAL_CAP,
        instrument.quote_ccy
    );
    let expected_net_base = match fee_type {
        OkxSpotFeeType::ReceivedCurrency => size * (Decimal::ONE - taker_cost_rate),
        OkxSpotFeeType::QuoteCurrency => size,
    };
    let protected_size = quantize_decimal_down(expected_net_base, lot)?;
    ensure!(
        protected_size >= instrument.min_size()? + lot,
        "OKX Demo acquisition plan cannot preserve a valid one-lot OCO resize after taker fees"
    );
    Ok(AcquisitionPlan {
        price,
        size,
        required_quote,
    })
}

fn validate_acquisition_capacity(
    instrument: &OkxInstrument,
    plan: AcquisitionPlan,
    maximum: &OkxMaximumOrderSize,
    available: &OkxMaximumAvailableSize,
    base_balance: CurrencyBalance,
    quote_balance: CurrencyBalance,
) -> Result<()> {
    ensure!(
        maximum.inst_id == instrument.inst_id && available.inst_id == instrument.inst_id,
        "OKX Demo acquisition sizing endpoints returned a different instrument identity"
    );
    maximum.ensure_cash_spot_margin_currency(&instrument.base_ccy)?;
    ensure!(
        maximum.max_buy_base()? >= plan.size,
        "OKX Demo acquisition maxBuy is below the planned base size"
    );
    maximum.max_sell_quote()?;
    ensure!(
        available.available_buy_quote()? >= plan.required_quote,
        "OKX Demo acquisition availBuy is below the required quote capacity"
    );
    let available_sell = available.available_sell_base()?;
    ensure!(
        available.available_buy_quote()? == quote_balance.available
            && available_sell == base_balance.available,
        "OKX Demo acquisition sizing endpoints contradict the unfrozen account balances"
    );
    Ok(())
}

async fn acquire_disposable_delta(
    client: &OkxRestClient,
    validated: &ValidatedTradingInstrument,
    baseline: CurrencyBalance,
    marker: char,
) -> Result<DisposableDelta> {
    let instrument = validated.instrument();
    ensure_unfrozen_baseline(baseline, &instrument.base_ccy)?;
    let plan = preflight_acquisition_capacity(client, validated).await?;
    acquire_disposable_delta_with_plan(client, instrument, baseline, marker, plan).await
}

async fn acquire_disposable_delta_with_plan(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    baseline: CurrencyBalance,
    marker: char,
    plan: AcquisitionPlan,
) -> Result<DisposableDelta> {
    let client_order_id = scenario_client_id(client, marker, "B").await?;
    acquire_disposable_delta_with_identity(client, instrument, baseline, &client_order_id, plan)
        .await
}

async fn acquire_disposable_delta_with_identity(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    baseline: CurrencyBalance,
    client_order_id: &str,
    plan: AcquisitionPlan,
) -> Result<DisposableDelta> {
    ensure_unfrozen_baseline(baseline, &instrument.base_ccy)?;
    let place_result = client
        .place_order(
            &instrument.inst_id,
            OrderSide::Buy,
            OrderKind::Limit,
            &decimal_to_okx(plan.size),
            Some(&decimal_to_okx(plan.price)),
            client_order_id,
        )
        .await;
    let cancel_result = client
        .cancel_order(&instrument.inst_id, client_order_id)
        .await;
    if let Err(place_error) = place_result {
        return Err(place_error)
            .context("OKX Demo OCO acquisition placement failed or was ambiguous");
    }
    cancel_result.context("OKX Demo OCO acquisition remainder cancellation failed")?;
    let order = wait_for_terminal_order(client, &instrument.inst_id, client_order_id).await?;
    let accounting =
        order.cumulative_spot_accounting(&instrument.base_ccy, &instrument.quote_ccy)?;
    ensure!(
        accounting.base_change > Decimal::ZERO && accounting.quote_change < Decimal::ZERO,
        "OKX Demo OCO acquisition did not produce a positive fee-adjusted base delta"
    );
    ensure_acquisition_fill_evidence(client, instrument, client_order_id, accounting).await?;
    let delta = wait_for_positive_delta(client, instrument, baseline).await?;
    let lot = instrument.lot_size()?;
    let protected_size = quantize_decimal_down(delta, lot)?;
    ensure!(
        protected_size >= instrument.min_size()? && protected_size <= accounting.base_change,
        "OKX Demo OCO acquired delta {delta} cannot establish a minimum exact fee-adjusted protected size from REST fill accounting {}",
        accounting.base_change
    );
    ensure!(
        accounting.base_change - protected_size < lot,
        "OKX Demo OCO protected size differs from REST fee-adjusted acquisition by at least one lot"
    );
    Ok(DisposableDelta { protected_size })
}

async fn cleanup_acquisition_probe(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    baseline: CurrencyBalance,
    run_identity: &AcquisitionRunIdentity,
    plan: AcquisitionPlan,
) -> Result<()> {
    let terminal = reconcile_acquisition_order_for_cleanup(
        client,
        instrument,
        &run_identity.acquisition_client_order_id,
        plan,
    )
    .await?;
    let acquired = reconcile_acquisition_owned_accounting(
        client,
        instrument,
        &run_identity.acquisition_client_order_id,
        terminal.as_ref(),
    )
    .await?;
    let cleanup = liquidate_acquisition_owned_delta(
        client,
        instrument,
        baseline,
        acquired.base_change,
        &run_identity.cleanup_client_order_id,
    )
    .await?;
    let remaining_run_owned_base = acquired.base_change + cleanup.base_change;
    ensure!(
        remaining_run_owned_base >= Decimal::ZERO,
        "OKX Demo acquisition cleanup sold more base than this run acquired"
    );
    ensure!(
        remaining_run_owned_base < instrument.lot_size()?,
        "OKX Demo acquisition cleanup left run-owned base delta {remaining_run_owned_base} {} at or above one lot",
        instrument.base_ccy
    );

    ensure_clean_state(client, &instrument.inst_id, "probe cleanup").await?;
    let final_base = base_balance(client, instrument).await?;
    ensure_acquisition_operator_baseline(baseline, final_base, &instrument.base_ccy)
}

async fn reconcile_acquisition_order_for_cleanup(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    client_order_id: &str,
    plan: AcquisitionPlan,
) -> Result<Option<OkxOrder>> {
    let mut saw_order = false;
    let mut cancel_attempted = false;
    let mut last_state = "missing".to_owned();
    for attempt in 0..RECONCILE_ATTEMPTS {
        match client.order(&instrument.inst_id, client_order_id).await? {
            Some(order) => {
                saw_order = true;
                ensure_acquisition_order_shape(&order, instrument, client_order_id, plan)?;
                if order.is_terminal() {
                    return Ok(Some(order));
                }
                last_state = order.state.clone();
                if !cancel_attempted {
                    client
                        .cancel_order(&instrument.inst_id, client_order_id)
                        .await
                        .context(
                            "OKX Demo acquisition cleanup could not cancel the run-owned remainder",
                        )?;
                    cancel_attempted = true;
                }
            }
            None if saw_order => {
                return Err(anyhow!(
                    "OKX Demo acquisition order {client_order_id} disappeared after REST observed it; refusing ambiguous cleanup"
                ));
            }
            None => {}
        }
        if attempt + 1 < RECONCILE_ATTEMPTS {
            tokio::time::sleep(RECONCILE_DELAY).await;
        }
    }
    if saw_order {
        return Err(anyhow!(
            "OKX Demo acquisition order {client_order_id} did not reach a terminal REST state after {RECONCILE_ATTEMPTS} attempts; last state was {last_state}"
        ));
    }
    Ok(None)
}

fn ensure_acquisition_order_shape(
    order: &OkxOrder,
    instrument: &OkxInstrument,
    client_order_id: &str,
    plan: AcquisitionPlan,
) -> Result<()> {
    order.ensure_documented_state("acquisition cleanup")?;
    ensure!(
        order.inst_type == "SPOT"
            && order.inst_id == instrument.inst_id
            && order.client_order_id == client_order_id
            && order.parsed_side() == Some(OrderSide::Buy)
            && order.parsed_kind() == Some(OrderKind::Limit),
        "OKX Demo acquisition cleanup found a contradictory run-owned order shape"
    );
    ensure!(
        order.requested_size()? == plan.size,
        "OKX Demo acquisition cleanup order size contradicts the planned acquisition size"
    );
    let price = order
        .price
        .parse::<Decimal>()
        .context("OKX Demo acquisition cleanup order px must be a decimal")?;
    ensure!(
        price > Decimal::ZERO && price == plan.price,
        "OKX Demo acquisition cleanup order price contradicts the planned acquisition price"
    );
    Ok(())
}

async fn reconcile_acquisition_owned_accounting(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    client_order_id: &str,
    terminal: Option<&OkxOrder>,
) -> Result<OkxSpotFillAccounting> {
    let fills = client.order_fills(&instrument.inst_id).await?;
    let accounting = run_owned_fill_accounting(
        &fills,
        instrument,
        client_order_id,
        OrderSide::Buy,
        "acquisition",
    )?;
    ensure_acquisition_accounting_matches_terminal(
        accounting,
        terminal,
        &instrument.base_ccy,
        &instrument.quote_ccy,
        client_order_id,
    )?;
    ensure!(
        accounting.base_change >= Decimal::ZERO && accounting.quote_change <= Decimal::ZERO,
        "REST acquisition accounting has an invalid buy direction"
    );
    Ok(accounting)
}

fn ensure_acquisition_accounting_matches_terminal(
    accounting: OkxSpotFillAccounting,
    terminal: Option<&OkxOrder>,
    base_currency: &str,
    quote_currency: &str,
    client_order_id: &str,
) -> Result<()> {
    match terminal {
        Some(order) => {
            let cumulative = order.cumulative_spot_accounting(base_currency, quote_currency)?;
            ensure!(
                accounting == cumulative,
                "REST acquisition fill accounting {accounting:?} differs from terminal order accounting {cumulative:?}"
            );
        }
        None => ensure!(
            accounting == OkxSpotFillAccounting::default(),
            "REST acquisition fills exist for {client_order_id} while the exact order is missing"
        ),
    }
    Ok(())
}

fn run_owned_fill_accounting(
    fills: &[OkxFill],
    instrument: &OkxInstrument,
    client_order_id: &str,
    expected_side: OrderSide,
    context: &str,
) -> Result<OkxSpotFillAccounting> {
    let mut total = OkxSpotFillAccounting::default();
    let mut seen_fill_ids = HashSet::new();
    let mut expected_order_id: Option<&str> = None;
    for fill in fills
        .iter()
        .filter(|fill| fill.client_order_id == client_order_id)
    {
        ensure!(
            fill.inst_type == "SPOT"
                && fill.inst_id == instrument.inst_id
                && fill.parsed_side() == Some(expected_side)
                && fill.execution_type == "T",
            "REST {context} fill evidence contradicts the exact run-owned SPOT order"
        );
        ensure!(
            !fill.order_id.trim().is_empty(),
            "REST {context} fill evidence omitted ordId"
        );
        if let Some(order_id) = expected_order_id {
            ensure!(
                order_id == fill.order_id,
                "REST {context} fill evidence contains multiple order identities for one client order ID"
            );
        } else {
            expected_order_id = Some(&fill.order_id);
        }
        let fill_id = fill.dedupe_key();
        ensure!(
            !fill_id.trim().is_empty() && seen_fill_ids.insert(fill_id),
            "REST {context} fill evidence contains a missing or duplicate stable fill identity"
        );
        let accounting = fill.spot_accounting(&instrument.base_ccy, &instrument.quote_ccy)?;
        match expected_side {
            OrderSide::Buy => ensure!(
                accounting.base_change > Decimal::ZERO && accounting.quote_change < Decimal::ZERO,
                "REST {context} buy fill accounting has an invalid direction"
            ),
            OrderSide::Sell => ensure!(
                accounting.base_change < Decimal::ZERO && accounting.quote_change > Decimal::ZERO,
                "REST {context} sell fill accounting has an invalid direction"
            ),
        }
        total.base_change += accounting.base_change;
        total.quote_change += accounting.quote_change;
    }
    Ok(total)
}

fn acquisition_cleanup_sale_size(
    instrument: &OkxInstrument,
    baseline: CurrencyBalance,
    current: CurrencyBalance,
    run_owned_base: Decimal,
) -> Result<Decimal> {
    ensure!(
        run_owned_base >= Decimal::ZERO,
        "OKX Demo acquisition run-owned base must be non-negative"
    );
    ensure!(
        baseline.available <= baseline.total && current.available <= current.total,
        "OKX Demo acquisition balance availability exceeds total balance"
    );
    let total_delta = total_delta_from_baseline(baseline, current, &instrument.base_ccy)?;
    ensure!(
        current.available >= baseline.available,
        "OKX Demo acquisition cleanup refuses to consume the protected available {} baseline",
        instrument.base_ccy
    );
    let available_delta = current.available - baseline.available;
    let sellable = run_owned_base.min(total_delta).min(available_delta);
    if sellable < instrument.min_size()? {
        return Ok(Decimal::ZERO);
    }
    let size = quantize_decimal_down(sellable, instrument.lot_size()?)?;
    if size < instrument.min_size()? {
        return Ok(Decimal::ZERO);
    }
    ensure!(
        size <= run_owned_base && size <= total_delta && size <= available_delta,
        "OKX Demo acquisition cleanup quantization exceeded an ownership or balance authority"
    );
    Ok(size)
}

async fn liquidate_acquisition_owned_delta(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    baseline: CurrencyBalance,
    run_owned_base: Decimal,
    client_order_id: &str,
) -> Result<OkxSpotFillAccounting> {
    let current = base_balance(client, instrument).await?;
    let size = acquisition_cleanup_sale_size(instrument, baseline, current, run_owned_base)?;
    if size == Decimal::ZERO {
        return Ok(OkxSpotFillAccounting::default());
    }
    let reference_price = client
        .ticker(&instrument.inst_id)
        .await
        .context("OKX Demo acquisition cleanup ticker lookup failed")?
        .ask_decimal()?;
    instrument.ensure_spot_market_sell_size(
        size,
        reference_price,
        "OKX Demo acquisition cleanup sell size",
    )?;
    client
        .place_order(
            &instrument.inst_id,
            OrderSide::Sell,
            OrderKind::Market,
            &decimal_to_okx(size),
            None,
            client_order_id,
        )
        .await
        .context("OKX Demo acquisition cleanup market sell failed or remained ambiguous")?;
    let order = wait_for_terminal_order(client, &instrument.inst_id, client_order_id).await?;
    ensure!(
        order.inst_type == "SPOT"
            && order.inst_id == instrument.inst_id
            && order.client_order_id == client_order_id
            && order.parsed_side() == Some(OrderSide::Sell)
            && order.parsed_kind() == Some(OrderKind::Market)
            && order.requested_size()? == size,
        "OKX Demo acquisition cleanup found a contradictory run-owned sell order"
    );
    let cumulative =
        order.cumulative_spot_accounting(&instrument.base_ccy, &instrument.quote_ccy)?;
    ensure!(
        cumulative.base_change < Decimal::ZERO && cumulative.quote_change > Decimal::ZERO,
        "OKX Demo acquisition cleanup did not produce an exact fee-adjusted base sale"
    );
    let verified =
        ensure_liquidation_fill_evidence(client, instrument, client_order_id, cumulative).await?;
    ensure!(
        -verified.base_change <= run_owned_base,
        "OKX Demo acquisition cleanup fill accounting consumed more base than this run acquired"
    );
    Ok(verified)
}

async fn liquidate_disposable_delta(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    baseline: CurrencyBalance,
    marker: char,
) -> Result<()> {
    let current = base_balance(client, instrument).await?;
    let delta = total_delta_from_baseline(baseline, current, &instrument.base_ccy)?;
    let lot = instrument.lot_size()?;
    if delta >= instrument.min_size()? {
        let size = quantize_decimal_down(delta, lot)?;
        let reference_price = client
            .ticker(&instrument.inst_id)
            .await
            .context("OKX Demo OCO cleanup ticker lookup failed")?
            .ask_decimal()?;
        instrument.ensure_spot_market_sell_size(
            size,
            reference_price,
            "OKX Demo OCO cleanup sell size",
        )?;
        let client_order_id = scenario_client_id(client, marker, "S").await?;
        client
            .place_order(
                &instrument.inst_id,
                OrderSide::Sell,
                OrderKind::Market,
                &decimal_to_okx(size),
                None,
                &client_order_id,
            )
            .await
            .context("OKX Demo OCO cleanup market sell failed or remained ambiguous")?;
        let order = wait_for_terminal_order(client, &instrument.inst_id, &client_order_id).await?;
        let accounting =
            order.cumulative_spot_accounting(&instrument.base_ccy, &instrument.quote_ccy)?;
        ensure!(
            accounting.base_change < Decimal::ZERO && accounting.quote_change > Decimal::ZERO,
            "OKX Demo acquisition cleanup did not produce an exact fee-adjusted base sale"
        );
        ensure_liquidation_fill_evidence(client, instrument, &client_order_id, accounting).await?;
    }
    wait_for_delta_below_lot(client, instrument, baseline).await
}

async fn cleanup_test_owned_state(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    operator_baseline: CurrencyBalance,
) -> Result<()> {
    let open_oco = client.open_spot_oco_orders(&instrument.inst_id).await?;
    for order in open_oco {
        ensure_test_owned_oco(&order)?;
        client
            .cancel_spot_oco(&instrument.inst_id, &order.algo_id)
            .await?;
        wait_for_oco(
            client,
            instrument,
            &order.client_order_id,
            ExpectedOcoState::Canceled,
        )
        .await?;
    }
    liquidate_disposable_delta(client, instrument, operator_baseline, 'F').await?;
    ensure_clean_state(client, &instrument.inst_id, "final cleanup").await?;
    let final_base = base_balance(client, instrument).await?;
    ensure_operator_baseline(operator_baseline, final_base, &instrument.base_ccy)
}

fn ensure_test_owned_oco(order: &OkxOcoOrder) -> Result<()> {
    ensure!(
        order.client_order_id.starts_with(OCO_CLIENT_ID_PREFIX)
            && order.tag == OKX_CANCEL_ALL_AFTER_TAG,
        "OKX Demo OCO cleanup found an unowned OCO and refused to cancel it"
    );
    Ok(())
}

async fn ensure_acquisition_fill_evidence(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    client_order_id: &str,
    cumulative: OkxSpotFillAccounting,
) -> Result<OkxSpotFillAccounting> {
    let fills = client.order_fills(&instrument.inst_id).await?;
    let total = run_owned_fill_accounting(
        &fills,
        instrument,
        client_order_id,
        OrderSide::Buy,
        "acquisition",
    )?;
    ensure!(
        total != OkxSpotFillAccounting::default(),
        "REST fill history omitted the OCO acquisition fill"
    );
    ensure!(
        total == cumulative,
        "REST fill history accounting {total:?} differs from terminal order accounting {cumulative:?}"
    );
    Ok(total)
}

async fn ensure_liquidation_fill_evidence(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    client_order_id: &str,
    cumulative: OkxSpotFillAccounting,
) -> Result<OkxSpotFillAccounting> {
    let fills = client.order_fills(&instrument.inst_id).await?;
    let total = run_owned_fill_accounting(
        &fills,
        instrument,
        client_order_id,
        OrderSide::Sell,
        "acquisition-probe cleanup",
    )?;
    ensure!(
        total != OkxSpotFillAccounting::default(),
        "REST fill history omitted the acquisition-probe cleanup fill"
    );
    ensure!(
        total == cumulative,
        "REST cleanup fill accounting {total:?} differs from terminal order accounting {cumulative:?}"
    );
    Ok(total)
}

async fn ensure_execution_fill_evidence(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    order: &OkxOcoOrder,
) -> Result<()> {
    ensure!(
        !order.order_id.trim().is_empty(),
        "effective OCO detail omitted the spawned regular ordId"
    );
    let fills = client.order_fills(&instrument.inst_id).await?;
    let matching = fills
        .iter()
        .filter(|fill| fill.order_id == order.order_id)
        .collect::<Vec<_>>();
    ensure!(
        !matching.is_empty(),
        "REST fill history omitted the OCO execution fill"
    );
    let mut sold = Decimal::ZERO;
    for fill in matching {
        let accounting = fill.spot_accounting(&instrument.base_ccy, &instrument.quote_ccy)?;
        ensure!(
            accounting.base_change < Decimal::ZERO && accounting.quote_change > Decimal::ZERO,
            "OCO execution fill did not represent a SPOT base sale"
        );
        sold -= accounting.base_change;
    }
    ensure!(
        sold == order.requested_size()?,
        "OCO execution fills sold {sold} but the protected quantity was {}",
        order.sz
    );
    Ok(())
}

async fn wait_for_oco(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    client_order_id: &str,
    expected: ExpectedOcoState<'_>,
) -> Result<OkxOcoOrder> {
    let mut last = "missing".to_owned();
    for attempt in 0..RECONCILE_ATTEMPTS {
        if let Some(order) = client
            .oco_order_by_client_order_id(&instrument.inst_id, client_order_id)
            .await?
        {
            last = order.state.clone();
            if expected.matches(&order) {
                return Ok(order);
            }
        }
        if attempt + 1 < RECONCILE_ATTEMPTS {
            tokio::time::sleep(RECONCILE_DELAY).await;
        }
    }
    Err(anyhow!(
        "timed out waiting for OCO {client_order_id} to reach {}; last REST state was {last}",
        expected.label()
    ))
}

async fn ensure_history_matches(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    detail: &OkxOcoOrder,
) -> Result<()> {
    let history = client
        .oco_history_by_algo_id(&instrument.inst_id, &detail.algo_id)
        .await?
        .context("terminal OCO was missing from REST algo history")?;
    ensure!(
        history.client_order_id == detail.client_order_id
            && history.state == detail.state
            && history.requested_size()? == detail.requested_size()?,
        "REST OCO history did not match terminal detail evidence"
    );
    Ok(())
}

async fn ensure_no_pending_oco(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    algo_id: &str,
) -> Result<()> {
    let pending = client.open_spot_oco_orders(&instrument.inst_id).await?;
    ensure!(
        pending.iter().all(|order| order.algo_id != algo_id),
        "terminal OCO {algo_id} remained present in REST pending state"
    );
    Ok(())
}

async fn passive_oco_prices(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
) -> Result<OcoPrices> {
    let ticker = client.ticker(&instrument.inst_id).await?;
    let last = ticker.last_decimal()?;
    let take_profit = quantize_decimal_up(last * Decimal::new(110, 2), instrument.tick_size()?)?;
    let stop_loss = quantize_decimal_down(last * Decimal::new(90, 2), instrument.tick_size()?)?;
    Ok(OcoPrices {
        take_profit,
        stop_loss,
    })
}

async fn immediate_oco_prices(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    side: ExecutionSide,
) -> Result<OcoPrices> {
    let ticker = client.ticker(&instrument.inst_id).await?;
    let last = ticker.last_decimal()?;
    let tick = instrument.tick_size()?;
    let prices = match side {
        ExecutionSide::TakeProfit => OcoPrices {
            take_profit: quantize_decimal_down(last * Decimal::new(995, 3), tick)?,
            stop_loss: quantize_decimal_down(last * Decimal::new(90, 2), tick)?,
        },
        ExecutionSide::StopLoss => OcoPrices {
            take_profit: quantize_decimal_up(last * Decimal::new(110, 2), tick)?,
            stop_loss: quantize_decimal_up(last * Decimal::new(1005, 3), tick)?,
        },
    };
    ensure!(
        prices.take_profit > prices.stop_loss,
        "immediate OCO triggers are invalid"
    );
    Ok(prices)
}

async fn ensure_baseline_is_not_protected(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    baseline: CurrencyBalance,
    protected_size: Decimal,
) -> Result<()> {
    let current = base_balance(client, instrument).await?;
    ensure!(
        current.total >= baseline.total + protected_size && current.available >= baseline.total,
        "OKX Demo OCO would encumber or sell the protected operator baseline"
    );
    Ok(())
}

async fn wait_for_positive_delta(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    baseline: CurrencyBalance,
) -> Result<Decimal> {
    let mut last = Decimal::ZERO;
    for attempt in 0..RECONCILE_ATTEMPTS {
        let current = base_balance(client, instrument).await?;
        last = total_delta_from_baseline(baseline, current, &instrument.base_ccy)?;
        if last >= instrument.min_size()? {
            return Ok(last);
        }
        if attempt + 1 < RECONCILE_ATTEMPTS {
            tokio::time::sleep(RECONCILE_DELAY).await;
        }
    }
    Ok(last)
}

async fn wait_for_delta_below_lot(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
    baseline: CurrencyBalance,
) -> Result<()> {
    let mut last = Decimal::ZERO;
    for attempt in 0..RECONCILE_ATTEMPTS {
        let current = base_balance(client, instrument).await?;
        last = total_delta_from_baseline(baseline, current, &instrument.base_ccy)?;
        if last < instrument.lot_size()? {
            return Ok(());
        }
        if attempt + 1 < RECONCILE_ATTEMPTS {
            tokio::time::sleep(RECONCILE_DELAY).await;
        }
    }
    Err(anyhow!(
        "OKX Demo OCO left disposable base delta {last} {} at or above one lot",
        instrument.base_ccy
    ))
}

async fn base_balance(
    client: &OkxRestClient,
    instrument: &OkxInstrument,
) -> Result<CurrencyBalance> {
    let balances = client.balances().await?;
    currency_balance(&balances, &instrument.base_ccy)
}

fn currency_balance(balances: &[OkxBalance], currency: &str) -> Result<CurrencyBalance> {
    let matching = balances
        .iter()
        .flat_map(|balance| &balance.details)
        .filter(|detail| detail.ccy == currency)
        .collect::<Vec<_>>();
    ensure!(
        matching.len() == 1,
        "expected exactly one {currency} balance row"
    );
    Ok(CurrencyBalance {
        available: matching[0].available()?,
        total: matching[0].total()?,
        frozen: matching[0].frozen()?,
    })
}

fn ensure_unfrozen_baseline(balance: CurrencyBalance, currency: &str) -> Result<()> {
    ensure!(
        balance.frozen == Decimal::ZERO && balance.available == balance.total,
        "OKX Demo OCO requires an unfrozen {currency} baseline and made no mutation"
    );
    Ok(())
}

fn ensure_operator_baseline(
    operator_baseline: CurrencyBalance,
    current: CurrencyBalance,
    currency: &str,
) -> Result<()> {
    ensure!(
        current.total >= operator_baseline.total,
        "OKX Demo OCO {currency} total fell below the protected operator baseline"
    );
    Ok(())
}

fn ensure_acquisition_operator_baseline(
    operator_baseline: CurrencyBalance,
    current: CurrencyBalance,
    currency: &str,
) -> Result<()> {
    ensure_operator_baseline(operator_baseline, current, currency)?;
    ensure!(
        current.available >= operator_baseline.available,
        "OKX Demo acquisition cleanup {currency} available balance fell below the protected operator baseline"
    );
    Ok(())
}

fn total_delta_from_baseline(
    baseline: CurrencyBalance,
    current: CurrencyBalance,
    currency: &str,
) -> Result<Decimal> {
    ensure!(
        current.total >= baseline.total,
        "OKX Demo OCO refuses to consume the protected {currency} baseline"
    );
    Ok(current.total - baseline.total)
}

fn ensure_protected_quantity(order: &OkxOcoOrder, expected: Decimal) -> Result<()> {
    ensure!(
        order.requested_size()? == expected,
        "OKX Demo OCO protects {} but exact acquired quantity is {expected}",
        order.sz
    );
    Ok(())
}

async fn scenario_client_id(client: &OkxRestClient, marker: char, kind: &str) -> Result<String> {
    let server_timestamp = client.websocket_login_timestamp().await?;
    let digits = server_timestamp
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>();
    scenario_client_id_from_digits(marker, kind, &digits)
}

fn scenario_client_id_from_digits(marker: char, kind: &str, digits: &str) -> Result<String> {
    ensure!(
        !digits.is_empty(),
        "OKX server timestamp did not contain digits"
    );
    let client_order_id = format!("{OCO_CLIENT_ID_PREFIX}{marker}{kind}{digits}");
    ensure!(
        client_order_id.len() <= 32
            && client_order_id
                .chars()
                .all(|character| character.is_ascii_alphanumeric()),
        "OKX Demo OCO client identifier is invalid"
    );
    Ok(client_order_id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcquisitionRunIdentity {
    acquisition_client_order_id: String,
    cleanup_client_order_id: String,
}

impl AcquisitionRunIdentity {
    async fn from_client(client: &OkxRestClient) -> Result<Self> {
        let server_timestamp = client.websocket_login_timestamp().await?;
        let digits = server_timestamp
            .chars()
            .filter(char::is_ascii_digit)
            .collect::<String>();
        Self::from_digits(&digits)
    }

    fn from_digits(digits: &str) -> Result<Self> {
        Ok(Self {
            acquisition_client_order_id: scenario_client_id_from_digits('P', "B", digits)?,
            cleanup_client_order_id: scenario_client_id_from_digits('P', "S", digits)?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct DisposableDelta {
    protected_size: Decimal,
}

#[derive(Clone, Copy, Debug)]
struct AcquisitionPlan {
    price: Decimal,
    size: Decimal,
    required_quote: Decimal,
}

#[derive(Clone, Copy, Debug, Default)]
struct CurrencyBalance {
    available: Decimal,
    total: Decimal,
    frozen: Decimal,
}

#[derive(Clone, Copy, Debug)]
struct OcoPrices {
    take_profit: Decimal,
    stop_loss: Decimal,
}

#[derive(Clone, Copy, Debug)]
enum ExecutionSide {
    TakeProfit,
    StopLoss,
}

impl ExecutionSide {
    const fn marker(self) -> char {
        match self {
            Self::TakeProfit => 'T',
            Self::StopLoss => 'L',
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::TakeProfit => "take-profit",
            Self::StopLoss => "stop-loss",
        }
    }

    const fn actual_side(self) -> &'static str {
        match self {
            Self::TakeProfit => "tp",
            Self::StopLoss => "sl",
        }
    }

    const fn preflight_label(self) -> &'static str {
        match self {
            Self::TakeProfit => "take-profit preflight",
            Self::StopLoss => "stop-loss preflight",
        }
    }

    const fn cleanup_label(self) -> &'static str {
        match self {
            Self::TakeProfit => "take-profit cleanup",
            Self::StopLoss => "stop-loss cleanup",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ExpectedOcoState<'a> {
    Pending,
    Canceled,
    Executed(&'a str),
}

impl ExpectedOcoState<'_> {
    fn matches(self, order: &OkxOcoOrder) -> bool {
        match self {
            Self::Pending => order.is_pending(),
            Self::Canceled => order.state == "canceled",
            Self::Executed(actual_side) => {
                order.state == "effective" && order.actual_side == actual_side
            }
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Canceled => "canceled",
            Self::Executed("tp") => "effective take-profit",
            Self::Executed("sl") => "effective stop-loss",
            Self::Executed(_) => "effective expected leg",
        }
    }
}

#[cfg(test)]
#[path = "demo_oco_smoke_tests.rs"]
mod tests;
