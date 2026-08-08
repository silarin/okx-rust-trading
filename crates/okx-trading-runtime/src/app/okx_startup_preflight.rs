//! Owns the OKX account-mode and SPOT fee checks that must complete before
//! exchange-side dead-man protection is armed.

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use tracing::info;

use super::okx_stream_config::enabled_strategy_trading_instruments;
use crate::{
    config::types::{BotConfig, OkxTradingService},
    okx::{trading_client::OkxTradingClient, trading_instrument::ValidatedTradingInstrument},
    strategies::okx_ema_atr_maker_trend::{MAX_MAKER_FEE_RATE, MAX_TAKER_FEE_RATE},
};

pub(super) async fn preflight_strategy_enabled_account(
    client: &OkxTradingClient,
    config: &BotConfig,
) -> Result<HashMap<String, Arc<ValidatedTradingInstrument>>> {
    let account_config = client.account_config().await?;
    account_config.ensure_spot_trading_enabled()?;
    let trading_service = config
        .okx
        .as_ref()
        .context("strategy-enabled startup requires an OKX configuration")?
        .trading_service;
    if trading_service == OkxTradingService::Production {
        let kyc_level = account_config.validated_live_kyc_level()?;
        info!(
            safety_event = "runtime_live_kyc_preflight_ok",
            okx_kyc_level = kyc_level.as_okx(),
            "validated OKX Production order-placement KYC eligibility"
        );
    }
    info!(
        safety_event = "runtime_account_preflight_ok",
        okx_account_mode = %account_config.account_level,
        "validated OKX account trading preflight"
    );

    let max_maker_fee_rate = MAX_MAKER_FEE_RATE
        .parse::<Decimal>()
        .context("OkxEmaAtrMakerTrend MAX_MAKER_FEE_RATE must be a decimal")?;
    let max_taker_fee_rate = MAX_TAKER_FEE_RATE
        .parse::<Decimal>()
        .context("OkxEmaAtrMakerTrend MAX_TAKER_FEE_RATE must be a decimal")?;
    ensure!(
        max_maker_fee_rate > Decimal::ZERO && max_taker_fee_rate > Decimal::ZERO,
        "OkxEmaAtrMakerTrend maximum maker and taker fee rates must be positive"
    );
    let mut validated_instruments = HashMap::new();
    for requested in enabled_strategy_trading_instruments(config) {
        if validated_instruments.contains_key(requested.instrument.as_str()) {
            continue;
        }
        let generation = client
            .validate_trading_instrument(requested, &account_config)
            .await
            .with_context(|| {
                format!(
                    "OKX requested trading tuple validation failed for {} + {} + {}",
                    requested.instrument, requested.inst_type, requested.td_mode
                )
            })?;
        let validated = generation.cash_spot_context();
        let instrument_id = validated.inst_id().to_owned();
        info!(
            safety_event = "runtime_trading_tuple_preflight_ok",
            instrument_id,
            inst_type = validated.inst_type().as_okx(),
            td_mode = validated.td_mode().as_okx(),
            trade_quote_currency = validated.trade_quote_ccy(),
            account_level_diagnostic = generation.account_level_diagnostic().value().as_okx(),
            "validated requested OKX trading tuple through public, account, and sizing evidence"
        );
        let fee = generation.fee();
        fee.ensure_commissions_at_most(&instrument_id, max_maker_fee_rate, max_taker_fee_rate)?;
        let round_trip_commission_rate = fee.round_trip_commission_rate()?;
        info!(
            safety_event = "runtime_spot_fee_preflight_ok",
            instrument_id,
            okx_fee_level = %fee.level,
            okx_fee_group_id = %fee.group_id,
            okx_maker_fee_rate = %fee.maker,
            okx_taker_fee_rate = %fee.taker,
            okx_round_trip_commission_rate = %round_trip_commission_rate,
            strategy_max_maker_fee_rate = %max_maker_fee_rate,
            strategy_max_taker_fee_rate = %max_taker_fee_rate,
            "validated OKX SPOT fee-rate preflight"
        );
        validated_instruments.insert(instrument_id, validated);
    }
    Ok(validated_instruments)
}

#[cfg(test)]
#[path = "okx_startup_preflight_tests.rs"]
mod tests;
