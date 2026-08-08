mod cancel_all_after_heartbeat;
pub(crate) mod economics_preflight;
pub mod live;
mod okx_startup_preflight;
mod okx_stream_config;
mod strategy_tick_execution;
mod strategy_tick_failure;
mod websocket_health_tracker;

use anyhow::Result;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt};

use crate::config::types::BotConfig;

pub(crate) const DEFAULT_TELEMETRY_FILTER: &str = "info";
const RUST_LOG_ENV: &str = "RUST_LOG";

pub fn init_telemetry(config: &BotConfig) -> Result<()> {
    let filter = telemetry_filter_from_env_or_default()?;
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer());

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        tracing::debug!("telemetry already initialized");
    }

    tracing::info!(
        trader_id = %config.runtime.trader_id,
        "telemetry initialized"
    );
    Ok(())
}

fn telemetry_filter_from_env_or_default() -> Result<EnvFilter> {
    match std::env::var(RUST_LOG_ENV) {
        Ok(value) => telemetry_filter_from_rust_log_or_default(Some(value.as_str())),
        Err(_) => telemetry_filter_from_rust_log_or_default(None),
    }
}

fn telemetry_filter_from_rust_log_or_default(rust_log: Option<&str>) -> Result<EnvFilter> {
    if let Some(value) = rust_log
        && !value.trim().is_empty()
    {
        return match EnvFilter::try_new(value) {
            Ok(filter) => Ok(filter),
            Err(_) => default_telemetry_filter(),
        };
    }

    default_telemetry_filter()
}

fn default_telemetry_filter() -> Result<EnvFilter> {
    EnvFilter::try_new(DEFAULT_TELEMETRY_FILTER).map_err(Into::into)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
