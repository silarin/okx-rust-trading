#![forbid(unsafe_code)]

pub mod app;
pub mod config;
pub mod okx;
pub mod strategies;
#[cfg(test)]
pub(crate) mod test_support;

use anyhow::Result;
use config::loader::{load_config_path, load_selected_config, selected_config_path};
use tracing::info;

pub fn validate_selected_profile_with_args(args: impl IntoIterator<Item = String>) -> Result<()> {
    let _ = load_selected_config(args)?;
    Ok(())
}

pub fn run_with_args_blocking(args: impl IntoIterator<Item = String>) -> Result<()> {
    let mut args = args.into_iter();
    let first = args.next();
    if is_economics_preflight_command(first.as_deref()) {
        return run_economics_preflight_blocking(args);
    }

    run_runtime_blocking(first.into_iter().chain(args))
}

fn is_economics_preflight_command(first: Option<&str>) -> bool {
    first == Some("economics-preflight")
}

fn run_runtime_blocking(args: impl IntoIterator<Item = String>) -> Result<()> {
    let config = load_selected_config(args)?.1;
    app::init_telemetry(&config)?;
    info!(
        runtime = "direct-okx",
        instrument_count = config.instruments.len(),
        "loaded OKX spot profile"
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(app::live::run(config))
}

fn run_economics_preflight_blocking(args: impl IntoIterator<Item = String>) -> Result<()> {
    let command = app::economics_preflight::EconomicsPreflightCommand::parse(args)?;
    let path = selected_config_path(Some(&command.profile_selector))?;
    let config = load_config_path(&path)?;
    let validated =
        app::economics_preflight::validate_before_client_construction(&command, &config)?;

    eprintln!("orders will not be submitted");
    eprintln!("Cancel-All-After will not be called");
    eprintln!("strategies will not be constructed");
    eprintln!("balances and positions will not be read");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(app::economics_preflight::run(command, config, validated))
}

#[cfg(test)]
mod tests {
    use super::is_economics_preflight_command;

    #[test]
    fn profile_selectors_do_not_enter_economics_preflight() {
        for selector in [None, Some("example"), Some("/tmp/profile.toml")] {
            assert!(!is_economics_preflight_command(selector));
        }
        assert!(is_economics_preflight_command(Some("economics-preflight")));
    }
}
