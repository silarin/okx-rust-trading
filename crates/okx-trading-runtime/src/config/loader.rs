use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, bail, ensure};

use super::types::BotConfig;

pub fn config_path() -> PathBuf {
    profile_path("example")
}

pub fn selected_config_path_from_args(args: impl IntoIterator<Item = String>) -> Result<PathBuf> {
    let mut args = args.into_iter();
    let selector = args.next();
    if let Some(extra) = args.next() {
        bail!(
            "runtime config accepts at most one complete profile selector; unexpected extra CLI argument {extra:?}"
        );
    }

    selected_config_path(selector.as_deref())
}

pub fn selected_config_path(selector: Option<&str>) -> Result<PathBuf> {
    match selector {
        Some(value) => resolve_profile_selector(value),
        None => Ok(config_path()),
    }
}

pub fn load_selected_config(
    args: impl IntoIterator<Item = String>,
) -> Result<(PathBuf, BotConfig)> {
    let path = selected_config_path_from_args(args)?;
    let config = load_config_path(&path)?;
    Ok((path, config))
}

pub fn load_config_path(path: &Path) -> Result<BotConfig> {
    let read_path = readable_config_path(path);
    let contents = std::fs::read_to_string(&read_path)
        .with_context(|| format!("failed reading config file {}", path.display()))?;
    load_config_from_str(&contents)
        .with_context(|| format!("failed loading config file {}", path.display()))
}

pub fn load_config_path_with_secret_resolver(
    path: &Path,
    resolver: impl Fn(&str) -> Option<String>,
) -> Result<BotConfig> {
    let read_path = readable_config_path(path);
    let contents = std::fs::read_to_string(&read_path)
        .with_context(|| format!("failed reading config file {}", path.display()))?;
    load_config_from_str_with_secret_resolver(&contents, resolver)
        .with_context(|| format!("failed loading config file {}", path.display()))
}

fn readable_config_path(path: &Path) -> PathBuf {
    if path.is_absolute() || path.exists() {
        return path.to_path_buf();
    }
    let workspace_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path);
    if workspace_path.is_file() {
        workspace_path
    } else {
        path.to_path_buf()
    }
}

pub fn load_config_from_str(contents: &str) -> Result<BotConfig> {
    load_config_from_str_with_secret_resolver(contents, |name| std::env::var(name).ok())
}

pub fn load_config_from_str_with_secret_resolver(
    contents: &str,
    resolver: impl Fn(&str) -> Option<String>,
) -> Result<BotConfig> {
    let config = toml::from_str::<BotConfig>(contents).context("failed parsing TOML profile")?;
    finalize_config_with_secret_resolver(config, resolver)
}

pub(crate) fn finalize_config_with_secret_resolver(
    mut config: BotConfig,
    resolver: impl Fn(&str) -> Option<String>,
) -> Result<BotConfig> {
    resolve_okx_secret_placeholders(&mut config, &resolver)?;
    reject_non_secret_placeholders(&config)?;
    config.validate()?;
    Ok(config)
}

fn profile_path(name: &str) -> PathBuf {
    PathBuf::from("config").join(format!("{name}.toml"))
}

fn resolve_profile_selector(value: &str) -> Result<PathBuf> {
    let value = value.trim();
    if value.is_empty() {
        bail!("runtime config profile selector must not be empty");
    }
    if value.starts_with('-') || value.contains('=') {
        bail!(
            "field-level CLI overrides and mode flags are prohibited; select one complete TOML profile"
        );
    }

    let path = Path::new(value);
    if path.components().count() > 1 || path.extension().is_some() {
        return Ok(path.to_path_buf());
    }

    if !value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        bail!("profile names may contain only ASCII letters, digits, '_' or '-'");
    }

    Ok(profile_path(value))
}

impl FromStr for BotConfig {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        load_config_from_str(s)
    }
}

fn resolve_okx_secret_placeholders(
    config: &mut BotConfig,
    resolver: &impl Fn(&str) -> Option<String>,
) -> Result<()> {
    let Some(okx) = config.okx.as_mut() else {
        return Ok(());
    };

    let placeholders = OkxSecretPlaceholders {
        api_key: "OKX_API_KEY",
        api_secret: "OKX_API_SECRET",
        api_passphrase: "OKX_API_PASSPHRASE",
    };

    okx.api_key =
        resolve_okx_secret_field("okx.api_key", &okx.api_key, placeholders.api_key, resolver)?
            .into();
    okx.api_secret = resolve_okx_secret_field(
        "okx.api_secret",
        &okx.api_secret,
        placeholders.api_secret,
        resolver,
    )?
    .into();
    okx.api_passphrase = resolve_okx_secret_field(
        "okx.api_passphrase",
        &okx.api_passphrase,
        placeholders.api_passphrase,
        resolver,
    )?
    .into();
    Ok(())
}

struct OkxSecretPlaceholders {
    api_key: &'static str,
    api_secret: &'static str,
    api_passphrase: &'static str,
}

fn resolve_okx_secret_field(
    field: &str,
    value: &str,
    allowed_name: &str,
    resolver: &impl Fn(&str) -> Option<String>,
) -> Result<String> {
    let allowed_placeholder = format!("${{{allowed_name}}}");
    if value == allowed_placeholder {
        let resolved = resolve_secret_value(field, &allowed_placeholder, allowed_name, resolver)?;
        ensure!(
            !resolved.trim().is_empty(),
            "{field} resolved secret for {allowed_name} must not be empty"
        );
        return Ok(resolved);
    }

    if let Some(name) = exact_placeholder_name(value) {
        bail!("{field} uses unknown placeholder ${{{name}}}; expected {allowed_placeholder}");
    }

    if contains_placeholder(value) {
        bail!("{field} placeholder syntax must be exactly {allowed_placeholder}");
    }

    Ok(value.to_owned())
}

fn resolve_secret_value(
    field: &str,
    allowed_placeholder: &str,
    allowed_name: &str,
    resolver: &impl Fn(&str) -> Option<String>,
) -> Result<String> {
    if let Some(resolved) = resolver(allowed_name) {
        ensure!(
            !resolved.trim().is_empty(),
            "{field} environment variable {allowed_name} must not be empty"
        );
        return resolve_direct_secret_value(field, allowed_name, &resolved);
    }

    let file_name = format!("{allowed_name}_FILE");
    let Some(path) = resolver(&file_name) else {
        bail!(
            "{field} placeholder {allowed_placeholder} requires environment variable {allowed_name} or {file_name}"
        );
    };
    ensure!(
        !path.trim().is_empty(),
        "{field} environment variable {file_name} must not be empty"
    );

    let resolved = std::fs::read_to_string(&path)
        .with_context(|| format!("{field} failed reading secret file from {file_name}={path:?}"))?;
    let resolved = resolved.trim_end_matches(['\r', '\n']).to_owned();
    ensure!(
        !resolved.trim().is_empty(),
        "{field} secret file from {file_name} must not be empty"
    );
    validate_resolved_secret_value(field, allowed_name, &resolved)?;
    Ok(resolved)
}

fn resolve_direct_secret_value(field: &str, allowed_name: &str, value: &str) -> Result<String> {
    let path = Path::new(value);
    if path.is_absolute() && path.is_file() {
        let resolved = std::fs::read_to_string(value).with_context(|| {
            format!("{field} failed reading secret file from {allowed_name}={value:?}")
        })?;
        let resolved = resolved.trim_end_matches(['\r', '\n']).to_owned();
        ensure!(
            !resolved.trim().is_empty(),
            "{field} resolved secret for {allowed_name} must not be empty"
        );
        validate_resolved_secret_value(field, allowed_name, &resolved)?;
        return Ok(resolved);
    } else if value.starts_with("/run/secrets/") {
        bail!("{field} failed reading Docker secret file from {allowed_name}={value:?}");
    }
    ensure!(
        !value.trim().is_empty(),
        "{field} resolved secret for {allowed_name} must not be empty"
    );
    validate_resolved_secret_value(field, allowed_name, value)?;
    Ok(value.to_owned())
}

fn validate_resolved_secret_value(field: &str, allowed_name: &str, value: &str) -> Result<()> {
    ensure!(
        value == value.trim(),
        "{field} resolved secret for {allowed_name} must not contain leading or trailing whitespace"
    );
    ensure!(
        !value.contains('\r') && !value.contains('\n'),
        "{field} resolved secret for {allowed_name} must be a single raw value without embedded newlines"
    );

    let trimmed_start = value.trim_start();
    const OKX_SECRET_ENV_NAMES: [&str; 3] = ["OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"];
    ensure!(
        !OKX_SECRET_ENV_NAMES.iter().any(|name| {
            trimmed_start.starts_with(&format!("{name}="))
                || trimmed_start.starts_with(&format!("export {name}="))
        }),
        "{field} resolved secret for {allowed_name} must contain only the raw secret value, not an environment assignment"
    );

    Ok(())
}

fn reject_non_secret_placeholders(config: &BotConfig) -> Result<()> {
    reject_placeholder("product.name", &config.product.name)?;
    reject_placeholder("runtime.trader_id", &config.runtime.trader_id)?;

    if let Some(okx) = &config.okx {
        reject_placeholder("okx.base_url", &okx.base_url)?;
        if let Some(value) = &okx.base_url_ws_public {
            reject_placeholder("okx.base_url_ws_public", value)?;
        }
        if let Some(value) = &okx.base_url_ws_private {
            reject_placeholder("okx.base_url_ws_private", value)?;
        }
        if let Some(value) = &okx.base_url_ws_business {
            reject_placeholder("okx.base_url_ws_business", value)?;
        }
        if let Some(value) = &okx.proxy_url {
            reject_placeholder("okx.proxy_url", value)?;
        }
        reject_placeholder("okx.account_id", &okx.account_id)?;
    }

    for (index, instrument) in config.instruments.iter().enumerate() {
        reject_placeholder(
            &format!("instruments[{index}].instrument_id"),
            instrument.instrument_id.as_str(),
        )?;
        reject_placeholder(
            &format!("instruments[{index}].base_currency"),
            &instrument.base_currency,
        )?;
        reject_placeholder(
            &format!("instruments[{index}].quote_currency"),
            &instrument.quote_currency,
        )?;
    }

    for (strategy_index, instance) in config.strategies.instances.iter().enumerate() {
        reject_placeholder(
            &format!("strategies.instances[{strategy_index}].id"),
            &instance.id,
        )?;
        reject_placeholder(
            &format!("strategies.instances[{strategy_index}].instrument"),
            instance.instrument_id(),
        )?;
        reject_placeholder(
            &format!("strategies.instances[{strategy_index}].bar"),
            &instance.bar,
        )?;
        let quote_notional_keys = match &instance.params {
            super::types::StrategyParamsConfig::OkxEmaAtrMakerTrend(config) => {
                config.max_quote_notional_by_instrument.keys()
            }
        };
        for instrument_id in quote_notional_keys {
            reject_placeholder(
                &format!(
                    "strategies.instances[{strategy_index}].params.max_quote_notional_by_instrument[{instrument_id}]"
                ),
                instrument_id,
            )?;
        }
    }

    Ok(())
}

fn reject_placeholder(context: &str, value: &str) -> Result<()> {
    ensure!(
        !contains_placeholder(value),
        "environment placeholders are not allowed in {context}"
    );
    Ok(())
}

fn exact_placeholder_name(value: &str) -> Option<&str> {
    value
        .strip_prefix("${")
        .and_then(|value| value.strip_suffix('}'))
        .filter(|name| !name.is_empty())
}

fn contains_placeholder(value: &str) -> bool {
    value.contains("${")
}
