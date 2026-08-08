use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::Write,
    path::{Component, Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::time;

use crate::{
    config::types::{BotConfig, InstrumentConfig, OkxApiDomain, OkxTradingService},
    okx::{
        economics_preflight::{OkxEconomicsPreflightClient, OkxEconomicsPreflightSource},
        types::{OkxAccountConfig, OkxInstrument, OkxSpotFeeType, OkxTradeFeeRate},
    },
};

const SCHEMA: &str = "okx-trading-runtime-economics-preflight-v1";
const PRODUCT: &str = "okx-rust-trading";
const DEFAULT_REST_SAMPLES: usize = 20;
const MIN_REST_SAMPLES: usize = 3;
const MAX_REST_SAMPLES: usize = 100;
const DEFAULT_WEBSOCKET_SAMPLES: usize = 3;
const MIN_WEBSOCKET_SAMPLES: usize = 1;
const MAX_WEBSOCKET_SAMPLES: usize = 10;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
const MIN_REQUEST_TIMEOUT_MS: u64 = 100;
const MAX_REQUEST_TIMEOUT_MS: u64 = 60_000;
const SERVER_TIME_SAMPLE_DELAY: Duration = Duration::from_millis(210);
const AUTHENTICATED_SAMPLE_DELAY: Duration = Duration::from_millis(410);
const PUBLIC_MARKET_SAMPLE_DELAY: Duration = Duration::from_millis(110);
const WEBSOCKET_SAMPLE_DELAY: Duration = Duration::from_millis(350);
const TRADING_SESSION_LATENCY_NOTE: &str =
    "WebSocket trading-session preparation latency is not order-command acknowledgement latency.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EconomicsPreflightCommand {
    pub(crate) profile_selector: String,
    pub(crate) output: PathBuf,
    pub(crate) rest_samples: usize,
    pub(crate) websocket_samples: usize,
    pub(crate) request_timeout_ms: u64,
    pub(crate) acknowledge_read_only_production: bool,
}

impl EconomicsPreflightCommand {
    pub(crate) fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = args.into_iter();
        let profile_selector = args
            .next()
            .context("economics-preflight requires one complete profile selector")?;
        ensure!(
            !profile_selector.starts_with('-'),
            "economics-preflight profile selector must precede options"
        );

        let mut output = None;
        let mut rest_samples = None;
        let mut websocket_samples = None;
        let mut request_timeout_ms = None;
        let mut acknowledge_read_only_production = false;

        while let Some(option) = args.next() {
            match option.as_str() {
                "--output" => parse_once(
                    &mut output,
                    PathBuf::from(next_value(&mut args, "--output")?),
                    "--output",
                )?,
                "--rest-samples" => parse_once(
                    &mut rest_samples,
                    parse_usize(next_value(&mut args, "--rest-samples")?, "--rest-samples")?,
                    "--rest-samples",
                )?,
                "--websocket-samples" => parse_once(
                    &mut websocket_samples,
                    parse_usize(
                        next_value(&mut args, "--websocket-samples")?,
                        "--websocket-samples",
                    )?,
                    "--websocket-samples",
                )?,
                "--request-timeout-ms" => parse_once(
                    &mut request_timeout_ms,
                    parse_u64(
                        next_value(&mut args, "--request-timeout-ms")?,
                        "--request-timeout-ms",
                    )?,
                    "--request-timeout-ms",
                )?,
                "--acknowledge-read-only-production" => {
                    ensure!(
                        !acknowledge_read_only_production,
                        "duplicate economics-preflight option --acknowledge-read-only-production"
                    );
                    acknowledge_read_only_production = true;
                }
                _ => bail!("unknown economics-preflight option {option:?}"),
            }
        }

        let output = output.context("economics-preflight requires --output")?;
        ensure!(
            output.is_absolute(),
            "economics-preflight --output must be an absolute external path"
        );
        let rest_samples = rest_samples.unwrap_or(DEFAULT_REST_SAMPLES);
        ensure!(
            (MIN_REST_SAMPLES..=MAX_REST_SAMPLES).contains(&rest_samples),
            "economics-preflight --rest-samples must be between {MIN_REST_SAMPLES} and {MAX_REST_SAMPLES}"
        );
        let websocket_samples = websocket_samples.unwrap_or(DEFAULT_WEBSOCKET_SAMPLES);
        ensure!(
            (MIN_WEBSOCKET_SAMPLES..=MAX_WEBSOCKET_SAMPLES).contains(&websocket_samples),
            "economics-preflight --websocket-samples must be between {MIN_WEBSOCKET_SAMPLES} and {MAX_WEBSOCKET_SAMPLES}"
        );
        let request_timeout_ms = request_timeout_ms.unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS);
        ensure!(
            (MIN_REQUEST_TIMEOUT_MS..=MAX_REQUEST_TIMEOUT_MS).contains(&request_timeout_ms),
            "economics-preflight --request-timeout-ms must be between {MIN_REQUEST_TIMEOUT_MS} and {MAX_REQUEST_TIMEOUT_MS}"
        );

        Ok(Self {
            profile_selector,
            output,
            rest_samples,
            websocket_samples,
            request_timeout_ms,
            acknowledge_read_only_production,
        })
    }
}

pub(crate) fn validate_before_client_construction(
    command: &EconomicsPreflightCommand,
    config: &BotConfig,
) -> Result<ValidatedPreflight> {
    let okx = config.okx.as_ref().context("OKX config is required")?;
    if okx.trading_service == OkxTradingService::Production {
        ensure!(
            command.acknowledge_read_only_production,
            "Production economics-preflight requires --acknowledge-read-only-production; this acknowledges authenticated read-only requests and login handshakes only, never order intent"
        );
    }

    let enabled_instruments = config
        .instruments
        .iter()
        .filter(|instrument| instrument.enabled)
        .collect::<Vec<_>>();
    ensure!(
        enabled_instruments.len() == 1,
        "economics-preflight requires exactly one enabled configured SPOT instrument"
    );
    let instrument = PreflightInstrument::from_config(enabled_instruments[0]);

    let output = ValidatedOutput::new(&command.output)?;
    Ok(ValidatedPreflight {
        output,
        profile: ProfileArtifact {
            selector: sanitized_profile_selector(&command.profile_selector)?,
            region: api_domain_label(okx.api_domain).to_owned(),
            trading_service: trading_service_label(okx.trading_service).to_owned(),
            instrument_id: instrument.instrument_id.clone(),
        },
        instrument,
        timeout: Duration::from_millis(command.request_timeout_ms),
    })
}

pub(crate) async fn run(
    command: EconomicsPreflightCommand,
    config: BotConfig,
    validated: ValidatedPreflight,
) -> Result<()> {
    let client = OkxEconomicsPreflightClient::new(&config, validated.timeout)?;
    let artifact = collect_artifact(&command, &validated, &client).await?;
    validated.output.write(&artifact)
}

async fn collect_artifact(
    command: &EconomicsPreflightCommand,
    validated: &ValidatedPreflight,
    source: &impl OkxEconomicsPreflightSource,
) -> Result<EconomicsPreflightArtifact> {
    let instrument = &validated.instrument;
    let instrument_id = instrument.instrument_id.as_str();
    source
        .server_time()
        .await
        .context("failed warming OKX server time for authenticated preflight measurements")?;

    let (account_config_round_trip, account_rows) = collect_samples(
        "account_config_round_trip",
        command.rest_samples,
        MIN_REST_SAMPLES,
        validated.timeout,
        AUTHENTICATED_SAMPLE_DELAY,
        || source.account_config(),
        validate_account_config,
    )
    .await?;
    let account = consistent_account_safety(&account_rows)?;

    let (instrument_metadata_round_trip, instrument_rows) = collect_samples(
        "instrument_metadata_round_trip",
        command.rest_samples,
        command.rest_samples,
        validated.timeout,
        PUBLIC_MARKET_SAMPLE_DELAY,
        || source.instrument(instrument_id),
        |row| validate_fee_group_instrument(row, instrument),
    )
    .await?;
    let fee_group_id = consistent_instrument_fee_group(&instrument_rows)?;

    let (spot_fee_round_trip, fee_rows) = collect_samples(
        "spot_fee_round_trip",
        command.rest_samples,
        command.rest_samples,
        validated.timeout,
        AUTHENTICATED_SAMPLE_DELAY,
        || source.spot_trade_fee(instrument_id, &fee_group_id),
        |fee| validate_fee(fee, instrument_id, &fee_group_id),
    )
    .await?;
    let fees = fee_artifact(&fee_rows)?;

    let (ticker_round_trip, _) = collect_samples(
        "ticker_round_trip",
        command.rest_samples,
        MIN_REST_SAMPLES,
        validated.timeout,
        PUBLIC_MARKET_SAMPLE_DELAY,
        || source.ticker(instrument_id),
        |ticker| {
            ensure!(
                ticker.inst_id == instrument_id,
                "OKX ticker returned {} for requested {instrument_id}",
                ticker.inst_id
            );
            Ok(())
        },
    )
    .await?;

    let (server_time_round_trip, _) = collect_samples(
        "server_time_round_trip",
        command.rest_samples,
        MIN_REST_SAMPLES,
        validated.timeout,
        SERVER_TIME_SAMPLE_DELAY,
        || source.server_time(),
        |_| Ok(()),
    )
    .await?;

    let (public_websocket_connect_and_subscribe, _) = collect_samples(
        "public_websocket_connect_and_subscribe",
        command.websocket_samples,
        command.websocket_samples,
        validated.timeout,
        WEBSOCKET_SAMPLE_DELAY,
        || source.probe_public_websocket(instrument_id),
        |_| Ok(()),
    )
    .await?;

    let (private_websocket_login_and_subscribe, _) = collect_samples(
        "private_websocket_login_and_subscribe",
        command.websocket_samples,
        command.websocket_samples,
        validated.timeout,
        WEBSOCKET_SAMPLE_DELAY,
        || source.probe_private_websocket(instrument_id),
        |_| Ok(()),
    )
    .await?;

    let (trading_command_session_prepare, _) = collect_samples(
        "trading_command_session_prepare",
        command.websocket_samples,
        command.websocket_samples,
        validated.timeout,
        WEBSOCKET_SAMPLE_DELAY,
        || source.probe_trading_session(),
        |_| Ok(()),
    )
    .await?;

    Ok(EconomicsPreflightArtifact {
        schema: SCHEMA.to_owned(),
        generated_at_ms: generated_at_ms()?,
        product: PRODUCT.to_owned(),
        package_version: env!("CARGO_PKG_VERSION").to_owned(),
        profile: validated.profile.clone(),
        account_safety: account,
        fees,
        latency: LatencyArtifact {
            server_time_round_trip,
            account_config_round_trip,
            spot_fee_round_trip,
            instrument_metadata_round_trip,
            ticker_round_trip,
            public_websocket_connect_and_subscribe,
            private_websocket_login_and_subscribe,
            trading_command_session_prepare,
            trading_command_session_prepare_note: TRADING_SESSION_LATENCY_NOTE.to_owned(),
        },
        safety_assertions: SafetyAssertions::all_false(),
    })
}

async fn collect_samples<T, Operation, OperationFuture, Validate>(
    label: &str,
    attempts: usize,
    minimum_successes: usize,
    timeout: Duration,
    delay: Duration,
    mut operation: Operation,
    mut validate: Validate,
) -> Result<(LatencyDistribution, Vec<T>)>
where
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = Result<T>>,
    Validate: FnMut(&T) -> Result<()>,
{
    let mut durations = Vec::with_capacity(attempts);
    let mut values = Vec::with_capacity(attempts);
    let mut failures = 0usize;
    let mut first_failure = None;
    for attempt in 0..attempts {
        let started_at = Instant::now();
        match time::timeout(timeout, operation()).await {
            Ok(Ok(value)) => {
                validate(&value).with_context(|| format!("{label} returned unsafe data"))?;
                durations.push(duration_to_micros(started_at.elapsed())?);
                values.push(value);
            }
            Ok(Err(error)) => {
                failures += 1;
                first_failure.get_or_insert_with(|| error.to_string());
            }
            Err(_) => {
                failures += 1;
                first_failure.get_or_insert_with(|| "request timed out".to_owned());
            }
        }
        if attempt + 1 < attempts {
            time::sleep(delay).await;
        }
    }
    if values.len() < minimum_successes {
        let first_failure = first_failure.as_deref().unwrap_or("unknown failure");
        bail!(
            "{label} produced {} successful samples out of {attempts}; at least {minimum_successes} are required; first failure: {first_failure}",
            values.len()
        );
    }
    let distribution = LatencyDistribution::from_samples(attempts, failures, durations)
        .context("required latency distribution unexpectedly had no successful samples")?;
    Ok((distribution, values))
}

fn validate_account_config(account: &OkxAccountConfig) -> Result<()> {
    account.ensure_spot_economics_safe()
}

fn validate_fee_group_instrument(
    instrument: &OkxInstrument,
    expected: &PreflightInstrument,
) -> Result<()> {
    ensure!(
        instrument.inst_type == "SPOT" && instrument.inst_id == expected.instrument_id,
        "OKX instrument metadata did not return exact SPOT {}",
        expected.instrument_id
    );
    ensure!(
        instrument.base_ccy == expected.base_currency
            && instrument.quote_ccy == expected.quote_currency,
        "OKX instrument {} currencies {}/{} contradict configured operator currencies {}/{}",
        expected.instrument_id,
        instrument.base_ccy,
        instrument.quote_ccy,
        expected.base_currency,
        expected.quote_currency
    );
    instrument.ensure_live()?;
    instrument.ensure_trade_quote_currency(&expected.quote_currency)?;
    instrument.fee_group_id()?;
    Ok(())
}

fn consistent_instrument_fee_group(rows: &[OkxInstrument]) -> Result<String> {
    let first = rows
        .first()
        .context("SPOT instrument metadata is required")?;
    let first_group_id = first.fee_group_id()?;
    for row in rows.iter().skip(1) {
        ensure!(
            row.inst_type == first.inst_type
                && row.inst_id == first.inst_id
                && row.state == first.state
                && row.base_ccy == first.base_ccy
                && row.quote_ccy == first.quote_ccy
                && row.trade_quote_currencies == first.trade_quote_currencies
                && row.fee_group_id()? == first_group_id,
            "OKX SPOT instrument identity or fee group changed during economics preflight"
        );
    }
    Ok(first_group_id.to_owned())
}

fn validate_fee(fee: &OkxTradeFeeRate, instrument_id: &str, expected_group_id: &str) -> Result<()> {
    fee.ensure_spot(instrument_id)?;
    ensure!(
        fee.group_id == expected_group_id,
        "OKX fee rate groupId {} does not match instrument groupId {expected_group_id}",
        fee.group_id
    );
    ensure!(
        !fee.level.trim().is_empty(),
        "OKX fee level must not be empty"
    );
    let _ = fee.normalized_maker_cost_rate()?;
    ensure!(
        fee.normalized_taker_cost_rate()? >= Decimal::ZERO,
        "OKX taker fee sign is unsupported; a taker rebate cannot be treated as a conservative cost"
    );
    Ok(())
}

fn consistent_account_safety(rows: &[OkxAccountConfig]) -> Result<AccountSafetyArtifact> {
    let first = rows.first().context("account configuration is required")?;
    let first_fee_type = first.spot_fee_type()?;
    for row in rows.iter().skip(1) {
        ensure!(
            row.account_level == first.account_level
                && row.auto_loan == first.auto_loan
                && row.enable_spot_borrow == first.enable_spot_borrow
                && row.spot_borrow_auto_repay == first.spot_borrow_auto_repay
                && row.spot_fee_type()? == first_fee_type,
            "OKX account safety configuration changed during economics preflight"
        );
    }
    Ok(AccountSafetyArtifact {
        spot_mode: true,
        borrowing_disabled: true,
        fee_type: spot_fee_type_label(first_fee_type).to_owned(),
    })
}

fn fee_artifact(rows: &[OkxTradeFeeRate]) -> Result<FeeArtifact> {
    let first = rows.first().context("SPOT fee rate is required")?;
    for row in rows.iter().skip(1) {
        ensure!(
            row.inst_type == first.inst_type
                && row.level == first.level
                && row.group_id == first.group_id
                && row.maker == first.maker
                && row.taker == first.taker,
            "OKX SPOT fee rate changed during economics preflight"
        );
    }
    let maker = first.normalized_maker_cost_rate()?;
    let taker = first.normalized_taker_cost_rate()?;
    ensure!(
        taker >= Decimal::ZERO,
        "OKX taker fee sign is unsupported; a taker rebate cannot be treated as a conservative cost"
    );
    let bps = Decimal::from(10_000u32);
    Ok(FeeArtifact {
        level: first.level.clone(),
        raw_maker: first.maker.clone(),
        raw_taker: first.taker.clone(),
        normalized_maker_cost_rate: decimal_string(maker),
        normalized_taker_cost_rate: decimal_string(taker),
        maker_semantics: if maker > Decimal::ZERO {
            "commission"
        } else if maker < Decimal::ZERO {
            "rebate"
        } else {
            "zero"
        }
        .to_owned(),
        maker_cost_bps: decimal_string(maker * bps),
        taker_cost_bps: decimal_string(taker * bps),
        maker_maker_round_trip_bps: decimal_string((maker + maker) * bps),
        maker_taker_round_trip_bps: decimal_string((maker + taker) * bps),
        taker_taker_round_trip_bps: decimal_string((taker + taker) * bps),
    })
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn duration_to_micros(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_micros()).context("latency duration exceeded u64 microseconds")
}

fn generated_at_ms() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?;
    u64::try_from(duration.as_millis()).context("generated timestamp exceeded u64 milliseconds")
}

fn parse_once<T>(slot: &mut Option<T>, value: T, option: &str) -> Result<()> {
    ensure!(
        slot.is_none(),
        "duplicate economics-preflight option {option}"
    );
    *slot = Some(value);
    Ok(())
}

fn next_value(args: &mut impl Iterator<Item = String>, option: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("economics-preflight option {option} requires a value"))
}

fn parse_usize(value: String, option: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .with_context(|| format!("economics-preflight option {option} must be an integer"))
}

fn parse_u64(value: String, option: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .with_context(|| format!("economics-preflight option {option} must be an integer"))
}

fn sanitized_profile_selector(selector: &str) -> Result<String> {
    Path::new(selector)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .context("economics-preflight profile selector must have a UTF-8 logical name")
}

const fn api_domain_label(api_domain: OkxApiDomain) -> &'static str {
    match api_domain {
        OkxApiDomain::Global => "GLOBAL",
        OkxApiDomain::UsAu => "US_AU",
        OkxApiDomain::Eea => "EEA",
    }
}

const fn trading_service_label(service: OkxTradingService) -> &'static str {
    match service {
        OkxTradingService::Production => "PRODUCTION",
        OkxTradingService::Demo => "DEMO",
    }
}

const fn spot_fee_type_label(fee_type: OkxSpotFeeType) -> &'static str {
    match fee_type {
        OkxSpotFeeType::ReceivedCurrency => "received_currency",
        OkxSpotFeeType::QuoteCurrency => "quote_currency",
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ValidatedPreflight {
    output: ValidatedOutput,
    profile: ProfileArtifact,
    instrument: PreflightInstrument,
    timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreflightInstrument {
    instrument_id: String,
    base_currency: String,
    quote_currency: String,
}

impl PreflightInstrument {
    fn from_config(instrument: &InstrumentConfig) -> Self {
        Self {
            instrument_id: instrument.okx_instrument_id(),
            base_currency: instrument.base_currency.clone(),
            quote_currency: instrument.quote_currency.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct ValidatedOutput {
    path: PathBuf,
}

impl ValidatedOutput {
    fn new(path: &Path) -> Result<Self> {
        ensure!(path.is_absolute(), "preflight output path must be absolute");
        ensure!(
            path.extension().and_then(|extension| extension.to_str()) == Some("json"),
            "preflight output path must use the .json extension"
        );
        match std::fs::symlink_metadata(path) {
            Ok(_) => bail!("preflight output already exists; refusing to overwrite it"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).context("failed checking preflight output create-new state");
            }
        }
        let parent = path
            .parent()
            .context("preflight output must have a parent directory")?;
        let parent = parent.canonicalize().with_context(|| {
            format!(
                "failed resolving preflight output parent {}",
                parent.display()
            )
        })?;
        ensure!(
            parent.is_dir(),
            "preflight output parent must be a directory"
        );
        let repository_root = repository_root();
        ensure!(
            !parent.starts_with(&repository_root),
            "preflight output must be outside the repository"
        );
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .context("preflight output must have a non-empty UTF-8 file name")?;
        Ok(Self {
            path: parent.join(file_name),
        })
    }

    fn write(&self, artifact: &EconomicsPreflightArtifact) -> Result<()> {
        let mut bytes = serde_json::to_vec_pretty(artifact)
            .context("failed serializing economics preflight artifact")?;
        bytes.push(b'\n');
        let parent = self
            .path
            .parent()
            .context("validated output lost its parent")?;
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .context("validated output file name must be UTF-8")?;

        let (temporary_path, mut temporary_file) = (0..100u32)
            .find_map(|nonce| {
                let temporary_path =
                    parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));
                match protected_create_new(&temporary_path) {
                    Ok(file) => Some(Ok((temporary_path, file))),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .context("failed allocating a unique protected preflight temporary file")?
            .context("failed creating protected preflight temporary file")?;

        let write_result = (|| -> Result<()> {
            temporary_file
                .write_all(&bytes)
                .context("failed writing economics preflight temporary artifact")?;
            temporary_file
                .sync_all()
                .context("failed syncing economics preflight temporary artifact")?;
            std::fs::hard_link(&temporary_path, &self.path)
                .context("failed atomically finalizing create-new economics preflight artifact")?;
            if let Err(error) = std::fs::remove_file(&temporary_path) {
                let _ = std::fs::remove_file(&self.path);
                return Err(error).context("failed removing economics preflight temporary link");
            }
            if let Err(error) = sync_directory(parent) {
                let _ = std::fs::remove_file(&self.path);
                return Err(error);
            }
            Ok(())
        })();
        if write_result.is_err() {
            let _ = std::fs::remove_file(&temporary_path);
        }
        write_result
    }
}

fn protected_create_new(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .context("failed syncing economics preflight output directory")
}

fn repository_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| normalize_path(&manifest.join("../..")))
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EconomicsPreflightArtifact {
    schema: String,
    generated_at_ms: u64,
    product: String,
    package_version: String,
    profile: ProfileArtifact,
    account_safety: AccountSafetyArtifact,
    fees: FeeArtifact,
    latency: LatencyArtifact,
    safety_assertions: SafetyAssertions,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProfileArtifact {
    selector: String,
    region: String,
    trading_service: String,
    instrument_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AccountSafetyArtifact {
    spot_mode: bool,
    borrowing_disabled: bool,
    fee_type: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FeeArtifact {
    level: String,
    raw_maker: String,
    raw_taker: String,
    normalized_maker_cost_rate: String,
    normalized_taker_cost_rate: String,
    maker_semantics: String,
    maker_cost_bps: String,
    taker_cost_bps: String,
    maker_maker_round_trip_bps: String,
    maker_taker_round_trip_bps: String,
    taker_taker_round_trip_bps: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LatencyArtifact {
    server_time_round_trip: LatencyDistribution,
    account_config_round_trip: LatencyDistribution,
    spot_fee_round_trip: LatencyDistribution,
    instrument_metadata_round_trip: LatencyDistribution,
    ticker_round_trip: LatencyDistribution,
    public_websocket_connect_and_subscribe: LatencyDistribution,
    private_websocket_login_and_subscribe: LatencyDistribution,
    trading_command_session_prepare: LatencyDistribution,
    trading_command_session_prepare_note: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LatencyDistribution {
    attempts: usize,
    successes: usize,
    failures: usize,
    minimum_microseconds: u64,
    p50_microseconds: u64,
    p95_microseconds: u64,
    p99_microseconds: u64,
    maximum_microseconds: u64,
    bounded_sample_count: usize,
}

impl LatencyDistribution {
    fn from_samples(attempts: usize, failures: usize, mut samples: Vec<u64>) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        Some(Self {
            attempts,
            successes: samples.len(),
            failures,
            minimum_microseconds: samples[0],
            p50_microseconds: nearest_rank(&samples, 50),
            p95_microseconds: nearest_rank(&samples, 95),
            p99_microseconds: nearest_rank(&samples, 99),
            maximum_microseconds: samples[samples.len() - 1],
            bounded_sample_count: samples.len(),
        })
    }
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    let rank = (percentile * sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SafetyAssertions {
    strategies_constructed: bool,
    orders_submitted: bool,
    orders_amended: bool,
    orders_cancelled: bool,
    cancel_all_after_called: bool,
    balances_read: bool,
    positions_read: bool,
    order_history_read: bool,
}

impl SafetyAssertions {
    const fn all_false() -> Self {
        Self {
            strategies_constructed: false,
            orders_submitted: false,
            orders_amended: false,
            orders_cancelled: false,
            cancel_all_after_called: false,
            balances_read: false,
            positions_read: false,
            order_history_read: false,
        }
    }
}

#[cfg(test)]
#[path = "economics_preflight_tests.rs"]
mod tests;
