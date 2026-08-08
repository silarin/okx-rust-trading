use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::Deserialize;

use crate::{
    config::{
        loader::finalize_config_with_secret_resolver,
        types::{
            BotConfig, InstrumentConfig, OkxConfig, OkxTradingService, ProductConfig,
            RequestedTradingInstrument, RuntimeConfig, StrategyConfig,
        },
        validation::validate_requested_trading_instrument,
    },
    okx::{
        client::{OkxCancelAllAfterTimeout, OkxRestClient},
        websocket::{
            OKX_PUBLIC_CANDLE_1M_CHANNEL, OkxMarketDataCache, OkxPrivateEventCache,
            OkxPrivateStream, OkxPrivateStreamConfig, OkxPrivateStreamCredentials,
            OkxPrivateStreamKind, OkxPrivateStreamTiming, OkxPublicMarketStream,
            OkxPublicMarketStreamConfig, OkxPublicMarketStreamTiming, OkxWebsocketHealthEventKind,
            OkxWebsocketHealthReporter, OkxWebsocketReconnectPolicy, OkxWebsocketStreamKind,
            trading_session::{
                OkxWebsocketTradingCommandConfig, OkxWebsocketTradingCommandCredentials,
                OkxWebsocketTradingCommandSession,
            },
        },
    },
};

#[path = "demo_caa_expiry_smoke.rs"]
mod demo_caa_expiry_smoke;
#[path = "demo_fill_lifecycle_smoke.rs"]
mod demo_fill_lifecycle_smoke;
#[path = "demo_oco_smoke.rs"]
mod demo_oco_smoke;
#[path = "demo_order_smoke.rs"]
mod demo_order_smoke;
#[path = "demo_post_only_cross_smoke.rs"]
mod demo_post_only_cross_smoke;
#[path = "demo_private_order_observer.rs"]
mod demo_private_order_observer;
#[path = "demo_private_stream_soak.rs"]
mod demo_private_stream_soak;
#[path = "demo_websocket_expiry_smoke.rs"]
mod demo_websocket_expiry_smoke;
#[path = "demo_websocket_order_smoke.rs"]
mod demo_websocket_order_smoke;

const SMOKE_ENABLED_ENV: &str = "OKX_DEMO_SMOKE";
const SMOKE_CAA_ENV: &str = "OKX_DEMO_SMOKE_CAA";
const SMOKE_ORDER_ENV: &str = "OKX_DEMO_SMOKE_ORDER";
const SMOKE_WEBSOCKET_ORDER_ENV: &str = "OKX_DEMO_SMOKE_WEBSOCKET_ORDER";
const SMOKE_WEBSOCKET_AMEND_ENV: &str = "OKX_DEMO_SMOKE_WEBSOCKET_AMEND";
const SMOKE_CAA_EXPIRY_ENV: &str = "OKX_DEMO_SMOKE_CAA_EXPIRY";
const SMOKE_POST_ONLY_CROSS_ENV: &str = "OKX_DEMO_SMOKE_POST_ONLY_CROSS";
const SMOKE_WEBSOCKET_EXPIRED_ENV: &str = "OKX_DEMO_SMOKE_WEBSOCKET_EXPIRED";
const SMOKE_FILL_LIFECYCLE_ENV: &str = "OKX_DEMO_SMOKE_FILL_LIFECYCLE";
const SMOKE_SPOT_OCO_ENV: &str = "OKX_DEMO_SMOKE_SPOT_OCO";
const SMOKE_ACQUISITION_PROBE_ENV: &str = "OKX_DEMO_SMOKE_ACQUISITION_PROBE";
const SMOKE_PRIVATE_SOAK_ENV: &str = "OKX_DEMO_SMOKE_PRIVATE_SOAK";
const SMOKE_PLACEHOLDER_SECRET: &str = "okx-demo-smoke-placeholder";
const DEMO_FUNCTIONAL_PROFILE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/demo-functional-profile.toml"
));
const PRIVATE_SECRET_ENVS: [&str; 3] = ["OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"];
const WEBSOCKET_SMOKE_TIMEOUT: Duration = Duration::from_secs(20);
const WEBSOCKET_ACK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DemoFunctionalProfileDto {
    product: ProductConfig,
    runtime: RuntimeConfig,
    okx: OkxConfig,
    instruments: Vec<InstrumentConfig>,
    trading_tuple: RequestedTradingInstrument,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn okx_demo_contract_smoke() -> Result<()> {
    let env = SmokeEnvironment::from_process();
    let plan = SmokePlan::from_environment(&env);
    if !plan.enabled {
        eprintln!(
            "skipping OKX demo smoke: set {SMOKE_ENABLED_ENV}=1 to run networked demo checks"
        );
        return Ok(());
    }

    let (config, requested) = load_demo_functional_profile_for_smoke(&env)?;
    crate::app::init_telemetry(&config)?;
    let instrument_id = requested.instrument.as_str().to_owned();
    let client = OkxRestClient::from_config(&config)?;

    eprintln!("running OKX demo smoke public REST checks for {instrument_id}");
    client
        .refresh_server_time_if_expiring()
        .await
        .context("OKX demo public server time smoke check failed")?;
    let instrument = client
        .validate_requested_public_instrument(&requested)
        .await
        .context("OKX demo SPOT instrument smoke check failed")?;
    ensure!(
        instrument.inst_id == instrument_id,
        "OKX demo smoke received instrument {} for requested {instrument_id}",
        instrument.inst_id
    );

    let requires_validated_tuple = matches!(&plan.private_checks, CheckPlan::Run)
        || matches!(&plan.private_soak, CheckPlan::Run)
        || matches!(&plan.cancel_all_after, CheckPlan::Run)
        || matches!(&plan.order_mutation, OrderMutationPlan::Run(_));
    let validated_instrument = if requires_validated_tuple {
        let account_config = client
            .account_config()
            .await
            .context("OKX Demo trading tuple account-config validation failed")?;
        let validated = client
            .validate_trading_instrument(&requested, &account_config)
            .await
            .context(
                "OKX Demo trading tuple public/account/sizing validation failed before private access or mutation",
            )?;
        Some(validated)
    } else {
        None
    };
    let candles = client
        .candles(&instrument_id, "1m", 3)
        .await
        .context("OKX demo 1m candles smoke check failed")?;
    ensure!(
        !candles.is_empty(),
        "OKX demo 1m candles smoke check returned no candles for {instrument_id}"
    );

    eprintln!("running OKX demo smoke public WebSocket checks for {instrument_id}");
    run_public_ticker_websocket_smoke(&config, &requested)
        .await
        .context("OKX demo public ticker WebSocket smoke check failed")?;
    run_business_candle_websocket_smoke(&config, &requested)
        .await
        .context("OKX demo business candle WebSocket smoke check failed")?;

    match &plan.private_checks {
        CheckPlan::Run => {
            eprintln!("running OKX demo smoke private read-only checks for {instrument_id}");
            run_private_read_only_smoke(&client, &config, &requested).await?;
        }
        CheckPlan::Skip(reason) => {
            eprintln!("skipping OKX demo smoke private checks: {reason}");
        }
    }

    match &plan.private_soak {
        CheckPlan::Run => {
            eprintln!("running bounded OKX Demo authenticated private-stream soak");
            demo_private_stream_soak::run_private_stream_soak(&client, &config, &instrument_id)
                .await?;
        }
        CheckPlan::Skip(reason) => {
            eprintln!("skipping OKX Demo private-stream soak: {reason}");
        }
    }

    match &plan.cancel_all_after {
        CheckPlan::Run => {
            eprintln!("running OKX demo smoke Cancel-All-After arm/disarm check");
            run_cancel_all_after_smoke(&client).await?;
        }
        CheckPlan::Skip(reason) => {
            eprintln!("skipping OKX demo smoke Cancel-All-After check: {reason}");
        }
    }

    let mutation_instrument = match &plan.order_mutation {
        OrderMutationPlan::Run(_) => Some(
            validated_instrument
                .as_deref()
                .context("OKX Demo mutation plan omitted its validated trading instrument")?,
        ),
        OrderMutationPlan::Skip(_) => None,
    };
    match &plan.order_mutation {
        OrderMutationPlan::Run(OrderMutationKind::Rest) => {
            eprintln!(
                "running one minimum-size {instrument_id} post-only Demo order with immediate cancel and REST cleanup verification"
            );
            demo_order_smoke::run_post_only_place_cancel_smoke(
                &client,
                mutation_instrument.context("validated Demo mutation instrument")?,
            )
            .await?;
        }
        OrderMutationPlan::Run(OrderMutationKind::WebSocket) => {
            eprintln!(
                "running one minimum-size {instrument_id} post-only Demo order through WebSocket commands with immediate cancel and REST cleanup verification"
            );
            demo_order_smoke::run_websocket_post_only_place_cancel_smoke(
                &client,
                &config,
                mutation_instrument.context("validated Demo mutation instrument")?,
            )
            .await?;
        }
        OrderMutationPlan::Run(OrderMutationKind::WebSocketAmend) => {
            eprintln!(
                "running one minimum-size {instrument_id} post-only Demo WebSocket place/amend/cancel lifecycle with REST amendment and cleanup verification"
            );
            demo_order_smoke::run_websocket_post_only_place_amend_cancel_smoke(
                &client,
                &config,
                mutation_instrument.context("validated Demo mutation instrument")?,
            )
            .await?;
        }
        OrderMutationPlan::Run(OrderMutationKind::CancelAllAfterExpiry) => {
            eprintln!(
                "running one minimum-size {instrument_id} post-only Demo order until Cancel-All-After expiry with private-event observation and REST cleanup verification"
            );
            demo_caa_expiry_smoke::run_cancel_all_after_expiry_smoke(
                &client,
                &config,
                mutation_instrument.context("validated Demo mutation instrument")?,
            )
            .await?;
        }
        OrderMutationPlan::Run(OrderMutationKind::PostOnlyCross) => {
            eprintln!(
                "running one minimum-size {instrument_id} crossing post-only Demo order with immediate private-event and REST zero-fill verification"
            );
            demo_post_only_cross_smoke::run_crossing_post_only_smoke(
                &client,
                &config,
                mutation_instrument.context("validated Demo mutation instrument")?,
            )
            .await?;
        }
        OrderMutationPlan::Run(OrderMutationKind::WebSocketExpired) => {
            eprintln!(
                "running one expired {instrument_id} WebSocket Demo post-only request with REST absence verification"
            );
            demo_websocket_expiry_smoke::run_expired_websocket_place_smoke(
                &client,
                &config,
                mutation_instrument.context("validated Demo mutation instrument")?,
            )
            .await?;
        }
        OrderMutationPlan::Run(OrderMutationKind::FillLifecycle) => {
            eprintln!(
                "running one explicitly gated {instrument_id} Demo taker buy/sell fill lifecycle with fee, fill-classification, balance, and REST cleanup verification"
            );
            demo_fill_lifecycle_smoke::run_fill_lifecycle_smoke(
                &client,
                mutation_instrument.context("validated Demo mutation instrument")?,
            )
            .await?;
        }
        OrderMutationPlan::Run(OrderMutationKind::SpotOco) => {
            eprintln!(
                "running the explicitly gated {instrument_id} Demo standalone OCO placement, cancellation, TP, SL, restart, amendment, ownership, and REST cleanup contract"
            );
            demo_oco_smoke::run_spot_oco_lifecycle_smoke(
                &client,
                &config,
                mutation_instrument.context("validated Demo mutation instrument")?,
            )
            .await?;
        }
        OrderMutationPlan::Run(OrderMutationKind::AcquisitionProbe) => {
            eprintln!(
                "running one explicitly gated {instrument_id} Demo acquisition-only buy/cleanup probe with OKX sizing preflight; no OCO scenario will run"
            );
            demo_oco_smoke::run_acquisition_probe(
                &client,
                mutation_instrument.context("validated Demo mutation instrument")?,
            )
            .await?;
        }
        OrderMutationPlan::Skip(reason) => {
            eprintln!("skipping OKX demo order-mutation check: {reason}");
        }
    }

    Ok(())
}

fn load_demo_functional_profile_for_smoke(
    env: &SmokeEnvironment,
) -> Result<(BotConfig, RequestedTradingInstrument)> {
    load_demo_functional_profile_from_str(DEMO_FUNCTIONAL_PROFILE, env)
}

fn load_demo_functional_profile_from_str(
    contents: &str,
    env: &SmokeEnvironment,
) -> Result<(BotConfig, RequestedTradingInstrument)> {
    let dto = toml::from_str::<DemoFunctionalProfileDto>(contents)
        .context("failed parsing strict OKX Demo functional profile DTO")?;
    validate_requested_trading_instrument(&dto.trading_tuple)
        .context("invalid OKX Demo functional trading-tuple DTO")?;
    let enabled_instrument_ids = dto
        .instruments
        .iter()
        .filter(|instrument| instrument.enabled)
        .map(InstrumentConfig::okx_instrument_id)
        .collect::<Vec<_>>();
    ensure!(
        enabled_instrument_ids.len() == 1
            && enabled_instrument_ids[0] == dto.trading_tuple.instrument.as_str(),
        "OKX Demo functional profile requires exactly one enabled operator instrument matching its trading_tuple"
    );
    let requested = dto.trading_tuple;
    let config = BotConfig {
        product: dto.product,
        runtime: dto.runtime,
        okx: Some(dto.okx),
        instruments: dto.instruments,
        strategies: StrategyConfig::default(),
    };
    let config = finalize_config_with_secret_resolver(config, |name| smoke_secret_value(env, name))
        .context("failed validating strict OKX Demo functional profile")?;
    let okx = config
        .okx
        .as_ref()
        .context("OKX demo smoke functional profile must include [okx]")?;
    ensure_demo_trading_service(okx.trading_service)?;
    Ok((config, requested))
}

fn ensure_demo_trading_service(trading_service: OkxTradingService) -> Result<()> {
    ensure!(
        trading_service == OkxTradingService::Demo,
        "OKX demo smoke must run only against the functional profile with okx.trading_service = DEMO"
    );
    Ok(())
}

fn smoke_secret_value(env: &SmokeEnvironment, name: &str) -> Option<String> {
    if name.ends_with("_FILE") {
        return env.value(name);
    }
    if let Some(value) = env.value(name) {
        return Some(value);
    }
    let file_name = format!("{name}_FILE");
    if env.value(&file_name).is_some() {
        return None;
    }
    PRIVATE_SECRET_ENVS
        .contains(&name)
        .then(|| SMOKE_PLACEHOLDER_SECRET.to_owned())
}

async fn run_public_ticker_websocket_smoke(
    config: &BotConfig,
    requested: &RequestedTradingInstrument,
) -> Result<()> {
    let okx = config.okx.as_ref().context("OKX config is required")?;
    let url = okx
        .base_url_ws_public
        .clone()
        .context("OKX base_url_ws_public is required for demo smoke")?;
    let stream_config =
        OkxPublicMarketStreamConfig::new(url, vec![requested.instrument.as_str().to_owned()])?
            .with_validated_instrument_type(requested.inst_type.as_okx())?;
    run_public_websocket_until_ack(stream_config, OkxWebsocketStreamKind::Public).await
}

async fn run_business_candle_websocket_smoke(
    config: &BotConfig,
    requested: &RequestedTradingInstrument,
) -> Result<()> {
    let okx = config.okx.as_ref().context("OKX config is required")?;
    let url = okx
        .base_url_ws_business
        .clone()
        .context("OKX base_url_ws_business is required for demo smoke")?;
    let reconnect_policy = OkxWebsocketReconnectPolicy::new(
        Duration::from_millis(okx.websocket.reconnect_initial_backoff_ms),
        Duration::from_millis(okx.websocket.reconnect_max_backoff_ms),
    )?;
    let stream_config = OkxPublicMarketStreamConfig::with_reconnect_policy(
        url,
        vec![requested.instrument.as_str().to_owned()],
        /*subscribe_tickers*/ false,
        /*subscribe_instruments*/ false,
        vec![OKX_PUBLIC_CANDLE_1M_CHANNEL.to_owned()],
        reconnect_policy,
    )?
    .with_validated_instrument_type(requested.inst_type.as_okx())?;
    run_public_websocket_until_ack(stream_config, OkxWebsocketStreamKind::Business).await
}

async fn run_public_websocket_until_ack(
    stream_config: OkxPublicMarketStreamConfig,
    expected_kind: OkxWebsocketStreamKind,
) -> Result<()> {
    let (health, mut receiver) = OkxWebsocketHealthReporter::channel(16);
    let timing = OkxPublicMarketStreamTiming::new(
        /*idle_ping_after*/ WEBSOCKET_SMOKE_TIMEOUT,
        /*idle_pong_timeout*/ WEBSOCKET_SMOKE_TIMEOUT,
        /*subscription_ack_timeout*/ WEBSOCKET_ACK_TIMEOUT,
    )?;
    let stream = OkxPublicMarketStream::spawn_with_health_and_timing(
        stream_config,
        OkxMarketDataCache::default(),
        Some(health),
        timing,
    );
    let result = wait_for_websocket_subscription_ack(&mut receiver, expected_kind).await;
    drop(stream);
    result
}

async fn wait_for_websocket_subscription_ack(
    receiver: &mut crate::okx::websocket::OkxWebsocketHealthReceiver,
    expected_kind: OkxWebsocketStreamKind,
) -> Result<()> {
    tokio::time::timeout(WEBSOCKET_SMOKE_TIMEOUT, async {
        while let Some(event) = receiver.recv().await {
            let stream = event.stream();
            if stream.kind() != expected_kind {
                continue;
            }
            match event.kind() {
                OkxWebsocketHealthEventKind::SubscriptionAckSucceeded => return Ok(()),
                OkxWebsocketHealthEventKind::SubscriptionAckFailed
                | OkxWebsocketHealthEventKind::LoginFailed
                | OkxWebsocketHealthEventKind::StreamFailedBeforeSubscription
                | OkxWebsocketHealthEventKind::StreamTaskPanicked
                | OkxWebsocketHealthEventKind::StreamTaskExitedUnexpectedly => {
                    bail!(
                        "OKX demo WebSocket smoke check failed before subscription readiness: {}",
                        event.kind()
                    );
                }
                OkxWebsocketHealthEventKind::ConnectAttempt
                | OkxWebsocketHealthEventKind::Connected
                | OkxWebsocketHealthEventKind::LoginAckSucceeded
                | OkxWebsocketHealthEventKind::ReconnectScheduled
                | OkxWebsocketHealthEventKind::StreamDisconnectedAfterSubscription
                | OkxWebsocketHealthEventKind::StreamFailedAfterSubscription => {}
            }
        }
        bail!("OKX demo WebSocket smoke health channel closed before subscription readiness")
    })
    .await
    .context("timed out waiting for OKX demo WebSocket subscription acknowledgement")?
}

async fn run_private_read_only_smoke(
    client: &OkxRestClient,
    config: &BotConfig,
    requested: &RequestedTradingInstrument,
) -> Result<()> {
    let instrument_id = requested.instrument.as_str();
    let account_config = client
        .account_config()
        .await
        .context("OKX demo account config smoke check failed")?;
    account_config
        .ensure_spot_trading_enabled()
        .context("OKX demo account config is not compatible with spot cash smoke checks")?;
    client
        .balances()
        .await
        .context("OKX demo balance smoke check failed")?;
    client
        .spot_trade_fee(instrument_id)
        .await
        .context("OKX demo SPOT trade-fee smoke check failed")?;
    client
        .open_orders(instrument_id)
        .await
        .context("OKX demo open SPOT orders smoke check failed")?;
    client
        .order_history(instrument_id)
        .await
        .context("OKX demo SPOT order history smoke check failed")?;
    client
        .order_fills(instrument_id)
        .await
        .context("OKX demo SPOT fills smoke check failed")?;
    client
        .open_algo_orders(instrument_id)
        .await
        .context("OKX demo open SPOT algo orders smoke check failed")?;
    client
        .algo_order_history(instrument_id)
        .await
        .context("OKX demo SPOT algo order history smoke check failed")?;
    run_private_websocket_login_smoke(client, config).await?;
    run_private_websocket_subscription_smoke(client, config, requested).await
}

async fn run_private_websocket_login_smoke(
    client: &OkxRestClient,
    config: &BotConfig,
) -> Result<()> {
    let okx = config.okx.as_ref().context("OKX config is required")?;
    let url = okx
        .base_url_ws_private
        .clone()
        .context("OKX base_url_ws_private is required for private WebSocket demo smoke")?;
    let credentials = OkxWebsocketTradingCommandCredentials::new(
        okx.api_key.clone(),
        okx.api_secret.clone(),
        okx.api_passphrase.clone(),
    )?;
    let command_config = OkxWebsocketTradingCommandConfig::with_ack_timeout(
        url,
        credentials,
        WEBSOCKET_ACK_TIMEOUT,
    )?;
    let login_timestamp = client
        .websocket_login_timestamp()
        .await
        .context("OKX demo private WebSocket login timestamp sync failed")?;
    let session = tokio::time::timeout(
        WEBSOCKET_SMOKE_TIMEOUT,
        OkxWebsocketTradingCommandSession::connect(command_config, &login_timestamp),
    )
    .await
    .context("timed out waiting for OKX demo private WebSocket login acknowledgement")?
    .context("OKX demo private WebSocket login smoke check failed")?;
    drop(session);
    Ok(())
}

async fn run_private_websocket_subscription_smoke(
    client: &OkxRestClient,
    config: &BotConfig,
    requested: &RequestedTradingInstrument,
) -> Result<()> {
    let instrument_id = requested.instrument.as_str();
    let okx = config.okx.as_ref().context("OKX config is required")?;
    let credentials = Arc::new(OkxPrivateStreamCredentials::new(
        okx.api_key.clone(),
        okx.api_secret.clone(),
        okx.api_passphrase.clone(),
    )?);
    let reconnect_policy = OkxWebsocketReconnectPolicy::new(
        Duration::from_millis(okx.websocket.reconnect_initial_backoff_ms),
        Duration::from_millis(okx.websocket.reconnect_max_backoff_ms),
    )?;
    let timing = OkxPrivateStreamTiming::new(
        /*idle_ping_after*/ WEBSOCKET_SMOKE_TIMEOUT,
        /*idle_pong_timeout*/ WEBSOCKET_SMOKE_TIMEOUT,
        /*login_ack_timeout*/ WEBSOCKET_ACK_TIMEOUT,
        /*subscription_ack_timeout*/ WEBSOCKET_ACK_TIMEOUT,
    )?;
    let login_timestamp_provider = client.websocket_login_timestamp_provider();

    let private_url = okx
        .base_url_ws_private
        .clone()
        .context("OKX base_url_ws_private is required for private subscription smoke")?;
    let private_config = OkxPrivateStreamConfig::with_reconnect_policy(
        private_url,
        OkxPrivateStreamKind::Trading,
        vec![instrument_id.to_owned()],
        okx.api_domain,
        Arc::clone(&credentials),
        reconnect_policy,
    )?
    .with_validated_instrument_type(requested.inst_type.as_okx())?
    .without_optional_fills();
    run_private_websocket_until_ack(
        private_config,
        login_timestamp_provider.clone(),
        OkxWebsocketStreamKind::Private,
        timing,
    )
    .await
    .context("OKX demo private account/order subscription smoke check failed")?;

    let business_url = okx
        .base_url_ws_business
        .clone()
        .context("OKX base_url_ws_business is required for algo subscription smoke")?;
    let business_config = OkxPrivateStreamConfig::with_reconnect_policy(
        business_url,
        OkxPrivateStreamKind::Business,
        vec![instrument_id.to_owned()],
        okx.api_domain,
        credentials,
        reconnect_policy,
    )?
    .with_validated_instrument_type(requested.inst_type.as_okx())?;
    run_private_websocket_until_ack(
        business_config,
        login_timestamp_provider,
        OkxWebsocketStreamKind::Business,
        timing,
    )
    .await
    .context("OKX demo private algo-order subscription smoke check failed")
}

async fn run_private_websocket_until_ack(
    stream_config: OkxPrivateStreamConfig,
    login_timestamp_provider: crate::okx::client::OkxWebsocketLoginTimestampProvider,
    expected_kind: OkxWebsocketStreamKind,
    timing: OkxPrivateStreamTiming,
) -> Result<()> {
    let (health, mut receiver) = OkxWebsocketHealthReporter::channel(16);
    let stream = OkxPrivateStream::spawn_with_health_and_timing(
        stream_config,
        OkxPrivateEventCache::default(),
        login_timestamp_provider,
        Some(health),
        timing,
    );
    let result = wait_for_websocket_subscription_ack(&mut receiver, expected_kind).await;
    drop(stream);
    result
}

async fn run_cancel_all_after_smoke(client: &OkxRestClient) -> Result<()> {
    let timeout = OkxCancelAllAfterTimeout::new(OkxCancelAllAfterTimeout::MIN_SECONDS)?;
    let arm_result = client
        .cancel_all_after(timeout)
        .await
        .context("OKX demo Cancel-All-After arm smoke check failed");
    let disarm_result = client
        .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
        .await
        .context("OKX demo Cancel-All-After disarm cleanup failed");

    match (arm_result, disarm_result) {
        (Ok(_), Ok(_)) => Ok(()),
        (Err(arm_error), Ok(_)) => Err(arm_error),
        (Ok(_), Err(disarm_error)) => Err(disarm_error),
        (Err(arm_error), Err(disarm_error)) => Err(anyhow!(
            "OKX demo Cancel-All-After arm failed: {arm_error:#}; disarm cleanup also failed: {disarm_error:#}"
        )),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct SmokePlan {
    enabled: bool,
    private_checks: CheckPlan,
    private_soak: CheckPlan,
    cancel_all_after: CheckPlan,
    order_mutation: OrderMutationPlan,
}

impl SmokePlan {
    fn from_environment(env: &SmokeEnvironment) -> Self {
        let enabled = env.is_enabled(SMOKE_ENABLED_ENV);
        if !enabled {
            return Self {
                enabled,
                private_checks: CheckPlan::Skip(format!("{SMOKE_ENABLED_ENV} is not set to 1")),
                private_soak: CheckPlan::Skip(format!("{SMOKE_ENABLED_ENV} is not set to 1")),
                cancel_all_after: CheckPlan::Skip(format!("{SMOKE_ENABLED_ENV} is not set to 1")),
                order_mutation: OrderMutationPlan::Skip(format!(
                    "{SMOKE_ENABLED_ENV} is not set to 1"
                )),
            };
        }

        let missing_private = missing_private_credentials(env);
        let private_checks = if missing_private.is_empty() {
            CheckPlan::Run
        } else {
            CheckPlan::Skip(format!(
                "missing demo credential environment values: {}",
                missing_private.join(", ")
            ))
        };
        let order_mutation = order_mutation_plan(env, &missing_private);
        let private_soak = if !env.is_enabled(SMOKE_PRIVATE_SOAK_ENV) {
            CheckPlan::Skip(format!("{SMOKE_PRIVATE_SOAK_ENV} is not set to 1"))
        } else if missing_private.is_empty() {
            CheckPlan::Run
        } else {
            CheckPlan::Skip(format!(
                "{SMOKE_PRIVATE_SOAK_ENV}=1 was set, but private soak requires demo credentials: {}",
                missing_private.join(", ")
            ))
        };
        let cancel_all_after = if let OrderMutationPlan::Run(kind) = &order_mutation {
            CheckPlan::Skip(format!(
                "{}=1 includes its own Cancel-All-After lifecycle",
                kind.gate()
            ))
        } else if !env.is_enabled(SMOKE_CAA_ENV) {
            CheckPlan::Skip(format!(
                "{SMOKE_CAA_ENV} is not set to 1; this check mutates OKX demo account dead-man-switch state"
            ))
        } else if missing_private.is_empty() {
            CheckPlan::Run
        } else {
            CheckPlan::Skip(format!(
                "{SMOKE_CAA_ENV}=1 was set, but CAA requires demo credentials: {}",
                missing_private.join(", ")
            ))
        };

        Self {
            enabled,
            private_checks,
            private_soak,
            cancel_all_after,
            order_mutation,
        }
    }
}

fn order_mutation_plan(env: &SmokeEnvironment, missing_private: &[&str]) -> OrderMutationPlan {
    let requested = OrderMutationKind::ALL
        .iter()
        .copied()
        .filter(|kind| env.is_enabled(kind.gate()))
        .collect::<Vec<_>>();
    match requested.as_slice() {
        [] => OrderMutationPlan::Skip(format!(
            "none of {}, {}, {}, {}, {}, {}, {}, {}, or {} is set to 1",
            SMOKE_ORDER_ENV,
            SMOKE_WEBSOCKET_ORDER_ENV,
            SMOKE_WEBSOCKET_AMEND_ENV,
            SMOKE_CAA_EXPIRY_ENV,
            SMOKE_POST_ONLY_CROSS_ENV,
            SMOKE_WEBSOCKET_EXPIRED_ENV,
            SMOKE_FILL_LIFECYCLE_ENV,
            SMOKE_SPOT_OCO_ENV,
            SMOKE_ACQUISITION_PROBE_ENV
        )),
        [kind] if missing_private.is_empty() => OrderMutationPlan::Run(*kind),
        [kind] => OrderMutationPlan::Skip(format!(
            "{}=1 was set, but order mutation requires demo credentials: {}",
            kind.gate(),
            missing_private.join(", ")
        )),
        [_, _, ..] => OrderMutationPlan::Skip(format!(
            "multiple OKX Demo order-mutation gates are set: {}; choose exactly one",
            requested
                .iter()
                .map(|kind| kind.gate())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrderMutationKind {
    Rest,
    WebSocket,
    WebSocketAmend,
    CancelAllAfterExpiry,
    PostOnlyCross,
    WebSocketExpired,
    FillLifecycle,
    SpotOco,
    AcquisitionProbe,
}

impl OrderMutationKind {
    const ALL: [Self; 9] = [
        Self::Rest,
        Self::WebSocket,
        Self::WebSocketAmend,
        Self::CancelAllAfterExpiry,
        Self::PostOnlyCross,
        Self::WebSocketExpired,
        Self::FillLifecycle,
        Self::SpotOco,
        Self::AcquisitionProbe,
    ];

    const fn gate(self) -> &'static str {
        match self {
            Self::Rest => SMOKE_ORDER_ENV,
            Self::WebSocket => SMOKE_WEBSOCKET_ORDER_ENV,
            Self::WebSocketAmend => SMOKE_WEBSOCKET_AMEND_ENV,
            Self::CancelAllAfterExpiry => SMOKE_CAA_EXPIRY_ENV,
            Self::PostOnlyCross => SMOKE_POST_ONLY_CROSS_ENV,
            Self::WebSocketExpired => SMOKE_WEBSOCKET_EXPIRED_ENV,
            Self::FillLifecycle => SMOKE_FILL_LIFECYCLE_ENV,
            Self::SpotOco => SMOKE_SPOT_OCO_ENV,
            Self::AcquisitionProbe => SMOKE_ACQUISITION_PROBE_ENV,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum OrderMutationPlan {
    Run(OrderMutationKind),
    Skip(String),
}

#[derive(Debug, Eq, PartialEq)]
enum CheckPlan {
    Run,
    Skip(String),
}

fn missing_private_credentials(env: &SmokeEnvironment) -> Vec<&'static str> {
    PRIVATE_SECRET_ENVS
        .iter()
        .copied()
        .filter(|name| !env.has_value_or_file(name))
        .collect()
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SmokeEnvironment {
    values: HashMap<String, String>,
}

impl SmokeEnvironment {
    fn from_process() -> Self {
        Self {
            values: std::env::vars().collect(),
        }
    }

    fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        Self {
            values: pairs
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
        }
    }

    fn value(&self, name: &str) -> Option<String> {
        self.values
            .get(name)
            .filter(|value| !value.is_empty())
            .cloned()
    }

    fn is_enabled(&self, name: &str) -> bool {
        self.values.get(name).is_some_and(|value| value == "1")
    }

    fn has_value_or_file(&self, name: &str) -> bool {
        self.value(name).is_some() || self.value(&format!("{name}_FILE")).is_some()
    }
}

#[cfg(test)]
#[path = "demo_smoke_plan_tests.rs"]
mod smoke_plan_tests;
