use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::Path,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, KeyInit, Mac};
use pretty_assertions::assert_eq;
use sha2::Sha256;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt};

use crate::{
    app::DEFAULT_TELEMETRY_FILTER,
    config::{
        loader::load_config_path_with_secret_resolver,
        types::{
            OkxAccountJurisdiction, OkxApiDomain, OkxConfig, OkxTradingService, OkxWebsocketConfig,
        },
    },
    okx::{
        capability::AccountLevelDiagnosticSnapshot,
        types::{OkxAccountConfig, OkxFill, OkxInstrument, OrderKind, OrderSide},
    },
    test_support::{CapturedLogs, HttpTestServer as OrderHistoryServer},
};

use super::{
    AlgoHistoryFilter, Method, OKX_ALGO_HISTORY_MAX_PAGES, OKX_API_KEY, OKX_API_PASSPHRASE,
    OKX_API_SIGN, OKX_API_TIMESTAMP, OKX_CANCEL_ALL_AFTER_TAG, OKX_DOCUMENTED_SIMULATED_TRADING,
    OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW, OKX_OPEN_ALGO_ORDERS_MAX_PAGES,
    OKX_OPEN_ORDERS_MAX_PAGES, OKX_ORDER_FILLS_MAX_PAGES, OKX_ORDER_HISTORY_MAX_PAGES,
    OKX_RATE_LIMIT_COOLDOWN, OKX_RATE_LIMIT_RULES, OkxEnvelope, OkxEnvelopeLatency, OkxEnvelopeRaw,
    OkxEnvelopeTiming, OkxGatewayLatencyRecorder, OkxGatewayLatencySummary, OkxRateLimitRule,
    OkxRestClient, RateLimitBucket, RateLimitPacer, RateLimitScope, ServerTimeSnapshot,
    algo_order_history_query, emit_okx_gateway_latency_summary, emit_okx_gateway_timing,
    format_okx_timestamp, okx_gateway_latency_exceeds_warn_threshold, okx_rate_limit_bucket,
    open_algo_orders_query, open_orders_query, order_fills_query, order_history_query, sign,
    websocket_login_timestamp_from_unix_millis,
};

#[test]
fn signs_okx_prehash_payload() {
    let signature = sign(
        "EXAMPLE-API-SECRET",
        "2020-12-08T09:08:57.715Z",
        "GET",
        "/api/v5/account/balance?ccy=BTC",
        "",
    )
    .expect("signature should be generated");

    assert_eq!(signature, "rD20POigM/qN7CE149IXHtWhgRNJIR90tQMntPYLSms=");
}

#[test]
fn capability_validation_reuses_the_rest_observation_timestamp() {
    let client = test_client("127.0.0.1:1".parse().expect("socket address"))
        .expect("test client should build");
    let account = OkxAccountConfig {
        uid: "1001".to_owned(),
        main_uid: "1001".to_owned(),
        account_level: "1".to_owned(),
        perm: "read_only,trade".to_owned(),
        auto_loan: false,
        enable_spot_borrow: false,
        spot_borrow_auto_repay: false,
        fee_type: "0".to_owned(),
        kyc_level: "2".to_owned(),
    };
    let stale = AccountLevelDiagnosticSnapshot::stale_for_test(&account, Duration::from_secs(31))
        .expect("stale diagnostic should build");
    *client
        .account_level_diagnostic
        .lock()
        .expect("diagnostic lock") = Some(stale);

    let selected = client
        .matching_observed_account_level_diagnostic(&account)
        .expect("matching observation should be retained");

    assert!(selected.ensure_fresh(Duration::from_secs(30)).is_err());
}

#[test]
fn formats_okx_timestamps_with_millisecond_precision() {
    assert_eq!(
        format_okx_timestamp(4_102_444_800_123).expect("timestamp should format"),
        "2100-01-01T00:00:00.123Z"
    );
    assert_eq!(
        format_okx_timestamp(4_102_444_800_000).expect("timestamp should format"),
        "2100-01-01T00:00:00.000Z"
    );
}

#[test]
fn okx_envelope_timing_reports_gateway_latency_microseconds() {
    let envelope = serde_json::from_str::<OkxEnvelope<Vec<serde_json::Value>>>(
        r#"{"code":"0","msg":"","data":[],"inTime":"1695190491421339","outTime":"1695190491423240"}"#,
    )
    .expect("OKX envelope with gateway timing should parse");

    assert_eq!(envelope.code, "0");
    assert_eq!(envelope.msg, "");
    assert_eq!(envelope.data, Vec::<serde_json::Value>::new());
    assert_eq!(
        envelope.timing.gateway_latency(),
        Some(OkxEnvelopeLatency {
            in_time_microseconds: 1_695_190_491_421_339,
            out_time_microseconds: 1_695_190_491_423_240,
            gateway_latency_microseconds: 1_901,
        })
    );
}

#[test]
fn okx_raw_envelope_defers_typed_data_parsing() {
    #[derive(Debug, Eq, PartialEq, serde::Deserialize)]
    struct RawOrder {
        #[serde(rename = "ordId")]
        order_id: String,
    }

    let envelope: OkxEnvelopeRaw<'_> =
        serde_json::from_str(r#"{"code":"0","msg":"","data":[{"ordId":"ord-raw"}]}"#)
            .expect("raw OKX envelope should parse");
    let raw_data = envelope.data.expect("raw OKX data should be present");
    let orders: Vec<RawOrder> =
        serde_json::from_str(raw_data.get()).expect("raw OKX data should parse into DTO");

    assert_eq!(envelope.code, "0");
    assert_eq!(envelope.msg, "");
    assert_eq!(raw_data.get(), r#"[{"ordId":"ord-raw"}]"#);
    assert_eq!(
        orders,
        vec![RawOrder {
            order_id: "ord-raw".to_owned()
        }]
    );
}

#[test]
fn okx_envelope_timing_ignores_missing_malformed_or_inverted_values() {
    let cases = [
        OkxEnvelopeTiming::default(),
        OkxEnvelopeTiming {
            in_time_microseconds: Some("not-a-timestamp".to_owned()),
            out_time_microseconds: Some("1695190491423240".to_owned()),
        },
        OkxEnvelopeTiming {
            in_time_microseconds: Some("1695190491423240".to_owned()),
            out_time_microseconds: Some("1695190491421339".to_owned()),
        },
    ];

    for timing in cases {
        assert_eq!(timing.gateway_latency(), None);
    }
}

#[test]
fn okx_envelope_timing_classifies_slow_gateway_latency_for_operator_visibility() {
    assert!(!okx_gateway_latency_exceeds_warn_threshold(
        OkxEnvelopeLatency {
            in_time_microseconds: 1_000,
            out_time_microseconds: 250_999,
            gateway_latency_microseconds: 249_999,
        }
    ));
    assert!(okx_gateway_latency_exceeds_warn_threshold(
        OkxEnvelopeLatency {
            in_time_microseconds: 1_000,
            out_time_microseconds: 251_000,
            gateway_latency_microseconds: 250_000,
        }
    ));
}

#[test]
fn default_telemetry_filter_keeps_normal_gateway_timing_opt_in() {
    let logs = CapturedLogs::default();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(
                EnvFilter::try_new(DEFAULT_TELEMETRY_FILTER)
                    .expect("default telemetry filter should parse"),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(logs.clone()),
            ),
    );
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let bucket = RateLimitBucket::new("GET /api/v5/test|ip".to_owned(), 20);

    emit_okx_gateway_timing(
        &bucket,
        OkxEnvelopeLatency {
            in_time_microseconds: 1_000,
            out_time_microseconds: 2_000,
            gateway_latency_microseconds: 1_000,
        },
    );
    emit_okx_gateway_timing(
        &bucket,
        OkxEnvelopeLatency {
            in_time_microseconds: 1_000,
            out_time_microseconds: 251_000,
            gateway_latency_microseconds: 250_000,
        },
    );
    emit_okx_gateway_latency_summary(OkxGatewayLatencySummary {
        sample_count: OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW,
        slow_sample_count: 1,
        average_gateway_latency_microseconds: 10_000,
        max_gateway_latency_microseconds: 250_000,
    });
    tracing::warn!(
        safety_event = "runtime_fatal_fail_closed",
        "runtime fatal fail-closed safety event"
    );
    let contents = logs.contents();

    assert!(!contents.contains("captured OKX REST gateway timing"));
    assert!(contents.contains("slow OKX REST gateway timing"));
    assert!(contents.contains("summarized OKX REST gateway timing"));
    assert!(contents.contains("runtime_fatal_fail_closed"));
}

#[test]
fn explicit_debug_filter_enables_normal_gateway_timing() {
    let logs = CapturedLogs::default();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(
                EnvFilter::try_new("info,okx_trading_runtime=debug")
                    .expect("explicit debug telemetry filter should parse"),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(logs.clone()),
            ),
    );
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let bucket = RateLimitBucket::new("GET /api/v5/test|ip".to_owned(), 20);

    emit_okx_gateway_timing(
        &bucket,
        OkxEnvelopeLatency {
            in_time_microseconds: 1_000,
            out_time_microseconds: 2_000,
            gateway_latency_microseconds: 1_000,
        },
    );

    assert!(logs.contents().contains("captured OKX REST gateway timing"));
}

#[test]
fn okx_gateway_latency_recorder_summarizes_bounded_windows() {
    let recorder = OkxGatewayLatencyRecorder::default();
    let normal_latency = OkxEnvelopeLatency {
        in_time_microseconds: 1_000,
        out_time_microseconds: 2_000,
        gateway_latency_microseconds: 1_000,
    };
    let slow_latency = OkxEnvelopeLatency {
        in_time_microseconds: 1_000,
        out_time_microseconds: 251_000,
        gateway_latency_microseconds: 250_000,
    };

    for _ in 1..OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW {
        assert_eq!(recorder.record(normal_latency), None);
    }

    let expected_average = u64::try_from(
        ((u128::from(OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW) - 1) * 1_000 + 250_000)
            / u128::from(OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW),
    )
    .expect("expected average should fit in u64");
    assert_eq!(
        recorder.record(slow_latency),
        Some(OkxGatewayLatencySummary {
            sample_count: OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW,
            slow_sample_count: 1,
            average_gateway_latency_microseconds: expected_average,
            max_gateway_latency_microseconds: 250_000,
        })
    );
    assert_eq!(recorder.record(normal_latency), None);
}

#[test]
fn okx_gateway_latency_recorder_recovers_poisoned_window_with_warning() {
    let recorder = OkxGatewayLatencyRecorder::default();
    let normal_latency = OkxEnvelopeLatency {
        in_time_microseconds: 1_000,
        out_time_microseconds: 2_000,
        gateway_latency_microseconds: 1_000,
    };
    let slow_latency = OkxEnvelopeLatency {
        in_time_microseconds: 1_000,
        out_time_microseconds: 251_000,
        gateway_latency_microseconds: 250_000,
    };
    assert_eq!(recorder.record(slow_latency), None);

    let poison_result = std::panic::catch_unwind(|| {
        let _guard = recorder
            .window
            .lock()
            .expect("test recorder lock should work");
        panic!("poison gateway latency recorder");
    });
    assert!(poison_result.is_err());

    let logs = CapturedLogs::default();
    let dispatch = logs.dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    assert_eq!(recorder.record(normal_latency), None);
    let contents = logs.contents();
    assert!(contents.contains("okx_gateway_latency_recorder_poisoned"));
    assert!(contents.contains("reset_window"));

    for _ in 0..(OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW - 2) {
        assert_eq!(recorder.record(normal_latency), None);
    }

    let expected_average = u64::try_from(
        ((u128::from(OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW) - 1) * 1_000 + 250_000)
            / u128::from(OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW),
    )
    .expect("expected average should fit in u64");
    assert_eq!(
        recorder.record(slow_latency),
        Some(OkxGatewayLatencySummary {
            sample_count: OKX_GATEWAY_LATENCY_SUMMARY_SAMPLE_WINDOW,
            slow_sample_count: 1,
            average_gateway_latency_microseconds: expected_average,
            max_gateway_latency_microseconds: 250_000,
        })
    );
    assert_eq!(recorder.record(normal_latency), None);
}

#[test]
fn builds_open_orders_query_without_cursor() {
    let after = None;

    assert_eq!(
        open_orders_query("SPOT", "BTC-USDT", after),
        "instType=SPOT&instId=BTC-USDT&limit=100"
    );
}

#[test]
fn builds_open_orders_query_with_after_cursor() {
    assert_eq!(
        open_orders_query("SPOT", "BTC-USDT", Some("123456789")),
        "instType=SPOT&instId=BTC-USDT&limit=100&after=123456789"
    );
}

#[test]
fn query_builders_percent_encode_values() {
    assert_eq!(
        open_orders_query("SPOT", "BTC-USDT", Some("cursor/1?x=2&y=3")),
        "instType=SPOT&instId=BTC-USDT&limit=100&after=cursor%2F1%3Fx%3D2%26y%3D3"
    );
    assert_eq!(
        order_history_query("SPOT", "BTC-USDT", Some("cursor/1?x=2&y=3")),
        "instType=SPOT&instId=BTC-USDT&limit=100&after=cursor%2F1%3Fx%3D2%26y%3D3"
    );
    assert_eq!(
        order_fills_query("SPOT", "BTC-USDT", Some("bill/1?x=2&y=3")),
        "instType=SPOT&instId=BTC-USDT&limit=100&after=bill%2F1%3Fx%3D2%26y%3D3"
    );
    assert_eq!(
        open_algo_orders_query("SPOT", "BTC-USDT", Some("cursor/1?x=2&y=3")),
        "instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100&after=cursor%2F1%3Fx%3D2%26y%3D3"
    );
    assert_eq!(
        algo_order_history_query(
            "SPOT",
            "BTC-USDT",
            AlgoHistoryFilter::AlgoId("algo/1?x=2&y=3"),
            Some("cursor/1?x=2&y=3"),
        ),
        "instType=SPOT&instId=BTC-USDT&ordType=trigger&algoId=algo%2F1%3Fx%3D2%26y%3D3&limit=100&after=cursor%2F1%3Fx%3D2%26y%3D3"
    );
}

#[test]
fn builds_order_history_query_without_cursor() {
    let after = None;

    assert_eq!(
        order_history_query("SPOT", "BTC-USDT", after),
        "instType=SPOT&instId=BTC-USDT&limit=100"
    );
}

#[test]
fn builds_order_history_query_with_after_cursor() {
    assert_eq!(
        order_history_query("SPOT", "BTC-USDT", Some("123456789")),
        "instType=SPOT&instId=BTC-USDT&limit=100&after=123456789"
    );
}

#[test]
fn builds_order_fills_query_without_cursor() {
    let after = None;

    assert_eq!(
        order_fills_query("SPOT", "BTC-USDT", after),
        "instType=SPOT&instId=BTC-USDT&limit=100"
    );
}

#[test]
fn builds_order_fills_query_with_after_cursor() {
    assert_eq!(
        order_fills_query("SPOT", "BTC-USDT", Some("bill-123")),
        "instType=SPOT&instId=BTC-USDT&limit=100&after=bill-123"
    );
}

#[test]
fn builds_open_algo_orders_query_without_cursor() {
    let after = None;

    assert_eq!(
        open_algo_orders_query("SPOT", "BTC-USDT", after),
        "instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100"
    );
}

#[test]
fn builds_open_algo_orders_query_with_after_cursor() {
    assert_eq!(
        open_algo_orders_query("SPOT", "BTC-USDT", Some("123456789")),
        "instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100&after=123456789"
    );
}

#[test]
fn builds_algo_order_history_query_without_cursor() {
    let after = None;

    assert_eq!(
        algo_order_history_query(
            "SPOT",
            "BTC-USDT",
            AlgoHistoryFilter::State("effective"),
            after
        ),
        "instType=SPOT&instId=BTC-USDT&ordType=trigger&state=effective&limit=100"
    );
}

#[test]
fn builds_algo_order_history_query_with_after_cursor() {
    assert_eq!(
        algo_order_history_query(
            "SPOT",
            "BTC-USDT",
            AlgoHistoryFilter::State("canceled"),
            Some("123456789"),
        ),
        "instType=SPOT&instId=BTC-USDT&ordType=trigger&state=canceled&limit=100&after=123456789"
    );
}

#[test]
fn builds_algo_order_history_query_with_algo_id_filter() {
    assert_eq!(
        algo_order_history_query(
            "SPOT",
            "BTC-USDT",
            AlgoHistoryFilter::AlgoId("123456789"),
            /*after*/ None,
        ),
        "instType=SPOT&instId=BTC-USDT&ordType=trigger&algoId=123456789&limit=100"
    );
}

#[test]
fn okx_rate_limit_rules_are_auditable_inventory() {
    assert_eq!(
        OKX_RATE_LIMIT_RULES,
        &[
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/public/time",
                limit: 10,
                scope: RateLimitScope::Ip,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/public/instruments",
                limit: 20,
                scope: RateLimitScope::IpInstrumentType,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/public/price-limit",
                limit: 20,
                scope: RateLimitScope::Ip,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/public/market-data-history",
                limit: 5,
                scope: RateLimitScope::Ip,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/market/candles",
                limit: 40,
                scope: RateLimitScope::Ip,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/market/history-candles",
                limit: 20,
                scope: RateLimitScope::Ip,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/market/history-trades",
                limit: 20,
                scope: RateLimitScope::Ip,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/market/ticker",
                limit: 20,
                scope: RateLimitScope::Ip,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/market/index-tickers",
                limit: 20,
                scope: RateLimitScope::Ip,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/market/books",
                limit: 40,
                scope: RateLimitScope::Ip,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/account/instruments",
                limit: 20,
                scope: RateLimitScope::UserInstrumentType,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/account/config",
                limit: 5,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/account/balance",
                limit: 10,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/account/trade-fee",
                limit: 5,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/account/max-size",
                limit: 20,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/account/max-avail-size",
                limit: 20,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/trade/order",
                limit: 60,
                scope: RateLimitScope::UserInstrument,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/trade/order-algo",
                limit: 20,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/trade/orders-pending",
                limit: 60,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/trade/orders-history",
                limit: 40,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/trade/orders-history-archive",
                limit: 20,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/trade/fills",
                limit: 60,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/trade/fills-history",
                limit: 10,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "POST",
                path: "/api/v5/trade/order",
                limit: 60,
                scope: RateLimitScope::UserInstrument,
            },
            OkxRateLimitRule {
                method: "POST",
                path: "/api/v5/trade/cancel-order",
                limit: 60,
                scope: RateLimitScope::UserInstrument,
            },
            OkxRateLimitRule {
                method: "POST",
                path: "/api/v5/trade/amend-order",
                limit: 60,
                scope: RateLimitScope::UserInstrument,
            },
            OkxRateLimitRule {
                method: "POST",
                path: "/api/v5/trade/order-algo",
                limit: 20,
                scope: RateLimitScope::UserInstrument,
            },
            OkxRateLimitRule {
                method: "POST",
                path: "/api/v5/trade/cancel-algos",
                limit: 20,
                scope: RateLimitScope::UserInstrument,
            },
            OkxRateLimitRule {
                method: "POST",
                path: "/api/v5/trade/amend-algos",
                limit: 20,
                scope: RateLimitScope::UserInstrument,
            },
            OkxRateLimitRule {
                method: "POST",
                path: "/api/v5/trade/cancel-all-after",
                limit: 1,
                scope: RateLimitScope::UserTag,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/trade/orders-algo-pending",
                limit: 20,
                scope: RateLimitScope::User,
            },
            OkxRateLimitRule {
                method: "GET",
                path: "/api/v5/trade/orders-algo-history",
                limit: 20,
                scope: RateLimitScope::User,
            },
        ]
    );

    let mut endpoint_keys = BTreeSet::new();
    for rule in OKX_RATE_LIMIT_RULES {
        assert!(
            endpoint_keys.insert((rule.method, rule.path)),
            "duplicate OKX rate limit rule for {} {}",
            rule.method,
            rule.path
        );
    }
}

#[test]
fn okx_rate_limit_buckets_follow_documented_scopes() {
    let open_btc = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/trade/orders-pending",
        Some("instType=SPOT&instId=BTC-USDT"),
        None,
    )
    .expect("open order bucket should be built");
    let open_eth = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/trade/orders-pending",
        Some("instType=SPOT&instId=ETH-USDT"),
        None,
    )
    .expect("open order bucket should be built");
    assert_eq!(open_btc, open_eth);
    assert_eq!(open_btc.limit, 60);
    assert_eq!(open_btc.key, "GET /api/v5/trade/orders-pending|user");

    let place_btc =
        okx_rate_limit_bucket(&Method::POST, "/api/v5/trade/order", None, Some("BTC-USDT"))
            .expect("place order bucket should be built");
    let place_eth =
        okx_rate_limit_bucket(&Method::POST, "/api/v5/trade/order", None, Some("ETH-USDT"))
            .expect("place order bucket should be built");
    assert_ne!(place_btc.key, place_eth.key);
    assert_eq!(place_btc.limit, 60);
    assert_eq!(
        place_btc.key,
        "POST /api/v5/trade/order|user+instId:BTC-USDT"
    );
    let amend_btc = okx_rate_limit_bucket(
        &Method::POST,
        "/api/v5/trade/amend-order",
        None,
        Some("BTC-USDT"),
    )
    .expect("amend order bucket should be built");
    assert_eq!(amend_btc.limit, 60);
    assert_eq!(
        amend_btc.key,
        "POST /api/v5/trade/amend-order|user+instId:BTC-USDT"
    );

    let spot_instruments = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/public/instruments",
        Some("instType=SPOT&instId=BTC-USDT"),
        None,
    )
    .expect("instrument bucket should be built");
    let margin_instruments = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/public/instruments",
        Some("instType=MARGIN&instId=BTC-USDT"),
        None,
    )
    .expect("instrument bucket should be built");
    assert_ne!(spot_instruments.key, margin_instruments.key);
    assert_eq!(spot_instruments.limit, 20);
    assert_eq!(
        spot_instruments.key,
        "GET /api/v5/public/instruments|ip+instType:SPOT"
    );

    let ticker_btc = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/market/ticker",
        Some("instId=BTC-USDT"),
        None,
    )
    .expect("ticker bucket should be built");
    let ticker_eth = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/market/ticker",
        Some("instId=ETH-USDT"),
        None,
    )
    .expect("ticker bucket should be built");
    assert_eq!(ticker_btc, ticker_eth);
    assert_eq!(ticker_btc.limit, 20);
    assert_eq!(ticker_btc.key, "GET /api/v5/market/ticker|ip");

    let index_ticker_usdt = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/market/index-tickers",
        Some("instId=USDT-USD"),
        None,
    )
    .expect("index-ticker bucket should be built");
    let index_ticker_usdc = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/market/index-tickers",
        Some("instId=USDC-USD"),
        None,
    )
    .expect("index-ticker bucket should be built");
    assert_eq!(index_ticker_usdt, index_ticker_usdc);
    assert_eq!(index_ticker_usdt.limit, 20);
    assert_eq!(index_ticker_usdt.key, "GET /api/v5/market/index-tickers|ip");

    let books = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/market/books",
        Some("instId=BTC-USDT&sz=50"),
        None,
    )
    .expect("order-book bucket should be built");
    assert_eq!(books.limit, 40);
    assert_eq!(books.key, "GET /api/v5/market/books|ip");

    let account_instruments = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/account/instruments",
        Some("instType=SPOT"),
        None,
    )
    .expect("account-instruments bucket should be built");
    assert_eq!(account_instruments.limit, 20);
    assert_eq!(
        account_instruments.key,
        "GET /api/v5/account/instruments|user+instType:SPOT"
    );

    let account_config = okx_rate_limit_bucket(&Method::GET, "/api/v5/account/config", None, None)
        .expect("account config bucket should be built");
    assert_eq!(account_config.limit, 5);
    assert_eq!(account_config.key, "GET /api/v5/account/config|user");

    let trade_fee = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/account/trade-fee",
        Some("instType=SPOT&instId=BTC-USDT"),
        None,
    )
    .expect("trade-fee bucket should be built");
    assert_eq!(trade_fee.limit, 5);
    assert_eq!(trade_fee.key, "GET /api/v5/account/trade-fee|user");

    for path in ["/api/v5/account/max-size", "/api/v5/account/max-avail-size"] {
        let sizing = okx_rate_limit_bucket(
            &Method::GET,
            path,
            Some("instId=BTC-USDT&tdMode=cash"),
            None,
        )
        .expect("account sizing bucket should be built");
        assert_eq!(sizing.limit, 20);
        assert_eq!(sizing.key, format!("GET {path}|user"));
    }

    let algo_lookup = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/trade/order-algo",
        Some("algoClOrdId=stop-1"),
        None,
    )
    .expect("algo lookup bucket should be built");
    assert_eq!(algo_lookup.limit, 20);
    assert_eq!(algo_lookup.key, "GET /api/v5/trade/order-algo|user");

    let cancel_all_after =
        okx_rate_limit_bucket(&Method::POST, "/api/v5/trade/cancel-all-after", None, None)
            .expect("cancel-all-after bucket should be built");
    assert_eq!(cancel_all_after.limit, 1);
    assert_eq!(cancel_all_after.window, std::time::Duration::from_secs(1));
    assert_eq!(
        cancel_all_after.key,
        format!("POST /api/v5/trade/cancel-all-after|user+tag:{OKX_CANCEL_ALL_AFTER_TAG}")
    );
}

#[test]
fn okx_rate_limit_bucket_requires_instrument_for_instrument_scoped_endpoints() {
    let error = okx_rate_limit_bucket(&Method::POST, "/api/v5/trade/order", None, None)
        .expect_err("instrument-scoped order endpoint should require instId");

    assert!(
        error.to_string().contains("requires OKX instId"),
        "missing instrument should explain the bad rate-limit bucket: {error}"
    );
}

#[test]
fn proactive_rate_limit_pacer_reserves_two_second_windows() {
    let pacer = RateLimitPacer::default();
    let bucket = RateLimitBucket {
        key: "test bucket".to_owned(),
        limit: 2,
        window: std::time::Duration::from_millis(100),
    };
    let now = Instant::now();

    assert_eq!(
        pacer
            .reserve_or_wait(&bucket, now)
            .expect("first request should reserve"),
        None
    );
    assert_eq!(
        pacer
            .reserve_or_wait(&bucket, now + std::time::Duration::from_millis(10))
            .expect("second request should reserve"),
        None
    );
    assert_eq!(
        pacer
            .reserve_or_wait(&bucket, now + std::time::Duration::from_millis(20))
            .expect("full window should wait"),
        Some(std::time::Duration::from_millis(130))
    );
    assert_eq!(
        pacer
            .reserve_or_wait(&bucket, now + std::time::Duration::from_millis(101))
            .expect("safety margin should still pace"),
        Some(std::time::Duration::from_millis(49))
    );
    assert_eq!(
        pacer
            .reserve_or_wait(&bucket, now + std::time::Duration::from_millis(151))
            .expect("window plus safety margin should reserve"),
        None
    );
}

#[test]
fn reactive_rate_limit_cooldown_blocks_bucket_after_okx_rejection() {
    let pacer = RateLimitPacer::default();
    let bucket = RateLimitBucket {
        key: "test cooldown".to_owned(),
        limit: 100,
        window: std::time::Duration::from_millis(100),
    };
    let now = Instant::now();

    pacer
        .record_rate_limit_at(&bucket, now)
        .expect("cooldown should record");
    assert_eq!(
        pacer
            .reserve_or_wait(&bucket, now + std::time::Duration::from_millis(250))
            .expect("cooldown should be checked"),
        Some(OKX_RATE_LIMIT_COOLDOWN - std::time::Duration::from_millis(250))
    );
}

#[tokio::test]
async fn private_requests_sync_okx_server_time_before_signing() {
    let server = OrderHistoryServer::spawn(vec![
        okx_server_time_body("4102444810123"),
        empty_okx_data_body(),
    ])
    .await
    .expect("test server should start");
    let client = unsynced_test_client(server.addr()).expect("test client should build");

    client
        .open_orders("BTC-USDT")
        .await
        .expect("open order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 2);
    assert!(
        requests[0].starts_with("GET /api/v5/public/time "),
        "first private call should sync OKX server time first: {}",
        requests[0]
    );
    assert!(
        requests[1].starts_with(
            "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100 "
        ),
        "second request should be the private OKX request: {}",
        requests[1]
    );
    let timestamp = request_header_value(&requests[1], OKX_API_TIMESTAMP)
        .expect("private request should include OKX timestamp");
    assert!(
        timestamp.starts_with("2100-01-01T00:00:"),
        "private request should use OKX server time, got {timestamp}"
    );
    assert_private_auth_headers(&requests[1]);
    let signature = request_header_value(&requests[1], OKX_API_SIGN)
        .expect("private request should include OKX signature");
    let expected_signature = sign(
        "secret",
        timestamp,
        "GET",
        raw_request_target(&requests[1]),
        "",
    )
    .expect("expected signature should be generated");
    assert_eq!(signature, expected_signature);
}

#[test]
fn websocket_login_timestamp_uses_unix_seconds() {
    assert_eq!(
        websocket_login_timestamp_from_unix_millis(4_102_444_810_123)
            .expect("positive server time should format"),
        "4102444810"
    );
}

#[tokio::test]
async fn private_post_signs_exact_serialized_json_body() {
    let server = OrderHistoryServer::spawn(vec![order_ack_body("ord-new", "entry-1")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100"),
            "entry-1",
        )
        .await
        .expect("place order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].starts_with("POST /api/v5/trade/order "),
        "private POST request used unexpected target: {}",
        requests[0]
    );
    assert_private_auth_headers(&requests[0]);
    let timestamp = request_header_value(&requests[0], OKX_API_TIMESTAMP)
        .expect("private request should include OKX timestamp");
    let signature = request_header_value(&requests[0], OKX_API_SIGN)
        .expect("private request should include OKX signature");
    let body = request_body(&requests[0]);
    assert!(!body.is_empty(), "private POST body should be sent");
    let request_target = raw_request_target(&requests[0]);
    let prehash = format!("{timestamp}POST{request_target}{body}");
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret")
        .expect("fixed test secret should initialize HMAC");
    mac.update(prehash.as_bytes());
    let expected_signature = BASE64.encode(mac.finalize().into_bytes());

    assert_eq!(signature, expected_signature);
}

#[tokio::test]
async fn private_requests_reuse_recent_okx_server_time_offset() {
    let server = OrderHistoryServer::spawn(vec![
        okx_server_time_body("4102444810123"),
        empty_okx_data_body(),
        empty_okx_data_body(),
    ])
    .await
    .expect("test server should start");
    let client = unsynced_test_client(server.addr()).expect("test client should build");

    client
        .open_orders("BTC-USDT")
        .await
        .expect("first open order request should succeed");
    client
        .open_orders("BTC-USDT")
        .await
        .expect("second open order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 3);
    assert!(
        requests[0].starts_with("GET /api/v5/public/time "),
        "first request should sync OKX server time: {}",
        requests[0]
    );
    assert!(
        requests[1].starts_with("GET /api/v5/trade/orders-pending?"),
        "second request should be private: {}",
        requests[1]
    );
    assert!(
        requests[2].starts_with("GET /api/v5/trade/orders-pending?"),
        "third request should reuse cached OKX server time: {}",
        requests[2]
    );
}

#[tokio::test]
async fn invalid_okx_server_time_blocks_private_request() {
    let server = OrderHistoryServer::spawn(vec![empty_okx_data_body()])
        .await
        .expect("test server should start");
    let client = unsynced_test_client(server.addr()).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("invalid OKX server time should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX server time returned 0 rows"),
        "server time failure should identify invalid OKX time data: {error}"
    );
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].starts_with("GET /api/v5/public/time "),
        "private request should stop after failed server time sync: {}",
        requests[0]
    );
}

#[tokio::test]
async fn private_requests_include_only_documented_simulated_trading_header_when_enabled() {
    let server = OrderHistoryServer::spawn(vec![empty_okx_data_body()])
        .await
        .expect("test server should start");
    let client = simulated_test_client(server.addr()).expect("test client should build");

    client
        .open_orders("BTC-USDT")
        .await
        .expect("open order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 1);
    assert!(
        !request_has_header_name(&requests[0], "OKX-SIMULATED-TRADING"),
        "demo OKX private requests should omit removed simulated trading header: {}",
        requests[0]
    );
    assert!(
        request_has_header(&requests[0], OKX_DOCUMENTED_SIMULATED_TRADING, "1"),
        "demo OKX private requests should include documented simulated trading header: {}",
        requests[0]
    );
}

#[tokio::test]
async fn private_requests_omit_simulated_trading_header_when_disabled() {
    let server = OrderHistoryServer::spawn(vec![empty_okx_data_body()])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .open_orders("BTC-USDT")
        .await
        .expect("open order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 1);
    assert!(
        !request_has_header_name(&requests[0], "OKX-SIMULATED-TRADING"),
        "live OKX private requests should omit removed simulated trading header: {}",
        requests[0]
    );
    assert!(
        !request_has_header_name(&requests[0], OKX_DOCUMENTED_SIMULATED_TRADING),
        "live OKX private requests should omit documented simulated trading header: {}",
        requests[0]
    );
}

#[tokio::test]
async fn from_config_uses_demo_profile_routing_for_simulated_trading_header() {
    let server = OrderHistoryServer::spawn(vec![empty_okx_data_body()])
        .await
        .expect("test server should start");
    let client = profile_test_client(
        "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        server.addr(),
    )
    .expect("demo profile client should build");

    client
        .open_orders("BTC-USDT")
        .await
        .expect("open order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 1);
    assert!(
        !request_has_header_name(&requests[0], "OKX-SIMULATED-TRADING"),
        "checked-in demo routing should omit removed simulated trading header: {}",
        requests[0]
    );
    assert!(
        request_has_header(&requests[0], OKX_DOCUMENTED_SIMULATED_TRADING, "1"),
        "checked-in demo routing should enable documented simulated trading header: {}",
        requests[0]
    );
}

#[tokio::test]
async fn from_config_uses_live_profile_routing_without_simulated_trading_header() {
    let server = OrderHistoryServer::spawn(vec![empty_okx_data_body()])
        .await
        .expect("test server should start");
    let client = profile_test_client("config/live.toml", server.addr())
        .expect("live profile client should build");

    client
        .open_orders("BTC-USDT")
        .await
        .expect("open order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 1);
    assert!(
        !request_has_header_name(&requests[0], "OKX-SIMULATED-TRADING"),
        "checked-in live routing should omit removed simulated trading header: {}",
        requests[0]
    );
    assert!(
        !request_has_header_name(&requests[0], OKX_DOCUMENTED_SIMULATED_TRADING),
        "checked-in live routing should omit documented simulated trading header: {}",
        requests[0]
    );
}

#[tokio::test]
async fn open_orders_pages_with_after_cursor() {
    let first_page = order_history_body((0..100).map(|index| format!("ord-{index:03}")));
    let second_page = order_history_body(["ord-older"]);
    let server = OrderHistoryServer::spawn(vec![first_page, second_page])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let orders = client
        .open_orders("BTC-USDT")
        .await
        .expect("open orders should page");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(orders.len(), 101);
    assert_eq!(orders[0].order_id, "ord-000");
    assert_eq!(orders[100].order_id, "ord-older");
    assert!(
        requests[0].starts_with(
            "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100 "
        ),
        "first open orders request used unexpected target: {}",
        requests[0]
    );
    assert!(
        requests[1].starts_with(
            "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100&after=ord-099 "
        ),
        "second open orders request used unexpected target: {}",
        requests[1]
    );
}

#[tokio::test]
async fn open_orders_fails_closed_when_page_cap_is_reached() {
    let pages = (0..OKX_OPEN_ORDERS_MAX_PAGES)
        .map(|page| {
            order_history_body((0..100).map(move |index| format!("ord-{page:02}-{index:03}")))
        })
        .collect();
    let server = OrderHistoryServer::spawn(pages)
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("open orders should fail when the bounded page cap is reached");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("refusing to use partial open orders"),
        "page cap failure should refuse partial open orders: {error}"
    );
    assert_eq!(requests.len(), OKX_OPEN_ORDERS_MAX_PAGES);
}

#[tokio::test]
async fn order_history_pages_with_after_cursor() {
    let first_page = order_history_body((0..100).map(|index| format!("ord-{index:03}")));
    let second_page = order_history_body(["ord-older"]);
    let server = OrderHistoryServer::spawn(vec![first_page, second_page, empty_okx_data_body()])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let orders = client
        .order_history("BTC-USDT")
        .await
        .expect("order history should page");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(orders.len(), 101);
    assert_eq!(orders[0].order_id, "ord-000");
    assert_eq!(orders[100].order_id, "ord-older");
    assert!(
        requests[0].starts_with(
            "GET /api/v5/trade/orders-history?instType=SPOT&instId=BTC-USDT&limit=100 "
        ),
        "first request used unexpected target: {}",
        requests[0]
    );
    assert!(
        requests[1].starts_with(
            "GET /api/v5/trade/orders-history?instType=SPOT&instId=BTC-USDT&limit=100&after=ord-099 "
        ),
        "second request used unexpected target: {}",
        requests[1]
    );
    assert!(
        requests[2].starts_with(
            "GET /api/v5/trade/orders-history-archive?instType=SPOT&instId=BTC-USDT&limit=100 "
        ),
        "archive request used unexpected target: {}",
        requests[2]
    );
}

#[tokio::test]
async fn order_history_reads_archive_and_dedupes_recent_overlap() {
    let server = OrderHistoryServer::spawn(vec![
        order_history_body(["ord-recent"]),
        order_history_body(["ord-recent", "ord-older"]),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let orders = client
        .order_history("BTC-USDT")
        .await
        .expect("order history should include archive rows");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        orders
            .iter()
            .map(|order| order.order_id.as_str())
            .collect::<Vec<_>>(),
        ["ord-recent", "ord-older"]
    );
    assert!(
        requests[1].starts_with(
            "GET /api/v5/trade/orders-history-archive?instType=SPOT&instId=BTC-USDT&limit=100 "
        ),
        "archive request used unexpected target: {}",
        requests[1]
    );
}

#[tokio::test]
async fn order_history_fails_closed_when_page_cap_is_reached() {
    let pages = (0..OKX_ORDER_HISTORY_MAX_PAGES)
        .map(|page| {
            order_history_body((0..100).map(move |index| format!("ord-{page:02}-{index:03}")))
        })
        .collect();
    let server = OrderHistoryServer::spawn(pages)
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .order_history("BTC-USDT")
        .await
        .expect_err("order history should fail when the bounded page cap is reached");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("refusing to use partial history"),
        "page cap failure should refuse partial history: {error}"
    );
    assert_eq!(requests.len(), OKX_ORDER_HISTORY_MAX_PAGES);
}

#[tokio::test]
async fn order_history_rejects_mismatched_instrument_rows() {
    let server = OrderHistoryServer::spawn(vec![order_history_body_with_instrument(
        "ETH-USDT",
        ["ord-wrong"],
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .order_history("BTC-USDT")
        .await
        .expect_err("order history should reject mismatched instruments");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("for instrument ETH-USDT while requesting BTC-USDT"),
        "mismatched order history instrument should be reported: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn order_fills_pages_with_after_cursor() {
    let first_page = order_fills_body((0..100).map(|index| format!("bill-{index:03}")));
    let second_page = order_fills_body(["bill-older"]);
    let server = OrderHistoryServer::spawn(vec![first_page, second_page, empty_okx_data_body()])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let fills = client
        .order_fills("BTC-USDT")
        .await
        .expect("order fills should page");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(fills.len(), 101);
    assert_eq!(fills[0].bill_id, "bill-000");
    assert_eq!(fills[100].bill_id, "bill-older");
    assert!(
        requests[0].starts_with("GET /api/v5/trade/fills?instType=SPOT&instId=BTC-USDT&limit=100 "),
        "first fills request used unexpected target: {}",
        requests[0]
    );
    assert!(
        requests[1].starts_with(
            "GET /api/v5/trade/fills?instType=SPOT&instId=BTC-USDT&limit=100&after=bill-099 "
        ),
        "second fills request used unexpected target: {}",
        requests[1]
    );
    assert!(
        requests[2].starts_with(
            "GET /api/v5/trade/fills-history?instType=SPOT&instId=BTC-USDT&limit=100 "
        ),
        "fills archive request used unexpected target: {}",
        requests[2]
    );
}

#[tokio::test]
async fn order_fills_reads_history_and_dedupes_recent_overlap() {
    let server = OrderHistoryServer::spawn(vec![
        order_fills_body(["bill-recent"]),
        order_fills_body(["bill-recent", "bill-older"]),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let fills = client
        .order_fills("BTC-USDT")
        .await
        .expect("order fills should include history rows");

    assert_eq!(
        fills
            .iter()
            .map(|fill| fill.bill_id.as_str())
            .collect::<Vec<_>>(),
        ["bill-recent", "bill-older"]
    );
}

#[tokio::test]
async fn order_fills_parses_distinct_identifiers_and_timestamps() {
    let server = OrderHistoryServer::spawn(vec![
        r#"{"code":"0","msg":"","data":[{"instType":"SPOT","instId":"BTC-USDT","ordId":"ord-1","clOrdId":"client-1","billId":"bill-1","tradeId":"trade-1","side":"buy","fillSz":"0.001","fillPx":"100","fillTime":"1700000000000","ts":"1700000000001"}]}"#
            .to_owned(),
        empty_okx_data_body(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let fills = client
        .order_fills("BTC-USDT")
        .await
        .expect("OKX fills may contain distinct identifier and timestamp fields");

    assert_eq!(fills[0].dedupe_key(), "bill-1");
    assert_eq!(
        fills,
        vec![OkxFill {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            order_id: "ord-1".to_owned(),
            client_order_id: "client-1".to_owned(),
            bill_id: "bill-1".to_owned(),
            trade_id: "trade-1".to_owned(),
            side: "buy".to_owned(),
            fill_size: "0.001".to_owned(),
            fill_price: "100".to_owned(),
            fee: String::new(),
            fee_currency: String::new(),
            fee_rate: String::new(),
            execution_type: String::new(),
            fill_time_ms: "1700000000000".to_owned(),
            event_time_ms: "1700000000001".to_owned(),
        }]
    );
}

#[tokio::test]
async fn order_fills_fails_closed_when_page_cap_is_reached() {
    let pages = (0..OKX_ORDER_FILLS_MAX_PAGES)
        .map(|page| {
            order_fills_body((0..100).map(move |index| format!("bill-{page:02}-{index:03}")))
        })
        .collect();
    let server = OrderHistoryServer::spawn(pages)
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .order_fills("BTC-USDT")
        .await
        .expect_err("order fills should fail when the bounded page cap is reached");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error.to_string().contains("refusing to use partial fills"),
        "page cap failure should refuse partial fills: {error}"
    );
    assert_eq!(requests.len(), OKX_ORDER_FILLS_MAX_PAGES);
}

#[tokio::test]
async fn order_fills_rejects_mismatched_instrument_rows() {
    let server = OrderHistoryServer::spawn(vec![order_fills_body_with_instrument(
        "ETH-USDT",
        ["bill-wrong"],
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .order_fills("BTC-USDT")
        .await
        .expect_err("order fills should reject mismatched instruments");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("for instrument ETH-USDT while requesting BTC-USDT"),
        "mismatched order fills instrument should be reported: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn order_fills_rejects_non_spot_inst_type_rows() {
    let server = OrderHistoryServer::spawn(vec![format!(
        r#"{{"code":"0","msg":"","data":[{}]}}"#,
        fill_json_with_inst_type("SWAP", "BTC-USDT", "bill-wrong")
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .order_fills("BTC-USDT")
        .await
        .expect_err("order fills should reject non-spot OKX rows");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX order fills returned instType SWAP for BTC-USDT; expected SPOT"),
        "non-spot order fill row should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn open_algo_orders_pages_with_after_cursor() {
    let first_page = algo_history_body((0..100).map(|index| format!("algo-{index:03}")));
    let second_page = algo_history_body(["algo-older"]);
    let server = OrderHistoryServer::spawn(vec![first_page, second_page])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let orders = client
        .open_algo_orders("BTC-USDT")
        .await
        .expect("open algo orders should page");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(orders.len(), 101);
    assert_eq!(orders[0].algo_id, "algo-000");
    assert_eq!(orders[100].algo_id, "algo-older");
    assert!(
        requests[0].starts_with(
            "GET /api/v5/trade/orders-algo-pending?instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100 "
        ),
        "first open algo request used unexpected target: {}",
        requests[0]
    );
    assert!(
        requests[1].starts_with(
            "GET /api/v5/trade/orders-algo-pending?instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100&after=algo-099 "
        ),
        "second open algo request used unexpected target: {}",
        requests[1]
    );
}

#[tokio::test]
async fn open_algo_orders_fails_closed_when_page_cap_is_reached() {
    let pages = (0..OKX_OPEN_ALGO_ORDERS_MAX_PAGES)
        .map(|page| {
            algo_history_body((0..100).map(move |index| format!("algo-{page:02}-{index:03}")))
        })
        .collect();
    let server = OrderHistoryServer::spawn(pages)
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_algo_orders("BTC-USDT")
        .await
        .expect_err("open algo orders should fail when the bounded page cap is reached");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("refusing to use partial open algo orders"),
        "page cap failure should refuse partial open algo orders: {error}"
    );
    assert_eq!(requests.len(), OKX_OPEN_ALGO_ORDERS_MAX_PAGES);
}

#[tokio::test]
async fn algo_order_history_pages_with_after_cursor() {
    let first_page = algo_history_body((0..100).map(|index| format!("algo-{index:03}")));
    let second_page = algo_history_body(["algo-older"]);
    let server = OrderHistoryServer::spawn(vec![
        first_page,
        second_page,
        empty_okx_data_body(),
        empty_okx_data_body(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let orders = client
        .algo_order_history("BTC-USDT")
        .await
        .expect("algo order history should page");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(orders.len(), 101);
    assert_eq!(orders[0].algo_id, "algo-000");
    assert_eq!(orders[100].algo_id, "algo-older");
    assert!(
        requests[0].starts_with(
            "GET /api/v5/trade/orders-algo-history?instType=SPOT&instId=BTC-USDT&ordType=trigger&state=effective&limit=100 "
        ),
        "first request used unexpected target: {}",
        requests[0]
    );
    assert!(
        requests[1].starts_with(
            "GET /api/v5/trade/orders-algo-history?instType=SPOT&instId=BTC-USDT&ordType=trigger&state=effective&limit=100&after=algo-099 "
        ),
        "second request used unexpected target: {}",
        requests[1]
    );
}

#[tokio::test]
async fn algo_order_history_fails_closed_when_page_cap_is_reached() {
    let pages = (0..OKX_ALGO_HISTORY_MAX_PAGES)
        .map(|page| {
            algo_history_body((0..100).map(move |index| format!("algo-{page:02}-{index:03}")))
        })
        .collect();
    let server = OrderHistoryServer::spawn(pages)
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .algo_order_history("BTC-USDT")
        .await
        .expect_err("algo order history should fail when the bounded page cap is reached");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("refusing to use partial history"),
        "page cap failure should refuse partial history: {error}"
    );
    assert_eq!(requests.len(), OKX_ALGO_HISTORY_MAX_PAGES);
    assert!(
        requests[0].starts_with(
            "GET /api/v5/trade/orders-algo-history?instType=SPOT&instId=BTC-USDT&ordType=trigger&state=effective&limit=100 "
        ),
        "first capped request used unexpected target: {}",
        requests[0]
    );
}

#[tokio::test]
async fn algo_order_history_rejects_mismatched_instrument_rows() {
    let server = OrderHistoryServer::spawn(vec![algo_history_body_with_instrument(
        "ETH-USDT",
        ["algo-wrong"],
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .algo_order_history("BTC-USDT")
        .await
        .expect_err("algo history should reject mismatched instruments");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("for instrument ETH-USDT while requesting BTC-USDT"),
        "mismatched algo history instrument should be reported: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn algo_order_history_rejects_non_spot_inst_type_rows() {
    let server = OrderHistoryServer::spawn(vec![format!(
        r#"{{"code":"0","msg":"","data":[{}]}}"#,
        algo_json_with_inst_type("SWAP", "BTC-USDT", "algo-wrong")
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .algo_order_history("BTC-USDT")
        .await
        .expect_err("algo history should reject non-spot OKX rows");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX algo order history returned instType SWAP for BTC-USDT; expected SPOT"),
        "non-spot algo history row should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_fetches_matching_spot_spec() {
    let server = OrderHistoryServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let instrument = client
        .instruments("BTC-USDT")
        .await
        .expect("instrument lookup should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        instrument,
        OkxInstrument {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            group_id: "12".to_owned(),
            inst_id_code: Some(123_456),
            state: "live".to_owned(),
            base_ccy: "BTC".to_owned(),
            quote_ccy: "USDT".to_owned(),
            trade_quote_currencies: vec!["USDT".to_owned()],
            tick_size: "0.1".to_owned(),
            lot_size: "0.0001".to_owned(),
            min_size: "0.0001".to_owned(),
            max_limit_size: "999".to_owned(),
            max_limit_amount: "100000".to_owned(),
            max_market_size: "100".to_owned(),
            max_market_amount: "100000".to_owned(),
            max_trigger_size: "999".to_owned(),
            initial_price_limit_pct: "0.05".to_owned(),
            float_price_limit_pct: "0.03".to_owned(),
            maximum_price_limit_pct: "0.15".to_owned(),
        }
    );
    assert!(
        requests[0].starts_with("GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT "),
        "instrument request used unexpected target: {}",
        requests[0]
    );
}

#[tokio::test]
async fn instrument_lookup_uses_simulated_trading_headers_for_demo() {
    let server = OrderHistoryServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await
    .expect("test server should start");
    let client = simulated_test_client(server.addr()).expect("test client should build");

    client
        .instruments("BTC-USDT")
        .await
        .expect("instrument lookup should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 1);
    assert!(
        !request_has_header_name(&requests[0], "OKX-SIMULATED-TRADING"),
        "demo OKX instrument requests should omit removed simulated trading header: {}",
        requests[0]
    );
    assert!(
        request_has_header(&requests[0], OKX_DOCUMENTED_SIMULATED_TRADING, "1"),
        "demo OKX instrument requests should include documented simulated trading header: {}",
        requests[0]
    );
    assert_no_private_auth_headers(&requests[0]);
}

#[tokio::test]
async fn instrument_lookup_omits_simulated_trading_headers_for_production() {
    let server = OrderHistoryServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .instruments("BTC-USDT")
        .await
        .expect("instrument lookup should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 1);
    assert!(
        !request_has_header_name(&requests[0], "OKX-SIMULATED-TRADING"),
        "production OKX instrument requests should omit removed simulated trading header: {}",
        requests[0]
    );
    assert!(
        !request_has_header_name(&requests[0], OKX_DOCUMENTED_SIMULATED_TRADING),
        "production OKX instrument requests should omit documented simulated trading header: {}",
        requests[0]
    );
    assert_no_private_auth_headers(&requests[0]);
}

#[tokio::test]
async fn instrument_lookup_rejects_zero_rows() {
    let server = OrderHistoryServer::spawn(vec![r#"{"code":"0","msg":"","data":[]}"#.to_owned()])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject missing OKX metadata");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX returned 0 instrument specs for BTC-USDT"),
        "missing instrument metadata should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_rejects_multiple_rows() {
    let first =
        InstrumentFixture::spot("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001").json();
    let second =
        InstrumentFixture::spot("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001").json();
    let server = OrderHistoryServer::spawn(vec![format!(
        r#"{{"code":"0","msg":"","data":[{first},{second}]}}"#
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject ambiguous OKX metadata");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX returned 2 instrument specs for BTC-USDT"),
        "ambiguous instrument metadata should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn private_auth_failure_redacts_response_body_and_request_secrets() {
    let sensitive_body = r#"{"code":"50119","msg":"API key doesn't exist for key passphrase secret signature","data":[{"apiKey":"key","secret":"secret"}]}"#;
    let server = OrderHistoryServer::spawn_with_status(vec![(401, sensitive_body.to_owned())])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .balances()
        .await
        .expect_err("auth failure should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = error.to_string();

    assert!(error.contains("HTTP 401"));
    assert!(error.contains("response body omitted"));
    for secret in ["key", "passphrase", "secret", "signature"] {
        assert!(
            !error.contains(secret),
            "auth failure must not leak request or response secrets: {error}"
        );
    }
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn private_auth_envelope_failure_omits_sensitive_message() {
    let sensitive_body = r#"{"code":"50119","msg":"API key doesn't exist for key passphrase secret signature","data":[{"apiKey":"key","secret":"secret"}]}"#;
    let server = OrderHistoryServer::spawn(vec![sensitive_body.to_owned()])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .balances()
        .await
        .expect_err("OKX auth envelope failure should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = error.to_string();

    assert!(error.contains("OKX API error 50119"));
    assert!(error.contains("response message omitted"));
    for secret in ["key", "passphrase", "secret", "signature"] {
        assert!(
            !error.contains(secret),
            "auth envelope failure must not leak response secrets: {error}"
        );
    }
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_rejects_non_live_state() {
    let server = OrderHistoryServer::spawn(vec![instrument_body_with_state(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001", "suspend",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject non-live OKX metadata");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX instrument BTC-USDT state suspend is not live"),
        "non-live instrument metadata should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_rejects_unmatched_symbol_response() {
    let server = OrderHistoryServer::spawn(vec![instrument_body(
        "ETH-USDT", "ETH", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject mismatched OKX metadata");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("returned instrument spec ETH-USDT for requested BTC-USDT"),
        "mismatched instrument metadata should report the returned instrument: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_rejects_non_spot_inst_type() {
    let server = OrderHistoryServer::spawn(vec![
        InstrumentFixture::spot("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001")
            .inst_type("MARGIN")
            .body(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject non-spot OKX metadata");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX returned instType MARGIN for requested SPOT instrument BTC-USDT"),
        "non-spot instrument metadata should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_rejects_base_currency_mismatch() {
    let server = OrderHistoryServer::spawn(vec![instrument_body(
        "BTC-USDT", "ETH", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject baseCcy mismatches");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("returned currencies ETH/USDT that do not compose"),
        "baseCcy mismatch should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_rejects_quote_currency_mismatch() {
    let server = OrderHistoryServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDC", "0.1", "0.0001", "0.0001",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject quoteCcy mismatches");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("returned currencies BTC/USDC that do not compose"),
        "quoteCcy mismatch should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_rejects_unsupported_trade_quote_currency() {
    let server = OrderHistoryServer::spawn(vec![
        InstrumentFixture::spot("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001")
            .trade_quote_currencies(&["USDC"])
            .body(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject unsupported OKX trade quote currency");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("tradeQuoteCcyList [\"USDC\"] does not include USDT"),
        "unsupported trade quote currency should report tradeQuoteCcyList: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_rejects_invalid_precision() {
    let server = OrderHistoryServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0", "0.0001", "0.0001",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject invalid precision");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX instrument tickSz must be positive"),
        "invalid instrument precision should report the OKX field: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn instrument_lookup_rejects_invalid_limit_amount() {
    let body = instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001")
        .replace(r#""maxLmtAmt":"100000""#, r#""maxLmtAmt":"0""#);
    let server = OrderHistoryServer::spawn(vec![body])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("instrument lookup should reject invalid maxLmtAmt");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX instrument maxLmtAmt must be positive"),
        "invalid maxLmtAmt should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

fn test_client(addr: SocketAddr) -> anyhow::Result<OkxRestClient> {
    test_client_with_simulated_trading(addr, /*simulated_trading*/ false)
}

fn unsynced_test_client(addr: SocketAddr) -> anyhow::Result<OkxRestClient> {
    new_test_client_with_simulated_trading(addr, /*simulated_trading*/ false)
}

fn simulated_test_client(addr: SocketAddr) -> anyhow::Result<OkxRestClient> {
    test_client_with_simulated_trading(addr, /*simulated_trading*/ true)
}

fn profile_test_client(path: &str, addr: SocketAddr) -> anyhow::Result<OkxRestClient> {
    let source = if path == "config/live.toml" {
        "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml"
    } else {
        path
    };
    let mut config =
        load_config_path_with_secret_resolver(Path::new(source), test_secret_resolver)?;
    let okx = config
        .okx
        .as_mut()
        .expect("test profile should configure OKX");
    if path == "config/live.toml" {
        okx.trading_service = OkxTradingService::Production;
    }
    okx.base_url = format!("http://{addr}");
    let client = OkxRestClient::from_config(&config)?;
    seed_local_server_time(&client);
    seed_btc_usdt_trade_quote_currency(&client);
    Ok(client)
}

fn test_client_with_simulated_trading(
    addr: SocketAddr,
    simulated_trading: bool,
) -> anyhow::Result<OkxRestClient> {
    let client = new_test_client_with_simulated_trading(addr, simulated_trading)?;
    seed_local_server_time(&client);
    seed_btc_usdt_trade_quote_currency(&client);
    Ok(client)
}

fn new_test_client_with_simulated_trading(
    addr: SocketAddr,
    simulated_trading: bool,
) -> anyhow::Result<OkxRestClient> {
    OkxRestClient::new(
        &OkxConfig {
            api_key: "key".to_owned().into(),
            api_secret: "secret".to_owned().into(),
            api_passphrase: "passphrase".to_owned().into(),
            account_id: "OKX-test".to_owned(),
            api_domain: OkxApiDomain::Global,
            account_jurisdiction: OkxAccountJurisdiction::Singapore,
            trading_service: OkxTradingService::Production,
            base_url: format!("http://{addr}"),
            base_url_ws_public: None,
            base_url_ws_private: None,
            base_url_ws_business: None,
            proxy_url: None,
            request_timeout_ms: 1_000,
            websocket: OkxWebsocketConfig::default(),
        },
        simulated_trading,
    )
}

fn seed_local_server_time(client: &OkxRestClient) {
    *client
        .server_time_clock
        .state
        .lock()
        .expect("server time test clock should lock") = Some(ServerTimeSnapshot {
        offset_millis: 0,
        measured_at: Instant::now(),
    });
}

fn seed_btc_usdt_trade_quote_currency(client: &OkxRestClient) {
    client
        .remember_account_spot_trade_quote_currency("BTC-USDT", "USDT")
        .expect("test account trade quote currency should seed");
}

fn test_secret_resolver(name: &str) -> Option<String> {
    match name {
        "OKX_API_KEY" => Some("test-api-key".to_owned()),
        "OKX_API_SECRET" => Some("test-api-secret".to_owned()),
        "OKX_API_PASSPHRASE" => Some("test-passphrase".to_owned()),
        _ => None,
    }
}

fn empty_okx_data_body() -> String {
    r#"{"code":"0","msg":"","data":[]}"#.to_owned()
}

fn okx_server_time_body(timestamp: &str) -> String {
    format!(r#"{{"code":"0","msg":"","data":[{{"ts":"{timestamp}"}}]}}"#)
}

fn request_has_header(request: &str, name: &str, value: &str) -> bool {
    let needle = format!("{}: {value}", name.to_ascii_lowercase());
    request
        .lines()
        .any(|line| line.to_ascii_lowercase() == needle)
}

fn request_header_value<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().find_map(|line| {
        let (header_name, value) = line.split_once(':')?;
        header_name
            .eq_ignore_ascii_case(name)
            .then_some(value.trim())
    })
}

fn request_has_header_name(request: &str, name: &str) -> bool {
    let needle = format!("{}:", name.to_ascii_lowercase());
    request
        .lines()
        .any(|line| line.to_ascii_lowercase().starts_with(&needle))
}

fn assert_private_auth_headers(request: &str) {
    assert!(request_has_header(request, OKX_API_KEY, "key"));
    assert!(request_has_header_name(request, OKX_API_SIGN));
    assert!(request_has_header_name(request, OKX_API_TIMESTAMP));
    assert!(request_has_header(
        request,
        OKX_API_PASSPHRASE,
        "passphrase"
    ));
}

fn assert_no_private_auth_headers(request: &str) {
    for header in [
        OKX_API_KEY,
        OKX_API_SIGN,
        OKX_API_TIMESTAMP,
        OKX_API_PASSPHRASE,
    ] {
        assert!(
            !request_has_header_name(request, header),
            "public request should not include private REST auth header {header}: {request}"
        );
    }
}

fn raw_request_target(request: &str) -> &str {
    request
        .lines()
        .next()
        .expect("request should include request line")
        .split_whitespace()
        .nth(1)
        .expect("request line should include target")
}

fn request_body(request: &str) -> &str {
    request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("request should include header terminator")
}

fn order_ack_body(order_id: &str, client_order_id: &str) -> String {
    format!(
        r#"{{"code":"0","msg":"","data":[{{"ordId":"{order_id}","clOrdId":"{client_order_id}","sCode":"0","sMsg":""}}]}}"#
    )
}

fn order_history_body(ids: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    order_history_body_with_instrument("BTC-USDT", ids)
}

fn order_history_body_with_instrument(
    inst_id: &str,
    ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> String {
    let orders = ids
        .into_iter()
        .map(|id| order_json(inst_id, id.as_ref()))
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"code":"0","msg":"","data":[{orders}]}}"#)
}

fn order_json(inst_id: &str, id: &str) -> String {
    order_json_with_inst_type("SPOT", inst_id, id)
}

fn order_json_with_inst_type(inst_type: &str, inst_id: &str, id: &str) -> String {
    format!(
        r#"{{"instType":"{inst_type}","instId":"{inst_id}","ordId":"{id}","clOrdId":"client-{id}","state":"filled","avgPx":"100","accFillSz":"0.001","sz":"0.001"}}"#
    )
}

fn order_fills_body(ids: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    order_fills_body_with_instrument("BTC-USDT", ids)
}

fn order_fills_body_with_instrument(
    inst_id: &str,
    ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> String {
    let fills = ids
        .into_iter()
        .map(|id| fill_json(inst_id, id.as_ref()))
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"code":"0","msg":"","data":[{fills}]}}"#)
}

fn fill_json(inst_id: &str, bill_id: &str) -> String {
    fill_json_with_inst_type("SPOT", inst_id, bill_id)
}

fn fill_json_with_inst_type(inst_type: &str, inst_id: &str, bill_id: &str) -> String {
    format!(
        r#"{{"instType":"{inst_type}","instId":"{inst_id}","ordId":"ord-{bill_id}","clOrdId":"client-{bill_id}","billId":"{bill_id}","side":"buy","fillSz":"0.001","fillPx":"100","fillTime":"1700000000000"}}"#
    )
}

fn algo_history_body(ids: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    algo_history_body_with_instrument("BTC-USDT", ids)
}

fn algo_history_body_with_instrument(
    inst_id: &str,
    ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> String {
    let orders = ids
        .into_iter()
        .map(|id| algo_json(inst_id, id.as_ref()))
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"code":"0","msg":"","data":[{orders}]}}"#)
}

fn algo_json(inst_id: &str, id: &str) -> String {
    algo_json_with_inst_type("SPOT", inst_id, id)
}

fn algo_json_with_inst_type(inst_type: &str, inst_id: &str, id: &str) -> String {
    format!(
        r#"{{"instType":"{inst_type}","instId":"{inst_id}","algoId":"{id}","algoClOrdId":"client-{id}","side":"sell","ordType":"trigger","orderPx":"-1","state":"effective","triggerPx":"100","sz":"0.001"}}"#
    )
}

fn instrument_body(
    inst_id: &str,
    base_ccy: &str,
    quote_ccy: &str,
    tick_size: &str,
    lot_size: &str,
    min_size: &str,
) -> String {
    instrument_body_with_state(
        inst_id, base_ccy, quote_ccy, tick_size, lot_size, min_size, "live",
    )
}

fn instrument_body_with_state(
    inst_id: &str,
    base_ccy: &str,
    quote_ccy: &str,
    tick_size: &str,
    lot_size: &str,
    min_size: &str,
    state: &str,
) -> String {
    InstrumentFixture::spot(inst_id, base_ccy, quote_ccy, tick_size, lot_size, min_size)
        .state(state)
        .body()
}

#[derive(Clone, Copy)]
struct InstrumentFixture<'a> {
    inst_type: &'a str,
    inst_id: &'a str,
    base_ccy: &'a str,
    quote_ccy: &'a str,
    tick_size: &'a str,
    lot_size: &'a str,
    min_size: &'a str,
    state: &'a str,
    trade_quote_currencies: Option<&'a [&'a str]>,
}

impl<'a> InstrumentFixture<'a> {
    fn spot(
        inst_id: &'a str,
        base_ccy: &'a str,
        quote_ccy: &'a str,
        tick_size: &'a str,
        lot_size: &'a str,
        min_size: &'a str,
    ) -> Self {
        Self {
            inst_type: "SPOT",
            inst_id,
            base_ccy,
            quote_ccy,
            tick_size,
            lot_size,
            min_size,
            state: "live",
            trade_quote_currencies: None,
        }
    }

    const fn inst_type(mut self, inst_type: &'a str) -> Self {
        self.inst_type = inst_type;
        self
    }

    const fn state(mut self, state: &'a str) -> Self {
        self.state = state;
        self
    }

    const fn trade_quote_currencies(mut self, trade_quote_currencies: &'a [&'a str]) -> Self {
        self.trade_quote_currencies = Some(trade_quote_currencies);
        self
    }

    fn body(self) -> String {
        format!(r#"{{"code":"0","msg":"","data":[{}]}}"#, self.json())
    }

    fn json(self) -> String {
        let trade_quote_currencies = match self.trade_quote_currencies {
            Some(currencies) => currencies
                .iter()
                .map(|currency| format!(r#""{currency}""#))
                .collect::<Vec<_>>()
                .join(","),
            None => {
                let quote_ccy = self.quote_ccy;
                format!(r#""{quote_ccy}""#)
            }
        };
        let inst_type = self.inst_type;
        let inst_id = self.inst_id;
        let state = self.state;
        let base_ccy = self.base_ccy;
        let quote_ccy = self.quote_ccy;
        let tick_size = self.tick_size;
        let lot_size = self.lot_size;
        let min_size = self.min_size;
        format!(
            r#"{{"instType":"{inst_type}","instId":"{inst_id}","instIdCode":"123456","groupId":"12","state":"{state}","baseCcy":"{base_ccy}","quoteCcy":"{quote_ccy}","tradeQuoteCcyList":[{trade_quote_currencies}],"tickSz":"{tick_size}","lotSz":"{lot_size}","minSz":"{min_size}","maxLmtSz":"999","maxLmtAmt":"100000","maxMktSz":"100","maxMktAmt":"100000","maxTriggerSz":"999","initPxLmtPct":"0.05","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}}"#,
        )
    }
}
