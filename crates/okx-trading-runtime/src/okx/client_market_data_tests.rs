use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::{
    config::types::{
        OkxAccountJurisdiction, OkxApiDomain, OkxConfig, OkxTradingService, OkxWebsocketConfig,
    },
    okx::{
        types::{MarketBar, OkxInstrument, OkxTicker, OrderKind, OrderSide},
        websocket::{
            OkxInstrumentUpdateHint, OkxMarketCandleHint, OkxMarketTickerHint,
            OkxWebsocketInstrumentUpdate,
        },
    },
    test_support::{CapturedLogs, HttpTestServer as TestServer},
};

use super::{
    OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS, OkxCancelAllAfterTimeout, OkxOrderAmend, OkxRestClient,
    current_unix_millis, ensure_fresh_rest_ticker_timestamp,
    ensure_instrument_hint_matches_rest_snapshot,
};

#[tokio::test]
async fn ticker_request_rejects_unmatched_symbol_response() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        ticker_json("ETH-USDT", "100.1", "100.2", "100.15")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .ticker("BTC-USDT")
        .await
        .expect_err("ticker should reject mismatched OKX metadata");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX returned ticker ETH-USDT for requested BTC-USDT"),
        "mismatched ticker metadata should report the returned instrument: {error}"
    );
    assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
}

#[tokio::test]
async fn ticker_request_rejects_stale_rest_generation_timestamp() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        ticker_json_with_timestamp("BTC-USDT", "100.1", "100.2", "100.15", "1")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .ticker("BTC-USDT")
        .await
        .expect_err("stale REST ticker evidence should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX REST ticker timestamp is stale"),
        "stale REST ticker evidence should report its age: {error}"
    );
    assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
}

#[tokio::test]
async fn ticker_request_requires_rest_generation_timestamp() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        ticker_json_without_timestamp("BTC-USDT", "100.1", "100.2", "100.15")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .ticker("BTC-USDT")
        .await
        .expect_err("REST ticker evidence without ts should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        format!("{error:#}").contains("missing field `ts`"),
        "missing REST ticker generation time should report the absent field: {error:#}"
    );
    assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
}

#[test]
fn rest_ticker_generation_timestamp_validation_is_strict_and_bounded() {
    let server_now_ms = 10_000;
    let max_staleness = Duration::from_millis(3_000);

    ensure_fresh_rest_ticker_timestamp("7000", server_now_ms, max_staleness)
        .expect("timestamp at the configured maximum age should pass");

    for (timestamp, expected) in [
        ("", "must not be empty"),
        ("not-millis", "must be Unix milliseconds"),
        ("0", "must be positive"),
        ("-1", "must be positive"),
        ("10001", "is in the future"),
        ("6999", "is stale"),
    ] {
        let error = ensure_fresh_rest_ticker_timestamp(timestamp, server_now_ms, max_staleness)
            .expect_err("invalid REST ticker timestamp should fail closed");
        assert!(
            error.to_string().contains(expected),
            "timestamp {timestamp:?} should report {expected:?}: {error}"
        );
    }
}

#[tokio::test]
async fn ticker_uses_fresh_public_websocket_hint_without_rest_request() {
    let server = TestServer::spawn(Vec::new())
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    let websocket_ticker = OkxTicker {
        inst_type: "SPOT".to_owned(),
        inst_id: "BTC-USDT".to_owned(),
        bid_px: "100.1".to_owned(),
        ask_px: "100.2".to_owned(),
        last: "100.15".to_owned(),
    };
    client
        .market_data_cache()
        .update_ticker(OkxMarketTickerHint {
            ticker: websocket_ticker.clone(),
            source_ts_ms: Some(1_710_000_000_123),
            received_at: Instant::now(),
        })
        .expect("fresh WebSocket hint should validate");

    let ticker = client
        .ticker("BTC-USDT")
        .await
        .expect("fresh WebSocket hint should satisfy ticker request");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(ticker, websocket_ticker);
    assert_eq!(requests, Vec::<String>::new());
}

#[tokio::test]
async fn ticker_fails_closed_when_fresh_instrument_hint_is_not_live() {
    let server = TestServer::spawn(Vec::new())
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    client
        .market_data_cache()
        .update_ticker(OkxMarketTickerHint {
            ticker: OkxTicker {
                inst_type: "SPOT".to_owned(),
                inst_id: "BTC-USDT".to_owned(),
                bid_px: "100.1".to_owned(),
                ask_px: "100.2".to_owned(),
                last: "100.15".to_owned(),
            },
            source_ts_ms: Some(1_710_000_000_123),
            received_at: Instant::now(),
        })
        .expect("fresh WebSocket ticker hint should validate");
    client
        .market_data_cache()
        .update_instrument(OkxInstrumentUpdateHint {
            instrument: websocket_instrument_update("BTC-USDT", "suspend"),
            source_ts_ms: Some(1_710_000_000_124),
            received_at: Instant::now(),
        })
        .expect("fresh WebSocket instrument hint should validate");

    let error = client
        .ticker("BTC-USDT")
        .await
        .expect_err("fresh non-live instrument hint should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX WebSocket instrument BTC-USDT state suspend is not live"),
        "fresh non-live instrument hint should report the unsafe state: {error}"
    );
    assert_eq!(requests, Vec::<String>::new());
}

#[test]
fn instrument_hint_snapshot_comparison_rejects_parameter_changes() {
    let snapshot = rest_instrument("BTC-USDT", "0.1", "0.00000001", "0.00001");
    let cases = [
        (InstrumentHintField::TickSize, "0.2"),
        (InstrumentHintField::LotSize, "0.00000002"),
        (InstrumentHintField::MinSize, "0.00002"),
        (InstrumentHintField::MaxLimitSize, "998"),
        (InstrumentHintField::MaxLimitAmount, "99999"),
        (InstrumentHintField::MaxMarketSize, "99"),
        (InstrumentHintField::MaxMarketAmount, "99999"),
        (InstrumentHintField::MaxTriggerSize, "998"),
    ];

    for (field, changed_value) in cases {
        let mut hint = websocket_instrument_update("BTC-USDT", "live");
        field.set(&mut hint, changed_value);

        let error = ensure_instrument_hint_matches_rest_snapshot(&hint, &snapshot)
            .expect_err("changed WebSocket instrument parameters should fail closed");

        assert_instrument_hint_change_error(&error, field.okx_name());
    }
}

#[test]
fn instrument_hint_snapshot_comparison_rejects_fee_group_changes() {
    let snapshot = rest_instrument("BTC-USDT", "0.1", "0.00000001", "0.00001");
    let mut hint = websocket_instrument_update("BTC-USDT", "live");
    hint.group_id = "13".to_owned();

    let error = ensure_instrument_hint_matches_rest_snapshot(&hint, &snapshot)
        .expect_err("changed WebSocket fee group should fail closed");

    assert!(
        error.to_string().contains("groupId"),
        "fee-group change should identify groupId: {error}"
    );
}

#[test]
fn instrument_hint_snapshot_comparison_allows_missing_optional_limits() {
    let snapshot = rest_instrument("BTC-USDT", "0.1", "0.00000001", "0.00001");
    let mut hint = websocket_instrument_update("BTC-USDT", "live");
    hint.max_limit_size = String::new();
    hint.max_limit_amount = String::new();
    hint.max_market_size = String::new();
    hint.max_market_amount = String::new();
    hint.max_trigger_size = String::new();

    ensure_instrument_hint_matches_rest_snapshot(&hint, &snapshot)
        .expect("missing optional WebSocket instrument limits should not imply a parameter change");
}

#[tokio::test]
async fn ticker_fails_closed_when_fresh_instrument_hint_changes_rest_snapshot() {
    let server = TestServer::spawn(vec![instrument_body(
        "BTC-USDT",
        "BTC",
        "USDT",
        "0.1",
        "0.00000001",
        "0.00001",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    client
        .instruments("BTC-USDT")
        .await
        .expect("REST instrument snapshot should load");
    client
        .market_data_cache()
        .update_ticker(OkxMarketTickerHint {
            ticker: OkxTicker {
                inst_type: "SPOT".to_owned(),
                inst_id: "BTC-USDT".to_owned(),
                bid_px: "100.1".to_owned(),
                ask_px: "100.2".to_owned(),
                last: "100.15".to_owned(),
            },
            source_ts_ms: Some(1_710_000_000_123),
            received_at: Instant::now(),
        })
        .expect("fresh WebSocket ticker hint should validate");
    let mut instrument_hint = websocket_instrument_update("BTC-USDT", "live");
    instrument_hint.tick_size = "0.2".to_owned();
    client
        .market_data_cache()
        .update_instrument(OkxInstrumentUpdateHint {
            instrument: instrument_hint,
            source_ts_ms: Some(1_710_000_000_124),
            received_at: Instant::now(),
        })
        .expect("fresh WebSocket instrument hint should validate");

    let error = client
        .ticker("BTC-USDT")
        .await
        .expect_err("changed WebSocket instrument hint should block ticker hints");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_instrument_hint_change_error(&error, "tickSz");
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn order_intent_fails_closed_when_fresh_instrument_hint_changes_rest_snapshot() {
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.00000001", "0.00001"),
        okx_data_body(r#"[{"ts":"4102444810123"}]"#),
        okx_data_body(r#"[{"ordId":"1","clOrdId":"cleanup-1","sCode":"0","sMsg":""}]"#),
        okx_data_body(
            r#"[{"algoId":"algo-cleanup-1","algoClOrdId":"stop-1","sCode":"0","sMsg":""}]"#,
        ),
        okx_data_body(
            r#"[{"triggerTime":"1710000010000","tag":"okxrusttrading","ts":"1710000000000"}]"#,
        ),
    ])
    .await
    .expect("test server should start");
    let client = unsynced_test_client(server.addr()).expect("test client should build");
    let logs = CapturedLogs::default();
    let dispatch = logs.dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);
    client
        .instruments("BTC-USDT")
        .await
        .expect("REST instrument snapshot should load");
    let mut instrument_hint = websocket_instrument_update("BTC-USDT", "live");
    instrument_hint.lot_size = "0.00000002".to_owned();
    client
        .market_data_cache()
        .update_instrument(OkxInstrumentUpdateHint {
            instrument: instrument_hint,
            source_ts_ms: Some(1_710_000_000_124),
            received_at: Instant::now(),
        })
        .expect("fresh WebSocket instrument hint should validate");

    assert_instrument_hint_change_error(
        &client
            .place_order(
                "BTC-USDT",
                OrderSide::Buy,
                OrderKind::Limit,
                "0.001",
                Some("100"),
                "entry-1",
            )
            .await
            .expect_err("regular place order should fail before REST submit"),
        "lotSz",
    );
    assert_instrument_hint_change_error(
        &client
            .amend_order(OkxOrderAmend {
                inst_id: "BTC-USDT",
                side: OrderSide::Sell,
                client_order_id: "take-profit-1",
                new_size: Some("0.001"),
                new_price: None,
            })
            .await
            .expect_err("regular amend should fail before REST submit"),
        "lotSz",
    );
    assert_instrument_hint_change_error(
        &client
            .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "95", "stop-1")
            .await
            .expect_err("trigger placement should fail before REST submit"),
        "lotSz",
    );
    assert_instrument_hint_change_error(
        &client
            .prepare_websocket_place_order(
                "BTC-USDT",
                OrderSide::Buy,
                OrderKind::PostOnly,
                Some("100"),
            )
            .await
            .expect_err("WebSocket place preparation should fail before server time request"),
        "lotSz",
    );
    assert_instrument_hint_change_error(
        &client
            .prepare_websocket_amend_order("BTC-USDT", OrderSide::Sell, Some("100"))
            .await
            .expect_err("WebSocket amend preparation should fail before server time request"),
        "lotSz",
    );
    client
        .market_data_cache()
        .update_instrument(OkxInstrumentUpdateHint {
            instrument: websocket_instrument_update("BTC-USDT", "live"),
            source_ts_ms: Some(1_710_000_000_125),
            received_at: Instant::now(),
        })
        .expect("later matching WebSocket instrument hint should validate structurally");
    let latched_error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::Limit,
            "0.001",
            Some("100"),
            "entry-2",
        )
        .await
        .expect_err("a later matching hint must not clear a detected metadata contradiction");
    assert!(
        latched_error
            .to_string()
            .contains("instrument metadata safety latch"),
        "detected metadata contradiction should remain latched for the process lifetime: {latched_error}"
    );
    assert_instrument_hint_change_error(&latched_error, "lotSz");
    client
        .cancel_order("BTC-USDT", "cleanup-1")
        .await
        .expect("metadata latch must preserve risk-reducing order cancellation");
    client
        .cancel_algo_order("BTC-USDT", "algo-cleanup-1")
        .await
        .expect("metadata latch must preserve risk-reducing algo cancellation");
    client
        .cancel_all_after(
            OkxCancelAllAfterTimeout::new(OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS)
                .expect("test CAA timeout should validate"),
        )
        .await
        .expect("metadata latch must preserve Cancel-All-After protection");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(&requests[1], "GET /api/v5/public/time ");
    assert_request_target(&requests[2], "POST /api/v5/trade/cancel-order ");
    assert_request_target(&requests[3], "POST /api/v5/trade/cancel-algos ");
    assert_request_target(&requests[4], "POST /api/v5/trade/cancel-all-after ");
    assert_eq!(requests.len(), 5);
    assert_eq!(
        logs.contents()
            .matches("instrument_metadata_safety_latched")
            .count(),
        1,
        "the first contradiction should emit one bounded safety event"
    );
}

#[tokio::test]
async fn ticker_ignores_stale_non_live_instrument_hint() {
    let server = TestServer::spawn(Vec::new())
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    let websocket_ticker = OkxTicker {
        inst_type: "SPOT".to_owned(),
        inst_id: "BTC-USDT".to_owned(),
        bid_px: "100.1".to_owned(),
        ask_px: "100.2".to_owned(),
        last: "100.15".to_owned(),
    };
    client
        .market_data_cache()
        .update_ticker(OkxMarketTickerHint {
            ticker: websocket_ticker.clone(),
            source_ts_ms: Some(1_710_000_000_123),
            received_at: Instant::now(),
        })
        .expect("fresh WebSocket ticker hint should validate");
    client
        .market_data_cache()
        .update_instrument(OkxInstrumentUpdateHint {
            instrument: websocket_instrument_update("BTC-USDT", "suspend"),
            source_ts_ms: Some(1_710_000_000_124),
            received_at: Instant::now() - Duration::from_secs(10),
        })
        .expect("stale WebSocket instrument hint should validate");

    let ticker = client
        .ticker("BTC-USDT")
        .await
        .expect("stale non-live instrument hint should not block ticker");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(ticker, websocket_ticker);
    assert_eq!(requests, Vec::<String>::new());
}

#[tokio::test]
async fn ticker_falls_back_to_rest_when_public_websocket_hint_is_stale() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        ticker_json("BTC-USDT", "101.1", "101.2", "101.15")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    client
        .market_data_cache()
        .update_ticker(OkxMarketTickerHint {
            ticker: OkxTicker {
                inst_type: "SPOT".to_owned(),
                inst_id: "BTC-USDT".to_owned(),
                bid_px: "100.1".to_owned(),
                ask_px: "100.2".to_owned(),
                last: "100.15".to_owned(),
            },
            source_ts_ms: Some(1_710_000_000_123),
            received_at: Instant::now() - Duration::from_secs(10),
        })
        .expect("stale WebSocket hint should validate");
    let logs = CapturedLogs::default();
    let dispatch = logs.dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let ticker = client
        .ticker("BTC-USDT")
        .await
        .expect("REST ticker fallback should satisfy stale hint request");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        ticker,
        OkxTicker {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            bid_px: "101.1".to_owned(),
            ask_px: "101.2".to_owned(),
            last: "101.15".to_owned(),
        }
    );
    assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
    assert!(
        logs.contents()
            .contains("rest_fallback_ws_hint_unavailable")
    );
}

#[tokio::test]
async fn ticker_falls_back_to_rest_when_public_websocket_hint_is_untimestamped() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        ticker_json("BTC-USDT", "101.1", "101.2", "101.15")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    client
        .market_data_cache()
        .update_ticker(OkxMarketTickerHint {
            ticker: OkxTicker {
                inst_type: "SPOT".to_owned(),
                inst_id: "BTC-USDT".to_owned(),
                bid_px: "100.1".to_owned(),
                ask_px: "100.2".to_owned(),
                last: "100.15".to_owned(),
            },
            source_ts_ms: None,
            received_at: Instant::now(),
        })
        .expect("untimestamped WebSocket hint should be ignored without failing");

    let ticker = client
        .ticker("BTC-USDT")
        .await
        .expect("REST ticker fallback should satisfy untimestamped hint request");

    assert_eq!(
        ticker,
        OkxTicker {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            bid_px: "101.1".to_owned(),
            ask_px: "101.2".to_owned(),
            last: "101.15".to_owned(),
        }
    );

    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
}

#[tokio::test]
async fn ticker_falls_back_to_rest_when_public_websocket_hint_is_missing() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        ticker_json("BTC-USDT", "101.1", "101.2", "101.15")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let ticker = client
        .ticker("BTC-USDT")
        .await
        .expect("REST ticker fallback should satisfy missing hint request");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        ticker,
        OkxTicker {
            inst_type: "SPOT".to_owned(),
            inst_id: "BTC-USDT".to_owned(),
            bid_px: "101.1".to_owned(),
            ask_px: "101.2".to_owned(),
            last: "101.15".to_owned(),
        }
    );
    assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
}

#[tokio::test]
async fn okx_rate_limit_errors_are_classified() {
    let cases = [
        (
            r#"{"code":"50011","msg":"Rate limit reached. Please throttle requests.","data":{"unexpected":true}}"#,
            "OKX API rate limit 50011: Rate limit reached",
        ),
        (
            r#"{"code":"50040","msg":"Too frequent operations.","data":{"unexpected":true}}"#,
            "OKX API rate limit 50040: Too frequent operations.",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body.to_owned()])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .ticker("BTC-USDT")
            .await
            .expect_err("rate-limited OKX response should fail closed");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "rate-limit error should be classified distinctly: {error}"
        );
        assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
    }
}

#[tokio::test]
async fn okx_rate_limit_errors_are_classified_before_payload_parsing() {
    let server = TestServer::spawn(vec![
        r#"{"code":"50061","msg":"Requests are too frequent.","data":{"unexpected":true}}"#
            .to_owned(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .ticker("BTC-USDT")
        .await
        .expect_err("rate-limited OKX response should fail closed before ticker parsing");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX API rate limit 50061: Requests are too frequent"),
        "rate-limit error should be classified before endpoint payload parsing: {error}"
    );
    assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
}

#[tokio::test]
async fn candles_request_rejects_short_okx_payload() {
    let server = TestServer::spawn(vec![okx_data_body(r#"[["1000","100"]]"#)])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .candles("BTC-USDT", "5m", /*limit*/ 1)
        .await
        .expect_err("candles should reject malformed OKX payloads");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        format!("{error:#}").contains("OKX candle payload must contain at least 9 fields"),
        "short candle payload should report the malformed OKX candle: {error}"
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=5m&limit=1 ",
    );
}

#[tokio::test]
async fn candles_request_rejects_invalid_numeric_fields() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        candle_json("not-a-timestamp", "100")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .candles("BTC-USDT", "5m", /*limit*/ 1)
        .await
        .expect_err("candles should reject invalid OKX numeric fields");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        format!("{error:#}").contains("invalid OKX candle ts field"),
        "invalid candle timestamp should report the malformed OKX field: {error}"
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=5m&limit=1 ",
    );
}

#[tokio::test]
async fn candles_request_accepts_confirmed_and_unconfirmed_candles() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{},{}]",
        candle_json_with_confirm("1000", "100", "1"),
        candle_json_with_confirm("2000", "101", "0")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let candles = client
        .candles("BTC-USDT", "5m", /*limit*/ 2)
        .await
        .expect("valid confirmed and unconfirmed candles should parse");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        candles,
        vec![
            market_bar_with_confirm(1_000, 100.0, /*confirm*/ true),
            market_bar_with_confirm(2_000, 101.0, /*confirm*/ false),
        ]
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=5m&limit=2 ",
    );
}

#[tokio::test]
async fn candles_request_fails_when_any_returned_row_is_invalid() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        r#"[{},["2000","99","100","95","101","1","1","1","1"]]"#,
        candle_json("1000", "100")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .candles("BTC-USDT", "5m", /*limit*/ 2)
        .await
        .expect_err("candles should reject invalid OKX candle rows");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        format!("{error:#}").contains("OKX candle high must be at least close"),
        "invalid candle row should fail before REST candles are returned: {error}"
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=5m&limit=2 ",
    );
}

#[tokio::test]
async fn live_candles_uses_fresh_websocket_hints_without_rest_request() {
    let server = TestServer::spawn(Vec::new())
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    let current_bar = market_bar_with_confirm(3_000, 102.0, /*confirm*/ false);
    for candle in [
        market_candle_hint(1_000, market_bar(1_000, 100.0), Instant::now()),
        market_candle_hint(2_000, market_bar(2_000, 101.0), Instant::now()),
        market_candle_hint(3_000, current_bar.clone(), Instant::now()),
    ] {
        client
            .market_data_cache()
            .update_candle(candle)
            .expect("fresh candle hint should validate");
    }

    let candles = client
        .live_candles("BTC-USDT", "1m", /*limit*/ 3)
        .await
        .expect("fresh WebSocket candle hints should satisfy runtime live candle refresh");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        candles,
        vec![
            market_bar(1_000, 100.0),
            market_bar(2_000, 101.0),
            current_bar
        ]
    );
    assert_eq!(requests, Vec::<String>::new());
}

#[tokio::test]
async fn live_candles_falls_back_when_rest_baseline_candle_is_invalid() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        r#"[{},["2000","100","105","0","101","1","1","1","1"]]"#,
        candle_json("1000", "100")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    client
        .market_data_cache()
        .update_candle(market_candle_hint(
            3_000,
            market_bar(3_000, 102.0),
            Instant::now(),
        ))
        .expect("fresh candle hint should validate");

    let error = client
        .live_candles("BTC-USDT", "1m", /*limit*/ 3)
        .await
        .expect_err("invalid REST fallback candles should still fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        format!("{error:#}").contains("OKX candle low must be finite and positive"),
        "invalid REST fallback should fail closed: {error}"
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=3 ",
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn live_candles_falls_back_to_rest_when_websocket_hints_are_insufficient() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{},{}]",
        candle_json("1000", "100"),
        candle_json("2000", "101")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    client
        .market_data_cache()
        .update_candle(market_candle_hint(
            3_000,
            market_bar(3_000, 102.0),
            Instant::now(),
        ))
        .expect("fresh candle hint should validate");

    let candles = client
        .live_candles("BTC-USDT", "1m", /*limit*/ 3)
        .await
        .expect("insufficient WebSocket candle hints should fall back to REST");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        candles,
        vec![
            market_bar(1_000, 100.0),
            market_bar(2_000, 101.0),
            market_bar(3_000, 102.0)
        ]
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=3 ",
    );
}

#[tokio::test]
async fn one_minute_candles_request_keeps_warmup_rest_owned() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        candle_json("1000", "100")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    client
        .market_data_cache()
        .update_candle(crate::okx::websocket::OkxMarketCandleHint {
            inst_id: "BTC-USDT".to_owned(),
            channel: "candle1m".to_owned(),
            bar: market_bar(2_000, 101.0),
            source_ts_ms: Some(2_000),
            received_at: Instant::now(),
        })
        .expect("fresh candle hint should validate");

    let candles = client
        .candles("BTC-USDT", "1m", /*limit*/ 1)
        .await
        .expect("1m candle warmup request should remain REST-only");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(candles, vec![market_bar(1_000, 100.0)]);
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=1 ",
    );
}

#[tokio::test]
async fn live_candles_ignore_stale_websocket_hints() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        candle_json("1000", "100")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    client
        .market_data_cache()
        .update_candle(crate::okx::websocket::OkxMarketCandleHint {
            inst_id: "BTC-USDT".to_owned(),
            channel: "candle1m".to_owned(),
            bar: market_bar(2_000, 101.0),
            source_ts_ms: Some(2_000),
            received_at: Instant::now() - Duration::from_secs(10),
        })
        .expect("stale candle hint should validate");

    let candles = client
        .live_candles("BTC-USDT", "1m", /*limit*/ 2)
        .await
        .expect("stale WebSocket candle hint should not drive live refresh");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(candles, vec![market_bar(1_000, 100.0)]);
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=2 ",
    );
}

#[tokio::test]
async fn live_candles_uses_rest_for_non_websocket_bars_even_when_candle_hints_are_fresh() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{},{}]",
        candle_json("1000", "96"),
        candle_json("2000", "97")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    for candle in [
        market_candle_hint(1_000, market_bar(1_000, 100.0), Instant::now()),
        market_candle_hint(2_000, market_bar(2_000, 101.0), Instant::now()),
        market_candle_hint(3_000, market_bar(3_000, 102.0), Instant::now()),
    ] {
        client
            .market_data_cache()
            .update_candle(candle)
            .expect("fresh candle hint should validate");
    }

    let candles = client
        .live_candles("BTC-USDT", "15m", /*limit*/ 2)
        .await
        .expect("non-WebSocket live candles should use REST");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        candles,
        vec![market_bar(1_000, 96.0), market_bar(2_000, 97.0)]
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=15m&limit=2 ",
    );
}

#[tokio::test]
async fn place_order_fails_closed_when_fresh_instrument_hint_is_not_live() {
    let server = TestServer::spawn(Vec::new())
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    client
        .market_data_cache()
        .update_instrument(OkxInstrumentUpdateHint {
            instrument: websocket_instrument_update("BTC-USDT", "suspend"),
            source_ts_ms: Some(1_710_000_000_124),
            received_at: Instant::now(),
        })
        .expect("fresh WebSocket instrument hint should validate");

    let error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::Limit,
            "0.001",
            Some("100"),
            "entry-1",
        )
        .await
        .expect_err("fresh non-live instrument hint should block new order placement");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX WebSocket instrument BTC-USDT state suspend is not live"),
        "new order placement should report unsafe instrument state: {error}"
    );
    assert_eq!(requests, Vec::<String>::new());
}

fn test_client(addr: SocketAddr) -> anyhow::Result<OkxRestClient> {
    let client = unsynced_test_client(addr)?;
    let now_ms = current_unix_millis();
    client
        .server_time_clock
        .record(now_ms, now_ms)
        .expect("test server time should seed");
    Ok(client)
}

fn unsynced_test_client(addr: SocketAddr) -> anyhow::Result<OkxRestClient> {
    let client = OkxRestClient::new(
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
        /*simulated_trading*/ false,
    )?;
    client
        .remember_account_spot_trade_quote_currency("BTC-USDT", "USDT")
        .expect("test account trade quote currency should seed");
    Ok(client)
}

fn okx_data_body(data: &str) -> String {
    format!(r#"{{"code":"0","msg":"","data":{data}}}"#)
}

fn assert_request_target(request: &str, expected_prefix: &str) {
    assert!(
        request.starts_with(expected_prefix),
        "request used unexpected target; expected prefix {expected_prefix:?}: {request}"
    );
}

fn candle_json(ts_ms: &str, close: &str) -> String {
    candle_json_with_confirm(ts_ms, close, "1")
}

fn candle_json_with_confirm(ts_ms: &str, close: &str, confirm: &str) -> String {
    format!(r#"["{ts_ms}","100","105","95","{close}","1","1","1","{confirm}"]"#)
}

fn market_bar(ts_ms: i64, close: f64) -> MarketBar {
    market_bar_with_confirm(ts_ms, close, /*confirm*/ true)
}

fn market_bar_with_confirm(ts_ms: i64, close: f64, confirm: bool) -> MarketBar {
    MarketBar {
        ts_ms,
        open: 100.0,
        high: 105.0,
        low: 95.0,
        close,
        confirm,
    }
}

fn market_candle_hint(ts_ms: i64, bar: MarketBar, received_at: Instant) -> OkxMarketCandleHint {
    OkxMarketCandleHint {
        inst_id: "BTC-USDT".to_owned(),
        channel: "candle1m".to_owned(),
        bar,
        source_ts_ms: Some(ts_ms),
        received_at,
    }
}

fn ticker_json(inst_id: &str, bid_px: &str, ask_px: &str, last: &str) -> String {
    ticker_json_with_timestamp(
        inst_id,
        bid_px,
        ask_px,
        last,
        &current_unix_millis().to_string(),
    )
}

fn ticker_json_without_timestamp(inst_id: &str, bid_px: &str, ask_px: &str, last: &str) -> String {
    format!(
        r#"{{"instType":"SPOT","instId":"{inst_id}","bidPx":"{bid_px}","askPx":"{ask_px}","last":"{last}"}}"#
    )
}

fn ticker_json_with_timestamp(
    inst_id: &str,
    bid_px: &str,
    ask_px: &str,
    last: &str,
    timestamp: &str,
) -> String {
    format!(
        r#"{{"instType":"SPOT","instId":"{inst_id}","bidPx":"{bid_px}","askPx":"{ask_px}","last":"{last}","ts":"{timestamp}"}}"#
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
    okx_data_body(&format!(
        r#"[{}]"#,
        instrument_json(inst_id, base_ccy, quote_ccy, tick_size, lot_size, min_size)
    ))
}

fn instrument_json(
    inst_id: &str,
    base_ccy: &str,
    quote_ccy: &str,
    tick_size: &str,
    lot_size: &str,
    min_size: &str,
) -> String {
    format!(
        r#"{{"instType":"SPOT","instId":"{inst_id}","instIdCode":"123456","groupId":"12","state":"live","baseCcy":"{base_ccy}","quoteCcy":"{quote_ccy}","tradeQuoteCcyList":["{quote_ccy}"],"tickSz":"{tick_size}","lotSz":"{lot_size}","minSz":"{min_size}","maxLmtSz":"999","maxLmtAmt":"100000","maxMktSz":"100","maxMktAmt":"100000","maxTriggerSz":"999","initPxLmtPct":"0.05","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}}"#
    )
}

fn rest_instrument(
    inst_id: &str,
    tick_size: &str,
    lot_size: &str,
    min_size: &str,
) -> OkxInstrument {
    OkxInstrument {
        inst_type: "SPOT".to_owned(),
        inst_id: inst_id.to_owned(),
        group_id: "12".to_owned(),
        inst_id_code: Some(123456),
        state: "live".to_owned(),
        base_ccy: "BTC".to_owned(),
        quote_ccy: "USDT".to_owned(),
        trade_quote_currencies: vec!["USDT".to_owned()],
        tick_size: tick_size.to_owned(),
        lot_size: lot_size.to_owned(),
        min_size: min_size.to_owned(),
        max_limit_size: "999".to_owned(),
        max_limit_amount: "100000".to_owned(),
        max_market_size: "100".to_owned(),
        max_market_amount: "100000".to_owned(),
        max_trigger_size: "999".to_owned(),
        initial_price_limit_pct: "0.05".to_owned(),
        float_price_limit_pct: "0.03".to_owned(),
        maximum_price_limit_pct: "0.15".to_owned(),
    }
}

#[derive(Clone, Copy)]
enum InstrumentHintField {
    TickSize,
    LotSize,
    MinSize,
    MaxLimitSize,
    MaxLimitAmount,
    MaxMarketSize,
    MaxMarketAmount,
    MaxTriggerSize,
}

impl InstrumentHintField {
    fn okx_name(self) -> &'static str {
        match self {
            Self::TickSize => "tickSz",
            Self::LotSize => "lotSz",
            Self::MinSize => "minSz",
            Self::MaxLimitSize => "maxLmtSz",
            Self::MaxLimitAmount => "maxLmtAmt",
            Self::MaxMarketSize => "maxMktSz",
            Self::MaxMarketAmount => "maxMktAmt",
            Self::MaxTriggerSize => "maxTriggerSz",
        }
    }

    fn set(self, hint: &mut OkxWebsocketInstrumentUpdate, value: &str) {
        match self {
            Self::TickSize => hint.tick_size = value.to_owned(),
            Self::LotSize => hint.lot_size = value.to_owned(),
            Self::MinSize => hint.min_size = value.to_owned(),
            Self::MaxLimitSize => hint.max_limit_size = value.to_owned(),
            Self::MaxLimitAmount => hint.max_limit_amount = value.to_owned(),
            Self::MaxMarketSize => hint.max_market_size = value.to_owned(),
            Self::MaxMarketAmount => hint.max_market_amount = value.to_owned(),
            Self::MaxTriggerSize => hint.max_trigger_size = value.to_owned(),
        }
    }
}

fn assert_instrument_hint_change_error(error: &anyhow::Error, field: &str) {
    assert!(
        error.to_string().contains(&format!("changed {field}")),
        "instrument hint change should report {field}: {error}"
    );
    assert!(
        error
            .to_string()
            .contains("refusing to use stale instrument metadata"),
        "instrument hint change should fail closed before order intent: {error}"
    );
}

fn websocket_instrument_update(inst_id: &str, state: &str) -> OkxWebsocketInstrumentUpdate {
    OkxWebsocketInstrumentUpdate {
        inst_type: "SPOT".to_owned(),
        inst_id: inst_id.to_owned(),
        group_id: "12".to_owned(),
        state: state.to_owned(),
        tick_size: "0.1".to_owned(),
        lot_size: "0.00000001".to_owned(),
        min_size: "0.00001".to_owned(),
        max_limit_size: "999".to_owned(),
        max_limit_amount: "100000".to_owned(),
        max_market_size: "100".to_owned(),
        max_market_amount: "100000".to_owned(),
        max_trigger_size: "999".to_owned(),
        continuous_trading_switch_time: String::new(),
        upcoming_changes: Vec::new(),
    }
}
