use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use pretty_assertions::assert_eq;
use rust_decimal::Decimal;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use crate::{
    config::types::{
        OkxAccountJurisdiction, OkxApiDomain, OkxConfig, OkxTradingService, OkxWebsocketConfig,
        RequestedInstrumentId, RequestedInstrumentType, RequestedTradeMode,
        RequestedTradingInstrument,
    },
    okx::{
        client::OkxOrderAmend,
        trading_instrument::ValidatedTradingInstrument,
        types::{
            MarketBar, OkxAccountConfig, OkxBalance, OkxBalanceDetail, OkxInstrument, OkxOrder,
            OrderKind, OrderSide,
        },
        websocket::{
            OKX_PUBLIC_CANDLE_1M_CHANNEL, OkxMarketCandleHint, OkxPrivateAccountHint,
            OkxPrivateOrderHint,
            trading_session::{
                OkxWebsocketTradingCommandConfig, OkxWebsocketTradingCommandCredentials,
            },
        },
    },
    test_support::{CapturedLogs, HttpTestServer as TestServer},
};

use super::{
    Method, OKX_API_TIMESTAMP, OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS, OKX_CANCEL_ALL_AFTER_TAG,
    OKX_ORDER_EXP_TIME, OKX_ORDER_EXPIRY_WINDOW_MS, OKX_REST_MAX_RESPONSE_BODY_BYTES,
    OKX_SERVER_TIME_TTL, OkxCancelAllAfterTimeout, OkxOcoAmend, OkxOcoProtection, OkxRestClient,
    ServerTimeSnapshot, current_unix_millis, format_okx_timestamp, okx_rate_limit_bucket,
};
use crate::okx::trading_client::{OkxServerTimeRefresher, OkxTradingClient};

const TEST_HTTP_TIMEOUT: Duration = Duration::from_secs(1);
const TEST_HTTP_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
const TEST_WEBSOCKET_TIMEOUT: Duration = Duration::from_secs(1);

#[tokio::test]
async fn spot_price_limit_request_requires_one_exact_fresh_row() -> Result<()> {
    let timestamp = current_unix_millis();
    let server = TestServer::spawn(vec![price_limit_body(
        "MARGIN", "BTC-USDT", "101", "99", timestamp, true,
    )])
    .await?;
    let client = test_client(server.addr())?;
    seed_validated_btc_usdt(&client)?;

    let evidence = client.fresh_spot_price_limit("BTC-USDT").await?;
    let requests = server.await_requests().await?;

    assert_eq!(evidence.source_timestamp_ms(), timestamp);
    evidence.ensure_price(OrderSide::Buy, Decimal::new(101, 0), "test buy")?;
    evidence.ensure_price(OrderSide::Sell, Decimal::new(99, 0), "test sell")?;
    assert_eq!(requests.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/price-limit?instId=BTC-USDT ",
    );
    Ok(())
}

#[tokio::test]
async fn spot_price_limit_contract_failures_do_not_try_a_fallback() -> Result<()> {
    let now = current_unix_millis();
    let cases = [
        (okx_data_body("[]"), "returned 0 price-limit rows"),
        (
            okx_data_body(&format!(
                "[{},{}]",
                price_limit_row("SPOT", "BTC-USDT", "101", "99", now, true),
                price_limit_row("SPOT", "BTC-USDT", "101", "99", now, true)
            )),
            "returned 2 price-limit rows",
        ),
        (
            price_limit_body("SPOT", "ETH-USDT", "101", "99", now, true),
            "returned ETH-USDT",
        ),
        (
            price_limit_body("SWAP", "BTC-USDT", "101", "99", now, true),
            "unsupported instType",
        ),
        (
            price_limit_body("SPOT", "BTC-USDT", "0", "99", now, true),
            "buyLmt must be positive",
        ),
        (
            price_limit_body("SPOT", "BTC-USDT", "", "", now, true),
            "buyLmt must be non-empty",
        ),
        (
            price_limit_body("SPOT", "BTC-USDT", "101", "99", 1, true),
            "is stale",
        ),
        (
            price_limit_body("SPOT", "BTC-USDT", "101", "99", now + 10_000, true),
            "is in the future",
        ),
        (
            price_limit_body("SPOT", "BTC-USDT", "101", "", now, false),
            "must return empty",
        ),
        (
            r#"{"code":"51000","msg":"Parameter instId error","data":[]}"#.to_owned(),
            "Parameter instId error",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body]).await?;
        let client = test_client(server.addr())?;
        seed_validated_btc_usdt(&client)?;
        let error = client
            .fresh_spot_price_limit("BTC-USDT")
            .await
            .expect_err("invalid price-limit evidence must fail closed");
        let requests = server.await_requests().await?;

        assert!(
            format!("{error:#}").contains(expected),
            "expected {expected:?} in {error:#}"
        );
        assert_eq!(
            requests.len(),
            1,
            "price-limit rejection must not try another instrument or endpoint"
        );
        assert_request_target(
            &requests[0],
            "GET /api/v5/public/price-limit?instId=BTC-USDT ",
        );
    }
    Ok(())
}

#[tokio::test]
async fn price_limit_rejection_blocks_place_and_amend_before_mutation() -> Result<()> {
    let now = current_unix_millis();
    for (side, price, expected) in [
        (OrderSide::Buy, "102", "exceeds fresh OKX buyLmt"),
        (OrderSide::Sell, "98", "is below fresh OKX sellLmt"),
    ] {
        let server = TestServer::spawn(vec![price_limit_body(
            "SPOT", "BTC-USDT", "101", "99", now, true,
        )])
        .await?;
        let client = test_client(server.addr())?;
        seed_validated_btc_usdt(&client)?;

        let error = if side == OrderSide::Buy {
            client
                .place_order(
                    "BTC-USDT",
                    side,
                    OrderKind::PostOnly,
                    "0.001",
                    Some(price),
                    "entry1",
                )
                .await
                .expect_err("out-of-band place must fail before mutation")
        } else {
            client
                .amend_order(OkxOrderAmend {
                    inst_id: "BTC-USDT",
                    side,
                    client_order_id: "takeprofit1",
                    new_size: None,
                    new_price: Some(price),
                })
                .await
                .expect_err("out-of-band amend must fail before mutation")
        };
        let requests = server.await_requests().await?;

        assert!(error.to_string().contains(expected), "{error:#}");
        assert_eq!(requests.len(), 1);
        assert_request_target(
            &requests[0],
            "GET /api/v5/public/price-limit?instId=BTC-USDT ",
        );
    }
    Ok(())
}

#[tokio::test]
async fn price_bearing_rest_order_reserves_one_mutation_slot_before_validation() -> Result<()> {
    let server = TestServer::spawn(vec![
        price_limit_body("SPOT", "BTC-USDT", "101", "99", current_unix_millis(), true),
        order_ack_body("ord-entry", "entry1"),
    ])
    .await?;
    let client = test_client(server.addr())?;
    seed_validated_btc_usdt(&client)?;
    let mutation_bucket =
        okx_rate_limit_bucket(&Method::POST, "/api/v5/trade/order", None, Some("BTC-USDT"))?;

    client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100"),
            "entry1",
        )
        .await?;
    let requests = server.await_requests().await?;

    assert_eq!(requests.len(), 2);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/price-limit?instId=BTC-USDT ",
    );
    assert_request_target(&requests[1], "POST /api/v5/trade/order ");
    assert_eq!(
        client
            .rate_limit_pacer
            .reservation_count(&mutation_bucket)?,
        1,
        "the private mutation slot must be reserved before price validation and consumed only once"
    );
    Ok(())
}

#[tokio::test]
async fn websocket_price_preparation_rejects_before_command_identity_or_mutation() -> Result<()> {
    let server = TestServer::spawn(vec![price_limit_body(
        "SPOT",
        "BTC-USDT",
        "101",
        "99",
        current_unix_millis(),
        true,
    )])
    .await?;
    let client = test_client(server.addr())?;
    seed_validated_btc_usdt(&client)?;

    let error = client
        .prepare_websocket_place_order("BTC-USDT", OrderSide::Buy, OrderKind::PostOnly, Some("102"))
        .await
        .expect_err("out-of-band WebSocket order preparation must fail");
    let requests = server.await_requests().await?;

    assert!(
        error.to_string().contains("exceeds fresh OKX buyLmt"),
        "{error:#}"
    );
    assert_eq!(requests.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/price-limit?instId=BTC-USDT ",
    );
    Ok(())
}

#[tokio::test]
async fn market_orders_and_size_only_amends_do_not_invent_a_price_limit_request() -> Result<()> {
    let server = TestServer::spawn(vec![
        order_ack_body("ord-market", "market1"),
        order_ack_body("ord-live", "entry1"),
        order_body_with_amended_shape("BTC-USDT", "ord-live", "entry1", "0.002", "100.2"),
    ])
    .await?;
    let client = test_client(server.addr())?;
    seed_validated_btc_usdt(&client)?;

    client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::Market,
            "100",
            None,
            "market1",
        )
        .await?;
    client
        .amend_order(OkxOrderAmend {
            inst_id: "BTC-USDT",
            side: OrderSide::Buy,
            client_order_id: "entry1",
            new_size: Some("0.002"),
            new_price: None,
        })
        .await?;
    let requests = server.await_requests().await?;

    assert_eq!(requests.len(), 3);
    assert_request_target(&requests[0], "POST /api/v5/trade/order ");
    assert_request_target(&requests[1], "POST /api/v5/trade/amend-order ");
    assert_request_target(
        &requests[2],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn quote_to_usd_index_request_requires_one_exact_fresh_positive_row() -> Result<()> {
    let timestamp = current_unix_millis().to_string();
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        r#"[{{"instId":"USDT-USD","idxPx":"0.9998","ts":"{timestamp}"}}]"#
    ))])
    .await?;
    let client = test_client(server.addr())?;

    let rate = client.quote_usd_rate_for_quote("USDT").await?;
    let requests = server.await_requests().await?;

    assert_eq!(rate.usd_per_quote(), "0.9998".parse::<Decimal>()?);
    assert_eq!(rate.source_timestamp_ms(), Some(timestamp.parse::<i128>()?));
    assert_eq!(requests.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/index-tickers?instId=USDT-USD ",
    );
    Ok(())
}

#[tokio::test]
async fn quote_to_usd_index_accepts_its_bounded_generation_cadence() -> Result<()> {
    let timestamp = current_unix_millis() - 4_000;
    let server = TestServer::spawn(vec![index_ticker_body("USDT", "0.9998", timestamp)]).await?;
    let client = test_client(server.addr())?;

    let rate = client.quote_usd_rate_for_quote("USDT").await?;
    let requests = server.await_requests().await?;

    assert_eq!(rate.usd_per_quote(), "0.9998".parse::<Decimal>()?);
    assert_eq!(rate.source_timestamp_ms(), Some(timestamp));
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn quote_to_usd_index_retries_only_stale_cache_evidence() -> Result<()> {
    let fresh_timestamp = current_unix_millis();
    let server = TestServer::spawn(vec![
        index_ticker_body("USDT", "0.9997", 1),
        index_ticker_body("USDT", "0.9998", fresh_timestamp),
    ])
    .await?;
    let client = test_client(server.addr())?;

    let rate = client.quote_usd_rate_for_quote("USDT").await?;
    let requests = server.await_requests().await?;

    assert_eq!(rate.usd_per_quote(), "0.9998".parse::<Decimal>()?);
    assert_eq!(rate.source_timestamp_ms(), Some(fresh_timestamp));
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_request_target(request, "GET /api/v5/market/index-tickers?instId=USDT-USD ");
    }
    Ok(())
}

#[tokio::test]
async fn quote_to_usd_index_exhausts_bounded_stale_cache_retries() -> Result<()> {
    let server = TestServer::spawn(vec![
        index_ticker_body("USDT", "0.9998", 1),
        index_ticker_body("USDT", "0.9998", 1),
        index_ticker_body("USDT", "0.9998", 1),
    ])
    .await?;
    let client = test_client(server.addr())?;

    let error = client
        .quote_usd_rate_for_quote("USDT")
        .await
        .expect_err("persistently stale index evidence must fail closed");
    let requests = server.await_requests().await?;

    assert!(
        error
            .to_string()
            .contains("OKX REST index ticker timestamp is stale"),
        "the terminal stale response should remain the conversion error: {error}"
    );
    assert_eq!(requests.len(), 3);
    Ok(())
}

#[tokio::test]
async fn usd_quote_uses_identity_without_an_index_request() -> Result<()> {
    let server = TestServer::spawn(Vec::new()).await?;
    let client = test_client(server.addr())?;

    let rate = client.quote_usd_rate_for_quote("USD").await?;
    let requests = server.await_requests().await?;

    assert_eq!(rate.usd_per_quote(), Decimal::ONE);
    assert_eq!(rate.source_timestamp_ms(), None);
    assert!(requests.is_empty());
    Ok(())
}

#[tokio::test]
async fn quote_to_usd_index_contract_failures_do_not_try_a_fallback() -> Result<()> {
    let now = current_unix_millis();
    let cases = [
        (okx_data_body("[]"), "returned 0 index tickers"),
        (
            okx_data_body(&format!(
                r#"[{{"instId":"USDT-USD","idxPx":"1","ts":"{now}"}},{{"instId":"USDT-USD","idxPx":"1","ts":"{now}"}}]"#
            )),
            "returned 2 index tickers",
        ),
        (
            okx_data_body(&format!(
                r#"[{{"instId":"USD-USDT","idxPx":"1","ts":"{now}"}}]"#
            )),
            "expected USDT-USD",
        ),
        (
            okx_data_body(&format!(
                r#"[{{"instId":"USDT-USD","idxPx":"not-a-decimal","ts":"{now}"}}]"#
            )),
            "idxPx",
        ),
        (
            okx_data_body(&format!(
                r#"[{{"instId":"USDT-USD","idxPx":"0","ts":"{now}"}}]"#
            )),
            "must be positive",
        ),
        (
            okx_data_body(&format!(
                r#"[{{"instId":"USDT-USD","idxPx":"1","ts":"{}"}}]"#,
                now + 10_000
            )),
            "is in the future",
        ),
        (
            r#"{"code":"51000","msg":"Parameter instId error","data":[]}"#.to_owned(),
            "Parameter instId error",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body]).await?;
        let client = test_client(server.addr())?;
        let error = client
            .quote_usd_rate_for_quote("USDT")
            .await
            .expect_err("invalid index evidence must fail closed");
        let requests = server.await_requests().await?;

        assert!(
            format!("{error:#}").contains(expected),
            "expected {expected:?} in {error:#}"
        );
        assert_eq!(
            requests.len(),
            1,
            "index-contract rejection must not trigger inverse or fallback requests"
        );
        assert_request_target(
            &requests[0],
            "GET /api/v5/market/index-tickers?instId=USDT-USD ",
        );
    }
    Ok(())
}

#[tokio::test]
async fn one_minute_candles_request_uses_latest_candles_and_sorts_oldest_first() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{},{}]",
        candle_json(2_000, "101"),
        candle_json(1_000, "100")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let candles = client
        .candles("BTC-USDT", "1m", /*limit*/ 2)
        .await
        .expect("candles request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(candles.len(), 2);
    assert_eq!(candles[0].ts_ms, 1_000);
    assert_eq!(candles[1].ts_ms, 2_000);
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=2 ",
    );
}

#[tokio::test]
async fn one_second_candles_request_uses_history_endpoint() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{},{}]",
        candle_json(2_000, "101"),
        candle_json(1_000, "100")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let candles = client
        .candles("BTC-USDT", "1s", /*limit*/ 2)
        .await
        .expect("1s candles request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        candles,
        vec![rest_candle_bar(1_000, 100.0), rest_candle_bar(2_000, 101.0)]
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/history-candles?instId=BTC-USDT&bar=1s&limit=2 ",
    );
}

#[tokio::test]
async fn live_candles_uses_fresh_websocket_hints_without_rest_fallback() -> Result<()> {
    let server = TestServer::spawn(Vec::new()).await?;
    let client = test_client(server.addr())?;
    for ts_ms in [1_000, 2_000, 3_000] {
        client.market_data_cache().update_candle(test_candle_hint(
            "BTC-USDT",
            ts_ms,
            100.0 + ts_ms as f64,
        ))?;
    }

    let candles = client.live_candles("BTC-USDT", "1m", /*limit*/ 3).await?;
    let requests = server.await_requests().await?;

    assert_eq!(
        candles,
        vec![
            market_bar(1_000, 1_100.0),
            market_bar(2_000, 2_100.0),
            market_bar(3_000, 3_100.0),
        ]
    );
    assert_eq!(requests, Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn live_candles_reuses_recent_validated_rest_fallback_for_stale_hints() -> Result<()> {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{},{}]",
        candle_json(2_000, "101"),
        candle_json(1_000, "100")
    ))])
    .await?;
    let client = test_client(server.addr())?;
    client
        .market_data_cache()
        .update_candle(OkxMarketCandleHint {
            inst_id: "BTC-USDT".to_owned(),
            channel: OKX_PUBLIC_CANDLE_1M_CHANNEL.to_owned(),
            bar: market_bar(3_000, 102.0),
            source_ts_ms: Some(3_000),
            received_at: Instant::now() - Duration::from_secs(60),
        })?;

    let first = client.live_candles("BTC-USDT", "1m", /*limit*/ 2).await?;
    let second = client.live_candles("BTC-USDT", "1m", /*limit*/ 2).await?;
    let requests = server.await_requests().await?;

    assert_eq!(
        first,
        vec![rest_candle_bar(1_000, 100.0), rest_candle_bar(2_000, 101.0)]
    );
    assert_eq!(second, first);
    assert_eq!(requests.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=2 ",
    );
    Ok(())
}

#[tokio::test]
async fn live_candles_fetches_rest_again_after_fallback_throttle_expires() -> Result<()> {
    let server = TestServer::spawn(vec![
        okx_data_body(&format!("[{}]", candle_json(1_000, "100"))),
        okx_data_body(&format!("[{}]", candle_json(2_000, "101"))),
    ])
    .await?;
    let client =
        test_client_with_websocket_max_staleness(server.addr(), /*max_staleness_ms*/ 1)?;

    let first = client.live_candles("BTC-USDT", "1m", /*limit*/ 1).await?;
    time::sleep(Duration::from_millis(5)).await;
    let second = client.live_candles("BTC-USDT", "1m", /*limit*/ 1).await?;
    let requests = server.await_requests().await?;

    assert_eq!(first, vec![rest_candle_bar(1_000, 100.0)]);
    assert_eq!(second, vec![rest_candle_bar(2_000, 101.0)]);
    assert_eq!(requests.len(), 2);
    Ok(())
}

#[tokio::test]
async fn live_candle_rest_fallback_cache_keys_include_bar_and_limit() -> Result<()> {
    let server = TestServer::spawn(vec![
        okx_data_body(&format!("[{}]", candle_json(1_000, "100"))),
        okx_data_body(&format!("[{}]", candle_json(2_000, "101"))),
        okx_data_body(&format!(
            "[{},{}]",
            candle_json(3_000, "102"),
            candle_json(4_000, "103")
        )),
    ])
    .await?;
    let client = test_client(server.addr())?;

    let one_minute = client.live_candles("BTC-USDT", "1m", /*limit*/ 1).await?;
    let one_second = client.live_candles("BTC-USDT", "1s", /*limit*/ 1).await?;
    let larger_limit = client.live_candles("BTC-USDT", "1m", /*limit*/ 2).await?;
    let requests = server.await_requests().await?;

    assert_eq!(one_minute, vec![rest_candle_bar(1_000, 100.0)]);
    assert_eq!(one_second, vec![rest_candle_bar(2_000, 101.0)]);
    assert_eq!(
        larger_limit,
        vec![rest_candle_bar(3_000, 102.0), rest_candle_bar(4_000, 103.0)]
    );
    assert_eq!(requests.len(), 3);
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=1 ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/market/history-candles?instId=BTC-USDT&bar=1s&limit=1 ",
    );
    assert_request_target(
        &requests[2],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=2 ",
    );
    Ok(())
}

#[tokio::test]
async fn candles_startup_rest_fetches_are_not_throttled_by_live_fallback_cache() -> Result<()> {
    let server = TestServer::spawn(vec![
        okx_data_body(&format!("[{}]", candle_json(1_000, "100"))),
        okx_data_body(&format!("[{}]", candle_json(2_000, "101"))),
    ])
    .await?;
    let client = test_client(server.addr())?;

    let first = client.candles("BTC-USDT", "1m", /*limit*/ 1).await?;
    let second = client.candles("BTC-USDT", "1m", /*limit*/ 1).await?;
    let requests = server.await_requests().await?;

    assert_eq!(first, vec![rest_candle_bar(1_000, 100.0)]);
    assert_eq!(second, vec![rest_candle_bar(2_000, 101.0)]);
    assert_eq!(requests.len(), 2);
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=1 ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=1 ",
    );
    Ok(())
}

#[tokio::test]
async fn live_candles_non_websocket_bars_request_rest_path() -> Result<()> {
    let server = TestServer::spawn(vec![
        okx_data_body(&format!("[{}]", candle_json(1_000, "100"))),
        okx_data_body(&format!("[{}]", candle_json(2_000, "101"))),
    ])
    .await?;
    let client = test_client(server.addr())?;

    let first = client.live_candles("BTC-USDT", "15m", /*limit*/ 1).await?;
    let second = client.live_candles("BTC-USDT", "15m", /*limit*/ 1).await?;
    let requests = server.await_requests().await?;

    assert_eq!(first, vec![rest_candle_bar(1_000, 100.0)]);
    assert_eq!(second, vec![rest_candle_bar(2_000, 101.0)]);
    assert_eq!(requests.len(), 2);
    assert_request_target(
        &requests[0],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=15m&limit=1 ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/market/candles?instId=BTC-USDT&bar=15m&limit=1 ",
    );
    Ok(())
}

#[tokio::test]
async fn ticker_request_requires_single_ticker_response() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        ticker_json("BTC-USDT", "100.1", "100.2", "100.15")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let ticker = client
        .ticker("BTC-USDT")
        .await
        .expect("ticker request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(ticker.inst_id, "BTC-USDT");
    assert_eq!(ticker.bid_px, "100.1");
    assert_eq!(ticker.ask_px, "100.2");
    assert_eq!(ticker.last, "100.15");
    assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
}

#[tokio::test]
async fn ticker_request_rejects_ambiguous_responses() {
    let cases = [
        (okx_data_body("[]"), "OKX returned 0 tickers for BTC-USDT"),
        (
            okx_data_body(&format!(
                "[{},{}]",
                ticker_json("BTC-USDT", "100.1", "100.2", "100.15"),
                ticker_json("ETH-USDT", "10.1", "10.2", "10.15")
            )),
            "OKX returned 2 tickers for BTC-USDT",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .ticker("BTC-USDT")
            .await
            .expect_err("ticker should fail closed for ambiguous OKX responses");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "ticker failure should mention the ambiguous count: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn ticker_request_rejects_malformed_or_non_positive_prices() {
    let cases = [
        (
            ticker_json("BTC-USDT", "0", "100.2", "100.15"),
            "OKX ticker bidPx must be positive",
        ),
        (
            ticker_json("BTC-USDT", "100.1", "-1", "100.15"),
            "OKX ticker askPx must be positive",
        ),
        (
            ticker_json("BTC-USDT", "100.1", "100.2", "bad"),
            "OKX ticker last must be a decimal",
        ),
    ];

    for (ticker, expected) in cases {
        let server = TestServer::spawn(vec![okx_data_body(&format!("[{ticker}]"))])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .ticker("BTC-USDT")
            .await
            .expect_err("ticker should fail closed for invalid OKX prices");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "ticker failure should mention the malformed price field: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn ticker_request_rejects_non_spot_inst_type() {
    let server = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        ticker_json_with_inst_type("SWAP", "BTC-USDT", "100.1", "100.2", "100.15")
    ))])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .ticker("BTC-USDT")
        .await
        .expect_err("ticker should reject non-spot OKX rows");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX ticker returned instType SWAP for BTC-USDT; expected SPOT"),
        "non-spot ticker row should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn request_timeout_ms_bounds_unresponsive_okx_request() {
    let server = TestServer::spawn_with_response_delay(
        vec![okx_data_body(&format!(
            "[{}]",
            ticker_json("BTC-USDT", "100.1", "100.2", "100.15")
        ))],
        Duration::from_millis(1_500),
    )
    .await
    .expect("test server should start");
    let mut okx = test_okx_config(format!("http://{}", server.addr()));
    okx.request_timeout_ms = 1;
    let client =
        OkxRestClient::new(&okx, /*simulated_trading*/ false).expect("test client should build");

    let error = client
        .ticker("BTC-USDT")
        .await
        .expect_err("ticker request should respect the configured timeout");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        format!("{error:#}").contains("timed out"),
        "timeout failure should report the bounded request timeout: {error:#}"
    );
    assert_eq!(requests.len(), 1);
    assert_request_target(&requests[0], "GET /api/v5/market/ticker?instId=BTC-USDT ");
}

#[tokio::test]
async fn proxy_url_routes_public_rest_request_through_configured_proxy() {
    let proxy = TestServer::spawn(vec![okx_data_body(&format!(
        "[{}]",
        ticker_json("BTC-USDT", "100.1", "100.2", "100.15")
    ))])
    .await
    .expect("proxy server should start");
    let mut okx = test_okx_config("http://okx.test");
    okx.proxy_url = Some(format!("http://{}", proxy.addr()));
    let client =
        OkxRestClient::new(&okx, /*simulated_trading*/ false).expect("test client should build");
    seed_local_server_time(&client);

    let ticker = client
        .ticker("BTC-USDT")
        .await
        .expect("proxied ticker request should succeed");
    let requests = proxy
        .await_requests()
        .await
        .expect("proxy should serve requests");

    assert_eq!(ticker.inst_id, "BTC-USDT");
    assert_request_target(
        &requests[0],
        "GET http://okx.test/api/v5/market/ticker?instId=BTC-USDT ",
    );
}

#[tokio::test]
async fn balances_request_maps_account_balance_endpoint() {
    let server = TestServer::spawn(vec![okx_data_body(
        r#"[{"details":[{"ccy":"BTC","availBal":"0.1","cashBal":"0.12","frozenBal":"0.02"}]}]"#,
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let balances = client
        .balances()
        .await
        .expect("balance request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(balances.len(), 1);
    assert_eq!(balances[0].details[0].ccy, "BTC");
    assert_eq!(balances[0].details[0].available_balance, "0.1");
    assert_request_target(&requests[0], "GET /api/v5/account/balance ");
}

#[tokio::test]
async fn balances_request_rejects_missing_or_malformed_balance_fields() {
    let cases = [
        (
            okx_data_body(r#"[{"details":[{"ccy":"BTC","availBal":"0.1","frozenBal":"0"}]}]"#),
            "OKX balance cashBal must be provided",
        ),
        (
            okx_data_body(r#"[{"details":[{"ccy":"BTC","availBal":"0.1","cashBal":"0.1"}]}]"#),
            "OKX balance frozenBal must be provided",
        ),
        (
            okx_data_body(
                r#"[{"details":[{"ccy":"BTC","availBal":"bad","cashBal":"0.1","frozenBal":"0"}]}]"#,
            ),
            "OKX balance availBal must be a decimal",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .balances()
            .await
            .expect_err("malformed OKX balance response should fail closed");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert_error_chain_contains(&error, expected);
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn account_config_request_maps_account_config_endpoint() {
    let server = TestServer::spawn(vec![account_config_body(
        "1",
        "read_only,trade",
        /*auto_loan*/ false,
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let config = client
        .account_config()
        .await
        .expect("account config request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(config.account_level, "1");
    assert_eq!(config.perm, "read_only,trade");
    assert_request_target(&requests[0], "GET /api/v5/account/config ");
}

#[tokio::test]
async fn zero_fee_selection_uses_authenticated_account_spot_instruments_without_group_filter() {
    let server = TestServer::spawn(vec![okx_data_body(
        r#"[{"instType":"SPOT","instId":"USDC-USDT","baseCcy":"USDC","quoteCcy":"USDT","tradeQuoteCcyList":["USDT"],"groupId":"11","state":"live","ruleType":"normal","openType":"continuous","tickSz":"0.0001","lotSz":"0.01","minSz":"1","upcChg":[]}]"#,
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let instruments = client
        .selection_account_spot_instruments()
        .await
        .expect("account instrument discovery should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(instruments.len(), 1);
    assert_eq!(instruments[0].group_id, "11");
    assert_request_target(
        &requests[0],
        "GET /api/v5/account/instruments?instType=SPOT ",
    );
    assert!(
        requests[0]
            .to_ascii_lowercase()
            .contains("ok-access-key: key")
    );
    assert!(!requests[0].contains("groupId="));
}

#[tokio::test]
async fn zero_fee_selection_uses_public_ticker_and_bounded_books_endpoints() {
    let ticker_server = TestServer::spawn(vec![okx_data_body(
        r#"[{"instType":"SPOT","instId":"USDC-USDT","bidPx":"0.9999","askPx":"1.0001","bidSz":"100","askSz":"100","last":"1","vol24h":"1000000","volCcy24h":"1000100","ts":"1710000000123"}]"#,
    )])
    .await
    .expect("ticker server should start");
    let ticker_client = test_client(ticker_server.addr()).expect("test client should build");
    let ticker = ticker_client
        .selection_ticker("USDC-USDT")
        .await
        .expect("selection ticker should succeed");
    let ticker_requests = ticker_server
        .await_requests()
        .await
        .expect("ticker server should serve requests");
    assert_eq!(ticker.quote_volume_24h, "1000100");
    assert_request_target(
        &ticker_requests[0],
        "GET /api/v5/market/ticker?instId=USDC-USDT ",
    );
    assert!(
        !ticker_requests[0]
            .to_ascii_lowercase()
            .contains("ok-access-key")
    );

    let book_server = TestServer::spawn(vec![okx_data_body(
        r#"[{"asks":[["1.0001","100","0","1"]],"bids":[["0.9999","100","0","1"]],"ts":"1710000000123","seqId":1}]"#,
    )])
    .await
    .expect("book server should start");
    let book_client = test_client(book_server.addr()).expect("test client should build");
    let book = book_client
        .selection_order_book("USDC-USDT", 50)
        .await
        .expect("selection order book should succeed");
    let book_requests = book_server
        .await_requests()
        .await
        .expect("book server should serve requests");
    assert_eq!(book.bids.len(), 1);
    assert_request_target(
        &book_requests[0],
        "GET /api/v5/market/books?instId=USDC-USDT&sz=50 ",
    );
    assert!(
        !book_requests[0]
            .to_ascii_lowercase()
            .contains("ok-access-key")
    );
}

#[tokio::test]
async fn account_config_rejects_missing_preflight_fields() {
    let cases = [
        (
            okx_data_body(
                r#"[{"uid":"1001","mainUid":"1001","acctLv":"1","autoLoan":false,"enableSpotBorrow":false,"spotBorrowAutoRepay":false}]"#,
            ),
            "perm",
        ),
        (
            okx_data_body(
                r#"[{"uid":"1001","mainUid":"1001","acctLv":"1","perm":"read_only,trade","enableSpotBorrow":false,"spotBorrowAutoRepay":false}]"#,
            ),
            "autoLoan",
        ),
        (
            okx_data_body(
                r#"[{"uid":"1001","mainUid":"1001","acctLv":"1","perm":"read_only,trade","autoLoan":false,"spotBorrowAutoRepay":false}]"#,
            ),
            "enableSpotBorrow",
        ),
        (
            okx_data_body(
                r#"[{"uid":"1001","mainUid":"1001","acctLv":"1","perm":"read_only,trade","autoLoan":false,"enableSpotBorrow":false}]"#,
            ),
            "spotBorrowAutoRepay",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .account_config()
            .await
            .expect_err("missing OKX account preflight fields should fail closed");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert_error_chain_contains(&error, expected);
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn account_config_rejects_ambiguous_responses() {
    let cases = [
        (
            okx_data_body("[]"),
            "OKX returned 0 account configuration rows",
        ),
        (
            okx_data_body(&format!(
                "[{},{}]",
                account_config_json("1", "read_only,trade", /*auto_loan*/ false),
                account_config_json("1", "read_only,trade", /*auto_loan*/ false)
            )),
            "OKX returned 2 account configuration rows",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .account_config()
            .await
            .expect_err("account config should fail closed on ambiguous responses");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "account config failure should mention the row count: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn spot_trade_fee_request_maps_spot_fee_rate_endpoint() {
    let server = TestServer::spawn(vec![trade_fee_body("SPOT", "-0.0008", "-0.001")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let fee = client
        .spot_trade_fee_for_group("BTC-USDT", "12")
        .await
        .expect("SPOT fee-rate request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(fee.inst_type, "SPOT");
    assert_eq!(fee.group_id, "12");
    assert_eq!(fee.maker, "-0.0008");
    assert_eq!(fee.taker, "-0.001");
    assert_request_target(
        &requests[0],
        "GET /api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT ",
    );
}

#[tokio::test]
async fn spot_trade_fee_uses_the_instrument_group_instead_of_deprecated_top_level_rates() {
    let server = TestServer::spawn(vec![
        okx_data_body(
            r#"[{"instType":"SPOT","instId":"BTC-USDT","instIdCode":"123456","groupId":"12","state":"live","baseCcy":"BTC","quoteCcy":"USDT","tradeQuoteCcyList":["USDT"],"tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","maxLmtSz":"999","maxLmtAmt":"100000","maxMktSz":"100","maxMktAmt":"100000","maxTriggerSz":"999","initPxLmtPct":"0.05","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}]"#,
        ),
        okx_data_body(
            r#"[{"instType":"SPOT","level":"Lv1","maker":"-0.0001","taker":"-0.0001","feeGroup":[{"groupId":"12","maker":"-0.01","taker":"-0.02"}],"ts":"1763979985847"}]"#,
        ),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .instruments("BTC-USDT")
        .await
        .expect("instrument metadata should establish the fee group");
    let fee = client
        .spot_trade_fee("BTC-USDT")
        .await
        .expect("matching instrument fee group should be selected");

    assert_eq!(fee.maker, "-0.01");
    assert_eq!(fee.taker, "-0.02");
}

#[tokio::test]
async fn spot_trade_fee_rejects_a_requested_group_that_contradicts_instrument_metadata() {
    let server = TestServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .instruments("BTC-USDT")
        .await
        .expect("instrument metadata should establish groupId 12");
    let error = client
        .spot_trade_fee_for_group("BTC-USDT", "13")
        .await
        .expect_err("contradictory groupId must fail before the authenticated request");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("contradicts instrument groupId 12")
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn spot_trade_fee_rejects_missing_duplicate_mismatched_or_malformed_fee_groups() {
    let cases = [
        (
            okx_data_body(
                r#"[{"instType":"SPOT","level":"Lv1","maker":"-0.0008","taker":"-0.001","ts":"1763979985847"}]"#,
            ),
            "missing field `feeGroup`",
        ),
        (
            okx_data_body(
                r#"[{"instType":"SPOT","level":"Lv1","feeGroup":[],"ts":"1763979985847"}]"#,
            ),
            "returned 0 feeGroup rows matching groupId 12",
        ),
        (
            okx_data_body(
                r#"[{"instType":"SPOT","level":"Lv1","feeGroup":[{"groupId":"12","maker":"-0.0008","taker":"-0.001"},{"groupId":"12","maker":"-0.0008","taker":"-0.001"}],"ts":"1763979985847"}]"#,
            ),
            "returned 2 feeGroup rows matching groupId 12",
        ),
        (
            okx_data_body(
                r#"[{"instType":"SPOT","level":"Lv1","feeGroup":[{"groupId":"13","maker":"-0.0008","taker":"-0.001"}],"ts":"1763979985847"}]"#,
            ),
            "returned 0 feeGroup rows matching groupId 12",
        ),
        (
            okx_data_body(
                r#"[{"instType":"SPOT","level":"Lv1","feeGroup":[{"groupId":"12","maker":"not-a-rate","taker":"-0.001"}],"ts":"1763979985847"}]"#,
            ),
            "OKX fee maker must be a decimal",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .spot_trade_fee_for_group("BTC-USDT", "12")
            .await
            .expect_err("ambiguous fee-group evidence should fail closed");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert_error_chain_contains(&error, expected);
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn spot_trade_fee_rejects_ambiguous_or_non_spot_responses() {
    let cases = [
        (okx_data_body("[]"), "OKX returned 0 SPOT fee-rate rows"),
        (
            okx_data_body(&format!(
                "[{},{}]",
                trade_fee_json("SPOT", "-0.0008", "-0.001"),
                trade_fee_json("SPOT", "-0.0008", "-0.001")
            )),
            "OKX returned 2 SPOT fee-rate rows",
        ),
        (
            trade_fee_body("MARGIN", "-0.0008", "-0.001"),
            "returned instType MARGIN",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .spot_trade_fee_for_group("BTC-USDT", "12")
            .await
            .expect_err("fee-rate response should fail closed");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "fee-rate failure should mention the mismatch: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn account_sizing_requests_map_cash_spot_units_and_exact_queries() {
    let server = TestServer::spawn(vec![
        okx_data_body(r#"[{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"0.001","maxSell":"100"}]"#),
        okx_data_body(r#"[{"instId":"BTC-USDT","availBuy":"100","availSell":"0.001"}]"#),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let maximum = client
        .maximum_spot_order_size("BTC-USDT", "100000.1", "USDT")
        .await
        .expect("maximum SPOT order size should parse");
    let available = client
        .maximum_spot_available_size("BTC-USDT", "USDT")
        .await
        .expect("maximum SPOT available amount should parse");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(maximum.max_buy_base().unwrap(), Decimal::new(1, 3));
    assert_eq!(maximum.max_sell_quote().unwrap(), Decimal::from(100u32));
    assert_eq!(
        available.available_buy_quote().unwrap(),
        Decimal::from(100u32)
    );
    assert_eq!(available.available_sell_base().unwrap(), Decimal::new(1, 3));
    assert_request_target(
        &requests[0],
        "GET /api/v5/account/max-size?instId=BTC-USDT&tdMode=cash&px=100000.1&tradeQuoteCcy=USDT ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/account/max-avail-size?instId=BTC-USDT&tdMode=cash&tradeQuoteCcy=USDT ",
    );
}

#[tokio::test]
async fn account_trade_quote_currency_uses_exact_private_instrument_contract() {
    let server = TestServer::spawn(vec![okx_data_body(
        r#"[{"instType":"SPOT","instId":"BTC-USDT","baseCcy":"BTC","quoteCcy":"USDT","tradeQuoteCcyList":["USDT"],"state":"live","ruleType":"normal","tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","upcChg":[]}]"#,
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let trade_quote_currency = client
        .prepare_account_spot_trade_quote_currency("BTC-USDT")
        .await
        .expect("exact account instrument should admit its configured quote");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(trade_quote_currency, "USDT");
    assert_eq!(
        client
            .account_spot_trade_quote_currency("BTC-USDT")
            .unwrap(),
        "USDT"
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/account/instruments?instType=SPOT&instId=BTC-USDT ",
    );
}

#[tokio::test]
async fn account_trade_quote_currency_rejects_absent_or_unadmitted_contracts() {
    let cases = [
        (okx_data_body("[]"), "returned 0 rows for exact BTC-USDT"),
        (
            okx_data_body(
                r#"[{"instType":"SPOT","instId":"BTC-USDT","baseCcy":"BTC","quoteCcy":"USDT","tradeQuoteCcyList":["USD"],"state":"live","ruleType":"normal","tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","upcChg":[]}]"#,
            ),
            "does not admit configured quote USDT",
        ),
        (
            okx_data_body(
                r#"[{"instType":"SPOT","instId":"ETH-USDT","baseCcy":"ETH","quoteCcy":"USDT","tradeQuoteCcyList":["USDT"],"state":"live","ruleType":"normal","tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","upcChg":[]}]"#,
            ),
            "contradicts the configured live SPOT identity",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .prepare_account_spot_trade_quote_currency("BTC-USDT")
            .await
            .expect_err("unsupported account trade route should fail closed");
        assert!(
            error.to_string().contains(expected),
            "account trade route failure should mention the contract mismatch: {error}"
        );
    }
}

#[tokio::test]
async fn requested_trading_tuple_validation_uses_all_read_only_contracts() {
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        okx_data_body(&format!(
            "[{}]",
            ticker_json("BTC-USDT", "99999", "100001", "100000")
        )),
        index_ticker_body("USDT", "1", current_unix_millis()),
        okx_data_body(
            r#"[{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"0.001","maxSell":"100"}]"#,
        ),
        okx_data_body(
            r#"[{"instId":"BTC-USDT","availBuy":"100","availSell":"0.001"}]"#,
        ),
        okx_data_body(
            r#"[{"details":[{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"},{"ccy":"USDT","availBal":"100","cashBal":"100","frozenBal":"0"}]}]"#,
        ),
        trade_fee_body("SPOT", "-0.001", "-0.002"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    let requested = RequestedTradingInstrument {
        instrument: RequestedInstrumentId::new("BTC-USDT".to_owned()).expect("instrument"),
        inst_type: RequestedInstrumentType::Spot,
        td_mode: RequestedTradeMode::Cash,
    };
    let account_config = OkxAccountConfig {
        uid: "1".to_owned(),
        main_uid: "1".to_owned(),
        account_level: "1".to_owned(),
        perm: "read_only,trade".to_owned(),
        auto_loan: false,
        enable_spot_borrow: false,
        spot_borrow_auto_repay: false,
        fee_type: "0".to_owned(),
        kyc_level: String::new(),
    };

    let validated = client
        .validate_trading_instrument(&requested, &account_config)
        .await
        .expect("composite read-only tuple evidence should validate");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(validated.inst_id(), "BTC-USDT");
    assert_eq!(validated.inst_type().as_okx(), "SPOT");
    assert_eq!(validated.td_mode().as_okx(), "cash");
    assert_eq!(validated.trade_quote_ccy(), "USDT");
    assert_eq!(validated.inst_id_code().unwrap(), Some(123_456));
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/account/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(&requests[2], "GET /api/v5/market/ticker?instId=BTC-USDT ");
    assert_request_target(
        &requests[3],
        "GET /api/v5/market/index-tickers?instId=USDT-USD ",
    );
    assert_request_target(
        &requests[4],
        "GET /api/v5/account/max-size?instId=BTC-USDT&tdMode=cash&px=100000&tradeQuoteCcy=USDT ",
    );
    assert_request_target(
        &requests[5],
        "GET /api/v5/account/max-avail-size?instId=BTC-USDT&tdMode=cash&tradeQuoteCcy=USDT ",
    );
    assert_request_target(&requests[6], "GET /api/v5/account/balance ");
    assert_request_target(
        &requests[7],
        "GET /api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT ",
    );
}

#[tokio::test]
async fn account_level_diagnostic_failure_revokes_cached_capability_readiness() {
    let valid_config = account_config_json("1", "read_only,trade", false);
    let cases = [
        (okx_data_body(&format!("[{valid_config}]")), "changed"),
        (
            okx_data_body(&format!(
                "[{}]",
                account_config_json("unknown", "read_only,trade", false)
            )),
            "missing, malformed, or undocumented",
        ),
        (
            okx_data_body(&format!("[{valid_config},{valid_config}]")),
            "2 account configuration rows",
        ),
        (
            okx_data_body(
                r#"[{"uid":"1","mainUid":"1","perm":"read_only,trade","autoLoan":false,"enableSpotBorrow":false,"spotBorrowAutoRepay":false,"feeType":"0"}]"#,
            ),
            "missing field `acctLv`",
        ),
        (
            okx_data_body(
                r#"[{"uid":"1","mainUid":"1","acctLv":"1","acctLv":"1","perm":"read_only,trade","autoLoan":false,"enableSpotBorrow":false,"spotBorrowAutoRepay":false,"feeType":"0"}]"#,
            ),
            "duplicate field `acctLv`",
        ),
    ];

    for (mut response, expected_error) in cases {
        if expected_error == "changed" {
            response = okx_data_body(&format!(
                "[{}]",
                account_config_json("2", "read_only,trade", false)
            ));
        }
        let mut responses = vec![
            instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
            instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
            okx_data_body(&format!(
                "[{}]",
                ticker_json("BTC-USDT", "99999", "100001", "100000")
            )),
            index_ticker_body("USDT", "1", current_unix_millis()),
            okx_data_body(
                r#"[{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"0.001","maxSell":"100"}]"#,
            ),
            okx_data_body(r#"[{"instId":"BTC-USDT","availBuy":"100","availSell":"0.001"}]"#),
            okx_data_body(
                r#"[{"details":[{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"},{"ccy":"USDT","availBal":"100","cashBal":"100","frozenBal":"0"}]}]"#,
            ),
            trade_fee_body("SPOT", "-0.001", "-0.002"),
        ];
        responses.push(response);
        let server = TestServer::spawn(responses)
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");
        client
            .validate_trading_instrument(&requested_btc_usdt(), &cash_spot_account_config())
            .await
            .expect("initial capability generation should validate");

        let error = client
            .account_config()
            .await
            .expect_err("invalid diagnostic observation must revoke readiness");
        assert!(
            format!("{error:#}").contains(expected_error),
            "unexpected diagnostic error for {expected_error}: {error:#}"
        );
        let readiness_error = client
            .validated_trading_instrument("BTC-USDT")
            .expect_err("revoked capability generation must not remain routable");
        assert!(readiness_error.to_string().contains("was not validated"));
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");
        assert_eq!(requests.len(), 9);
    }
}

#[tokio::test]
async fn requested_trading_tuple_uses_cold_start_ticker_generation_bound() {
    let startup_timestamp = (current_unix_millis() - 4_000).to_string();
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        okx_data_body(&format!(
            "[{}]",
            ticker_json_with_timestamp(
                "BTC-USDT",
                "99999",
                "100001",
                "100000",
                &startup_timestamp,
            )
        )),
        index_ticker_body("USDT", "1", current_unix_millis()),
        okx_data_body(
            r#"[{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"0.001","maxSell":"100"}]"#,
        ),
        okx_data_body(
            r#"[{"instId":"BTC-USDT","availBuy":"100","availSell":"0.001"}]"#,
        ),
        okx_data_body(
            r#"[{"details":[{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"},{"ccy":"USDT","availBal":"100","cashBal":"100","frozenBal":"0"}]}]"#,
        ),
        trade_fee_body("SPOT", "-0.001", "-0.002"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let validated = client
        .validate_trading_instrument(&requested_btc_usdt(), &cash_spot_account_config())
        .await
        .expect("cold startup may use a bounded ticker older than the order-time limit");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(validated.inst_id(), "BTC-USDT");
    assert_eq!(requests.len(), 8);
    assert_request_target(&requests[2], "GET /api/v5/market/ticker?instId=BTC-USDT ");
}

#[tokio::test]
async fn requested_trading_tuple_retries_only_stale_startup_ticker_evidence() {
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        okx_data_body(&format!(
            "[{}]",
            ticker_json_with_timestamp("BTC-USDT", "99999", "100001", "100000", "1")
        )),
        okx_data_body(&format!(
            "[{}]",
            ticker_json("BTC-USDT", "99999", "100001", "100000")
        )),
        index_ticker_body("USDT", "1", current_unix_millis()),
        okx_data_body(
            r#"[{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"0.001","maxSell":"100"}]"#,
        ),
        okx_data_body(
            r#"[{"instId":"BTC-USDT","availBuy":"100","availSell":"0.001"}]"#,
        ),
        okx_data_body(
            r#"[{"details":[{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"},{"ccy":"USDT","availBal":"100","cashBal":"100","frozenBal":"0"}]}]"#,
        ),
        trade_fee_body("SPOT", "-0.001", "-0.002"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let validated = client
        .validate_trading_instrument(&requested_btc_usdt(), &cash_spot_account_config())
        .await
        .expect("a fresh retry should replace stale startup ticker evidence");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(validated.inst_id(), "BTC-USDT");
    assert_eq!(requests.len(), 9);
    assert_request_target(&requests[2], "GET /api/v5/market/ticker?instId=BTC-USDT ");
    assert_request_target(&requests[3], "GET /api/v5/market/ticker?instId=BTC-USDT ");
    assert_request_target(
        &requests[4],
        "GET /api/v5/market/index-tickers?instId=USDT-USD ",
    );
}

#[tokio::test]
async fn requested_trading_tuple_exhausts_bounded_stale_ticker_retries() {
    let stale_ticker = || {
        okx_data_body(&format!(
            "[{}]",
            ticker_json_with_timestamp("BTC-USDT", "99999", "100001", "100000", "1")
        ))
    };
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        stale_ticker(),
        stale_ticker(),
        stale_ticker(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .validate_trading_instrument(&requested_btc_usdt(), &cash_spot_account_config())
        .await
        .expect_err("startup must fail closed after its stale ticker retry budget");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX REST ticker timestamp is stale"),
        "the terminal stale response should remain the startup error: {error}"
    );
    assert_eq!(requests.len(), 5);
    for request in &requests[2..] {
        assert_request_target(request, "GET /api/v5/market/ticker?instId=BTC-USDT ");
    }
}

#[tokio::test]
async fn eth_tuple_derives_the_same_quote_index_from_api_metadata() -> Result<()> {
    let server = TestServer::spawn(vec![
        instrument_body("ETH-USDT", "ETH", "USDT", "0.01", "0.0001", "0.0001"),
        instrument_body("ETH-USDT", "ETH", "USDT", "0.01", "0.0001", "0.0001"),
        okx_data_body(&format!(
            "[{}]",
            ticker_json("ETH-USDT", "1999", "2001", "2000")
        )),
        index_ticker_body("USDT", "1", current_unix_millis()),
        okx_data_body(
            r#"[{"instId":"ETH-USDT","ccy":"ETH","maxBuy":"0.05","maxSell":"100"}]"#,
        ),
        okx_data_body(
            r#"[{"instId":"ETH-USDT","availBuy":"100","availSell":"0.05"}]"#,
        ),
        okx_data_body(
            r#"[{"details":[{"ccy":"ETH","availBal":"0.05","cashBal":"0.05","frozenBal":"0"},{"ccy":"USDT","availBal":"100","cashBal":"100","frozenBal":"0"}]}]"#,
        ),
        trade_fee_body("SPOT", "-0.001", "-0.002"),
    ])
    .await?;
    let client = test_client(server.addr())?;
    let requested = RequestedTradingInstrument {
        instrument: RequestedInstrumentId::new("ETH-USDT".to_owned())
            .expect("ETH-USDT should be canonical"),
        inst_type: RequestedInstrumentType::Spot,
        td_mode: RequestedTradeMode::Cash,
    };
    let account_config = OkxAccountConfig {
        uid: "1".to_owned(),
        main_uid: "1".to_owned(),
        account_level: "1".to_owned(),
        perm: "read_only,trade".to_owned(),
        auto_loan: false,
        enable_spot_borrow: false,
        spot_borrow_auto_repay: false,
        fee_type: "0".to_owned(),
        kyc_level: String::new(),
    };

    let validated = client
        .validate_trading_instrument(&requested, &account_config)
        .await?;
    let requests = server.await_requests().await?;

    assert_eq!(validated.inst_id(), "ETH-USDT");
    assert_eq!(validated.quote_ccy(), "USDT");
    assert_request_target(
        &requests[3],
        "GET /api/v5/market/index-tickers?instId=USDT-USD ",
    );
    Ok(())
}

#[tokio::test]
async fn validated_trading_tuple_latches_a_contradictory_public_refresh() {
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        okx_data_body(&format!(
            "[{}]",
            ticker_json("BTC-USDT", "99999", "100001", "100000")
        )),
        index_ticker_body("USDT", "1", current_unix_millis()),
        okx_data_body(
            r#"[{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"0.001","maxSell":"100"}]"#,
        ),
        okx_data_body(
            r#"[{"instId":"BTC-USDT","availBuy":"100","availSell":"0.001"}]"#,
        ),
        okx_data_body(
            r#"[{"details":[{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"},{"ccy":"USDT","availBal":"100","cashBal":"100","frozenBal":"0"}]}]"#,
        ),
        trade_fee_body("SPOT", "-0.001", "-0.002"),
        instrument_body("BTC-USDT", "BTC", "USDT", "0.2", "0.0001", "0.0001"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");
    let requested = RequestedTradingInstrument {
        instrument: RequestedInstrumentId::new("BTC-USDT".to_owned()).expect("instrument"),
        inst_type: RequestedInstrumentType::Spot,
        td_mode: RequestedTradeMode::Cash,
    };
    let account_config = OkxAccountConfig {
        uid: "1".to_owned(),
        main_uid: "1".to_owned(),
        account_level: "1".to_owned(),
        perm: "read_only,trade".to_owned(),
        auto_loan: false,
        enable_spot_borrow: false,
        spot_borrow_auto_repay: false,
        fee_type: "0".to_owned(),
        kyc_level: String::new(),
    };
    client
        .validate_trading_instrument(&requested, &account_config)
        .await
        .expect("startup tuple should validate before the contradictory refresh");

    let refresh_error = client
        .instruments("BTC-USDT")
        .await
        .expect_err("changed public precision must contradict the immutable validated context");
    assert!(
        refresh_error
            .to_string()
            .contains("instrument metadata safety latch"),
        "contradictory public refresh should set the process-lifetime latch: {refresh_error}"
    );
    assert!(
        refresh_error
            .to_string()
            .contains("contradicts validated startup context"),
        "contradictory public refresh should identify the violated authority: {refresh_error}"
    );

    let cloned_client = client.clone();
    let repeated_error = cloned_client
        .instruments("BTC-USDT")
        .await
        .expect_err("a cloned client must share the metadata safety latch");
    assert_eq!(repeated_error.to_string(), refresh_error.to_string());
    let mutation_error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::Limit,
            "0.001",
            Some("100"),
            "entry-after-metadata-change",
        )
        .await
        .expect_err("latched metadata contradiction must block order mutation before transport");
    assert_eq!(mutation_error.to_string(), refresh_error.to_string());

    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    assert_eq!(
        requests.len(),
        9,
        "latched REST refresh and order mutation must not issue additional requests"
    );
}

#[tokio::test]
async fn requested_trading_tuple_rejects_missing_or_duplicate_account_rows() {
    for account_responses in [
        vec![okx_data_body("[]"), okx_data_body("[]")],
        vec![okx_data_body(&format!(
            "[{},{}]",
            instrument_body_data("BTC-USDT"),
            instrument_body_data("BTC-USDT")
        ))],
    ] {
        let mut responses = vec![instrument_body(
            "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
        )];
        responses.extend(account_responses);
        let server = TestServer::spawn(responses)
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");
        let requested = RequestedTradingInstrument {
            instrument: RequestedInstrumentId::new("BTC-USDT".to_owned()).expect("instrument"),
            inst_type: RequestedInstrumentType::Spot,
            td_mode: RequestedTradeMode::Cash,
        };
        let account_config = OkxAccountConfig {
            uid: "1".to_owned(),
            main_uid: "1".to_owned(),
            account_level: "1".to_owned(),
            perm: "trade".to_owned(),
            auto_loan: false,
            enable_spot_borrow: false,
            spot_borrow_auto_repay: false,
            fee_type: "0".to_owned(),
            kyc_level: String::new(),
        };
        client
            .validate_trading_instrument(&requested, &account_config)
            .await
            .expect_err("ambiguous account instrument evidence must fail");
    }
}

#[tokio::test]
async fn requested_public_tuple_rejects_missing_or_duplicate_rows() {
    let duplicate = instrument_body_data("BTC-USDT");
    for response in [
        okx_data_body("[]"),
        okx_data_body(&format!("[{duplicate},{duplicate}]")),
    ] {
        let server = TestServer::spawn(vec![response])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");
        let requested = RequestedTradingInstrument {
            instrument: RequestedInstrumentId::new("BTC-USDT".to_owned()).expect("instrument"),
            inst_type: RequestedInstrumentType::Spot,
            td_mode: RequestedTradeMode::Cash,
        };

        client
            .validate_requested_public_instrument(&requested)
            .await
            .expect_err("public instrument identity must be unique");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");
        assert_eq!(requests.len(), 1);
        assert_request_target(
            &requests[0],
            "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
        );
    }
}

#[tokio::test]
async fn order_mutation_refuses_an_unprepared_account_trade_quote_currency() {
    let server = TestServer::spawn(Vec::new())
        .await
        .expect("test server should start");
    let client = unsynced_test_client(server.addr()).expect("test client should build");
    seed_local_server_time(&client);

    let error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect_err("unprepared trade quote currency should block order mutation");
    let requests = server
        .await_requests()
        .await
        .expect("server should remain unused");

    assert!(
        error
            .to_string()
            .contains("was not validated before order mutation")
    );
    assert!(requests.is_empty());
}

#[tokio::test]
async fn account_sizing_rejects_missing_malformed_or_mismatched_rows() {
    let cases = [
        (
            okx_data_body("[]"),
            true,
            "returned 0 maximum order size rows",
        ),
        (
            okx_data_body(
                r#"[{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"1","maxSell":"1"},{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"1","maxSell":"1"}]"#,
            ),
            true,
            "returned 2 maximum order size rows",
        ),
        (
            okx_data_body(r#"[{"instId":"ETH-USDT","ccy":"ETH","maxBuy":"1","maxSell":"1"}]"#),
            true,
            "returned instId ETH-USDT",
        ),
        (
            okx_data_body(
                r#"[{"instId":"BTC-USDT","ccy":"BTC","maxBuy":"not-a-number","maxSell":"1"}]"#,
            ),
            true,
            "must be a decimal",
        ),
        (
            okx_data_body(r#"[{"instId":"BTC-USDT","availBuy":"1"}]"#),
            false,
            "missing field `availSell`",
        ),
    ];

    for (body, maximum, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = if maximum {
            client
                .maximum_spot_order_size("BTC-USDT", "100000.1", "USDT")
                .await
                .expect_err("bad maximum-size response should fail closed")
        } else {
            client
                .maximum_spot_available_size("BTC-USDT", "USDT")
                .await
                .expect_err("bad maximum-available response should fail closed")
        };

        assert!(
            format!("{error:#}").contains(expected),
            "sizing failure should preserve its strict cause: {error:#}"
        );
    }
}

#[tokio::test]
async fn open_orders_request_maps_pending_spot_endpoint() {
    let server = TestServer::spawn(vec![order_list_body(["ord-live"])])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let orders = client
        .open_orders("BTC-USDT")
        .await
        .expect("open orders request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].order_id, "ord-live");
    assert_request_target(
        &requests[0],
        "GET /api/v5/trade/orders-pending?instType=SPOT&instId=BTC-USDT&limit=100 ",
    );
}

#[tokio::test]
async fn private_http_errors_omit_response_body_details() {
    let sensitive_body = r#"{"code":"50000","msg":"account balance leaked","data":[{"ordId":"secret-order-123","availBal":"999999"}]}"#;
    let server = TestServer::spawn_with_status(vec![(500, sensitive_body.to_owned())])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("private HTTP error should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(
        error.contains("OKX HTTP 500"),
        "HTTP status should remain visible for diagnosis: {error}"
    );
    assert!(
        error.contains("response body omitted"),
        "private HTTP response body should be summarized without raw payload: {error}"
    );
    assert!(
        !error.contains("secret-order-123")
            && !error.contains("999999")
            && !error.contains("account balance leaked"),
        "private response body details must not leak into errors: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn private_malformed_json_errors_omit_response_body_details() {
    let sensitive_body =
        r#"{"code":"0","msg":"","data":[{"ordId":"secret-order-456","availBal":"888888"}"#;
    let server = TestServer::spawn(vec![sensitive_body.to_owned()])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("malformed private response should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(
        error.contains("failed parsing OKX response body"),
        "malformed body should still identify parser failure: {error}"
    );
    assert!(
        error.contains("response body omitted"),
        "malformed private response should be summarized without raw payload: {error}"
    );
    assert!(
        !error.contains("secret-order-456") && !error.contains("888888"),
        "malformed private response body details must not leak into errors: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn okx_rest_response_body_under_limit_parses_normally() {
    let server = TestServer::spawn(vec![order_list_body(["ord-under-limit"])])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let orders = client
        .open_orders("BTC-USDT")
        .await
        .expect("under-limit OKX envelope should parse");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].order_id, "ord-under-limit");
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn okx_api_error_skips_endpoint_data_deserialization() {
    let server = TestServer::spawn(vec![
        r#"{"code":"51000","msg":"Parameter error","data":{"ordId":"secret-order-error"}}"#
            .to_owned(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("non-zero OKX code should fail before endpoint data parsing");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(error.contains("OKX API error 51000: Parameter error"));
    assert!(!error.contains("invalid type"));
    assert!(
        !error.contains("secret-order-error"),
        "OKX API error path must not leak raw response data: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn okx_api_error_without_data_skips_endpoint_data_deserialization() {
    let server = TestServer::spawn(vec![
        r#"{"code":"51000","msg":"Parameter error"}"#.to_owned(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("non-zero OKX code without data should fail before endpoint data parsing");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(error.contains("OKX API error 51000: Parameter error"));
    assert!(!error.contains("missing field"));
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn okx_rest_response_body_under_limit_preserves_gateway_timing_telemetry() {
    let logs = CapturedLogs::default();
    let dispatch = logs.dispatch();
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let server = TestServer::spawn(vec![format!(
        r#"{{"code":"0","msg":"","data":[{}],"inTime":"1000","outTime":"251000"}}"#,
        order_json("BTC-USDT", "ord-timed", "client-ord-timed", "filled")
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let orders = client
        .open_orders("BTC-USDT")
        .await
        .expect("under-limit OKX envelope with timing should parse");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let logs = logs.contents();

    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].order_id, "ord-timed");
    assert!(logs.contains("slow OKX REST gateway timing"));
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn http_429_under_limit_records_rate_limit_and_redacts_body() -> Result<()> {
    let sensitive_body = r#"{"code":"50011","msg":"secret rate limit account body","data":[{"ordId":"secret-429"}]}"#;
    let server = TestServer::spawn_with_status(vec![(429, sensitive_body.to_owned())])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("HTTP 429 should fail closed");
    assert_open_orders_rate_limit_pacer_blocked(&client).await?;
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(error.contains("OKX rate limit"));
    assert!(error.contains("HTTP 429"));
    assert!(error.contains("response body omitted"));
    assert!(
        !error.contains("secret-429") && !error.contains("secret rate limit account body"),
        "HTTP 429 body must remain redacted: {error}"
    );
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn http_429_over_limit_records_rate_limit_and_redacts_body() -> Result<()> {
    let sensitive_body = oversized_okx_body("secret-oversized-429");
    let (addr, requests) = spawn_raw_http_response(
        429,
        "Too Many Requests",
        Some(sensitive_body.len()),
        Some(sensitive_body),
        Duration::ZERO,
    )
    .await?;
    let client = test_client(addr).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("oversized HTTP 429 should fail closed");
    assert_open_orders_rate_limit_pacer_blocked(&client).await?;
    let requests = await_raw_http_requests(requests).await?;
    let error = format!("{error:#}");

    assert!(error.contains("OKX rate limit"));
    assert!(error.contains("HTTP 429"));
    assert!(error.contains("exceeds"));
    assert!(
        !error.contains("secret-oversized-429"),
        "oversized HTTP 429 body must remain redacted: {error}"
    );
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn non_success_over_limit_fails_with_redacted_summary() -> Result<()> {
    let sensitive_body = oversized_okx_body("secret-http-500");
    let (addr, requests) = spawn_raw_http_response(
        500,
        "Internal Server Error",
        Some(sensitive_body.len()),
        Some(sensitive_body),
        Duration::ZERO,
    )
    .await?;
    let client = test_client(addr).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("oversized HTTP error should fail closed");
    let requests = await_raw_http_requests(requests).await?;
    let error = format!("{error:#}");

    assert!(error.contains("OKX HTTP 500"));
    assert!(error.contains("exceeds"));
    assert!(
        !error.contains("secret-http-500"),
        "oversized HTTP error body must remain redacted: {error}"
    );
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn malformed_json_over_limit_fails_before_parsing_and_redacts_body() -> Result<()> {
    let padding = "x".repeat(OKX_REST_MAX_RESPONSE_BODY_BYTES);
    let sensitive_body =
        format!(r#"{{"secret":"secret-malformed-over-limit","padding":"{padding}""#);
    let (addr, requests) = spawn_raw_http_response(
        200,
        "OK",
        Some(sensitive_body.len()),
        Some(sensitive_body),
        Duration::ZERO,
    )
    .await?;
    let client = test_client(addr).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("oversized malformed response should fail before parsing");
    let requests = await_raw_http_requests(requests).await?;
    let error = format!("{error:#}");

    assert!(error.contains("failed reading OKX response body"));
    assert!(error.contains("exceeds"));
    assert!(
        !error.contains("failed parsing OKX response body")
            && !error.contains("secret-malformed-over-limit"),
        "oversized malformed body should fail before parsing and stay redacted: {error}"
    );
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn successful_http_status_with_oversized_body_fails_safely() -> Result<()> {
    let sensitive_body = oversized_okx_body("secret-success-over-limit");
    let (addr, requests) = spawn_raw_http_response(
        200,
        "OK",
        Some(sensitive_body.len()),
        Some(sensitive_body),
        Duration::ZERO,
    )
    .await?;
    let client = test_client(addr).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("oversized success response should fail closed");
    let requests = await_raw_http_requests(requests).await?;
    let error = format!("{error:#}");

    assert!(error.contains("failed reading OKX response body"));
    assert!(error.contains("exceeds"));
    assert!(
        !error.contains("secret-success-over-limit"),
        "oversized successful response body must remain redacted: {error}"
    );
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn content_length_above_limit_fails_before_body_read() -> Result<()> {
    let (addr, requests) = spawn_raw_http_response(
        200,
        "OK",
        Some(OKX_REST_MAX_RESPONSE_BODY_BYTES + 1),
        None,
        Duration::from_millis(250),
    )
    .await?;
    let client = test_client(addr).expect("test client should build");

    let error = time::timeout(Duration::from_millis(100), client.open_orders("BTC-USDT"))
        .await
        .context("declared oversized response should fail before waiting for body bytes")?
        .expect_err("declared oversized response should fail closed");
    let requests = await_raw_http_requests(requests).await?;
    let error = format!("{error:#}");

    assert!(error.contains("declared"));
    assert!(error.contains("exceeds"));
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn no_content_length_streamed_body_over_limit_fails_after_crossing_limit() -> Result<()> {
    let sensitive_body = oversized_okx_body("secret-streamed-over-limit");
    let (addr, requests) =
        spawn_raw_http_response(200, "OK", None, Some(sensitive_body), Duration::ZERO).await?;
    let client = test_client(addr).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("streamed oversized response should fail closed");
    let requests = await_raw_http_requests(requests).await?;
    let error = format!("{error:#}");

    assert!(error.contains("read at least"));
    assert!(error.contains("exceeding"));
    assert!(
        !error.contains("secret-streamed-over-limit"),
        "streamed oversized body must remain redacted: {error}"
    );
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn raw_http_response_server_times_out_when_no_client_connects() -> Result<()> {
    let (_addr, requests) =
        spawn_raw_http_response(200, "OK", Some(0), None, Duration::ZERO).await?;

    let error = await_raw_http_requests(requests)
        .await
        .expect_err("raw response server should time out without a client");

    assert!(
        format!("{error:#}").contains("timed out accepting raw test HTTP connection"),
        "unexpected timeout error: {error:#}"
    );
    Ok(())
}

#[tokio::test]
async fn open_orders_rejects_mismatched_instrument_rows() {
    let server = TestServer::spawn(vec![order_body("ETH-USDT", "ord-live", "entry-1", "live")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("open order responses should fail closed on mismatched instruments");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("for instrument ETH-USDT while requesting BTC-USDT"),
        "mismatched open order instrument should be reported: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn open_orders_rejects_non_spot_inst_type_rows() {
    let server = TestServer::spawn(vec![okx_data_body(
        r#"[{"instType":"MARGIN","instId":"BTC-USDT","ordId":"ord-live","clOrdId":"entry-1","side":"buy","ordType":"post_only","state":"live","avgPx":"100","accFillSz":"0.001","sz":"0.001"}]"#,
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_orders("BTC-USDT")
        .await
        .expect_err("open orders should reject non-spot OKX rows");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX open orders returned instType MARGIN for BTC-USDT; expected SPOT"),
        "non-spot open order row should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn order_lookup_request_maps_client_order_query() {
    let server = TestServer::spawn(vec![order_body(
        "BTC-USDT", "ord-open", "entry-1", "filled",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let order = client
        .order("BTC-USDT", "entry-1")
        .await
        .expect("order lookup should succeed")
        .expect("order lookup should return the OKX order");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(order.order_id, "ord-open");
    assert_request_target(
        &requests[0],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry-1 ",
    );
}

#[tokio::test]
async fn order_lookup_maps_okx_missing_order_code_to_none() {
    let server = TestServer::spawn(vec![
        r#"{"code":"51603","msg":"Order does not exist","data":[]}"#.to_owned(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let order = client
        .order("BTC-USDT", "missing-entry")
        .await
        .expect("documented missing-order response should be authoritative");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(order, None);
    assert_request_target(
        &requests[0],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=missing-entry ",
    );
}

#[tokio::test]
async fn order_lookup_percent_encodes_client_order_query_value() {
    let client_order_id = "entry+1&x=2";
    let server = TestServer::spawn(vec![order_body(
        "BTC-USDT",
        "ord-open",
        client_order_id,
        "filled",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let order = client
        .order("BTC-USDT", client_order_id)
        .await
        .expect("order lookup should succeed")
        .expect("order lookup should return the OKX order");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(order.client_order_id, client_order_id);
    assert_request_target(
        &requests[0],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry%2B1%26x%3D2 ",
    );
}

#[tokio::test]
async fn private_account_hint_does_not_replace_rest_balance_snapshot() -> Result<()> {
    let server = TestServer::spawn(vec![okx_data_body(
        r#"[{"details":[{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"}]}]"#,
    )])
    .await?;
    let client = test_client(server.addr())?;
    client
        .private_event_cache()
        .update_account(OkxPrivateAccountHint {
            balance: OkxBalance {
                details: vec![OkxBalanceDetail {
                    ccy: "BTC".to_owned(),
                    available_balance: "0.002".to_owned(),
                    cash_balance: "0.002".to_owned(),
                    frozen_balance: "0".to_owned(),
                }],
            },
            source_ts_ms: Some(2_000),
            received_at: Instant::now(),
        })?;

    let balances = client.balances().await?;
    let requests = server.await_requests().await?;

    assert_eq!(
        balances,
        vec![OkxBalance {
            details: vec![OkxBalanceDetail {
                ccy: "BTC".to_owned(),
                available_balance: "0.001".to_owned(),
                cash_balance: "0.001".to_owned(),
                frozen_balance: "0".to_owned(),
            }],
        }]
    );
    assert_request_target(&requests[0], "GET /api/v5/account/balance ");
    Ok(())
}

#[tokio::test]
async fn private_order_hint_does_not_replace_rest_order_lookup() -> Result<()> {
    let server =
        TestServer::spawn(vec![order_body("BTC-USDT", "ord-rest", "entry-1", "live")]).await?;
    let client = test_client(server.addr())?;
    let private_order = serde_json::from_str::<OkxOrder>(&order_json(
        "BTC-USDT",
        "ord-private",
        "entry-1",
        "filled",
    ))?;
    client
        .private_event_cache()
        .update_order(OkxPrivateOrderHint {
            order: private_order,
            source_ts_ms: Some(2_000),
            received_at: Instant::now(),
        })?;

    let order = client
        .order("BTC-USDT", "entry-1")
        .await?
        .context("REST order lookup should still return the REST order")?;
    let requests = server.await_requests().await?;

    assert_eq!(order.order_id, "ord-rest");
    assert_eq!(order.state, "live");
    assert_request_target(
        &requests[0],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry-1 ",
    );
    Ok(())
}

#[tokio::test]
async fn order_lookup_rejects_ambiguous_or_mismatched_responses() {
    let cases = [
        (
            okx_data_body(&format!(
                "[{},{}]",
                order_json("BTC-USDT", "ord-open-1", "entry-1", "filled"),
                order_json("BTC-USDT", "ord-open-2", "entry-1", "filled")
            )),
            "OKX returned 2 orders for entry-1",
        ),
        (
            order_body("ETH-USDT", "ord-open", "entry-1", "filled"),
            "for instrument ETH-USDT while requesting BTC-USDT",
        ),
        (
            order_body("BTC-USDT", "ord-open", "other-entry", "filled"),
            "with clOrdId other-entry for requested entry-1",
        ),
        (
            order_body("BTC-USDT", "ord-open", "entry-1", "pending_cancel"),
            "undocumented state \"pending_cancel\"",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .order("BTC-USDT", "entry-1")
            .await
            .expect_err("order lookup should fail closed for ambiguous OKX responses");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "order lookup failure should mention the mismatch: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn place_order_posts_cash_order_body() {
    let server = TestServer::spawn(vec![order_ack_body("ord-new", "entry-1")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect("place order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(acknowledgement.order_id, "ord-new");
    assert_eq!(acknowledgement.client_order_id, "entry-1");
    assert_request_target(&requests[0], "POST /api/v5/trade/order ");
    assert_order_exp_time_header(&requests[0]);
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "instId": "BTC-USDT",
            "tdMode": "cash",
            "side": "buy",
            "ordType": "post_only",
            "sz": "0.001",
            "px": "100.1",
            "pxAmendType": "0",
            "tradeQuoteCcy": "USDT",
            "tag": OKX_CANCEL_ALL_AFTER_TAG,
            "clOrdId": "entry-1",
        }),
    );
}

#[tokio::test]
async fn unprepared_websocket_order_command_falls_back_to_rest_without_lazy_connect() -> Result<()>
{
    let (websocket_url, websocket_server) =
        spawn_websocket_command_server_without_login_ack().await?;
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        order_ack_body("ord-new", "entry1"),
    ])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client.instruments("BTC-USDT").await?;

    let acknowledgement = time::timeout(
        Duration::from_millis(100),
        client.place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry1",
        ),
    )
    .await
    .context("unprepared WebSocket command path should not lazy-connect before REST fallback")??;
    let requests = server.await_requests().await?;
    websocket_server.abort();

    assert_eq!(acknowledgement.order_id, "ord-new");
    assert_eq!(requests.len(), 2);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(&requests[1], "POST /api/v5/trade/order ");
    Ok(())
}

#[tokio::test]
async fn websocket_place_order_preparation_uses_cached_server_time() -> Result<()> {
    let server = TestServer::spawn(Vec::new()).await?;
    let client = test_client(server.addr())?;

    let exp_time = client
        .prepare_websocket_place_order("BTC-USDT", OrderSide::Buy, OrderKind::PostOnly, Some("100"))
        .await?;
    let requests = server.await_requests().await?;

    assert!(!exp_time.is_empty());
    assert_eq!(requests, Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn websocket_amend_order_preparation_uses_cached_server_time() -> Result<()> {
    let server = TestServer::spawn(Vec::new()).await?;
    let client = test_client(server.addr())?;

    let exp_time = client
        .prepare_websocket_amend_order("BTC-USDT", OrderSide::Sell, Some("100"))
        .await?;
    let requests = server.await_requests().await?;

    assert!(!exp_time.is_empty());
    assert_eq!(requests, Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn websocket_order_preparation_lazy_refreshes_when_cache_is_empty() -> Result<()> {
    let server = TestServer::spawn(vec![okx_server_time_body("4102444810123")]).await?;
    let client = unsynced_test_client(server.addr())?;
    seed_btc_usdt_trade_quote_currency(&client);

    let exp_time = client
        .prepare_websocket_place_order("BTC-USDT", OrderSide::Buy, OrderKind::PostOnly, Some("100"))
        .await?;
    let requests = server.await_requests().await?;

    assert!(!exp_time.is_empty());
    assert_eq!(requests.len(), 1);
    assert_request_target(&requests[0], "GET /api/v5/public/time ");
    Ok(())
}

#[tokio::test]
async fn websocket_order_command_login_uses_synced_server_time() -> Result<()> {
    let (websocket_url, websocket_server) =
        spawn_websocket_command_server_that_closes_after_login().await?;
    let server = TestServer::spawn(vec![okx_server_time_body("4102444810123")]).await?;
    let rest = unsynced_test_client(server.addr())?;
    let client = OkxTradingClient::new(
        rest,
        Some(OkxWebsocketTradingCommandConfig::with_ack_timeout(
            websocket_url,
            OkxWebsocketTradingCommandCredentials::new(
                "key".to_owned(),
                "secret".to_owned(),
                "passphrase".to_owned(),
            )?,
            Duration::from_secs(1),
        )?),
    );

    client.prepare_order_command_path().await?;
    let requests = server.await_requests().await?;
    let websocket_messages = await_websocket_command_server(websocket_server).await?;
    let login = serde_json::from_str::<serde_json::Value>(&websocket_messages[0])?;
    let login_timestamp = login["args"][0]["timestamp"]
        .as_str()
        .context("login request should include a timestamp")?;

    assert_eq!(requests.len(), 1);
    assert_request_target(&requests[0], "GET /api/v5/public/time ");
    assert!(
        login_timestamp.starts_with("410244481"),
        "login timestamp should come from mocked OKX server time, got {login_timestamp}"
    );
    Ok(())
}

#[tokio::test]
async fn server_time_refresher_refreshes_expired_cache_before_order_preparation() -> Result<()> {
    let server = TestServer::spawn(vec![okx_server_time_body("4102444810123")]).await?;
    let client = test_client(server.addr())?;
    seed_expired_server_time(&client);
    let trading_client = OkxTradingClient::new(client.clone(), None);
    let mut refresher = OkxServerTimeRefresher::spawn_with_timing(
        trading_client.server_time_refresh_client(),
        Duration::from_secs(60),
        Duration::from_secs(1),
    );

    time::timeout(Duration::from_secs(1), async {
        while client.server_time_cache_needs_refresh()? {
            tokio::task::yield_now().await;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("server time refresher did not refresh the expired cache")??;
    let exp_time = client
        .prepare_websocket_place_order("BTC-USDT", OrderSide::Buy, OrderKind::PostOnly, Some("100"))
        .await?;
    refresher.stop().await?;
    let requests = server.await_requests().await?;

    assert!(!exp_time.is_empty());
    assert_eq!(requests.len(), 1);
    assert_request_target(&requests[0], "GET /api/v5/public/time ");
    Ok(())
}

#[tokio::test]
async fn websocket_order_command_retries_after_failed_startup_prepare() -> Result<()> {
    let (websocket_url, websocket_server) = spawn_recovering_websocket_command_server(
        FirstWebsocketCommandConnection::CloseBeforeLoginAck,
    )
    .await?;
    let server = TestServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client
        .prepare_order_command_path()
        .await
        .expect_err("first startup prewarm attempt should fail");
    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry1",
        )
        .await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;

    assert_eq!(acknowledgement.order_id, "ord-entry1");
    assert_eq!(websocket_messages.len(), 3);
    assert!(
        websocket_command_has_exp_time(&websocket_messages[2]),
        "WebSocket order command should include expTime after reconnect: {}",
        websocket_messages[2]
    );
    assert_eq!(requests.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_order_command_presend_failure_falls_back_to_rest_submit() -> Result<()> {
    let (websocket_url, websocket_server) =
        spawn_websocket_command_server_that_closes_after_login().await?;
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        order_ack_body("ord-new", "entry-1"),
    ])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client.prepare_order_command_path().await?;
    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;

    assert_eq!(acknowledgement.order_id, "ord-new");
    assert_eq!(websocket_messages.len(), 1);
    assert_eq!(requests.len(), 2);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(&requests[1], "POST /api/v5/trade/order ");
    Ok(())
}

#[tokio::test]
async fn websocket_price_rejections_block_place_and_amend_without_rest_fallback() -> Result<()> {
    for amend in [false, true] {
        let (websocket_url, websocket_server) =
            spawn_websocket_command_server_that_closes_after_login().await?;
        let server = TestServer::spawn(vec![
            instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
            price_limit_body("SPOT", "BTC-USDT", "101", "99", current_unix_millis(), true),
        ])
        .await?;
        let client = websocket_command_test_client_with_validated_instrument(
            server.addr(),
            websocket_url,
            Duration::from_secs(1),
        )?;

        client.prepare_order_command_path().await?;
        let error = if amend {
            client
                .amend_order(OkxOrderAmend {
                    inst_id: "BTC-USDT",
                    side: OrderSide::Sell,
                    client_order_id: "takeprofit1",
                    new_size: None,
                    new_price: Some("98"),
                })
                .await
                .expect_err("out-of-band WebSocket amend must fail without REST fallback")
        } else {
            client
                .place_order(
                    "BTC-USDT",
                    OrderSide::Buy,
                    OrderKind::PostOnly,
                    "0.001",
                    Some("102"),
                    "entry1",
                )
                .await
                .expect_err("out-of-band WebSocket place must fail without REST fallback")
        };
        let requests = server.await_requests().await?;
        let websocket_messages = websocket_server.await.context("server task panicked")??;

        assert!(
            format!("{error:#}").contains("refusing REST fallback"),
            "{error:#}"
        );
        assert_eq!(requests.len(), 2);
        assert_request_target(
            &requests[0],
            "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[1],
            "GET /api/v5/public/price-limit?instId=BTC-USDT ",
        );
        assert_eq!(
            websocket_messages.len(),
            1,
            "price rejection must occur before a WebSocket command is sent"
        );
    }
    Ok(())
}

#[tokio::test]
async fn websocket_order_command_retries_after_transient_session_failure() -> Result<()> {
    let (websocket_url, websocket_server) = spawn_recovering_websocket_command_server(
        FirstWebsocketCommandConnection::CloseAfterLoginAck,
    )
    .await?;
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        order_body_with_amended_shape("BTC-USDT", "ord-reconciled", "entry1", "0.001", "100.1"),
    ])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client.prepare_order_command_path().await?;
    let rest_acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry1",
        )
        .await?;
    let websocket_acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.2"),
            "entry2",
        )
        .await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;

    assert_eq!(rest_acknowledgement.order_id, "ord-reconciled");
    assert_eq!(websocket_acknowledgement.order_id, "ord-entry2");
    assert_eq!(websocket_messages.len(), 3);
    assert!(
        websocket_command_has_exp_time(&websocket_messages[2]),
        "WebSocket order command should include expTime after reconnect: {}",
        websocket_messages[2]
    );
    assert_eq!(requests.len(), 2);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_order_command_ambiguous_ack_reconciles_through_rest_lookup() -> Result<()> {
    let (websocket_url, websocket_server) = spawn_websocket_command_server_that_closes().await?;
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        order_body_with_amended_shape("BTC-USDT", "ord-reconciled", "entry1", "0.001", "100.1"),
    ])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client.prepare_order_command_path().await?;
    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry1",
        )
        .await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;

    assert_eq!(acknowledgement.order_id, "ord-reconciled");
    assert_eq!(websocket_messages.len(), 2);
    assert!(
        websocket_command_has_exp_time(&websocket_messages[1]),
        "WebSocket order command should include expTime: {}",
        websocket_messages[1]
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_place_order_missing_ack_serializes_concurrent_commands_and_reconciles()
-> Result<()> {
    let (websocket_url, websocket_server, first_command_received) =
        spawn_websocket_command_server_that_stalls_after_first_command(Duration::from_millis(300))
            .await?;
    let server = RoutedHttpTestServer::spawn(vec![
        RoutedResponse::new(
            "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
            instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        ),
        RoutedResponse::new(
            "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
            order_body_with_amended_shape("BTC-USDT", "ord-reconciled", "entry1", "0.001", "100.1"),
        ),
        RoutedResponse::new(
            "POST /api/v5/trade/order ",
            order_ack_body("ord-rest", "entry2"),
        ),
    ])
    .await?;
    let client = std::sync::Arc::new(websocket_command_test_client(
        server.addr(),
        websocket_url,
        Duration::from_millis(100),
    )?);

    client.instruments("BTC-USDT").await?;
    client.prepare_order_command_path().await?;
    let first_client = client.clone();
    let first = tokio::spawn(async move {
        first_client
            .place_order(
                "BTC-USDT",
                OrderSide::Buy,
                OrderKind::PostOnly,
                "0.001",
                Some("100.1"),
                "entry1",
            )
            .await
    });
    first_command_received
        .await
        .context("test WebSocket server should observe the first command")?;
    let second_client = client.clone();
    let second = tokio::spawn(async move {
        second_client
            .place_order(
                "BTC-USDT",
                OrderSide::Buy,
                OrderKind::PostOnly,
                "0.001",
                Some("100.2"),
                "entry2",
            )
            .await
    });

    let (first_acknowledgement, second_acknowledgement) =
        time::timeout(TEST_WEBSOCKET_TIMEOUT, async {
            let first_acknowledgement = first.await.context("first order task panicked")??;
            let second_acknowledgement = second.await.context("second order task panicked")??;
            Ok::<_, anyhow::Error>((first_acknowledgement, second_acknowledgement))
        })
        .await
        .context("concurrent WebSocket order commands should remain bounded")??;
    let requests = server.await_requests().await?;
    let websocket_messages = await_websocket_command_server(websocket_server).await?;

    assert_eq!(first_acknowledgement.order_id, "ord-reconciled");
    assert_eq!(first_acknowledgement.client_order_id, "entry1");
    assert_eq!(second_acknowledgement.order_id, "ord-rest");
    assert_eq!(second_acknowledgement.client_order_id, "entry2");
    assert_eq!(
        websocket_messages.len(),
        2,
        "the second command must not be sent on a session made ambiguous by the first timeout"
    );
    assert_eq!(json_value(&websocket_messages[1])?["op"], "order");
    assert!(requests.iter().any(|request| {
        request.starts_with("GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ")
    }));
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("POST /api/v5/trade/order ")),
        "the command waiting behind the stalled session should fall back to REST submit"
    );
    Ok(())
}

#[tokio::test]
async fn websocket_place_order_malformed_ack_reconciles_without_rest_resubmit() -> Result<()> {
    let (websocket_url, websocket_server) =
        spawn_websocket_command_server_with_command_response("not-json".to_owned()).await?;
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        order_body_with_amended_shape("BTC-USDT", "ord-reconciled", "entry1", "0.001", "100.1"),
    ])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_millis(100))?;

    client.prepare_order_command_path().await?;
    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry1",
        )
        .await?;
    let requests = server.await_requests().await?;
    let websocket_messages = await_websocket_command_server(websocket_server).await?;

    assert_eq!(acknowledgement.order_id, "ord-reconciled");
    assert_eq!(websocket_messages.len(), 2);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_order_command_success_uses_websocket_without_rest_submit() -> Result<()> {
    let (websocket_url, websocket_server) = spawn_websocket_command_server_that_acks().await?;
    let server = TestServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client.prepare_order_command_path().await?;
    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry1",
        )
        .await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;

    assert_eq!(acknowledgement.order_id, "ord-entry1");
    assert_eq!(websocket_messages.len(), 2);
    assert!(
        websocket_command_has_exp_time(&websocket_messages[1]),
        "WebSocket order command should include expTime: {}",
        websocket_messages[1]
    );
    assert_eq!(requests.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_order_command_reuses_startup_instrument_inst_id_code() -> Result<()> {
    let (websocket_url, websocket_server) = spawn_websocket_command_server_that_acks().await?;
    let server = TestServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    let instrument = client.instruments("BTC-USDT").await?;
    client.prepare_order_command_path().await?;
    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry1",
        )
        .await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;
    let command = serde_json::from_str::<serde_json::Value>(&websocket_messages[1])?;

    assert_eq!(instrument.inst_id, "BTC-USDT");
    assert_eq!(acknowledgement.order_id, "ord-entry1");
    assert_eq!(command["args"][0]["instIdCode"], 123456);
    assert_eq!(requests.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_order_commands_route_market_orders_through_rest() -> Result<()> {
    let server = TestServer::spawn(vec![order_ack_body("ord-market", "stop-exit-1")]).await?;
    let client = websocket_command_test_client(
        server.addr(),
        "ws://127.0.0.1:1".to_owned(),
        Duration::from_millis(100),
    )?;

    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Sell,
            OrderKind::Market,
            "0.001",
            /*price*/ None,
            "stop-exit-1",
        )
        .await?;
    let requests = server.await_requests().await?;

    assert_eq!(acknowledgement.order_id, "ord-market");
    assert_eq!(requests.len(), 1);
    assert_request_target(&requests[0], "POST /api/v5/trade/order ");
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "instId": "BTC-USDT",
            "tdMode": "cash",
            "side": "sell",
            "ordType": "market",
            "sz": "0.001",
            "tgtCcy": "base_ccy",
            "tradeQuoteCcy": "USDT",
            "banAmend": true,
            "slippagePct": "0",
            "pxAmendType": "0",
            "tag": OKX_CANCEL_ALL_AFTER_TAG,
            "clOrdId": "stop-exit-1",
        }),
    );
    Ok(())
}

#[tokio::test]
async fn websocket_cancel_command_success_uses_websocket_without_rest_cancel() -> Result<()> {
    let (websocket_url, websocket_server) = spawn_websocket_command_server_that_acks().await?;
    let server = TestServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client.prepare_order_command_path().await?;
    client.cancel_order("BTC-USDT", "entry1").await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;

    assert_eq!(websocket_messages.len(), 2);
    let command = serde_json::from_str::<serde_json::Value>(&websocket_messages[1])?;
    assert_eq!(command["op"], "cancel-order");
    assert_eq!(command["args"][0]["clOrdId"], "entry1");
    assert!(command.get("expTime").is_none());
    assert!(command["args"][0].get("expTime").is_none());
    assert_eq!(requests.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_cancel_disconnect_reconciles_through_rest_cancel() -> Result<()> {
    let (websocket_url, websocket_server) = spawn_websocket_command_server_that_closes().await?;
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        order_ack_body("ord-live", "entry1"),
    ])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client.prepare_order_command_path().await?;
    client.cancel_order("BTC-USDT", "entry1").await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;

    assert_eq!(websocket_messages.len(), 2);
    assert_eq!(requests.len(), 2);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(&requests[1], "POST /api/v5/trade/cancel-order ");
    Ok(())
}

#[tokio::test]
async fn websocket_cancel_missing_ack_uses_rest_cancel_without_ws_success() -> Result<()> {
    let (websocket_url, websocket_server, _first_command_received) =
        spawn_websocket_command_server_that_stalls_after_first_command(Duration::from_millis(200))
            .await?;
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        order_ack_body("ord-live", "entry1"),
    ])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_millis(100))?;

    client.prepare_order_command_path().await?;
    time::timeout(
        TEST_WEBSOCKET_TIMEOUT,
        client.cancel_order("BTC-USDT", "entry1"),
    )
    .await
    .context("WebSocket cancel ACK timeout should remain bounded")??;
    let requests = server.await_requests().await?;
    let websocket_messages = await_websocket_command_server(websocket_server).await?;

    assert_eq!(websocket_messages.len(), 2);
    assert_eq!(json_value(&websocket_messages[1])?["op"], "cancel-order");
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(&requests[1], "POST /api/v5/trade/cancel-order ");
    Ok(())
}

#[tokio::test]
async fn amend_order_posts_client_order_amend_body() -> Result<()> {
    let server = TestServer::spawn(vec![
        order_ack_body("ord-live", "entry1"),
        order_body_with_amended_shape("BTC-USDT", "ord-live", "entry1", "0.002", "100.2"),
    ])
    .await?;
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .amend_order(OkxOrderAmend {
            inst_id: "BTC-USDT",
            side: OrderSide::Buy,
            client_order_id: "entry1",
            new_size: Some("0.002"),
            new_price: Some("100.2"),
        })
        .await?;
    let requests = server.await_requests().await?;

    assert_eq!(acknowledgement.order_id, "ord-live");
    assert_eq!(acknowledgement.client_order_id, "entry1");
    assert_request_target(&requests[0], "POST /api/v5/trade/amend-order ");
    assert_order_exp_time_header(&requests[0]);
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "instId": "BTC-USDT",
            "clOrdId": "entry1",
            "newSz": "0.002",
            "newPx": "100.2",
            "pxAmendType": "0",
        }),
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn amend_order_reconciles_endpoint_timeout_when_lookup_confirms_shape() -> Result<()> {
    let server = TestServer::spawn(vec![
        okx_endpoint_timeout_body(),
        order_body_with_amended_shape("BTC-USDT", "ord-reconciled", "entry1", "0.002", "100.2"),
    ])
    .await?;
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .amend_order(OkxOrderAmend {
            inst_id: "BTC-USDT",
            side: OrderSide::Buy,
            client_order_id: "entry1",
            new_size: Some("0.002"),
            new_price: Some("100.2"),
        })
        .await?;
    let requests = server.await_requests().await?;

    assert_eq!(acknowledgement.order_id, "ord-reconciled");
    assert_request_target(&requests[0], "POST /api/v5/trade/amend-order ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn amend_order_requires_confirmation_after_acceptance() -> Result<()> {
    let server = TestServer::spawn(vec![
        order_ack_body("ord-live", "entry1"),
        order_body_with_amended_shape("BTC-USDT", "ord-live", "entry1", "0.001", "100.1"),
    ])
    .await?;
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .amend_order(OkxOrderAmend {
            inst_id: "BTC-USDT",
            side: OrderSide::Buy,
            client_order_id: "entry1",
            new_size: Some("0.002"),
            new_price: Some("100.2"),
        })
        .await
        .expect_err("accepted amend should fail closed until lookup confirms the new shape");
    let requests = server.await_requests().await?;
    let error = format!("{error:#}");

    assert!(
        error.contains("amend was accepted but confirmation lookup did not confirm"),
        "unconfirmed amend should explain the accepted-but-unproven state: {error}"
    );
    assert!(
        error.contains("returned sz 0.001 for requested newSz 0.002"),
        "unconfirmed amend should include the mismatched field: {error}"
    );
    assert_request_target(&requests[0], "POST /api/v5/trade/amend-order ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn amend_order_reconciliation_requires_the_requested_side() -> Result<()> {
    let server = TestServer::spawn(vec![
        order_ack_body("ord-live", "entry1"),
        order_body_with_shape(
            "BTC-USDT",
            "ord-live",
            "entry1",
            "live",
            "sell",
            "post_only",
        ),
    ])
    .await?;
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .amend_order(OkxOrderAmend {
            inst_id: "BTC-USDT",
            side: OrderSide::Buy,
            client_order_id: "entry1",
            new_size: Some("0.001"),
            new_price: None,
        })
        .await
        .expect_err("amend reconciliation must preserve the requested order side");
    let requests = server.await_requests().await?;
    let error = format!("{error:#}");

    assert!(
        error.contains("returned side sell for requested buy"),
        "side disagreement must fail closed: {error}"
    );
    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/amend-order ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_amend_command_success_uses_websocket_without_rest_amend() -> Result<()> {
    let (websocket_url, websocket_server) = spawn_websocket_command_server_that_acks().await?;
    let server = TestServer::spawn(vec![instrument_body(
        "BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001",
    )])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client.prepare_order_command_path().await?;
    let acknowledgement = client
        .amend_order(OkxOrderAmend {
            inst_id: "BTC-USDT",
            side: OrderSide::Buy,
            client_order_id: "entry1",
            new_size: Some("0.002"),
            new_price: Some("100.2"),
        })
        .await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;

    assert_eq!(acknowledgement.order_id, "ord-entry1");
    assert_eq!(websocket_messages.len(), 2);
    let command = serde_json::from_str::<serde_json::Value>(&websocket_messages[1])?;
    assert_eq!(command["op"], "amend-order");
    assert_eq!(command["args"][0]["clOrdId"], "entry1");
    assert_eq!(command["args"][0]["newSz"], "0.002");
    assert_eq!(command["args"][0]["newPx"], "100.2");
    assert!(
        websocket_command_has_exp_time(&websocket_messages[1]),
        "WebSocket amend command should include expTime: {}",
        websocket_messages[1]
    );
    assert_eq!(requests.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_amend_command_ambiguous_ack_reconciles_through_rest_lookup() -> Result<()> {
    let (websocket_url, websocket_server) = spawn_websocket_command_server_that_closes().await?;
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        order_body_with_amended_shape("BTC-USDT", "ord-reconciled", "entry1", "0.002", "100.2"),
    ])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_secs(1))?;

    client.prepare_order_command_path().await?;
    let acknowledgement = client
        .amend_order(OkxOrderAmend {
            inst_id: "BTC-USDT",
            side: OrderSide::Buy,
            client_order_id: "entry1",
            new_size: Some("0.002"),
            new_price: Some("100.2"),
        })
        .await?;
    let requests = server.await_requests().await?;
    let websocket_messages = websocket_server.await.context("server task panicked")??;

    assert_eq!(acknowledgement.order_id, "ord-reconciled");
    assert_eq!(websocket_messages.len(), 2);
    assert!(
        websocket_command_has_exp_time(&websocket_messages[1]),
        "WebSocket amend command should include expTime: {}",
        websocket_messages[1]
    );
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn websocket_amend_missing_ack_reconciles_through_rest_lookup() -> Result<()> {
    let (websocket_url, websocket_server, _first_command_received) =
        spawn_websocket_command_server_that_stalls_after_first_command(Duration::from_millis(200))
            .await?;
    let server = TestServer::spawn(vec![
        instrument_body("BTC-USDT", "BTC", "USDT", "0.1", "0.0001", "0.0001"),
        order_body_with_amended_shape("BTC-USDT", "ord-reconciled", "entry1", "0.002", "100.2"),
    ])
    .await?;
    let client =
        websocket_command_test_client(server.addr(), websocket_url, Duration::from_millis(100))?;

    client.prepare_order_command_path().await?;
    let acknowledgement = time::timeout(
        TEST_WEBSOCKET_TIMEOUT,
        client.amend_order(OkxOrderAmend {
            inst_id: "BTC-USDT",
            side: OrderSide::Buy,
            client_order_id: "entry1",
            new_size: Some("0.002"),
            new_price: Some("100.2"),
        }),
    )
    .await
    .context("WebSocket amend ACK timeout should remain bounded")??;
    let requests = server.await_requests().await?;
    let websocket_messages = await_websocket_command_server(websocket_server).await?;

    assert_eq!(acknowledgement.order_id, "ord-reconciled");
    assert_eq!(websocket_messages.len(), 2);
    assert_eq!(json_value(&websocket_messages[1])?["op"], "amend-order");
    assert_request_target(
        &requests[0],
        "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry1 ",
    );
    Ok(())
}

#[tokio::test]
async fn place_order_rejects_empty_or_mismatched_acknowledgements() {
    let cases = [
        (
            okx_data_body("[]"),
            "OKX returned 0 order acknowledgements for entry-1",
        ),
        (
            okx_data_body(
                r#"[{"ordId":"ord-1","clOrdId":"entry-1","sCode":"0","sMsg":""},{"ordId":"ord-2","clOrdId":"entry-1","sCode":"0","sMsg":""}]"#,
            ),
            "OKX returned 2 order acknowledgements for entry-1",
        ),
        (
            order_ack_body("", "entry-1"),
            "OKX order acknowledgement omitted ordId for entry-1",
        ),
        (
            order_ack_body("ord-new", "other-entry"),
            "returned clOrdId other-entry for requested entry-1",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body, okx_data_body("[]")])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .place_order(
                "BTC-USDT",
                OrderSide::Buy,
                OrderKind::PostOnly,
                "0.001",
                Some("100.1"),
                "entry-1",
            )
            .await
            .expect_err("place order should fail closed on bad OKX acknowledgements");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            format!("{error:#}").contains(expected),
            "bad place order acknowledgement should report the mismatch: {error:#}"
        );
        assert_eq!(requests.len(), 2);
        assert_request_target(
            &requests[1],
            "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry-1 ",
        );
    }
}

#[tokio::test]
async fn place_order_reports_per_row_rejection_when_order_id_is_empty() {
    let server = TestServer::spawn(vec![
        order_ack_status_body("", "entry-1", "51000", "Parameter error"),
        okx_data_body("[]"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect_err("place order should surface per-row OKX rejection");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    let error = format!("{error:#}");
    assert!(
        error.contains(r#"sCode="51000""#)
            && error.contains(r#"sMsg="Parameter error""#)
            && error.contains(r#"clOrdId="entry-1""#),
        "per-row rejection should not be masked by empty ordId: {error}"
    );
    assert!(error.contains("reconciliation lookup did not find the order"));
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn place_order_preserves_aggregate_item_rejection_and_reconciles_absence() {
    let server = TestServer::spawn(vec![
        r#"{
          "code":"1","msg":"All operations failed",
          "data":[{
            "ordId":"","clOrdId":"entry-1","sCode":"51008",
            "sMsg":"Insufficient balance","subCode":"51008","ts":"1700000000000"
          }],
          "inTime":"1700000000000000","outTime":"1700000000001000"
        }"#
        .to_owned(),
        okx_data_body("[]"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::Limit,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect_err("aggregate item rejection must fail after reconciliation");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    for expected in [
        r#"code="1""#,
        r#"msg="All operations failed""#,
        r#"sCode="51008""#,
        r#"sMsg="Insufficient balance""#,
        r#"subCode="51008""#,
        r#"ordId="""#,
        r#"clOrdId="entry-1""#,
        r#"ts="1700000000000""#,
        r#"inTime="1700000000000000""#,
        r#"outTime="1700000000001000""#,
        "reconciliation lookup did not find the order",
    ] {
        assert!(
            error.contains(expected),
            "sanitized order rejection should preserve {expected}: {error}"
        );
    }
    assert_eq!(requests.len(), 2);
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry-1 ",
    );
}

#[tokio::test]
async fn place_order_preserves_code_two_mixed_item_results() {
    let server = TestServer::spawn(vec![
        r#"{
          "code":"2","msg":"Bulk operation partially succeeded",
          "data":[
            {"ordId":"ord-1","clOrdId":"entry-1","sCode":"0","sMsg":"","subCode":"","ts":"1700000000000"},
            {"ordId":"","clOrdId":"entry-1","sCode":"51000","sMsg":"Parameter error","subCode":"51001","ts":"1700000000001"}
          ]
        }"#
        .to_owned(),
        okx_data_body("[]"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::Limit,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect_err("mixed aggregate results must fail closed");
    let error = format!("{error:#}");

    assert!(error.contains(r#"code="2""#));
    assert!(error.contains(r#"item[0] sCode="0""#));
    assert!(error.contains(r#"item[1] sCode="51000""#));
    assert!(error.contains(r#"subCode="51001""#));
    assert!(error.contains("reconciliation lookup did not find the order"));
}

#[tokio::test]
async fn place_order_rejects_missing_item_status_code_and_reconciles() {
    let server = TestServer::spawn(vec![
        r#"{"code":"1","msg":"All operations failed","data":[{"ordId":"","clOrdId":"entry-1","sMsg":"Parameter error","subCode":"51001","ts":"1700000000000"}]}"#.to_owned(),
        okx_data_body("[]"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::Limit,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect_err("missing sCode must fail closed");
    let error = format!("{error:#}");

    assert!(error.contains("missing field `sCode`"));
    assert!(error.contains("reconciliation lookup did not find the order"));
}

#[tokio::test]
async fn place_order_reconciles_duplicate_client_order_id_row_when_lookup_matches() {
    let server = TestServer::spawn(vec![
        order_ack_status_body("", "entry-1", "51016", "Client order ID already exists"),
        order_body_with_amended_shape("BTC-USDT", "ord-existing", "entry-1", "0.001", "100.1"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect("duplicate client order id should reconcile to matching existing order");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(acknowledgement.order_id, "ord-existing");
    assert_eq!(acknowledgement.client_order_id, "entry-1");
    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/order ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry-1 ",
    );
}

#[tokio::test]
async fn place_order_rejects_duplicate_client_order_id_row_when_lookup_mismatches() {
    let server = TestServer::spawn(vec![
        order_ack_status_body("", "entry-1", "51016", "Client order ID already exists"),
        order_body_with_shape(
            "BTC-USDT",
            "ord-existing",
            "entry-1",
            "live",
            "sell",
            "post_only",
        ),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect_err("mismatched duplicate client order id should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(
        error.contains(r#"sCode="51016""#)
            && error.contains(r#"sMsg="Client order ID already exists""#),
        "duplicate rejection should remain in the error chain: {error}"
    );
    assert!(
        error.contains("returned side sell for requested buy"),
        "mismatched duplicate reconciliation should explain the unsafe exchange state: {error}"
    );
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn place_order_reconciles_endpoint_timeout_when_order_lookup_finds_client_id() {
    let server = TestServer::spawn_with_status(vec![
        (500, okx_endpoint_timeout_body()),
        (
            200,
            order_body_with_amended_shape(
                "BTC-USDT",
                "ord-reconciled",
                "entry-1",
                "0.001",
                "100.1",
            ),
        ),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect("ambiguous place order should reconcile by client order id");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        acknowledgement,
        crate::okx::types::OkxOrderAck {
            order_id: "ord-reconciled".to_owned(),
            client_order_id: "entry-1".to_owned(),
            status_code: "0".to_owned(),
            status_message: String::new(),
            status_sub_code: String::new(),
            timestamp: String::new(),
        }
    );
    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/order ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry-1 ",
    );
}

#[tokio::test]
async fn place_order_preserves_submit_error_when_reconciliation_finds_no_order() {
    let server = TestServer::spawn_with_status(vec![
        (500, okx_endpoint_timeout_body()),
        (200, okx_data_body("[]")),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect_err("missing reconciliation row should preserve submit failure");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        format!("{error:#}").contains("reconciliation lookup did not find the order"),
        "missing order reconciliation should explain the unresolved submit: {error:#}"
    );
    assert!(
        format!("{error:#}").contains("OKX HTTP 500"),
        "original OKX submit error should be preserved: {error:#}"
    );
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn place_order_rejects_reconciliation_row_with_unknown_state() {
    let server = TestServer::spawn_with_status(vec![
        (500, okx_endpoint_timeout_body()),
        (
            200,
            order_body("BTC-USDT", "ord-reconciled", "entry-1", "pending_cancel"),
        ),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect_err("unknown order state should not reconcile an ambiguous submit");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(
        error.contains("reconciliation lookup failed"),
        "unknown-state reconciliation should preserve the ambiguous submit context: {error}"
    );
    assert!(
        error.contains("undocumented state \"pending_cancel\""),
        "unknown-state reconciliation should report the unsafe state: {error}"
    );
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn place_order_rejects_reconciliation_row_with_wrong_order_shape() {
    let cases = [
        (
            order_body_with_shape(
                "BTC-USDT",
                "ord-reconciled",
                "entry-1",
                "live",
                "sell",
                "post_only",
            ),
            "returned side sell for requested buy",
        ),
        (
            order_body_with_shape(
                "BTC-USDT",
                "ord-reconciled",
                "entry-1",
                "live",
                "buy",
                "market",
            ),
            "returned ordType market for requested post_only",
        ),
        (
            order_body_with_amended_shape(
                "BTC-USDT",
                "ord-reconciled",
                "entry-1",
                "0.002",
                "100.1",
            ),
            "returned sz 0.002 for requested sz 0.001",
        ),
        (
            order_body_with_amended_shape(
                "BTC-USDT",
                "ord-reconciled",
                "entry-1",
                "0.001",
                "100.2",
            ),
            "returned px 100.2 for requested px 100.1",
        ),
    ];

    for (body, expected) in cases {
        let server =
            TestServer::spawn_with_status(vec![(500, okx_endpoint_timeout_body()), (200, body)])
                .await
                .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .place_order(
                "BTC-USDT",
                OrderSide::Buy,
                OrderKind::PostOnly,
                "0.001",
                Some("100.1"),
                "entry-1",
            )
            .await
            .expect_err("wrong-shape reconciliation row should preserve submit failure");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");
        let error = format!("{error:#}");

        assert!(
            error.contains(expected),
            "wrong-shape order reconciliation should explain the mismatch: {error}"
        );
        assert!(
            error.contains("OKX HTTP 500"),
            "original OKX submit error should be preserved: {error}"
        );
        assert_eq!(requests.len(), 2);
    }
}

#[tokio::test]
async fn place_market_sell_order_uses_base_target_currency_and_fail_closed_policy() {
    let server = TestServer::spawn(vec![order_ack_body("ord-market", "stop-exit-1")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .place_order(
            "BTC-USDT",
            OrderSide::Sell,
            OrderKind::Market,
            "0.001",
            /*price*/ None,
            "stop-exit-1",
        )
        .await
        .expect("market order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_request_target(&requests[0], "POST /api/v5/trade/order ");
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "instId": "BTC-USDT",
            "tdMode": "cash",
            "side": "sell",
            "ordType": "market",
            "sz": "0.001",
            "tgtCcy": "base_ccy",
            "tradeQuoteCcy": "USDT",
            "banAmend": true,
            "slippagePct": "0",
            "pxAmendType": "0",
            "tag": OKX_CANCEL_ALL_AFTER_TAG,
            "clOrdId": "stop-exit-1",
        }),
    );
}

#[tokio::test]
async fn place_market_buy_order_uses_quote_target_currency_and_fail_closed_policy() {
    let server = TestServer::spawn(vec![order_ack_body("ord-market", "market-entry-1")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .place_order(
            "BTC-USDT",
            OrderSide::Buy,
            OrderKind::Market,
            "100",
            /*price*/ None,
            "market-entry-1",
        )
        .await
        .expect("market order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_request_target(&requests[0], "POST /api/v5/trade/order ");
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "instId": "BTC-USDT",
            "tdMode": "cash",
            "side": "buy",
            "ordType": "market",
            "sz": "100",
            "tgtCcy": "quote_ccy",
            "tradeQuoteCcy": "USDT",
            "banAmend": true,
            "slippagePct": "0",
            "pxAmendType": "0",
            "tag": OKX_CANCEL_ALL_AFTER_TAG,
            "clOrdId": "market-entry-1",
        }),
    );
}

#[tokio::test]
async fn place_order_rejects_instrument_without_a_validated_route_before_submit() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let client = test_client(
        listener
            .local_addr()
            .expect("test listener should have address"),
    )
    .expect("test client should build");

    let error = client
        .place_order(
            "BTC",
            OrderSide::Buy,
            OrderKind::PostOnly,
            "0.001",
            Some("100.1"),
            "entry-1",
        )
        .await
        .expect_err("unvalidated OKX spot instrument should fail before submit");

    assert!(
        error
            .to_string()
            .contains("OKX trading tuple for BTC was not validated before order mutation"),
        "order mutation must require the immutable validated route: {error}"
    );
}

#[tokio::test]
async fn cancel_order_posts_client_order_cancel_body() {
    let server = TestServer::spawn(vec![order_ack_body("ord-live", "entry-1")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .cancel_order("BTC-USDT", "entry-1")
        .await
        .expect("cancel order request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_request_target(&requests[0], "POST /api/v5/trade/cancel-order ");
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "instId": "BTC-USDT",
            "clOrdId": "entry-1",
        }),
    );
}

#[tokio::test]
async fn cancel_order_rejects_empty_or_mismatched_acknowledgements() {
    let cases = [
        (
            okx_data_body("[]"),
            "OKX returned 0 cancel acknowledgements for entry-1",
        ),
        (
            order_ack_body("ord-live", "other-entry"),
            "returned clOrdId other-entry for requested entry-1",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .cancel_order("BTC-USDT", "entry-1")
            .await
            .expect_err("cancel order should fail closed on bad OKX acknowledgements");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "bad cancel order acknowledgement should report the mismatch: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn cancel_order_treats_rejected_cancel_as_resolved_when_order_is_terminal_or_missing() {
    let cases = [
        vec![
            order_ack_status_body("ord-filled", "entry-1", "51400", "Order already done"),
            order_body("BTC-USDT", "ord-filled", "entry-1", "filled"),
        ],
        vec![
            order_ack_status_body("ord-missing", "entry-1", "51400", "Order not found"),
            okx_data_body("[]"),
        ],
    ];

    for responses in cases {
        let server = TestServer::spawn(responses)
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        client
            .cancel_order("BTC-USDT", "entry-1")
            .await
            .expect("terminal or missing order should resolve failed cancel");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[0], "POST /api/v5/trade/cancel-order ");
        assert_request_target(
            &requests[1],
            "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry-1 ",
        );
    }
}

#[tokio::test]
async fn cancel_order_reconciles_aggregate_api_error_when_order_is_terminal() {
    let server = TestServer::spawn(vec![
        r#"{"code":"1","msg":"All operations failed","data":[{"ordId":"ord-filled","clOrdId":"entry-1","sCode":"51400","sMsg":"Order already done"}]}"#
            .to_owned(),
        order_body("BTC-USDT", "ord-filled", "entry-1", "filled"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .cancel_order("BTC-USDT", "entry-1")
        .await
        .expect("terminal REST state should resolve aggregate cancel error");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/cancel-order ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order?instId=BTC-USDT&clOrdId=entry-1 ",
    );
}

#[tokio::test]
async fn cancel_order_preserves_aggregate_api_error_when_order_is_live() {
    let server = TestServer::spawn(vec![
        r#"{"code":"1","msg":"All operations failed","data":[{"ordId":"ord-live","clOrdId":"entry-1","sCode":"51400","sMsg":"Order still live"}]}"#
            .to_owned(),
        order_body("BTC-USDT", "ord-live", "entry-1", "live"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .cancel_order("BTC-USDT", "entry-1")
        .await
        .expect_err("live REST state must keep aggregate cancel error unresolved");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(error.contains("REST still reports the order live"));
    assert!(error.contains(r#"OKX order API error code="1" msg="All operations failed""#));
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn cancel_order_keeps_rejected_cancel_failed_when_order_is_still_live() {
    let server = TestServer::spawn(vec![
        order_ack_status_body("ord-live", "entry-1", "51400", "Order still live"),
        order_body("BTC-USDT", "ord-live", "entry-1", "live"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .cancel_order("BTC-USDT", "entry-1")
        .await
        .expect_err("live order should keep failed cancel rejected");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    let error = format!("{error:#}");
    assert!(
        error.contains(r#"sCode="51400""#) && error.contains(r#"sMsg="Order still live""#),
        "live cancel race should preserve the OKX rejection: {error}"
    );
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn open_algo_orders_request_maps_pending_trigger_endpoint() {
    let server = TestServer::spawn(vec![algo_list_body(["algo-live"])])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let orders = client
        .open_algo_orders("BTC-USDT")
        .await
        .expect("open algo orders request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].algo_id, "algo-live");
    assert_request_target(
        &requests[0],
        "GET /api/v5/trade/orders-algo-pending?instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100 ",
    );
}

#[tokio::test]
async fn open_algo_orders_rejects_mismatched_instrument_rows() {
    let server = TestServer::spawn(vec![algo_body("ETH-USDT", "algo-live", "stop-1", "live")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_algo_orders("BTC-USDT")
        .await
        .expect_err("open algo responses should fail closed on mismatched instruments");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("for instrument ETH-USDT while requesting BTC-USDT"),
        "mismatched open algo instrument should be reported: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn open_algo_orders_rejects_non_spot_inst_type_rows() {
    let server = TestServer::spawn(vec![okx_data_body(
        r#"[{"instType":"SWAP","instId":"BTC-USDT","algoId":"algo-live","algoClOrdId":"stop-1","side":"sell","ordType":"trigger","orderPx":"-1","state":"live","triggerPx":"100","sz":"0.001"}]"#,
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .open_algo_orders("BTC-USDT")
        .await
        .expect_err("open algo orders should reject non-spot OKX rows");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX open algo orders returned instType SWAP for BTC-USDT; expected SPOT"),
        "non-spot open algo row should fail closed: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn open_algo_orders_rejects_undocumented_states() {
    let cases = ["", " ", "failed", "unknown"];

    for state in cases {
        let server = TestServer::spawn(vec![algo_body("BTC-USDT", "algo-live", "stop-1", state)])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .open_algo_orders("BTC-USDT")
            .await
            .expect_err("open algo orders should reject undocumented states");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error
                .to_string()
                .contains("OKX open algo orders returned algo algo-live with undocumented state"),
            "undocumented open algo state should fail closed: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn place_trigger_order_posts_spot_trigger_body() {
    let server = TestServer::spawn(vec![algo_ack_body("algo-new", "stop-1")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
        .await
        .expect("place trigger request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(acknowledgement.algo_id, "algo-new");
    assert_eq!(acknowledgement.client_order_id, "stop-1");
    assert_request_target(&requests[0], "POST /api/v5/trade/order-algo ");
    assert!(!requests[0].contains(OKX_ORDER_EXP_TIME));
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "instId": "BTC-USDT",
            "tdMode": "cash",
            "side": "sell",
            "ordType": "trigger",
            "sz": "0.001",
            "triggerPx": "99.5",
            "triggerPxType": "last",
            "orderPx": "-1",
            "tradeQuoteCcy": "USDT",
            "tag": OKX_CANCEL_ALL_AFTER_TAG,
            "algoClOrdId": "stop-1",
        }),
    );
}

#[tokio::test]
async fn place_trigger_order_rejects_empty_or_mismatched_acknowledgements() {
    let cases = [
        (
            okx_data_body("[]"),
            "OKX returned 0 algo order acknowledgements for stop-1",
        ),
        (
            algo_ack_body("", "stop-1"),
            "OKX algo order acknowledgement omitted algoId for stop-1",
        ),
        (
            algo_ack_body("algo-new", "other-stop"),
            "returned algoClOrdId other-stop for requested stop-1",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
            .await
            .expect_err("place trigger should fail closed on bad OKX acknowledgements");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "bad trigger acknowledgement should report the mismatch: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn place_trigger_order_reports_per_row_rejection_when_algo_id_is_empty() {
    let server = TestServer::spawn(vec![algo_ack_status_body(
        "",
        "stop-1",
        "51000",
        "Parameter error",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
        .await
        .expect_err("place trigger should surface per-row OKX rejection");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX algo order stop-1 rejected: 51000 Parameter error"),
        "per-row algo rejection should not be masked by empty algoId: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn place_trigger_order_preserves_aggregate_rejection_evidence_when_reconciliation_is_missing()
{
    let server = TestServer::spawn(vec![
        algo_mutation_error_body(
            r#"[{"algoId":"","algoClOrdId":"stop-1","sCode":"51000","sMsg":"Parameter error"}]"#,
            Some(("100", "125")),
        ),
        okx_data_body("[]"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
        .await
        .expect_err("missing reconciliation row should preserve aggregate algo rejection");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(
        error.contains(r#"OKX algo API error code="1" msg="All operations failed""#),
        "aggregate rejection should remain in the error chain: {error}"
    );
    assert!(
        error.contains(r#"item[0] sCode="51000" sMsg="Parameter error""#),
        "row rejection should remain in the error chain: {error}"
    );
    assert!(
        error.contains(r#"algoClOrdId="stop-1""#),
        "stable algo identity should remain in the error chain: {error}"
    );
    assert!(
        error.contains(r#"inTime="100" outTime="125""#),
        "gateway timing should remain in the sanitized error: {error}"
    );
    assert!(
        error.contains("reconciliation lookup did not find the algo order"),
        "REST reconciliation result should remain authoritative: {error}"
    );
    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/order-algo ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order-algo?algoClOrdId=stop-1 ",
    );
}

#[tokio::test]
async fn place_trigger_order_reconciles_aggregate_error_without_resubmission() {
    let server = TestServer::spawn(vec![
        algo_mutation_error_body(
            r#"[{"algoId":"","algoClOrdId":"stop-1","sCode":"51065","sMsg":"Duplicate algoClOrdId"}]"#,
            None,
        ),
        algo_body_with_order_shape(
            "BTC-USDT",
            "algo-existing",
            "stop-1",
            "live",
            AlgoOrderShape {
                side: "sell",
                order_type: "trigger",
                order_price: "-1",
                trigger_price: "99.5",
                size: "0.001",
            },
        ),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
        .await
        .expect("exact live REST state should reconcile aggregate submit error");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(acknowledgement.algo_id, "algo-existing");
    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/order-algo ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order-algo?algoClOrdId=stop-1 ",
    );
}

#[tokio::test]
async fn place_trigger_order_algo_error_envelope_is_strict_and_sanitized() {
    let cases = [
        (algo_mutation_error_body("[]", None), r#"data=[]"#, None),
        (
            r#"{"code":"1","msg":"All operations failed"}"#.to_owned(),
            r#"data=[]"#,
            None,
        ),
        (
            algo_mutation_error_body(
                r#"[{"algoId":"foreign","algoClOrdId":"other","sCode":"51000","sMsg":"Foreign"},{"algoId":"foreign-2","algoClOrdId":"other","sCode":"51000","sMsg":"Duplicate"}]"#,
                None,
            ),
            r#"item[1] sCode="51000" sMsg="Duplicate""#,
            None,
        ),
        (
            algo_mutation_error_body(
                r#"[{"algoId":"secret-algo","algoClOrdId":"stop-1","sMsg":"secret-payload"}]"#,
                None,
            ),
            "failed parsing OKX algo mutation response body",
            Some("secret-payload"),
        ),
    ];

    for (body, expected, prohibited) in cases {
        let server = TestServer::spawn(vec![body, okx_data_body("[]")])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
            .await
            .expect_err("invalid aggregate algo response should fail closed");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");
        let error = format!("{error:#}");

        assert!(
            error.contains(expected),
            "aggregate algo response should preserve sanitized diagnostics: {error}"
        );
        if let Some(prohibited) = prohibited {
            assert!(
                !error.contains(prohibited),
                "malformed private response body must remain redacted: {error}"
            );
        }
        assert!(
            error.contains("reconciliation lookup did not find the algo order"),
            "exact REST reconciliation should remain authoritative: {error}"
        );
        assert_eq!(requests.len(), 2);
    }
}

#[tokio::test]
async fn place_trigger_order_reconciles_duplicate_algo_client_id_row_when_lookup_matches() {
    let server = TestServer::spawn(vec![
        algo_ack_status_body("", "stop-1", "51065", "Duplicate algoClOrdId"),
        algo_body_with_order_shape(
            "BTC-USDT",
            "algo-existing",
            "stop-1",
            "live",
            AlgoOrderShape {
                side: "sell",
                order_type: "trigger",
                order_price: "-1",
                trigger_price: "99.5",
                size: "0.001",
            },
        ),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
        .await
        .expect("duplicate algo client order id should reconcile to matching existing algo");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(acknowledgement.algo_id, "algo-existing");
    assert_eq!(acknowledgement.client_order_id, "stop-1");
    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/order-algo ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order-algo?algoClOrdId=stop-1 ",
    );
}

#[tokio::test]
async fn place_trigger_order_rejects_duplicate_algo_client_id_row_when_lookup_mismatches() {
    let server = TestServer::spawn(vec![
        algo_ack_status_body("", "stop-1", "51065", "Duplicate algoClOrdId"),
        algo_body_with_order_shape(
            "BTC-USDT",
            "algo-existing",
            "stop-1",
            "live",
            AlgoOrderShape {
                side: "buy",
                order_type: "trigger",
                order_price: "-1",
                trigger_price: "99.5",
                size: "0.001",
            },
        ),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
        .await
        .expect_err("mismatched duplicate algo client id should fail closed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(
        error.contains("OKX algo order stop-1 rejected: 51065 Duplicate algoClOrdId"),
        "duplicate algo rejection should remain in the error chain: {error}"
    );
    assert!(
        error.contains("returned side buy for requested sell"),
        "mismatched duplicate algo reconciliation should explain the unsafe exchange state: {error}"
    );
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn place_trigger_order_reconciles_endpoint_timeout_when_algo_lookup_finds_client_id() {
    let server = TestServer::spawn_with_status(vec![
        (500, okx_endpoint_timeout_body()),
        (
            200,
            algo_body_with_order_shape(
                "BTC-USDT",
                "algo-reconciled",
                "stop-1",
                "live",
                AlgoOrderShape {
                    side: "sell",
                    order_type: "trigger",
                    order_price: "-1",
                    trigger_price: "99.5",
                    size: "0.001",
                },
            ),
        ),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
        .await
        .expect("ambiguous trigger submit should reconcile by algo client order id");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(
        acknowledgement,
        crate::okx::types::OkxAlgoOrderAck {
            algo_id: "algo-reconciled".to_owned(),
            client_order_id: "stop-1".to_owned(),
            status_code: "0".to_owned(),
            status_message: String::new(),
        }
    );
    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/order-algo ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order-algo?algoClOrdId=stop-1 ",
    );
}

#[tokio::test]
async fn place_trigger_order_rejects_reconciliation_row_with_wrong_algo_shape() {
    let cases = [
        (
            algo_body_with_shape(
                "BTC-USDT",
                "algo-reconciled",
                "stop-1",
                "live",
                "buy",
                "trigger",
                "-1",
            ),
            "returned side buy for requested sell",
        ),
        (
            algo_body_with_shape(
                "BTC-USDT",
                "algo-reconciled",
                "stop-1",
                "live",
                "sell",
                "trigger",
                "100",
            ),
            "returned ordType trigger orderPx 100 for requested trigger market order",
        ),
        (
            algo_body_with_order_shape(
                "BTC-USDT",
                "algo-reconciled",
                "stop-1",
                "effective",
                AlgoOrderShape {
                    side: "sell",
                    order_type: "trigger",
                    order_price: "-1",
                    trigger_price: "99.5",
                    size: "0.001",
                },
            ),
            "returned state effective instead of live protection",
        ),
        (
            algo_body_with_order_shape(
                "BTC-USDT",
                "algo-reconciled",
                "stop-1",
                "live",
                AlgoOrderShape {
                    side: "sell",
                    order_type: "trigger",
                    order_price: "-1",
                    trigger_price: "99.5",
                    size: "0.002",
                },
            ),
            "returned size 0.002 for requested 0.001",
        ),
        (
            algo_body_with_order_shape(
                "BTC-USDT",
                "algo-reconciled",
                "stop-1",
                "live",
                AlgoOrderShape {
                    side: "sell",
                    order_type: "trigger",
                    order_price: "-1",
                    trigger_price: "98.5",
                    size: "0.001",
                },
            ),
            "returned triggerPx 98.5 for requested 99.5",
        ),
    ];

    for (body, expected) in cases {
        let server =
            TestServer::spawn_with_status(vec![(500, okx_endpoint_timeout_body()), (200, body)])
                .await
                .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .place_trigger_order("BTC-USDT", OrderSide::Sell, "0.001", "99.5", "stop-1")
            .await
            .expect_err("wrong-shape algo reconciliation row should preserve submit failure");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");
        let error = format!("{error:#}");

        assert!(
            error.contains(expected),
            "wrong-shape algo reconciliation should explain the mismatch: {error}"
        );
        assert!(
            error.contains("OKX HTTP 500"),
            "original OKX submit error should be preserved: {error}"
        );
        assert_eq!(requests.len(), 2);
    }
}

#[tokio::test]
async fn place_spot_oco_posts_exact_cash_market_leg_contract() -> Result<()> {
    let server = TestServer::spawn(vec![algo_ack_body("oco-new", "OKXOCOTEST1")]).await?;
    let client = test_client(server.addr())?;

    let acknowledgement = client
        .place_spot_oco(OkxOcoProtection {
            inst_id: "BTC-USDT",
            size: "0.00002",
            take_profit_trigger_price: "110000",
            stop_loss_trigger_price: "90000",
            client_order_id: "OKXOCOTEST1",
        })
        .await?;
    let requests = server.await_requests().await?;

    assert_eq!(acknowledgement.algo_id, "oco-new");
    assert_request_target(&requests[0], "POST /api/v5/trade/order-algo ");
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "instId": "BTC-USDT",
            "tdMode": "cash",
            "side": "sell",
            "ordType": "oco",
            "sz": "0.00002",
            "tpTriggerPx": "110000",
            "tpTriggerPxType": "last",
            "tpOrdPx": "-1",
            "slTriggerPx": "90000",
            "slTriggerPxType": "last",
            "slOrdPx": "-1",
            "tradeQuoteCcy": "USDT",
            "tag": OKX_CANCEL_ALL_AFTER_TAG,
            "algoClOrdId": "OKXOCOTEST1",
        }),
    );
    Ok(())
}

#[tokio::test]
async fn place_spot_oco_reconciles_ambiguous_ack_by_stable_client_id() -> Result<()> {
    let server = TestServer::spawn_with_status(vec![
        (500, okx_endpoint_timeout_body()),
        (
            200,
            oco_body("BTC-USDT", "oco-existing", "OKXOCOTEST1", "live", "", ""),
        ),
    ])
    .await?;
    let client = test_client(server.addr())?;

    let acknowledgement = client
        .place_spot_oco(OkxOcoProtection {
            inst_id: "BTC-USDT",
            size: "0.00002",
            take_profit_trigger_price: "110000",
            stop_loss_trigger_price: "90000",
            client_order_id: "OKXOCOTEST1",
        })
        .await?;
    let requests = server.await_requests().await?;

    assert_eq!(acknowledgement.algo_id, "oco-existing");
    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/order-algo ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order-algo?algoClOrdId=OKXOCOTEST1 ",
    );
    Ok(())
}

#[tokio::test]
async fn place_spot_oco_reconciles_aggregate_rejection_by_stable_client_id() -> Result<()> {
    let server = TestServer::spawn(vec![
        algo_mutation_error_body(
            r#"[{"algoId":"","algoClOrdId":"OKXOCOTEST1","sCode":"51065","sMsg":"Duplicate algoClOrdId"}]"#,
            None,
        ),
        oco_body("BTC-USDT", "oco-existing", "OKXOCOTEST1", "live", "", ""),
    ])
    .await?;
    let client = test_client(server.addr())?;

    let acknowledgement = client
        .place_spot_oco(OkxOcoProtection {
            inst_id: "BTC-USDT",
            size: "0.00002",
            take_profit_trigger_price: "110000",
            stop_loss_trigger_price: "90000",
            client_order_id: "OKXOCOTEST1",
        })
        .await?;
    let requests = server.await_requests().await?;

    assert_eq!(acknowledgement.algo_id, "oco-existing");
    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/order-algo ");
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/order-algo?algoClOrdId=OKXOCOTEST1 ",
    );
    Ok(())
}

#[tokio::test]
async fn spot_oco_detail_and_pending_queries_enforce_exact_identity() -> Result<()> {
    let server = TestServer::spawn(vec![
        oco_body("BTC-USDT", "oco-1", "OKXOCOTEST1", "live", "", ""),
        oco_body("BTC-USDT", "oco-1", "OKXOCOTEST1", "live", "", ""),
    ])
    .await?;
    let client = test_client(server.addr())?;

    let detail = client
        .oco_order_by_client_order_id("BTC-USDT", "OKXOCOTEST1")
        .await?
        .context("expected OCO detail")?;
    let pending = client.open_spot_oco_orders("BTC-USDT").await?;
    let requests = server.await_requests().await?;

    assert_eq!(detail.algo_id, "oco-1");
    assert_eq!(pending.len(), 1);
    assert_request_target(
        &requests[0],
        "GET /api/v5/trade/order-algo?algoClOrdId=OKXOCOTEST1 ",
    );
    assert_request_target(
        &requests[1],
        "GET /api/v5/trade/orders-algo-pending?ordType=oco&instType=SPOT&instId=BTC-USDT&limit=100 ",
    );
    Ok(())
}

#[tokio::test]
async fn spot_oco_detail_rejects_wrong_instrument() -> Result<()> {
    let server = TestServer::spawn(vec![oco_body(
        "ETH-USDT",
        "oco-1",
        "OKXOCOTEST1",
        "live",
        "",
        "",
    )])
    .await?;
    let client = test_client(server.addr())?;

    let error = client
        .oco_order_by_client_order_id("BTC-USDT", "OKXOCOTEST1")
        .await
        .expect_err("OCO detail must reject a different instrument");
    assert!(
        error
            .to_string()
            .contains("instrument ETH-USDT while requesting BTC-USDT")
    );
    Ok(())
}

#[tokio::test]
async fn amend_spot_oco_posts_exact_quantity_and_trigger_update() -> Result<()> {
    let server = TestServer::spawn(vec![algo_ack_body("oco-1", "OKXOCOTEST1")]).await?;
    let client = test_client(server.addr())?;

    client
        .amend_spot_oco(OkxOcoAmend {
            inst_id: "BTC-USDT",
            algo_id: "oco-1",
            client_order_id: "OKXOCOTEST1",
            new_size: "0.00001",
            new_take_profit_trigger_price: "109999.9",
            new_stop_loss_trigger_price: "90000.1",
        })
        .await?;
    let requests = server.await_requests().await?;

    assert_request_target(&requests[0], "POST /api/v5/trade/amend-algos ");
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "instId": "BTC-USDT",
            "algoId": "oco-1",
            "algoClOrdId": "OKXOCOTEST1",
            "cxlOnFail": true,
            "newSz": "0.00001",
            "newTpTriggerPx": "109999.9",
            "newTpTriggerPxType": "last",
            "newTpOrdPx": "-1",
            "newSlTriggerPx": "90000.1",
            "newSlTriggerPxType": "last",
            "newSlOrdPx": "-1",
        }),
    );
    Ok(())
}

#[tokio::test]
async fn amend_spot_oco_preserves_aggregate_rejection_evidence() -> Result<()> {
    let server = TestServer::spawn(vec![algo_mutation_error_body(
        r#"[{"algoId":"oco-1","algoClOrdId":"OKXOCOTEST1","sCode":"51503","sMsg":"OCO amend rejected"}]"#,
        Some(("200", "240")),
    )])
    .await?;
    let client = test_client(server.addr())?;

    let error = client
        .amend_spot_oco(OkxOcoAmend {
            inst_id: "BTC-USDT",
            algo_id: "oco-1",
            client_order_id: "OKXOCOTEST1",
            new_size: "0.00001",
            new_take_profit_trigger_price: "109999.9",
            new_stop_loss_trigger_price: "90000.1",
        })
        .await
        .expect_err("aggregate OCO amend rejection should fail closed");
    let requests = server.await_requests().await?;
    let error = format!("{error:#}");

    assert!(error.contains(r#"OKX algo API error code="1""#));
    assert!(error.contains(r#"item[0] sCode="51503" sMsg="OCO amend rejected""#));
    assert!(error.contains(r#"algoId="oco-1" algoClOrdId="OKXOCOTEST1""#));
    assert!(error.contains(r#"inTime="200" outTime="240""#));
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test]
async fn cancel_spot_oco_uses_documented_algo_cancel_surface() -> Result<()> {
    let server = TestServer::spawn(vec![algo_ack_body("oco-1", "OKXOCOTEST1")]).await?;
    let client = test_client(server.addr())?;

    client.cancel_spot_oco("BTC-USDT", "oco-1").await?;
    let requests = server.await_requests().await?;

    assert_request_target(&requests[0], "POST /api/v5/trade/cancel-algos ");
    assert_request_json(
        &requests[0],
        serde_json::json!([{"instId":"BTC-USDT","algoId":"oco-1"}]),
    );
    Ok(())
}

#[tokio::test]
async fn cancel_spot_oco_reconciles_aggregate_rejection_through_rest() -> Result<()> {
    let server = TestServer::spawn(vec![
        algo_mutation_error_body(
            r#"[{"algoId":"oco-1","algoClOrdId":"OKXOCOTEST1","sCode":"51400","sMsg":"Algo already done"}]"#,
            None,
        ),
        oco_body(
            "BTC-USDT",
            "oco-1",
            "OKXOCOTEST1",
            "effective",
            "tp",
            "0.00002",
        ),
    ])
    .await?;
    let client = test_client(server.addr())?;

    client.cancel_spot_oco("BTC-USDT", "oco-1").await?;
    let requests = server.await_requests().await?;

    assert_eq!(requests.len(), 2);
    assert_request_target(&requests[0], "POST /api/v5/trade/cancel-algos ");
    assert_request_target(&requests[1], "GET /api/v5/trade/order-algo?algoId=oco-1 ");
    Ok(())
}

#[tokio::test]
async fn cancel_algo_order_posts_algo_cancel_body() {
    let server = TestServer::spawn(vec![algo_ack_body("algo-live", "stop-1")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    client
        .cancel_algo_order("BTC-USDT", "algo-live")
        .await
        .expect("cancel algo request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_request_target(&requests[0], "POST /api/v5/trade/cancel-algos ");
    assert_request_json(
        &requests[0],
        serde_json::json!([
            {
                "instId": "BTC-USDT",
                "algoId": "algo-live",
            }
        ]),
    );
}

#[tokio::test]
async fn cancel_algo_order_rejects_empty_or_mismatched_acknowledgements() {
    let cases = [
        (
            okx_data_body("[]"),
            "OKX returned 0 cancel algo acknowledgements for algo-live",
        ),
        (
            algo_ack_body("other-algo", "stop-1"),
            "returned algoId other-algo for requested algo-live",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .cancel_algo_order("BTC-USDT", "algo-live")
            .await
            .expect_err("cancel algo should fail closed on bad OKX acknowledgements");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "bad cancel algo acknowledgement should report the mismatch: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn cancel_algo_order_treats_rejected_cancel_as_resolved_when_algo_is_terminal_or_missing() {
    let cases = [
        vec![
            algo_ack_status_body("algo-live", "stop-1", "51400", "Algo already done"),
            okx_data_body("[]"),
            algo_body("BTC-USDT", "algo-live", "stop-1", "effective"),
        ],
        vec![
            algo_ack_status_body("algo-live", "stop-1", "51400", "Algo not found"),
            okx_data_body("[]"),
            okx_data_body("[]"),
        ],
    ];

    for responses in cases {
        let server = TestServer::spawn(responses)
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        client
            .cancel_algo_order("BTC-USDT", "algo-live")
            .await
            .expect("terminal or missing algo should resolve failed cancel");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert_eq!(requests.len(), 3);
        assert_request_target(&requests[0], "POST /api/v5/trade/cancel-algos ");
        assert_request_target(
            &requests[1],
            "GET /api/v5/trade/orders-algo-pending?instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100 ",
        );
        assert_request_target(
            &requests[2],
            "GET /api/v5/trade/orders-algo-history?instType=SPOT&instId=BTC-USDT&ordType=trigger&algoId=algo-live&limit=100 ",
        );
    }
}

#[tokio::test]
async fn cancel_algo_order_reconciles_aggregate_error_when_algo_is_terminal_or_missing() {
    let cases = [
        vec![
            algo_mutation_error_body(
                r#"[{"algoId":"algo-live","algoClOrdId":"stop-1","sCode":"51400","sMsg":"Algo already done"}]"#,
                None,
            ),
            okx_data_body("[]"),
            algo_body("BTC-USDT", "algo-live", "stop-1", "effective"),
        ],
        vec![
            algo_mutation_error_body(
                r#"[{"algoId":"algo-live","algoClOrdId":"stop-1","sCode":"51400","sMsg":"Algo not found"}]"#,
                None,
            ),
            okx_data_body("[]"),
            okx_data_body("[]"),
        ],
    ];

    for responses in cases {
        let server = TestServer::spawn(responses)
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        client
            .cancel_algo_order("BTC-USDT", "algo-live")
            .await
            .expect("terminal or missing REST state should resolve aggregate cancel error");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert_eq!(requests.len(), 3);
        assert_request_target(&requests[0], "POST /api/v5/trade/cancel-algos ");
        assert_request_target(
            &requests[1],
            "GET /api/v5/trade/orders-algo-pending?instType=SPOT&instId=BTC-USDT&ordType=trigger&limit=100 ",
        );
        assert_request_target(
            &requests[2],
            "GET /api/v5/trade/orders-algo-history?instType=SPOT&instId=BTC-USDT&ordType=trigger&algoId=algo-live&limit=100 ",
        );
    }
}

#[tokio::test]
async fn cancel_algo_order_preserves_aggregate_error_when_algo_is_still_live() {
    let server = TestServer::spawn(vec![
        algo_mutation_error_body(
            r#"[{"algoId":"algo-live","algoClOrdId":"stop-1","sCode":"51400","sMsg":"Algo still live"}]"#,
            Some(("300", "360")),
        ),
        algo_body("BTC-USDT", "algo-live", "stop-1", "live"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .cancel_algo_order("BTC-USDT", "algo-live")
        .await
        .expect_err("live REST state must keep aggregate algo cancel error unresolved");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(error.contains("aggregate API error while REST still reports the algo order live"));
    assert!(error.contains(r#"OKX algo API error code="1" msg="All operations failed""#));
    assert!(error.contains(r#"item[0] sCode="51400" sMsg="Algo still live""#));
    assert!(error.contains(r#"inTime="300" outTime="360""#));
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn cancel_algo_order_preserves_aggregate_error_when_reconciliation_fails() {
    let server = TestServer::spawn(vec![
        algo_mutation_error_body(
            r#"[{"algoId":"algo-live","algoClOrdId":"stop-1","sCode":"51400","sMsg":"Algo cancel rejected"}]"#,
            None,
        ),
        r#"{"code":"0","msg":"","data":{"private":"secret-open-algo-state"}}"#.to_owned(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .cancel_algo_order("BTC-USDT", "algo-live")
        .await
        .expect_err("failed REST reconciliation must preserve aggregate rejection");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(error.contains("aggregate API error and REST reconciliation failed"));
    assert!(error.contains(r#"OKX algo API error code="1" msg="All operations failed""#));
    assert!(error.contains(r#"item[0] sCode="51400" sMsg="Algo cancel rejected""#));
    assert!(
        !error.contains("secret-open-algo-state"),
        "private reconciliation response body must remain redacted: {error}"
    );
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn cancel_algo_order_other_aggregate_codes_fail_without_reconciliation() {
    let server = TestServer::spawn(vec![
        r#"{"code":"2","msg":"Partial operations failed","data":[{"algoId":"algo-live","algoClOrdId":"stop-1","sCode":"51400","sMsg":"Algo cancel rejected"}]}"#
            .to_owned(),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .cancel_algo_order("BTC-USDT", "algo-live")
        .await
        .expect_err("non-aggregate-failure code must fail without alternate handling");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");
    let error = format!("{error:#}");

    assert!(error.contains(r#"OKX algo API error code="2" msg="Partial operations failed""#));
    assert!(error.contains(r#"item[0] sCode="51400" sMsg="Algo cancel rejected""#));
    assert_eq!(
        requests.len(),
        1,
        "only aggregate code 1 may enter cancel reconciliation"
    );
}

#[tokio::test]
async fn cancel_algo_order_keeps_rejected_cancel_failed_when_algo_is_still_live() {
    let server = TestServer::spawn(vec![
        algo_ack_status_body("algo-live", "stop-1", "51400", "Algo still live"),
        algo_body("BTC-USDT", "algo-live", "stop-1", "live"),
    ])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .cancel_algo_order("BTC-USDT", "algo-live")
        .await
        .expect_err("live algo should keep failed cancel rejected");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error
            .to_string()
            .contains("OKX cancel algo algo-live rejected: 51400 Algo still live"),
        "live algo cancel race should preserve the OKX rejection: {error}"
    );
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn cancel_all_after_posts_deadman_timeout_body() {
    let server = TestServer::spawn(vec![cancel_all_after_ack_body(
        "1710000010000",
        "1710000000000",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .cancel_all_after(
            OkxCancelAllAfterTimeout::new(OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS)
                .expect("test timeout should be valid"),
        )
        .await
        .expect("cancel-all-after request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(acknowledgement.trigger_time, "1710000010000");
    assert_eq!(acknowledgement.ts, "1710000000000");
    assert_request_target(&requests[0], "POST /api/v5/trade/cancel-all-after ");
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "timeOut": "120",
            "tag": OKX_CANCEL_ALL_AFTER_TAG,
        }),
    );
}

#[tokio::test]
async fn cancel_all_after_disarm_posts_zero_timeout_body() {
    let server = TestServer::spawn(vec![cancel_all_after_ack_body("0", "1710000000000")])
        .await
        .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let acknowledgement = client
        .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
        .await
        .expect("cancel-all-after disarm request should succeed");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert_eq!(acknowledgement.trigger_time, "0");
    assert_eq!(acknowledgement.ts, "1710000000000");
    assert_request_target(&requests[0], "POST /api/v5/trade/cancel-all-after ");
    assert_request_json(
        &requests[0],
        serde_json::json!({
            "timeOut": "0",
            "tag": OKX_CANCEL_ALL_AFTER_TAG,
        }),
    );
}

#[tokio::test]
async fn cancel_all_after_disarm_rejects_nonzero_trigger_time() {
    let server = TestServer::spawn(vec![cancel_all_after_ack_body(
        "1710000010000",
        "1710000000000",
    )])
    .await
    .expect("test server should start");
    let client = test_client(server.addr()).expect("test client should build");

    let error = client
        .cancel_all_after(OkxCancelAllAfterTimeout::disarm())
        .await
        .expect_err("cancel-all-after disarm should require disabled trigger time");
    let requests = server
        .await_requests()
        .await
        .expect("server should serve requests");

    assert!(
        error.to_string().contains("expected 0"),
        "bad cancel-all-after disarm acknowledgement should report triggerTime mismatch: {error}"
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn cancel_all_after_rejects_empty_or_incomplete_acknowledgements() {
    let cases = [
        (
            okx_data_body("[]"),
            "OKX returned 0 cancel-all-after acknowledgements",
        ),
        (
            cancel_all_after_ack_body("", "1710000000000"),
            "omitted triggerTime",
        ),
        (cancel_all_after_ack_body("1710000010000", ""), "omitted ts"),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .cancel_all_after(
                OkxCancelAllAfterTimeout::new(OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS)
                    .expect("test timeout should be valid"),
            )
            .await
            .expect_err("cancel-all-after should fail closed on bad OKX acknowledgements");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "bad cancel-all-after acknowledgement should report the mismatch: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn cancel_all_after_rejects_mismatched_or_missing_tag_acknowledgements() {
    let cases = [
        (
            okx_data_body(r#"[{"triggerTime":"1710000010000","ts":"1710000000000"}]"#),
            "returned tag ; expected okxrusttrading",
        ),
        (
            okx_data_body(
                r#"[{"triggerTime":"1710000010000","tag":"manual","ts":"1710000000000"}]"#,
            ),
            "returned tag manual; expected okxrusttrading",
        ),
    ];

    for (body, expected) in cases {
        let server = TestServer::spawn(vec![body])
            .await
            .expect("test server should start");
        let client = test_client(server.addr()).expect("test client should build");

        let error = client
            .cancel_all_after(
                OkxCancelAllAfterTimeout::new(OKX_CANCEL_ALL_AFTER_MAX_TIMEOUT_SECS)
                    .expect("test timeout should be valid"),
            )
            .await
            .expect_err("cancel-all-after should require the acknowledged tag");
        let requests = server
            .await_requests()
            .await
            .expect("server should serve requests");

        assert!(
            error.to_string().contains(expected),
            "bad cancel-all-after tag acknowledgement should report the mismatch: {error}"
        );
        assert_eq!(requests.len(), 1);
    }
}

fn test_client(addr: SocketAddr) -> anyhow::Result<OkxRestClient> {
    let client = OkxRestClient::new(
        &test_okx_config(format!("http://{addr}")),
        /*simulated_trading*/ false,
    )?;
    seed_local_server_time(&client);
    seed_btc_usdt_trade_quote_currency(&client);
    Ok(client)
}

fn unsynced_test_client(addr: SocketAddr) -> anyhow::Result<OkxRestClient> {
    OkxRestClient::new(
        &test_okx_config(format!("http://{addr}")),
        /*simulated_trading*/ false,
    )
}

fn test_client_with_websocket_max_staleness(
    addr: SocketAddr,
    max_staleness_ms: u64,
) -> anyhow::Result<OkxRestClient> {
    let mut config = test_okx_config(format!("http://{addr}"));
    config.websocket.max_staleness_ms = max_staleness_ms;
    let client = OkxRestClient::new(&config, /*simulated_trading*/ false)?;
    seed_local_server_time(&client);
    seed_btc_usdt_trade_quote_currency(&client);
    Ok(client)
}

fn seed_btc_usdt_trade_quote_currency(client: &OkxRestClient) {
    client
        .remember_account_spot_trade_quote_currency("BTC-USDT", "USDT")
        .expect("test account trade quote currency should seed");
}

fn seed_validated_btc_usdt(client: &OkxRestClient) -> Result<()> {
    let instrument = serde_json::from_str::<OkxInstrument>(&instrument_body_data("BTC-USDT"))
        .context("test instrument should parse")?;
    let validated = Arc::new(ValidatedTradingInstrument::from_test_instrument(
        instrument,
    )?);
    client.remember_validated_trading_instrument(validated)
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

fn seed_expired_server_time(client: &OkxRestClient) {
    *client
        .server_time_clock
        .state
        .lock()
        .expect("server time test clock should lock") = Some(ServerTimeSnapshot {
        offset_millis: 0,
        measured_at: Instant::now() - OKX_SERVER_TIME_TTL - Duration::from_secs(1),
    });
}

async fn assert_open_orders_rate_limit_pacer_blocked(client: &OkxRestClient) -> Result<()> {
    let bucket = okx_rate_limit_bucket(
        &Method::GET,
        "/api/v5/trade/orders-pending",
        Some("instType=SPOT&instId=BTC-USDT&limit=100"),
        None,
    )?;
    time::timeout(
        Duration::from_millis(25),
        client.rate_limit_pacer.wait(&bucket),
    )
    .await
    .expect_err("HTTP 429 should activate the open-orders rate-limit cooldown");
    Ok(())
}

fn oversized_okx_body(secret: &str) -> String {
    let padding = "x".repeat(OKX_REST_MAX_RESPONSE_BODY_BYTES);
    format!(r#"{{"code":"0","msg":"","data":[],"secret":"{secret}","padding":"{padding}"}}"#)
}

async fn spawn_raw_http_response(
    status: u16,
    reason: &'static str,
    content_length: Option<usize>,
    body: Option<String>,
    hold_open: Duration,
) -> anyhow::Result<(SocketAddr, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let mut stream = accept_raw_test_http_connection(&listener).await?;
        let request = read_raw_test_http_request(&mut stream).await?;

        let mut response =
            format!("HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n");
        if let Some(content_length) = content_length {
            response.push_str(&format!("Content-Length: {content_length}\r\n"));
        }
        response.push_str("Connection: close\r\n\r\n");
        write_raw_test_http_response(&mut stream, response.as_bytes()).await?;
        if let Some(body) = body {
            let _ = time::timeout(TEST_HTTP_TIMEOUT, stream.write_all(body.as_bytes())).await;
        }
        if !hold_open.is_zero() {
            time::sleep(hold_open).await;
        }
        Ok(vec![String::from_utf8_lossy(&request).to_string()])
    });
    Ok((addr, handle))
}

async fn await_raw_http_requests(handle: JoinHandle<Result<Vec<String>>>) -> Result<Vec<String>> {
    let mut handle = handle;
    let join_result = match time::timeout(TEST_HTTP_JOIN_TIMEOUT, &mut handle).await {
        Ok(join_result) => join_result,
        Err(error) => {
            handle.abort();
            let _ = handle.await;
            return Err(error).context("timed out waiting for raw test HTTP server task");
        }
    };
    join_result.context("raw test HTTP server task panicked")?
}

async fn accept_raw_test_http_connection(listener: &TcpListener) -> Result<TcpStream> {
    let (stream, _) = time::timeout(TEST_HTTP_TIMEOUT, listener.accept())
        .await
        .context("timed out accepting raw test HTTP connection")??;
    Ok(stream)
}

async fn read_raw_test_http_request(stream: &mut TcpStream) -> Result<Vec<u8>> {
    time::timeout(TEST_HTTP_TIMEOUT, async {
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let bytes_read = stream.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..bytes_read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        Ok(request)
    })
    .await
    .context("timed out reading raw test HTTP request")?
}

async fn write_raw_test_http_response(stream: &mut TcpStream, response: &[u8]) -> Result<()> {
    time::timeout(TEST_HTTP_TIMEOUT, stream.write_all(response))
        .await
        .context("timed out writing raw test HTTP response")??;
    Ok(())
}

struct RoutedHttpTestServer {
    addr: SocketAddr,
    requests: JoinHandle<anyhow::Result<Vec<String>>>,
}

impl RoutedHttpTestServer {
    async fn spawn(responses: Vec<RoutedResponse>) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let requests = tokio::spawn(async move {
            let mut responses = responses;
            let mut requests = Vec::new();
            while !responses.is_empty() {
                let (mut stream, _) = time::timeout(TEST_HTTP_TIMEOUT, listener.accept())
                    .await
                    .context("timed out accepting routed test HTTP request")??;
                let request = read_routed_http_request(&mut stream).await?;
                let request_line = request.lines().next().unwrap_or_default();
                let Some(response_index) = responses
                    .iter()
                    .position(|response| request_line.starts_with(response.target_prefix))
                else {
                    anyhow::bail!("no routed test HTTP response matched {request_line}");
                };
                let response = responses.remove(response_index);
                write_json_http_response(&mut stream, &response.body).await?;
                requests.push(request);
            }
            Ok(requests)
        });
        Ok(Self { addr, requests })
    }

    const fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn await_requests(self) -> Result<Vec<String>> {
        await_raw_http_requests(self.requests).await
    }
}

struct RoutedResponse {
    target_prefix: &'static str,
    body: String,
}

impl RoutedResponse {
    fn new(target_prefix: &'static str, body: String) -> Self {
        Self {
            target_prefix,
            body,
        }
    }
}

async fn read_routed_http_request(stream: &mut TcpStream) -> anyhow::Result<String> {
    let mut request = Vec::new();
    let mut header_end = None;
    loop {
        let mut buffer = [0; 1024];
        let bytes_read = time::timeout(TEST_WEBSOCKET_TIMEOUT, stream.read(&mut buffer))
            .await
            .context("timed out reading routed test HTTP request")??;
        if bytes_read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..bytes_read]);
        if header_end.is_none() {
            header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4);
        }
        let Some(header_end) = header_end else {
            continue;
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if request.len() >= header_end + content_length {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&request).to_string())
}

async fn write_json_http_response(stream: &mut TcpStream, body: &str) -> anyhow::Result<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    time::timeout(
        TEST_WEBSOCKET_TIMEOUT,
        stream.write_all(response.as_bytes()),
    )
    .await
    .context("timed out writing routed test HTTP response")??;
    Ok(())
}

fn websocket_command_test_client(
    rest_addr: SocketAddr,
    websocket_url: String,
    ack_timeout: Duration,
) -> anyhow::Result<OkxTradingClient> {
    let rest = test_client(rest_addr)?;
    Ok(OkxTradingClient::new(
        rest,
        Some(OkxWebsocketTradingCommandConfig::with_ack_timeout(
            websocket_url,
            OkxWebsocketTradingCommandCredentials::new(
                "key".to_owned(),
                "secret".to_owned(),
                "passphrase".to_owned(),
            )?,
            ack_timeout,
        )?),
    ))
}

fn websocket_command_test_client_with_validated_instrument(
    rest_addr: SocketAddr,
    websocket_url: String,
    ack_timeout: Duration,
) -> anyhow::Result<OkxTradingClient> {
    let rest = test_client(rest_addr)?;
    seed_validated_btc_usdt(&rest)?;
    Ok(OkxTradingClient::new(
        rest,
        Some(OkxWebsocketTradingCommandConfig::with_ack_timeout(
            websocket_url,
            OkxWebsocketTradingCommandCredentials::new(
                "key".to_owned(),
                "secret".to_owned(),
                "passphrase".to_owned(),
            )?,
            ack_timeout,
        )?),
    ))
}

async fn spawn_websocket_command_server_that_closes()
-> anyhow::Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(&listener).await?;
        let mut received = Vec::new();

        received.push(next_websocket_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_websocket_text(&mut websocket).await?);

        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_websocket_command_server_that_acks()
-> anyhow::Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(&listener).await?;
        let mut received = Vec::new();

        received.push(next_websocket_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        let command = next_websocket_text(&mut websocket).await?;
        websocket
            .send(Message::Text(websocket_command_ack(&command)?.into()))
            .await?;
        received.push(command);

        Ok(received)
    });
    Ok((url, handle))
}

enum FirstWebsocketCommandConnection {
    CloseBeforeLoginAck,
    CloseAfterLoginAck,
}

async fn spawn_recovering_websocket_command_server(
    first_connection: FirstWebsocketCommandConnection,
) -> anyhow::Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut received = Vec::new();

        let mut websocket = accept_test_websocket(&listener).await?;
        received.push(next_websocket_text(&mut websocket).await?);
        match first_connection {
            FirstWebsocketCommandConnection::CloseBeforeLoginAck => {}
            FirstWebsocketCommandConnection::CloseAfterLoginAck => {
                websocket
                    .send(Message::Text(
                        r#"{"event":"login","code":"0","msg":""}"#.into(),
                    ))
                    .await?;
            }
        }
        drop(websocket);

        let mut websocket = accept_test_websocket(&listener).await?;
        received.push(next_websocket_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        let command = next_websocket_text(&mut websocket).await?;
        websocket
            .send(Message::Text(websocket_command_ack(&command)?.into()))
            .await?;
        received.push(command);

        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_websocket_command_server_that_closes_after_login()
-> anyhow::Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(&listener).await?;
        let received = vec![next_websocket_text(&mut websocket).await?];
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;

        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_websocket_command_server_without_login_ack()
-> anyhow::Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(&listener).await?;
        let received = vec![next_websocket_text(&mut websocket).await?];
        time::sleep(Duration::from_secs(10)).await;

        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_websocket_command_server_with_command_response(
    response: String,
) -> anyhow::Result<(String, JoinHandle<Result<Vec<String>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(&listener).await?;
        let mut received = Vec::new();

        received.push(next_websocket_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_websocket_text(&mut websocket).await?);
        websocket.send(Message::Text(response.into())).await?;

        Ok(received)
    });
    Ok((url, handle))
}

async fn spawn_websocket_command_server_that_stalls_after_first_command(
    stall_for: Duration,
) -> anyhow::Result<(
    String,
    JoinHandle<Result<Vec<String>>>,
    oneshot::Receiver<()>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let (first_command_tx, first_command_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut websocket = accept_test_websocket(&listener).await?;
        let mut received = Vec::new();

        received.push(next_websocket_text(&mut websocket).await?);
        websocket
            .send(Message::Text(
                r#"{"event":"login","code":"0","msg":""}"#.into(),
            ))
            .await?;
        received.push(next_websocket_text(&mut websocket).await?);
        let _ = first_command_tx.send(());
        time::sleep(stall_for).await;

        Ok(received)
    });
    Ok((url, handle, first_command_rx))
}

async fn accept_test_websocket(
    listener: &TcpListener,
) -> anyhow::Result<tokio_tungstenite::WebSocketStream<TcpStream>> {
    let (stream, _) = time::timeout(TEST_WEBSOCKET_TIMEOUT, listener.accept())
        .await
        .context("timed out accepting test WebSocket TCP connection")??;
    time::timeout(TEST_WEBSOCKET_TIMEOUT, accept_async(stream))
        .await
        .context("timed out accepting test WebSocket handshake")?
        .context("failed accepting test WebSocket handshake")
}

async fn await_websocket_command_server(
    handle: JoinHandle<Result<Vec<String>>>,
) -> anyhow::Result<Vec<String>> {
    time::timeout(TEST_WEBSOCKET_TIMEOUT, handle)
        .await
        .context("timed out waiting for test WebSocket command server task")?
        .context("test WebSocket command server task panicked")?
}

fn websocket_command_ack(command: &str) -> anyhow::Result<String> {
    let command = serde_json::from_str::<serde_json::Value>(command)
        .context("test WebSocket command should parse")?;
    let id = command
        .get("id")
        .and_then(serde_json::Value::as_str)
        .context("test WebSocket command should include id")?;
    let op = command
        .get("op")
        .and_then(serde_json::Value::as_str)
        .context("test WebSocket command should include op")?;
    let client_order_id = command
        .get("args")
        .and_then(serde_json::Value::as_array)
        .and_then(|args| args.first())
        .and_then(|arg| arg.get("clOrdId"))
        .and_then(serde_json::Value::as_str)
        .context("test WebSocket command should include clOrdId")?;
    let request_id = command
        .get("args")
        .and_then(serde_json::Value::as_array)
        .and_then(|args| args.first())
        .and_then(|arg| arg.get("reqId"))
        .and_then(serde_json::Value::as_str);
    let request_id_field = request_id
        .map(|request_id| format!(r#", "reqId": "{request_id}""#))
        .unwrap_or_default();

    Ok(format!(
        r#"{{
            "id": "{id}",
            "op": "{op}",
            "code": "0",
            "msg": "",
            "data": [{{
                "ordId": "ord-{client_order_id}",
                "clOrdId": "{client_order_id}"{request_id_field},
                "sCode": "0",
                "sMsg": ""
            }}]
        }}"#
    ))
}

fn websocket_command_has_exp_time(command: &str) -> bool {
    let Some(value) = serde_json::from_str::<serde_json::Value>(command).ok() else {
        return false;
    };
    value
        .get("expTime")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|exp_time| !exp_time.is_empty())
}

async fn next_websocket_text(
    websocket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
) -> anyhow::Result<String> {
    loop {
        let message = time::timeout(TEST_WEBSOCKET_TIMEOUT, websocket.next())
            .await
            .context("timed out waiting for test WebSocket client text frame")?
            .context("test WebSocket closed before text frame")??;
        if let Message::Text(payload) = message {
            return Ok(payload.to_string());
        }
    }
}

fn json_value(payload: &str) -> anyhow::Result<serde_json::Value> {
    serde_json::from_str(payload).context("payload should be JSON")
}

fn test_okx_config(base_url: impl Into<String>) -> OkxConfig {
    OkxConfig {
        api_key: "key".to_owned().into(),
        api_secret: "secret".to_owned().into(),
        api_passphrase: "passphrase".to_owned().into(),
        account_id: "OKX-test".to_owned(),
        api_domain: OkxApiDomain::Global,
        account_jurisdiction: OkxAccountJurisdiction::Singapore,
        trading_service: OkxTradingService::Production,
        base_url: base_url.into(),
        base_url_ws_public: None,
        base_url_ws_private: None,
        base_url_ws_business: None,
        proxy_url: None,
        request_timeout_ms: 1_000,
        websocket: OkxWebsocketConfig::default(),
    }
}

fn okx_data_body(data: &str) -> String {
    format!(r#"{{"code":"0","msg":"","data":{data}}}"#)
}

fn index_ticker_body(quote_ccy: &str, index_price: &str, timestamp_ms: i128) -> String {
    okx_data_body(&format!(
        r#"[{{"instId":"{quote_ccy}-USD","idxPx":"{index_price}","ts":"{timestamp_ms}"}}]"#
    ))
}

fn price_limit_body(
    inst_type: &str,
    inst_id: &str,
    buy_limit: &str,
    sell_limit: &str,
    timestamp_ms: i128,
    enabled: bool,
) -> String {
    okx_data_body(&format!(
        "[{}]",
        price_limit_row(
            inst_type,
            inst_id,
            buy_limit,
            sell_limit,
            timestamp_ms,
            enabled
        )
    ))
}

fn price_limit_row(
    inst_type: &str,
    inst_id: &str,
    buy_limit: &str,
    sell_limit: &str,
    timestamp_ms: i128,
    enabled: bool,
) -> String {
    format!(
        r#"{{"instType":"{inst_type}","instId":"{inst_id}","buyLmt":"{buy_limit}","sellLmt":"{sell_limit}","ts":"{timestamp_ms}","enabled":{enabled}}}"#
    )
}

fn okx_server_time_body(timestamp: &str) -> String {
    okx_data_body(&format!(r#"[{{"ts":"{timestamp}"}}]"#))
}

fn okx_endpoint_timeout_body() -> String {
    r#"{"code":"50004","msg":"API endpoint request timeout. Please check the request result.","data":[]}"#
        .to_owned()
}

fn account_config_body(account_level: &str, permissions: &str, auto_loan: bool) -> String {
    okx_data_body(&format!(
        "[{}]",
        account_config_json(account_level, permissions, auto_loan)
    ))
}

fn account_config_json(account_level: &str, permissions: &str, auto_loan: bool) -> String {
    format!(
        r#"{{"uid":"1001","mainUid":"1001","acctLv":"{account_level}","perm":"{permissions}","autoLoan":{auto_loan},"enableSpotBorrow":false,"spotBorrowAutoRepay":false,"feeType":"0"}}"#
    )
}

fn assert_error_chain_contains(error: &anyhow::Error, expected: &str) {
    assert!(
        error
            .chain()
            .any(|source| source.to_string().contains(expected)),
        "error chain should contain {expected:?}: {error:?}"
    );
}

fn trade_fee_body(inst_type: &str, maker: &str, taker: &str) -> String {
    okx_data_body(&format!("[{}]", trade_fee_json(inst_type, maker, taker)))
}

fn trade_fee_json(inst_type: &str, maker: &str, taker: &str) -> String {
    format!(
        r#"{{"instType":"{inst_type}","level":"Lv1","maker":"{maker}","taker":"{taker}","feeGroup":[{{"groupId":"12","maker":"{maker}","taker":"{taker}"}}],"ts":"1763979985847"}}"#
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
        r#"[{{"instType":"SPOT","instId":"{inst_id}","instIdCode":"123456","groupId":"12","state":"live","baseCcy":"{base_ccy}","quoteCcy":"{quote_ccy}","tradeQuoteCcyList":["{quote_ccy}"],"tickSz":"{tick_size}","lotSz":"{lot_size}","minSz":"{min_size}","maxLmtSz":"999","maxLmtAmt":"100000","maxMktSz":"100","maxMktAmt":"100000","maxTriggerSz":"999","initPxLmtPct":"0.05","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}}]"#
    ))
}

fn instrument_body_data(inst_id: &str) -> String {
    format!(
        r#"{{"instType":"SPOT","instId":"{inst_id}","instIdCode":"123456","groupId":"12","state":"live","baseCcy":"BTC","quoteCcy":"USDT","tradeQuoteCcyList":["USDT"],"tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","maxLmtSz":"999","maxLmtAmt":"100000","maxMktSz":"100","maxMktAmt":"100000","maxTriggerSz":"999","initPxLmtPct":"0.05","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}}"#
    )
}

fn cancel_all_after_ack_body(trigger_time: &str, ts: &str) -> String {
    okx_data_body(&format!(
        r#"[{{"triggerTime":"{trigger_time}","tag":"{OKX_CANCEL_ALL_AFTER_TAG}","ts":"{ts}"}}]"#
    ))
}

fn assert_request_target(request: &str, expected_prefix: &str) {
    assert!(
        request.starts_with(expected_prefix),
        "request used unexpected target; expected prefix {expected_prefix:?}: {request}"
    );
}

fn assert_request_json(request: &str, expected: serde_json::Value) {
    let body = request
        .split("\r\n\r\n")
        .nth(1)
        .expect("HTTP request should contain a body");
    let actual = serde_json::from_str::<serde_json::Value>(body)
        .unwrap_or_else(|error| panic!("request body should be JSON: {body}: {error}"));

    assert_eq!(actual, expected);
}

fn assert_order_exp_time_header(request: &str) {
    let exp_time = request_header(request, OKX_ORDER_EXP_TIME)
        .unwrap_or_else(|| panic!("{OKX_ORDER_EXP_TIME} header should be present: {request}"))
        .parse::<i128>()
        .unwrap_or_else(|error| {
            panic!("{OKX_ORDER_EXP_TIME} header should be milliseconds: {error}")
        });
    let signing_millis = exp_time
        .checked_sub(OKX_ORDER_EXPIRY_WINDOW_MS)
        .expect("expTime should be after the signing timestamp");
    let expected_timestamp =
        format_okx_timestamp(signing_millis).expect("signing timestamp should format");
    assert_eq!(
        request_header(request, OKX_API_TIMESTAMP),
        Some(expected_timestamp.as_str())
    );
}

fn request_header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request
        .lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .find_map(|line| {
            let (header_name, value) = line.split_once(':')?;
            header_name
                .eq_ignore_ascii_case(name)
                .then_some(value.trim())
        })
}

fn order_list_body(ids: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    let orders = ids
        .into_iter()
        .map(|id| {
            let order_id = id.as_ref();
            order_json(
                "BTC-USDT",
                order_id,
                &format!("client-{order_id}"),
                "filled",
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    okx_data_body(&format!("[{orders}]"))
}

fn order_body(inst_id: &str, order_id: &str, client_order_id: &str, state: &str) -> String {
    order_body_with_shape(
        inst_id,
        order_id,
        client_order_id,
        state,
        "buy",
        "post_only",
    )
}

fn order_body_with_shape(
    inst_id: &str,
    order_id: &str,
    client_order_id: &str,
    state: &str,
    side: &str,
    order_type: &str,
) -> String {
    okx_data_body(&format!(
        "[{}]",
        order_json_with_shape(inst_id, order_id, client_order_id, state, side, order_type)
    ))
}

fn order_body_with_amended_shape(
    inst_id: &str,
    order_id: &str,
    client_order_id: &str,
    size: &str,
    price: &str,
) -> String {
    okx_data_body(&format!(
        r#"[{{"instType":"SPOT","instId":"{inst_id}","ordId":"{order_id}","clOrdId":"{client_order_id}","side":"buy","ordType":"post_only","state":"live","px":"{price}","avgPx":"100","accFillSz":"0.001","sz":"{size}"}}]"#
    ))
}

fn order_json(inst_id: &str, order_id: &str, client_order_id: &str, state: &str) -> String {
    order_json_with_shape(
        inst_id,
        order_id,
        client_order_id,
        state,
        "buy",
        "post_only",
    )
}

fn order_json_with_shape(
    inst_id: &str,
    order_id: &str,
    client_order_id: &str,
    state: &str,
    side: &str,
    order_type: &str,
) -> String {
    format!(
        r#"{{"instType":"SPOT","instId":"{inst_id}","ordId":"{order_id}","clOrdId":"{client_order_id}","side":"{side}","ordType":"{order_type}","state":"{state}","avgPx":"100","accFillSz":"0.001","sz":"0.001"}}"#
    )
}

fn order_ack_body(order_id: &str, client_order_id: &str) -> String {
    order_ack_status_body(order_id, client_order_id, "0", "")
}

fn order_ack_status_body(
    order_id: &str,
    client_order_id: &str,
    status_code: &str,
    status_message: &str,
) -> String {
    okx_data_body(&format!(
        r#"[{{"ordId":"{order_id}","clOrdId":"{client_order_id}","sCode":"{status_code}","sMsg":"{status_message}"}}]"#
    ))
}

fn algo_list_body(ids: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    let orders = ids
        .into_iter()
        .map(|id| {
            let algo_id = id.as_ref();
            algo_json("BTC-USDT", algo_id, &format!("client-{algo_id}"), "live")
        })
        .collect::<Vec<_>>()
        .join(",");
    okx_data_body(&format!("[{orders}]"))
}

fn algo_body(inst_id: &str, algo_id: &str, client_order_id: &str, state: &str) -> String {
    algo_body_with_shape(
        inst_id,
        algo_id,
        client_order_id,
        state,
        "sell",
        "trigger",
        "-1",
    )
}

fn algo_body_with_shape(
    inst_id: &str,
    algo_id: &str,
    client_order_id: &str,
    state: &str,
    side: &str,
    order_type: &str,
    order_price: &str,
) -> String {
    okx_data_body(&format!(
        "[{}]",
        algo_json_with_shape(
            inst_id,
            algo_id,
            client_order_id,
            state,
            side,
            order_type,
            order_price
        )
    ))
}

#[derive(Clone, Copy)]
struct AlgoOrderShape<'a> {
    side: &'a str,
    order_type: &'a str,
    order_price: &'a str,
    trigger_price: &'a str,
    size: &'a str,
}

fn algo_body_with_order_shape(
    inst_id: &str,
    algo_id: &str,
    client_order_id: &str,
    state: &str,
    shape: AlgoOrderShape<'_>,
) -> String {
    okx_data_body(&format!(
        "[{}]",
        algo_json_with_order_shape(inst_id, algo_id, client_order_id, state, shape,)
    ))
}

fn algo_json(inst_id: &str, algo_id: &str, client_order_id: &str, state: &str) -> String {
    algo_json_with_shape(
        inst_id,
        algo_id,
        client_order_id,
        state,
        "sell",
        "trigger",
        "-1",
    )
}

fn algo_json_with_shape(
    inst_id: &str,
    algo_id: &str,
    client_order_id: &str,
    state: &str,
    side: &str,
    order_type: &str,
    order_price: &str,
) -> String {
    algo_json_with_order_shape(
        inst_id,
        algo_id,
        client_order_id,
        state,
        AlgoOrderShape {
            side,
            order_type,
            order_price,
            trigger_price: "100",
            size: "0.001",
        },
    )
}

fn algo_json_with_order_shape(
    inst_id: &str,
    algo_id: &str,
    client_order_id: &str,
    state: &str,
    shape: AlgoOrderShape<'_>,
) -> String {
    let AlgoOrderShape {
        side,
        order_type,
        order_price,
        trigger_price,
        size,
    } = shape;
    format!(
        r#"{{"instType":"SPOT","instId":"{inst_id}","algoId":"{algo_id}","algoClOrdId":"{client_order_id}","side":"{side}","ordType":"{order_type}","orderPx":"{order_price}","state":"{state}","triggerPx":"{trigger_price}","sz":"{size}"}}"#
    )
}

fn algo_ack_body(algo_id: &str, client_order_id: &str) -> String {
    algo_ack_status_body(algo_id, client_order_id, "0", "")
}

fn algo_ack_status_body(
    algo_id: &str,
    client_order_id: &str,
    status_code: &str,
    status_message: &str,
) -> String {
    okx_data_body(&format!(
        r#"[{{"algoId":"{algo_id}","algoClOrdId":"{client_order_id}","sCode":"{status_code}","sMsg":"{status_message}"}}]"#
    ))
}

fn algo_mutation_error_body(data: &str, timing: Option<(&str, &str)>) -> String {
    let timing = timing
        .map(|(in_time, out_time)| format!(r#","inTime":"{in_time}","outTime":"{out_time}""#))
        .unwrap_or_default();
    format!(r#"{{"code":"1","msg":"All operations failed","data":{data}{timing}}}"#)
}

fn oco_body(
    inst_id: &str,
    algo_id: &str,
    client_order_id: &str,
    state: &str,
    actual_side: &str,
    actual_size: &str,
) -> String {
    okx_data_body(&format!(
        r#"[{{"instType":"SPOT","instId":"{inst_id}","algoId":"{algo_id}","algoClOrdId":"{client_order_id}","ordId":"ord-oco-1","side":"sell","ordType":"oco","state":"{state}","sz":"0.00002","tpTriggerPx":"110000","tpTriggerPxType":"last","tpOrdPx":"-1","slTriggerPx":"90000","slTriggerPxType":"last","slOrdPx":"-1","actualSide":"{actual_side}","actualSz":"{actual_size}","actualPx":"100000","tag":"okxrusttrading","cTime":"1","uTime":"2"}}]"#
    ))
}

fn candle_json(ts_ms: i64, close: &str) -> String {
    format!(r#"["{ts_ms}","100","105","95","{close}","1","1","1","1"]"#)
}

fn test_candle_hint(inst_id: &str, ts_ms: i64, close: f64) -> OkxMarketCandleHint {
    OkxMarketCandleHint {
        inst_id: inst_id.to_owned(),
        channel: OKX_PUBLIC_CANDLE_1M_CHANNEL.to_owned(),
        bar: market_bar(ts_ms, close),
        source_ts_ms: Some(ts_ms),
        received_at: Instant::now(),
    }
}

fn market_bar(ts_ms: i64, close: f64) -> MarketBar {
    MarketBar {
        ts_ms,
        open: close,
        high: close + 5.0,
        low: close - 5.0,
        close,
        confirm: true,
    }
}

fn rest_candle_bar(ts_ms: i64, close: f64) -> MarketBar {
    MarketBar {
        ts_ms,
        open: 100.0,
        high: 105.0,
        low: 95.0,
        close,
        confirm: true,
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

fn ticker_json_with_inst_type(
    inst_type: &str,
    inst_id: &str,
    bid_px: &str,
    ask_px: &str,
    last: &str,
) -> String {
    format!(
        r#"{{"instType":"{inst_type}","instId":"{inst_id}","bidPx":"{bid_px}","askPx":"{ask_px}","last":"{last}","ts":"{}"}}"#,
        current_unix_millis()
    )
}

fn requested_btc_usdt() -> RequestedTradingInstrument {
    RequestedTradingInstrument {
        instrument: RequestedInstrumentId::new("BTC-USDT".to_owned()).expect("instrument"),
        inst_type: RequestedInstrumentType::Spot,
        td_mode: RequestedTradeMode::Cash,
    }
}

fn cash_spot_account_config() -> OkxAccountConfig {
    OkxAccountConfig {
        uid: "1".to_owned(),
        main_uid: "1".to_owned(),
        account_level: "1".to_owned(),
        perm: "read_only,trade".to_owned(),
        auto_loan: false,
        enable_spot_borrow: false,
        spot_borrow_auto_repay: false,
        fee_type: "0".to_owned(),
        kyc_level: String::new(),
    }
}
