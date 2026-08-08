use std::path::Path;

use anyhow::Result;

use crate::{
    config::{
        loader::load_config_path_with_secret_resolver,
        types::{OkxTradingService, RuntimeOrderIntent},
    },
    okx::trading_client::OkxTradingClient,
    test_support::HttpTestServer as TestServer,
};

use super::preflight_strategy_enabled_account;

#[tokio::test]
async fn strategy_enabled_startup_preflight_is_independent_of_documented_account_level()
-> Result<()> {
    for account_level in ["1", "2", "3", "4"] {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            account_config_body_with_kyc(
                account_level,
                "read_only,trade",
                /*auto_loan*/ false,
                "1",
            ),
            instrument_body("BTC-USDT", "BTC", "USDT"),
            instrument_body("BTC-USDT", "BTC", "USDT"),
            ticker_body("BTC-USDT", "100000"),
            index_ticker_body("USDT", "1"),
            maximum_order_size_body("BTC-USDT"),
            maximum_available_size_body("BTC-USDT"),
            balances_body(),
            trade_fee_body("SPOT", "-0.001", "-0.002"),
        ])
        .await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX")
            .base_url = format!("http://{}", server.addr());
        let client = OkxTradingClient::from_config(&config)?;

        preflight_strategy_enabled_account(&client, &config).await?;
        let requests = server.await_requests().await?;

        assert_eq!(requests.len(), 10, "acctLv {account_level}");
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "GET /api/v5/account/config ");
        assert_request_target(
            &requests[2],
            "GET /api/v5/public/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(
            &requests[3],
            "GET /api/v5/account/instruments?instType=SPOT&instId=BTC-USDT ",
        );
        assert_request_target(&requests[4], "GET /api/v5/market/ticker?instId=BTC-USDT ");
        assert_request_target(
            &requests[5],
            "GET /api/v5/market/index-tickers?instId=USDT-USD ",
        );
        assert_request_target(
            &requests[6],
            "GET /api/v5/account/max-size?instId=BTC-USDT&tdMode=cash&px=100000&tradeQuoteCcy=USDT ",
        );
        assert_request_target(
            &requests[7],
            "GET /api/v5/account/max-avail-size?instId=BTC-USDT&tdMode=cash&tradeQuoteCcy=USDT ",
        );
        assert_request_target(&requests[8], "GET /api/v5/account/balance ");
        assert_request_target(
            &requests[9],
            "GET /api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT ",
        );
    }
    Ok(())
}

#[tokio::test]
async fn strategy_enabled_production_rejects_invalid_kyc_before_instrument_requests() -> Result<()>
{
    for kyc_level in [
        None,
        Some(""),
        Some("0"),
        Some("1"),
        Some("4"),
        Some(" 2 "),
        Some("unknown"),
    ] {
        let server = TestServer::spawn(vec![
            okx_server_time_body("4102444810123"),
            account_config_body_with_optional_kyc(
                "2",
                "read_only,trade",
                /*auto_loan*/ false,
                kyc_level,
            ),
        ])
        .await?;
        let mut config = load_profile_config(
            "crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml",
        );
        let okx = config
            .okx
            .as_mut()
            .expect("demo profile should configure OKX");
        okx.trading_service = OkxTradingService::Production;
        okx.base_url = format!("http://{}", server.addr());
        config.runtime.order_intent = Some(RuntimeOrderIntent::LiveOkxSpotConfirmed);
        let client = OkxTradingClient::from_config(&config)?;

        let error = preflight_strategy_enabled_account(&client, &config)
            .await
            .expect_err("invalid Production KYC evidence should fail closed");
        let requests = server.await_requests().await?;

        assert!(
            error
                .to_string()
                .contains("Production order placement requires OKX kycLv 2 or 3"),
            "kycLv {kyc_level:?} should report the live order eligibility boundary: {error}"
        );
        assert_eq!(requests.len(), 2);
        assert_request_target(&requests[0], "GET /api/v5/public/time ");
        assert_request_target(&requests[1], "GET /api/v5/account/config ");
    }
    Ok(())
}

#[tokio::test]
async fn strategy_enabled_startup_preflight_rejects_fee_above_assumption() -> Result<()> {
    let server = TestServer::spawn(vec![
        okx_server_time_body("4102444810123"),
        account_config_body("1", "read_only,trade", /*auto_loan*/ false),
        instrument_body("BTC-USDT", "BTC", "USDT"),
        instrument_body("BTC-USDT", "BTC", "USDT"),
        ticker_body("BTC-USDT", "100000"),
        index_ticker_body("USDT", "1"),
        maximum_order_size_body("BTC-USDT"),
        maximum_available_size_body("BTC-USDT"),
        balances_body(),
        trade_fee_body_with_group("SPOT", "-0.0001", "-0.0001", "12", "-0.0015", "-0.002"),
    ])
    .await?;
    let mut config =
        load_profile_config("crates/okx-trading-runtime/tests/fixtures/demo-strategy-profile.toml");
    config
        .okx
        .as_mut()
        .expect("demo profile should configure OKX")
        .base_url = format!("http://{}", server.addr());
    let client = OkxTradingClient::from_config(&config)?;

    let error = preflight_strategy_enabled_account(&client, &config)
        .await
        .expect_err("fee above strategy assumption should fail closed");
    let requests = server.await_requests().await?;

    assert!(
        error
            .to_string()
            .contains("exceeds strategy maker fee assumption"),
        "fee assumption mismatch should be reported: {error}"
    );
    assert_eq!(requests.len(), 10);
    Ok(())
}

fn load_profile_config(path: &str) -> crate::config::types::BotConfig {
    load_config_path_with_secret_resolver(Path::new(path), test_secret_resolver)
        .expect("checked-in OKX profile should load")
}

fn test_secret_resolver(name: &str) -> Option<String> {
    match name {
        "OKX_API_KEY" => Some("demo-key".to_owned()),
        "OKX_API_SECRET" => Some("demo-secret".to_owned()),
        "OKX_API_PASSPHRASE" => Some("demo-passphrase".to_owned()),
        _ => None,
    }
}

fn okx_server_time_body(timestamp: &str) -> String {
    okx_data_body(&format!(r#"[{{"ts":"{timestamp}"}}]"#))
}

fn account_config_body(account_level: &str, permissions: &str, auto_loan: bool) -> String {
    account_config_body_with_optional_kyc(account_level, permissions, auto_loan, None)
}

fn account_config_body_with_kyc(
    account_level: &str,
    permissions: &str,
    auto_loan: bool,
    kyc_level: &str,
) -> String {
    account_config_body_with_optional_kyc(account_level, permissions, auto_loan, Some(kyc_level))
}

fn account_config_body_with_optional_kyc(
    account_level: &str,
    permissions: &str,
    auto_loan: bool,
    kyc_level: Option<&str>,
) -> String {
    let kyc_level = kyc_level
        .map(|value| format!(r#","kycLv":"{value}""#))
        .unwrap_or_default();
    okx_data_body(&format!(
        r#"[{{"uid":"1001","mainUid":"1001","acctLv":"{account_level}","perm":"{permissions}","autoLoan":{auto_loan},"enableSpotBorrow":false,"spotBorrowAutoRepay":false,"feeType":"0"{kyc_level}}}]"#
    ))
}

fn instrument_body(inst_id: &str, base_ccy: &str, quote_ccy: &str) -> String {
    okx_data_body(&format!(
        r#"[{{"instType":"SPOT","instId":"{inst_id}","instIdCode":"123456","groupId":"12","state":"live","baseCcy":"{base_ccy}","quoteCcy":"{quote_ccy}","tradeQuoteCcyList":["{quote_ccy}"],"tickSz":"0.1","lotSz":"0.0001","minSz":"0.0001","maxLmtSz":"999","maxLmtAmt":"100000","maxMktSz":"100","maxMktAmt":"100000","maxTriggerSz":"999","initPxLmtPct":"0.05","floatPxLmtPct":"0.03","maxPxLmtPct":"0.15"}}]"#
    ))
}

fn ticker_body(inst_id: &str, last: &str) -> String {
    okx_data_body(&format!(
        r#"[{{"instType":"SPOT","instId":"{inst_id}","last":"{last}","lastSz":"0.001","askPx":"100001","askSz":"1","bidPx":"99999","bidSz":"1","open24h":"99000","high24h":"101000","low24h":"98000","volCcy24h":"1000000","vol24h":"10","sodUtc0":"99000","sodUtc8":"99500","ts":"4102444810123"}}]"#
    ))
}

fn index_ticker_body(quote_ccy: &str, index_price: &str) -> String {
    okx_data_body(&format!(
        r#"[{{"instId":"{quote_ccy}-USD","idxPx":"{index_price}","ts":"4102444810123"}}]"#
    ))
}

fn maximum_order_size_body(inst_id: &str) -> String {
    okx_data_body(&format!(
        r#"[{{"instId":"{inst_id}","ccy":"BTC","maxBuy":"0.001","maxSell":"100"}}]"#
    ))
}

fn maximum_available_size_body(inst_id: &str) -> String {
    okx_data_body(&format!(
        r#"[{{"instId":"{inst_id}","availBuy":"100","availSell":"0.001"}}]"#
    ))
}

fn balances_body() -> String {
    okx_data_body(
        r#"[{"details":[{"ccy":"BTC","availBal":"0.001","cashBal":"0.001","frozenBal":"0"},{"ccy":"USDT","availBal":"100","cashBal":"100","frozenBal":"0"}]}]"#,
    )
}

fn trade_fee_body(inst_type: &str, maker: &str, taker: &str) -> String {
    trade_fee_body_with_group(inst_type, maker, taker, "12", maker, taker)
}

fn trade_fee_body_with_group(
    inst_type: &str,
    deprecated_maker: &str,
    deprecated_taker: &str,
    group_id: &str,
    maker: &str,
    taker: &str,
) -> String {
    okx_data_body(&format!(
        r#"[{{"instType":"{inst_type}","level":"Lv1","maker":"{deprecated_maker}","taker":"{deprecated_taker}","feeGroup":[{{"groupId":"{group_id}","maker":"{maker}","taker":"{taker}"}}],"ts":"1763979985847"}}]"#
    ))
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
