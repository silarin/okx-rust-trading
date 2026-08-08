use std::collections::{HashSet, VecDeque};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::okx::types::{OkxOrderAck, OrderKind, OrderSide};

const OKX_WEBSOCKET_ORDER_OP: &str = "order";
const OKX_WEBSOCKET_AMEND_ORDER_OP: &str = "amend-order";
const OKX_WEBSOCKET_CANCEL_ORDER_OP: &str = "cancel-order";
const OKX_WEBSOCKET_REQUEST_ID_MAX_LEN: usize = 32;
const OKX_CLIENT_ORDER_ID_MAX_LEN: usize = 32;
const OKX_ORDER_TAG_MAX_LEN: usize = 16;
const OKX_WEBSOCKET_ACK_TRACKER_MAX_IDS: usize = 256;
const OKX_PRICE_AMEND_TYPE_REJECT: &str = "0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxWebsocketOrderOperation {
    PlaceOrder,
    AmendOrder,
    CancelOrder,
}

impl OkxWebsocketOrderOperation {
    pub const fn as_okx(self) -> &'static str {
        match self {
            Self::PlaceOrder => OKX_WEBSOCKET_ORDER_OP,
            Self::AmendOrder => OKX_WEBSOCKET_AMEND_ORDER_OP,
            Self::CancelOrder => OKX_WEBSOCKET_CANCEL_ORDER_OP,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxWebsocketPlaceOrder<'a> {
    pub id: &'a str,
    pub inst_id_code: u64,
    pub exp_time: &'a str,
    pub side: OrderSide,
    pub kind: OrderKind,
    pub size: &'a str,
    pub price: Option<&'a str>,
    pub td_mode: &'static str,
    pub trade_quote_currency: &'a str,
    pub client_order_id: &'a str,
    pub tag: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxWebsocketAmendOrder<'a> {
    pub id: &'a str,
    pub inst_id_code: u64,
    pub exp_time: &'a str,
    pub client_order_id: &'a str,
    pub request_id: &'a str,
    pub new_size: Option<&'a str>,
    pub new_price: Option<&'a str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxWebsocketCancelOrder<'a> {
    pub id: &'a str,
    pub inst_id_code: u64,
    pub client_order_id: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OkxWebsocketExpectedOrderAck<'a> {
    pub id: &'a str,
    pub operation: OkxWebsocketOrderOperation,
    pub client_order_id: &'a str,
    pub request_id: Option<&'a str>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxWebsocketOrderCommandResponse {
    pub id: String,
    pub op: String,
    pub code: String,
    #[serde(default)]
    pub msg: String,
    #[serde(default)]
    pub data: Vec<OkxWebsocketOrderAckRow>,
    #[serde(rename = "inTime", default)]
    pub in_time: String,
    #[serde(rename = "outTime", default)]
    pub out_time: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OkxWebsocketOrderAckRow {
    #[serde(rename = "ordId")]
    pub order_id: String,
    #[serde(rename = "clOrdId")]
    pub client_order_id: String,
    #[serde(rename = "reqId", default)]
    pub request_id: String,
    #[serde(rename = "sCode")]
    pub status_code: String,
    #[serde(rename = "sMsg")]
    pub status_message: String,
    #[serde(rename = "subCode", default)]
    pub status_sub_code: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxWebsocketAckRecord {
    FirstSeen,
    Duplicate,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OkxWebsocketAckTracker {
    seen_request_ids: HashSet<String>,
    insertion_order: VecDeque<String>,
}

impl OkxWebsocketAckTracker {
    pub fn record(&mut self, request_id: &str) -> Result<OkxWebsocketAckRecord> {
        ensure_request_id(request_id, "OKX WebSocket acknowledgement id")?;
        if self.seen_request_ids.contains(request_id) {
            return Ok(OkxWebsocketAckRecord::Duplicate);
        }

        let request_id = request_id.to_owned();
        self.seen_request_ids.insert(request_id.clone());
        self.insertion_order.push_back(request_id);
        while self.insertion_order.len() > OKX_WEBSOCKET_ACK_TRACKER_MAX_IDS {
            if let Some(evicted_request_id) = self.insertion_order.pop_front() {
                self.seen_request_ids.remove(&evicted_request_id);
            }
        }
        Ok(OkxWebsocketAckRecord::FirstSeen)
    }
}

pub fn place_order_command_json(request: OkxWebsocketPlaceOrder<'_>) -> Result<String> {
    ensure_request_id(request.id, "OKX WebSocket order id")?;
    ensure_inst_id_code(request.inst_id_code)?;
    ensure_order_exp_time(request.exp_time)?;
    ensure_client_order_id(request.client_order_id)?;
    ensure_trimmed_non_empty("OKX WebSocket order size", request.size)?;
    ensure_trimmed_non_empty(
        "OKX WebSocket order tradeQuoteCcy",
        request.trade_quote_currency,
    )?;
    ensure!(
        request.td_mode == "cash",
        "OKX WebSocket order tdMode must be validated cash for the current runtime"
    );
    ensure_order_tag(request.tag)?;

    ensure!(
        matches!(request.kind, OrderKind::Limit | OrderKind::PostOnly),
        "OKX WebSocket place order supports only limit or post-only orders"
    );
    let price = request
        .price
        .context("OKX WebSocket limit/post-only order requires px")?;
    ensure_trimmed_non_empty("OKX WebSocket order price", price)?;
    let arg = OkxWebsocketPlaceOrderArg {
        inst_id_code: request.inst_id_code,
        td_mode: request.td_mode,
        side: request.side.as_okx(),
        order_type: request.kind.as_okx(),
        sz: request.size,
        price,
        price_amend_type: OKX_PRICE_AMEND_TYPE_REJECT,
        trade_quote_currency: request.trade_quote_currency,
        tag: request.tag,
        client_order_id: request.client_order_id,
    };

    command_json(
        request.id,
        OkxWebsocketOrderOperation::PlaceOrder.as_okx(),
        OkxWebsocketCommandExpiry::ExpTime(request.exp_time),
        arg,
        "OKX WebSocket place order command",
    )
}

pub fn amend_order_command_json(request: OkxWebsocketAmendOrder<'_>) -> Result<String> {
    ensure_request_id(request.id, "OKX WebSocket amend order id")?;
    ensure_request_id(request.request_id, "OKX WebSocket amend request id")?;
    ensure_inst_id_code(request.inst_id_code)?;
    ensure_order_exp_time(request.exp_time)?;
    ensure_client_order_id(request.client_order_id)?;
    ensure!(
        request.new_size.is_some() || request.new_price.is_some(),
        "OKX WebSocket amend order requires newSz or newPx"
    );
    if let Some(new_size) = request.new_size {
        ensure_trimmed_non_empty("OKX WebSocket amend newSz", new_size)?;
    }
    if let Some(new_price) = request.new_price {
        ensure_trimmed_non_empty("OKX WebSocket amend newPx", new_price)?;
    }

    let arg = OkxWebsocketAmendOrderArg {
        inst_id_code: request.inst_id_code,
        client_order_id: request.client_order_id,
        request_id: request.request_id,
        new_size: request.new_size,
        new_price: request.new_price,
        price_amend_type: OKX_PRICE_AMEND_TYPE_REJECT,
    };

    command_json(
        request.id,
        OkxWebsocketOrderOperation::AmendOrder.as_okx(),
        OkxWebsocketCommandExpiry::ExpTime(request.exp_time),
        arg,
        "OKX WebSocket amend order command",
    )
}

pub fn cancel_order_command_json(request: OkxWebsocketCancelOrder<'_>) -> Result<String> {
    ensure_request_id(request.id, "OKX WebSocket cancel order id")?;
    ensure_inst_id_code(request.inst_id_code)?;
    ensure_client_order_id(request.client_order_id)?;

    let arg = OkxWebsocketCancelOrderArg {
        inst_id_code: request.inst_id_code,
        client_order_id: request.client_order_id,
    };

    command_json(
        request.id,
        OkxWebsocketOrderOperation::CancelOrder.as_okx(),
        OkxWebsocketCommandExpiry::None,
        arg,
        "OKX WebSocket cancel order command",
    )
}

pub fn parse_order_command_ack(
    payload: &str,
    expected: OkxWebsocketExpectedOrderAck<'_>,
) -> Result<OkxOrderAck> {
    ensure_request_id(expected.id, "OKX WebSocket expected acknowledgement id")?;
    ensure_client_order_id(expected.client_order_id)?;
    let mut response: OkxWebsocketOrderCommandResponse = serde_json::from_str(payload)
        .context("failed parsing OKX WebSocket order command acknowledgement")?;
    ensure!(
        response.id == expected.id,
        "OKX WebSocket {} acknowledgement id {} did not match requested {}",
        expected.operation.as_okx(),
        response.id,
        expected.id
    );
    ensure!(
        response.op == expected.operation.as_okx(),
        "OKX WebSocket acknowledgement op {} did not match expected {} for {}",
        response.op,
        expected.operation.as_okx(),
        expected.id
    );
    if response.code != "0" && response.data.is_empty() {
        ensure!(
            response.code == "0",
            "OKX WebSocket {} {} rejected: {} {}",
            expected.operation.as_okx(),
            expected.id,
            response.code,
            response.msg
        );
    }
    ensure!(
        response.data.len() == 1,
        "OKX WebSocket {} {} returned {} acknowledgement rows for {}",
        expected.operation.as_okx(),
        expected.id,
        response.data.len(),
        expected.client_order_id
    );

    let mut acknowledgement_row = response.data.remove(0);
    if let Some(request_id) = expected.request_id {
        ensure!(
            acknowledgement_row.request_id == request_id,
            "OKX WebSocket {} acknowledgement returned reqId {} for requested {}",
            expected.operation.as_okx(),
            acknowledgement_row.request_id,
            request_id
        );
    }
    if acknowledgement_row.client_order_id.trim().is_empty() {
        acknowledgement_row.client_order_id = expected.client_order_id.to_owned();
    } else {
        ensure!(
            acknowledgement_row.client_order_id == expected.client_order_id,
            "OKX WebSocket {} acknowledgement returned clOrdId {} for requested {}",
            expected.operation.as_okx(),
            acknowledgement_row.client_order_id,
            expected.client_order_id
        );
    }
    ensure!(
        acknowledgement_row.status_code == "0",
        "OKX WebSocket {} {} rejected: {} {} subCode={:?}",
        expected.operation.as_okx(),
        expected.client_order_id,
        acknowledgement_row.status_code,
        acknowledgement_row.status_message,
        acknowledgement_row.status_sub_code
    );
    ensure!(
        response.code == "0",
        "OKX WebSocket {} {} rejected: {} {}",
        expected.operation.as_okx(),
        expected.id,
        response.code,
        response.msg
    );
    ensure!(
        !acknowledgement_row.order_id.trim().is_empty(),
        "OKX WebSocket {} acknowledgement omitted ordId for {}",
        expected.operation.as_okx(),
        expected.client_order_id
    );
    let acknowledgement = OkxOrderAck {
        order_id: acknowledgement_row.order_id,
        client_order_id: acknowledgement_row.client_order_id,
        status_code: acknowledgement_row.status_code,
        status_message: acknowledgement_row.status_message,
        status_sub_code: acknowledgement_row.status_sub_code,
        timestamp: String::new(),
    };
    Ok(acknowledgement)
}

fn command_json<T: Serialize>(
    id: &str,
    op: &'static str,
    expiry: OkxWebsocketCommandExpiry<'_>,
    arg: T,
    context: &'static str,
) -> Result<String> {
    let exp_time = match expiry {
        OkxWebsocketCommandExpiry::None => None,
        OkxWebsocketCommandExpiry::ExpTime(exp_time) => Some(exp_time),
    };
    let request = OkxWebsocketCommand {
        id,
        op,
        exp_time,
        args: [arg],
    };
    serde_json::to_string(&request).with_context(|| format!("failed serializing {context}"))
}

fn ensure_inst_id_code(inst_id_code: u64) -> Result<()> {
    ensure!(
        inst_id_code > 0,
        "OKX WebSocket instIdCode must be positive"
    );
    Ok(())
}

fn ensure_request_id(value: &str, label: &str) -> Result<()> {
    ensure_trimmed_non_empty(label, value)?;
    ensure_ascii_alphanumeric(label, value)?;
    ensure!(
        value.len() <= OKX_WEBSOCKET_REQUEST_ID_MAX_LEN,
        "{label} must not exceed {OKX_WEBSOCKET_REQUEST_ID_MAX_LEN} characters"
    );
    Ok(())
}

fn ensure_client_order_id(value: &str) -> Result<()> {
    ensure_trimmed_non_empty("OKX WebSocket clOrdId", value)?;
    ensure_ascii_alphanumeric("OKX WebSocket clOrdId", value)?;
    ensure!(
        value.len() <= OKX_CLIENT_ORDER_ID_MAX_LEN,
        "OKX WebSocket clOrdId must not exceed {OKX_CLIENT_ORDER_ID_MAX_LEN} characters"
    );
    Ok(())
}

fn ensure_order_tag(value: &str) -> Result<()> {
    ensure_trimmed_non_empty("OKX WebSocket order tag", value)?;
    ensure!(
        value.len() <= OKX_ORDER_TAG_MAX_LEN,
        "OKX WebSocket order tag must not exceed {OKX_ORDER_TAG_MAX_LEN} characters"
    );
    Ok(())
}

fn ensure_order_exp_time(value: &str) -> Result<()> {
    ensure_trimmed_non_empty("OKX WebSocket order expTime", value)?;
    ensure!(
        value.bytes().all(|byte| byte.is_ascii_digit()),
        "OKX WebSocket order expTime must be an unsigned millisecond timestamp"
    );
    Ok(())
}

fn ensure_trimmed_non_empty(label: &str, value: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{label} must not be empty");
    ensure!(value == value.trim(), "{label} must be trimmed");
    Ok(())
}

fn ensure_ascii_alphanumeric(label: &str, value: &str) -> Result<()> {
    ensure!(
        value.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "{label} must be ASCII alphanumeric"
    );
    Ok(())
}

#[derive(Serialize)]
struct OkxWebsocketCommand<'a, T> {
    id: &'a str,
    op: &'static str,
    #[serde(rename = "expTime", skip_serializing_if = "Option::is_none")]
    exp_time: Option<&'a str>,
    args: [T; 1],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OkxWebsocketCommandExpiry<'a> {
    None,
    ExpTime(&'a str),
}

#[derive(Serialize)]
struct OkxWebsocketPlaceOrderArg<'a> {
    #[serde(rename = "instIdCode")]
    inst_id_code: u64,
    #[serde(rename = "tdMode")]
    td_mode: &'static str,
    side: &'static str,
    #[serde(rename = "ordType")]
    order_type: &'static str,
    sz: &'a str,
    #[serde(rename = "px")]
    price: &'a str,
    #[serde(rename = "pxAmendType")]
    price_amend_type: &'static str,
    #[serde(rename = "tradeQuoteCcy")]
    trade_quote_currency: &'a str,
    tag: &'a str,
    #[serde(rename = "clOrdId")]
    client_order_id: &'a str,
}

#[derive(Serialize)]
struct OkxWebsocketAmendOrderArg<'a> {
    #[serde(rename = "instIdCode")]
    inst_id_code: u64,
    #[serde(rename = "clOrdId")]
    client_order_id: &'a str,
    #[serde(rename = "reqId")]
    request_id: &'a str,
    #[serde(rename = "newSz", skip_serializing_if = "Option::is_none")]
    new_size: Option<&'a str>,
    #[serde(rename = "newPx", skip_serializing_if = "Option::is_none")]
    new_price: Option<&'a str>,
    #[serde(rename = "pxAmendType")]
    price_amend_type: &'static str,
}

#[derive(Serialize)]
struct OkxWebsocketCancelOrderArg<'a> {
    #[serde(rename = "instIdCode")]
    inst_id_code: u64,
    #[serde(rename = "clOrdId")]
    client_order_id: &'a str,
}

#[cfg(test)]
#[path = "trading_tests.rs"]
mod tests;
