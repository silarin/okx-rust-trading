#![forbid(unsafe_code)]
//! Pure, deterministic validation of credential-free OKX public-market payloads.

use std::{collections::BTreeSet, error::Error, fmt, str::FromStr, sync::Arc};

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::value::RawValue;

pub const OKX_LEVEL2_BOOKS_CHANNEL: &str = "books";
pub const OKX_PUBLIC_TRADES_CHANNEL: &str = "trades";
pub const OKX_LEVEL2_MAX_DEPTH: usize = 400;
pub const OKX_SPOT_INSTRUMENT_ID_MAX_LEN: usize = 32;

/// A canonical OKX SPOT instrument identity.
///
/// This type validates syntax only. It does not establish venue eligibility,
/// fee status, liquidity, or trading authority. Clones share one immutable
/// allocation so consumers can attach identity to every event without copying
/// the instrument string.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OkxSpotInstrumentId(Arc<str>);

impl OkxSpotInstrumentId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OkxSpotInstrumentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("OkxSpotInstrumentId")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for OkxSpotInstrumentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for OkxSpotInstrumentId {
    type Error = OkxPublicProtocolError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_spot_instrument_id(value)?;
        Ok(Self(Arc::from(value)))
    }
}

impl FromStr for OkxSpotInstrumentId {
    type Err = OkxPublicProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl Serialize for OkxSpotInstrumentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for OkxSpotInstrumentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value.as_str()).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxLevel2Action {
    Snapshot,
    Update,
}

impl OkxLevel2Action {
    pub fn parse(value: &str) -> Result<Self, OkxPublicProtocolError> {
        match value {
            "snapshot" => Ok(Self::Snapshot),
            "update" => Ok(Self::Update),
            _ => Err(OkxPublicProtocolError::UnsupportedAction(value.to_owned())),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OkxLevel2Data {
    pub asks: Vec<OkxLevel2Level>,
    pub bids: Vec<OkxLevel2Level>,
    #[serde(deserialize_with = "positive_i64_string")]
    pub ts: i64,
    #[serde(rename = "seqId")]
    pub sequence_id: i64,
    #[serde(rename = "prevSeqId")]
    pub previous_sequence_id: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OkxLevel2Level {
    pub price: Decimal,
    pub quantity: Decimal,
    pub order_count: u64,
}

impl OkxLevel2Level {
    pub const fn new(price: Decimal, quantity: Decimal, order_count: u64) -> Self {
        Self {
            price,
            quantity,
            order_count,
        }
    }
}

impl<'de> Deserialize<'de> for OkxLevel2Level {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let row = Vec::<String>::deserialize(deserializer)?;
        if row.len() != 4 {
            return Err(de::Error::custom(
                "OKX books level must contain price, quantity, deprecated field, and order count",
            ));
        }
        let price = Decimal::from_str(&row[0])
            .map_err(|_| de::Error::custom("OKX books price must be a Decimal"))?;
        let quantity = Decimal::from_str(&row[1])
            .map_err(|_| de::Error::custom("OKX books quantity must be a Decimal"))?;
        if price <= Decimal::ZERO {
            return Err(de::Error::custom("OKX books price must be positive"));
        }
        if quantity < Decimal::ZERO {
            return Err(de::Error::custom("OKX books quantity must be non-negative"));
        }
        if row[2] != "0" {
            return Err(de::Error::custom(
                "OKX books deprecated level field must be zero",
            ));
        }
        let order_count = row[3]
            .parse::<u64>()
            .map_err(|_| de::Error::custom("OKX books order count must be an integer"))?;
        if quantity.is_zero() && order_count != 0 {
            return Err(de::Error::custom(
                "OKX books removal must have zero order count",
            ));
        }
        if quantity > Decimal::ZERO && order_count == 0 {
            return Err(de::Error::custom(
                "OKX books live level must have positive order count",
            ));
        }
        Ok(Self {
            price,
            quantity,
            order_count,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedBooksMessage {
    pub instrument_id: OkxSpotInstrumentId,
    pub action: OkxLevel2Action,
    pub data: OkxLevel2Data,
}

pub fn parse_books_message(
    expected_instrument: &OkxSpotInstrumentId,
    channel: &str,
    received_instrument: &str,
    action: &str,
    data_json: &str,
) -> Result<ValidatedBooksMessage, OkxPublicProtocolError> {
    if channel != OKX_LEVEL2_BOOKS_CHANNEL {
        return Err(OkxPublicProtocolError::UnsupportedChannel(
            channel.to_owned(),
        ));
    }
    if received_instrument != expected_instrument.as_str() {
        validate_spot_instrument_id(received_instrument)?;
        return Err(OkxPublicProtocolError::InstrumentMismatch {
            expected: expected_instrument.clone(),
            received: OkxSpotInstrumentId::try_from(received_instrument)
                .expect("received instrument was validated"),
        });
    }
    let action = OkxLevel2Action::parse(action)?;
    let mut rows = serde_json::from_str::<Vec<OkxLevel2Data>>(data_json)
        .map_err(|error| OkxPublicProtocolError::MalformedPayload(error.to_string()))?;
    if rows.len() != 1 {
        return Err(OkxPublicProtocolError::UnexpectedRowCount(rows.len()));
    }
    let message = ValidatedBooksMessage {
        instrument_id: expected_instrument.clone(),
        action,
        data: rows.remove(0),
    };
    reject_duplicate_book_levels(&message)?;
    Ok(message)
}

pub fn reject_duplicate_book_levels(
    message: &ValidatedBooksMessage,
) -> Result<(), OkxPublicProtocolError> {
    for levels in [&message.data.asks, &message.data.bids] {
        let mut prices = BTreeSet::new();
        for level in levels {
            if !prices.insert(level.price) {
                return Err(OkxPublicProtocolError::DuplicatePrice(level.price));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OkxTradeSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedPublicTrade {
    pub instrument_id: OkxSpotInstrumentId,
    pub terminal_trade_id: u64,
    pub timestamp_ms: i64,
    pub aggregation_count: u64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: OkxTradeSide,
}

pub fn parse_trade_message(
    expected_instrument: &OkxSpotInstrumentId,
    channel: &str,
    received_instrument: &str,
    data_json: &str,
) -> Result<Vec<ValidatedPublicTrade>, OkxPublicProtocolError> {
    if channel != OKX_PUBLIC_TRADES_CHANNEL {
        return Err(OkxPublicProtocolError::UnsupportedChannel(
            channel.to_owned(),
        ));
    }
    if received_instrument != expected_instrument.as_str() {
        validate_spot_instrument_id(received_instrument)?;
        return Err(OkxPublicProtocolError::InstrumentMismatch {
            expected: expected_instrument.clone(),
            received: OkxSpotInstrumentId::try_from(received_instrument)
                .expect("received instrument was validated"),
        });
    }
    let rows = serde_json::from_str::<Vec<PublicTradeRow<'_>>>(data_json)
        .map_err(|error| OkxPublicProtocolError::MalformedPayload(error.to_string()))?;
    rows.into_iter()
        .map(|row| validated_public_trade(expected_instrument, row))
        .collect()
}

#[derive(Deserialize)]
struct PublicTradeRow<'a> {
    #[serde(rename = "instId")]
    instrument_id: &'a str,
    #[serde(rename = "tradeId")]
    trade_id: &'a str,
    ts: &'a str,
    count: Option<&'a str>,
    px: &'a str,
    sz: &'a str,
    side: &'a str,
}

fn validated_public_trade(
    expected_instrument: &OkxSpotInstrumentId,
    row: PublicTradeRow<'_>,
) -> Result<ValidatedPublicTrade, OkxPublicProtocolError> {
    if row.instrument_id != expected_instrument.as_str() {
        validate_spot_instrument_id(row.instrument_id)?;
        return Err(OkxPublicProtocolError::MixedInstrumentPayload {
            expected: expected_instrument.clone(),
            received: OkxSpotInstrumentId::try_from(row.instrument_id)
                .expect("trade row instrument was validated"),
        });
    }
    let terminal_trade_id = positive_u64(row.trade_id, "tradeId")?;
    let timestamp_ms = positive_i64(row.ts, "trade ts")?;
    let aggregation_count = row
        .count
        .map(|value| positive_u64(value, "trade count"))
        .transpose()?
        .unwrap_or(1);
    let price = positive_decimal(row.px, "trade price")?;
    let quantity = positive_decimal(row.sz, "trade quantity")?;
    let side = match row.side {
        "buy" => OkxTradeSide::Buy,
        "sell" => OkxTradeSide::Sell,
        value => {
            return Err(OkxPublicProtocolError::UnsupportedTradeSide(
                value.to_owned(),
            ));
        }
    };
    Ok(ValidatedPublicTrade {
        instrument_id: expected_instrument.clone(),
        terminal_trade_id,
        timestamp_ms,
        aggregation_count,
        price,
        quantity,
        side,
    })
}

#[derive(Clone, Debug)]
pub struct OkxPublicTrade {
    pub inst_id: OkxSpotInstrumentId,
    pub trade_id: u64,
    pub timestamp_ms: i64,
    pub raw: Box<RawValue>,
}

impl PartialEq for OkxPublicTrade {
    fn eq(&self, other: &Self) -> bool {
        self.inst_id == other.inst_id
            && self.trade_id == other.trade_id
            && self.timestamp_ms == other.timestamp_ms
            && self.raw.get() == other.raw.get()
    }
}

impl Eq for OkxPublicTrade {}

impl<'de> Deserialize<'de> for OkxPublicTrade {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let fields: PublicTradeArchiveFields<'_> =
            serde_json::from_str(raw.get()).map_err(de::Error::custom)?;
        let trade_id = fields.trade_id.parse::<u64>().map_err(de::Error::custom)?;
        let timestamp_ms = fields
            .timestamp_ms
            .parse::<i64>()
            .map_err(de::Error::custom)?;
        let inst_id = OkxSpotInstrumentId::try_from(fields.inst_id).map_err(de::Error::custom)?;
        if trade_id == 0 {
            return Err(de::Error::custom(
                "OKX public trade tradeId must be positive",
            ));
        }
        if timestamp_ms <= 0 {
            return Err(de::Error::custom("OKX public trade ts must be positive"));
        }
        Ok(Self {
            inst_id,
            trade_id,
            timestamp_ms,
            raw,
        })
    }
}

#[derive(Deserialize)]
struct PublicTradeArchiveFields<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "tradeId")]
    trade_id: &'a str,
    #[serde(rename = "ts")]
    timestamp_ms: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxPublicProtocolError {
    UnsupportedAction(String),
    UnsupportedChannel(String),
    MalformedSpotInstrument {
        value: String,
        reason: &'static str,
    },
    InstrumentMismatch {
        expected: OkxSpotInstrumentId,
        received: OkxSpotInstrumentId,
    },
    MixedInstrumentPayload {
        expected: OkxSpotInstrumentId,
        received: OkxSpotInstrumentId,
    },
    UnsupportedTradeSide(String),
    MalformedPayload(String),
    UnexpectedRowCount(usize),
    DuplicatePrice(Decimal),
    InvalidField(&'static str),
}

impl fmt::Display for OkxPublicProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedAction(value) => {
                write!(formatter, "unsupported OKX books action {value}")
            }
            Self::UnsupportedChannel(value) => {
                write!(formatter, "unsupported OKX public channel {value}")
            }
            Self::MalformedSpotInstrument { value, reason } => {
                write!(
                    formatter,
                    "malformed OKX SPOT instrument {value:?}: {reason}"
                )
            }
            Self::InstrumentMismatch { expected, received } => write!(
                formatter,
                "OKX public instrument mismatch: expected {expected}, received {received}"
            ),
            Self::MixedInstrumentPayload { expected, received } => write!(
                formatter,
                "OKX public payload mixed instruments: expected {expected}, received row for {received}"
            ),
            Self::UnsupportedTradeSide(value) => {
                write!(formatter, "unsupported OKX public trade side {value}")
            }
            Self::MalformedPayload(value) => {
                write!(formatter, "malformed OKX public payload: {value}")
            }
            Self::UnexpectedRowCount(count) => write!(
                formatter,
                "OKX public message contained {count} rows; expected exactly one"
            ),
            Self::DuplicatePrice(price) => write!(
                formatter,
                "OKX public books side contained duplicate price {price}"
            ),
            Self::InvalidField(field) => write!(formatter, "invalid OKX public {field}"),
        }
    }
}

impl Error for OkxPublicProtocolError {}

fn validate_spot_instrument_id(value: &str) -> Result<(), OkxPublicProtocolError> {
    let malformed = |reason| OkxPublicProtocolError::MalformedSpotInstrument {
        value: value.to_owned(),
        reason,
    };
    if value.is_empty() {
        return Err(malformed("identifier must not be empty"));
    }
    if value.len() > OKX_SPOT_INSTRUMENT_ID_MAX_LEN {
        return Err(malformed("identifier exceeds the 32-byte maximum"));
    }
    if !value.is_ascii() {
        return Err(malformed("identifier must contain ASCII characters only"));
    }
    if value.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(malformed("identifier must not contain whitespace"));
    }
    let mut components = value.split('-');
    let base = components.next().unwrap_or_default();
    let quote = components.next().unwrap_or_default();
    if components.next().is_some() || base.is_empty() || quote.is_empty() {
        return Err(malformed(
            "identifier must contain exactly one separator and two non-empty assets",
        ));
    }
    let valid_asset = |asset: &str| {
        asset
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    };
    if !valid_asset(base) || !valid_asset(quote) {
        return Err(malformed(
            "asset components must contain uppercase ASCII letters or digits only",
        ));
    }
    if base == quote {
        return Err(malformed("base and quote assets must differ"));
    }
    Ok(())
}

fn positive_i64_string<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let value = value
        .parse::<i64>()
        .map_err(|_| de::Error::custom("OKX books timestamp must be an integer string"))?;
    if value <= 0 {
        return Err(de::Error::custom("OKX books timestamp must be positive"));
    }
    Ok(value)
}

fn positive_u64(value: &str, field: &'static str) -> Result<u64, OkxPublicProtocolError> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OkxPublicProtocolError::InvalidField(field))
}

fn positive_i64(value: &str, field: &'static str) -> Result<i64, OkxPublicProtocolError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(OkxPublicProtocolError::InvalidField(field))
}

fn positive_decimal(value: &str, field: &'static str) -> Result<Decimal, OkxPublicProtocolError> {
    Decimal::from_str(value)
        .ok()
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(OkxPublicProtocolError::InvalidField(field))
}

#[cfg(test)]
mod tests {
    use std::{hint::black_box, sync::Arc, time::Instant};

    use super::*;

    const SNAPSHOT: &str = r#"[{"asks":[["101","1","0","1"]],"bids":[["100","2","0","1"]],"ts":"1700000000000","seqId":100,"prevSeqId":-1,"ignored":"preserved-compatible"}]"#;

    fn instrument(value: &str) -> OkxSpotInstrumentId {
        OkxSpotInstrumentId::try_from(value).expect("valid test instrument")
    }

    fn parse_golden(envelope: &str) -> ValidatedBooksMessage {
        let envelope: serde_json::Value = serde_json::from_str(envelope).unwrap();
        let arg = &envelope["arg"];
        let expected = instrument(arg["instId"].as_str().unwrap());
        parse_books_message(
            &expected,
            arg["channel"].as_str().unwrap(),
            arg["instId"].as_str().unwrap(),
            envelope["action"].as_str().unwrap(),
            &serde_json::to_string(&envelope["data"]).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn shared_golden_vectors_preserve_validated_event_content() {
        let snapshot = parse_golden(include_str!(
            "../../../fixtures/public-market/books-snapshot.json"
        ));
        let update = parse_golden(include_str!(
            "../../../fixtures/public-market/books-update.json"
        ));

        assert_eq!(snapshot.instrument_id.as_str(), "BTC-USDT");
        assert_eq!(snapshot.action, OkxLevel2Action::Snapshot);
        assert_eq!(snapshot.data.sequence_id, 100);
        assert_eq!(snapshot.data.bids[0].price, Decimal::from(100));
        assert_eq!(snapshot.data.asks[0].quantity, Decimal::ONE);
        assert_eq!(update.action, OkxLevel2Action::Update);
        assert_eq!(update.data.previous_sequence_id, 100);
        assert_eq!(update.data.bids[1].price, Decimal::new(1005, 1));
        assert_eq!(update.data.asks[0].quantity, Decimal::ZERO);
    }

    #[test]
    fn validates_round_trips_and_cheaply_clones_spot_instrument_ids() {
        for value in ["BTC-USDT", "USDC-USDT", "1INCH-USDT"] {
            let parsed = instrument(value);
            assert_eq!(parsed.as_str(), value);
            assert_eq!(parsed.to_string(), value);
            assert!(format!("{parsed:?}").contains(value));
            let cloned = parsed.clone();
            assert_eq!(parsed, cloned);
            assert!(Arc::ptr_eq(&parsed.0, &cloned.0));
            let json = serde_json::to_string(&parsed).expect("serialize instrument");
            assert_eq!(
                serde_json::from_str::<OkxSpotInstrumentId>(&json).expect("deserialize instrument"),
                parsed
            );
        }
    }

    #[test]
    fn rejects_malformed_spot_instrument_ids() {
        for invalid in [
            "",
            "btc-usdt",
            "BTCUSDT",
            "BTC-",
            "-USDT",
            "BTC-USDT-SWAP",
            "BTC-USDT-260925",
            "BTC-USDT-C",
            "BTC--USDT",
            "BTC BTC-USDT",
            "BTC-USDT ",
            "BTC-BTC",
            "BTC-ÉUR",
        ] {
            assert!(
                matches!(
                    OkxSpotInstrumentId::try_from(invalid),
                    Err(OkxPublicProtocolError::MalformedSpotInstrument { .. })
                ),
                "accepted invalid instrument {invalid:?}"
            );
        }
        let too_long = format!("{}-B", "A".repeat(OKX_SPOT_INSTRUMENT_ID_MAX_LEN));
        assert!(OkxSpotInstrumentId::try_from(too_long.as_str()).is_err());
        let maximum = format!("{}-B", "A".repeat(OKX_SPOT_INSTRUMENT_ID_MAX_LEN - 2));
        assert_eq!(instrument(&maximum).as_str(), maximum);
    }

    #[test]
    fn parses_btc_and_usdc_books_with_explicit_identity() {
        for value in ["BTC-USDT", "USDC-USDT"] {
            let expected = instrument(value);
            let snapshot =
                parse_books_message(&expected, "books", value, "snapshot", SNAPSHOT).unwrap();
            assert_eq!(snapshot.instrument_id, expected);
            assert_eq!(snapshot.action, OkxLevel2Action::Snapshot);
            assert_eq!(snapshot.data.ts, 1_700_000_000_000);
        }
        let expected = instrument("USDC-USDT");
        let heartbeat = parse_books_message(
            &expected,
            "books",
            "USDC-USDT",
            "update",
            r#"[{"asks":[],"bids":[],"ts":"1700000000001","seqId":100,"prevSeqId":100}]"#,
        )
        .unwrap();
        assert!(heartbeat.data.asks.is_empty() && heartbeat.data.bids.is_empty());
    }

    #[test]
    fn accepts_four_hundred_levels_and_rejects_invalid_decimal() {
        let expected = instrument("BTC-USDT");
        let levels = (0..400)
            .map(|index| format!(r#"["{}","1","0","1"]"#, 10_000 + index))
            .collect::<Vec<_>>()
            .join(",");
        let message = format!(
            r#"[{{"asks":[{levels}],"bids":[["9999","1","0","1"]],"ts":"1700000000000","seqId":1,"prevSeqId":-1}}]"#
        );
        assert_eq!(
            parse_books_message(&expected, "books", "BTC-USDT", "snapshot", &message)
                .unwrap()
                .data
                .asks
                .len(),
            400
        );
        assert!(
            parse_books_message(
                &expected,
                "books",
                "BTC-USDT",
                "snapshot",
                r#"[{"asks":[["bad","1","0","1"]],"bids":[],"ts":"1","seqId":1,"prevSeqId":-1}]"#,
            )
            .is_err()
        );
    }

    #[test]
    fn books_errors_distinguish_malformed_mismatch_and_payload_validation() {
        let expected = instrument("BTC-USDT");
        assert!(matches!(
            parse_books_message(&expected, "books", "USDC-USDT", "snapshot", SNAPSHOT),
            Err(OkxPublicProtocolError::InstrumentMismatch { .. })
        ));
        assert!(matches!(
            parse_books_message(&expected, "books", "BTC-USDT-SWAP", "snapshot", SNAPSHOT),
            Err(OkxPublicProtocolError::MalformedSpotInstrument { .. })
        ));
        assert!(matches!(
            parse_books_message(&expected, "books5", "BTC-USDT", "snapshot", SNAPSHOT),
            Err(OkxPublicProtocolError::UnsupportedChannel(_))
        ));
        assert!(matches!(
            parse_books_message(&expected, "books", "BTC-USDT", "other", SNAPSHOT),
            Err(OkxPublicProtocolError::UnsupportedAction(_))
        ));
        assert!(matches!(
            parse_books_message(
                &expected,
                "books",
                "BTC-USDT",
                "snapshot",
                r#"[{"asks":[["101","1","0","1"],["101","2","0","1"]],"bids":[["100","1","0","1"]],"ts":"1","seqId":1,"prevSeqId":-1}]"#,
            ),
            Err(OkxPublicProtocolError::DuplicatePrice(_))
        ));
        assert!(
            parse_books_message(
                &expected,
                "books",
                "BTC-USDT",
                "snapshot",
                r#"[{"asks":[],"bids":[],"ts":"0","seqId":1,"prevSeqId":-1}]"#,
            )
            .is_err()
        );
        assert!(matches!(
            parse_books_message(&expected, "books", "BTC-USDT", "snapshot", "[]"),
            Err(OkxPublicProtocolError::UnexpectedRowCount(0))
        ));
    }

    #[test]
    fn parses_btc_and_usdc_trade_batches_and_preserves_aggregation() {
        for value in ["BTC-USDT", "USDC-USDT"] {
            let expected = instrument(value);
            let payload = format!(
                r#"[{{"instId":"{value}","tradeId":"20","ts":"1700000000000","count":"3","px":"100.5","sz":"0.01","side":"buy"}}]"#
            );
            let trades = parse_trade_message(&expected, "trades", value, &payload).unwrap();
            assert_eq!(trades[0].instrument_id, expected);
            assert_eq!(trades[0].terminal_trade_id, 20);
            assert_eq!(trades[0].aggregation_count, 3);
            assert_eq!(trades[0].price, Decimal::new(1005, 1));
        }
    }

    #[test]
    fn rejects_outer_row_mixed_and_malformed_trade_instruments() {
        let expected = instrument("BTC-USDT");
        assert!(matches!(
            parse_trade_message(&expected, "trades", "USDC-USDT", "[]"),
            Err(OkxPublicProtocolError::InstrumentMismatch { .. })
        ));
        for payload in [
            r#"[{"instId":"USDC-USDT","tradeId":"20","ts":"1700000000000","px":"1","sz":"1","side":"buy"}]"#,
            r#"[{"instId":"BTC-USDT","tradeId":"20","ts":"1700000000000","px":"1","sz":"1","side":"buy"},{"instId":"USDC-USDT","tradeId":"21","ts":"1700000000001","px":"1","sz":"1","side":"sell"}]"#,
        ] {
            assert!(matches!(
                parse_trade_message(&expected, "trades", "BTC-USDT", payload),
                Err(OkxPublicProtocolError::MixedInstrumentPayload { .. })
            ));
        }
        assert!(matches!(
            parse_trade_message(
                &expected,
                "trades",
                "BTC-USDT",
                r#"[{"instId":"BTC-USDT-SWAP","tradeId":"20","ts":"1700000000000","px":"1","sz":"1","side":"buy"}]"#,
            ),
            Err(OkxPublicProtocolError::MalformedSpotInstrument { .. })
        ));
        assert!(matches!(
            parse_trade_message(&expected, "books", "BTC-USDT", "[]"),
            Err(OkxPublicProtocolError::UnsupportedChannel(_))
        ));
    }

    #[test]
    fn public_trade_archive_identity_is_strictly_validated() {
        let trade: OkxPublicTrade =
            serde_json::from_str(r#"{"instId":"USDC-USDT","tradeId":"20","ts":"1700000000000"}"#)
                .expect("valid archive trade");
        assert_eq!(trade.inst_id.as_str(), "USDC-USDT");
        assert!(
            serde_json::from_str::<OkxPublicTrade>(
                r#"{"instId":"BTC-USDT-SWAP","tradeId":"20","ts":"1700000000000"}"#
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_invalid_trade_values_without_weakening_identity_checks() {
        let expected = instrument("USDC-USDT");
        for payload in [
            r#"[{"instId":"USDC-USDT","tradeId":"0","ts":"1700000000000","px":"1","sz":"1","side":"buy"}]"#,
            r#"[{"instId":"USDC-USDT","tradeId":"1","ts":"0","px":"1","sz":"1","side":"buy"}]"#,
            r#"[{"instId":"USDC-USDT","tradeId":"1","ts":"1700000000000","count":"0","px":"1","sz":"1","side":"buy"}]"#,
            r#"[{"instId":"USDC-USDT","tradeId":"1","ts":"1700000000000","px":"0","sz":"1","side":"buy"}]"#,
            r#"[{"instId":"USDC-USDT","tradeId":"1","ts":"1700000000000","px":"1","sz":"0","side":"buy"}]"#,
            r#"[{"instId":"USDC-USDT","tradeId":"1","ts":"1700000000000","px":"1","sz":"1","side":"hold"}]"#,
        ] {
            assert!(parse_trade_message(&expected, "trades", "USDC-USDT", payload).is_err());
        }
    }

    #[test]
    #[ignore = "performance-only; run explicitly with --release --ignored --nocapture"]
    fn instrument_protocol_benchmark() {
        const SAMPLE_COUNT: usize = 10_000;
        let expected = instrument("USDC-USDT");
        let validation = benchmark_samples(SAMPLE_COUNT, || {
            black_box(OkxSpotInstrumentId::try_from(black_box("USDC-USDT"))).expect("instrument")
        });
        let clone = benchmark_samples(SAMPLE_COUNT, || black_box(expected.clone()));
        let books = benchmark_samples(SAMPLE_COUNT, || {
            black_box(parse_books_message(
                &expected,
                "books",
                "USDC-USDT",
                "snapshot",
                SNAPSHOT,
            ))
            .expect("books message")
        });
        print_benchmark("instrument_validation", &validation);
        print_benchmark("instrument_clone", &clone);
        print_benchmark("books_message_validation", &books);
    }

    fn benchmark_samples<T>(count: usize, mut operation: impl FnMut() -> T) -> Vec<u64> {
        let mut samples = Vec::with_capacity(count);
        for _ in 0..count {
            let started_at = Instant::now();
            black_box(operation());
            samples.push(started_at.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        }
        samples.sort_unstable();
        samples
    }

    fn print_benchmark(label: &str, samples: &[u64]) {
        println!(
            "{label}: samples={} min={}ns p50={}ns p95={}ns p99={}ns max={}ns",
            samples.len(),
            samples[0],
            percentile_nanos(samples, 50),
            percentile_nanos(samples, 95),
            percentile_nanos(samples, 99),
            samples[samples.len() - 1]
        );
    }

    fn percentile_nanos(sorted: &[u64], percentile: usize) -> u64 {
        let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
        sorted[rank.saturating_sub(1).min(sorted.len().saturating_sub(1))]
    }
}
