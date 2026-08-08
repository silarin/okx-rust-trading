use std::sync::atomic::{AtomicU64, Ordering};

use time::OffsetDateTime;

pub(super) const OKX_CLIENT_ORDER_ID_MAX_LEN: usize = 32;
pub(super) const ORDER_ID_PREFIX: &str = "ROX";
const STRATEGY_TAG_PREFIX_LEN: usize = 4;
const STRATEGY_TAG_HASH_LEN: usize = 7;
const STRATEGY_TAG_LEN: usize = STRATEGY_TAG_PREFIX_LEN + STRATEGY_TAG_HASH_LEN;
const LEGACY_STRATEGY_TAG_MAX_LEN: usize = 8;
const LEGACY_STRATEGY_TAG_FALLBACK: &str = "STRAT";
const MIN_SUFFIX_LEN: usize = 5;
const MAX_SUFFIX_LEN: usize =
    OKX_CLIENT_ORDER_ID_MAX_LEN - ORDER_ID_PREFIX.len() - STRATEGY_TAG_LEN - OrderPurpose::CODE_LEN;
const FNV_1A_32_OFFSET_BASIS: u32 = 0x811c_9dc5;
const FNV_1A_32_PRIME: u32 = 0x0100_0193;

static ORDER_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OrderPurpose {
    Entry,
    TakeProfit,
    StopLoss,
}

pub(super) fn client_order_id(strategy_id: &str, purpose: OrderPurpose) -> String {
    let tag = strategy_tag(strategy_id);
    let now_ms = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    let now_ms = u64::try_from(now_ms).unwrap_or_default();
    let sequence = ORDER_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed) % 10_000;
    let client_order_id = format!(
        "{ORDER_ID_PREFIX}{tag}{}{time}{sequence:04}",
        purpose.as_code(),
        time = base36(now_ms)
    );
    debug_assert!(
        client_order_id.len() <= OKX_CLIENT_ORDER_ID_MAX_LEN,
        "OKX clOrdId/algoClOrdId must not exceed {OKX_CLIENT_ORDER_ID_MAX_LEN} characters"
    );
    client_order_id
}

pub(crate) fn strategy_ownership_tag_for_config(strategy_id: &str) -> String {
    strategy_tag(strategy_id)
}

pub(super) fn strategy_tag(strategy_id: &str) -> String {
    let mut tag = strategy_tag_prefix(strategy_id);
    tag.push_str(&padded_base36(
        u64::from(fnv_1a_32(strategy_id.as_bytes())),
        STRATEGY_TAG_HASH_LEN,
    ));
    debug_assert_eq!(tag.len(), STRATEGY_TAG_LEN);
    tag
}

pub(super) fn legacy_strategy_tag(strategy_id: &str) -> String {
    let tag = strategy_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(LEGACY_STRATEGY_TAG_MAX_LEN)
        .collect::<String>()
        .to_ascii_uppercase();
    if tag.is_empty() {
        LEGACY_STRATEGY_TAG_FALLBACK.to_owned()
    } else {
        tag
    }
}

fn strategy_tag_prefix(strategy_id: &str) -> String {
    let mut tag = strategy_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(STRATEGY_TAG_PREFIX_LEN)
        .collect::<String>()
        .to_ascii_uppercase();
    if tag.is_empty() {
        tag.push_str("STR");
    }
    while tag.len() < STRATEGY_TAG_PREFIX_LEN {
        tag.push('0');
    }
    tag
}

pub(super) fn parse_strategy_client_order_id(
    client_order_id: &str,
    strategy_tag: &str,
) -> Option<OrderPurpose> {
    if !valid_strategy_tag(strategy_tag) {
        return None;
    }
    let suffix = client_order_id
        .strip_prefix(ORDER_ID_PREFIX)?
        .strip_prefix(strategy_tag)?;
    parse_order_suffix(suffix, MAX_SUFFIX_LEN)
}

pub(super) fn parse_legacy_strategy_client_order_id(
    client_order_id: &str,
    legacy_strategy_tag: &str,
) -> Option<OrderPurpose> {
    if !valid_legacy_strategy_tag(legacy_strategy_tag) {
        return None;
    }
    let max_suffix_len = OKX_CLIENT_ORDER_ID_MAX_LEN
        - ORDER_ID_PREFIX.len()
        - legacy_strategy_tag.len()
        - OrderPurpose::CODE_LEN;
    let suffix = client_order_id
        .strip_prefix(ORDER_ID_PREFIX)?
        .strip_prefix(legacy_strategy_tag)?;
    parse_order_suffix(suffix, max_suffix_len)
}

fn parse_order_suffix(suffix: &str, max_suffix_len: usize) -> Option<OrderPurpose> {
    let mut suffix_chars = suffix.chars();
    let purpose = match suffix_chars.next()? {
        'B' => Some(OrderPurpose::Entry),
        'T' => Some(OrderPurpose::TakeProfit),
        'S' => Some(OrderPurpose::StopLoss),
        _ => None,
    }?;
    let generated_suffix = suffix_chars.as_str();
    if !(MIN_SUFFIX_LEN..=max_suffix_len).contains(&generated_suffix.len())
        || !generated_suffix
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric())
    {
        return None;
    }
    Some(purpose)
}

fn valid_strategy_tag(strategy_tag: &str) -> bool {
    strategy_tag.len() == STRATEGY_TAG_LEN
        && strategy_tag.chars().all(|ch| ch.is_ascii_alphanumeric())
}

fn valid_legacy_strategy_tag(strategy_tag: &str) -> bool {
    matches!(strategy_tag.len(), 1..=LEGACY_STRATEGY_TAG_MAX_LEN)
        && strategy_tag.chars().all(|ch| ch.is_ascii_alphanumeric())
}

fn fnv_1a_32(bytes: &[u8]) -> u32 {
    let mut hash = FNV_1A_32_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(FNV_1A_32_PRIME);
    }
    hash
}

fn padded_base36(value: u64, width: usize) -> String {
    let encoded = base36(value);
    if encoded.len() >= width {
        return encoded;
    }

    let mut padded = String::with_capacity(width);
    for _ in encoded.len()..width {
        padded.push('0');
    }
    padded.push_str(&encoded);
    padded
}

pub(super) fn base36(mut value: u64) -> String {
    if value == 0 {
        return "0".to_owned();
    }
    let mut encoded = Vec::new();
    while value > 0 {
        let digit = (value % 36) as u8;
        encoded.push(match digit {
            0..=9 => char::from(b'0' + digit),
            10..=35 => char::from(b'A' + digit - 10),
            36..=u8::MAX => unreachable!("base36 digit should be below 36"),
        });
        value /= 36;
    }
    encoded.iter().rev().collect()
}

impl OrderPurpose {
    const CODE_LEN: usize = 1;

    pub(super) const fn as_code(self) -> &'static str {
        match self {
            Self::Entry => "B",
            Self::TakeProfit => "T",
            Self::StopLoss => "S",
        }
    }
}
