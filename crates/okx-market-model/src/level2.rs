//! OKX `books` channel reconstruction and bounded Decimal feature extraction.

use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
    sync::Arc,
    time::Instant,
};

pub use okx_public_protocol::{OKX_LEVEL2_BOOKS_CHANNEL, OKX_LEVEL2_MAX_DEPTH};
use okx_public_protocol::{
    OkxLevel2Action, OkxLevel2Data, OkxLevel2Level, OkxSpotInstrumentId, ValidatedBooksMessage,
};
use rust_decimal::Decimal;
const FEATURE_ROLLING_WINDOW: usize = 32;
const BASIS_POINTS_PER_UNIT: u64 = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxLevel2Update {
    pub instrument_id: OkxSpotInstrumentId,
    pub action: OkxLevel2Action,
    pub data: OkxLevel2Data,
    pub received_at: Instant,
    pub parsed_at: Instant,
}

impl OkxLevel2Update {
    /// Attaches consumer-local timing to an already validated public payload.
    pub fn from_validated(
        message: ValidatedBooksMessage,
        received_at: Instant,
        parsed_at: Instant,
    ) -> Self {
        Self {
            instrument_id: message.instrument_id,
            action: message.action,
            data: message.data,
            received_at,
            parsed_at,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxLevel2FeatureSnapshot {
    pub instrument_id: OkxSpotInstrumentId,
    pub epoch: u64,
    pub generation: u64,
    pub sequence_id: i64,
    pub exchange_ts_ms: i64,
    pub received_at: Instant,
    pub parsed_at: Instant,
    pub book_applied_at: Instant,
    pub features_ready_at: Instant,
    pub bid_level_count: usize,
    pub ask_level_count: usize,
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub mid: Decimal,
    pub spread: Decimal,
    pub classic_microprice: Decimal,
    pub multi_level_microprice: Decimal,
    pub microprice_displacement: Decimal,
    pub imbalance_l1: Decimal,
    pub imbalance_l3: Decimal,
    pub imbalance_l5: Decimal,
    pub imbalance_l10: Decimal,
    pub imbalance_l25: Decimal,
    pub imbalance_l50: Decimal,
    pub imbalance_l100: Decimal,
    pub imbalance_l200: Decimal,
    pub imbalance_l400: Decimal,
    pub distance_weighted_imbalance: Decimal,
    pub notional_imbalance: Decimal,
    pub near_book_depth: OkxNearBookDepth,
    pub depth_move_1bps: OkxDepthMove,
    pub depth_move_2bps: OkxDepthMove,
    pub depth_move_5bps: OkxDepthMove,
    pub depth_move_10bps: OkxDepthMove,
    pub depth_slope: OkxDepthSlope,
    pub liquidity_vacuum: Decimal,
    pub imbalance_velocity_per_second: Decimal,
    pub imbalance_persistence: Decimal,
    pub short_horizon_book_volatility_bps: Decimal,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OkxNearBookDepth {
    pub bid_quantity_l10: Decimal,
    pub ask_quantity_l10: Decimal,
    pub bid_notional_l10: Decimal,
    pub ask_notional_l10: Decimal,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OkxDepthMove {
    pub bid_quantity: Decimal,
    pub ask_quantity: Decimal,
    pub bid_notional: Decimal,
    pub ask_notional: Decimal,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OkxDepthSlope {
    pub bid_bps_per_base_unit: Decimal,
    pub ask_bps_per_base_unit: Decimal,
}

#[derive(Debug)]
pub struct OkxLevel2Book {
    instrument_id: OkxSpotInstrumentId,
    bids: BTreeMap<Decimal, BookLevel>,
    asks: BTreeMap<Decimal, BookLevel>,
    valid: bool,
    epoch: u64,
    generation: u64,
    sequence_id: Option<i64>,
    features: Option<Arc<OkxLevel2FeatureSnapshot>>,
    rolling: RollingFeatureState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BookLevel {
    quantity: Decimal,
    order_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RollingFeaturePoint {
    mid: Decimal,
    imbalance_l10: Decimal,
    received_at: Instant,
}

#[derive(Debug, Default)]
struct RollingFeatureState {
    points: VecDeque<RollingFeaturePoint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxLevel2ApplyOutcome {
    Features(Arc<OkxLevel2FeatureSnapshot>),
    Heartbeat,
}

impl OkxLevel2Book {
    #[must_use]
    pub fn new(instrument_id: OkxSpotInstrumentId) -> Self {
        Self {
            instrument_id,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            valid: false,
            epoch: 0,
            generation: 0,
            sequence_id: None,
            features: None,
            rolling: RollingFeatureState::default(),
        }
    }

    #[must_use]
    pub const fn instrument_id(&self) -> &OkxSpotInstrumentId {
        &self.instrument_id
    }

    pub fn apply(
        &mut self,
        update: OkxLevel2Update,
    ) -> Result<OkxLevel2ApplyOutcome, OkxLevel2BookError> {
        let deterministic_at = update.parsed_at;
        self.apply_with_clock(update, || deterministic_at)
    }

    /// Applies one event while allowing the live consumer to supply its
    /// monotonic observation clock for latency telemetry.
    pub fn apply_with_clock(
        &mut self,
        update: OkxLevel2Update,
        mut observe_now: impl FnMut() -> Instant,
    ) -> Result<OkxLevel2ApplyOutcome, OkxLevel2BookError> {
        if update.instrument_id != self.instrument_id {
            return Err(OkxLevel2BookError::InstrumentMismatch {
                expected: self.instrument_id.clone(),
                received: update.instrument_id,
            });
        }
        match update.action {
            OkxLevel2Action::Snapshot => self.apply_snapshot(update, &mut observe_now),
            OkxLevel2Action::Update => self.apply_increment(update, &mut observe_now),
        }
    }

    pub fn invalidate(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.valid = false;
        self.sequence_id = None;
        self.features = None;
        self.rolling.points.clear();
    }

    pub fn features(&self) -> Option<Arc<OkxLevel2FeatureSnapshot>> {
        self.valid.then(|| self.features.clone()).flatten()
    }

    pub const fn is_valid(&self) -> bool {
        self.valid
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    fn apply_snapshot(
        &mut self,
        update: OkxLevel2Update,
        observe_now: &mut impl FnMut() -> Instant,
    ) -> Result<OkxLevel2ApplyOutcome, OkxLevel2BookError> {
        let next_book = (|| {
            if update.data.asks.len() > OKX_LEVEL2_MAX_DEPTH
                || update.data.bids.len() > OKX_LEVEL2_MAX_DEPTH
            {
                return Err(OkxLevel2BookError::DepthOverflow);
            }
            let bids = build_snapshot_side(&update.data.bids)?;
            let asks = build_snapshot_side(&update.data.asks)?;
            validate_book(&bids, &asks)?;
            Ok((bids, asks))
        })();
        let (bids, asks) = match next_book {
            Ok(book) => book,
            Err(error) => {
                self.invalidate();
                return Err(error);
            }
        };

        self.bids = bids;
        self.asks = asks;
        self.valid = true;
        self.epoch = self.epoch.saturating_add(1);
        self.generation = self.generation.saturating_add(1);
        self.sequence_id = Some(update.data.sequence_id);
        self.rolling.points.clear();
        match self.finish_features(update, observe_now) {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                self.invalidate();
                Err(error)
            }
        }
    }

    fn apply_increment(
        &mut self,
        update: OkxLevel2Update,
        observe_now: &mut impl FnMut() -> Instant,
    ) -> Result<OkxLevel2ApplyOutcome, OkxLevel2BookError> {
        if !self.valid {
            return Err(OkxLevel2BookError::UpdateBeforeSnapshot);
        }
        let current_sequence = self
            .sequence_id
            .ok_or(OkxLevel2BookError::UpdateBeforeSnapshot)?;
        let empty_update = update.data.asks.is_empty() && update.data.bids.is_empty();
        if empty_update
            && update.data.sequence_id == current_sequence
            && update.data.previous_sequence_id == current_sequence
        {
            return Ok(OkxLevel2ApplyOutcome::Heartbeat);
        }
        if update.data.previous_sequence_id != current_sequence {
            let error = OkxLevel2BookError::SequenceGap {
                expected_previous: current_sequence,
                actual_previous: update.data.previous_sequence_id,
                sequence_id: update.data.sequence_id,
            };
            self.invalidate();
            return Err(error);
        }
        if update.data.sequence_id <= current_sequence {
            let error = OkxLevel2BookError::RegressingSequence {
                current: current_sequence,
                incoming: update.data.sequence_id,
            };
            self.invalidate();
            return Err(error);
        }

        if let Err(error) = apply_side(&mut self.bids, &update.data.bids) {
            self.invalidate();
            return Err(error);
        }
        if let Err(error) = apply_side(&mut self.asks, &update.data.asks) {
            self.invalidate();
            return Err(error);
        }
        trim_to_depth(&mut self.bids, BookSide::Bid);
        trim_to_depth(&mut self.asks, BookSide::Ask);
        if let Err(error) = validate_book(&self.bids, &self.asks) {
            self.invalidate();
            return Err(error);
        }

        self.generation = self.generation.saturating_add(1);
        self.sequence_id = Some(update.data.sequence_id);
        match self.finish_features(update, observe_now) {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                self.invalidate();
                Err(error)
            }
        }
    }

    fn finish_features(
        &mut self,
        update: OkxLevel2Update,
        observe_now: &mut impl FnMut() -> Instant,
    ) -> Result<OkxLevel2ApplyOutcome, OkxLevel2BookError> {
        let book_applied_at = observe_now();
        let features = self.calculate_features(&update, book_applied_at, observe_now)?;
        let features = Arc::new(features);
        self.features = Some(Arc::clone(&features));
        Ok(OkxLevel2ApplyOutcome::Features(features))
    }

    fn calculate_features(
        &mut self,
        update: &OkxLevel2Update,
        book_applied_at: Instant,
        observe_now: &mut impl FnMut() -> Instant,
    ) -> Result<OkxLevel2FeatureSnapshot, OkxLevel2BookError> {
        let (best_bid, best_bid_level) = self
            .bids
            .last_key_value()
            .map(|(price, level)| (*price, *level))
            .ok_or(OkxLevel2BookError::EmptySide)?;
        let (best_ask, best_ask_level) = self
            .asks
            .first_key_value()
            .map(|(price, level)| (*price, *level))
            .ok_or(OkxLevel2BookError::EmptySide)?;
        let spread = best_ask - best_bid;
        let mid = (best_ask + best_bid) / Decimal::TWO;
        let top_quantity = best_bid_level.quantity + best_ask_level.quantity;
        if top_quantity <= Decimal::ZERO {
            return Err(OkxLevel2BookError::ZeroDenominator("classic microprice"));
        }
        let classic_microprice = (best_ask * best_bid_level.quantity
            + best_bid * best_ask_level.quantity)
            / top_quantity;
        let imbalance_l1 = self.imbalance(1);
        let imbalance_l3 = self.imbalance(3);
        let imbalance_l5 = self.imbalance(5);
        let imbalance_l10 = self.imbalance(10);
        let imbalance_l25 = self.imbalance(25);
        let imbalance_l50 = self.imbalance(50);
        let imbalance_l100 = self.imbalance(100);
        let imbalance_l200 = self.imbalance(200);
        let imbalance_l400 = self.imbalance(400);
        let distance_weighted_imbalance = self.distance_weighted_imbalance();
        let multi_level_microprice = mid + spread * distance_weighted_imbalance / Decimal::TWO;
        let near_book_depth = self.near_book_depth(10);
        let depth_slope = self.depth_slope(mid, 10);
        let liquidity_vacuum = self.liquidity_vacuum();
        let (imbalance_velocity_per_second, imbalance_persistence, volatility) =
            self.rolling_features(mid, imbalance_l10, update.received_at);
        let features_ready_at = observe_now();

        Ok(OkxLevel2FeatureSnapshot {
            instrument_id: self.instrument_id.clone(),
            epoch: self.epoch,
            generation: self.generation,
            sequence_id: update.data.sequence_id,
            exchange_ts_ms: update.data.ts,
            received_at: update.received_at,
            parsed_at: update.parsed_at,
            book_applied_at,
            features_ready_at,
            bid_level_count: self.bids.len(),
            ask_level_count: self.asks.len(),
            best_bid,
            best_ask,
            mid,
            spread,
            classic_microprice,
            multi_level_microprice,
            microprice_displacement: multi_level_microprice - mid,
            imbalance_l1,
            imbalance_l3,
            imbalance_l5,
            imbalance_l10,
            imbalance_l25,
            imbalance_l50,
            imbalance_l100,
            imbalance_l200,
            imbalance_l400,
            distance_weighted_imbalance,
            notional_imbalance: self.notional_imbalance(OKX_LEVEL2_MAX_DEPTH),
            near_book_depth,
            depth_move_1bps: self.depth_to_move(mid, 1),
            depth_move_2bps: self.depth_to_move(mid, 2),
            depth_move_5bps: self.depth_to_move(mid, 5),
            depth_move_10bps: self.depth_to_move(mid, 10),
            depth_slope,
            liquidity_vacuum,
            imbalance_velocity_per_second,
            imbalance_persistence,
            short_horizon_book_volatility_bps: volatility,
        })
    }

    fn imbalance(&self, levels: usize) -> Decimal {
        let bid = self
            .bids
            .iter()
            .rev()
            .take(levels)
            .map(|(_, level)| level.quantity)
            .sum::<Decimal>();
        let ask = self
            .asks
            .iter()
            .take(levels)
            .map(|(_, level)| level.quantity)
            .sum::<Decimal>();
        signed_ratio(bid, ask)
    }

    fn distance_weighted_imbalance(&self) -> Decimal {
        let bid = self
            .bids
            .iter()
            .rev()
            .take(OKX_LEVEL2_MAX_DEPTH)
            .enumerate()
            .map(|(index, (_, level))| level.quantity / Decimal::from((index + 1) as u64))
            .sum::<Decimal>();
        let ask = self
            .asks
            .iter()
            .take(OKX_LEVEL2_MAX_DEPTH)
            .enumerate()
            .map(|(index, (_, level))| level.quantity / Decimal::from((index + 1) as u64))
            .sum::<Decimal>();
        signed_ratio(bid, ask)
    }

    fn notional_imbalance(&self, levels: usize) -> Decimal {
        let bid = self
            .bids
            .iter()
            .rev()
            .take(levels)
            .map(|(price, level)| *price * level.quantity)
            .sum::<Decimal>();
        let ask = self
            .asks
            .iter()
            .take(levels)
            .map(|(price, level)| *price * level.quantity)
            .sum::<Decimal>();
        signed_ratio(bid, ask)
    }

    fn near_book_depth(&self, levels: usize) -> OkxNearBookDepth {
        let (bid_quantity_l10, bid_notional_l10) = side_depth(self.bids.iter().rev().take(levels));
        let (ask_quantity_l10, ask_notional_l10) = side_depth(self.asks.iter().take(levels));
        OkxNearBookDepth {
            bid_quantity_l10,
            ask_quantity_l10,
            bid_notional_l10,
            ask_notional_l10,
        }
    }

    fn depth_to_move(&self, mid: Decimal, basis_points: u64) -> OkxDepthMove {
        let fraction = Decimal::from(basis_points) / Decimal::from(BASIS_POINTS_PER_UNIT);
        let bid_floor = mid * (Decimal::ONE - fraction);
        let ask_ceiling = mid * (Decimal::ONE + fraction);
        let (bid_quantity, bid_notional) = side_depth(
            self.bids
                .iter()
                .rev()
                .take_while(|(price, _)| **price >= bid_floor),
        );
        let (ask_quantity, ask_notional) = side_depth(
            self.asks
                .iter()
                .take_while(|(price, _)| **price <= ask_ceiling),
        );
        OkxDepthMove {
            bid_quantity,
            ask_quantity,
            bid_notional,
            ask_notional,
        }
    }

    fn depth_slope(&self, mid: Decimal, levels: usize) -> OkxDepthSlope {
        let bid_quantity = self
            .bids
            .iter()
            .rev()
            .take(levels)
            .map(|(_, level)| level.quantity)
            .sum::<Decimal>();
        let ask_quantity = self
            .asks
            .iter()
            .take(levels)
            .map(|(_, level)| level.quantity)
            .sum::<Decimal>();
        let bid_edge = self
            .bids
            .iter()
            .rev()
            .nth(levels.saturating_sub(1))
            .or_else(|| self.bids.first_key_value())
            .map(|(price, _)| *price)
            .unwrap_or(mid);
        let ask_edge = self
            .asks
            .iter()
            .nth(levels.saturating_sub(1))
            .or_else(|| self.asks.last_key_value())
            .map(|(price, _)| *price)
            .unwrap_or(mid);
        let scale = Decimal::from(BASIS_POINTS_PER_UNIT);
        OkxDepthSlope {
            bid_bps_per_base_unit: positive_ratio((mid - bid_edge) * scale, mid * bid_quantity),
            ask_bps_per_base_unit: positive_ratio((ask_edge - mid) * scale, mid * ask_quantity),
        }
    }

    fn liquidity_vacuum(&self) -> Decimal {
        let near_bid = self
            .bids
            .iter()
            .rev()
            .take(5)
            .map(|(price, level)| *price * level.quantity)
            .sum::<Decimal>();
        let near_ask = self
            .asks
            .iter()
            .take(5)
            .map(|(price, level)| *price * level.quantity)
            .sum::<Decimal>();
        let deep_bid = self
            .bids
            .iter()
            .rev()
            .take(50)
            .map(|(price, level)| *price * level.quantity)
            .sum::<Decimal>();
        let deep_ask = self
            .asks
            .iter()
            .take(50)
            .map(|(price, level)| *price * level.quantity)
            .sum::<Decimal>();
        let near_share = positive_ratio(near_bid, deep_bid).min(positive_ratio(near_ask, deep_ask));
        (Decimal::ONE - near_share).clamp(Decimal::ZERO, Decimal::ONE)
    }

    fn rolling_features(
        &mut self,
        mid: Decimal,
        imbalance_l10: Decimal,
        received_at: Instant,
    ) -> (Decimal, Decimal, Decimal) {
        let velocity = self
            .rolling
            .points
            .back()
            .map(|previous| {
                let elapsed_micros = received_at
                    .saturating_duration_since(previous.received_at)
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64;
                if elapsed_micros == 0 {
                    Decimal::ZERO
                } else {
                    (imbalance_l10 - previous.imbalance_l10) * Decimal::from(1_000_000_u64)
                        / Decimal::from(elapsed_micros)
                }
            })
            .unwrap_or(Decimal::ZERO);
        let current_sign = decimal_sign(imbalance_l10);
        let mut observed = 0_u64;
        let mut matching = 0_u64;
        for point in &self.rolling.points {
            let sign = decimal_sign(point.imbalance_l10);
            if sign != 0 {
                observed += 1;
                matching += u64::from(sign == current_sign);
            }
        }
        if current_sign != 0 {
            observed += 1;
            matching += 1;
        }
        let persistence = if observed == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(matching) / Decimal::from(observed)
        };
        let mut volatility_sum = Decimal::ZERO;
        let mut volatility_count = 0_u64;
        let mut previous_mid = None;
        for point in &self.rolling.points {
            if let Some(previous) = previous_mid {
                volatility_sum += absolute_return_bps(previous, point.mid);
                volatility_count += 1;
            }
            previous_mid = Some(point.mid);
        }
        if let Some(previous) = previous_mid {
            volatility_sum += absolute_return_bps(previous, mid);
            volatility_count += 1;
        }
        let volatility = if volatility_count == 0 {
            Decimal::ZERO
        } else {
            volatility_sum / Decimal::from(volatility_count)
        };

        if self.rolling.points.len() == FEATURE_ROLLING_WINDOW {
            self.rolling.points.pop_front();
        }
        self.rolling.points.push_back(RollingFeaturePoint {
            mid,
            imbalance_l10,
            received_at,
        });
        (velocity, persistence, volatility)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxLevel2BookError {
    InstrumentMismatch {
        expected: OkxSpotInstrumentId,
        received: OkxSpotInstrumentId,
    },
    UpdateBeforeSnapshot,
    SequenceGap {
        expected_previous: i64,
        actual_previous: i64,
        sequence_id: i64,
    },
    RegressingSequence {
        current: i64,
        incoming: i64,
    },
    DuplicatePrice(Decimal),
    UnknownRemoval(Decimal),
    EmptySide,
    CrossedBook {
        best_bid: Decimal,
        best_ask: Decimal,
    },
    DepthOverflow,
    ZeroDenominator(&'static str),
}

impl OkxLevel2BookError {
    pub const fn is_sequence_gap(&self) -> bool {
        matches!(self, Self::SequenceGap { .. })
    }

    pub const fn is_stale_rejection(&self) -> bool {
        matches!(self, Self::RegressingSequence { .. })
    }
}

impl fmt::Display for OkxLevel2BookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InstrumentMismatch { expected, received } => write!(
                formatter,
                "OKX Level-2 book instrument mismatch: expected {expected}, received {received}"
            ),
            Self::UpdateBeforeSnapshot => {
                formatter.write_str("OKX books update arrived before an authoritative snapshot")
            }
            Self::SequenceGap {
                expected_previous,
                actual_previous,
                sequence_id,
            } => write!(
                formatter,
                "OKX books sequence gap: expected prevSeqId {expected_previous}, received {actual_previous} for seqId {sequence_id}"
            ),
            Self::RegressingSequence { current, incoming } => write!(
                formatter,
                "OKX books seqId {incoming} did not advance current seqId {current}"
            ),
            Self::DuplicatePrice(price) => {
                write!(
                    formatter,
                    "OKX books side contained duplicate price {price}"
                )
            }
            Self::UnknownRemoval(price) => {
                write!(formatter, "OKX books removed unknown price {price}")
            }
            Self::EmptySide => formatter.write_str("OKX books book has an empty side"),
            Self::CrossedBook { best_bid, best_ask } => write!(
                formatter,
                "OKX books book is crossed: best bid {best_bid}, best ask {best_ask}"
            ),
            Self::DepthOverflow => write!(
                formatter,
                "OKX books snapshot exceeded {OKX_LEVEL2_MAX_DEPTH} levels"
            ),
            Self::ZeroDenominator(feature) => {
                write!(formatter, "OKX books {feature} has a zero denominator")
            }
        }
    }
}

impl Error for OkxLevel2BookError {}

#[derive(Clone, Copy)]
enum BookSide {
    Bid,
    Ask,
}

fn build_snapshot_side(
    updates: &[OkxLevel2Level],
) -> Result<BTreeMap<Decimal, BookLevel>, OkxLevel2BookError> {
    let mut side = BTreeMap::new();
    for update in updates {
        if update.quantity.is_zero() {
            continue;
        }
        if side
            .insert(
                update.price,
                BookLevel {
                    quantity: update.quantity,
                    order_count: update.order_count,
                },
            )
            .is_some()
        {
            return Err(OkxLevel2BookError::DuplicatePrice(update.price));
        }
    }
    Ok(side)
}

fn apply_side(
    side: &mut BTreeMap<Decimal, BookLevel>,
    updates: &[OkxLevel2Level],
) -> Result<(), OkxLevel2BookError> {
    let mut seen = BTreeMap::new();
    for update in updates {
        if seen.insert(update.price, ()).is_some() {
            return Err(OkxLevel2BookError::DuplicatePrice(update.price));
        }
        if update.quantity.is_zero() {
            if side.remove(&update.price).is_none() {
                return Err(OkxLevel2BookError::UnknownRemoval(update.price));
            }
        } else {
            side.insert(
                update.price,
                BookLevel {
                    quantity: update.quantity,
                    order_count: update.order_count,
                },
            );
        }
    }
    Ok(())
}

fn trim_to_depth(side: &mut BTreeMap<Decimal, BookLevel>, book_side: BookSide) {
    while side.len() > OKX_LEVEL2_MAX_DEPTH {
        let price = match book_side {
            BookSide::Bid => side.first_key_value().map(|(price, _)| *price),
            BookSide::Ask => side.last_key_value().map(|(price, _)| *price),
        };
        let Some(price) = price else {
            break;
        };
        side.remove(&price);
    }
}

fn validate_book(
    bids: &BTreeMap<Decimal, BookLevel>,
    asks: &BTreeMap<Decimal, BookLevel>,
) -> Result<(), OkxLevel2BookError> {
    let best_bid = bids
        .last_key_value()
        .map(|(price, _)| *price)
        .ok_or(OkxLevel2BookError::EmptySide)?;
    let best_ask = asks
        .first_key_value()
        .map(|(price, _)| *price)
        .ok_or(OkxLevel2BookError::EmptySide)?;
    if best_bid >= best_ask {
        return Err(OkxLevel2BookError::CrossedBook { best_bid, best_ask });
    }
    Ok(())
}

fn signed_ratio(bid: Decimal, ask: Decimal) -> Decimal {
    let total = bid + ask;
    if total <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (bid - ask) / total
    }
}

fn positive_ratio(numerator: Decimal, denominator: Decimal) -> Decimal {
    if numerator <= Decimal::ZERO || denominator <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        numerator / denominator
    }
}

fn side_depth<'a>(
    levels: impl Iterator<Item = (&'a Decimal, &'a BookLevel)>,
) -> (Decimal, Decimal) {
    levels.fold(
        (Decimal::ZERO, Decimal::ZERO),
        |(quantity, notional), (price, level)| {
            (
                quantity + level.quantity,
                notional + *price * level.quantity,
            )
        },
    )
}

fn decimal_sign(value: Decimal) -> i8 {
    if value > Decimal::ZERO {
        1
    } else if value < Decimal::ZERO {
        -1
    } else {
        0
    }
}

fn absolute_return_bps(previous: Decimal, current: Decimal) -> Decimal {
    if previous <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    ((current - previous).abs() / previous) * Decimal::from(BASIS_POINTS_PER_UNIT)
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use pretty_assertions::assert_eq;

    use super::*;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).expect("test decimal")
    }

    fn instrument(value: &str) -> OkxSpotInstrumentId {
        OkxSpotInstrumentId::try_from(value).expect("valid test instrument")
    }

    fn test_book() -> OkxLevel2Book {
        OkxLevel2Book::new(instrument("BTC-USDT"))
    }

    fn golden_message(envelope: &str) -> ValidatedBooksMessage {
        let envelope: serde_json::Value = serde_json::from_str(envelope).expect("golden envelope");
        let arg = &envelope["arg"];
        let expected = instrument(arg["instId"].as_str().expect("instrument"));
        okx_public_protocol::parse_books_message(
            &expected,
            arg["channel"].as_str().expect("channel"),
            arg["instId"].as_str().expect("instrument"),
            envelope["action"].as_str().expect("action"),
            &serde_json::to_string(&envelope["data"]).expect("data"),
        )
        .expect("valid golden message")
    }

    #[test]
    fn shared_golden_vectors_reconstruct_identical_top_generation_and_features()
    -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let mut book = test_book();
        let snapshot = features(book.apply(OkxLevel2Update::from_validated(
            golden_message(include_str!(
                "../../../fixtures/public-market/books-snapshot.json"
            )),
            now,
            now,
        ))?);
        assert_eq!(snapshot.best_bid, d("100"));
        assert_eq!(book.instrument_id().as_str(), "BTC-USDT");
        assert_eq!(snapshot.instrument_id.as_str(), "BTC-USDT");
        assert_eq!(snapshot.best_ask, d("101"));
        assert_eq!(snapshot.generation, 1);
        assert_eq!(snapshot.imbalance_l1, d("0.3333333333333333333333333333"));

        let update = features(book.apply(OkxLevel2Update::from_validated(
            golden_message(include_str!(
                "../../../fixtures/public-market/books-update.json"
            )),
            now + Duration::from_millis(100),
            now + Duration::from_millis(100),
        ))?);
        assert_eq!(update.best_bid, d("100.5"));
        assert_eq!(update.best_ask, d("101.5"));
        assert_eq!(update.generation, 2);
        assert_eq!(update.epoch, 1);
        assert_eq!(update.imbalance_l1, d("0.4285714285714285714285714286"));
        Ok(())
    }

    fn level(price: &str, quantity: &str) -> OkxLevel2Level {
        OkxLevel2Level {
            price: d(price),
            quantity: d(quantity),
            order_count: u64::from(quantity != "0"),
        }
    }

    fn update(
        action: OkxLevel2Action,
        sequence_id: i64,
        previous_sequence_id: i64,
        bids: Vec<OkxLevel2Level>,
        asks: Vec<OkxLevel2Level>,
        received_at: Instant,
    ) -> OkxLevel2Update {
        update_for(
            instrument("BTC-USDT"),
            action,
            sequence_id,
            previous_sequence_id,
            bids,
            asks,
            received_at,
        )
    }

    fn update_for(
        instrument_id: OkxSpotInstrumentId,
        action: OkxLevel2Action,
        sequence_id: i64,
        previous_sequence_id: i64,
        bids: Vec<OkxLevel2Level>,
        asks: Vec<OkxLevel2Level>,
        received_at: Instant,
    ) -> OkxLevel2Update {
        OkxLevel2Update {
            instrument_id,
            action,
            data: OkxLevel2Data {
                asks,
                bids,
                ts: 1_700_000_000_000 + sequence_id,
                sequence_id,
                previous_sequence_id,
            },
            received_at,
            parsed_at: received_at,
        }
    }

    fn snapshot(received_at: Instant) -> OkxLevel2Update {
        update(
            OkxLevel2Action::Snapshot,
            100,
            -1,
            vec![level("100", "2"), level("99", "3")],
            vec![level("101", "1"), level("102", "4")],
            received_at,
        )
    }

    fn features(outcome: OkxLevel2ApplyOutcome) -> Arc<OkxLevel2FeatureSnapshot> {
        match outcome {
            OkxLevel2ApplyOutcome::Features(features) => features,
            OkxLevel2ApplyOutcome::Heartbeat => panic!("expected features"),
        }
    }

    #[test]
    fn snapshot_builds_ordered_book_and_exact_features() -> Result<(), OkxLevel2BookError> {
        let mut book = test_book();
        let snapshot = features(book.apply(snapshot(Instant::now()))?);
        assert_eq!(snapshot.instrument_id, *book.instrument_id());
        assert_eq!(snapshot.best_bid, d("100"));
        assert_eq!(snapshot.best_ask, d("101"));
        assert_eq!(snapshot.mid, d("100.5"));
        assert_eq!(snapshot.spread, Decimal::ONE);
        assert_eq!(
            snapshot.classic_microprice,
            d("100.66666666666666666666666667")
        );
        assert_eq!(snapshot.imbalance_l1, d("0.3333333333333333333333333333"));
        assert_eq!(snapshot.imbalance_l3, Decimal::ZERO);
        assert_eq!(snapshot.imbalance_l400, Decimal::ZERO);
        assert_eq!(
            snapshot.notional_imbalance,
            (d("497") - d("509")) / (d("497") + d("509"))
        );
        assert_eq!(snapshot.epoch, 1);
        assert_eq!(snapshot.generation, 1);
        Ok(())
    }

    #[test]
    fn instrument_identity_is_explicit_preserved_and_formula_independent()
    -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let btc = instrument("BTC-USDT");
        let usdc = instrument("USDC-USDT");
        let mut btc_book = OkxLevel2Book::new(btc.clone());
        let mut usdc_book = OkxLevel2Book::new(usdc.clone());
        let btc_features = features(btc_book.apply(update_for(
            btc.clone(),
            OkxLevel2Action::Snapshot,
            100,
            -1,
            vec![level("100", "2"), level("99", "3")],
            vec![level("101", "1"), level("102", "4")],
            now,
        ))?);
        let usdc_features = features(usdc_book.apply(update_for(
            usdc.clone(),
            OkxLevel2Action::Snapshot,
            100,
            -1,
            vec![level("100", "2"), level("99", "3")],
            vec![level("101", "1"), level("102", "4")],
            now,
        ))?);

        assert_eq!(btc_features.instrument_id, btc);
        assert_eq!(usdc_features.instrument_id, usdc);
        let mut normalized_usdc = (*usdc_features).clone();
        normalized_usdc.instrument_id = btc.clone();
        assert_eq!(*btc_features, normalized_usdc);

        btc_book.apply(update_for(
            btc.clone(),
            OkxLevel2Action::Update,
            101,
            100,
            vec![level("100", "3")],
            Vec::new(),
            now + Duration::from_millis(100),
        ))?;
        assert_eq!(btc_book.rolling.points.len(), 2);
        assert_eq!(usdc_book.rolling.points.len(), 1);

        let before = usdc_book.features().expect("USDC features");
        assert!(matches!(
            usdc_book.apply(update_for(
                btc,
                OkxLevel2Action::Update,
                101,
                100,
                vec![level("100", "3")],
                Vec::new(),
                now + Duration::from_millis(100),
            )),
            Err(OkxLevel2BookError::InstrumentMismatch { .. })
        ));
        assert_eq!(usdc_book.features().expect("unchanged features"), before);
        assert_eq!(usdc_book.rolling.points.len(), 1);

        usdc_book.invalidate();
        assert_eq!(usdc_book.instrument_id().as_str(), "USDC-USDT");
        assert!(usdc_book.features().is_none());
        Ok(())
    }

    #[test]
    fn increment_inserts_updates_and_removes_levels() -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let mut book = test_book();
        book.apply(snapshot(now))?;
        let changed = features(book.apply(update(
            OkxLevel2Action::Update,
            101,
            100,
            vec![level("100.5", "5"), level("100", "1")],
            vec![level("101", "0"), level("101.5", "2")],
            now + Duration::from_millis(100),
        ))?);
        assert_eq!(changed.best_bid, d("100.5"));
        assert_eq!(changed.best_ask, d("101.5"));
        assert_eq!(changed.bid_level_count, 3);
        assert_eq!(changed.ask_level_count, 2);
        assert_eq!(changed.sequence_id, 101);
        Ok(())
    }

    #[test]
    fn sequence_gap_invalidates_until_fresh_snapshot() -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let mut book = test_book();
        book.apply(snapshot(now))?;
        let error = book
            .apply(update(
                OkxLevel2Action::Update,
                102,
                101,
                vec![level("100", "1")],
                Vec::new(),
                now,
            ))
            .expect_err("gap must fail");
        assert!(error.is_sequence_gap());
        assert_eq!(book.instrument_id().as_str(), "BTC-USDT");
        assert!(book.features().is_none());
        assert_eq!(
            book.apply(update(
                OkxLevel2Action::Update,
                103,
                102,
                Vec::new(),
                Vec::new(),
                now,
            )),
            Err(OkxLevel2BookError::UpdateBeforeSnapshot)
        );
        let replacement = features(book.apply(snapshot(now))?);
        assert_eq!(replacement.epoch, 2);
        Ok(())
    }

    #[test]
    fn empty_same_sequence_update_is_a_heartbeat() -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let mut book = test_book();
        book.apply(snapshot(now))?;
        assert_eq!(
            book.apply(update(
                OkxLevel2Action::Update,
                100,
                100,
                Vec::new(),
                Vec::new(),
                now,
            ))?,
            OkxLevel2ApplyOutcome::Heartbeat
        );
        assert_eq!(book.features().expect("features").generation, 1);
        Ok(())
    }

    #[test]
    fn reconnect_invalidation_requires_new_snapshot() -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let mut book = test_book();
        book.apply(snapshot(now))?;
        book.invalidate();
        assert!(matches!(
            book.apply(update(
                OkxLevel2Action::Update,
                101,
                100,
                Vec::new(),
                Vec::new(),
                now,
            )),
            Err(OkxLevel2BookError::UpdateBeforeSnapshot)
        ));
        Ok(())
    }

    #[test]
    fn rejects_crossed_empty_duplicate_and_unknown_removal_books() {
        let now = Instant::now();
        let mut crossed = test_book();
        assert!(matches!(
            crossed.apply(update(
                OkxLevel2Action::Snapshot,
                1,
                -1,
                vec![level("101", "1")],
                vec![level("101", "1")],
                now,
            )),
            Err(OkxLevel2BookError::CrossedBook { .. })
        ));
        let mut empty = test_book();
        assert_eq!(
            empty.apply(update(
                OkxLevel2Action::Snapshot,
                1,
                -1,
                Vec::new(),
                vec![level("101", "1")],
                now,
            )),
            Err(OkxLevel2BookError::EmptySide)
        );
        let mut duplicate = test_book();
        assert_eq!(
            duplicate.apply(update(
                OkxLevel2Action::Snapshot,
                1,
                -1,
                vec![level("100", "1"), level("100", "2")],
                vec![level("101", "1")],
                now,
            )),
            Err(OkxLevel2BookError::DuplicatePrice(d("100")))
        );
        let mut removal = test_book();
        removal.apply(snapshot(now)).expect("snapshot");
        assert_eq!(
            removal.apply(update(
                OkxLevel2Action::Update,
                101,
                100,
                vec![level("98", "0")],
                Vec::new(),
                now,
            )),
            Err(OkxLevel2BookError::UnknownRemoval(d("98")))
        );
    }

    #[test]
    fn accepts_four_hundred_levels_and_rejects_larger_snapshot() {
        let now = Instant::now();
        let bids = (0..400)
            .map(|index| level(&(1000 - index).to_string(), "1"))
            .collect();
        let asks = (0..400)
            .map(|index| level(&(1001 + index).to_string(), "1"))
            .collect();
        let mut book = test_book();
        let snapshot = features(
            book.apply(update(OkxLevel2Action::Snapshot, 1, -1, bids, asks, now))
                .expect("400 levels"),
        );
        assert_eq!(snapshot.bid_level_count, 400);
        assert_eq!(snapshot.ask_level_count, 400);

        let too_many = (0..401)
            .map(|index| level(&(1000 - index).to_string(), "1"))
            .collect();
        assert_eq!(
            book.apply(update(
                OkxLevel2Action::Snapshot,
                2,
                -1,
                too_many,
                vec![level("1001", "1")],
                now,
            )),
            Err(OkxLevel2BookError::DepthOverflow)
        );
    }

    #[test]
    fn feature_output_is_deterministic_and_uses_bounded_rolling_state()
    -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let mut first = test_book();
        let mut second = test_book();
        let mut first_features = (*features(first.apply(snapshot(now))?)).clone();
        let second_features = features(second.apply(snapshot(now))?);
        // Monotonic processing timestamps intentionally differ between runs;
        // normalize only those observation fields before comparing formulas.
        first_features.book_applied_at = second_features.book_applied_at;
        first_features.features_ready_at = second_features.features_ready_at;
        assert_eq!(first_features, *second_features);
        for sequence in 101..=200 {
            first.apply(update(
                OkxLevel2Action::Update,
                sequence,
                sequence - 1,
                vec![level("100", if sequence % 2 == 0 { "2" } else { "3" })],
                Vec::new(),
                now + Duration::from_millis((sequence - 100) as u64 * 100),
            ))?;
        }
        assert_eq!(first.rolling.points.len(), FEATURE_ROLLING_WINDOW);
        Ok(())
    }

    #[test]
    fn malformed_decimal_level_is_rejected_during_protocol_parse() {
        let error = serde_json::from_str::<OkxLevel2Data>(
            r#"{"asks":[["bad","1","0","1"]],"bids":[],"ts":"1700000000000","seqId":1,"prevSeqId":-1}"#,
        )
        .expect_err("invalid decimal");
        assert!(error.to_string().contains("price must be a Decimal"));
    }

    #[test]
    fn level2_depth_imbalances_weighting_and_notional_use_exact_decimal_formulas()
    -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let bids = (0..400)
            .map(|index| level(&(10_000 - index).to_string(), "2"))
            .collect();
        let asks = (0..400)
            .map(|index| level(&(10_001 + index).to_string(), "1"))
            .collect();
        let mut book = test_book();
        let features =
            features(book.apply(update(OkxLevel2Action::Snapshot, 1, -1, bids, asks, now))?);
        let one_third = Decimal::ONE / Decimal::from(3_u64);
        for imbalance in [
            features.imbalance_l1,
            features.imbalance_l3,
            features.imbalance_l5,
            features.imbalance_l10,
            features.imbalance_l25,
            features.imbalance_l50,
            features.imbalance_l100,
            features.imbalance_l200,
            features.imbalance_l400,
        ] {
            assert_eq!(imbalance, one_third);
        }
        assert_eq!(
            features.distance_weighted_imbalance,
            d("0.3333333333333333333333333334")
        );
        assert_eq!(
            features.notional_imbalance,
            book.notional_imbalance(OKX_LEVEL2_MAX_DEPTH)
        );
        Ok(())
    }

    #[test]
    fn level2_depth_to_move_and_zero_denominators_are_deterministic()
    -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let mut book = test_book();
        let features = features(book.apply(update(
            OkxLevel2Action::Snapshot,
            1,
            -1,
            vec![
                level("99.99", "2"),
                level("99.98", "3"),
                level("99.95", "5"),
            ],
            vec![
                level("100.01", "4"),
                level("100.02", "6"),
                level("100.05", "8"),
            ],
            now,
        ))?);
        assert_eq!(features.mid, d("100"));
        assert_eq!(features.depth_move_1bps.bid_quantity, d("2"));
        assert_eq!(features.depth_move_1bps.ask_quantity, d("4"));
        assert_eq!(features.depth_move_2bps.bid_quantity, d("5"));
        assert_eq!(features.depth_move_2bps.ask_quantity, d("10"));
        assert_eq!(features.depth_move_5bps.bid_quantity, d("10"));
        assert_eq!(features.depth_move_5bps.ask_quantity, d("18"));
        assert_eq!(signed_ratio(Decimal::ZERO, Decimal::ZERO), Decimal::ZERO);
        assert_eq!(positive_ratio(Decimal::ONE, Decimal::ZERO), Decimal::ZERO);
        Ok(())
    }

    #[test]
    fn level2_invalid_replacement_snapshot_clears_prior_epoch() -> Result<(), OkxLevel2BookError> {
        let now = Instant::now();
        let mut book = test_book();
        book.apply(snapshot(now))?;
        let error = book
            .apply(update(
                OkxLevel2Action::Snapshot,
                200,
                -1,
                vec![level("102", "1")],
                vec![level("101", "1")],
                now,
            ))
            .expect_err("crossed replacement snapshot");
        assert!(matches!(error, OkxLevel2BookError::CrossedBook { .. }));
        assert!(book.features().is_none());
        assert_eq!(
            book.apply(update(
                OkxLevel2Action::Update,
                201,
                200,
                vec![level("100", "1")],
                Vec::new(),
                now,
            )),
            Err(OkxLevel2BookError::UpdateBeforeSnapshot)
        );
        Ok(())
    }

    #[test]
    #[ignore = "performance-only; run explicitly with --release --ignored --nocapture"]
    fn level2_book_processing_benchmark() -> Result<(), OkxLevel2BookError> {
        const UPDATE_COUNT: i64 = 2_000;
        let now = Instant::now();
        let instrument_id = instrument("BTC-USDT");
        let bids = (0..400)
            .map(|index| level(&(10_000 - index).to_string(), "2"))
            .collect();
        let asks = (0..400)
            .map(|index| level(&(10_001 + index).to_string(), "2"))
            .collect();
        let mut book = OkxLevel2Book::new(instrument_id.clone());
        book.apply(update_for(
            instrument_id.clone(),
            OkxLevel2Action::Snapshot,
            1,
            -1,
            bids,
            asks,
            now,
        ))?;

        let mut samples = Vec::with_capacity(UPDATE_COUNT as usize);
        for offset in 1..=UPDATE_COUNT {
            let started_at = Instant::now();
            book.apply(update_for(
                instrument_id.clone(),
                OkxLevel2Action::Update,
                offset + 1,
                offset,
                vec![level("10000", if offset % 2 == 0 { "2" } else { "3" })],
                Vec::new(),
                now + Duration::from_millis(offset as u64 * 100),
            ))?;
            samples.push(started_at.elapsed().as_micros() as u64);
        }
        samples.sort_unstable();
        println!(
            "level2_book_processing: updates={} min={}us p50={}us p95={}us p99={}us max={}us",
            samples.len(),
            samples[0],
            percentile_micros(&samples, 50),
            percentile_micros(&samples, 95),
            percentile_micros(&samples, 99),
            samples[samples.len() - 1]
        );
        Ok(())
    }

    fn percentile_micros(sorted: &[u64], percentile: usize) -> u64 {
        let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
        sorted[rank.saturating_sub(1).min(sorted.len().saturating_sub(1))]
    }
}
