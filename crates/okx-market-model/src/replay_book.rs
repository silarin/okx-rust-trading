use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use okx_public_protocol::OkxSpotInstrumentId;
use rust_decimal::Decimal;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookAction {
    Snapshot,
    Update,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LevelUpdate {
    pub price: Decimal,
    pub quantity: Decimal,
    pub order_count: u64,
}

pub trait SequencedBookEventView {
    fn instrument_id(&self) -> &OkxSpotInstrumentId;
    fn action(&self) -> BookAction;
    fn asks(&self) -> &[LevelUpdate];
    fn bids(&self) -> &[LevelUpdate];
    fn sequence_epoch(&self) -> u64;
    fn sequence_id(&self) -> i64;
}

pub trait HistoricalBookEventView {
    fn instrument_id(&self) -> &OkxSpotInstrumentId;
    fn action(&self) -> BookAction;
    fn asks(&self) -> &[LevelUpdate];
    fn bids(&self) -> &[LevelUpdate];
    fn research_epoch(&self) -> u64;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBook {
    instrument_id: OkxSpotInstrumentId,
    bids: BTreeMap<Decimal, BookLevel>,
    asks: BTreeMap<Decimal, BookLevel>,
    trusted: bool,
    has_snapshot: bool,
    sequence_epoch: Option<u64>,
    sequence_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BookLevel {
    quantity: Decimal,
    order_count: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ApplyOutcome {
    pub invalid_levels: u64,
    pub unknown_removals: u64,
    pub duplicate_levels: u64,
    pub non_monotonic_snapshot_sides: u64,
    pub update_before_snapshot: u64,
    pub depth_overflow: u64,
    pub empty_sides: u64,
    pub crossed: u64,
}

impl OrderBook {
    #[must_use]
    pub fn new(instrument_id: OkxSpotInstrumentId) -> Self {
        Self {
            instrument_id,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            trusted: false,
            has_snapshot: false,
            sequence_epoch: None,
            sequence_id: None,
        }
    }

    #[must_use]
    pub const fn instrument_id(&self) -> &OkxSpotInstrumentId {
        &self.instrument_id
    }

    /// Clears one replay epoch without changing the instrument bound to this
    /// book.
    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.trusted = false;
        self.has_snapshot = false;
        self.sequence_epoch = None;
        self.sequence_id = None;
    }

    pub fn mark_untrusted(&mut self) {
        self.trusted = false;
    }

    pub fn apply(
        &mut self,
        event: &impl SequencedBookEventView,
    ) -> Result<ApplyOutcome, OkxReplayBookError> {
        self.ensure_instrument(event.instrument_id())?;
        let mut outcome = self.apply_levels(event.action(), event.asks(), event.bids());
        self.sequence_epoch = Some(event.sequence_epoch());
        self.sequence_id = Some(event.sequence_id());
        self.finish_apply(&mut outcome);
        Ok(outcome)
    }

    pub fn apply_historical(
        &mut self,
        event: &impl HistoricalBookEventView,
    ) -> Result<ApplyOutcome, OkxReplayBookError> {
        self.ensure_instrument(event.instrument_id())?;
        let mut outcome = self.apply_levels(event.action(), event.asks(), event.bids());
        // Official archives do not contain seqId/prevSeqId. Keep the daily
        // research epoch for isolation without inventing WebSocket identity.
        self.sequence_epoch = Some(event.research_epoch());
        self.sequence_id = None;
        self.finish_apply(&mut outcome);
        Ok(outcome)
    }

    fn ensure_instrument(&self, received: &OkxSpotInstrumentId) -> Result<(), OkxReplayBookError> {
        if received == &self.instrument_id {
            Ok(())
        } else {
            Err(OkxReplayBookError::InstrumentMismatch {
                expected: self.instrument_id.clone(),
                received: received.clone(),
            })
        }
    }

    fn apply_levels(
        &mut self,
        action: BookAction,
        asks: &[LevelUpdate],
        bids: &[LevelUpdate],
    ) -> ApplyOutcome {
        let mut outcome = ApplyOutcome::default();
        if action == BookAction::Snapshot {
            self.bids.clear();
            self.asks.clear();
            self.trusted = true;
            self.has_snapshot = true;
            outcome.non_monotonic_snapshot_sides += u64::from(!strictly_increasing(asks));
            outcome.non_monotonic_snapshot_sides += u64::from(!strictly_decreasing(bids));
        } else if !self.has_snapshot {
            outcome.update_before_snapshot += 1;
            self.trusted = false;
        }

        apply_side(&mut self.asks, asks, &mut outcome);
        apply_side(&mut self.bids, bids, &mut outcome);
        outcome
    }

    fn finish_apply(&mut self, outcome: &mut ApplyOutcome) {
        if self.bids.len() > 400 || self.asks.len() > 400 {
            outcome.depth_overflow += 1;
        }
        if outcome.invalid_levels > 0
            || outcome.duplicate_levels > 0
            || outcome.non_monotonic_snapshot_sides > 0
            || outcome.update_before_snapshot > 0
            || outcome.depth_overflow > 0
        {
            self.trusted = false;
        }

        if self.bids.is_empty() || self.asks.is_empty() {
            outcome.empty_sides += 1;
            self.trusted = false;
        } else if self
            .best_bid()
            .is_some_and(|bid| self.best_ask().is_some_and(|ask| bid.price >= ask.price))
        {
            outcome.crossed += 1;
            self.trusted = false;
        }
    }

    pub fn is_trusted(&self) -> bool {
        self.trusted
    }

    pub fn best_bid(&self) -> Option<TopLevel> {
        self.bids.last_key_value().map(|(price, level)| TopLevel {
            price: *price,
            quantity: level.quantity,
        })
    }

    pub fn best_ask(&self) -> Option<TopLevel> {
        self.asks.first_key_value().map(|(price, level)| TopLevel {
            price: *price,
            quantity: level.quantity,
        })
    }

    pub fn spread(&self) -> Option<Decimal> {
        Some(self.best_ask()?.price - self.best_bid()?.price)
    }

    pub fn midpoint(&self) -> Option<Decimal> {
        Some((self.best_ask()?.price + self.best_bid()?.price) / Decimal::TWO)
    }

    pub fn microprice(&self) -> Option<Decimal> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        let quantity = bid.quantity + ask.quantity;
        (quantity > Decimal::ZERO)
            .then(|| (ask.price * bid.quantity + bid.price * ask.quantity) / quantity)
    }

    pub fn top_imbalance(&self) -> Option<Decimal> {
        self.depth_imbalance(1)
    }

    pub fn depth_imbalance(&self, levels: usize) -> Option<Decimal> {
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
        let total = bid + ask;
        (levels > 0 && total > Decimal::ZERO).then(|| (bid - ask) / total)
    }

    pub fn total_depth(&self, levels: usize) -> Decimal {
        self.bids
            .iter()
            .rev()
            .take(levels)
            .chain(self.asks.iter().take(levels))
            .map(|(_, level)| level.quantity)
            .sum()
    }

    pub fn weighted_microprice(&self, levels: usize) -> Option<Decimal> {
        let mut weighted_price = Decimal::ZERO;
        let mut weight_sum = Decimal::ZERO;
        for (index, ((bid_price, bid), (ask_price, ask))) in self
            .bids
            .iter()
            .rev()
            .take(levels)
            .zip(self.asks.iter().take(levels))
            .enumerate()
        {
            let quantity = bid.quantity + ask.quantity;
            if quantity <= Decimal::ZERO {
                continue;
            }
            let rank_weight = Decimal::ONE / Decimal::from((index + 1) as u64);
            let level_microprice =
                (*ask_price * bid.quantity + *bid_price * ask.quantity) / quantity;
            weighted_price += level_microprice * rank_weight;
            weight_sum += rank_weight;
        }
        (weight_sum > Decimal::ZERO).then(|| weighted_price / weight_sum)
    }

    pub fn multi_level_microprice(&self, levels: usize) -> Option<Decimal> {
        let midpoint = self.midpoint()?;
        let spread = self.spread()?;
        Some(midpoint + spread * self.depth_imbalance(levels)? / Decimal::TWO)
    }

    pub fn bid_level_count(&self) -> usize {
        self.bids.len()
    }

    pub fn ask_level_count(&self) -> usize {
        self.asks.len()
    }

    pub fn bid_quantity_at(&self, price: Decimal) -> Decimal {
        self.bids
            .get(&price)
            .map_or(Decimal::ZERO, |level| level.quantity)
    }

    pub fn ask_quantity_at(&self, price: Decimal) -> Decimal {
        self.asks
            .get(&price)
            .map_or(Decimal::ZERO, |level| level.quantity)
    }

    pub fn asks_ascending(&self) -> impl Iterator<Item = TopLevel> + '_ {
        self.asks.iter().map(|(price, level)| TopLevel {
            price: *price,
            quantity: level.quantity,
        })
    }

    pub fn bids_descending(&self) -> impl Iterator<Item = TopLevel> + '_ {
        self.bids.iter().rev().map(|(price, level)| TopLevel {
            price: *price,
            quantity: level.quantity,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxReplayBookError {
    InstrumentMismatch {
        expected: OkxSpotInstrumentId,
        received: OkxSpotInstrumentId,
    },
}

impl fmt::Display for OkxReplayBookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InstrumentMismatch { expected, received } => write!(
                formatter,
                "OKX replay book instrument mismatch: expected {expected}, received {received}"
            ),
        }
    }
}

impl Error for OkxReplayBookError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

fn apply_side(
    side: &mut BTreeMap<Decimal, BookLevel>,
    updates: &[LevelUpdate],
    outcome: &mut ApplyOutcome,
) {
    let mut seen = BTreeSet::new();
    for update in updates {
        if !seen.insert(update.price) {
            outcome.duplicate_levels += 1;
            continue;
        }
        if update.price <= Decimal::ZERO || update.quantity < Decimal::ZERO {
            outcome.invalid_levels += 1;
            continue;
        }
        if update.quantity == Decimal::ZERO {
            if side.remove(&update.price).is_none() {
                outcome.unknown_removals += 1;
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
}

fn strictly_increasing(levels: &[LevelUpdate]) -> bool {
    levels.windows(2).all(|pair| pair[0].price < pair[1].price)
}

fn strictly_decreasing(levels: &[LevelUpdate]) -> bool {
    levels.windows(2).all(|pair| pair[0].price > pair[1].price)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use pretty_assertions::assert_eq;

    use super::*;

    #[derive(Clone)]
    struct SequencedEvent {
        instrument_id: OkxSpotInstrumentId,
        action: BookAction,
        asks: Vec<LevelUpdate>,
        bids: Vec<LevelUpdate>,
        sequence_epoch: u64,
        sequence_id: i64,
    }

    impl SequencedBookEventView for SequencedEvent {
        fn instrument_id(&self) -> &OkxSpotInstrumentId {
            &self.instrument_id
        }

        fn action(&self) -> BookAction {
            self.action
        }

        fn asks(&self) -> &[LevelUpdate] {
            &self.asks
        }

        fn bids(&self) -> &[LevelUpdate] {
            &self.bids
        }

        fn sequence_epoch(&self) -> u64 {
            self.sequence_epoch
        }

        fn sequence_id(&self) -> i64 {
            self.sequence_id
        }
    }

    #[derive(Clone)]
    struct HistoricalEvent {
        instrument_id: OkxSpotInstrumentId,
        action: BookAction,
        asks: Vec<LevelUpdate>,
        bids: Vec<LevelUpdate>,
        research_epoch: u64,
    }

    impl HistoricalBookEventView for HistoricalEvent {
        fn instrument_id(&self) -> &OkxSpotInstrumentId {
            &self.instrument_id
        }

        fn action(&self) -> BookAction {
            self.action
        }

        fn asks(&self) -> &[LevelUpdate] {
            &self.asks
        }

        fn bids(&self) -> &[LevelUpdate] {
            &self.bids
        }

        fn research_epoch(&self) -> u64 {
            self.research_epoch
        }
    }

    fn instrument(value: &str) -> OkxSpotInstrumentId {
        OkxSpotInstrumentId::try_from(value).expect("valid test instrument")
    }

    fn level(price: &str, quantity: &str) -> LevelUpdate {
        LevelUpdate {
            price: Decimal::from_str(price).expect("price"),
            quantity: Decimal::from_str(quantity).expect("quantity"),
            order_count: 1,
        }
    }

    fn sequenced_snapshot(instrument_id: OkxSpotInstrumentId) -> SequencedEvent {
        SequencedEvent {
            instrument_id,
            action: BookAction::Snapshot,
            asks: vec![level("101", "1")],
            bids: vec![level("100", "2")],
            sequence_epoch: 7,
            sequence_id: 100,
        }
    }

    fn historical_snapshot(
        instrument_id: OkxSpotInstrumentId,
        research_epoch: u64,
    ) -> HistoricalEvent {
        HistoricalEvent {
            instrument_id,
            action: BookAction::Snapshot,
            asks: vec![level("101", "1")],
            bids: vec![level("100", "2")],
            research_epoch,
        }
    }

    #[test]
    fn sequenced_replay_requires_and_preserves_explicit_identity() {
        let btc = instrument("BTC-USDT");
        let mut book = OrderBook::new(btc.clone());
        assert_eq!(book.instrument_id(), &btc);
        assert_eq!(
            book.apply(&sequenced_snapshot(btc.clone())),
            Ok(ApplyOutcome::default())
        );
        assert!(book.is_trusted());
        assert_eq!(book.best_bid().expect("bid").price, Decimal::from(100));

        book.reset();
        assert_eq!(book.instrument_id(), &btc);
        assert!(!book.is_trusted());
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn mixed_instrument_replay_fails_before_mutation() {
        let btc = instrument("BTC-USDT");
        let usdc = instrument("USDC-USDT");
        let mut book = OrderBook::new(btc.clone());
        assert!(matches!(
            book.apply(&sequenced_snapshot(usdc)),
            Err(OkxReplayBookError::InstrumentMismatch { .. })
        ));
        assert_eq!(book.instrument_id(), &btc);
        assert_eq!(book.bid_level_count(), 0);
        assert_eq!(book.ask_level_count(), 0);
        assert!(!book.is_trusted());
        assert!(matches!(
            book.apply_historical(&historical_snapshot(instrument("USDC-USDT"), 1)),
            Err(OkxReplayBookError::InstrumentMismatch { .. })
        ));
        assert_eq!(book.bid_level_count(), 0);
    }

    #[test]
    fn historical_epoch_reset_and_snapshot_replacement_preserve_identity() {
        let usdc = instrument("USDC-USDT");
        let mut book = OrderBook::new(usdc.clone());
        book.apply_historical(&historical_snapshot(usdc.clone(), 1))
            .expect("first daily snapshot");
        assert!(book.is_trusted());
        book.apply_historical(&historical_snapshot(usdc.clone(), 1))
            .expect("replacement snapshot");
        assert_eq!(book.instrument_id(), &usdc);

        book.reset();
        assert_eq!(book.instrument_id(), &usdc);
        book.apply_historical(&historical_snapshot(usdc.clone(), 2))
            .expect("next daily snapshot");
        assert!(book.is_trusted());
        assert_eq!(book.instrument_id(), &usdc);
    }

    #[test]
    fn independent_replay_books_have_independent_state_fingerprints() {
        let btc = instrument("BTC-USDT");
        let usdc = instrument("USDC-USDT");
        let mut btc_book = OrderBook::new(btc.clone());
        let mut usdc_book = OrderBook::new(usdc.clone());
        btc_book
            .apply(&sequenced_snapshot(btc))
            .expect("BTC snapshot");
        usdc_book
            .apply(&sequenced_snapshot(usdc))
            .expect("USDC snapshot");

        assert_eq!(btc_book.best_bid(), usdc_book.best_bid());
        assert_ne!(format!("{btc_book:?}"), format!("{usdc_book:?}"));
    }
}
