#![forbid(unsafe_code)]
//! Deterministic OKX public-market state and bounded Level-2 features.

mod level2;
mod replay_book;

pub use level2::{
    OKX_LEVEL2_BOOKS_CHANNEL, OKX_LEVEL2_MAX_DEPTH, OkxDepthMove, OkxDepthSlope,
    OkxLevel2ApplyOutcome, OkxLevel2Book, OkxLevel2BookError, OkxLevel2FeatureSnapshot,
    OkxLevel2Update, OkxNearBookDepth,
};
pub use okx_public_protocol::{
    OkxLevel2Action, OkxLevel2Data, OkxLevel2Level, OkxSpotInstrumentId,
};
pub use replay_book::{
    ApplyOutcome, BookAction, HistoricalBookEventView, LevelUpdate, OkxReplayBookError, OrderBook,
    SequencedBookEventView, TopLevel,
};
