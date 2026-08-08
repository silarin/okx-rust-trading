# okx-rust-trading

`okx-rust-trading` is a Rust workspace for direct OKX API v5 SPOT trading and deterministic public market-data processing. It keeps exchange interaction OKX-specific and uses exact decimal arithmetic at order and book boundaries.

## Workspace

- `okx-trading-runtime` provides strict TOML configuration, direct REST and WebSocket clients, exact SPOT capability checks, order submission and reconciliation, Cancel-All-After protection, private-stream hints with REST authority, and a compiled EMA/ATR example strategy.
- `okx-public-protocol` validates credential-free OKX public `books` and trade payloads, including exact instrument identity and Decimal fields.
- `okx-market-model` reconstructs bounded Level-2 books deterministically and derives causal book features.

The runtime supports OKX cash-SPOT only. It does not provide a generic exchange adapter.

## Build and test

Rust 1.96.0 is selected by `rust-toolchain.toml`.

```bash
cargo fmt --all --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Configuration and credentials

[`config/example.toml`](config/example.toml) is an inert OKX Demo Trading Services example. It contains fictitious operator/account identifiers, enables no strategy, and does not include an order-intent acknowledgement. Supplying credentials alone does not enable trading.

Credentials are resolved at runtime from these environment variables:

```text
OKX_API_KEY
OKX_API_SECRET
OKX_API_PASSPHRASE
```

Each variable also supports a corresponding `_FILE` variable containing the path to a single-value secret file. Never commit credential values or local operator profiles. Files under `config/` other than the public example are ignored by Git.

An operator who deliberately enables trading must create a complete local profile, add an enabled compiled strategy, choose Demo or Production routing explicitly, and set the matching order-intent acknowledgement. Start with Demo and review every notional, ownership, precision, and cleanup setting before use.

## Level-2 and order-flow capability

The public `books` path validates one exact SPOT instrument, parses prices and quantities as `Decimal`, and reconstructs snapshots plus incremental updates with strict `seqId`/`prevSeqId` continuity. Sequence gaps, regressions, malformed levels, crossed books, and reconnects invalidate the book until a fresh snapshot begins a new epoch.

The market model maintains bounded depth and exposes quantity and notional imbalance, classic and multi-level microprice, near-book depth, depth-to-move, slopes, liquidity-vacuum, imbalance velocity/persistence, and short-horizon book volatility. Runtime delivery is bounded and coalesces Level-2 observations without treating them as order authority.

Book-driven strategies can opt into these deterministic feature snapshots. Future order-flow imbalance logic should consume this shared model instead of reconstructing a second book inside a strategy.

## Trading safety

WebSocket order acknowledgements and private stream events are hints, not final exchange truth. Ambiguous mutations are reconciled through bounded REST lookups using stable client identities. Prices, sizes, notionals, fills, fees, rebates, and balances use exact Decimal arithmetic. Cancel-All-After remains independently refreshed while a strategy runtime is active, and fatal or ambiguous exits fail closed.

Trading digital assets can result in rapid and total loss. Software defects, network failures, stale market data, exchange behavior, and misconfiguration can create unintended orders or positions. This project is provided without financial advice or any guarantee of correctness, availability, or profitability. Use only accounts, permissions, and capital you are prepared to risk.

## License

Licensed under the [MIT License](LICENSE). Copyright (c) 2026 `silarin`.

See [SECURITY.md](SECURITY.md) for credential and vulnerability reporting guidance.
