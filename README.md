# wallet-trading-audit

Local Python scaffold for a personal cross-chain trading analytics system focused on Solana and BNB EVM wallet activity.

This repository is intentionally **not** a web app, dashboard, or API server. The current scope is a local analysis engine that:

- ingests raw wallet transaction data
- normalizes Solana and EVM transactions into a unified schema
- reconstructs trades using FIFO cost basis
- computes real trading performance metrics
- supports behavioral analysis such as overtrading, re-entry, profit capture, and drawdowns

The current commit only establishes the project structure, canonical schema, metric definitions, and report placeholders. Real ingestion and PnL logic are intentionally deferred.

## Design Principles

- Auditability over convenience
- Contract or mint addresses only, never token symbols, for asset identity
- No guessed provider fields, endpoints, or chain-specific payload layouts
- Explicit `TODO` markers anywhere real Solana or EVM payload mapping is still undecided
- Deterministic, replayable transformations from `data/raw/` to `data/processed/`

## Project Layout

```text
wallet-trading-audit/
  src/
    ingestion/
    normalize/
    pnl/
    analytics/
    reports/
  data/
    raw/
    processed/
    exports/
  notebooks/
  tests/
```

## Canonical Transaction Schema

The minimum normalized transaction record is defined in [src/normalize/schema.py](/Users/slimeball/Documents/Coding Projects/wallet-trading-audit/src/normalize/schema.py). Field definitions are captured explicitly in `CANONICAL_TRANSACTION_SCHEMA`, and the typed record is represented by `NormalizedTransaction`.

| Field | Type | Notes |
| --- | --- | --- |
| `chain` | enum | `solana` or `bnb_evm` |
| `wallet` | string | Wallet under analysis |
| `tx_hash` | string | Transaction signature/hash as provided by the chain |
| `block_time` | timezone-aware datetime | UTC-normalized event timestamp |
| `token_in_address` | string or null | Contract/mint address for the incoming leg |
| `token_out_address` | string or null | Contract/mint address for the outgoing leg |
| `amount_in` | decimal | Explicitly set to `0` when absent |
| `amount_out` | decimal | Explicitly set to `0` when absent |
| `usd_value` | decimal or null | Null until trusted valuation exists |
| `fee_native` | decimal | Native chain fee in the chain's gas token |
| `fee_usd` | decimal or null | Null until trusted fee valuation exists |
| `event_type` | enum | `swap`, `transfer`, `fee`, or `unknown` |
| `source` | string or null | DEX or protocol name if known |

Notes:

- The schema uses wallet-relative directionality. `token_in_address` and `amount_in` refer to the asset entering the analyzed wallet, while `token_out_address` and `amount_out` refer to the asset leaving it.
- The schema requires the columns to exist even when an event is one-sided. In those cases, the missing token leg stays `null` and the corresponding amount is `0`.
- Additional deterministic ordering fields may be needed later if a single transaction expands into multiple normalized rows with the same timestamp. That extension is intentionally deferred until raw payloads are available.
- `TODO` markers are intentional anywhere Solana instruction decoding, EVM log decoding, native-vs-wrapped handling, or protocol naming would otherwise require assumptions.

## Metric Definitions

Metric formulas and implementation placeholders live in [src/analytics/metrics.py](/Users/slimeball/Documents/Coding Projects/wallet-trading-audit/src/analytics/metrics.py).

- PnL is split into `realized_pnl`, `unrealized_pnl`, and `net_pnl`.
- Fees are defined via `total_fees`, with explicit `TODO`s when native-fee USD conversion still needs price data.
- Trade quality metrics include `win_rate`, `avg_win`, `avg_loss`, `profit_factor`, and `expectancy`.
- Equity-risk metrics include `max_drawdown`, which is intentionally a placeholder until the project chooses the canonical equity-curve construction.
- Behavior and execution capture metrics include `reentry_behavior`, `capture_ratio`, and `giveback_ratio`.

## Sample Dataset

A tiny normalized transaction fixture lives at [tests/fixtures/normalized_transactions_sample.csv](/Users/slimeball/Documents/Coding Projects/wallet-trading-audit/tests/fixtures/normalized_transactions_sample.csv).

It intentionally includes:

- Solana and BNB EVM rows
- buys, partial sells, and full exits
- one-sided transfer activity
- a standalone fee event
- a full exit followed by a re-entry

The fixture is small enough to inspect by hand and is validated by unit tests before future FIFO or analytics logic is added.

## Module Status

- [src/ingestion](/Users/slimeball/Documents/Coding Projects/wallet-trading-audit/src/ingestion): raw source interfaces and Solana/BNB EVM ingestion placeholders
- [src/normalize](/Users/slimeball/Documents/Coding Projects/wallet-trading-audit/src/normalize): canonical schema and normalizer interfaces
- [src/pnl](/Users/slimeball/Documents/Coding Projects/wallet-trading-audit/src/pnl): FIFO lot and trade reconstruction interfaces
- [src/analytics](/Users/slimeball/Documents/Coding Projects/wallet-trading-audit/src/analytics): metric definitions and formulas
- [src/reports](/Users/slimeball/Documents/Coding Projects/wallet-trading-audit/src/reports): export/report definitions and placeholders
- [tests](/Users/slimeball/Documents/Coding Projects/wallet-trading-audit/tests): lightweight scaffold verification only

## Phased Implementation Plan

1. Raw ingestion
   Capture verbatim Solana and BNB EVM wallet transaction payloads into `data/raw/` with immutable snapshots and provenance metadata.
2. Deterministic normalization
   Map raw payloads into the canonical schema without guessing fields. Add explicit per-chain transform tests using saved fixtures.
3. FIFO trade reconstruction
   Build lot opening, lot closing, and partial-fill handling with deterministic ordering rules and fee allocation strategy.
4. Price enrichment
   Add a pricing layer for historical USD valuation, fee conversion, and open-position marks needed for unrealized PnL and advanced capture metrics.
5. Performance analytics
   Compute net, realized, and unrealized PnL plus win/loss, expectancy, volume, and token or wallet-level breakdowns.
6. Behavioral analytics
   Add re-entry detection, trade-sequence analysis, drawdown framing, and profit-capture vs giveback metrics.
7. Reports and exports
   Produce flat CSV or parquet outputs for normalized transactions, FIFO roundtrips, open positions, and metric summaries.

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
python -m unittest discover -s tests
```

## Environment Setup

Create a local `.env` file before running real ingestion:

```bash
cp .env.example .env
```

Required variables:

- `ENV`: local environment label such as `dev` or `prod`
- `HELIUS_API_KEY`: required for Solana ingestion unless you set `SOLANA_RPC_URL`
- `ETHERSCAN_API_KEY`: required for the current BNB/EVM ingestion flow

Optional variables:

- `SOLANA_RPC_URL`: explicit Solana RPC override if you do not want to derive it from `HELIUS_API_KEY`
- `EVM_RPC_URL`: reserved for future direct EVM RPC ingestion

Example ingestion usage after configuring `.env`:

```bash
cp .env.example .env
python scripts/fetch_from_wallet_manifest.py
```

## Current Limitations

- No ingestion backends are implemented yet
- No Solana or EVM field mapping is assumed yet
- No FIFO reconstruction logic is implemented yet
- No pricing, PnL, or reporting pipeline is implemented yet
