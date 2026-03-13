# Project Requirements (Draft v2)

## 1. Project Purpose
Build an automated gold trading system using cross-market divergence between:
- Primary symbol: `XAUUSD`
- Secondary symbol: `MCX GOLD FUTURES`

The system should detect divergence setups and execute entries with strict risk controls.

## 2. Core Strategy Flow (Agreed)
1. Fetch live tick data continuously for both `XAUUSD` and `MCX GOLD FUTURES`.
2. Build live OHLC candles for both symbols in these timeframes:
- `1m`, `3m`, `5m`, `10m`, `15m`
3. Use `XAUUSD` as the primary time reference.
4. Detect pivots on `XAUUSD`.
5. For MCX pivots, align by `XAUUSD` pivot time reference (same mapping concept currently used for NIFTY spot vs NIFTY futures).
6. Detect divergence on `10m` timeframe.
7. Once divergence setup is detected, switch to `3m` timeframe for entry execution.
8. Keep all existing entry methods/logic behavior same unless explicitly changed.

## 3. Symbol Mapping Rule
- Previous mental model: `NIFTY spot` vs `NIFTY future`
- New model:
  - `NIFTY spot` equivalent -> `XAUUSD`
  - `NIFTY future` equivalent -> `MCX GOLD FUTURES`

## 4. Functional Requirements
1. System must maintain synchronized multi-timeframe candle streams for both symbols.
2. System must preserve source label per candle/tick (`xauusd` vs `mcx_gold_fut`).
3. Pivot engine must run on primary (`XAUUSD`) and expose pivot timestamps.
4. Secondary pivot evaluation must consume primary pivot timestamps for alignment.
5. Divergence engine must evaluate setups on `10m` data.
6. Entry engine must execute using `3m` context after divergence confirmation.
7. Order/risk module behavior remains same as current entry methods.
8. Logging must include:
- Live tick status for both feeds
- Candle completion events per timeframe
- Pivot detection events
- Divergence detection timestamp
- Entry trigger and order details

## 5. Non-Functional Requirements
- Reliability: reconnect/retry for both data feeds.
- Consistency: deterministic candle construction across restarts.
- Traceability: every trade must be traceable to divergence + entry candle context.
- Configurability: symbols, timeframes, divergence window, and entry timeframe from config/env.

## 6. Current Scope Decisions
- In scope:
- Dual live feed ingestion (`XAUUSD`, `MCX GOLD FUTURES`)
- Multi-timeframe candle builder (`1/3/5/10/15`)
- Pivot + divergence + entry pipeline as above
- Existing entry method compatibility
- Out of scope for now:
- UI redesign
- ML-based signal models
- Multi-asset portfolio expansion beyond this pair

## 7. Success Criteria
- Both live feeds run stably and update candles in all required timeframes.
- Divergence signals are generated on `10m` with correct cross-symbol alignment.
- Entries are triggered only from `3m` execution logic after valid divergence setup.
- Existing risk controls remain active and block invalid orders.
- End-to-end logs prove the full path: tick -> candle -> pivot -> divergence -> entry.

## 8. Implementation Milestones
1. Dual-feed normalizer for `XAUUSD` + `MCX GOLD FUTURES` ticks.
2. Unified multi-timeframe candle engine (`1/3/5/10/15`).
3. Primary pivot service (`XAUUSD`) with timestamp export.
4. Secondary alignment service (MCX using XAUUSD time references).
5. Divergence detector on `10m`.
6. Entry trigger adapter on `3m` using current entry methods.
7. End-to-end paper-trade validation and logs audit.

---
Status: Updated as per agreed live trading flow (XAUUSD primary + MCX GOLD FUT secondary).
