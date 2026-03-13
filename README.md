# XAUUSD Swing-Structure Trading Bot

Automated trading bot for XAUUSD on MT5/Exness using a custom swing-structure
pattern recognition strategy (H1-L1-A-B-C-D breakout).

This repository is configured for **XAUUSD-only trading** and does **not** include derivatives contract trading.

## Architecture

```
Crypto_Lambo2026/
├── config/
│   └── settings.py            # All tuneable parameters (dataclasses)
├── core/
│   ├── mt5_client.py          # MT5 connection & market data
│   ├── order_manager.py       # Order execution & position management
│   └── risk_manager.py        # Spread filter, lot sizing, daily-loss guard
├── strategies/
│   ├── base.py                # Abstract strategy interface
│   └── swing_structure.py     # Bullish + Bearish H1-L1-A-B-C-D engine
├── utils/
│   └── logger.py              # Centralised logging
├── main.py                    # Entry point
├── requirements.txt
├── .env.example
└── .gitignore
```

## Strategy

The bot runs **two state machines in parallel** on each closed M5 bar:

| Direction | Pattern | Entry trigger |
|-----------|---------|---------------|
| Bullish   | L1 → H1 → A → B → C → **D** | Price breaks **above** D |
| Bearish   | H1 → L1 → A → B → C → **D** | Price breaks **below** D |

- **SL** is placed beyond point C (with buffer)
- **TP** is calculated from a configurable risk:reward ratio (default 1:2)
- One position at a time; opposite signal closes the current trade

## Risk Management

| Guard | Default |
|-------|---------|
| Risk per trade | 0.5 % of balance |
| Max daily loss | 2.0 % of balance |
| Max spread | 120 points |
| Lot sizing | Automatic (risk-based) |

## Prerequisites

- Windows with MetaTrader 5 installed
- Logged in to your Exness account in MT5
- Python 3.10+
- Algo trading enabled in MT5 terminal settings

## Quick Start

```powershell
pip install -r requirements.txt
python main.py
```

Logs are written to `logs/trading_bot.log` and the console.

## Configuration

Edit `config/settings.py` to adjust:

- `MT5Config` — symbol, timeframe, deviation, loop interval
- `RiskConfig` — risk %, daily loss cap, spread limit
- `SwingStructureConfig` — bars to load, entry buffer, R:R ratio

## Safety

- **Use demo accounts** until you are confident the strategy is profitable.
- Keep MT5 open while the bot is running.
- This is a trading tool, not financial advice.

## MCX Gold Live Ticks (Angel One)

The project now includes an Angel One bridge and MCX GOLD futures token resolver:

1. Set `ANGEL_*` credentials and `MCX_*` filters in `.env` (see `.env.example`).
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the live tick listener:

```bash
python main_mcx_gold_ticks.py
```

This resolves the nearest `GOLD` MCX futures token from ScripMaster and streams live ticks.

## Dual Feed Candle Pipeline (XAUUSD + MCX)

Run unified dual-feed ingestion and candle building on `1m,3m,5m,10m,15m`:

```bash
python main_dual_feed.py
```

This starts:
- Primary feed: `XAUUSD` from OANDA pricing (`OANDA_ENV`, `OANDA_ACCOUNT_ID`, `OANDA_API_TOKEN`, `OANDA_INSTRUMENT`)
- Secondary feed: MCX GOLD futures from Angel websocket
- Unified candle engine for both feeds
- Signal pipeline: divergence detection on `10m`, entry trigger on `3m`
- Signal journaling at `logs/signal_journal.csv` (phase-1, no live order execution)
