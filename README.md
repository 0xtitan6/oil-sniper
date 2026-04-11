# Oil Sniper

Pyth-fed volatility sniper for oil prediction markets. Watches WTI crude oil via Pyth Network, detects headline-driven volatility spikes, and front-runs prediction market repricing on Polymarket/Kalshi via Synthesis Trade.

## Strategy

Prediction markets on oil (Polymarket) reprice **minutes** after a spot move. Pyth oracle reprices in **milliseconds**. This bot sits in the gap.

```
T+0s     OPEC headline drops
T+0.5s   Pyth WTI feed reprices
T+5s     Bot detects vol spike, fires order on Synthesis
T+120s   Prediction market traders react
T+???    Market fully reprices — we're already in
```

**Target markets:** WTI crude oil strike ladders on Polymarket ("Will WTI hit $120 HIGH in April?", "Will WTI hit $80 LOW in April?", etc.) — 100+ liquid contracts with $1-4M volume each.

**Edge model:** Strike-aware probability shift estimation. When oil moves 1%, an at-the-money contract should shift ~8% in probability. Far out-of-the-money contracts barely move. The bot computes this per-market and only trades when the expected repricing exceeds the current market price by >5%.

## Architecture

```
                     +-----------------+
                     |   Pyth Hermes   |
                     |   (WebSocket)   |
                     +--------+--------+
                              |
                     price updates (sub-second)
                              |
                     +--------v--------+
                     |   Vol Detector  |
                     |  (rolling std)  |
                     +--------+--------+
                              |
                     spike signal (>2.5 sigma)
                              |
                     +--------v--------+
                     | Signal Engine   |
                     | - edge calc     |
                     | - liquidity     |
                     | - depth check   |
                     | - position mgmt |
                     +--------+--------+
                              |
                     limit orders via REST
                              |
                     +--------v--------+
                     | Synthesis Trade |
                     |  (Polymarket)   |
                     +-----------------+
```

### Components

| File | Purpose |
|------|---------|
| `pyth_feed.py` | WebSocket consumer for Pyth Hermes WTI crude price feed |
| `volatility.py` | Rolling standard deviation spike detector with cooldown |
| `signal_engine.py` | Brain — edge calculation, liquidity filter, position management, exits |
| `synthesis_client.py` | Synthesis REST client with retry/backoff and orderbook depth checks |
| `feed_resolver.py` | Auto-detects active WTI front-month contract on Pyth (handles expiry rollover) |
| `trade_store.py` | SQLite-backed persistent position and P&L tracking |
| `health.py` | HTTP health endpoint (`/health`, `/stats`) and watchdog |
| `config.py` | All settings via `SNIPER_*` environment variables |
| `recorder.py` | Data collection — records Pyth prices + Synthesis snapshots for backtesting |
| `backtest.py` | Replays recorded data to validate strategy before going live |
| `main.py` | Production entry point with graceful shutdown |

## Setup

```bash
# Clone and install
cd oil-sniper
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your Synthesis API key (only needed for live trading)
```

### Requirements

- Python 3.9+
- `websockets`, `httpx`, `pydantic`, `pydantic-settings`, `numpy`
- Synthesis Trade API key (for live trading)

## Usage

### Phase 1: Record Data

Run for 3-5 days to collect Pyth prices and prediction market snapshots. This builds the dataset you need to validate the strategy.

```bash
python recorder.py
```

Check status:
```bash
# View log
tail -20 recorder.log

# Query database
python -c "
import sqlite3; db = sqlite3.connect('oil_data.db')
print(db.execute('SELECT COUNT(*) FROM pyth_prices').fetchone()[0], 'prices')
print(db.execute('SELECT COUNT(*) FROM vol_events').fetchone()[0], 'spikes')
print(db.execute('SELECT COUNT(DISTINCT market_id) FROM market_snapshots').fetchone()[0], 'markets')
"
```

### Phase 2: Backtest

Replay recorded data and simulate trades at different hold times:

```bash
python backtest.py                      # default 10min hold
python backtest.py --hold-minutes 5     # faster exits
python backtest.py --hold-minutes 15    # longer holds
python backtest.py --trade-size 100     # larger positions
```

The backtest reports:
- Win rate and Sharpe ratio
- Average P&L per trade
- **Latency gap** — seconds between Pyth spike and prediction market repricing
- Per-trade breakdown with entry/exit prices

**Only proceed to Phase 3 if the backtest shows positive edge.**

### Phase 3: Paper Trade

```bash
python main.py
```

Starts in **dry run mode** by default. Connects to live Pyth and Synthesis feeds, detects real spikes, but only logs what it *would* trade.

Monitor:
```bash
curl http://localhost:8080/health   # component status
curl http://localhost:8080/stats    # trade stats
```

### Phase 4: Live Trading

```bash
SNIPER_DRY_RUN=false SNIPER_SYNTHESIS_API_KEY=your_key python main.py
```

## Configuration

All settings are configurable via environment variables (prefix `SNIPER_`) or `.env` file.

### Vol Detection

| Variable | Default | Description |
|----------|---------|-------------|
| `VOL_SIGMA_THRESHOLD` | `2.5` | Z-score threshold to trigger a spike signal |
| `VOL_MIN_PCT_MOVE` | `0.3` | Minimum absolute % move (filters noise) |
| `VOL_COOLDOWN_SECS` | `30` | Seconds between signals (prevents repeat-firing) |
| `VOL_WINDOW` | `60` | Rolling window size for std calculation |

### Execution

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_TRADE_SIZE_USD` | `50` | Maximum USD per single trade |
| `MAX_TOTAL_EXPOSURE_USD` | `200` | Maximum total USD across all open positions |
| `MIN_EDGE` | `0.05` | Minimum edge (probability delta) to enter |
| `VENUE` | `polymarket` | Trading venue (`polymarket` or `kalshi`) |

### Exit Logic

| Variable | Default | Description |
|----------|---------|-------------|
| `TAKE_PROFIT_PCT` | `8.0` | Exit when position is up this % |
| `STOP_LOSS_PCT` | `5.0` | Exit when position is down this % |
| `MAX_HOLD_MINUTES` | `15` | Force exit after this many minutes |
| `EXIT_CHECK_INTERVAL` | `10` | Seconds between position checks |

### Liquidity Filters

| Variable | Default | Description |
|----------|---------|-------------|
| `MIN_MARKET_VOLUME` | `1000` | Skip markets below this USD volume |
| `MIN_PRICE_EXTREME` | `0.08` | Skip markets with YES price below this |
| `MAX_PRICE_EXTREME` | `0.92` | Skip markets with YES price above this |

### Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `SYNTHESIS_API_KEY` | `""` | Synthesis Trade API key |
| `SYNTHESIS_BASE_URL` | `https://synthesis.trade/api/v1` | API base URL |
| `PYTH_WS_URL` | `wss://hermes.pyth.network/ws` | Pyth Hermes WebSocket |
| `DRY_RUN` | `true` | Paper trade mode (no real orders) |

## Testing

```bash
# Run all tests (81 tests)
python -m pytest tests/ -v

# Run specific test module
python -m pytest tests/test_volatility.py -v
python -m pytest tests/test_signal_engine.py -v
python -m pytest tests/test_trade_store.py -v
python -m pytest tests/test_synthesis_client.py -v
python -m pytest tests/test_feed_resolver.py -v
python -m pytest tests/test_integration.py -v

# Run only unit tests (no network calls)
python -m pytest tests/ -v -k "not Live"

# Run with coverage
pip install pytest-cov
python -m pytest tests/ --cov=. --cov-report=term-missing
```

### Test Categories

- **Unit tests** — vol detector, strike parser, edge model, trade store, liquidity filters
- **Integration tests** — trade lifecycle, position persistence, exposure limits, pipeline flow
- **Live integration** — hits real Pyth and Synthesis APIs (feed resolution, market discovery)

## Production Features

- **Auto feed rollover** — resolves active WTI front-month on startup, never goes blind after contract expiry
- **Retry with backoff** — exponential retry on 429/5xx/timeouts across all Synthesis API calls
- **Orderbook depth check** — verifies 2x depth and <5% spread before executing
- **Persistent state** — positions and closed trades survive restarts via SQLite
- **Limit order exits** — exits at 1% haircut for fast fill, avoids market-sell front-running
- **Health monitoring** — HTTP `/health` and `/stats` endpoints, watchdog logs stale components
- **Graceful shutdown** — SIGINT/SIGTERM closes all positions and prints session summary

## Project Structure

```
oil-sniper/
├── config.py               # Settings (env vars)
├── pyth_feed.py            # Pyth WS consumer
├── volatility.py           # Vol spike detector
├── signal_engine.py        # Edge model + execution
├── synthesis_client.py     # Synthesis REST client
├── feed_resolver.py        # WTI feed auto-resolver
├── trade_store.py          # SQLite persistence
├── health.py               # Health endpoint
├── main.py                 # Production entry point
├── recorder.py             # Data recorder
├── backtest.py             # Strategy backtester
├── requirements.txt
├── .env.example
└── tests/
    ├── test_volatility.py
    ├── test_signal_engine.py
    ├── test_synthesis_client.py
    ├── test_trade_store.py
    ├── test_feed_resolver.py
    └── test_integration.py
```

## Risks

- **Liquidity** — prediction market oil contracts can be thin. The bot filters by volume and checks orderbook depth, but slippage is real.
- **Edge decay** — if others build similar bots, the repricing lag shrinks and the edge disappears.
- **Model risk** — the probability shift model is calibrated on assumptions, not historical data. Run the backtest before trusting it.
- **Execution risk** — Synthesis adds a REST layer over Polymarket's CLOB. Latency is ~90-200ms per call.
- **Contract expiry** — WTI futures feeds expire monthly. The feed resolver handles this, but verify after each rollover.

## License

Private. Not for distribution.
