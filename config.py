from typing import List

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    model_config = {"env_prefix": "SNIPER_", "env_file": ".env"}

    # ── Synthesis ──────────────────────────────────────────────
    synthesis_api_key: str = ""
    synthesis_base_url: str = "https://synthesis.trade/api/v1"

    # ── Pyth ──────────────────────────────────────────────────
    pyth_ws_url: str = "wss://hermes.pyth.network/ws"
    # WTI Crude Oil front-month futures — Pyth price feed IDs
    # WTIK6 = April 2026 (active front-month), WTIM6 = May 2026 (next month)
    pyth_oil_feed_id: str = (
        "40d9903bd7a727ad128c89fca177d77d0175de6ea5c789a81473bf17bad64b39"
    )

    # ── Vol Detection ─────────────────────────────────────────
    # Rolling window size (number of price updates)
    vol_window: int = 60
    # Trigger when move exceeds this many standard deviations
    vol_sigma_threshold: float = 2.5
    # Minimum absolute % move to avoid noise triggers
    vol_min_pct_move: float = 0.3
    # Cooldown seconds between triggers (don't spam on the same move)
    vol_cooldown_secs: float = 30.0

    # ── Execution ─────────────────────────────────────────────
    # Which venue to trade on
    venue: str = "polymarket"  # "polymarket" | "kalshi"
    # Max USD notional per single trade
    max_trade_size_usd: float = 50.0
    # Max total USD exposure across all open positions
    max_total_exposure_usd: float = 200.0
    # Min edge (implied probability delta) to enter
    min_edge: float = 0.05
    # Keywords to match oil-related prediction markets
    market_keywords: List[str] = [
        "oil",
        "crude",
        "wti",
        "brent",
        "petroleum",
        "opec",
        "barrel",
        "energy",
    ]

    # ── Exit Logic ─────────────────────────────────────────────
    # Take profit: exit when contract moves this much in our favor
    take_profit_pct: float = 8.0
    # Stop loss: exit when contract moves this much against us
    stop_loss_pct: float = 5.0
    # Max hold time in minutes — force exit after this
    max_hold_minutes: float = 15.0
    # How often to check positions for exit (seconds)
    exit_check_interval: float = 10.0

    # ── Liquidity Filters ─────────────────────────────────────
    # Skip markets with total volume below this USD amount
    min_market_volume: float = 1000.0
    # Skip markets where yes_price is too extreme (illiquid tails)
    max_price_extreme: float = 0.92
    min_price_extreme: float = 0.08
    # Depth check multiplier: require this multiple of trade size in orderbook
    depth_multiplier: float = 2.0
    # Max spread allowed for entering a position (e.g., 0.05 = 5%)
    max_spread: float = 0.05

    # ── Pyth Feed Resilience ───────────────────────────────────
    # Max seconds without messages before considering feed dead
    feed_timeout_secs: float = 120.0
    # Max consecutive parse failures before stopping
    max_parse_failures: int = 100

    # ── Logging ────────────────────────────────────────────────
    log_level: str = "INFO"
    dry_run: bool = True  # paper trade by default


settings = Settings()
