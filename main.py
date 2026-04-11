#!/usr/bin/env python3
"""
oil-sniper: Pyth-fed volatility sniper for oil prediction markets.

Watches WTI crude oil price via Pyth Network, detects headline-driven
volatility spikes, and front-runs prediction market repricing on
Polymarket/Kalshi via Synthesis Trade.

Production features:
  - Auto-resolves active WTI front-month feed on startup
  - Persistent position/trade tracking (survives restarts)
  - Orderbook depth checks before execution
  - Exponential backoff retry on all API calls
  - Health endpoint at /health and /stats
  - Graceful shutdown with position summary

Usage:
    # Dry run (default — no real orders)
    python main.py

    # Live trading
    SNIPER_DRY_RUN=false SNIPER_SYNTHESIS_API_KEY=xxx python main.py

    # Custom thresholds
    SNIPER_VOL_SIGMA_THRESHOLD=2.0 SNIPER_VOL_MIN_PCT_MOVE=0.2 python main.py

Environment variables (prefix SNIPER_):
    SYNTHESIS_API_KEY       - Synthesis Trade API key
    DRY_RUN                 - true/false (default: true)
    VOL_SIGMA_THRESHOLD     - z-score trigger (default: 2.5)
    VOL_MIN_PCT_MOVE        - min % move to trigger (default: 0.3)
    VOL_COOLDOWN_SECS       - seconds between triggers (default: 30)
    MAX_TRADE_SIZE_USD      - max per-trade size (default: 50)
    MAX_TOTAL_EXPOSURE_USD  - max total exposure (default: 200)
    MIN_EDGE                - min edge to enter (default: 0.05)
    VENUE                   - polymarket or kalshi (default: polymarket)
"""

import asyncio
import logging
import signal
import sys

from config import settings
from feed_resolver import resolve_active_wti_feed
from health import HealthMonitor
from pyth_feed import PriceUpdate, PythFeed
from signal_engine import SignalEngine
from synthesis_client import SynthesisClient


def setup_logging() -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


async def main() -> None:
    setup_logging()
    logger = logging.getLogger("oil-sniper")

    print(
        r"""
     ___  ___ _       _____       _
    / _ \ (_)| |     / ____|     (_)
   | | | | _ | |    | (___  _ __  _  _ __    ___  _ __
   | | | || || |     \___ \| '_ \| || '_ \  / _ \| '__|
   | |_| || || |     ____) | | | | || |_) ||  __/| |
    \___/ |_||_|    |_____/|_| |_|_|| .__/  \___||_|
                                    | |
                                    |_|          v2.0
    """
    )

    # ── Resolve active WTI feed ────────────────────────────────
    logger.info("Resolving active WTI front-month feed...")
    resolved_feed = await resolve_active_wti_feed()
    if resolved_feed:
        settings.pyth_oil_feed_id = resolved_feed
        logger.info("Using feed: %s", resolved_feed[:24])
    else:
        logger.warning(
            "Could not auto-resolve feed, using config default: %s",
            settings.pyth_oil_feed_id[:24],
        )

    # ── Config summary ─────────────────────────────────────────
    logger.info("=" * 55)
    logger.info("Configuration:")
    logger.info("  Mode:       %s", "DRY RUN" if settings.dry_run else "!! LIVE !!")
    logger.info("  Venue:      %s", settings.venue)
    logger.info("  Feed:       %s...", settings.pyth_oil_feed_id[:20])
    logger.info("  Sigma:      %.1f", settings.vol_sigma_threshold)
    logger.info("  Min move:   %.2f%%", settings.vol_min_pct_move)
    logger.info("  Cooldown:   %.0fs", settings.vol_cooldown_secs)
    logger.info("  Max trade:  $%.0f", settings.max_trade_size_usd)
    logger.info("  Max expo:   $%.0f", settings.max_total_exposure_usd)
    logger.info("  Min edge:   %.2f", settings.min_edge)
    logger.info("  TP / SL:    +%.1f%% / -%.1f%%", settings.take_profit_pct, settings.stop_loss_pct)
    logger.info("  Max hold:   %.0fm", settings.max_hold_minutes)
    logger.info("=" * 55)

    if not settings.dry_run and not settings.synthesis_api_key:
        logger.error("SNIPER_SYNTHESIS_API_KEY required for live trading")
        sys.exit(1)

    # ── Components ─────────────────────────────────────────────
    price_queue = asyncio.Queue(maxsize=500)
    pyth = PythFeed(price_queue)
    synthesis = SynthesisClient()
    engine = SignalEngine(price_queue, synthesis)
    health = HealthMonitor(port=8080)

    # Wire health stats
    health.set_stats_provider(lambda: engine._store.get_stats())

    # ── Graceful shutdown ──────────────────────────────────────
    stop_event = asyncio.Event()

    def on_shutdown(sig, _):
        logger.info("Received %s — shutting down...", signal.Signals(sig).name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, on_shutdown, sig, None)

    # ── Launch ─────────────────────────────────────────────────
    await health.start()

    tasks = [
        asyncio.create_task(pyth.start(), name="pyth-feed"),
        asyncio.create_task(engine.start(), name="signal-engine"),
        asyncio.create_task(health.watchdog(), name="watchdog"),
        asyncio.create_task(
            _heartbeat_loop(health, pyth, engine), name="heartbeat"
        ),
    ]

    logger.info("Bot is live — watching Pyth WTI feed for vol spikes...")
    logger.info("Health: http://localhost:8080/health")
    logger.info("Stats:  http://localhost:8080/stats")

    # Wait for shutdown
    await stop_event.wait()

    # ── Cleanup ────────────────────────────────────────────────
    logger.info("Stopping...")
    await pyth.stop()
    await engine.stop()
    await health.stop()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    await synthesis.close()
    engine._store.close()

    engine.print_summary()
    logger.info("Goodbye.")


async def _heartbeat_loop(health, pyth, engine) -> None:
    """Send heartbeats from each component to the health monitor."""
    while True:
        await asyncio.sleep(30)

        # Pyth feed heartbeat — check if queue is receiving
        if pyth._running:
            health.heartbeat("pyth_feed")

        # Signal engine heartbeat
        if engine._running:
            health.heartbeat("signal_engine")


if __name__ == "__main__":
    asyncio.run(main())
