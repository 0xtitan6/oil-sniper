"""
Data recorder — captures synchronized Pyth price updates and
Synthesis market snapshots for offline analysis and backtesting.

Writes to a local SQLite database with two tables:
  - pyth_prices: every price tick from the oil feed
  - market_snapshots: periodic snapshots of all oil-related
    prediction market prices on Synthesis

Run this for 3-5 days before going live to:
  1. Measure the actual latency gap between Pyth and prediction markets
  2. Count tradeable spikes per day
  3. Backtest the edge model against real data
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sqlite3
import time
from typing import Optional

from config import settings
from pyth_feed import PriceUpdate, PythFeed
from synthesis_client import Market, SynthesisClient
from volatility import VolDetector

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("SNIPER_DB_PATH", "oil_data.db")
SNAPSHOT_INTERVAL = 5.0  # poll Synthesis every 5 seconds


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pyth_prices (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          REAL    NOT NULL,
            publish_ts  REAL    NOT NULL,
            price       REAL    NOT NULL,
            conf        REAL    NOT NULL,
            feed_id     TEXT    NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          REAL    NOT NULL,
            market_id   TEXT    NOT NULL,
            title       TEXT    NOT NULL,
            yes_price   REAL    NOT NULL,
            no_price    REAL    NOT NULL,
            volume      REAL    NOT NULL,
            venue       TEXT    NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vol_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          REAL    NOT NULL,
            price       REAL    NOT NULL,
            prev_price  REAL    NOT NULL,
            pct_move    REAL    NOT NULL,
            z_score     REAL    NOT NULL,
            direction   INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pyth_ts ON pyth_prices(ts)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snap_ts ON market_snapshots(ts)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_vol_ts ON vol_events(ts)"
    )
    conn.commit()
    return conn


class DataRecorder:
    def __init__(self) -> None:
        self._db = init_db(DB_PATH)
        self._synth = SynthesisClient()
        self._vol = VolDetector()
        self._price_queue: asyncio.Queue[PriceUpdate] = asyncio.Queue(maxsize=1000)
        self._pyth = PythFeed(self._price_queue)
        self._running = False
        self._price_count = 0
        self._snap_count = 0
        self._spike_count = 0

    async def run(self) -> None:
        self._running = True
        logger.info("Data recorder started — writing to %s", DB_PATH)
        logger.info("Recording Pyth prices + Synthesis snapshots every %.0fs", SNAPSHOT_INTERVAL)
        logger.info("Press Ctrl+C to stop\n")

        tasks = [
            asyncio.create_task(self._pyth.start(), name="pyth"),
            asyncio.create_task(self._consume_prices(), name="price-consumer"),
            asyncio.create_task(self._snapshot_loop(), name="snapshot-loop"),
            asyncio.create_task(self._status_loop(), name="status"),
        ]

        stop = asyncio.Event()

        def on_sig(sig, _):
            logger.info("Shutting down recorder...")
            stop.set()

        for s in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(s, on_sig, s, None)

        await stop.wait()

        self._running = False
        await self._pyth.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._synth.close()
        self._db.close()

        self._print_summary()

    async def _consume_prices(self) -> None:
        """Drain the price queue, write to DB, and check for vol spikes."""
        batch = []
        while self._running:
            try:
                update = await asyncio.wait_for(self._price_queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                if batch:
                    self._flush_prices(batch)
                    batch = []
                continue

            batch.append(update)
            self._price_count += 1

            # Check for vol spike
            sig = self._vol.update(update.price)
            if sig:
                self._spike_count += 1
                self._db.execute(
                    "INSERT INTO vol_events (ts, price, prev_price, pct_move, z_score, direction) VALUES (?,?,?,?,?,?)",
                    (sig.timestamp, sig.price, sig.prev_price, sig.pct_move, sig.z_score, sig.direction),
                )
                self._db.commit()
                logger.info(
                    "SPIKE #%d: $%.2f → $%.2f (%.3f%%, z=%.2f)",
                    self._spike_count, sig.prev_price, sig.price, sig.pct_move, sig.z_score,
                )

            # Flush in batches of 50
            if len(batch) >= 50:
                self._flush_prices(batch)
                batch = []

    def _flush_prices(self, batch: list) -> None:
        self._db.executemany(
            "INSERT INTO pyth_prices (ts, publish_ts, price, conf, feed_id) VALUES (?,?,?,?,?)",
            [(u.recv_time, u.publish_time, u.price, u.conf, u.feed_id) for u in batch],
        )
        self._db.commit()

    async def _snapshot_loop(self) -> None:
        """Periodically snapshot all oil-related Synthesis markets."""
        while self._running:
            await asyncio.sleep(SNAPSHOT_INTERVAL)
            try:
                markets = await self._synth.find_oil_markets()
                if markets:
                    now = time.time()
                    self._db.executemany(
                        "INSERT INTO market_snapshots (ts, market_id, title, yes_price, no_price, volume, venue) VALUES (?,?,?,?,?,?,?)",
                        [
                            (now, m.market_id, m.title, m.yes_price, m.no_price, m.volume, m.venue)
                            for m in markets
                        ],
                    )
                    self._db.commit()
                    self._snap_count += len(markets)
            except Exception:
                logger.exception("Snapshot failed")

    async def _status_loop(self) -> None:
        """Print status every 60 seconds."""
        while self._running:
            await asyncio.sleep(60)
            logger.info(
                "STATUS: %d prices | %d snapshots | %d spikes | vol_std=%.6f",
                self._price_count,
                self._snap_count,
                self._spike_count,
                self._vol.current_std,
            )

    def _print_summary(self) -> None:
        # Query stats
        row = self._db.execute("SELECT MIN(ts), MAX(ts), COUNT(*) FROM pyth_prices").fetchone()
        if row and row[2] > 0:
            duration_hrs = (row[1] - row[0]) / 3600
        else:
            duration_hrs = 0

        snap_count = self._db.execute("SELECT COUNT(*) FROM market_snapshots").fetchone()[0]
        spike_count = self._db.execute("SELECT COUNT(*) FROM vol_events").fetchone()[0]
        market_count = self._db.execute("SELECT COUNT(DISTINCT market_id) FROM market_snapshots").fetchone()[0]

        print("\n" + "=" * 60)
        print("RECORDING SUMMARY")
        print("=" * 60)
        print(f"  Duration:       {duration_hrs:.1f} hours")
        print(f"  Price ticks:    {self._price_count:,}")
        print(f"  Market snaps:   {snap_count:,}")
        print(f"  Unique markets: {market_count}")
        print(f"  Vol spikes:     {spike_count}")
        print(f"  Database:       {DB_PATH}")
        print("=" * 60 + "\n")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    print(
        r"""
     ___  _  _   ___                        _
    / _ \(_)| | | _ \ ___  __ ___  _ _  __| | ___  _ _
   | | | | || | |   // -_)/ _/ _ \| '_|/ _` |/ -_)| '_|
   | |_| | || | |_|_\\___|\__\___/|_|  \__,_|\___||_|
    \___/|_||_|
    """
    )

    recorder = DataRecorder()
    await recorder.run()


if __name__ == "__main__":
    asyncio.run(main())
