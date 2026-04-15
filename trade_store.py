"""
Persistent trade store — SQLite-backed position and trade tracking.

Survives restarts. On startup, the signal engine loads open positions
from here so it can continue monitoring and exiting them.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("SNIPER_TRADE_DB", "trades.db")


def _init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=10.0)  # Explicit timeout to avoid indefinite hangs
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS open_positions (
            market_id    TEXT PRIMARY KEY,
            market_title TEXT NOT NULL,
            side         TEXT NOT NULL,
            entry_price  REAL NOT NULL,
            size_usd     REAL NOT NULL,
            entry_time   REAL NOT NULL,
            edge         REAL NOT NULL,
            signal_z     REAL NOT NULL,
            signal_pct   REAL NOT NULL,
            order_id     TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS closed_trades (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id     TEXT NOT NULL,
            market_title  TEXT NOT NULL,
            side          TEXT NOT NULL,
            entry_price   REAL NOT NULL,
            exit_price    REAL NOT NULL,
            size_usd      REAL NOT NULL,
            edge          REAL NOT NULL,
            signal_z      REAL NOT NULL,
            signal_pct    REAL NOT NULL,
            entry_time    REAL NOT NULL,
            exit_time     REAL NOT NULL,
            pnl_pct       REAL NOT NULL,
            pnl_usd       REAL NOT NULL,
            exit_reason   TEXT NOT NULL,
            order_id      TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_closed_ts ON closed_trades(exit_time)"
    )
    conn.commit()
    return conn


class TradeStore:
    def __init__(self, path: str = DB_PATH) -> None:
        self._db = _init_db(path)
        logger.info("Trade store initialized at %s", path)

    def close(self) -> None:
        self._db.close()

    # ── Open Positions ─────────────────────────────────────────

    def save_position(
        self,
        market_id: str,
        market_title: str,
        side: str,
        entry_price: float,
        size_usd: float,
        entry_time: float,
        edge: float,
        signal_z: float,
        signal_pct: float,
        order_id: str,
    ) -> None:
        try:
            self._db.execute(
                """
                INSERT OR REPLACE INTO open_positions
                (market_id, market_title, side, entry_price, size_usd, entry_time,
                 edge, signal_z, signal_pct, order_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (market_id, market_title, side, entry_price, size_usd,
                 entry_time, edge, signal_z, signal_pct, order_id),
            )
            self._db.commit()
        except sqlite3.Error as e:
            logger.error(
                "Failed to save position %s to database: %s",
                market_id[:12], e
            )
            raise  # Re-raise so caller knows the save failed

    def remove_position(self, market_id: str) -> None:
        try:
            self._db.execute(
                "DELETE FROM open_positions WHERE market_id = ?", (market_id,)
            )
            self._db.commit()
        except sqlite3.Error as e:
            logger.error(
                "Failed to remove position %s from database: %s",
                market_id[:12], e
            )
            raise  # Re-raise so caller knows the removal failed

    def load_positions(self) -> List[dict]:
        # Use explicit column list instead of SELECT * for schema stability
        cols = [
            "market_id", "market_title", "side", "entry_price", "size_usd",
            "entry_time", "edge", "signal_z", "signal_pct", "order_id",
        ]
        rows = self._db.execute(
            f"SELECT {', '.join(cols)} FROM open_positions"
        ).fetchall()
        return [dict(zip(cols, row)) for row in rows]

    def position_count(self) -> int:
        return self._db.execute(
            "SELECT COUNT(*) FROM open_positions"
        ).fetchone()[0]

    # ── Closed Trades ──────────────────────────────────────────

    def save_closed_trade(
        self,
        market_id: str,
        market_title: str,
        side: str,
        entry_price: float,
        exit_price: float,
        size_usd: float,
        edge: float,
        signal_z: float,
        signal_pct: float,
        entry_time: float,
        exit_time: float,
        pnl_pct: float,
        pnl_usd: float,
        exit_reason: str,
        order_id: str,
    ) -> None:
        try:
            self._db.execute(
                """
                INSERT INTO closed_trades
                (market_id, market_title, side, entry_price, exit_price, size_usd,
                 edge, signal_z, signal_pct, entry_time, exit_time,
                 pnl_pct, pnl_usd, exit_reason, order_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (market_id, market_title, side, entry_price, exit_price, size_usd,
                 edge, signal_z, signal_pct, entry_time, exit_time,
                 pnl_pct, pnl_usd, exit_reason, order_id),
            )
            self._db.commit()
        except sqlite3.Error as e:
            logger.error(
                "Failed to save closed trade %s to database: %s",
                market_id[:12], e
            )
            raise  # Re-raise so caller knows the save failed

    def get_total_pnl(self) -> float:
        row = self._db.execute(
            "SELECT COALESCE(SUM(pnl_usd), 0) FROM closed_trades"
        ).fetchone()
        return row[0]

    def get_trade_count(self) -> int:
        return self._db.execute(
            "SELECT COUNT(*) FROM closed_trades"
        ).fetchone()[0]

    def get_win_rate(self) -> float:
        total = self.get_trade_count()
        if total == 0:
            return 0.0
        wins = self._db.execute(
            "SELECT COUNT(*) FROM closed_trades WHERE pnl_usd > 0"
        ).fetchone()[0]
        return wins / total * 100

    def get_recent_trades(self, limit: int = 20) -> List[dict]:
        rows = self._db.execute(
            """
            SELECT market_title, side, entry_price, exit_price, size_usd,
                   pnl_usd, pnl_pct, exit_reason,
                   (exit_time - entry_time) / 60.0 as hold_mins
            FROM closed_trades
            ORDER BY exit_time DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        cols = [
            "title", "side", "entry", "exit", "size",
            "pnl_usd", "pnl_pct", "reason", "hold_mins",
        ]
        return [dict(zip(cols, row)) for row in rows]

    def get_stats(self) -> dict:
        """Get aggregate stats for display."""
        total = self.get_trade_count()
        if total == 0:
            return {
                "total_trades": 0, "total_pnl": 0, "win_rate": 0,
                "avg_pnl": 0, "best": 0, "worst": 0,
                "open_positions": self.position_count(),
            }

        row = self._db.execute(
            """
            SELECT COUNT(*),
                   SUM(pnl_usd),
                   AVG(pnl_usd),
                   MAX(pnl_usd),
                   MIN(pnl_usd),
                   SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END)
            FROM closed_trades
            """
        ).fetchone()

        return {
            "total_trades": row[0],
            "total_pnl": row[1],
            "avg_pnl": row[2],
            "best": row[3],
            "worst": row[4],
            "win_rate": (row[5] / row[0]) * 100 if row[0] > 0 else 0,
            "open_positions": self.position_count(),
        }
