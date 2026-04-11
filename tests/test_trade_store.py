"""Unit tests for the persistent trade store."""

from __future__ import annotations

import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trade_store import TradeStore


class TestTradeStore(unittest.TestCase):
    """Tests for SQLite-backed position and trade persistence."""

    def setUp(self):
        self.store = TradeStore(path=":memory:")

    def tearDown(self):
        self.store.close()

    def test_save_and_load_position(self):
        self.store.save_position(
            "m1", "Test Market", "yes", 0.50, 50.0, 1000.0,
            0.05, 2.5, 0.3, "ord-1",
        )
        positions = self.store.load_positions()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["market_id"], "m1")
        self.assertEqual(positions[0]["side"], "yes")
        self.assertAlmostEqual(positions[0]["entry_price"], 0.50)

    def test_remove_position(self):
        self.store.save_position(
            "m1", "Test", "yes", 0.50, 50.0, 1000.0,
            0.05, 2.5, 0.3, "ord-1",
        )
        self.assertEqual(self.store.position_count(), 1)
        self.store.remove_position("m1")
        self.assertEqual(self.store.position_count(), 0)

    def test_remove_nonexistent_position(self):
        """Removing a position that doesn't exist should not raise."""
        self.store.remove_position("nonexistent")
        self.assertEqual(self.store.position_count(), 0)

    def test_upsert_position(self):
        """Saving with same market_id should replace, not duplicate."""
        self.store.save_position(
            "m1", "V1", "yes", 0.50, 50.0, 1000.0, 0.05, 2.5, 0.3, "ord-1",
        )
        self.store.save_position(
            "m1", "V2", "no", 0.60, 100.0, 2000.0, 0.10, 3.0, 0.5, "ord-2",
        )
        positions = self.store.load_positions()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["market_title"], "V2")
        self.assertEqual(positions[0]["side"], "no")

    def test_multiple_positions(self):
        for i in range(5):
            self.store.save_position(
                f"m{i}", f"Market {i}", "yes", 0.5, 50.0, 1000.0,
                0.05, 2.5, 0.3, f"ord-{i}",
            )
        self.assertEqual(self.store.position_count(), 5)

    def test_save_closed_trade(self):
        self.store.save_closed_trade(
            "m1", "Test", "yes", 0.50, 0.55, 50.0,
            0.05, 2.5, 0.3, 1000.0, 1100.0,
            10.0, 5.0, "take_profit", "ord-1",
        )
        self.assertEqual(self.store.get_trade_count(), 1)

    def test_total_pnl(self):
        self.store.save_closed_trade(
            "m1", "Win", "yes", 0.50, 0.60, 50.0,
            0.05, 2.5, 0.3, 1000, 1100, 20.0, 10.0, "take_profit", "o1",
        )
        self.store.save_closed_trade(
            "m2", "Loss", "no", 0.50, 0.45, 50.0,
            0.05, 2.5, 0.3, 1000, 1100, -10.0, -5.0, "stop_loss", "o2",
        )
        self.assertAlmostEqual(self.store.get_total_pnl(), 5.0)

    def test_win_rate(self):
        # 2 wins, 1 loss
        for pnl in [10.0, 5.0, -3.0]:
            self.store.save_closed_trade(
                f"m{pnl}", "T", "yes", 0.5, 0.5, 50,
                0.05, 2.5, 0.3, 1000, 1100, 0, pnl, "tp", "o",
            )
        self.assertAlmostEqual(self.store.get_win_rate(), 66.666, places=1)

    def test_win_rate_no_trades(self):
        self.assertEqual(self.store.get_win_rate(), 0.0)

    def test_get_stats(self):
        self.store.save_closed_trade(
            "m1", "T", "yes", 0.5, 0.6, 50,
            0.05, 2.5, 0.3, 1000, 1100, 20, 10, "tp", "o",
        )
        stats = self.store.get_stats()
        self.assertEqual(stats["total_trades"], 1)
        self.assertAlmostEqual(stats["total_pnl"], 10.0)
        self.assertAlmostEqual(stats["win_rate"], 100.0)
        self.assertEqual(stats["open_positions"], 0)

    def test_get_stats_empty(self):
        stats = self.store.get_stats()
        self.assertEqual(stats["total_trades"], 0)
        self.assertEqual(stats["total_pnl"], 0)

    def test_recent_trades(self):
        for i in range(5):
            self.store.save_closed_trade(
                f"m{i}", f"Trade {i}", "yes", 0.5, 0.55, 50,
                0.05, 2.5, 0.3, 1000 + i, 1100 + i,
                10, 5, "tp", f"o{i}",
            )
        recent = self.store.get_recent_trades(limit=3)
        self.assertEqual(len(recent), 3)
        # Most recent first
        self.assertEqual(recent[0]["title"], "Trade 4")


class TestTradeStoreFilePersistence(unittest.TestCase):
    """Test that data actually persists to a file-backed database."""

    def test_survives_close_and_reopen(self):
        db_path = "/tmp/test_oil_sniper_trades.db"
        try:
            # Write
            store1 = TradeStore(path=db_path)
            store1.save_position(
                "m1", "Persist Test", "yes", 0.5, 50, 1000,
                0.05, 2.5, 0.3, "o1",
            )
            store1.save_closed_trade(
                "m0", "Old Trade", "no", 0.4, 0.5, 50,
                0.05, 2.5, 0.3, 900, 1000, 25, 12.5, "tp", "o0",
            )
            store1.close()

            # Re-open and verify
            store2 = TradeStore(path=db_path)
            self.assertEqual(store2.position_count(), 1)
            self.assertEqual(store2.get_trade_count(), 1)
            positions = store2.load_positions()
            self.assertEqual(positions[0]["market_title"], "Persist Test")
            self.assertAlmostEqual(store2.get_total_pnl(), 12.5)
            store2.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
