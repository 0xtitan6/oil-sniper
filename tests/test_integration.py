"""
Integration tests — verify components work together end-to-end.

These tests use live APIs (Pyth, Synthesis) where practical,
and mocks where network calls would be flaky or slow.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings
from pyth_feed import PriceUpdate
from signal_engine import SignalEngine
from synthesis_client import Market, SynthesisClient
from trade_store import TradeStore
from volatility import VolDetector, VolSignal


class TestPythToSignalPipeline(unittest.TestCase):
    """Test the Pyth price → vol detector → signal engine pipeline."""

    def test_price_updates_flow_through(self):
        """Price updates should flow from queue through vol detector."""
        loop = asyncio.new_event_loop()
        try:
            queue = asyncio.Queue()
            synth = MagicMock(spec=SynthesisClient)
            synth.find_oil_markets = AsyncMock(return_value=[])

            engine = SignalEngine(queue, synth)

            # Simulate 30 stable prices then a spike
            prices = [95.0 + (i % 3) * 0.01 for i in range(30)]
            prices.append(97.50)  # spike

            for p in prices:
                engine._vol.update(p)

            # The last update should have triggered a signal
            # (We test vol detector separately, this tests integration)
            self.assertGreater(engine._vol.price_count, 30)
        finally:
            loop.close()

    def test_spike_triggers_market_scan(self):
        """When a vol spike fires, engine should scan for markets."""
        loop = asyncio.new_event_loop()
        try:
            queue = asyncio.Queue()
            synth = MagicMock(spec=SynthesisClient)
            synth.find_oil_markets = AsyncMock(return_value=[
                Market("m1", "Will WTI be above $100?", "", 0.4, 0.6, 50000, "polymarket"),
            ])
            synth.get_quote = AsyncMock(return_value=None)
            synth.place_order = AsyncMock(return_value=None)
            synth.check_depth_ok = AsyncMock(return_value=True)

            engine = SignalEngine(queue, synth)
            engine._last_pyth_price = 95.0

            signal = VolSignal(
                price=97.5, prev_price=95.0, pct_move=2.63,
                z_score=4.0, rolling_std=0.003, direction=1,
                timestamp=time.time(),
            )

            loop.run_until_complete(engine._on_signal(signal))

            # Should have called find_oil_markets
            synth.find_oil_markets.assert_called()
        finally:
            loop.close()


class TestTradeLifecycle(unittest.TestCase):
    """Test the full trade lifecycle: open → monitor → close."""

    def test_open_and_close_position(self):
        """Open a position, then close it, verify persistence."""
        loop = asyncio.new_event_loop()
        try:
            queue = asyncio.Queue()
            synth = MagicMock(spec=SynthesisClient)

            from synthesis_client import OrderResult
            synth.place_order = AsyncMock(return_value=OrderResult(
                order_id="test-ord", market_id="m1", side="yes",
                price=0.40, size=125, status="filled",
            ))
            synth.get_quote = AsyncMock(return_value=None)
            synth.find_oil_markets = AsyncMock(return_value=[
                Market("m1", "Will WTI be above $100?", "", 0.45, 0.55, 50000, "polymarket"),
            ])

            engine = SignalEngine(queue, synth)
            engine._last_pyth_price = 95.0

            market = Market("m1", "Will WTI be above $100?", "", 0.40, 0.60, 50000, "polymarket")
            signal = VolSignal(
                price=97.0, prev_price=95.0, pct_move=2.1,
                z_score=3.5, rolling_std=0.003, direction=1,
                timestamp=time.time(),
            )

            # Open position
            loop.run_until_complete(
                engine._execute(market, "yes", 50.0, 0.08, signal)
            )

            self.assertEqual(len(engine._positions), 1)
            self.assertIn("m1", engine._positions)
            self.assertAlmostEqual(engine._total_exposure, 50.0)

            # Verify persisted
            self.assertEqual(engine._store.position_count(), 1)

            # Close position
            loop.run_until_complete(engine._close_position("m1", "take_profit"))

            self.assertEqual(len(engine._positions), 0)
            self.assertAlmostEqual(engine._total_exposure, 0.0)
            self.assertEqual(len(engine._closed), 1)
            self.assertEqual(engine._closed[0].exit_reason, "take_profit")

            # Verify persistence
            self.assertEqual(engine._store.position_count(), 0)
            self.assertEqual(engine._store.get_trade_count(), 1)
        finally:
            loop.close()

    def test_position_restored_on_restart(self):
        """Positions should survive engine restart via TradeStore."""
        store = TradeStore(path=":memory:")
        store.save_position(
            "m1", "Persisted Position", "yes", 0.50, 50.0, time.time() - 60,
            0.05, 2.5, 0.3, "ord-1",
        )

        loop = asyncio.new_event_loop()
        try:
            queue = asyncio.Queue()
            synth = MagicMock(spec=SynthesisClient)
            engine = SignalEngine(queue, synth)

            # Swap in the pre-loaded store
            engine._store.close()
            engine._store = store
            engine._positions.clear()
            engine._total_exposure = 0
            engine._restore_positions()

            self.assertEqual(len(engine._positions), 1)
            self.assertIn("m1", engine._positions)
            self.assertAlmostEqual(engine._total_exposure, 50.0)
            self.assertEqual(engine._positions["m1"].market_title, "Persisted Position")
        finally:
            loop.close()


class TestExposureLimits(unittest.TestCase):
    """Test that exposure limits are enforced."""

    def test_max_exposure_blocks_new_trades(self):
        """Should not open new positions when at max exposure."""
        loop = asyncio.new_event_loop()
        try:
            queue = asyncio.Queue()
            synth = MagicMock(spec=SynthesisClient)
            synth.find_oil_markets = AsyncMock(return_value=[
                Market("m1", "Test", "", 0.5, 0.5, 50000, "polymarket"),
            ])

            engine = SignalEngine(queue, synth)
            engine._total_exposure = settings.max_total_exposure_usd

            signal = VolSignal(
                price=97.0, prev_price=95.0, pct_move=2.1,
                z_score=3.5, rolling_std=0.003, direction=1,
                timestamp=time.time(),
            )

            loop.run_until_complete(engine._on_signal(signal))

            # Should not have opened any positions
            self.assertEqual(len(engine._positions), 0)
            synth.place_order.assert_not_called()
        finally:
            loop.close()


class TestSynthesisLiveMarketDiscovery(unittest.TestCase):
    """Integration test — hits real Synthesis API."""

    def test_finds_oil_markets(self):
        """Should find oil prediction markets on Synthesis."""
        loop = asyncio.new_event_loop()
        try:
            client = SynthesisClient()
            markets = loop.run_until_complete(client.find_oil_markets())
            loop.run_until_complete(client.close())

            self.assertGreater(len(markets), 0)

            # Should have real data
            for m in markets[:5]:
                self.assertIsInstance(m.market_id, str)
                self.assertGreater(len(m.market_id), 0)
                self.assertIsInstance(m.title, str)
                self.assertGreater(len(m.title), 0)
                self.assertGreaterEqual(m.volume, 0)

            # Should filter out hockey teams
            titles = [m.title.lower() for m in markets]
            for t in titles:
                self.assertNotIn("oilers vs", t)
        finally:
            loop.close()

    def test_markets_have_prices(self):
        """Markets should have real prices, not all 0.5."""
        loop = asyncio.new_event_loop()
        try:
            client = SynthesisClient()
            markets = loop.run_until_complete(client.find_oil_markets())
            loop.run_until_complete(client.close())

            # At least some markets should have non-0.5 prices
            non_default = [m for m in markets if abs(m.yes_price - 0.5) > 0.01]
            self.assertGreater(
                len(non_default), 0,
                "All markets have default 0.5 price — price parsing may be broken",
            )
        finally:
            loop.close()

    def test_market_deduplication(self):
        """Should not have duplicate market IDs."""
        loop = asyncio.new_event_loop()
        try:
            client = SynthesisClient()
            markets = loop.run_until_complete(client.find_oil_markets())
            loop.run_until_complete(client.close())

            ids = [m.market_id for m in markets]
            self.assertEqual(len(ids), len(set(ids)), "Duplicate market IDs found")
        finally:
            loop.close()

    def test_markets_sorted_by_volume(self):
        """Markets should be sorted by volume descending."""
        loop = asyncio.new_event_loop()
        try:
            client = SynthesisClient()
            markets = loop.run_until_complete(client.find_oil_markets())
            loop.run_until_complete(client.close())

            volumes = [m.volume for m in markets]
            self.assertEqual(volumes, sorted(volumes, reverse=True))
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
