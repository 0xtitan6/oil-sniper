"""Unit tests for the Synthesis API client."""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from synthesis_client import (
    Market,
    OrderbookDepth,
    OrderResult,
    Quote,
    SynthesisClient,
    _safe_float,
)


class TestSafeFloat(unittest.TestCase):
    """Tests for the _safe_float helper."""

    def test_normal_float(self):
        self.assertEqual(_safe_float(3.14), 3.14)

    def test_string_number(self):
        self.assertEqual(_safe_float("42.5"), 42.5)

    def test_int(self):
        self.assertEqual(_safe_float(10), 10.0)

    def test_none_returns_default(self):
        self.assertEqual(_safe_float(None), 0.0)
        self.assertEqual(_safe_float(None, 99.0), 99.0)

    def test_invalid_string(self):
        self.assertEqual(_safe_float("not a number"), 0.0)

    def test_empty_string(self):
        self.assertEqual(_safe_float(""), 0.0)


class TestMarketDataclass(unittest.TestCase):
    """Tests for the Market frozen dataclass."""

    def test_creation(self):
        m = Market("id1", "Test Market", "slug", 0.6, 0.4, 10000, "polymarket")
        self.assertEqual(m.market_id, "id1")
        self.assertEqual(m.yes_price, 0.6)

    def test_frozen(self):
        m = Market("id1", "Test", "slug", 0.5, 0.5, 1000, "polymarket")
        with self.assertRaises(AttributeError):
            m.yes_price = 0.9


class TestDryRunOrders(unittest.TestCase):
    """Test that dry run mode returns synthetic results without API calls."""

    def test_dry_run_place_order(self):
        loop = asyncio.new_event_loop()
        try:
            with patch("synthesis_client.settings") as mock_settings:
                mock_settings.dry_run = True
                mock_settings.synthesis_api_key = ""
                mock_settings.synthesis_base_url = "https://synthesis.trade/api/v1"
                mock_settings.venue = "polymarket"

                client = SynthesisClient()
                result = loop.run_until_complete(
                    client.place_order("market-1", "yes", 50.0, 0.5)
                )
                loop.run_until_complete(client.close())

            self.assertIsNotNone(result)
            self.assertEqual(result.order_id, "dry-run")
            self.assertEqual(result.status, "dry_run")
            self.assertEqual(result.side, "yes")
            self.assertAlmostEqual(result.price, 0.5)
            self.assertAlmostEqual(result.size, 100.0)  # 50 / 0.5
        finally:
            loop.close()


class TestOrderbookDepth(unittest.TestCase):
    """Tests for the OrderbookDepth dataclass."""

    def test_creation(self):
        depth = OrderbookDepth(
            market_id="m1",
            best_bid=0.48,
            best_ask=0.52,
            bid_depth_usd=500.0,
            ask_depth_usd=600.0,
            spread=0.04,
        )
        self.assertAlmostEqual(depth.spread, 0.04)
        self.assertAlmostEqual(depth.bid_depth_usd, 500.0)


class TestDepthCheck(unittest.TestCase):
    """Tests for the depth adequacy checker."""

    def test_sufficient_depth_passes(self):
        loop = asyncio.new_event_loop()
        try:
            client = SynthesisClient()
            # Mock get_orderbook_depth
            client.get_orderbook_depth = AsyncMock(
                return_value=OrderbookDepth(
                    market_id="m1", best_bid=0.48, best_ask=0.52,
                    bid_depth_usd=500, ask_depth_usd=500, spread=0.04,
                )
            )
            result = loop.run_until_complete(
                client.check_depth_ok("m1", 50.0, "yes")
            )
            loop.run_until_complete(client.close())
            self.assertTrue(result)
        finally:
            loop.close()

    def test_insufficient_depth_fails(self):
        loop = asyncio.new_event_loop()
        try:
            client = SynthesisClient()
            client.get_orderbook_depth = AsyncMock(
                return_value=OrderbookDepth(
                    market_id="m1", best_bid=0.48, best_ask=0.52,
                    bid_depth_usd=10, ask_depth_usd=10, spread=0.04,
                )
            )
            result = loop.run_until_complete(
                client.check_depth_ok("m1", 50.0, "yes")
            )
            loop.run_until_complete(client.close())
            self.assertFalse(result)
        finally:
            loop.close()

    def test_wide_spread_fails(self):
        loop = asyncio.new_event_loop()
        try:
            client = SynthesisClient()
            client.get_orderbook_depth = AsyncMock(
                return_value=OrderbookDepth(
                    market_id="m1", best_bid=0.40, best_ask=0.60,
                    bid_depth_usd=1000, ask_depth_usd=1000, spread=0.20,
                )
            )
            result = loop.run_until_complete(
                client.check_depth_ok("m1", 50.0, "yes")
            )
            loop.run_until_complete(client.close())
            self.assertFalse(result)
        finally:
            loop.close()

    def test_no_orderbook_fails(self):
        loop = asyncio.new_event_loop()
        try:
            client = SynthesisClient()
            client.get_orderbook_depth = AsyncMock(return_value=None)
            result = loop.run_until_complete(
                client.check_depth_ok("m1", 50.0, "yes")
            )
            loop.run_until_complete(client.close())
            self.assertFalse(result)
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
