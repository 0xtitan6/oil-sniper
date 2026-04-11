"""Unit tests for the Pyth WTI feed resolver."""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from feed_resolver import resolve_active_wti_feed, CODE_TO_MONTH, MONTH_CODES


class TestMonthCodes(unittest.TestCase):
    """Test the CME month code mapping."""

    def test_all_months_covered(self):
        self.assertEqual(len(MONTH_CODES), 12)
        for month in range(1, 13):
            self.assertIn(month, MONTH_CODES)

    def test_reverse_lookup(self):
        self.assertEqual(CODE_TO_MONTH["F"], 1)  # January
        self.assertEqual(CODE_TO_MONTH["Z"], 12)  # December
        self.assertEqual(CODE_TO_MONTH["K"], 5)  # May (not April!)

    def test_april_code(self):
        """April = J, not K. K = May."""
        self.assertEqual(MONTH_CODES[4], "J")  # April
        self.assertEqual(MONTH_CODES[5], "K")  # May


class TestFeedResolver(unittest.TestCase):
    """Tests for the auto-resolution of active WTI feeds."""

    def test_filters_deprecated_feeds(self):
        """Deprecated feeds should be excluded."""
        mock_feeds = [
            {
                "id": "deprecated-feed",
                "attributes": {
                    "base": "WTIH6",
                    "symbol": "Commodities.WTIH6/USD",
                    "description": "DEPRECATED FEED - PYTH WTI 20 FEB 2026 / US DOLLAR",
                },
            },
            {
                "id": "active-feed-123",
                "attributes": {
                    "base": "WTIZ6",
                    "symbol": "Commodities.WTIZ6/USD",
                    "description": "PYTH WTI 20 NOVEMBER 2026 / US DOLLAR",
                },
            },
        ]

        loop = asyncio.new_event_loop()
        try:
            with patch("feed_resolver.httpx.AsyncClient") as MockClient:
                mock_resp = MagicMock()
                mock_resp.json.return_value = mock_feeds
                mock_resp.raise_for_status = MagicMock()

                mock_instance = AsyncMock()
                mock_instance.get = AsyncMock(return_value=mock_resp)
                mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
                mock_instance.__aexit__ = AsyncMock(return_value=False)
                MockClient.return_value = mock_instance

                result = loop.run_until_complete(resolve_active_wti_feed())

            # Should pick the non-deprecated one
            self.assertEqual(result, "active-feed-123")
        finally:
            loop.close()


class TestFeedResolverLive(unittest.TestCase):
    """Integration test — actually hits Pyth API."""

    def test_resolves_a_feed(self):
        """Should resolve to some active WTI feed."""
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(resolve_active_wti_feed())
            self.assertIsNotNone(result)
            self.assertIsInstance(result, str)
            self.assertGreater(len(result), 10)
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
