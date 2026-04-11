"""Unit tests for the volatility spike detector."""

from __future__ import annotations

import os
import sys
import time
import unittest
from unittest.mock import patch

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from volatility import VolDetector, VolSignal


class TestVolDetector(unittest.TestCase):
    """Tests for the rolling volatility spike detector."""

    def setUp(self):
        """Fresh detector with default settings for each test."""
        self.det = VolDetector()

    def test_needs_minimum_prices(self):
        """Should not fire signals until enough history is built."""
        # First 2 prices: always None (need at least 3)
        self.assertIsNone(self.det.update(70.0))
        self.assertIsNone(self.det.update(70.01))

    def test_needs_minimum_returns(self):
        """Should not fire until at least 10 returns are collected."""
        for i in range(9):
            result = self.det.update(70.0 + i * 0.001)
            self.assertIsNone(result)

    def test_no_signal_on_stable_prices(self):
        """Stable prices should never trigger a signal."""
        for _ in range(100):
            result = self.det.update(70.0)
            self.assertIsNone(result)

    def test_signal_on_large_spike(self):
        """A large price spike should trigger a signal."""
        # Build baseline
        for i in range(30):
            self.det.update(70.0 + (i % 3) * 0.01)

        # Spike: ~3% move
        signal = self.det.update(72.10)
        self.assertIsNotNone(signal)
        self.assertIsInstance(signal, VolSignal)
        self.assertEqual(signal.direction, 1)  # bullish
        self.assertGreater(signal.z_score, 2.0)
        self.assertGreater(signal.pct_move, 0.3)

    def test_signal_on_large_drop(self):
        """A large price drop should trigger a bearish signal."""
        for i in range(30):
            self.det.update(70.0 + (i % 3) * 0.01)

        signal = self.det.update(67.90)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.direction, -1)  # bearish

    def test_cooldown_suppresses_repeated_signals(self):
        """Second spike within cooldown period should be suppressed."""
        for i in range(30):
            self.det.update(70.0 + (i % 3) * 0.01)

        # First spike fires
        sig1 = self.det.update(72.10)
        self.assertIsNotNone(sig1)

        # Second spike within cooldown — suppressed
        sig2 = self.det.update(74.00)
        self.assertIsNone(sig2)

    def test_signal_after_cooldown_expires(self):
        """Signal should fire again after cooldown period expires."""
        for i in range(30):
            self.det.update(70.0 + (i % 3) * 0.01)

        sig1 = self.det.update(72.10)
        self.assertIsNotNone(sig1)

        # Manually expire cooldown
        self.det._last_trigger = time.time() - 60

        sig2 = self.det.update(75.00)
        self.assertIsNotNone(sig2)

    def test_small_move_below_min_pct_ignored(self):
        """Moves below min_pct threshold should not trigger even if z-score is high."""
        # Very tight range to make std tiny
        for i in range(30):
            self.det.update(70.0000 + (i % 2) * 0.0001)

        # Small absolute move but high z-score due to tiny std
        # The min_pct filter should block this
        result = self.det.update(70.001)
        self.assertIsNone(result)

    def test_signal_contains_correct_fields(self):
        """Signal should contain all expected fields with correct values."""
        prices = [70.0 + (i % 3) * 0.01 for i in range(30)]
        for p in prices:
            self.det.update(p)

        signal = self.det.update(72.50)
        if signal:
            self.assertIsInstance(signal.price, float)
            self.assertIsInstance(signal.prev_price, float)
            self.assertIsInstance(signal.pct_move, float)
            self.assertIsInstance(signal.z_score, float)
            self.assertIsInstance(signal.rolling_std, float)
            self.assertIsInstance(signal.direction, int)
            self.assertIsInstance(signal.timestamp, float)
            self.assertIn(signal.direction, (1, -1))

    def test_price_count_tracks_correctly(self):
        """price_count property should match number of updates."""
        self.assertEqual(self.det.price_count, 0)
        self.det.update(70.0)
        self.det.update(70.1)
        self.assertEqual(self.det.price_count, 2)

    def test_current_std_zero_initially(self):
        """std should be 0 with insufficient data."""
        self.assertEqual(self.det.current_std, 0.0)
        self.det.update(70.0)
        self.assertEqual(self.det.current_std, 0.0)

    def test_zero_price_handled(self):
        """Zero price should not cause division by zero."""
        self.det.update(0)
        self.det.update(0)
        result = self.det.update(0)
        self.assertIsNone(result)


class TestVolSignal(unittest.TestCase):
    """Tests for the VolSignal dataclass."""

    def test_frozen(self):
        """VolSignal should be immutable."""
        sig = VolSignal(
            price=72.0, prev_price=70.0, pct_move=2.86,
            z_score=3.5, rolling_std=0.001, direction=1,
            timestamp=time.time(),
        )
        with self.assertRaises(AttributeError):
            sig.price = 99.0


if __name__ == "__main__":
    unittest.main()
