"""Unit tests for the signal engine — edge model, strike parsing, filters."""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from signal_engine import parse_strike, estimate_prob_shift
from synthesis_client import Market


class TestParseStrike(unittest.TestCase):
    """Tests for extracting strike prices from market titles."""

    def test_above_dollar_sign(self):
        self.assertEqual(parse_strike("Will oil be above $70?"), 70.0)

    def test_above_no_dollar(self):
        self.assertEqual(parse_strike("Oil price higher than 75 next week"), 75.0)

    def test_over_decimal(self):
        self.assertEqual(parse_strike("Crude oil over $65.50 per barrel"), 65.5)

    def test_below(self):
        self.assertEqual(parse_strike("WTI falls below $60?"), 60.0)

    def test_drop_to(self):
        self.assertEqual(parse_strike("Will oil drop to $55 by June?"), 55.0)

    def test_exceed(self):
        self.assertEqual(parse_strike("Will WTI exceed $120 in April?"), 120.0)

    def test_no_strike(self):
        self.assertIsNone(parse_strike("Random market with no price target"))

    def test_no_directional_keyword(self):
        self.assertIsNone(parse_strike("Oil is at $70 today"))

    def test_hit_high(self):
        """Polymarket format: 'hit (HIGH) $120'."""
        result = parse_strike("Will WTI Crude Oil (WTI) hit (HIGH) $120 in April?")
        # "hit" is now in our keyword list for better market coverage
        self.assertEqual(result, 120.0)

    def test_settle_over(self):
        """Polymarket format: 'settle over $90'."""
        self.assertEqual(
            parse_strike("Will Crude Oil (CL) settle over $90 on the final trading day?"),
            90.0,
        )

    def test_case_insensitive(self):
        self.assertEqual(parse_strike("CRUDE OIL ABOVE $100"), 100.0)


class TestEstimateProbShift(unittest.TestCase):
    """Tests for the strike-aware probability shift model."""

    def test_atm_shift(self):
        """At-the-money contracts should have maximum sensitivity."""
        shift = estimate_prob_shift(
            current_price=95.0, strike=95.0, pct_move=1.0, current_prob=0.5,
        )
        self.assertGreater(shift, 0.05)
        self.assertLess(shift, 0.25)

    def test_otm_shift_smaller(self):
        """Far OTM contracts should have much lower sensitivity."""
        atm = estimate_prob_shift(95.0, 95.0, 1.0, 0.5)
        otm = estimate_prob_shift(95.0, 130.0, 1.0, 0.1)
        self.assertGreater(atm, otm)

    def test_bigger_move_bigger_shift(self):
        """Larger price moves should produce larger probability shifts."""
        small = estimate_prob_shift(95.0, 95.0, 0.5, 0.5)
        large = estimate_prob_shift(95.0, 95.0, 2.0, 0.5)
        self.assertGreater(large, small)

    def test_capped_at_headroom(self):
        """Shift should never exceed available probability headroom."""
        # At prob=0.5, max headroom is 0.25
        shift = estimate_prob_shift(95.0, 95.0, 50.0, 0.5)
        self.assertLessEqual(shift, 0.25)
        # At prob=0.9, max headroom is 0.1 (can't go above 1.0)
        shift_high = estimate_prob_shift(95.0, 95.0, 50.0, 0.9)
        self.assertLessEqual(shift_high, 0.10)
        # At prob=0.05, max headroom is 0.05
        shift_low = estimate_prob_shift(95.0, 95.0, 50.0, 0.05)
        self.assertLessEqual(shift_low, 0.05)

    def test_zero_strike_fallback(self):
        """Zero strike should use fallback, not crash."""
        shift = estimate_prob_shift(95.0, 0, 1.0, 0.5)
        self.assertGreater(shift, 0)

    def test_zero_price_fallback(self):
        """Zero current price should use fallback."""
        shift = estimate_prob_shift(0, 95.0, 1.0, 0.5)
        self.assertGreater(shift, 0)

    def test_sensitivity_curve(self):
        """Verify the sensitivity decreases monotonically with distance."""
        shifts = []
        for strike in [95, 100, 110, 130, 150]:
            s = estimate_prob_shift(95.0, strike, 1.0, 0.5)
            shifts.append(s)

        # Each should be >= the next (monotonically decreasing)
        for i in range(len(shifts) - 1):
            self.assertGreaterEqual(shifts[i], shifts[i + 1])


class TestLiquidityFilter(unittest.TestCase):
    """Tests for the market liquidity filter logic."""

    def _make_engine(self):
        """Create a SignalEngine with mocked dependencies."""
        import asyncio
        from signal_engine import SignalEngine
        from unittest.mock import MagicMock

        queue = asyncio.Queue()
        synth = MagicMock()
        engine = SignalEngine(queue, synth)
        return engine

    def test_low_volume_rejected(self):
        engine = self._make_engine()
        market = Market(
            market_id="test", title="Test", slug="test",
            yes_price=0.5, no_price=0.5, volume=500, venue="polymarket",
        )
        self.assertFalse(engine._passes_liquidity_filter(market))

    def test_sufficient_volume_accepted(self):
        engine = self._make_engine()
        market = Market(
            market_id="test", title="Test", slug="test",
            yes_price=0.5, no_price=0.5, volume=5000, venue="polymarket",
        )
        self.assertTrue(engine._passes_liquidity_filter(market))

    def test_extreme_high_price_rejected(self):
        engine = self._make_engine()
        market = Market(
            market_id="test", title="Test", slug="test",
            yes_price=0.97, no_price=0.03, volume=5000, venue="polymarket",
        )
        self.assertFalse(engine._passes_liquidity_filter(market))

    def test_extreme_low_price_rejected(self):
        engine = self._make_engine()
        market = Market(
            market_id="test", title="Test", slug="test",
            yes_price=0.02, no_price=0.98, volume=5000, venue="polymarket",
        )
        self.assertFalse(engine._passes_liquidity_filter(market))

    def test_normal_market_accepted(self):
        engine = self._make_engine()
        market = Market(
            market_id="test", title="Test", slug="test",
            yes_price=0.45, no_price=0.55, volume=50000, venue="polymarket",
        )
        self.assertTrue(engine._passes_liquidity_filter(market))


class TestEdgeCalculation(unittest.TestCase):
    """Tests for the full edge calculation logic."""

    def _make_engine(self):
        import asyncio
        from signal_engine import SignalEngine
        from unittest.mock import MagicMock

        queue = asyncio.Queue()
        synth = MagicMock()
        engine = SignalEngine(queue, synth)
        engine._last_pyth_price = 95.0
        return engine

    def _make_signal(self, direction=1, pct_move=1.5, z_score=3.0):
        from volatility import VolSignal
        import time
        return VolSignal(
            price=95.0 + (direction * 1.5),
            prev_price=95.0,
            pct_move=pct_move * direction,
            z_score=z_score,
            rolling_std=0.003,
            direction=direction,
            timestamp=time.time(),
        )

    def test_bullish_above_market_buys_yes(self):
        """Oil spikes UP + 'above $X' market → buy YES."""
        engine = self._make_engine()
        market = Market("m1", "Will oil be above $100?", "", 0.4, 0.6, 10000, "polymarket")
        signal = self._make_signal(direction=1)
        result = engine._calc_edge(market, signal)
        if result:
            side, edge = result
            self.assertEqual(side, "yes")

    def test_bearish_above_market_buys_no(self):
        """Oil drops + 'above $X' market → buy NO."""
        engine = self._make_engine()
        market = Market("m1", "Will oil be above $100?", "", 0.4, 0.6, 10000, "polymarket")
        signal = self._make_signal(direction=-1)
        result = engine._calc_edge(market, signal)
        if result:
            side, edge = result
            self.assertEqual(side, "no")

    def test_bullish_below_market_buys_no(self):
        """Oil spikes UP + 'below $X' market → buy NO."""
        engine = self._make_engine()
        market = Market("m1", "Will oil fall below $80?", "", 0.3, 0.7, 10000, "polymarket")
        signal = self._make_signal(direction=1)
        result = engine._calc_edge(market, signal)
        if result:
            side, edge = result
            self.assertEqual(side, "no")

    def test_no_edge_returns_none(self):
        """Tiny move with no real edge should return None."""
        engine = self._make_engine()
        market = Market("m1", "Will oil be above $200?", "", 0.01, 0.99, 10000, "polymarket")
        signal = self._make_signal(direction=1, pct_move=0.1, z_score=2.6)
        result = engine._calc_edge(market, signal)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
