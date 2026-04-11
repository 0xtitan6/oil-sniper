"""
Rolling volatility spike detector.

Maintains a circular buffer of recent price returns, computes
rolling standard deviation, and fires a signal when the latest
move exceeds a configurable sigma threshold.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VolSignal:
    price: float
    prev_price: float
    pct_move: float        # signed percentage move
    z_score: float         # how many sigmas this move is
    rolling_std: float     # current rolling std of returns
    direction: int         # +1 bullish, -1 bearish
    timestamp: float


class VolDetector:
    def __init__(self) -> None:
        self._prices: list[float] = []
        self._returns: np.ndarray = np.array([], dtype=np.float64)
        self._window = settings.vol_window
        self._sigma = settings.vol_sigma_threshold
        self._min_pct = settings.vol_min_pct_move
        self._cooldown = settings.vol_cooldown_secs
        self._last_trigger: float = 0.0

    def update(self, price: float) -> Optional[VolSignal]:
        """Push a new price. Returns a VolSignal if a spike is detected."""
        self._prices.append(price)

        # Trim to avoid unbounded growth — only need last 2 for returns
        if len(self._prices) > self._window + 10:
            self._prices = self._prices[-(self._window + 10):]

        if len(self._prices) < 3:
            return None

        # Compute return vs previous price
        prev = self._prices[-2]
        if prev == 0:
            return None
        ret = (price - prev) / prev
        pct_move = ret * 100.0

        # Append to returns buffer
        self._returns = np.append(self._returns, ret)
        if len(self._returns) > self._window:
            self._returns = self._returns[-self._window :]

        # Need enough history for meaningful std
        if len(self._returns) < 10:
            return None

        rolling_std = float(np.std(self._returns))
        if rolling_std < 1e-10:
            return None

        z_score = abs(ret) / rolling_std

        # Check thresholds
        if z_score < self._sigma:
            return None
        if abs(pct_move) < self._min_pct:
            return None

        # Cooldown
        now = time.time()
        if now - self._last_trigger < self._cooldown:
            logger.debug(
                "Vol spike suppressed (cooldown): z=%.2f pct=%.3f%%",
                z_score,
                pct_move,
            )
            return None

        self._last_trigger = now
        direction = 1 if ret > 0 else -1

        signal = VolSignal(
            price=price,
            prev_price=prev,
            pct_move=pct_move,
            z_score=z_score,
            rolling_std=rolling_std,
            direction=direction,
            timestamp=now,
        )

        logger.info(
            "VOL SPIKE: $%.2f → $%.2f (%.3f%%, z=%.2f, dir=%s)",
            prev,
            price,
            pct_move,
            z_score,
            "BULL" if direction > 0 else "BEAR",
        )

        return signal

    @property
    def price_count(self) -> int:
        return len(self._prices)

    @property
    def current_std(self) -> float:
        if len(self._returns) < 2:
            return 0.0
        return float(np.std(self._returns))
