"""
Signal engine — the brain.

Consumes Pyth price updates, runs them through the vol detector,
and when a spike fires, scans Synthesis for mispriced oil prediction
markets and executes trades.

Now includes:
  - Exit logic (take profit / stop loss / time-based)
  - Liquidity filtering (volume, price extremes)
  - Strike-aware edge model (parses target price from market titles)
  - Position monitoring loop
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from config import settings
from pyth_feed import PriceUpdate
from synthesis_client import Market, SynthesisClient
from trade_store import TradeStore
from volatility import VolDetector, VolSignal

logger = logging.getLogger(__name__)


class ExposureLimitExceeded(Exception):
    """Raised when trade would exceed exposure limits."""
    pass


@dataclass
class OpenPosition:
    market_id: str
    market_title: str
    side: str
    entry_price: float
    size_usd: float
    entry_time: float
    edge: float
    signal_z: float
    signal_pct: float
    order_id: str


@dataclass
class ClosedTrade:
    market_id: str
    market_title: str
    side: str
    entry_price: float
    exit_price: float
    size_usd: float
    edge: float
    signal_z: float
    signal_pct: float
    entry_time: float
    exit_time: float
    pnl_pct: float
    pnl_usd: float
    exit_reason: str  # "take_profit" | "stop_loss" | "time_exit" | "shutdown"
    order_id: str


# ── Strike Parser ──────────────────────────────────────────────
# Matches patterns like "above $70", "over 65.50", "below $80/barrel"
# Extended to catch more production patterns from Polymarket/Kalshi

# Pattern 1: Directional keywords (above, below, etc.)
# Allows optional parenthetical like "(HIGH)" or "(LOW)" between keyword and price
_STRIKE_PATTERN_DIRECTIONAL = re.compile(
    r"(?:above|over|exceed|below|under|fall\s+to|drop\s+to|higher\s+than|lower\s+than|"
    r"hit|reach|touch|break|settle\s+(?:above|below|over|under)|"
    r"close\s+(?:above|below|over|under)|end\s+(?:above|below))"
    r"(?:\s+\([A-Z]+\))?"  # optional "(HIGH)" or "(LOW)" etc.
    r"\s+\$?(\d+(?:\.\d+)?)"
    r"(?:\s*(?:/|per)\s*(?:barrel|bbl))?",  # optional "/barrel" suffix
    re.IGNORECASE,
)

# Pattern 2: Prediction-style phrases like "to $70" or "at or above $70"
# Avoid matching simple statements like "Oil is at $70 today"
_STRIKE_PATTERN_TO_PRICE = re.compile(
    r"(?:to|@)\s+\$?(\d+(?:\.\d+)?)"
    r"(?:\s*(?:/|per)\s*(?:barrel|bbl))?",
    re.IGNORECASE,
)

# Pattern for "at or above/below" which is a prediction target
_STRIKE_PATTERN_AT_OR = re.compile(
    r"at\s+or\s+(?:above|below|over|under)\s+\$?(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

# Pattern 3: Range patterns like "between $60 and $70" - extract midpoint
_STRIKE_PATTERN_RANGE = re.compile(
    r"between\s+\$?(\d+(?:\.\d+)?)\s+(?:and|to|-)\s+\$?(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

# Maximum reasonable title length to prevent regex DoS
_MAX_TITLE_LENGTH = 500


def parse_strike(title: str) -> Optional[float]:
    """
    Extract the strike/target price from a prediction market title.

    Handles various patterns:
    - "Will WTI be above $70?"
    - "Oil to hit $80/barrel"
    - "WTI settle above $65"
    - "Crude oil close below $60"
    - "Price between $70 and $80" (returns midpoint)

    Returns None if no strike can be parsed.
    """
    # Guard against extremely long titles (regex DoS prevention)
    if len(title) > _MAX_TITLE_LENGTH:
        logger.warning("Market title too long (%d chars), skipping strike parse", len(title))
        return None

    # Try directional pattern first (most common)
    match = _STRIKE_PATTERN_DIRECTIONAL.search(title)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass

    # Try "to price" pattern (e.g., "rise to $70")
    match = _STRIKE_PATTERN_TO_PRICE.search(title)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass

    # Try "at or above/below" pattern
    match = _STRIKE_PATTERN_AT_OR.search(title)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass

    # Try range pattern (return midpoint)
    match = _STRIKE_PATTERN_RANGE.search(title)
    if match:
        try:
            low = float(match.group(1))
            high = float(match.group(2))
            return (low + high) / 2.0
        except ValueError:
            pass

    return None


def estimate_prob_shift(
    current_price: float,
    strike: float,
    pct_move: float,
    current_prob: float,
) -> float:
    """
    Estimate how much a prediction market probability should shift
    given a spot price move, accounting for distance to strike
    and current probability level.

    Uses a simplified model: the closer spot is to the strike,
    the more sensitive the probability is to spot moves. Contracts
    already near 0 or 1 have less room to move.
    """
    if strike <= 0 or current_price <= 0:
        return abs(pct_move) * 0.05  # fallback

    # Distance from current price to strike as a fraction
    distance_pct = abs(current_price - strike) / current_price * 100

    # Sensitivity: contracts near the money are most sensitive
    sensitivity = math.exp(-0.2 * distance_pct)

    # Base shift from the spot move magnitude
    base_shift = abs(pct_move) * 0.08  # 1% oil move ~ 8% prob shift ATM

    # Scale by sensitivity
    implied_shift = base_shift * sensitivity

    # Dampen shift based on current probability — contracts near 0 or 1
    # have less room to move (logistic saturation). A contract at 0.90
    # can't realistically shift +0.15 to 1.05.
    prob_headroom = min(current_prob, 1.0 - current_prob) * 2  # 0..1, max at 0.5
    implied_shift *= max(prob_headroom, 0.1)  # floor at 10% to avoid zero

    # Hard cap: never exceed available headroom
    max_shift = min(1.0 - current_prob, current_prob, 0.25)
    return min(implied_shift, max_shift)


class SignalEngine:
    def __init__(
        self,
        price_queue: asyncio.Queue,
        synthesis: SynthesisClient,
    ) -> None:
        self._queue = price_queue
        self._synth = synthesis
        self._vol = VolDetector()
        self._store = TradeStore()
        self._running = False
        self._total_exposure: float = 0.0
        self._positions: Dict[str, OpenPosition] = {}
        self._closed: List[ClosedTrade] = []
        self._markets_cache: List[Market] = []
        self._markets_cache_time: float = 0.0
        self._markets_cache_ttl: float = 60.0
        self._last_pyth_price: float = 0.0

        # Lock to prevent race conditions in exposure management
        self._execution_lock = asyncio.Lock()

        # Restore positions from persistent store
        self._restore_positions()

    def _restore_positions(self) -> None:
        """Load open positions from persistent store on startup."""
        saved = self._store.load_positions()
        for p in saved:
            pos = OpenPosition(
                market_id=p["market_id"],
                market_title=p["market_title"],
                side=p["side"],
                entry_price=p["entry_price"],
                size_usd=p["size_usd"],
                entry_time=p["entry_time"],
                edge=p["edge"],
                signal_z=p["signal_z"],
                signal_pct=p["signal_pct"],
                order_id=p["order_id"],
            )
            self._positions[pos.market_id] = pos
            self._total_exposure += pos.size_usd

        if saved:
            logger.info(
                "Restored %d positions ($%.2f exposure) from disk",
                len(saved), self._total_exposure,
            )

    @property
    def closed_trades(self) -> List[ClosedTrade]:
        return list(self._closed)

    async def start(self) -> None:
        self._running = True
        logger.info("Signal engine started (dry_run=%s)", settings.dry_run)
        logger.info(
            "Exit params: TP=%.1f%% SL=%.1f%% max_hold=%.0fm",
            settings.take_profit_pct,
            settings.stop_loss_pct,
            settings.max_hold_minutes,
        )

        # Pre-load markets
        await self._refresh_markets()

        # Launch position monitor alongside main loop
        monitor_task = asyncio.create_task(
            self._position_monitor(), name="position-monitor"
        )

        try:
            while self._running:
                try:
                    update = await asyncio.wait_for(self._queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue

                self._last_pyth_price = update.price
                signal = self._vol.update(update.price)
                if signal:
                    await self._on_signal(signal)
        finally:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

    async def stop(self) -> None:
        self._running = False
        # Close all open positions on shutdown
        for mid in list(self._positions.keys()):
            await self._close_position(mid, "shutdown")

    # ── Position Monitoring ────────────────────────────────────

    async def _position_monitor(self) -> None:
        """Periodically check open positions for exit conditions."""
        while self._running:
            await asyncio.sleep(settings.exit_check_interval)

            if not self._positions:
                continue

            # Refresh markets to get current prices
            await self._refresh_markets(force=True)
            market_prices = {m.market_id: m for m in self._markets_cache}

            now = time.time()
            to_close: List[Tuple[str, str]] = []  # (market_id, reason)

            for mid, pos in self._positions.items():
                market = market_prices.get(mid)

                # Time-based exit
                hold_secs = now - pos.entry_time
                if hold_secs >= settings.max_hold_minutes * 60:
                    to_close.append((mid, "time_exit"))
                    continue

                if not market:
                    continue

                # Get current price for our side
                current = market.yes_price if pos.side == "yes" else market.no_price

                # P&L calculation - handle invalid entry_price (zombie position fix)
                if pos.entry_price > 0:
                    pnl_pct = ((current - pos.entry_price) / pos.entry_price) * 100
                else:
                    # Invalid entry_price - this is a zombie position, close it
                    logger.error(
                        "Position %s has invalid entry_price=%.4f, closing as zombie",
                        mid[:12], pos.entry_price
                    )
                    to_close.append((mid, "invalid_entry_price"))
                    continue

                # Take profit
                if pnl_pct >= settings.take_profit_pct:
                    logger.info(
                        "TP HIT: %s %.1f%% (entry=%.3f now=%.3f)",
                        mid[:12], pnl_pct, pos.entry_price, current,
                    )
                    to_close.append((mid, "take_profit"))
                    continue

                # Stop loss
                if pnl_pct <= -settings.stop_loss_pct:
                    logger.info(
                        "SL HIT: %s %.1f%% (entry=%.3f now=%.3f)",
                        mid[:12], pnl_pct, pos.entry_price, current,
                    )
                    to_close.append((mid, "stop_loss"))
                    continue

            # Execute closes with lock to prevent race with signal processing
            for mid, reason in to_close:
                async with self._execution_lock:
                    await self._close_position(mid, reason)

    async def _close_position(self, market_id: str, reason: str) -> None:
        """Close an open position and record the trade."""
        pos = self._positions.get(market_id)
        if not pos:
            return

        # Force refresh markets to get fresh exit price (avoid stale data)
        await self._refresh_markets(force=True)

        # Get exit price from fresh market data
        exit_price = pos.entry_price  # fallback only if market not found
        market_found = False
        for m in self._markets_cache:
            if m.market_id == market_id:
                exit_price = m.yes_price if pos.side == "yes" else m.no_price
                market_found = True
                break

        if not market_found:
            logger.warning(
                "Market %s not found in cache during close, using entry_price as fallback",
                market_id[:12]
            )

        # Calculate P&L
        if pos.entry_price > 0:
            pnl_pct = ((exit_price - pos.entry_price) / pos.entry_price) * 100
        else:
            pnl_pct = 0.0
        pnl_usd = (pnl_pct / 100) * pos.size_usd

        # Exit by selling our shares — sell the SAME side we hold.
        # On Polymarket, to exit a YES position you sell YES shares,
        # NOT buy NO shares (which would be opening a new position).
        # Price with a 1% haircut for fast fill.
        exit_filled = True
        if not settings.dry_run:
            sell_price = exit_price * 0.99  # 1% haircut
            result = await self._synth.place_order(
                market_id, pos.side, pos.size_usd, sell_price
            )
            if not result:
                logger.error(
                    "EXIT ORDER FAILED for %s — position remains open",
                    market_id[:16],
                )
                exit_filled = False

        if not exit_filled:
            # Don't remove from tracking — retry next cycle
            return

        now = time.time()
        closed = ClosedTrade(
            market_id=market_id,
            market_title=pos.market_title,
            side=pos.side,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            size_usd=pos.size_usd,
            edge=pos.edge,
            signal_z=pos.signal_z,
            signal_pct=pos.signal_pct,
            entry_time=pos.entry_time,
            exit_time=now,
            pnl_pct=pnl_pct,
            pnl_usd=pnl_usd,
            exit_reason=reason,
            order_id=pos.order_id,
        )
        self._closed.append(closed)

        # Persist to database with error handling
        db_error = False
        try:
            self._store.remove_position(market_id)
            self._store.save_closed_trade(
                market_id=closed.market_id,
                market_title=closed.market_title,
                side=closed.side,
                entry_price=closed.entry_price,
                exit_price=closed.exit_price,
                size_usd=closed.size_usd,
                edge=closed.edge,
                signal_z=closed.signal_z,
                signal_pct=closed.signal_pct,
                entry_time=closed.entry_time,
                exit_time=closed.exit_time,
                pnl_pct=closed.pnl_pct,
                pnl_usd=closed.pnl_usd,
                exit_reason=closed.exit_reason,
                order_id=closed.order_id,
            )
        except Exception as e:
            # DB persistence failed - log but continue
            # Position is already closed in market, we need to update memory state
            logger.error(
                "DB error while closing position %s: %s. "
                "Trade executed but may not be persisted correctly.",
                market_id[:12], e
            )
            db_error = True

        # Update memory state regardless of DB error
        # (the position is closed in the market)
        self._total_exposure -= pos.size_usd
        del self._positions[market_id]

        hold_mins = (now - pos.entry_time) / 60
        logger.info(
            "CLOSED [%s]: %s %s P&L=$%+.2f (%.1f%%) held=%.1fm%s",
            reason,
            pos.side.upper(),
            market_id[:12],
            pnl_usd,
            pnl_pct,
            hold_mins,
            " (DB ERROR)" if db_error else "",
        )

    # ── Signal Handling ────────────────────────────────────────

    async def _on_signal(self, signal: VolSignal) -> None:
        """React to a volatility spike."""
        logger.info(
            "Processing signal: dir=%s z=%.2f pct=%.3f%% price=$%.2f",
            "BULL" if signal.direction > 0 else "BEAR",
            signal.z_score,
            signal.pct_move,
            signal.price,
        )

        await self._refresh_markets()

        if not self._markets_cache:
            logger.warning("No oil markets found — skipping signal")
            return

        # Use lock to prevent race conditions with position monitor
        # This ensures exposure checks and trade execution are atomic
        async with self._execution_lock:
            if self._total_exposure >= settings.max_total_exposure_usd:
                logger.warning(
                    "Max exposure reached ($%.2f) — skipping", self._total_exposure
                )
                return

            for market in self._markets_cache:
                # Skip if we already have a position in this market
                if market.market_id in self._positions:
                    continue

                # Liquidity filter
                if not self._passes_liquidity_filter(market):
                    continue

                # Edge calculation
                edge = self._calc_edge(market, signal)
                if edge is None:
                    continue

                side, expected_edge = edge
                if expected_edge < settings.min_edge:
                    logger.debug(
                        "Edge too small (%.3f < %.3f) on %s",
                        expected_edge,
                        settings.min_edge,
                        market.title[:50],
                    )
                    continue

                # Re-check exposure limit before each trade (prevents TOCTOU race)
                remaining = settings.max_total_exposure_usd - self._total_exposure
                size_usd = min(settings.max_trade_size_usd, remaining)
                if size_usd < 1.0:
                    logger.debug("Exposure limit reached mid-signal, stopping")
                    break

                # Orderbook depth check
                if not settings.dry_run:
                    depth_ok = await self._synth.check_depth_ok(
                        market.market_id, size_usd, side
                    )
                    if not depth_ok:
                        logger.debug("Depth check failed for %s", market.title[:40])
                        continue

                await self._execute(market, side, size_usd, expected_edge, signal)

    # ── Liquidity Filter ───────────────────────────────────────

    def _passes_liquidity_filter(self, market: Market) -> bool:
        """Check if a market has enough liquidity to trade."""
        # Volume check
        if market.volume < settings.min_market_volume:
            logger.debug(
                "Skipping %s — low volume ($%.0f < $%.0f)",
                market.title[:40],
                market.volume,
                settings.min_market_volume,
            )
            return False

        # Price extreme check — contracts near 0 or 1 are illiquid
        if market.yes_price > settings.max_price_extreme:
            logger.debug(
                "Skipping %s — yes_price too high (%.3f)",
                market.title[:40],
                market.yes_price,
            )
            return False

        if market.yes_price < settings.min_price_extreme:
            logger.debug(
                "Skipping %s — yes_price too low (%.3f)",
                market.title[:40],
                market.yes_price,
            )
            return False

        return True

    # ── Edge Model ─────────────────────────────────────────────

    def _calc_edge(
        self, market: Market, signal: VolSignal
    ) -> Optional[Tuple[str, float]]:
        """
        Strike-aware edge calculation.

        Parses the target price from the market title, estimates how much
        the probability should shift given the Pyth price move, and
        compares to the current market price.
        """
        title_lower = market.title.lower()

        is_above = any(
            w in title_lower for w in ["above", "over", "exceed", "higher", "rise"]
        )
        is_below = any(
            w in title_lower for w in ["below", "under", "fall", "drop", "lower"]
        )
        if not is_above and not is_below:
            is_above = True

        # Determine side
        if signal.direction > 0:
            side = "yes" if is_above else "no"
        else:
            side = "no" if is_above else "yes"

        current_prob = market.yes_price if side == "yes" else market.no_price

        # Try strike-aware model first
        strike = parse_strike(market.title)
        if strike and self._last_pyth_price > 0:
            implied_shift = estimate_prob_shift(
                current_price=self._last_pyth_price,
                strike=strike,
                pct_move=signal.pct_move,
                current_prob=current_prob,
            )
            logger.debug(
                "Strike-aware: strike=$%.2f spot=$%.2f shift=%.3f",
                strike,
                self._last_pyth_price,
                implied_shift,
            )
        else:
            # Fallback: simple linear model
            implied_shift = min(abs(signal.pct_move) * 0.08, 0.20)

        # Edge = implied shift minus a haircut for uncertainty
        # The haircut scales with how far the current prob is from 0.5
        # (contracts near 0.5 are most liquid and hardest to front-run)
        distance_from_fair = abs(current_prob - 0.5)
        uncertainty_haircut = 0.02 + distance_from_fair * 0.05

        edge = implied_shift - uncertainty_haircut

        if edge <= 0:
            return None

        return side, edge

    # ── Execution ──────────────────────────────────────────────

    async def _execute(
        self,
        market: Market,
        side: str,
        size_usd: float,
        edge: float,
        signal: VolSignal,
    ) -> None:
        """Quote and execute a trade, track as open position."""
        quote = await self._synth.get_quote(market.market_id, side, size_usd)

        if quote and quote.price > 0:
            price = quote.price
        else:
            price = market.yes_price if side == "yes" else market.no_price

        logger.info(
            "EXECUTING: %s $%.2f on '%s' @ %.4f (edge=%.3f)",
            side.upper(),
            size_usd,
            market.title[:60],
            price,
            edge,
        )

        result = await self._synth.place_order(market.market_id, side, size_usd, price)

        if result:
            now = time.time()
            pos = OpenPosition(
                market_id=market.market_id,
                market_title=market.title,
                side=side,
                entry_price=price,
                size_usd=size_usd,
                entry_time=now,
                edge=edge,
                signal_z=signal.z_score,
                signal_pct=signal.pct_move,
                order_id=result.order_id,
            )

            # Try to persist to disk first - if this fails, we don't want
            # to track the position in memory either (could lead to orphaned positions)
            try:
                self._store.save_position(
                    market_id=pos.market_id,
                    market_title=pos.market_title,
                    side=pos.side,
                    entry_price=pos.entry_price,
                    size_usd=pos.size_usd,
                    entry_time=pos.entry_time,
                    edge=pos.edge,
                    signal_z=pos.signal_z,
                    signal_pct=pos.signal_pct,
                    order_id=pos.order_id,
                )
            except Exception as e:
                # DB save failed - this is critical because we have a live position
                # but can't track it properly
                logger.critical(
                    "CRITICAL: Position opened in market but DB save failed! "
                    "market_id=%s, size=$%.2f, order_id=%s, error=%s",
                    market.market_id[:16], size_usd, result.order_id, e
                )
                # Still track in memory so we can try to close it
                self._positions[market.market_id] = pos
                self._total_exposure += size_usd
                return

            # DB save succeeded, now track in memory
            self._positions[market.market_id] = pos
            self._total_exposure += size_usd

            logger.info(
                "OPEN #%d: %s %s $%.2f @ %.4f [%s] exposure=$%.2f | positions=%d",
                len(self._positions),
                side.upper(),
                market.market_id[:12],
                size_usd,
                price,
                result.status,
                self._total_exposure,
                len(self._positions),
            )
        else:
            logger.error("Order failed for %s", market.market_id)

    # ── Market Cache ───────────────────────────────────────────

    async def _refresh_markets(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._markets_cache_time < self._markets_cache_ttl:
            return

        self._markets_cache = await self._synth.find_oil_markets()
        self._markets_cache_time = now

        if self._markets_cache:
            logger.debug(
                "Cached %d oil markets",
                len(self._markets_cache),
            )

    # ── Summary ────────────────────────────────────────────────

    def print_summary(self) -> None:
        total = len(self._closed)
        if total == 0 and not self._positions:
            # Check persistent store for historical trades
            stats = self._store.get_stats()
            if stats["total_trades"] > 0:
                print("\n" + "=" * 75)
                print("LIFETIME STATS (from disk)")
                print("=" * 75)
                print("  Total trades:  %d" % stats["total_trades"])
                print("  Total P&L:     $%+.2f" % stats["total_pnl"])
                print("  Win rate:      %.1f%%" % stats["win_rate"])
                print("  Avg P&L:       $%+.2f" % stats["avg_pnl"])
                print("=" * 75 + "\n")
            else:
                logger.info("No trades executed")
            return

        print("\n" + "=" * 75)
        print("SESSION SUMMARY")
        print("=" * 75)

        if self._closed:
            wins = [t for t in self._closed if t.pnl_usd > 0]
            losses = [t for t in self._closed if t.pnl_usd <= 0]
            total_pnl = sum(t.pnl_usd for t in self._closed)
            win_rate = len(wins) / total * 100 if total > 0 else 0

            print(f"\n  Closed Trades: {total}")
            print(f"  Win Rate:      {win_rate:.1f}% ({len(wins)}W / {len(losses)}L)")
            print(f"  Total P&L:     ${total_pnl:+.2f}")
            if total > 0:
                print(f"  Avg P&L:       ${total_pnl / total:+.2f}")
            print(f"  Best:          ${max(t.pnl_usd for t in self._closed):+.2f}")
            print(f"  Worst:         ${min(t.pnl_usd for t in self._closed):+.2f}")

            # Exit reasons
            reasons = {}
            for t in self._closed:
                reasons[t.exit_reason] = reasons.get(t.exit_reason, 0) + 1
            print(f"\n  Exit Reasons:")
            for reason, count in sorted(reasons.items()):
                reason_pnl = sum(
                    t.pnl_usd for t in self._closed if t.exit_reason == reason
                )
                print(f"    {reason:<14} {count:>3}x  ${reason_pnl:+.2f}")

            print(f"\n  {'#':>3}  {'Side':>4}  {'Entry':>6}  {'Exit':>6}  {'P&L':>8}  {'Hold':>6}  {'Exit':>12}  Market")
            print("  " + "-" * 72)
            for i, t in enumerate(self._closed, 1):
                hold = (t.exit_time - t.entry_time) / 60
                print(
                    f"  {i:>3}  {t.side:>4}  {t.entry_price:>6.3f}  {t.exit_price:>6.3f}  "
                    f"${t.pnl_usd:>+7.2f}  {hold:>5.1f}m  {t.exit_reason:<12}  "
                    f"{t.market_title[:28]}"
                )

        if self._positions:
            print(f"\n  Open Positions: {len(self._positions)}")
            for mid, pos in self._positions.items():
                hold = (time.time() - pos.entry_time) / 60
                print(
                    f"    {pos.side:>4} ${pos.size_usd:.0f} @ {pos.entry_price:.3f}  "
                    f"held={hold:.1f}m  {pos.market_title[:40]}"
                )

        print("=" * 75 + "\n")
