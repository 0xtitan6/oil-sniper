"""
Backtest engine — replays recorded data to validate the strategy.

Reads from the SQLite database populated by recorder.py and simulates
what would have happened if we traded every vol spike.

Key questions it answers:
  1. How many tradeable spikes occurred per day?
  2. What was the avg lag between Pyth spike and market repricing?
  3. What's the P&L if we entered on spike and exited N minutes later?
  4. What's the win rate and Sharpe ratio?

Usage:
    python backtest.py                     # default settings
    python backtest.py --hold-minutes 5    # custom hold time
    python backtest.py --db custom.db      # custom database
"""

from __future__ import annotations

import argparse
import math
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import List, Optional

logger_name = "backtest"


@dataclass
class Spike:
    ts: float
    price: float
    prev_price: float
    pct_move: float
    z_score: float
    direction: int


@dataclass
class MarketSnap:
    ts: float
    market_id: str
    title: str
    yes_price: float
    no_price: float


@dataclass
class SimTrade:
    spike: Spike
    market_id: str
    market_title: str
    side: str
    entry_price: float          # contract price at entry
    exit_price: float           # contract price at exit
    entry_ts: float
    exit_ts: float
    hold_seconds: float
    pnl_pct: float              # (exit - entry) / entry
    pnl_usd: float              # assuming $50 position
    lag_seconds: float           # time between spike and first market move


def load_spikes(db: sqlite3.Connection) -> List[Spike]:
    rows = db.execute(
        "SELECT ts, price, prev_price, pct_move, z_score, direction FROM vol_events ORDER BY ts"
    ).fetchall()
    return [Spike(*r) for r in rows]


def get_market_snaps_around(
    db: sqlite3.Connection, ts: float, window_before: float, window_after: float
) -> List[MarketSnap]:
    """Get market snapshots in a time window around a timestamp."""
    rows = db.execute(
        """
        SELECT ts, market_id, title, yes_price, no_price
        FROM market_snapshots
        WHERE ts BETWEEN ? AND ?
        ORDER BY ts
        """,
        (ts - window_before, ts + window_after),
    ).fetchall()
    return [MarketSnap(*r) for r in rows]


def find_entry_exit(
    snaps: List[MarketSnap],
    spike: Spike,
    market_id: str,
    side: str,
    hold_minutes: float,
) -> Optional[tuple]:
    """Find entry and exit prices for a specific market around a spike."""
    market_snaps = [s for s in snaps if s.market_id == market_id]
    if len(market_snaps) < 2:
        return None

    # Entry: first snapshot after the spike
    entry_snap = None
    for s in market_snaps:
        if s.ts >= spike.ts:
            entry_snap = s
            break

    if not entry_snap:
        return None

    entry_price = entry_snap.yes_price if side == "yes" else entry_snap.no_price

    # Exit: snapshot closest to hold_minutes after entry
    target_exit_ts = entry_snap.ts + (hold_minutes * 60)
    exit_snap = None
    for s in market_snaps:
        if s.ts >= target_exit_ts:
            exit_snap = s
            break

    # If no snap after hold time, use the last available
    if not exit_snap and market_snaps:
        exit_snap = market_snaps[-1]
        if exit_snap.ts <= entry_snap.ts:
            return None

    if not exit_snap:
        return None

    exit_price = exit_snap.yes_price if side == "yes" else exit_snap.no_price

    return entry_snap, entry_price, exit_snap, exit_price


def measure_lag(
    snaps: List[MarketSnap],
    spike: Spike,
    market_id: str,
    side: str,
    threshold: float = 0.01,
) -> float:
    """Measure seconds between Pyth spike and prediction market moving by threshold."""
    market_snaps = [s for s in snaps if s.market_id == market_id and s.ts >= spike.ts]
    if not market_snaps:
        return float("inf")

    base_price = market_snaps[0].yes_price if side == "yes" else market_snaps[0].no_price

    for s in market_snaps[1:]:
        price = s.yes_price if side == "yes" else s.no_price
        if abs(price - base_price) >= threshold:
            return s.ts - spike.ts

    return float("inf")


def run_backtest(db_path: str, hold_minutes: float, trade_size: float) -> None:
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        print("Run recorder.py first to collect data.")
        sys.exit(1)

    db = sqlite3.connect(db_path)

    # Stats
    price_count = db.execute("SELECT COUNT(*) FROM pyth_prices").fetchone()[0]
    snap_count = db.execute("SELECT COUNT(*) FROM market_snapshots").fetchone()[0]
    market_count = db.execute("SELECT COUNT(DISTINCT market_id) FROM market_snapshots").fetchone()[0]

    spikes = load_spikes(db)

    if price_count == 0:
        print("No price data recorded. Run recorder.py first.")
        sys.exit(1)

    time_range = db.execute("SELECT MIN(ts), MAX(ts) FROM pyth_prices").fetchone()
    duration_hrs = (time_range[1] - time_range[0]) / 3600 if time_range[0] else 0

    print("=" * 70)
    print("BACKTEST")
    print("=" * 70)
    print(f"  Data:           {price_count:,} prices, {snap_count:,} snapshots")
    print(f"  Duration:       {duration_hrs:.1f} hours")
    print(f"  Markets:        {market_count}")
    print(f"  Vol spikes:     {len(spikes)}")
    print(f"  Hold time:      {hold_minutes:.0f} minutes")
    print(f"  Trade size:     ${trade_size:.0f}")
    print("-" * 70)

    if not spikes:
        print("\nNo vol spikes detected in the data.")
        print("Either the recording period was too short or thresholds too high.")
        db.close()
        return

    # Simulate trades for each spike
    trades: List[SimTrade] = []
    lags: List[float] = []

    for spike in spikes:
        # Get market snapshots: 30s before spike to hold_minutes + 5min after
        snaps = get_market_snaps_around(
            db, spike.ts, window_before=30, window_after=(hold_minutes + 5) * 60
        )

        if not snaps:
            continue

        # Group by market
        market_ids = set(s.market_id for s in snaps)

        for mid in market_ids:
            # Determine side based on spike direction
            market_snaps_for_id = [s for s in snaps if s.market_id == mid]
            if not market_snaps_for_id:
                continue

            title = market_snaps_for_id[0].title.lower()

            is_above = any(w in title for w in ["above", "over", "exceed", "higher", "rise"])
            is_below = any(w in title for w in ["below", "under", "fall", "drop", "lower"])
            if not is_above and not is_below:
                is_above = True

            if spike.direction > 0:
                side = "yes" if is_above else "no"
            else:
                side = "no" if is_above else "yes"

            # Find entry/exit
            result = find_entry_exit(snaps, spike, mid, side, hold_minutes)
            if not result:
                continue

            entry_snap, entry_price, exit_snap, exit_price = result

            if entry_price <= 0 or entry_price >= 1:
                continue

            pnl_pct = (exit_price - entry_price) / entry_price
            pnl_usd = pnl_pct * trade_size

            lag = measure_lag(snaps, spike, mid, side)
            if lag < float("inf"):
                lags.append(lag)

            trades.append(
                SimTrade(
                    spike=spike,
                    market_id=mid,
                    market_title=market_snaps_for_id[0].title,
                    side=side,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    entry_ts=entry_snap.ts,
                    exit_ts=exit_snap.ts,
                    hold_seconds=exit_snap.ts - entry_snap.ts,
                    pnl_pct=pnl_pct,
                    pnl_usd=pnl_usd,
                    lag_seconds=lag if lag < float("inf") else -1,
                )
            )

    # Results
    print(f"\n  Simulated trades: {len(trades)}")

    if not trades:
        print("  No trades could be simulated (likely not enough market data overlap)")
        db.close()
        return

    wins = [t for t in trades if t.pnl_usd > 0]
    losses = [t for t in trades if t.pnl_usd <= 0]
    total_pnl = sum(t.pnl_usd for t in trades)
    avg_pnl = total_pnl / len(trades)
    win_rate = len(wins) / len(trades) * 100

    pnls = [t.pnl_usd for t in trades]
    avg = sum(pnls) / len(pnls)
    variance = sum((p - avg) ** 2 for p in pnls) / len(pnls)
    std = math.sqrt(variance) if variance > 0 else 0.001
    sharpe = (avg / std) * math.sqrt(252) if std > 0 else 0  # annualized

    print(f"  Win rate:         {win_rate:.1f}%")
    print(f"  Avg P&L/trade:    ${avg_pnl:+.2f}")
    print(f"  Total P&L:        ${total_pnl:+.2f}")
    print(f"  Sharpe (annual):  {sharpe:.2f}")
    print(f"  Best trade:       ${max(t.pnl_usd for t in trades):+.2f}")
    print(f"  Worst trade:      ${min(t.pnl_usd for t in trades):+.2f}")

    if lags:
        avg_lag = sum(lags) / len(lags)
        min_lag = min(lags)
        max_lag = max(lags)
        print(f"\n  LATENCY GAP (Pyth spike → market move):")
        print(f"    Average:  {avg_lag:.1f}s")
        print(f"    Min:      {min_lag:.1f}s")
        print(f"    Max:      {max_lag:.1f}s")
        print(f"    Samples:  {len(lags)}")

        if avg_lag < 10:
            print("    Verdict:  TOO FAST — markets reprice quickly, edge may not exist")
        elif avg_lag < 60:
            print("    Verdict:  PROMISING — enough lag to get in before repricing")
        else:
            print("    Verdict:  WIDE OPEN — prediction markets are very slow to react")
    else:
        print("\n  No lag data (markets may not have moved during recording)")

    # Individual trades
    print(f"\n  {'#':>3}  {'Side':>4}  {'Entry':>6}  {'Exit':>6}  {'P&L':>8}  {'Hold':>6}  {'Lag':>5}  Market")
    print("  " + "-" * 66)
    for i, t in enumerate(trades[:30], 1):
        lag_str = f"{t.lag_seconds:.0f}s" if t.lag_seconds >= 0 else "n/a"
        print(
            f"  {i:>3}  {t.side:>4}  {t.entry_price:>6.3f}  {t.exit_price:>6.3f}  "
            f"${t.pnl_usd:>+7.2f}  {t.hold_seconds/60:>5.1f}m  {lag_str:>5}  "
            f"{t.market_title[:30]}"
        )

    if len(trades) > 30:
        print(f"  ... and {len(trades) - 30} more trades")

    # Recommendations
    print("\n" + "=" * 70)
    print("RECOMMENDATIONS")
    print("=" * 70)

    if len(spikes) == 0:
        print("  - No spikes detected. Lower SNIPER_VOL_SIGMA_THRESHOLD or record longer.")
    elif total_pnl > 0 and win_rate > 50:
        print("  - Strategy shows positive edge. Consider paper trading next.")
        if lags and sum(lags) / len(lags) > 30:
            print("  - Lag is wide enough to trade through Synthesis REST API.")
        print(f"  - Optimal hold time needs more testing (tried {hold_minutes:.0f}m).")
    elif total_pnl > 0 and win_rate <= 50:
        print("  - Positive P&L but low win rate — few big winners carry the book.")
        print("  - Consider tighter exit logic to lock in gains earlier.")
    else:
        print("  - Negative P&L. Possible issues:")
        print("    - Edge model too naive (try strike-aware pricing)")
        print("    - Hold time too long/short (try different --hold-minutes)")
        print("    - Markets already efficient (reconsider the thesis)")

    print("=" * 70 + "\n")
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest oil sniper strategy")
    parser.add_argument("--db", default=DB_PATH, help="Path to recorded data")
    parser.add_argument("--hold-minutes", type=float, default=10, help="Hold time in minutes")
    parser.add_argument("--trade-size", type=float, default=50, help="Simulated trade size USD")
    args = parser.parse_args()

    run_backtest(args.db, args.hold_minutes, args.trade_size)
