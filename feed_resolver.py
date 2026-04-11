"""
Pyth WTI feed auto-resolver.

WTI crude futures on Pyth use month-code feed IDs (WTIK6, WTIM6, etc.)
that expire on specific dates. This module automatically finds the
active front-month contract so we never go blind after expiry.

Month codes: F=Jan G=Feb H=Mar J=Apr K=May M=Jun N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# CME month codes
MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}

# Reverse lookup
CODE_TO_MONTH = {v: k for k, v in MONTH_CODES.items()}


async def resolve_active_wti_feed() -> Optional[str]:
    """
    Query Pyth's feed registry for all WTI feeds, find the active
    front-month contract (not deprecated, nearest expiry), and return
    its feed ID.

    Returns None if no active feed is found.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(
                "https://hermes.pyth.network/v2/price_feeds",
                params={"query": "WTI"},
            )
            resp.raise_for_status()
            feeds = resp.json()
        except Exception:
            logger.exception("Failed to fetch Pyth WTI feeds")
            return None

    if not feeds:
        logger.warning("No WTI feeds found on Pyth")
        return None

    # Parse and filter feeds
    candidates: List[Tuple[datetime, str, str]] = []
    now = datetime.utcnow()

    for feed in feeds:
        attrs = feed.get("attributes", {})
        symbol = attrs.get("symbol", "")
        desc = attrs.get("description", "")
        feed_id = feed.get("id", "")

        # Skip deprecated feeds
        if "DEPRECATED" in desc.upper():
            continue

        # Parse month code from symbol like "Commodities.WTIK6/USD"
        base = attrs.get("base", "")  # e.g., "WTIK6"
        if not base.startswith("WTI") or len(base) < 5:
            continue

        month_code = base[3]
        year_digit = base[4]

        if month_code not in CODE_TO_MONTH:
            continue

        try:
            month = CODE_TO_MONTH[month_code]
            year = 2020 + int(year_digit)  # "6" = 2026
            # Approximate expiry: 20th of contract month
            expiry = datetime(year, month, 20)
        except (ValueError, IndexError):
            continue

        # Skip expired contracts (with 2-day grace)
        from datetime import timedelta
        if expiry < now - timedelta(days=2):
            continue

        candidates.append((expiry, feed_id, base))

    if not candidates:
        logger.warning("No active WTI feeds found")
        return None

    # Sort by expiry date, take the nearest (front-month)
    candidates.sort(key=lambda x: x[0])
    expiry, feed_id, base = candidates[0]

    logger.info(
        "Resolved active WTI feed: %s (expires ~%s) ID=%s",
        base,
        expiry.strftime("%Y-%m-%d"),
        feed_id[:20],
    )

    return feed_id


async def check_feed_health(feed_id: str) -> bool:
    """Check if a feed is returning fresh prices (< 60s old)."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.get(
                "https://hermes.pyth.network/v2/updates/price/latest",
                params={"ids[]": feed_id},
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return False

    parsed = data.get("parsed", [])
    if not parsed:
        return False

    import time
    publish_time = int(parsed[0].get("price", {}).get("publish_time", 0))
    age = time.time() - publish_time

    if age > 120:
        logger.warning("Feed %s is stale (age=%ds)", feed_id[:16], age)
        return False

    return True
