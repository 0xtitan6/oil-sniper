"""
Synthesis Trade API client.

Handles market discovery, quoting, order execution, and orderbook
depth checks on Polymarket and Kalshi via the Synthesis aggregation layer.

Production features:
  - Exponential backoff retry on all API calls
  - Orderbook depth verification before execution
  - Structured error handling
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional

import httpx

from config import settings

logger = logging.getLogger(__name__)

# ── Retry config ───────────────────────────────────────────────
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0  # seconds
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class Market:
    market_id: str
    title: str
    slug: str
    yes_price: float
    no_price: float
    volume: float
    venue: str


@dataclass(frozen=True)
class Quote:
    market_id: str
    side: str
    price: float
    size: float
    cost_usd: float


@dataclass(frozen=True)
class OrderResult:
    order_id: str
    market_id: str
    side: str
    price: float
    size: float
    status: str


@dataclass(frozen=True)
class OrderbookDepth:
    market_id: str
    best_bid: float
    best_ask: float
    bid_depth_usd: float
    ask_depth_usd: float
    spread: float


class SynthesisClient:
    def __init__(self) -> None:
        self._base = settings.synthesis_base_url
        self._client = httpx.AsyncClient(
            base_url=self._base,
            headers={"X-API-KEY": settings.synthesis_api_key},
            timeout=10.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def validate_auth(self) -> bool:
        """Probe a lightweight endpoint to verify API key works."""
        body = await self._request("GET", "/markets/search/test", params={"limit": 1})
        if body is None:
            return False
        return body.get("success", False)

    # ── Retry wrapper ──────────────────────────────────────────

    async def _request(
        self, method: str, path: str, **kwargs
    ) -> Optional[dict]:
        """Make an HTTP request with exponential backoff retry."""
        last_err = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = await self._client.request(method, path, **kwargs)

                if resp.status_code in RETRY_STATUS_CODES:
                    delay = RETRY_BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        "API %d on %s %s, retrying in %.1fs (attempt %d/%d)",
                        resp.status_code, method, path, delay, attempt + 1, MAX_RETRIES,
                    )
                    await asyncio.sleep(delay)
                    continue

                resp.raise_for_status()
                body = resp.json()

                if not body.get("success"):
                    logger.warning("API error on %s: %s", path, body.get("response"))
                    return None

                return body

            except httpx.TimeoutException:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "Timeout on %s %s, retrying in %.1fs (%d/%d)",
                    method, path, delay, attempt + 1, MAX_RETRIES,
                )
                last_err = "timeout"
                await asyncio.sleep(delay)

            except httpx.HTTPStatusError as e:
                logger.error("HTTP %d on %s %s: %s", e.response.status_code, method, path, e)
                return None

            except httpx.HTTPError as e:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "HTTP error on %s %s: %s, retrying in %.1fs",
                    method, path, e, delay,
                )
                last_err = str(e)
                await asyncio.sleep(delay)

        logger.error("All retries exhausted on %s %s (last: %s)", method, path, last_err)
        return None

    # ── Market Discovery ───────────────────────────────────────

    async def find_oil_markets(self) -> List[Market]:
        """
        Search Synthesis for oil/crude prediction markets.

        Uses the /markets/search/{query} endpoint which returns events
        with nested sub-markets. Filters out sports, deduplicates, and
        sorts by volume.
        """
        all_results: List[Market] = []
        search_terms = ["crude oil", "WTI", "oil price", "petroleum"]
        sports_filters = ["oilers vs", "vs. oilers", "vs oilers", "hockey", "nhl"]

        for term in search_terms:
            body = await self._request(
                "GET",
                "/markets/search/{}".format(term),
                params={"limit": 50},
            )
            if not body:
                continue

            events = body.get("response", [])
            if not isinstance(events, list):
                continue

            for event_wrapper in events:
                venue = event_wrapper.get("venue", "polymarket")
                event = event_wrapper.get("event", {})
                event_title = event.get("title", "")

                if any(s in event_title.lower() for s in sports_filters):
                    continue

                nested = event_wrapper.get("markets", [])
                for m in nested:
                    question = m.get("question") or m.get("title") or ""
                    if not question:
                        continue

                    left_outcome = (m.get("left_outcome") or "").lower()
                    right_outcome = (m.get("right_outcome") or "").lower()

                    if left_outcome in ("yes", "up"):
                        yes_price = _safe_float(m.get("left_price"), 0.5)
                    elif right_outcome in ("yes", "up"):
                        yes_price = _safe_float(m.get("right_price"), 0.5)
                    else:
                        yes_price = _safe_float(
                            m.get("left_price")
                            or m.get("yes_price")
                            or m.get("lastTradePrice"),
                            0.5,
                        )

                    # Parse NO price from right side (don't assume 1-yes)
                    if right_outcome in ("no", "down"):
                        no_price = _safe_float(m.get("right_price"), 1.0 - yes_price)
                    elif left_outcome in ("no", "down"):
                        no_price = _safe_float(m.get("left_price"), 1.0 - yes_price)
                    else:
                        no_price = 1.0 - yes_price  # fallback

                    vol = _safe_float(m.get("volume") or m.get("volumeNum"), 0)
                    condition_id = str(
                        m.get("condition_id") or m.get("market_id") or m.get("id") or ""
                    )
                    slug = m.get("slug") or event.get("slug") or ""

                    all_results.append(
                        Market(
                            market_id=condition_id,
                            title=question,
                            slug=slug,
                            yes_price=yes_price,
                            no_price=no_price,
                            volume=vol,
                            venue=venue,
                        )
                    )

        # Deduplicate and sort
        seen = set()
        unique: List[Market] = []
        for m in all_results:
            if m.market_id and m.market_id not in seen:
                seen.add(m.market_id)
                unique.append(m)

        unique.sort(key=lambda x: x.volume, reverse=True)
        logger.info("Found %d oil sub-markets across search terms", len(unique))
        return unique

    # ── Orderbook Depth ────────────────────────────────────────

    async def get_orderbook_depth(
        self, market_id: str
    ) -> Optional[OrderbookDepth]:
        """
        Fetch orderbook for a market and compute depth metrics.
        Returns None if orderbook is unavailable.
        """
        body = await self._request(
            "POST",
            "/markets/orderbooks",
            json=[market_id],
        )
        if not body:
            return None

        response = body.get("response", {})
        # Response is keyed by market_id or token_id
        book_data = None
        if isinstance(response, dict):
            book_data = response.get(market_id)
            if not book_data:
                # Try first value
                for v in response.values():
                    book_data = v
                    break
        elif isinstance(response, list) and response:
            book_data = response[0]

        if not book_data:
            return None

        bids = book_data.get("bids", [])
        asks = book_data.get("asks", [])

        if not bids and not asks:
            return None

        best_bid = _safe_float(bids[0].get("price")) if bids else 0
        best_ask = _safe_float(asks[0].get("price")) if asks else 1

        # Sum depth (price * size) for top 5 levels
        bid_depth = sum(
            _safe_float(b.get("price")) * _safe_float(b.get("size"))
            for b in bids[:5]
        )
        ask_depth = sum(
            _safe_float(a.get("price")) * _safe_float(a.get("size"))
            for a in asks[:5]
        )

        spread = best_ask - best_bid if best_ask > best_bid else 0

        return OrderbookDepth(
            market_id=market_id,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_depth_usd=bid_depth,
            ask_depth_usd=ask_depth,
            spread=spread,
        )

    async def check_depth_ok(
        self, market_id: str, size_usd: float, side: str
    ) -> bool:
        """
        Check if there's enough orderbook depth to fill our trade
        without excessive slippage.

        Rule: available depth on our side must be >= 2x our trade size.
        """
        depth = await self.get_orderbook_depth(market_id)
        if not depth:
            logger.warning("No orderbook data for %s — skipping", market_id[:16])
            return False

        # If buying YES, we need ask depth. If buying NO, we need bid depth.
        if side == "yes":
            available = depth.ask_depth_usd
        else:
            available = depth.bid_depth_usd

        min_required = size_usd * 2  # require 2x depth

        if available < min_required:
            logger.debug(
                "Insufficient depth for %s %s: $%.1f available, $%.1f required",
                side, market_id[:16], available, min_required,
            )
            return False

        if depth.spread > 0.05:
            logger.debug(
                "Spread too wide for %s: %.3f", market_id[:16], depth.spread,
            )
            return False

        return True

    # ── Quoting ────────────────────────────────────────────────

    async def get_quote(
        self, market_id: str, side: str, size_usd: float
    ) -> Optional[Quote]:
        """Get a fill quote for a market position."""
        body = await self._request(
            "POST",
            "/{}/quote".format(settings.venue),
            json={
                "marketId": market_id,
                "side": side,
                "amount": size_usd,
            },
        )
        if not body:
            return None

        data = body.get("response", {})
        if isinstance(data, dict) and "data" in data:
            data = data["data"]

        return Quote(
            market_id=market_id,
            side=side,
            price=_safe_float(data.get("price") or data.get("avgPrice"), 0),
            size=_safe_float(data.get("size") or data.get("shares"), 0),
            cost_usd=_safe_float(data.get("cost") or data.get("totalCost"), size_usd),
        )

    # ── Order Execution ────────────────────────────────────────

    async def place_order(
        self, market_id: str, side: str, size_usd: float, price: float
    ) -> Optional[OrderResult]:
        """Place a limit order on the specified market."""
        if settings.dry_run:
            logger.info(
                "[DRY RUN] Would place %s $%.2f on %s @ %.4f",
                side, size_usd, market_id[:16], price,
            )
            return OrderResult(
                order_id="dry-run",
                market_id=market_id,
                side=side,
                price=price,
                size=size_usd / price if price > 0 else 0,
                status="dry_run",
            )

        body = await self._request(
            "POST",
            "/{}/order".format(settings.venue),
            json={
                "marketId": market_id,
                "side": side,
                "amount": size_usd,
                "price": price,
                "type": "limit",
            },
        )
        if not body:
            return None

        data = body.get("response", {})
        if isinstance(data, dict) and "data" in data:
            data = data["data"]

        return OrderResult(
            order_id=str(data.get("orderId") or data.get("id") or "unknown"),
            market_id=market_id,
            side=side,
            price=price,
            size=_safe_float(data.get("size") or data.get("shares"), 0),
            status=str(data.get("status") or "submitted"),
        )

    # ── Positions ──────────────────────────────────────────────

    async def get_positions(self) -> List[dict]:
        """Fetch current open positions from Synthesis."""
        body = await self._request(
            "GET", "/{}/positions".format(settings.venue),
        )
        if not body:
            return []

        data = body.get("response", {})
        if isinstance(data, dict) and "data" in data:
            data = data["data"]

        return data if isinstance(data, list) else []


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default
