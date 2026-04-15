"""
Pyth Hermes WebSocket consumer for real-time oil price feeds.

Connects to Pyth's Hermes service, subscribes to the WTI crude oil
price feed, and pushes parsed price updates into an asyncio queue
for downstream consumption.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass

import websockets
from websockets.exceptions import ConnectionClosed

from config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PriceUpdate:
    feed_id: str
    price: float
    conf: float  # confidence interval
    expo: int
    publish_time: float
    recv_time: float


class FeedTimeoutError(Exception):
    """Raised when feed hasn't received messages in too long."""
    pass


class FeedParseError(Exception):
    """Raised when too many consecutive parse failures occur."""
    pass


class PythFeed:
    def __init__(self, queue: asyncio.Queue[PriceUpdate]):
        self._queue = queue
        self._ws = None
        self._running = False
        self.last_price_time: float = 0.0  # timestamp of last successful parse
        self._consecutive_parse_failures: int = 0
        self._queue_drops: int = 0  # track dropped updates
        self._last_drop_log_time: float = 0.0

    async def start(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._connect_and_stream()
            except FeedTimeoutError:
                logger.error(
                    "Feed timeout: no messages for %.0fs, reconnecting...",
                    settings.feed_timeout_secs
                )
                await asyncio.sleep(2)
            except FeedParseError:
                logger.critical(
                    "Feed parse error: %d consecutive failures, stopping feed",
                    settings.max_parse_failures
                )
                self._running = False
                raise
            except ConnectionClosed as e:
                logger.warning("Pyth WS closed (%s), reconnecting in 2s...", e)
                await asyncio.sleep(2)
            except Exception:
                logger.exception("Pyth WS error, reconnecting in 5s...")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(
            settings.pyth_ws_url,
            ping_interval=20,
            ping_timeout=10,
        ) as ws:
            self._ws = ws
            self.last_price_time = time.time()  # Reset on connect
            logger.info("Connected to Pyth Hermes at %s", settings.pyth_ws_url)

            # Subscribe to the oil price feed
            sub_msg = json.dumps(
                {
                    "type": "subscribe",
                    "ids": [settings.pyth_oil_feed_id],
                }
            )
            await ws.send(sub_msg)
            logger.info("Subscribed to feed %s", settings.pyth_oil_feed_id[:16])

            async for raw in ws:
                if not self._running:
                    break

                # Circuit breaker: check for feed timeout
                now = time.time()
                if self.last_price_time > 0:
                    silence = now - self.last_price_time
                    if silence > settings.feed_timeout_secs:
                        raise FeedTimeoutError(
                            f"No valid price updates for {silence:.0f}s"
                        )

                self._handle_message(raw)

    def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("Non-JSON message from Pyth: %s", raw[:100])
            return

        msg_type = msg.get("type")

        if msg_type == "price_update":
            self._parse_price_update(msg)
        elif msg_type == "response":
            status = msg.get("status")
            if status == "success":
                logger.info("Pyth subscription confirmed")
            else:
                logger.warning("Pyth subscription response: %s", msg)

    def _parse_price_update(self, msg: dict) -> None:
        try:
            price_feed = msg["price_feed"]
            price_data = price_feed["price"]

            price_raw = int(price_data["price"])
            expo = int(price_data["expo"])
            conf_raw = int(price_data["conf"])

            price = price_raw * (10**expo)
            conf = conf_raw * (10**expo)

            update = PriceUpdate(
                feed_id=price_feed["id"],
                price=price,
                conf=conf,
                expo=expo,
                publish_time=float(price_data.get("publish_time", 0)),
                recv_time=time.time(),
            )

            # Non-blocking put — drop if queue is full (backpressure)
            try:
                self._queue.put_nowait(update)
            except asyncio.QueueFull:
                self._queue_drops += 1
                now = time.time()
                # Log dropped updates every 60 seconds
                if now - self._last_drop_log_time > 60.0:
                    logger.warning(
                        "Queue full: dropped %d price updates in last 60s",
                        self._queue_drops
                    )
                    self._queue_drops = 0
                    self._last_drop_log_time = now

            self.last_price_time = time.time()
            self._consecutive_parse_failures = 0

        except (KeyError, ValueError, TypeError) as e:
            self._consecutive_parse_failures += 1
            if self._consecutive_parse_failures <= 3:
                logger.warning(
                    "Pyth parse failure #%d: %s - %s",
                    self._consecutive_parse_failures,
                    type(e).__name__,
                    str(msg)[:200],
                )
            elif self._consecutive_parse_failures == 50:
                logger.error(
                    "50 consecutive Pyth parse failures — schema may have changed"
                )

            # Circuit breaker: stop if too many consecutive failures
            if self._consecutive_parse_failures >= settings.max_parse_failures:
                raise FeedParseError(
                    f"{self._consecutive_parse_failures} consecutive parse failures"
                )
