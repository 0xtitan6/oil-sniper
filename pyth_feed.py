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


class PythFeed:
    def __init__(self, queue: asyncio.Queue[PriceUpdate]):
        self._queue = queue
        self._ws = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._connect_and_stream()
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
                logger.debug("Price queue full, dropping update")

        except (KeyError, ValueError):
            logger.debug("Malformed price update: %s", msg)
