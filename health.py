"""
Health monitoring — HTTP endpoint and watchdog.

Exposes /health for external monitoring (UptimeRobot, etc.)
and runs an internal watchdog that logs warnings if components
go silent.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class HealthMonitor:
    def __init__(self, port: int = 8080) -> None:
        self._port = port
        self._server = None
        self._heartbeats: Dict[str, float] = {}
        self._status: Dict[str, str] = {}
        self._start_time = time.time()
        self._stats_fn = None  # callable that returns trade stats dict

    def set_stats_provider(self, fn) -> None:
        """Set a callable that returns current trade stats."""
        self._stats_fn = fn

    def heartbeat(self, component: str) -> None:
        """Record a heartbeat from a component."""
        self._heartbeats[component] = time.time()
        self._status[component] = "ok"

    def set_status(self, component: str, status: str) -> None:
        self._status[component] = status

    async def start(self) -> None:
        """Start the health HTTP server."""
        self._server = await asyncio.start_server(
            self._handle_request, "0.0.0.0", self._port,
        )
        logger.info("Health endpoint listening on http://0.0.0.0:%d/health", self._port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            data = await asyncio.wait_for(reader.read(1024), timeout=5)
            request = data.decode()

            if "GET /health" in request:
                body = self._build_health_response()
                status = "200 OK" if body["healthy"] else "503 Service Unavailable"
            elif "GET /stats" in request:
                body = self._build_stats_response()
                status = "200 OK"
            else:
                body = {"error": "not found"}
                status = "404 Not Found"

            response_body = json.dumps(body, indent=2).encode("utf-8")
            header = (
                f"HTTP/1.1 {status}\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(response_body)}\r\n"
                f"\r\n"
            ).encode("utf-8")
            writer.write(header + response_body)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    def _build_health_response(self) -> dict:
        now = time.time()
        uptime = now - self._start_time

        components = {}
        healthy = True

        for name, last_hb in self._heartbeats.items():
            age = now - last_hb
            stale = age > 120  # 2 minutes without heartbeat = unhealthy
            components[name] = {
                "status": self._status.get(name, "unknown"),
                "last_heartbeat_secs_ago": round(age, 1),
                "stale": stale,
            }
            if stale:
                healthy = False

        return {
            "healthy": healthy,
            "uptime_seconds": round(uptime, 0),
            "components": components,
        }

    def _build_stats_response(self) -> dict:
        if self._stats_fn:
            return self._stats_fn()
        return {"message": "no stats provider configured"}

    async def watchdog(self) -> None:
        """Periodically check component health and log warnings."""
        while True:
            await asyncio.sleep(60)
            now = time.time()

            for name, last_hb in self._heartbeats.items():
                age = now - last_hb
                if age > 120:
                    logger.warning(
                        "WATCHDOG: %s has not sent a heartbeat in %.0fs",
                        name, age,
                    )

            # Log current stats
            if self._stats_fn:
                stats = self._stats_fn()
                open_pos = stats.get("open_positions", 0)
                total_pnl = stats.get("total_pnl", 0)
                total_trades = stats.get("total_trades", 0)
                if total_trades > 0:
                    logger.info(
                        "STATS: %d trades | P&L=$%+.2f | %d open positions",
                        total_trades, total_pnl, open_pos,
                    )
