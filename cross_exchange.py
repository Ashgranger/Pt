"""Public cross-exchange BBO feeds for Binance and Bybit.

No trading credentials are used.  The feeds are read-only public WebSockets and
forward normalized best-bid/ask updates into MarketMaker.on_external_venue_bbo().
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import Awaitable, Callable, Optional

log = logging.getLogger("cross_exchange")

BBOCallback = Callable[[str, Decimal, Decimal, Decimal, Decimal], None]


class CrossExchangeFeed:
    def __init__(self, cfg, callback: BBOCallback):
        self.cfg = cfg
        self.callback = callback
        self.stop_evt = asyncio.Event()
        self.tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        if not self.cfg.enable_cross_exchange:
            return
        if self.cfg.binance_enabled:
            self.tasks.append(asyncio.create_task(self._run_binance(), name="binance-bbo"))
        if self.cfg.bybit_enabled:
            self.tasks.append(asyncio.create_task(self._run_bybit(), name="bybit-bbo"))
        if self.tasks:
            log.info("Cross-exchange feeds enabled: %s", ", ".join(
                [x for x, enabled in (("Binance", self.cfg.binance_enabled), ("Bybit", self.cfg.bybit_enabled)) if enabled]
            ))

    async def stop(self) -> None:
        self.stop_evt.set()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

    async def _connect(self, url: str):
        import websockets
        return await websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            max_size=2**20,
        )

    async def _run_binance(self) -> None:
        symbol = self.cfg.binance_symbol.lower()
        url = f"{self.cfg.binance_ws_url.rstrip('/')}/ws/{symbol}@bookTicker"
        delay = 1.0
        while not self.stop_evt.is_set():
            try:
                log.info("Connecting Binance public BBO: %s", symbol.upper())
                async with await self._connect(url) as ws:
                    delay = 1.0
                    while not self.stop_evt.is_set():
                        raw = await ws.recv()
                        msg = json.loads(raw)
                        # Binance bookTicker: b/B = bid price/qty, a/A = ask price/qty.
                        bid = Decimal(str(msg["b"]))
                        ask = Decimal(str(msg["a"]))
                        bid_sz = Decimal(str(msg.get("B", "0")))
                        ask_sz = Decimal(str(msg.get("A", "0")))
                        if bid > 0 and ask > bid:
                            self.callback("BINANCE", bid, ask, bid_sz, ask_sz)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Binance BBO feed disconnected: %s; retrying in %.1fs", exc, delay)
                await self._sleep(delay)
                delay = min(delay * 2.0, 30.0)

    async def _run_bybit(self) -> None:
        symbol = self.cfg.bybit_symbol.upper()
        url = self.cfg.bybit_ws_url
        topic = f"tickers.{symbol}"
        delay = 1.0
        while not self.stop_evt.is_set():
            try:
                log.info("Connecting Bybit public BBO: %s (%s)", symbol, self.cfg.bybit_category)
                async with await self._connect(url) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))
                    delay = 1.0
                    while not self.stop_evt.is_set():
                        raw = await ws.recv()
                        msg = json.loads(raw)
                        if msg.get("op") in ("subscribe", "ping", "pong"):
                            continue
                        if msg.get("topic") != topic:
                            continue
                        data = msg.get("data") or {}
                        # Linear ticker uses bid1Price/bid1Size/ask1Price/ask1Size.
                        bid = Decimal(str(data.get("bid1Price", "0")))
                        ask = Decimal(str(data.get("ask1Price", "0")))
                        bid_sz = Decimal(str(data.get("bid1Size", "0")))
                        ask_sz = Decimal(str(data.get("ask1Size", "0")))
                        if bid > 0 and ask > bid:
                            self.callback("BYBIT", bid, ask, bid_sz, ask_sz)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Bybit BBO feed disconnected: %s; retrying in %.1fs", exc, delay)
                await self._sleep(delay)
                delay = min(delay * 2.0, 30.0)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop_evt.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass


class CrossExchangeSupervisor:
    """Small wrapper so the trading bot owns one lifecycle for both feeds."""
    def __init__(self, cfg, callback: BBOCallback):
        self.feed = CrossExchangeFeed(cfg, callback)

    async def start(self) -> None:
        await self.feed.start()

    async def stop(self) -> None:
        await self.feed.stop()
