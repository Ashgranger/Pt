"""External-venue market-data feeds (Binance USDT-M futures, Bybit v5 linear).

Read-only public streams. Each feed parses exchange messages and pushes them into a
`sink` (the MarketMaker) via:
    sink.on_external_venue_bbo(venue, bid, ask, bid_sz, ask_sz)
    sink.on_external_depth(venue, bids, asks)
    sink.on_external_trade(venue, side, size, price)       # side = AGGRESSOR side
    sink.on_external_liq(venue, forced_side, size, price)  # forced_side SELL = long liquidated
    sink.on_external_disconnect(venue)

Handlers are pure (`handle(raw)`) so they can be unit-tested without a network.
"""
from __future__ import annotations

import asyncio
import heapq
import json
import logging
from decimal import Decimal
from typing import Any, Callable, Optional

log = logging.getLogger("feeds")

try:  # optional speedup
    import orjson as _orjson
    _loads = _orjson.loads
except ImportError:  # pragma: no cover
    _loads = json.loads

D = Decimal


def derive_symbol(market: str, override: str = "") -> str:
    """Arcus 'SOL-USD' -> 'SOLUSDT' (override with BINANCE_SYMBOL / BYBIT_SYMBOL, e.g. 1000PEPEUSDT)."""
    if override:
        return override.upper()
    base = market.upper().split("-")[0].split("/")[0].strip()
    return f"{base}USDT"


class VenueFeed:
    name = "VENUE"
    idle_timeout_s = 10.0

    def __init__(self, sink: Any, symbol: str, url: str, connect: Optional[Callable] = None):
        self.sink = sink
        self.symbol = symbol
        self.url = url
        self._connect = connect
        self.msgs = 0
        self.connected = False
        self._stop = False

    # -- to override ------------------------------------------------------ #
    def full_url(self) -> str:
        return self.url

    async def on_open(self, ws) -> None:
        return None

    def handle(self, raw) -> None:
        raise NotImplementedError

    async def keepalive(self, ws) -> None:
        return None

    # -- connection loop -------------------------------------------------- #
    def _factory(self):
        if self._connect is not None:
            return self._connect
        import websockets  # lazy: bot still runs (without cross feeds) if missing
        return lambda url: websockets.connect(url, ping_interval=15, ping_timeout=20,
                                              max_size=2 ** 22, close_timeout=3, compression=None)

    async def run(self) -> None:
        delay = 1.0
        while not self._stop:
            ka = None
            try:
                url = self.full_url()
                log.info("[%s] connecting %s", self.name, url)
                async with self._factory()(url) as ws:
                    self.connected = True
                    self.msgs = 0
                    await self.on_open(ws)
                    ka = asyncio.create_task(self.keepalive(ws))
                    while not self._stop:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
                        except asyncio.TimeoutError:
                            log.warning("[%s] no data for %.0fs - reconnecting", self.name, self.idle_timeout_s)
                            break
                        try:
                            self.handle(raw)
                        except Exception as e:  # never let a bad packet kill the feed
                            log.debug("[%s] bad message: %r", self.name, e)
                        self.msgs += 1
                        delay = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] feed error: %s", self.name, e)
            finally:
                if ka:
                    ka.cancel()
                if self.connected:
                    self.connected = False
                    self.sink.on_external_disconnect(self.name)
            if self._stop:
                break
            log.info("[%s] reconnecting in %.1fs", self.name, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15.0)

    def stop(self) -> None:
        self._stop = True


# ---------------------------------------------------------------------------- #
class BinanceFeed(VenueFeed):
    """wss://fstream.binance.com combined stream: bookTicker + depth10@100ms + aggTrade + forceOrder."""
    name = "BINANCE"

    def full_url(self) -> str:
        s = self.symbol.lower()
        streams = f"{s}@bookTicker/{s}@depth10@100ms/{s}@aggTrade/{s}@forceOrder"
        return f"{self.url}/stream?streams={streams}"

    def handle(self, raw) -> None:
        msg = _loads(raw)
        d = msg.get("data", msg)
        e = d.get("e")
        if e == "bookTicker":
            self.sink.on_external_venue_bbo(self.name, D(d["b"]), D(d["a"]), D(d["B"]), D(d["A"]))
        elif e == "depthUpdate":
            self.sink.on_external_depth(self.name, d["b"], d["a"])
        elif e == "aggTrade":
            # m == True -> buyer is the maker -> the aggressor SOLD
            self.sink.on_external_trade(self.name, "SELL" if d["m"] else "BUY", D(d["q"]), D(d["p"]))
        elif e == "forceOrder":
            o = d["o"]
            price = D(o.get("ap") or o.get("p"))
            if price > 0:
                self.sink.on_external_liq(self.name, str(o["S"]).upper(), D(o.get("z") or o["q"]), price)


# ---------------------------------------------------------------------------- #
class BybitFeed(VenueFeed):
    """wss://stream.bybit.com/v5/public/linear: orderbook.50 (snapshot+delta) + publicTrade + liquidation."""
    name = "BYBIT"
    DEPTH = 10

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._bids: dict = {}
        self._asks: dict = {}

    async def on_open(self, ws) -> None:
        self._bids.clear()
        self._asks.clear()
        sym = self.symbol
        await ws.send(json.dumps({"op": "subscribe", "args": [
            f"orderbook.50.{sym}", f"publicTrade.{sym}", f"liquidation.{sym}"]}))

    async def keepalive(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(20)
                await ws.send(json.dumps({"op": "ping"}))
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    def _apply(self, book: dict, rows: list) -> None:
        for r in rows:
            px = r[0]
            if D(r[1]) == 0:
                book.pop(px, None)
            else:
                book[px] = r[1]

    def handle(self, raw) -> None:
        msg = _loads(raw)
        if "topic" not in msg:
            if msg.get("success") is False:
                log.error("[BYBIT] subscribe failed: %s (check BYBIT_SYMBOL=%s)", msg.get("ret_msg"), self.symbol)
            return
        topic = msg["topic"]
        data = msg["data"]
        if topic.startswith("orderbook."):
            if msg.get("type") == "snapshot" or data.get("u") == 1:
                self._bids.clear()
                self._asks.clear()
            self._apply(self._bids, data.get("b", []))
            self._apply(self._asks, data.get("a", []))
            if not self._bids or not self._asks:
                return
            top_b = heapq.nlargest(self.DEPTH, self._bids, key=lambda p: float(p))
            top_a = heapq.nsmallest(self.DEPTH, self._asks, key=lambda p: float(p))
            bids = [(p, self._bids[p]) for p in top_b]
            asks = [(p, self._asks[p]) for p in top_a]
            if D(bids[0][0]) >= D(asks[0][0]):   # crossed => out-of-sync book, wait for next snapshot
                return
            self.sink.on_external_venue_bbo(self.name, D(bids[0][0]), D(asks[0][0]), D(bids[0][1]), D(asks[0][1]))
            self.sink.on_external_depth(self.name, bids, asks)
        elif topic.startswith("publicTrade."):
            for t in data:
                self.sink.on_external_trade(self.name, str(t["S"]).upper(), D(t["v"]), D(t["p"]))
        elif topic.startswith("liquidation."):
            rows = data if isinstance(data, list) else [data]
            for t in rows:
                # Bybit 'side' = side of the POSITION liquidated (Buy = long) -> forced order is the opposite
                forced = "SELL" if str(t["side"]).lower() == "buy" else "BUY"
                self.sink.on_external_liq(self.name, forced, D(t["size"]), D(t["price"]))


# ---------------------------------------------------------------------------- #
class CrossFeedManager:
    def __init__(self, cfg, sink, connect: Optional[Callable] = None):
        self.cfg = cfg
        self.feeds: list = []
        self.tasks: list = []
        venues = {v.strip() for v in str(cfg.cross_venues).split(",") if v.strip()}
        if "binance" in venues:
            self.feeds.append(BinanceFeed(sink, derive_symbol(cfg.market, cfg.binance_symbol),
                                          cfg.binance_ws_url, connect))
        if "bybit" in venues:
            self.feeds.append(BybitFeed(sink, derive_symbol(cfg.market, cfg.bybit_symbol),
                                        cfg.bybit_ws_url, connect))

    def start(self) -> None:
        for f in self.feeds:
            log.info("Cross-venue feed: %s %s", f.name, f.symbol)
            self.tasks.append(asyncio.create_task(f.run()))

    async def stop(self) -> None:
        for f in self.feeds:
            f.stop()
        for t in self.tasks:
            t.cancel()
        for t in self.tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self.tasks.clear()
