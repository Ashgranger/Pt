"""Thin Arcus transport: WebSocket for data and signed mutations with auto-reconnect support."""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
import urllib.request
from decimal import Decimal
from typing import Any, Callable, Optional

from config import ENVS, Config
from utils import Fatal

log = logging.getLogger("exchange")

try:
    import orjson as _orjson
    _loads = _orjson.loads
    _dumps = lambda o: _orjson.dumps(o).decode()
except ImportError:  # graceful fallback
    _loads = json.loads
    _dumps = json.dumps

try:
    from websockets.exceptions import ConnectionClosed
except ImportError:
    class ConnectionClosed(Exception):
        pass


class Exchange:
    def __init__(self, cfg: Config, on_channel: Callable[[str, Any, bool], None]):
        self.cfg = cfg
        self.rest = ENVS[cfg.env_name]["rest"]
        self.ws_url = ENVS[cfg.env_name]["ws"]
        self.on_channel = on_channel
        self.ws = None
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._dry_seq = itertools.count(1)
        self.last_rx = time.monotonic()
        self.on_degraded: Optional[Callable[[], None]] = None

    @property
    def is_connected(self) -> bool:
        return self.ws is not None and not getattr(self.ws, "closed", False)

    async def _send(self, obj: dict) -> bool:
        if not self.is_connected:
            return False
        try:
            await self.ws.send(_dumps(obj))
            return True
        except (ConnectionClosed, ConnectionResetError, BrokenPipeError, OSError) as e:
            log.warning("WebSocket send failed (closed): %s", e)
            self.ws = None
            self._fail_all_pending("CONNECTION_CLOSED")
            return False
        except Exception as e:
            log.warning("WebSocket send error: %s", e)
            return False

    def _fail_all_pending(self, reason: str = "CONNECTION_CLOSED") -> None:
        for rid, fut in list(self._pending.items()):
            if not fut.done():
                fut.set_result({"status": 503, "error": {"type": reason, "message": "WebSocket disconnected"}})
        self._pending.clear()

    async def call(self, kind: str, request: dict, timeout: float = 10.0) -> dict:
        if not self.is_connected and not self.cfg.dry_run:
            return {"status": 503, "error": {"type": "NOT_CONNECTED", "message": "WebSocket not connected"}}
        rid = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            sent = await self._send({"type": kind, "id": rid, "request": request})
            if not sent and not self.cfg.dry_run:
                return {"status": 503, "error": {"type": "CONNECTION_CLOSED", "message": "Failed to send over WebSocket"}}
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return {"status": 408, "error": {"type": "TIMEOUT", "message": f"Request {kind} timed out after {timeout}s"}}
        except Exception as e:
            return {"status": 500, "error": {"type": "SEND_ERROR", "message": str(e)}}
        finally:
            self._pending.pop(rid, None)

    async def write(self, request: dict) -> dict:
        if self.cfg.dry_run:
            rtype = request["type"]
            log.debug("DRY %s %s", rtype, json.dumps(request["payload"])[:200])
            if rtype == "placeOrder":
                return {"status": 202, "result": {"orderId": f"dry-{next(self._dry_seq)}", "status": "ACK"}}
            return {"status": 202, "result": {"status": "ACK"}}
        return await self.call("post", request)

    async def get(self, rtype: str, payload: dict, timeout: float = 8.0) -> Optional[Any]:
        r = await self.call("get", {"type": rtype, "payload": payload}, timeout)
        return r.get("result") if r.get("status") == 200 else None

    async def subscribe(self, channel: str, sub_id: str, **extra) -> bool:
        return await self._send({"type": "subscribe", "channel": channel, "id": sub_id, **extra})

    def handle_message(self, raw) -> None:
        self.last_rx = time.monotonic()
        try:
            msg = _loads(raw)
        except ValueError:
            return
        mtype = msg.get("type")
        if mtype in ("channel_data", "subscribed"):
            try:
                self.on_channel(msg.get("channel"), msg.get("contents"), mtype == "subscribed")
            except Exception:
                log.exception("channel handler error (%s)", msg.get("channel"))
            return
        rid = msg.get("id")
        if isinstance(rid, int) and rid in self._pending:
            fut = self._pending[rid]
            if not fut.done():
                fut.set_result(msg)
            return
        if mtype in ("error", "degraded") or "error" in msg:
            log.warning("server message: %s", str(raw)[:300])
        if mtype == "degraded" and self.on_degraded:
            try:
                self.on_degraded()
            except Exception:
                log.exception("on_degraded failed")

    async def reader(self) -> None:
        try:
            if not self.ws:
                return
            async for raw in self.ws:
                self.handle_message(raw)
        except (ConnectionClosed, ConnectionResetError, BrokenPipeError, OSError) as e:
            log.warning("WebSocket reader disconnected: %s", e)
        except Exception as e:
            log.warning("WebSocket reader unexpected error: %s", e)
        finally:
            self._fail_all_pending("CONNECTION_CLOSED")
            self.ws = None

    async def fetch_position(self, address: str, account_index: int, market_id: int) -> Optional[Decimal]:
        """Authoritative position over REST (GET /v1/positions, weight 2, independent of the WebSocket).
        `size` is SIGNED (+long / -short) per the Arcus API; a market with no row is flat. None = lookup failed."""
        if self.cfg.dry_run:
            return None

        def _get():
            from urllib.parse import urlencode
            q = urlencode({"address": address, "accountIndex": account_index, "market": market_id})
            with urllib.request.urlopen(f"{self.rest}/v1/positions?{q}", timeout=6) as r:
                return json.loads(r.read())

        try:
            data = await asyncio.to_thread(_get)
        except Exception as e:
            log.warning("REST positions lookup failed: %s", e)
            return None
        rows = data.get("positions") if isinstance(data, dict) else None
        if isinstance(rows, dict):
            rows = list(rows.values())
        for r in rows or []:
            if isinstance(r, dict) and int(r.get("marketId", -1)) == int(market_id):
                side = str(r.get("side", "")).upper()
                sz = Decimal(str(r.get("size", "0")))
                if side == "LONG":
                    return abs(sz)
                if side == "SHORT":
                    return -abs(sz)
                return sz
        return Decimal("0")

    async def fetch_markets(self, market: Optional[str] = None) -> list:
        def _get():
            url = f"{self.rest}/v1/markets" + (f"?market={market}" if market else "")
            with urllib.request.urlopen(url, timeout=10) as r:
                return json.loads(r.read())

        data = await asyncio.to_thread(_get)
        rows = data.get("markets") or []
        if market and not rows:
            raise Fatal(f"market {market} not found on {self.cfg.env_name}")
        return rows
