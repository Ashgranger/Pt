"""Thin Arcus transport: WebSocket for data and signed mutations."""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import urllib.request
from typing import Any, Callable, Optional

from config import ENVS, Config
from utils import Fatal

log = logging.getLogger("exchange")


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

    async def _send(self, obj: dict) -> None:
        await self.ws.send(json.dumps(obj))

    async def call(self, kind: str, request: dict, timeout: float = 10.0) -> dict:
        rid = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            await self._send({"type": kind, "id": rid, "request": request})
            return await asyncio.wait_for(fut, timeout)
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

    async def subscribe(self, channel: str, sub_id: str, **extra) -> None:
        await self._send({"type": "subscribe", "channel": channel, "id": sub_id, **extra})

    def handle_message(self, raw) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
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

    async def reader(self) -> None:
        async for raw in self.ws:
            self.handle_message(raw)

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
