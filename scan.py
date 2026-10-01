"""`python main.py scan` - find markets where there is actually a spread to capture."""
from __future__ import annotations

import asyncio
import json
import statistics
import time
from decimal import Decimal

from config import ENVS, Config
from exchange import Exchange
from market import Market
from utils import BPS


async def scan(cfg: Config, seconds: float = 30.0) -> None:
    try:
        import websockets
    except ImportError:
        print("websockets package required for scanner: pip install websockets")
        return

    ex = Exchange(cfg, lambda *a: None)
    markets = [Market.from_api(r) for r in await ex.fetch_markets()]
    online = [m for m in markets if m.status == "ONLINE"]
    print(f"{len(online)} ONLINE markets on {cfg.env_name}; sampling bbo for {seconds:.0f}s ...")
    data: dict[str, dict] = {m.name: {"spread": [], "mid": [], "n": 0, "m": m} for m in online}

    async with websockets.connect(ENVS[cfg.env_name]["ws"], ping_interval=15, max_size=2 ** 23) as ws:
        for m in online:
            await ws.send(json.dumps({"type": "subscribe", "channel": "bbo", "id": m.name}))
        end = time.monotonic() + seconds
        while (left := end - time.monotonic()) > 0:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), left))
            except asyncio.TimeoutError:
                break
            if msg.get("type") not in ("channel_data", "subscribed") or msg.get("channel") != "bbo":
                continue
            d, c = data.get(msg.get("id")), msg.get("contents") or {}
            if not d or not c.get("bestBid") or not c.get("bestAsk"):
                continue
            bid, ask = Decimal(str(c["bestBid"]["price"])), Decimal(str(c["bestAsk"]["price"]))
            if bid >= ask:
                continue
            mid = (bid + ask) / 2
            d["spread"].append(float((ask - bid) / mid * BPS))
            d["mid"].append(float(mid))
            d["n"] += 1

    rows = []
    for name, d in data.items():
        if len(d["mid"]) < 3:
            continue
        mid0 = statistics.median(d["mid"])
        move = (max(d["mid"]) - min(d["mid"])) / mid0 * 1e4
        spr = statistics.median(d["spread"])
        tick_bps = float(d["m"].tick) / mid0 * 1e4
        rows.append((spr / max(move, 0.1), name, spr, tick_bps, move, d["n"], d["m"].is_outside_rth))
    rows.sort(reverse=True)
    print(f"\n{'market':<14}{'spread bps':>11}{'tick bps':>10}{'range bps':>11}{'updates':>9}{'spr/range':>10}  note")
    for score, name, spr, tick_bps, move, n, rth in rows[:25]:
        note = "closed (outside RTH)" if rth else ("spread <= 1 tick: nothing to capture at touch" if spr <= tick_bps * 1.05 else "")
        print(f"{name:<14}{spr:>11.2f}{tick_bps:>10.3f}{move:>11.2f}{n:>9}{score:>10.2f}  {note}")
    print("\nPick a market where spread bps >= ~2x your MIN_EDGE_BPS and spr/range is high, then set MARKET=... in .env")
