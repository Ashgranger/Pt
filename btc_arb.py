#!/usr/bin/env python3
"""
BTC 5-min UpDown — WebSocket Arb Detector
Polymarket CLOB market channel
"""

import asyncio
import json
import time
import sys
from datetime import datetime, timezone

import aiohttp
import websockets

# ── Constants ──────────────────────────────────────────────────────────────
GAMMA   = "https://gamma-api.polymarket.com"
WS_URL  = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_S  = 9      # send PING every 9s (server drops at 10s)
ARB_MIN = 0.001  # 0.1¢ minimum profit threshold

# ── ANSI colors (no deps) ─────────────────────────────────────────────────
R  = "\033[31m"   # red
G  = "\033[32m"   # green
Y  = "\033[33m"   # yellow
C  = "\033[36m"   # cyan
W  = "\033[97m"   # bright white
DM = "\033[2m"    # dim
BG = "\033[92m"   # bright green
BY = "\033[93m"   # bright yellow
B  = "\033[1m"    # bold
X  = "\033[0m"    # reset

# ── Price state ────────────────────────────────────────────────────────────
class TokenState:
    __slots__ = ("bid", "ask", "ts")
    def __init__(self):
        self.bid: float = 0.0
        self.ask: float = 0.0
        self.ts:  float = 0.0

    def set(self, bid: float, ask: float):
        self.bid = bid
        self.ask = ask
        self.ts  = time.monotonic()

    def age_ms(self) -> int:
        return int((time.monotonic() - self.ts) * 1000)

class Prices:
    def __init__(self):
        self.up   = TokenState()
        self.down = TokenState()

# ── Arb check ─────────────────────────────────────────────────────────────
def check_arb(p: Prices):
    ua, da = p.up.ask, p.down.ask
    if ua == 0.0 or da == 0.0:
        return None
    profit = 1.0 - (ua + da)
    return (ua, da, profit) if profit > ARB_MIN else None

# ── Display ────────────────────────────────────────────────────────────────
def print_arb(ua: float, da: float, profit: float, hits: int, elapsed_us: int, p: Prices):
    pct = (profit / (ua + da)) * 100
    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + " UTC"
    bar = "▓" * 66
    print(f"\n{BG}{bar}{X}")
    print(f"  {BY}★★★{X} ARB #{hits} │ {elapsed_us}µs │ {Y}{now}{X}")
    print(f"  UP  ask {C}{B}{ua:>8.4f}{X}  │  DOWN ask {C}{B}{da:>8.4f}{X}")
    print(f"  Cost {W}{ua+da:.4f}{X}  →  Profit {BG}{B}+${profit:.4f}{X}  ({BG}+{pct:.3f}%{X})")
    print(f"  bids UP {DM}{p.up.bid:.4f}{X} / DN {DM}{p.down.bid:.4f}{X}  age {DM}{p.up.age_ms()}ms{X}")
    print(f"{BG}{bar}{X}")

def print_tick(et: str, p: Prices, elapsed_us: int):
    ua, da = p.up.ask, p.down.ask
    if ua > 0.0 and da > 0.0:
        gap = ua + da - 1.0
        line = (
            f"\r  [{DM}{et}{X}] "
            f"UP {W}{p.up.bid:.3f}{X}/{Y}{ua:.3f}{X} "
            f"DN {W}{p.down.bid:.3f}{X}/{Y}{da:.3f}{X} "
            f"Σ={R}{ua+da:.4f}{X} gap={R}+{gap:.4f}{X} "
            f"{DM}{elapsed_us}µs{X}      "
        )
        print(line, end="", flush=True)

# ── Message handler ────────────────────────────────────────────────────────
def handle(raw: str, up_id: str, dn_id: str, p: Prices, state: dict):
    if raw == "PONG" or not raw:
        return

    t0 = time.perf_counter()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return

    # Polymarket wraps every event in a JSON array: [{...}]
    if isinstance(parsed, list):
        if not parsed:
            return
        msg = parsed[0]
    else:
        msg = parsed

    if not isinstance(msg, dict):
        return

    et = msg.get("event_type")
    if not et:
        return

    touched = False

    if et == "best_bid_ask":
        aid = msg.get("asset_id", "")
        bid = float(msg.get("best_bid", 0))
        ask = float(msg.get("best_ask", 0))
        if aid == up_id:
            p.up.set(bid, ask);   touched = True
        elif aid == dn_id:
            p.down.set(bid, ask); touched = True

    elif et == "price_change":
        for pc in msg.get("price_changes", []):
            aid = pc.get("asset_id", "")
            bid = float(pc.get("best_bid", 0))
            ask = float(pc.get("best_ask", 0))
            if aid == up_id:
                p.up.set(bid, ask);   touched = True
            elif aid == dn_id:
                p.down.set(bid, ask); touched = True

    elif et == "book":
        aid  = msg.get("asset_id", "")
        bids = msg.get("bids", [])
        asks = msg.get("asks", [])
        best_bid = max((float(b["price"]) for b in bids), default=0.0)
        raw_ask  = min((float(a["price"]) for a in asks), default=0.0)
        best_ask = raw_ask if raw_ask > 0 else 0.0
        if aid == up_id:
            p.up.set(best_bid, best_ask);   touched = True
        elif aid == dn_id:
            p.down.set(best_bid, best_ask); touched = True

    if not touched:
        return

    elapsed_us = int((time.perf_counter() - t0) * 1_000_000)

    result = check_arb(p)
    if result:
        state["hits"] += 1
        ua, da, profit = result
        print_arb(ua, da, profit, state["hits"], elapsed_us, p)
    else:
        print_tick(et, p, elapsed_us)

# ── Gamma API ─────────────────────────────────────────────────────────────
async def fetch_tokens(session: aiohttp.ClientSession, slug: str):
    url = f"{GAMMA}/markets?slug={slug}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
        text = await r.text()

    arr = json.loads(text)
    if not arr:
        raise ValueError(f"No market for slug: {slug}")

    m = arr[0]

    # clobTokenIds may be array or double-encoded string
    ids = None
    for field in ("clobTokenIds", "clob_token_ids"):
        v = m.get(field)
        if isinstance(v, list) and v:
            ids = v; break
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if parsed:
                    ids = parsed; break
            except json.JSONDecodeError:
                pass

    if not ids or len(ids) < 2:
        keys = list(m.keys())
        raise ValueError(f"Token IDs not found. Keys: {keys}")

    question = m.get("question", "BTC Up/Down 5m")
    return ids[0], ids[1], question

def current_ts() -> int:
    return (int(time.time()) // 300) * 300

def make_slug(ts: int) -> str:
    return f"btc-updown-5m-{ts}"

# ── WebSocket loop ─────────────────────────────────────────────────────────
async def run_ws(up_id: str, dn_id: str, prices: Prices, stop_event: asyncio.Event):
    state  = {"hits": 0}
    attempt = 0

    while not stop_event.is_set():
        attempt += 1
        if attempt > 1:
            print(f"\n  {Y}↻{X} Reconnect #{attempt}")
            await asyncio.sleep(0.5)

        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,   # we handle PING manually
                ping_timeout=None,
                close_timeout=5,
            ) as ws:

                sub = json.dumps({
                    "assets_ids": [up_id, dn_id],
                    "type": "market",
                    "custom_feature_enabled": True,
                })
                await ws.send(sub)
                print(f"  {G}✓{X} WS connected + subscribed")

                async def ping_loop():
                    while True:
                        await asyncio.sleep(PING_S)
                        try:
                            await ws.send("PING")
                        except Exception:
                            break

                ping_task = asyncio.create_task(ping_loop())

                try:
                    async for raw in ws:
                        if stop_event.is_set():
                            break
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        handle(raw, up_id, dn_id, prices, state)
                finally:
                    ping_task.cancel()

        except websockets.exceptions.ConnectionClosed as e:
            print(f"\n  {Y}!{X} WS closed: {e}")
        except Exception as e:
            print(f"\n  {R}✗{X} WS error: {e}")

# ── Market rollover watcher ────────────────────────────────────────────────
async def watcher(session, queue: asyncio.Queue, stop_event: asyncio.Event):
    last_ts = current_ts()
    while True:
        await asyncio.sleep(10)
        ts = current_ts()
        if ts == last_ts:
            continue
        last_ts = ts
        slug = make_slug(ts)
        print(f"\n  {C}↺{X} Rollover → {Y}{slug}{X}")
        stop_event.set()
        await asyncio.sleep(0.3)
        stop_event.clear()
        try:
            up, dn, q = await fetch_tokens(session, slug)
            await queue.put((up, dn, q))
        except Exception as e:
            print(f"  {R}✗{X} Token fetch: {e}")

# ── Main ───────────────────────────────────────────────────────────────────
async def main():
    print(f"\n{C}{B}  BTC 5-min UpDown — WS Arb Detector (Python){X}")
    print(f"  {DM}websockets + aiohttp | µs arb check | auto rollover{X}\n")

    async with aiohttp.ClientSession(
        headers={"User-Agent": "btc-arb/0.2"}
    ) as session:

        ts   = current_ts()
        slug = make_slug(ts)
        print(f"  Fetching: {Y}{slug}{X}")

        up, dn, q = await fetch_tokens(session, slug)
        print(f"  Market : {W}{B}{q}{X}")
        print(f"  UP     …{DM}{up[-16:]}{X}")
        print(f"  DOWN   …{DM}{dn[-16:]}{X}\n")

        prices     = Prices()
        stop_event = asyncio.Event()
        queue      = asyncio.Queue()

        # Rollover watcher
        asyncio.create_task(watcher(session, queue, stop_event))

        # Initial WS task
        ws_task = asyncio.create_task(run_ws(up, dn, prices, stop_event))

        while True:
            done, _ = await asyncio.wait(
                [ws_task, asyncio.create_task(queue.get())],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                result = task.result() if not task.cancelled() else None
                if isinstance(result, tuple):
                    # New market from rollover
                    new_up, new_dn, new_q = result
                    print(f"\n  {BY}★{X} New market: {W}{B}{new_q}{X}")
                    ws_task.cancel()
                    prices  = Prices()
                    ws_task = asyncio.create_task(run_ws(new_up, new_dn, prices, stop_event))
                else:
                    # WS task ended
                    print(f"\n  {Y}↻{X} Task ended — restarting")
                    ws_task = asyncio.create_task(run_ws(up, dn, prices, stop_event))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n  {DM}Stopped.{X}")
