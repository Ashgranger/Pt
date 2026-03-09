#!/usr/bin/env python3
"""
BTC 5-min UpDown — Superfast WS Arb Bot
========================================
• WS price feed → arb check in <5µs (hot path: no alloc, no lock)
• Parallel BUY orders fired via asyncio.gather (both legs simultaneous)
• Partial fill: poll every 500ms, cancel excess on imbalance
• One-leg failure: instant cancel of surviving leg
• Order heartbeat every 5s (CLOB cancels all if 10s gap)
• Auto-rollover on 5-min boundary
• DETECT-ONLY mode if no env vars set

Setup:
  pip install aiohttp websockets py-clob-client
  export PRIVATE_KEY=0x...
  export API_KEY=...
  export API_SECRET=...
  export API_PASSPHRASE=...
  export FUNDER_ADDRESS=0x...
  python btc_arb.py
"""

import asyncio, json, os, time, concurrent.futures

# ── Load .env file (falls back to real env vars if not present) ────────────
try:
    from dotenv import load_dotenv
    _env_loaded = load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=False)
except ImportError:
    _env_loaded = False
    print("\033[2m  tip: pip install python-dotenv to use .env file\033[0m")

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import aiohttp
import websockets

# ── Optional trading client ────────────────────────────────────────────────
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY
    HAS_CLOB = True
except ImportError:
    HAS_CLOB = False

# ── Endpoints ──────────────────────────────────────────────────────────────
GAMMA    = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
WS_URL   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID = 137

# ── Tuning ─────────────────────────────────────────────────────────────────
ARB_MIN       = 0.02    # min profit to fire — 2% threshold
ORDER_SIZE    = 5.0    # USDC per leg
FILL_TIMEOUT  = 30      # seconds before timeout handler
POLL_INTERVAL = 0.5     # fill poll interval
MAX_POSITIONS = 3       # max open arb positions
PING_S        = 9       # WS PING interval
HEARTBEAT_S   = 5       # order heartbeat (must be < 10s)

# ── ANSI ───────────────────────────────────────────────────────────────────
R="\033[31m"; G="\033[32m"; Y="\033[33m"; C="\033[36m"
W="\033[97m"; DIM="\033[2m"; BG="\033[92m"; BY="\033[93m"
B="\033[1m";  X="\033[0m"

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]

def log(sym, col, msg):
    print(f"  {col}{sym}{X} [{DIM}{ts()}{X}] {msg}")

# ── Price state (single asyncio thread — zero lock overhead) ────────────────
class Tok:
    __slots__ = ("bid", "ask", "t")
    def __init__(self):
        self.bid = 0.0; self.ask = 0.0; self.t = 0.0
    def set(self, bid, ask):
        self.bid = bid; self.ask = ask; self.t = time.monotonic()

class Prices:
    __slots__ = ("up", "dn")
    def __init__(self):
        self.up = Tok(); self.dn = Tok()

# ── Arb check — ~1µs hot path ──────────────────────────────────────────────
def arb_check(p: Prices):
    ua, da = p.up.ask, p.dn.ask
    if ua == 0.0 or da == 0.0:
        return None
    profit = 1.0 - ua - da
    return (ua, da, profit) if profit > ARB_MIN else None

# ── Position model ─────────────────────────────────────────────────────────
class S(Enum):
    PENDING="pending"; LIVE="live"; PARTIAL="partial"
    FILLED="filled";  FAILED="failed"; CANCELLED="cancelled"

@dataclass
class Leg:
    side:     str
    token_id: str
    price:    float
    size:     float
    order_id: str   = ""
    filled:   float = 0.0
    status:   S     = S.PENDING
    placed:   float = field(default_factory=time.monotonic)

    @property
    def remaining(self): return max(0.0, self.size - self.filled)
    @property
    def pct(self): return (self.filled / self.size * 100) if self.size else 0.0
    @property
    def done(self): return self.status in (S.FILLED, S.FAILED, S.CANCELLED)

@dataclass
class Pos:
    pid:    int
    up:     Leg
    dn:     Leg
    profit: float
    t0:     float = field(default_factory=time.monotonic)
    def both_filled(self): return self.up.status==S.FILLED and self.dn.status==S.FILLED
    def any_failed(self):  return self.up.status==S.FAILED  or self.dn.status==S.FAILED

# ── Global state ───────────────────────────────────────────────────────────
_positions  = []
_pid        = 0
_in_flight  = False
_hb_id      = ""
_clob       = None
_executor   = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="clob")

# ── Blocking CLOB calls (run in thread pool) ───────────────────────────────
def _place_b(token_id, price, size, tick_size, neg_risk):
    try:
        return _clob.create_and_post_order(
            OrderArgs(token_id=token_id, price=round(price,4),
                      size=round(size,2), side=BUY),
            OrderType.GTC,
            options={"tick_size": tick_size, "neg_risk": neg_risk},
        ) or {}
    except Exception as e:
        return {"errorMsg": str(e)}

def _cancel_b(order_id):
    try:    return _clob.cancel(order_id=order_id) or {}
    except Exception as e: return {"error": str(e)}

def _poll_b(order_id):
    try:    return _clob.get_order(order_id)
    except: return None

def _hb_b(hb_id):
    try:    return (_clob.post_heartbeat(hb_id) or {}).get("heartbeat_id", hb_id)
    except: return hb_id

# ── Async wrappers ─────────────────────────────────────────────────────────
async def _place(tid, price, size, tick_size, neg_risk):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _place_b, tid, price, size, tick_size, neg_risk)

async def _cancel(oid):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _cancel_b, oid)

async def _poll(oid):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _poll_b, oid)

# ── Apply order response to Leg ────────────────────────────────────────────
def _apply(leg, resp, label):
    oid = resp.get("orderID") or resp.get("order_id") or ""
    err = resp.get("errorMsg") or resp.get("error") or ""
    st  = resp.get("status", "")
    if oid:
        leg.order_id = oid
        leg.status   = S.FILLED if st == "matched" else S.LIVE
        if leg.status == S.FILLED: leg.filled = leg.size
        log("✓", G, f"{label} oid={oid[:14]}… insert={st or 'live'}")
    else:
        leg.status = S.FAILED
        log("✗", R, f"{label} FAILED — {err or 'no orderID'}")

# ── Fire arb: parallel place + one-leg guard ───────────────────────────────
async def fire_arb(ua, da, profit, up_id, dn_id, tick_size, neg_risk):
    global _in_flight, _pid

    if _in_flight: return
    if len(_positions) >= MAX_POSITIONS:
        log("⚠", Y, f"MAX_POSITIONS={MAX_POSITIONS} reached"); return

    _in_flight = True
    _pid += 1
    pid   = _pid

    up_leg = Leg("UP", up_id, ua, ORDER_SIZE)
    dn_leg = Leg("DN", dn_id, da, ORDER_SIZE)
    pos    = Pos(pid, up_leg, dn_leg, profit)
    _positions.append(pos)

    t0 = time.perf_counter()
    print(f"\n{'▓'*62}")
    log("★", BY, f"{B}ARB #{pid}{X}  profit=${profit*ORDER_SIZE*2:.4f}  ({profit*100:.3f}%/leg)")
    log("→", C,  f"UP BUY {ORDER_SIZE} @ {B}{ua:.4f}{X}   DN BUY {ORDER_SIZE} @ {B}{da:.4f}{X}")

    if not _clob:
        log("⚡", Y, "DETECT-ONLY — orders not placed")
        print(f"{'▓'*62}\n")
        _in_flight = False
        _positions.remove(pos)
        return

    # ── PARALLEL fire — hit the network simultaneously ─────────────────────
    up_resp, dn_resp = await asyncio.gather(
        _place(up_id, ua, ORDER_SIZE, tick_size, neg_risk),
        _place(dn_id, da, ORDER_SIZE, tick_size, neg_risk),
    )
    ms = (time.perf_counter() - t0) * 1000
    _apply(up_leg, up_resp, "UP")
    _apply(dn_leg, dn_resp, "DN")
    log("⏱", DIM, f"Both legs placed in {ms:.1f}ms")

    # ── One-leg failure: cancel survivor immediately ───────────────────────
    if up_leg.status == S.FAILED and not dn_leg.done:
        log("⚠", Y, "UP failed → cancel DN (naked guard)")
        await _cancel(dn_leg.order_id); dn_leg.status = S.CANCELLED
    elif dn_leg.status == S.FAILED and not up_leg.done:
        log("⚠", Y, "DN failed → cancel UP (naked guard)")
        await _cancel(up_leg.order_id); up_leg.status = S.CANCELLED

    print(f"{'▓'*62}\n")
    _in_flight = False

    if up_leg.status in (S.LIVE, S.FILLED) or dn_leg.status in (S.LIVE, S.FILLED):
        asyncio.create_task(_watch(pos))
    else:
        _positions.remove(pos)

# ── Fill watcher ───────────────────────────────────────────────────────────
async def _watch(pos: Pos):
    deadline = time.monotonic() + FILL_TIMEOUT

    while time.monotonic() < deadline:

        # Parallel poll both legs
        await asyncio.gather(_update_leg(pos.up), _update_leg(pos.dn))

        if pos.both_filled():
            print(); _complete(pos); _positions.remove(pos); return

        if pos.any_failed():
            print(); await _handle_failed(pos)
            if pos in _positions: _positions.remove(pos)
            return

        scol = lambda l: {S.FILLED:BG,S.PARTIAL:Y,S.FAILED:R,S.CANCELLED:R}.get(l.status,C)
        print(
            f"\r  [{DIM}{ts()}{X}] #{pos.pid} "
            f"UP {scol(pos.up)}{pos.up.status.value}{X} {pos.up.filled:.2f}/{pos.up.size:.2f} "
            f"DN {scol(pos.dn)}{pos.dn.status.value}{X} {pos.dn.filled:.2f}/{pos.dn.size:.2f}   ",
            end="", flush=True,
        )
        await asyncio.sleep(POLL_INTERVAL)

    print(); await _handle_timeout(pos)
    if pos in _positions: _positions.remove(pos)

async def _update_leg(leg: Leg):
    if leg.done or not leg.order_id: return
    data = await _poll(leg.order_id)
    if not data: return
    orig    = float(data.get("original_size", leg.size) or leg.size)
    matched = float(data.get("size_matched",  0)        or 0)
    leg.filled = matched
    if   matched >= orig * 0.9999: leg.status = S.FILLED
    elif matched >  0:             leg.status = S.PARTIAL

def _complete(pos: Pos):
    pnl = pos.profit * (pos.up.filled + pos.dn.filled) / 2
    log("✅", BG, f"{B}Pos #{pos.pid} COMPLETE{X}  {time.monotonic()-pos.t0:.1f}s  PnL≈${pnl:.4f}")

async def _handle_failed(pos: Pos):
    u, d = pos.up, pos.dn
    log("✗", R, f"Pos #{pos.pid} — cancelling survivor")
    if u.status==S.FAILED and not d.done: await _cancel(d.order_id); d.status=S.CANCELLED
    elif d.status==S.FAILED and not u.done: await _cancel(u.order_id); u.status=S.CANCELLED

async def _handle_timeout(pos: Pos):
    """
    Partial fill timeout strategy:
      FILLED + unfilled   → cancel unfilled, log naked exposure
      PARTIAL + PARTIAL   → cancel excess on over-filled leg, accept partial match
      both LIVE/unfilled  → cancel both (no liquidity)
    """
    u, d = pos.up, pos.dn
    log("⏰", Y, f"Pos #{pos.pid} timeout  UP={u.status.value}({u.pct:.0f}%)  DN={d.status.value}({d.pct:.0f}%)")

    if u.status == S.FILLED and not d.done:
        await _cancel(d.order_id); d.status = S.CANCELLED
        log("📊", Y, f"Naked UP +{u.filled-d.filled:.2f} shares — manual management")

    elif d.status == S.FILLED and not u.done:
        await _cancel(u.order_id); u.status = S.CANCELLED
        log("📊", Y, f"Naked DN +{d.filled-u.filled:.2f} shares — manual management")

    elif u.status == S.PARTIAL and d.status == S.PARTIAL:
        # Cancel the leg with MORE fill to equalise, keep matched portion
        if not u.done and u.filled > d.filled:
            await _cancel(u.order_id); u.status = S.CANCELLED
        elif not d.done and d.filled > u.filled:
            await _cancel(d.order_id); d.status = S.CANCELLED
        matched = min(u.filled, d.filled)
        log("📊", C, f"Partial match {matched:.2f} shares  PnL≈${pos.profit*matched:.4f}")

    else:
        tasks = ([_cancel(u.order_id)] if not u.done and u.order_id else []) + \
                ([_cancel(d.order_id)] if not d.done and d.order_id else [])
        if tasks: await asyncio.gather(*tasks)
        u.status = d.status = S.CANCELLED
        log("⚠", Y, f"Pos #{pos.pid} — zero fill, both cancelled")

# ── Heartbeat ─────────────────────────────────────────────────────────────
async def heartbeat_loop():
    global _hb_id
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(HEARTBEAT_S)
        _hb_id = await loop.run_in_executor(_executor, _hb_b, _hb_id)

# ── WS message handler (µs-fast, sync, no await) ───────────────────────────
def handle_msg(raw, up_id, dn_id, p, arb_q, tick_size, neg_risk):
    if raw == "PONG" or not raw: return
    t0 = time.perf_counter()

    try:    parsed = json.loads(raw)
    except: return

    if isinstance(parsed, list):
        if not parsed: return
        msg = parsed[0]
    else:
        msg = parsed

    if not isinstance(msg, dict): return
    et = msg.get("event_type")
    if not et: return

    touched = False

    if et == "best_bid_ask":
        aid = msg.get("asset_id","")
        bid = float(msg.get("best_bid",0) or 0)
        ask = float(msg.get("best_ask",0) or 0)
        if   aid == up_id: p.up.set(bid,ask); touched=True
        elif aid == dn_id: p.dn.set(bid,ask); touched=True

    elif et == "price_change":
        for pc in msg.get("price_changes",[]):
            aid = pc.get("asset_id","")
            bid = float(pc.get("best_bid",0) or 0)
            ask = float(pc.get("best_ask",0) or 0)
            if   aid == up_id: p.up.set(bid,ask); touched=True
            elif aid == dn_id: p.dn.set(bid,ask); touched=True

    elif et == "book":
        aid  = msg.get("asset_id","")
        bids = msg.get("bids",[])
        asks = msg.get("asks",[])
        bb   = max((float(b["price"]) for b in bids), default=0.0)
        ba   = min((float(a["price"]) for a in asks), default=0.0)
        if   aid == up_id: p.up.set(bb,ba); touched=True
        elif aid == dn_id: p.dn.set(bb,ba); touched=True

    if not touched: return

    elapsed_us = int((time.perf_counter() - t0) * 1_000_000)
    result = arb_check(p)
    ua, da = p.up.ask, p.dn.ask

    if result:
        ua, da, profit = result
        try:   arb_q.put_nowait((ua, da, profit, tick_size, neg_risk))
        except asyncio.QueueFull: pass
        print(f"\n{'▓'*62}")
        log("★", BY, f"{B}ARB{X} UP={B}{ua:.4f}{X} DN={B}{da:.4f}{X} "
                     f"profit={BG}{B}+{profit*100:.3f}%{X} {DIM}{elapsed_us}µs{X}")
        print(f"{'▓'*62}")
    else:
        if ua > 0 and da > 0:
            s = ua + da
            print(
                f"\r  [{DIM}{et[:3]}{X}] "
                f"UP {W}{p.up.bid:.3f}{X}/{Y}{ua:.3f}{X} "
                f"DN {W}{p.dn.bid:.3f}{X}/{Y}{da:.3f}{X} "
                f"Σ={R}{s:.4f}{X} gap={DIM}+{s-1:.4f}{X} "
                f"{DIM}{elapsed_us}µs{X}   ",
                end="", flush=True,
            )

# ── Arb executor (serial — drains queue one at a time) ────────────────────
async def arb_executor(q, up_id, dn_id):
    while True:
        ua, da, profit, tick_size, neg_risk = await q.get()
        await fire_arb(ua, da, profit, up_id, dn_id, tick_size, neg_risk)

# ── WebSocket loop ─────────────────────────────────────────────────────────
async def run_ws(up_id, dn_id, tick_size, neg_risk, prices, arb_q, stop):
    attempt = 0
    while not stop.is_set():
        attempt += 1
        if attempt > 1:
            log("↻", Y, f"Reconnect #{attempt}")
            await asyncio.sleep(min(0.5 * attempt, 5))
        try:
            async with websockets.connect(
                WS_URL, ping_interval=None, ping_timeout=None,
                close_timeout=5, max_size=2**20,
            ) as ws:
                await ws.send(json.dumps({
                    "assets_ids": [up_id, dn_id],
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                log("✓", G, "WS connected + subscribed")
                attempt = 0

                async def _ping():
                    while True:
                        await asyncio.sleep(PING_S)
                        try:   await ws.send("PING")
                        except: break

                ping_task = asyncio.create_task(_ping())
                try:
                    async for raw in ws:
                        if stop.is_set(): break
                        if isinstance(raw, bytes): raw = raw.decode()
                        handle_msg(raw, up_id, dn_id, prices, arb_q, tick_size, neg_risk)
                finally:
                    ping_task.cancel()

        except websockets.exceptions.ConnectionClosed as e:
            log("!", Y, f"WS closed {e.code}")
        except Exception as e:
            log("✗", R, f"WS error: {e}")

# ── Gamma API ──────────────────────────────────────────────────────────────
async def fetch_market(session, slug):
    async with session.get(
        f"{GAMMA}/markets?slug={slug}",
        timeout=aiohttp.ClientTimeout(total=8),
    ) as r:
        arr = json.loads(await r.text())
    if not arr: raise ValueError(f"No market: {slug}")
    m = arr[0]
    ids = None
    for key in ("clobTokenIds","clob_token_ids"):
        v = m.get(key)
        if isinstance(v,list) and v: ids=v; break
        if isinstance(v,str):
            try:
                p2=json.loads(v)
                if isinstance(p2,list) and p2: ids=p2; break
            except: pass
    if not ids or len(ids)<2:
        raise ValueError(f"Token IDs missing. Keys: {list(m.keys())}")
    return (
        ids[0], ids[1],
        m.get("question","BTC Up/Down 5m"),
        str(m.get("minimum_tick_size","0.01")),
        bool(m.get("neg_risk",False)),
    )

def current_ts_5m() -> int:
    return (int(time.time()) // 300) * 300

def make_slug(t): return f"btc-updown-5m-{t}"

# ── Rollover watcher ───────────────────────────────────────────────────────
async def rollover_watcher(session, roll_q, stop):
    last = current_ts_5m()
    while True:
        await asyncio.sleep(10)
        now = current_ts_5m()
        if now == last: continue
        last = now
        slug = make_slug(now)
        log("↺", C, f"Rollover → {Y}{slug}{X}")
        stop.set(); await asyncio.sleep(0.5); stop.clear()
        try:   await roll_q.put(await fetch_market(session, slug))
        except Exception as e: log("✗", R, f"Rollover: {e}")

# ── CLOB init ──────────────────────────────────────────────────────────────
def init_clob():
    if not HAS_CLOB:
        print(f"  {Y}⚠ py-clob-client not installed → DETECT-ONLY{X}")
        print(f"    {DIM}pip install py-clob-client{X}\n")
        return None
    pk  = os.getenv("PRIVATE_KEY")
    key = os.getenv("API_KEY")
    sec = os.getenv("API_SECRET")
    phr = os.getenv("API_PASSPHRASE")
    fnd = os.getenv("FUNDER_ADDRESS")
    if not all([pk,key,sec,phr,fnd]):
        missing = [n for n,v in {"PRIVATE_KEY":pk,"API_KEY":key,
            "API_SECRET":sec,"API_PASSPHRASE":phr,"FUNDER_ADDRESS":fnd}.items() if not v]
        log("⚠", Y, f"Missing env: {', '.join(missing)} → DETECT-ONLY")
        return None
    try:
        client = ClobClient(
            host=CLOB_URL, chain_id=CHAIN_ID, key=pk,
            creds=ApiCreds(api_key=key,api_secret=sec,api_passphrase=phr),
            signature_type=2, funder=fnd,
        )
        log("✓", G, "CLOB client ready  (GNOSIS_SAFE)")
        return client
    except Exception as e:
        log("✗", R, f"CLOB init: {e}"); return None

# ── Main ───────────────────────────────────────────────────────────────────
async def main():
    global _clob
    print(f"\n{C}{B}  BTC 5-min UpDown — Superfast Arb Bot{X}")
    print(f"  {DIM}parallel orders · partial fill · one-leg guard · heartbeat · rollover{X}\n")

    _clob = init_clob()
    mode  = f"{BG}{B}LIVE TRADING{X}" if _clob else f"{Y}DETECT ONLY{X}"
    log("⚡", C, f"Mode: {mode}  ${ORDER_SIZE}/leg  min_profit={ARB_MIN*100:.1f}%")

    async with aiohttp.ClientSession(headers={"User-Agent":"btc-arb/3.0"}) as session:
        slug = make_slug(current_ts_5m())
        log("→", DIM, f"Fetching: {slug}")
        up_id, dn_id, question, tick_size, neg_risk = await fetch_market(session, slug)
        log("📊", W, f"{B}{question}{X}")
        log(" ", DIM, f"UP  …{up_id[-20:]}  DN  …{dn_id[-20:]}")
        log(" ", DIM, f"tick={tick_size}  neg_risk={neg_risk}\n")

        prices = Prices()
        stop   = asyncio.Event()
        arb_q  = asyncio.Queue(maxsize=1)
        roll_q = asyncio.Queue()

        asyncio.create_task(rollover_watcher(session, roll_q, stop))
        arb_task = asyncio.create_task(arb_executor(arb_q, up_id, dn_id))
        if _clob: asyncio.create_task(heartbeat_loop())

        ws_task = asyncio.create_task(
            run_ws(up_id, dn_id, tick_size, neg_risk, prices, arb_q, stop)
        )

        while True:
            roll_task = asyncio.create_task(roll_q.get())
            done, _   = await asyncio.wait([ws_task, roll_task],
                                           return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:    result = task.result()
                except: result = None

                if isinstance(result, tuple) and len(result) == 5:
                    up_id, dn_id, question, tick_size, neg_risk = result
                    log("★", BY, f"New market: {W}{B}{question}{X}")
                    ws_task.cancel(); arb_task.cancel()
                    prices   = Prices()
                    arb_task = asyncio.create_task(arb_executor(arb_q, up_id, dn_id))
                    ws_task  = asyncio.create_task(
                        run_ws(up_id, dn_id, tick_size, neg_risk, prices, arb_q, stop)
                    )
                else:
                    if not roll_task.done(): roll_task.cancel()
                    log("↻", Y, "WS ended — restarting")
                    ws_task = asyncio.create_task(
                        run_ws(up_id, dn_id, tick_size, neg_risk, prices, arb_q, stop)
                    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n  {DIM}Stopped.{X}")
    finally:
        _executor.shutdown(wait=False)
