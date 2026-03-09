#!/usr/bin/env python3
"""
BTC 5-min UpDown — Ultra-Fast Arb Detector
============================================
• Detects arb windows in µs via WebSocket
• Measures EXACT duration each arb window stays open
• Tracks min/max/avg duration, profit, frequency stats
• 1% minimum threshold
• Zero order placement — pure detection + timing

pip install aiohttp websockets
python arb_detector.py
"""

import asyncio, json, time, os, sys, collections
from datetime import datetime, timezone
from typing import Optional

import aiohttp, websockets

# ── Endpoints ──────────────────────────────────────────────────────────────
GAMMA  = "https://gamma-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Config ─────────────────────────────────────────────────────────────────
ARB_MIN   = 0.01    # 1% minimum profit to register
PING_S    = 9       # WS keepalive interval

# ── ANSI ───────────────────────────────────────────────────────────────────
R="\033[31m";  G="\033[32m";  Y="\033[33m";  C="\033[36m"
W="\033[97m";  DIM="\033[2m"; BG="\033[92m"; BY="\033[93m"
M="\033[35m";  CY="\033[96m"; B="\033[1m";   X="\033[0m"
LB="\033[94m"  # light blue

def now_str() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]

def log(sym, col, msg):
    print(f"  {col}{sym}{X} [{DIM}{now_str()}{X}] {msg}")

# ── Price state ────────────────────────────────────────────────────────────
class Tok:
    __slots__ = ("bid", "ask", "t")
    def __init__(self): self.bid=0.0; self.ask=0.0; self.t=0.0
    def set(self, bid, ask):
        self.bid=bid; self.ask=ask; self.t=time.monotonic()

class Prices:
    __slots__ = ("up","dn")
    def __init__(self): self.up=Tok(); self.dn=Tok()

# ── Arb window record ──────────────────────────────────────────────────────
class ArbWindow:
    __slots__ = ("t_open","t_close","profit_open","profit_peak",
                 "ua_open","da_open","msg_count","closed")
    def __init__(self, t, profit, ua, da):
        self.t_open       = t
        self.t_close      = 0.0
        self.profit_open  = profit
        self.profit_peak  = profit
        self.ua_open      = ua
        self.da_open      = da
        self.msg_count    = 1        # WS messages while arb was live
        self.closed       = False

    @property
    def duration_ms(self) -> float:
        end = self.t_close if self.closed else time.monotonic()
        return (end - self.t_open) * 1000

    def tick(self, profit):
        self.msg_count += 1
        if profit > self.profit_peak:
            self.profit_peak = profit

    def close(self, t):
        self.t_close = t
        self.closed  = True

# ── Statistics tracker ─────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.total_windows   = 0
        self.total_messages  = 0      # all WS messages parsed
        self.arb_messages    = 0      # messages where arb was active
        self.durations_ms    = []     # closed window durations
        self.profits_pct     = []     # peak profits of closed windows
        self.session_start   = time.monotonic()
        self.last_print      = 0.0
        # Rolling last-60 windows
        self.recent          = collections.deque(maxlen=60)

    def record(self, w: ArbWindow):
        self.total_windows += 1
        d = w.duration_ms
        self.durations_ms.append(d)
        self.profits_pct.append(w.profit_peak * 100)
        self.recent.append(w)

    def summary(self) -> str:
        if not self.durations_ms:
            return f"{DIM}no closed windows yet{X}"
        d  = self.durations_ms
        p  = self.profits_pct
        up = time.monotonic() - self.session_start
        rate = self.total_windows / (up / 60) if up > 0 else 0
        return (
            f"{B}Windows:{X} {BY}{self.total_windows}{X}  "
            f"{B}Rate:{X} {CY}{rate:.1f}/min{X}  "
            f"{B}Duration:{X} "
            f"min={G}{min(d):.0f}ms{X} "
            f"avg={Y}{sum(d)/len(d):.0f}ms{X} "
            f"max={R}{max(d):.0f}ms{X}  "
            f"{B}Profit:{X} "
            f"min={G}{min(p):.2f}%{X} "
            f"avg={Y}{sum(p)/len(p):.2f}%{X} "
            f"max={BG}{max(p):.2f}%{X}"
        )

    def histogram(self) -> str:
        if len(self.durations_ms) < 2:
            return ""
        buckets = [0]*8
        labels  = ["<10ms","10-50","50-100","100-500","500ms-1s","1-5s","5-30s",">30s"]
        for d in self.durations_ms:
            if   d <    10: buckets[0] += 1
            elif d <    50: buckets[1] += 1
            elif d <   100: buckets[2] += 1
            elif d <   500: buckets[3] += 1
            elif d <  1000: buckets[4] += 1
            elif d <  5000: buckets[5] += 1
            elif d < 30000: buckets[6] += 1
            else:           buckets[7] += 1
        total = sum(buckets) or 1
        lines = [f"\n  {B}Duration histogram:{X}"]
        bar_w = 30
        colors = [BG, G, G, Y, Y, R, R, R]
        for i, (label, cnt) in enumerate(zip(labels, buckets)):
            pct  = cnt / total
            bars = int(pct * bar_w)
            col  = colors[i]
            lines.append(
                f"  {label:>10}  {col}{'█'*bars}{DIM}{'░'*(bar_w-bars)}{X}"
                f"  {col}{cnt:3d}{X}  {DIM}({pct*100:.0f}%){X}"
            )
        return "\n".join(lines)

# ── Global state ───────────────────────────────────────────────────────────
_stats       = Stats()
_current_arb: Optional[ArbWindow] = None   # currently open arb window
_msg_count   = 0

# ── Arb check ──────────────────────────────────────────────────────────────
def arb_check(p: Prices) -> Optional[tuple]:
    ua, da = p.up.ask, p.dn.ask
    if ua == 0.0 or da == 0.0: return None
    profit = 1.0 - ua - da
    return (ua, da, profit) if profit > ARB_MIN else None

# ── Message handler (µs-fast, no await) ────────────────────────────────────
def handle_msg(raw: str, up_id: str, dn_id: str, p: Prices):
    global _current_arb, _msg_count

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

    _msg_count += 1
    _stats.total_messages += 1
    elapsed_us = int((time.perf_counter() - t0) * 1_000_000)
    t_now      = time.monotonic()
    result     = arb_check(p)
    ua, da     = p.up.ask, p.dn.ask

    # ── Arb state machine ──────────────────────────────────────────────────
    if result:
        ua, da, profit = result
        _stats.arb_messages += 1

        if _current_arb is None:
            # ── ARB OPENED ─────────────────────────────────────────────────
            _current_arb = ArbWindow(t_now, profit, ua, da)
            print(f"\n  {'▓'*64}")
            log("▶", BY,
                f"{B}ARB OPEN{X}  "
                f"UP={W}{B}{ua:.4f}{X} DN={W}{B}{da:.4f}{X}  "
                f"profit={BG}{B}+{profit*100:.3f}%{X}  "
                f"{DIM}{elapsed_us}µs{X}")
        else:
            # ── ARB CONTINUING ─────────────────────────────────────────────
            _current_arb.tick(profit)
            dur_ms = _current_arb.duration_ms
            print(
                f"\r  {BY}◆{X} [{DIM}{now_str()}{X}] "
                f"LIVE {BY}{dur_ms:7.1f}ms{X}  "
                f"UP={W}{ua:.4f}{X} DN={W}{da:.4f}{X}  "
                f"profit={BG}{B}+{profit*100:.3f}%{X}  "
                f"peak={BG}{_current_arb.profit_peak*100:.3f}%{X}  "
                f"{DIM}{elapsed_us}µs{X}   ",
                end="", flush=True,
            )

    else:
        if _current_arb is not None:
            # ── ARB CLOSED ─────────────────────────────────────────────────
            _current_arb.close(t_now)
            w   = _current_arb
            dur = w.duration_ms
            _stats.record(w)
            _current_arb = None

            # Duration colour
            if   dur <   50: dcol = BG
            elif dur <  500: dcol = G
            elif dur < 2000: dcol = Y
            elif dur < 5000: dcol = M
            else:            dcol = R

            print()  # end the \r live line
            log("■", dcol,
                f"{B}ARB CLOSED{X}  "
                f"duration={dcol}{B}{dur:.1f}ms{X}  "
                f"peak={BG}{w.profit_peak*100:.3f}%{X}  "
                f"msgs={DIM}{w.msg_count}{X}")
            log(" ", DIM, _stats.summary())
            print(f"  {'▓'*64}\n")

        else:
            # ── NO ARB — rolling ticker ────────────────────────────────────
            if ua > 0 and da > 0:
                s    = ua + da
                gap  = s - 1.0
                gcol = G if gap < 0 else (Y if gap < 0.05 else R)
                n    = _stats.total_windows
                print(
                    f"\r  [{DIM}{et[:3]}{X}] "
                    f"UP {W}{p.up.bid:.3f}{X}/{Y}{ua:.3f}{X}  "
                    f"DN {W}{p.dn.bid:.3f}{X}/{Y}{da:.3f}{X}  "
                    f"Σ={gcol}{s:.4f}{X}  gap={gcol}{gap:+.4f}{X}  "
                    f"wins={CY}{n}{X}  {DIM}{elapsed_us}µs{X}   ",
                    end="", flush=True,
                )

# ── Periodic stats printer ─────────────────────────────────────────────────
async def stats_loop():
    while True:
        await asyncio.sleep(60)
        print(f"\n\n  {'═'*64}")
        log("📊", CY, f"{B}60-second stats{X}")
        log(" ", W, _stats.summary())
        hist = _stats.histogram()
        if hist: print(hist)
        print(f"  {'═'*64}\n")

# ── Gamma fetch ────────────────────────────────────────────────────────────
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
        raise ValueError(f"Token IDs missing: {list(m.keys())}")
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

# ── WebSocket loop ─────────────────────────────────────────────────────────
async def run_ws(up_id, dn_id, prices, stop):
    attempt = 0
    while not stop.is_set():
        attempt += 1
        if attempt > 1:
            log("↻", Y, f"Reconnect #{attempt}")
            await asyncio.sleep(min(0.3 * attempt, 5))
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None, ping_timeout=None,
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
                        handle_msg(raw, up_id, dn_id, prices)
                finally:
                    ping_task.cancel()

        except websockets.exceptions.ConnectionClosed as e:
            log("!", Y, f"WS closed {e.code}")
        except Exception as e:
            log("✗", R, f"WS error: {e}")

# ── Main ───────────────────────────────────────────────────────────────────
async def main():
    print(f"\n{CY}{B}  BTC 5-min UpDown — Ultra-Fast Arb Detector{X}")
    print(f"  {DIM}measures exact arb window duration | min profit={ARB_MIN*100:.0f}%{X}")
    print(f"  {DIM}no orders placed — pure timing + stats{X}\n")

    async with aiohttp.ClientSession(headers={"User-Agent":"arb-detector/1.0"}) as session:
        slug = make_slug(current_ts_5m())
        log("→", DIM, f"Fetching: {slug}")

        up_id, dn_id, question, tick_size, neg_risk = await fetch_market(session, slug)
        log("📊", W, f"{B}{question}{X}")
        log(" ", DIM, f"UP  …{up_id[-20:]}   DN  …{dn_id[-20:]}")
        log(" ", DIM, f"tick={tick_size}  neg_risk={neg_risk}  threshold={ARB_MIN*100:.0f}%\n")

        prices = Prices()
        stop   = asyncio.Event()
        roll_q = asyncio.Queue()

        asyncio.create_task(rollover_watcher(session, roll_q, stop))
        asyncio.create_task(stats_loop())

        ws_task = asyncio.create_task(run_ws(up_id, dn_id, prices, stop))

        while True:
            roll_task = asyncio.create_task(roll_q.get())
            done, _   = await asyncio.wait(
                [ws_task, roll_task], return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:    result = task.result()
                except: result = None
                if isinstance(result, tuple) and len(result) == 5:
                    up_id, dn_id, question, tick_size, neg_risk = result
                    log("★", BY, f"New market: {W}{B}{question}{X}")
                    ws_task.cancel()
                    prices  = Prices()
                    ws_task = asyncio.create_task(
                        run_ws(up_id, dn_id, prices, stop)
                    )
                else:
                    if not roll_task.done(): roll_task.cancel()
                    ws_task = asyncio.create_task(
                        run_ws(up_id, dn_id, prices, stop)
                    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n\n  {B}Final stats:{X}")
        print(f"  {_stats.summary()}")
        hist = _stats.histogram()
        if hist: print(hist)
        print(f"\n  {DIM}Stopped.{X}\n")
