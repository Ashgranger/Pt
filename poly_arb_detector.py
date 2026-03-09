"""
╔══════════════════════════════════════════════════════════════╗
║  Polymarket BTC 5-Min ARB Detector  ·  ULTRA-FAST EDITION   ║
╠══════════════════════════════════════════════════════════════╣
║  Speed stack:                                                ║
║  • WebSocket orderbook stream  → no HTTP round-trips         ║
║  • orjson / ujson fallback     → 3-5× faster JSON parse      ║
║  • Pre-built URL strings       → zero fmt overhead           ║
║  • time.perf_counter_ns()      → microsecond arb timing      ║
║  • Single TCP connection pool  → no handshake latency        ║
║  • Parallel next-window prefetch → token IDs ready at t=0    ║
║  • HTTP /books batch endpoint  → 1 call instead of 2         ║
║  • Zero sleep in hot path      → immediate re-poll           ║
║  • uvloop support              → 2× faster event loop        ║
╚══════════════════════════════════════════════════════════════╝

Install for max speed:
  pip install aiohttp orjson uvloop

Arb condition:
  ask(UP) + ask(DOWN) < 1.0  →  buy both sides = risk-free profit
  spread = 1.0 - (ask_up + ask_down)   [positive = profit in cents/dollar]
"""

import asyncio
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

# ── Fast JSON: orjson > ujson > stdlib ───────────────────────────────────────
try:
    import orjson as _json
    def _loads(b): return _json.loads(b)
    def _dumps(d): return _json.dumps(d).decode()
    _JSON_LIB = "orjson ⚡⚡"
except ImportError:
    try:
        import ujson as _json
        def _loads(b): return _json.loads(b)
        def _dumps(d): return _json.dumps(d)
        _JSON_LIB = "ujson ⚡"
    except ImportError:
        import json as _json
        def _loads(b): return _json.loads(b)
        def _dumps(d): return _json.dumps(d)
        _JSON_LIB = "json (pip install orjson for 5x speed)"

import aiohttp

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

GAMMA_BASE       = "https://gamma-api.polymarket.com"
CLOB_BASE        = "https://clob.polymarket.com"
WS_MARKET        = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

MIN_ARB_SPREAD   = 0.002        # 0.2¢ — low threshold catches fleeting arbs
MARKET_INTERVAL  = 300          # 5-min window
PING_INTERVAL    = 9.0          # WS keepalive (server drops at 10s)
HTTP_FALLBACK_MS = 200          # if WS data older than this, fire HTTP backup
STAT_INTERVAL    = 30.0         # print stats every N seconds
PREFETCH_BEFORE  = 30           # seconds before window end to prefetch next IDs

# ─────────────────────────────────────────────────────────────────────────────
#  ANSI COLORS (pre-computed strings, no function call overhead in hot path)
# ─────────────────────────────────────────────────────────────────────────────

_R = "\033[91m"   # red
_G = "\033[92m"   # green
_Y = "\033[93m"   # yellow
_C = "\033[96m"   # cyan
_M = "\033[95m"   # magenta
_B = "\033[1m"    # bold
_Z = "\033[0m"    # reset

# Pre-built tick symbols (no string building in hot path)
_SYM_ARB  = f"{_G}●{_Z}"
_SYM_IDLE = f"{_Y}○{_Z}"
_ARB_TAG  = f" {_G}{_B}◄ ARB{_Z}"

# ─────────────────────────────────────────────────────────────────────────────
#  DATA STRUCTURES  (__slots__ eliminates dict overhead)
# ─────────────────────────────────────────────────────────────────────────────

class ArbEvent:
    __slots__ = ("start_ns", "end_ns", "max_spread", "min_spread",
                 "spread_sum", "samples", "avg_spread")

    def __init__(self, spread: float):
        self.start_ns   = time.perf_counter_ns()
        self.end_ns     = 0
        self.max_spread = spread
        self.min_spread = spread
        self.spread_sum = spread
        self.samples    = 1
        self.avg_spread = spread

    def add(self, spread: float):
        self.samples    += 1
        self.spread_sum += spread
        if spread > self.max_spread: self.max_spread = spread
        if spread < self.min_spread: self.min_spread = spread

    def close(self):
        self.end_ns     = time.perf_counter_ns()
        self.avg_spread = self.spread_sum / self.samples

    def duration_ns(self) -> int:
        return (self.end_ns or time.perf_counter_ns()) - self.start_ns

    def duration_s(self) -> float:
        return self.duration_ns() / 1_000_000_000


class Stats:
    __slots__ = ("events", "total_ns", "longest_ns", "shortest_ns",
                 "max_spread", "polls", "ws_ticks", "http_ticks",
                 "errors", "history")

    def __init__(self):
        self.events      = 0
        self.total_ns    = 0
        self.longest_ns  = 0
        self.shortest_ns = 10 ** 18
        self.max_spread  = 0.0
        self.polls       = 0
        self.ws_ticks    = 0
        self.http_ticks  = 0
        self.errors      = 0
        self.history: deque = deque(maxlen=200)

    def record(self, ev: ArbEvent):
        d = ev.duration_ns()
        self.events   += 1
        self.total_ns += d
        if d > self.longest_ns:  self.longest_ns  = d
        if d < self.shortest_ns: self.shortest_ns = d
        if ev.max_spread > self.max_spread: self.max_spread = ev.max_spread
        self.history.append(ev)

    def avg_ns(self) -> int:
        return self.total_ns // self.events if self.events else 0


class MarketState:
    """Flat state object — hot fields first for CPU cache locality."""
    __slots__ = ("up_ask", "down_ask", "last_ws_ns",
                 "up_token", "down_token", "slug",
                 "next_up", "next_down", "next_slug")

    def __init__(self):
        # Hot fields (read every tick)
        self.up_ask     = 0.0
        self.down_ask   = 0.0
        self.last_ws_ns = 0
        # Identity fields
        self.up_token   = ""
        self.down_token = ""
        self.slug       = ""
        # Prefetched next window
        self.next_up    = ""
        self.next_down  = ""
        self.next_slug  = ""

    def ready(self) -> bool:
        return bool(self.up_token and self.down_token)

    def rollover_to_next(self):
        self.up_token   = self.next_up
        self.down_token = self.next_down
        self.slug       = self.next_slug
        self.next_up    = self.next_down = self.next_slug = ""
        self.up_ask     = self.down_ask = 0.0
        self.last_ws_ns = 0


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _floor_ts() -> int:
    return (int(time.time()) // MARKET_INTERVAL) * MARKET_INTERVAL

def _slug(ts: int) -> str:
    return f"btc-updown-5m-{ts}"

def _secs_to_next() -> int:
    return MARKET_INTERVAL - (int(time.time()) % MARKET_INTERVAL)

def _ts_str() -> str:
    """HH:MM:SS.mmm  — millisecond precision for arb timing."""
    t  = time.time()
    dt = datetime.fromtimestamp(t, tz=timezone.utc)
    return f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}.{dt.microsecond//1000:03d}"

def _fmt_ns(ns: int) -> str:
    if ns < 1_000:         return f"{ns}ns"
    if ns < 1_000_000:     return f"{ns/1_000:.1f}µs"
    if ns < 1_000_000_000: return f"{ns/1_000_000:.2f}ms"
    return f"{ns/1_000_000_000:.3f}s"

# Pre-built timeout objects (avoid object creation per request)
_TO_FAST = aiohttp.ClientTimeout(total=2, connect=1)
_TO_SLOW = aiohttp.ClientTimeout(total=5, connect=2)


# ─────────────────────────────────────────────────────────────────────────────
#  NETWORK
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_token_ids(session: aiohttp.ClientSession,
                           slug: str) -> Optional[tuple]:
    url = f"{GAMMA_BASE}/markets/slug/{slug}"
    try:
        async with session.get(url, timeout=_TO_SLOW) as r:
            if r.status != 200:
                return None
            data = _loads(await r.read())
            if isinstance(data, list):
                if not data: return None
                data = data[0]
            toks = data.get("clobTokenIds") or []
            return (toks[0], toks[1]) if len(toks) >= 2 else None
    except Exception:
        return None


async def _fetch_books_http(session: aiohttp.ClientSession,
                             up_tok: str,
                             dn_tok: str) -> tuple:
    """
    Batch /books endpoint — ONE round-trip for both order books.
    Returns (best_ask_up, best_ask_dn)  or  (None, None).
    """
    try:
        async with session.post(
            f"{CLOB_BASE}/books",
            json=[{"token_id": up_tok}, {"token_id": dn_tok}],
            timeout=_TO_FAST,
        ) as r:
            if r.status != 200:
                return None, None
            data = _loads(await r.read())
            if not isinstance(data, list) or len(data) < 2:
                return None, None

            def _best_ask(book):
                asks = book.get("asks")
                return float(asks[0]["price"]) if asks else None

            return _best_ask(data[0]), _best_ask(data[1])
    except Exception:
        return None, None


# ─────────────────────────────────────────────────────────────────────────────
#  WEBSOCKET FEED  (primary price source)
# ─────────────────────────────────────────────────────────────────────────────

async def ws_feed(state: MarketState, stats: Stats):
    """
    Streams best_bid_ask / price_change / book events from Polymarket WS.
    Updates state.up_ask and state.down_ask with nanosecond timestamps.
    Auto-reconnects; re-subscribes when tokens change.
    """
    while True:
        if not state.ready():
            await asyncio.sleep(0.05)
            continue

        up_tok = state.up_token
        dn_tok = state.down_token

        sub = _dumps({
            "type": "market",
            "assets_ids": [up_tok, dn_tok],
            "custom_feature_enabled": True,   # enables best_bid_ask events
        })

        try:
            async with aiohttp.ClientSession() as ws_sess:
                async with ws_sess.ws_connect(
                    WS_MARKET,
                    heartbeat=PING_INTERVAL,
                    compress=False,      # no gzip = lower latency + CPU
                    max_msg_size=0,
                    autoclose=True,
                ) as ws:
                    await ws.send_str(sub)
                    print(f"{_C}  ⚡ WS subscribed  UP={up_tok[:10]}…  "
                          f"DN={dn_tok[:10]}…{_Z}")

                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            break

                        raw = msg.data
                        if raw == "PONG":
                            continue

                        try:
                            ev = _loads(raw)
                        except Exception:
                            continue

                        etype = ev.get("event_type")

                        # ── Fastest path: dedicated best_bid_ask event ────
                        if etype == "best_bid_ask":
                            aid = ev.get("asset_id", "")
                            ask_raw = ev.get("best_ask")
                            if ask_raw is None: continue
                            ask = float(ask_raw)
                            if   aid == up_tok: state.up_ask   = ask
                            elif aid == dn_tok: state.down_ask = ask
                            state.last_ws_ns = time.perf_counter_ns()
                            stats.ws_ticks  += 1

                        # ── Book snapshot (on subscribe) ──────────────────
                        elif etype == "book":
                            aid  = ev.get("asset_id", "")
                            asks = ev.get("asks")
                            if asks:
                                ask = float(asks[0]["price"])
                                if   aid == up_tok: state.up_ask   = ask
                                elif aid == dn_tok: state.down_ask = ask
                                state.last_ws_ns = time.perf_counter_ns()

                        # ── price_change carries best_ask inline ──────────
                        elif etype == "price_change":
                            for pc in ev.get("price_changes") or []:
                                aid     = pc.get("asset_id", "")
                                ask_raw = pc.get("best_ask")
                                if ask_raw is None: continue
                                ask = float(ask_raw)
                                if   aid == up_tok: state.up_ask   = ask
                                elif aid == dn_tok: state.down_ask = ask
                            state.last_ws_ns = time.perf_counter_ns()
                            stats.ws_ticks  += 1

                        # Tokens changed → reconnect with new subscription
                        if (state.up_token != up_tok or
                                state.down_token != dn_tok):
                            break

        except Exception as exc:
            print(f"{_R}  WS error ({type(exc).__name__}): {exc}{_Z}")

        await asyncio.sleep(0.3)   # brief pause before reconnect


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP FALLBACK  (fires when WS goes stale)
# ─────────────────────────────────────────────────────────────────────────────

async def http_backup(session: aiohttp.ClientSession,
                      state: MarketState, stats: Stats):
    """Polls /books every 200ms, but only writes if WS is stale."""
    stale_ns = int(HTTP_FALLBACK_MS * 1_000_000)

    while True:
        await asyncio.sleep(0.20)

        if not state.ready():
            continue

        if time.perf_counter_ns() - state.last_ws_ns < stale_ns:
            continue   # WS is fresh

        up_ask, dn_ask = await _fetch_books_http(
            session, state.up_token, state.down_token)

        if up_ask is not None and dn_ask is not None:
            state.up_ask     = up_ask
            state.down_ask   = dn_ask
            state.last_ws_ns = time.perf_counter_ns()
            stats.http_ticks += 1
        else:
            stats.errors += 1


# ─────────────────────────────────────────────────────────────────────────────
#  NEXT-WINDOW PREFETCHER  (zero cold-start at rollover)
# ─────────────────────────────────────────────────────────────────────────────

async def prefetcher(session: aiohttp.ClientSession, state: MarketState):
    """
    Fetches token IDs for the NEXT 5-min window 30s before it starts.
    At rollover, detector instantly has tokens — no gap in coverage.
    """
    while True:
        wait = max(0.1, _secs_to_next() - PREFETCH_BEFORE)
        await asyncio.sleep(wait)

        next_ts   = _floor_ts() + MARKET_INTERVAL
        next_slug = _slug(next_ts)

        if state.next_slug == next_slug:
            await asyncio.sleep(5)
            continue

        print(f"{_M}  ⟳  Prefetching: {next_slug}{_Z}")

        for _ in range(90):    # retry up to 90s
            ids = await _fetch_token_ids(session, next_slug)
            if ids:
                state.next_up, state.next_down = ids
                state.next_slug = next_slug
                print(f"{_M}  ✓  Next tokens ready: "
                      f"UP={state.next_up[:14]}… "
                      f"DN={state.next_down[:14]}…{_Z}")
                break
            await asyncio.sleep(1)
        else:
            print(f"{_R}  ✗  Prefetch failed for {next_slug}{_Z}")

        await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
#  TOKEN RESOLVER  (startup + window gap recovery)
# ─────────────────────────────────────────────────────────────────────────────

async def resolver(session: aiohttp.ClientSession, state: MarketState):
    while True:
        if state.ready():
            await asyncio.sleep(1.0)
            continue

        ts   = _floor_ts()
        slug = _slug(ts)
        state.slug = slug

        print(f"{_C}  ↻  Resolving {slug}…{_Z}", end="\r")
        ids = await _fetch_token_ids(session, slug)
        if ids:
            state.up_token, state.down_token = ids
            print(f"{_G}  ✓  UP ={state.up_token[:16]}…{_Z}")
            print(f"{_G}     DN ={state.down_token[:16]}…{_Z}")
        else:
            await asyncio.sleep(0.5)


# ─────────────────────────────────────────────────────────────────────────────
#  DISPLAY
# ─────────────────────────────────────────────────────────────────────────────

def _header():
    print(f"\n{_C}{_B}{'═'*65}{_Z}")
    print(f"{_B}   Polymarket BTC 5-Min  ·  ARB DETECTOR  ·  µs PRECISION{_Z}")
    print(f"{_C}{'═'*65}{_Z}")
    print(f"  JSON lib   : {_Y}{_JSON_LIB}{_Z}")
    print(f"  Min spread : {_Y}{MIN_ARB_SPREAD:.4f}{_Z}  ({MIN_ARB_SPREAD*100:.2f}¢ per $1)")
    print(f"  Feed       : {_Y}WebSocket (primary) + HTTP batch (fallback){_Z}")
    print(f"  Timing     : {_Y}perf_counter_ns  (nanosecond resolution){_Z}")
    print(f"{_C}{'─'*65}{_Z}\n")


def _arb_open(spread: float, up: float, dn: float):
    print(f"\n{_G}{_B}🔥 [{_ts_str()}] ── ARB OPENED "
          f"{'─'*38}{_Z}")
    print(f"{_G}   UP={up:.5f}  DN={dn:.5f}  "
          f"SUM={up+dn:.5f}  "
          f"SPREAD={_B}{spread:.5f}{_Z}{_G} ({spread*100:.3f}¢){_Z}")


def _arb_close(ev: ArbEvent, n: int):
    dns = ev.duration_ns()
    print(f"\n{_R}⏹  [{_ts_str()}] ── ARB CLOSED #{n} "
          f"{'─'*36}{_Z}")
    print(f"{_R}   Duration : {_B}{_fmt_ns(dns)}{_Z}{_R}  ({ev.duration_s():.4f}s){_Z}")
    print(f"{_R}   Max Δ    : {ev.max_spread:.5f} ({ev.max_spread*100:.3f}¢){_Z}")
    print(f"{_R}   Avg Δ    : {ev.avg_spread:.5f}  "
          f"Min Δ: {ev.min_spread:.5f}{_Z}")
    print(f"{_R}   Samples  : {ev.samples}{_Z}")


def _tick(spread: float, up: float, dn: float,
          is_arb: bool, ws_age_ms: float, src: str):
    sym   = _SYM_ARB if is_arb else _SYM_IDLE
    label = _ARB_TAG if is_arb else ""
    src_c = _G if src == "WS" else _Y
    # Single print call = one syscall = minimum latency
    print(f"  {sym} {_ts_str()}  "
          f"UP={up:.4f} DN={dn:.4f} "
          f"Σ={up+dn:.4f} "
          f"Δ={spread:+.4f}  "
          f"[{src_c}{src}{_Z} {ws_age_ms:.0f}ms]"
          f"{label}",
          flush=True)


def _stats(stats: Stats, cur: Optional[ArbEvent]):
    total_ticks = stats.ws_ticks + stats.http_ticks
    ws_pct = 100 * stats.ws_ticks // total_ticks if total_ticks else 0

    print(f"\n{_C}┌── STATS {'─'*55}{_Z}")
    print(f"│  Arb events   : {_Y}{stats.events}{_Z}")
    print(f"│  Avg duration : {_Y}{_fmt_ns(stats.avg_ns())}{_Z}")
    print(f"│  Longest      : {_Y}{_fmt_ns(stats.longest_ns)}{_Z}  |  "
          f"Shortest: {_Y}{'—' if not stats.events else _fmt_ns(stats.shortest_ns)}{_Z}")
    print(f"│  Max spread   : {_Y}{stats.max_spread:.5f}{_Z} "
          f"({stats.max_spread*100:.3f}¢)")
    print(f"│  WS/HTTP ticks: {_Y}{stats.ws_ticks}/{stats.http_ticks}{_Z} "
          f"({ws_pct}% WS)  |  Errors: {_R}{stats.errors}{_Z}")

    if cur:
        print(f"│  {_G}{_B}▶ LIVE ARB{_Z}  "
              f"running={_G}{_fmt_ns(cur.duration_ns())}{_Z}  "
              f"max={_G}{cur.max_spread:.5f}{_Z}  "
              f"samples={cur.samples}")
    print(f"{_C}└{'─'*64}{_Z}")

    if stats.history:
        h = list(stats.history)[-10:]
        print(f"\n  {'#':>3}  {'Duration':>12}  {'Max Δ':>8}  "
              f"{'Avg Δ':>8}  {'Min Δ':>8}  {'Smpl':>5}")
        print(f"  {'─'*3}  {'─'*12}  {'─'*8}  {'─'*8}  {'─'*8}  {'─'*5}")
        for i, ev in enumerate(h, max(1, stats.events - len(h) + 1)):
            print(f"  {i:>3}  {_fmt_ns(ev.duration_ns()):>12}  "
                  f"{ev.max_spread:>8.5f}  "
                  f"{ev.avg_spread:>8.5f}  "
                  f"{ev.min_spread:>8.5f}  "
                  f"{ev.samples:>5}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
#  DETECTION HOT LOOP  (runs as fast as state updates arrive)
# ─────────────────────────────────────────────────────────────────────────────

async def detector(state: MarketState, stats: Stats):
    cur: Optional[ArbEvent] = None
    last_stat  = time.monotonic()
    last_up    = 0.0
    last_dn    = 0.0
    stale_ns   = int(HTTP_FALLBACK_MS * 1_000_000)

    while True:
        # ── Wait for initial data ─────────────────────────────────────────
        if not state.ready() or state.last_ws_ns == 0:
            await asyncio.sleep(0.01)
            continue

        up = state.up_ask
        dn = state.down_ask

        # ── Skip if nothing changed (no new WS/HTTP data) ─────────────────
        if up == last_up and dn == last_dn:
            await asyncio.sleep(0.002)    # 2ms yield — tight re-check
            continue

        last_up = up
        last_dn = dn
        stats.polls += 1

        # ── Core arb math ─────────────────────────────────────────────────
        spread = 1.0 - up - dn           # positive = arb profit
        is_arb = spread >= MIN_ARB_SPREAD

        ws_age_ms = (time.perf_counter_ns() - state.last_ws_ns) / 1_000_000
        src = "WS" if ws_age_ms < HTTP_FALLBACK_MS else "HTTP"

        _tick(spread, up, dn, is_arb, ws_age_ms, src)

        # ── State machine ─────────────────────────────────────────────────
        if is_arb:
            if cur is None:
                cur = ArbEvent(spread)
                _arb_open(spread, up, dn)
            else:
                cur.add(spread)
        else:
            if cur is not None:
                cur.close()
                _arb_close(cur, stats.events + 1)
                stats.record(cur)
                cur = None

        # ── Periodic stats ────────────────────────────────────────────────
        now = time.monotonic()
        if now - last_stat >= STAT_INTERVAL:
            _stats(stats, cur)
            last_stat = now

        # ── Window rollover detection ─────────────────────────────────────
        new_ts   = _floor_ts()
        new_slug = _slug(new_ts)
        if new_slug != state.slug:
            if cur is not None:
                cur.close()
                _arb_close(cur, stats.events + 1)
                stats.record(cur)
                cur = None

            print(f"\n{_C}  ⟳  Window rollover → {new_slug}{_Z}")

            if state.next_slug == new_slug:
                state.rollover_to_next()
                print(f"{_G}  ⚡ Prefetched tokens active — zero cold-start!{_Z}")
            else:
                state.up_token   = ""
                state.down_token = ""
                state.slug       = new_slug
                state.up_ask     = 0.0
                state.down_ask   = 0.0
                print(f"{_Y}  ⚠  No prefetch — fetching tokens now…{_Z}")

        # yield to event loop without sleeping (max throughput)
        await asyncio.sleep(0)


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    _header()

    state = MarketState()
    stats = Stats()

    connector = aiohttp.TCPConnector(
        limit=20,
        limit_per_host=10,
        ttl_dns_cache=600,
        use_dns_cache=True,
        keepalive_timeout=60,
        enable_cleanup_closed=True,
    )
    # disable gzip for HTTP calls — saves CPU decompression time
    headers = {"Accept-Encoding": "identity"}

    async with aiohttp.ClientSession(
            connector=connector, headers=headers) as session:

        print(f"{_C}  Spawning tasks…{_Z}\n")

        await asyncio.gather(
            resolver(session, state),            # initial token resolution
            ws_feed(state, stats),               # WebSocket price stream
            http_backup(session, state, stats),  # HTTP fallback poller
            prefetcher(session, state),          # next-window prefetch
            detector(state, stats),              # arb detection hot loop
        )


if __name__ == "__main__":
    try:
        # uvloop: C-based event loop, ~2x faster than asyncio default
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            print(f"{_C}  uvloop active ⚡⚡{_Z}")
        except ImportError:
            print(f"{_Y}  tip: pip install uvloop  for 2x faster event loop{_Z}")

        asyncio.run(main())

    except KeyboardInterrupt:
        print(f"\n{_Y}  Stopped.{_Z}\n")
        sys.exit(0)
