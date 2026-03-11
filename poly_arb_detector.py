"""
╔══════════════════════════════════════════════════════════════════╗
║   Polymarket  ALL-MARKET  ARB DETECTOR  ·  µs PRECISION         ║
╠══════════════════════════════════════════════════════════════════╣
║  • Scans EVERY active binary market on Polymarket                ║
║  • Single WebSocket subscription for ALL token IDs at once       ║
║  • Paginated Gamma API crawl → discovers every market            ║
║  • Batch /books HTTP fallback for stale tokens                   ║
║  • Arb = ask(YES) + ask(NO) < 1.0  per market                   ║
║  • Tracks duration in µs, history, top-arb leaderboard          ║
║  • Auto-refreshes market list every 5 min                        ║
║  • orjson / ujson fast JSON, uvloop event loop                   ║
╚══════════════════════════════════════════════════════════════════╝

Install for max speed:
    pip install aiohttp orjson uvloop
"""

import asyncio
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# ── Fast JSON ─────────────────────────────────────────────────────────────────
try:
    import orjson as _json
    _loads = _json.loads
    def _dumps(d): return _json.dumps(d).decode()
    _JSON_LIB = "orjson ⚡⚡"
except ImportError:
    try:
        import ujson as _json
        _loads = _json.loads
        _dumps = _json.dumps
        _JSON_LIB = "ujson ⚡"
    except ImportError:
        import json as _json
        _loads = _json.loads
        _dumps = _json.dumps
        _JSON_LIB = "json  (pip install orjson for 5× speed)"

import aiohttp

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

GAMMA_BASE        = "https://gamma-api.polymarket.com"
CLOB_BASE         = "https://clob.polymarket.com"
WS_URL            = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

MIN_ARB_SPREAD    = 0.002      # 0.2¢  — minimum spread to flag as arb
MARKET_PAGE_SIZE  = 500        # max per Gamma API page
REFRESH_MARKETS_S = 300        # re-crawl all markets every 5 min
WS_PING_S         = 9.0        # keepalive (server drops at 10 s)
HTTP_FALLBACK_MS  = 500        # HTTP backup if WS stale > 500 ms
HTTP_BATCH_SIZE   = 100        # tokens per /books batch call
STAT_INTERVAL_S   = 30.0       # print stats every N seconds
MAX_WS_TOKENS     = 2000       # WS can handle ~2 k tokens per connection
TOP_ARB_COUNT     = 10         # leaderboard size in stats

# ─────────────────────────────────────────────────────────────────────────────
#  ANSI
# ─────────────────────────────────────────────────────────────────────────────

_R = "\033[91m"; _G = "\033[92m"; _Y = "\033[93m"
_C = "\033[96m"; _M = "\033[95m"; _B = "\033[1m";  _Z = "\033[0m"

def _ts() -> str:
    t  = time.time()
    dt = datetime.fromtimestamp(t, tz=timezone.utc)
    return f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}.{dt.microsecond//1000:03d}"

def _fns(ns: int) -> str:
    if ns < 1_000:          return f"{ns}ns"
    if ns < 1_000_000:      return f"{ns/1e3:.1f}µs"
    if ns < 1_000_000_000:  return f"{ns/1e6:.2f}ms"
    return f"{ns/1e9:.3f}s"

# ─────────────────────────────────────────────────────────────────────────────
#  DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

class Market:
    """One binary market: YES token + NO token."""
    __slots__ = ("condition_id", "question", "slug",
                 "yes_tok", "no_tok",
                 "yes_ask", "no_ask",
                 "last_update_ns", "neg_risk")

    def __init__(self, condition_id: str, question: str, slug: str,
                 yes_tok: str, no_tok: str, neg_risk: bool = False):
        self.condition_id    = condition_id
        self.question        = question[:70]
        self.slug            = slug
        self.yes_tok         = yes_tok
        self.no_tok          = no_tok
        self.yes_ask         = 0.0
        self.no_ask          = 0.0
        self.last_update_ns  = 0
        self.neg_risk        = neg_risk

    def spread(self) -> float:
        return 1.0 - self.yes_ask - self.no_ask

    def ready(self) -> bool:
        return self.yes_ask > 0.0 and self.no_ask > 0.0


class ArbEvent:
    __slots__ = ("condition_id", "question", "start_ns", "end_ns",
                 "max_spread", "min_spread", "spread_sum", "samples", "avg_spread")

    def __init__(self, cid: str, question: str, spread: float):
        self.condition_id = cid
        self.question     = question[:50]
        self.start_ns     = time.perf_counter_ns()
        self.end_ns       = 0
        self.max_spread   = spread
        self.min_spread   = spread
        self.spread_sum   = spread
        self.samples      = 1
        self.avg_spread   = spread

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


class GlobalStats:
    __slots__ = ("total_events", "total_ns", "longest_ns", "shortest_ns",
                 "max_spread", "ws_ticks", "http_ticks", "errors",
                 "markets_tracked", "history")

    def __init__(self):
        self.total_events   = 0
        self.total_ns       = 0
        self.longest_ns     = 0
        self.shortest_ns    = 10**18
        self.max_spread     = 0.0
        self.ws_ticks       = 0
        self.http_ticks     = 0
        self.errors         = 0
        self.markets_tracked= 0
        self.history: deque = deque(maxlen=500)

    def record(self, ev: ArbEvent):
        d = ev.duration_ns()
        self.total_events += 1
        self.total_ns     += d
        if d > self.longest_ns:  self.longest_ns  = d
        if d < self.shortest_ns: self.shortest_ns = d
        if ev.max_spread > self.max_spread: self.max_spread = ev.max_spread
        self.history.append(ev)

    def avg_ns(self) -> int:
        return self.total_ns // self.total_events if self.total_events else 0


# ─────────────────────────────────────────────────────────────────────────────
#  SHARED STATE
# ─────────────────────────────────────────────────────────────────────────────

class State:
    def __init__(self):
        # condition_id → Market
        self.markets: Dict[str, Market] = {}
        # token_id → condition_id  (fast reverse lookup)
        self.tok_to_cid: Dict[str, str] = {}
        # condition_id → ArbEvent  (currently open arbs)
        self.open_arbs: Dict[str, ArbEvent] = {}
        # set of all token IDs currently subscribed on WS
        self.subscribed_tokens: set = set()
        # signals ws_manager to resubscribe
        self.ws_dirty = asyncio.Event()
        self.markets_lock = asyncio.Lock()

    def add_market(self, m: Market):
        self.markets[m.condition_id] = m
        self.tok_to_cid[m.yes_tok]   = m.condition_id
        self.tok_to_cid[m.no_tok]    = m.condition_id

    def remove_market(self, cid: str):
        m = self.markets.pop(cid, None)
        if m:
            self.tok_to_cid.pop(m.yes_tok, None)
            self.tok_to_cid.pop(m.no_tok, None)

    def all_tokens(self) -> List[str]:
        return list(self.tok_to_cid.keys())

    def update_ask(self, token_id: str, ask: float):
        cid = self.tok_to_cid.get(token_id)
        if cid is None:
            return
        m = self.markets.get(cid)
        if m is None:
            return
        if token_id == m.yes_tok:
            m.yes_ask = ask
        else:
            m.no_ask  = ask
        m.last_update_ns = time.perf_counter_ns()


# ─────────────────────────────────────────────────────────────────────────────
#  GAMMA API — CRAWL ALL ACTIVE MARKETS
# ─────────────────────────────────────────────────────────────────────────────

_TO_SLOW = aiohttp.ClientTimeout(total=10, connect=3)
_TO_FAST = aiohttp.ClientTimeout(total=3,  connect=1)

async def crawl_all_markets(session: aiohttp.ClientSession) -> List[Market]:
    """
    Paginate gamma-api /markets to get every active binary market.
    Returns list of Market objects.
    """
    markets: List[Market] = []
    offset = 0

    while True:
        url = (f"{GAMMA_BASE}/markets"
               f"?active=true&closed=false"
               f"&limit={MARKET_PAGE_SIZE}&offset={offset}"
               f"&order=volume_24hr&ascending=false")
        try:
            async with session.get(url, timeout=_TO_SLOW) as r:
                if r.status != 200:
                    print(f"{_R}  Gamma API {r.status} at offset {offset}{_Z}")
                    break
                data = _loads(await r.read())
        except Exception as e:
            print(f"{_R}  Gamma crawl error: {e}{_Z}")
            break

        if not isinstance(data, list):
            break

        for m_raw in data:
            # Only markets with an active order book
            if not m_raw.get("enableOrderBook", False):
                continue

            toks = m_raw.get("clobTokenIds") or []
            if len(toks) < 2:
                continue

            cid      = m_raw.get("conditionId") or m_raw.get("id", "")
            question = m_raw.get("question", m_raw.get("title", ""))
            slug     = m_raw.get("slug", "")
            neg_risk = bool(m_raw.get("negRisk", False))

            if not cid:
                continue

            markets.append(Market(
                condition_id = cid,
                question     = question,
                slug         = slug,
                yes_tok      = toks[0],
                no_tok       = toks[1],
                neg_risk     = neg_risk,
            ))

        if len(data) < MARKET_PAGE_SIZE:
            break   # last page
        offset += MARKET_PAGE_SIZE

    return markets


# ─────────────────────────────────────────────────────────────────────────────
#  MARKET REFRESHER  (periodic re-crawl)
# ─────────────────────────────────────────────────────────────────────────────

async def market_refresher(session: aiohttp.ClientSession,
                           state: State, gstats: GlobalStats):
    first = True
    while True:
        print(f"{_C}  ↻  Crawling all active markets…{_Z}")
        t0 = time.monotonic()
        fresh = await crawl_all_markets(session)
        elapsed = time.monotonic() - t0

        async with state.markets_lock:
            old_cids = set(state.markets.keys())
            new_cids = {m.condition_id for m in fresh}

            added   = new_cids - old_cids
            removed = old_cids - new_cids

            for m in fresh:
                state.add_market(m)
            for cid in removed:
                state.remove_market(cid)

            gstats.markets_tracked = len(state.markets)

        print(f"{_G}  ✓  Markets: {len(fresh)} total  "
              f"+{len(added)} added  -{len(removed)} removed  "
              f"({elapsed:.1f}s){_Z}")

        if added or first:
            state.ws_dirty.set()   # signal WS to resubscribe
            first = False

        await asyncio.sleep(REFRESH_MARKETS_S)


# ─────────────────────────────────────────────────────────────────────────────
#  WEBSOCKET MANAGER  (subscribes all tokens in one connection)
# ─────────────────────────────────────────────────────────────────────────────

async def ws_manager(state: State, gstats: GlobalStats):
    """
    Maintains ONE WebSocket connection subscribed to ALL token IDs.
    When state.ws_dirty is set (new markets), dynamically adds tokens
    without reconnecting using the 'subscribe' operation.
    """

    def _extract_ask(book_data: dict) -> Optional[float]:
        asks = book_data.get("asks")
        return float(asks[0]["price"]) if asks else None

    while True:
        # Wait until we have at least some markets
        if not state.markets:
            await asyncio.sleep(0.5)
            continue

        all_toks = state.all_tokens()
        if not all_toks:
            await asyncio.sleep(0.5)
            continue

        # Take first MAX_WS_TOKENS (WS limit per connection)
        sub_tokens = all_toks[:MAX_WS_TOKENS]

        initial_sub = _dumps({
            "type": "market",
            "assets_ids": sub_tokens,
            "custom_feature_enabled": True,
        })

        try:
            async with aiohttp.ClientSession() as ws_sess:
                async with ws_sess.ws_connect(
                    WS_URL,
                    heartbeat=WS_PING_S,
                    compress=False,
                    max_msg_size=0,
                    autoclose=True,
                ) as ws:

                    await ws.send_str(initial_sub)
                    state.subscribed_tokens = set(sub_tokens)
                    state.ws_dirty.clear()

                    print(f"{_G}  ⚡ WS connected — subscribed to "
                          f"{len(sub_tokens)} tokens{_Z}")

                    async def _handle_dirty():
                        """Dynamically subscribe new tokens when markets refresh."""
                        while True:
                            await state.ws_dirty.wait()
                            state.ws_dirty.clear()
                            new_all  = set(state.all_tokens())
                            new_toks = list(new_all - state.subscribed_tokens)
                            if new_toks:
                                chunks = [new_toks[i:i+500]
                                          for i in range(0, len(new_toks), 500)]
                                for chunk in chunks:
                                    await ws.send_str(_dumps({
                                        "assets_ids": chunk,
                                        "operation": "subscribe",
                                        "custom_feature_enabled": True,
                                    }))
                                    state.subscribed_tokens.update(chunk)
                                print(f"{_C}  ↳ Subscribed {len(new_toks)} "
                                      f"new tokens (total "
                                      f"{len(state.subscribed_tokens)}){_Z}")

                    dirty_task = asyncio.create_task(_handle_dirty())

                    try:
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

                            # ── best_bid_ask: fastest path ────────────────
                            if etype == "best_bid_ask":
                                ask_raw = ev.get("best_ask")
                                if ask_raw is None: continue
                                state.update_ask(
                                    ev.get("asset_id", ""), float(ask_raw))
                                gstats.ws_ticks += 1

                            # ── book snapshot (on subscribe) ──────────────
                            elif etype == "book":
                                asks = ev.get("asks")
                                if asks:
                                    state.update_ask(
                                        ev.get("asset_id", ""),
                                        float(asks[0]["price"]))

                            # ── price_change carries best_ask ─────────────
                            elif etype == "price_change":
                                for pc in ev.get("price_changes") or []:
                                    ask_raw = pc.get("best_ask")
                                    if ask_raw is None: continue
                                    state.update_ask(
                                        pc.get("asset_id", ""),
                                        float(ask_raw))
                                gstats.ws_ticks += 1

                            # ── market resolved → remove it ───────────────
                            elif etype == "market_resolved":
                                win_tok = ev.get("winning_asset_id", "")
                                cid = state.tok_to_cid.get(win_tok, "")
                                if cid:
                                    async with state.markets_lock:
                                        state.remove_market(cid)
                                    print(f"{_M}  ✓ Market resolved: "
                                          f"{ev.get('winning_outcome','?')}{_Z}")

                    finally:
                        dirty_task.cancel()

        except Exception as exc:
            print(f"{_R}  WS error ({type(exc).__name__}): {exc}{_Z}")

        await asyncio.sleep(1.0)


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP FALLBACK POLLER  (batch /books for stale tokens)
# ─────────────────────────────────────────────────────────────────────────────

async def http_fallback(session: aiohttp.ClientSession,
                        state: State, gstats: GlobalStats):
    stale_ns = int(HTTP_FALLBACK_MS * 1_000_000)

    while True:
        await asyncio.sleep(0.5)

        now_ns = time.perf_counter_ns()

        # Collect stale markets (both tokens not updated recently)
        stale_toks: List[str] = []
        tok_pairs: List[Tuple[str, str, str]] = []  # (yes_tok, no_tok, cid)

        for cid, m in list(state.markets.items()):
            if now_ns - m.last_update_ns > stale_ns:
                stale_toks.extend([m.yes_tok, m.no_tok])
                tok_pairs.append((m.yes_tok, m.no_tok, cid))

        if not stale_toks:
            continue

        # Batch call /books  (up to HTTP_BATCH_SIZE tokens per request)
        # Build flat list of {"token_id": ...} objects
        bodies = [{"token_id": t} for t in stale_toks[:HTTP_BATCH_SIZE * 2]]

        try:
            async with session.post(
                f"{CLOB_BASE}/books",
                json=bodies,
                timeout=_TO_FAST,
            ) as r:
                if r.status != 200:
                    gstats.errors += 1
                    continue
                data = _loads(await r.read())
        except Exception:
            gstats.errors += 1
            continue

        if not isinstance(data, list):
            continue

        # Map results back: data[i] corresponds to bodies[i]
        for i, book in enumerate(data):
            if i >= len(bodies):
                break
            tok_id = bodies[i]["token_id"]
            asks   = book.get("asks")
            if asks:
                state.update_ask(tok_id, float(asks[0]["price"]))

        gstats.http_ticks += 1


# ─────────────────────────────────────────────────────────────────────────────
#  DISPLAY
# ─────────────────────────────────────────────────────────────────────────────

def _print_header():
    print(f"\n{_C}{_B}{'═'*70}{_Z}")
    print(f"{_B}   Polymarket  ALL-MARKET  ARB DETECTOR  ·  µs PRECISION{_Z}")
    print(f"{_C}{'═'*70}{_Z}")
    print(f"  JSON lib   : {_Y}{_JSON_LIB}{_Z}")
    print(f"  Min spread : {_Y}{MIN_ARB_SPREAD:.4f}{_Z} ({MIN_ARB_SPREAD*100:.2f}¢ per $1)")
    print(f"  WS tokens  : {_Y}up to {MAX_WS_TOKENS} per connection{_Z}")
    print(f"  Refresh    : {_Y}every {REFRESH_MARKETS_S}s{_Z}")
    print(f"{_C}{'─'*70}{_Z}\n")


def _print_arb_open(ev: ArbEvent, yes: float, no: float):
    print(f"\n{_G}{_B}🔥 [{_ts()}] ARB OPENED {'─'*38}{_Z}")
    print(f"{_G}   {ev.question}{_Z}")
    print(f"{_G}   YES={yes:.5f}  NO={no:.5f}  "
          f"SUM={yes+no:.5f}  "
          f"SPREAD={_B}{ev.max_spread:.5f}{_Z}{_G} ({ev.max_spread*100:.3f}¢){_Z}")


def _print_arb_close(ev: ArbEvent, n: int):
    print(f"\n{_R}⏹  [{_ts()}] ARB CLOSED #{n} {'─'*37}{_Z}")
    print(f"{_R}   {ev.question}{_Z}")
    print(f"{_R}   Duration : {_B}{_fns(ev.duration_ns())}{_Z}{_R} "
          f"({ev.duration_s():.4f}s){_Z}")
    print(f"{_R}   Spread   : max={ev.max_spread:.5f}  "
          f"avg={ev.avg_spread:.5f}  "
          f"min={ev.min_spread:.5f}{_Z}")
    print(f"{_R}   Samples  : {ev.samples}{_Z}")


def _print_arb_tick(m: Market, spread: float):
    """Compact one-line update for ongoing arb."""
    print(f"  {_G}●{_Z} {_ts()}  "
          f"{m.question[:45]:<45}  "
          f"Δ={_G}{spread:+.4f}{_Z}  "
          f"Y={m.yes_ask:.4f} N={m.no_ask:.4f}",
          flush=True)


def _print_stats(gstats: GlobalStats, open_arbs: dict, markets: dict):
    total_ticks = gstats.ws_ticks + gstats.http_ticks
    ws_pct = 100 * gstats.ws_ticks // total_ticks if total_ticks else 0

    print(f"\n{_C}{'═'*70}{_Z}")
    print(f"{_B}  STATS  [{_ts()}]{_Z}")
    print(f"{_C}{'─'*70}{_Z}")
    print(f"  Markets tracked  : {_Y}{gstats.markets_tracked}{_Z}")
    print(f"  Arb events total : {_Y}{gstats.total_events}{_Z}")
    print(f"  Avg duration     : {_Y}{_fns(gstats.avg_ns())}{_Z}")
    print(f"  Longest          : {_Y}{_fns(gstats.longest_ns)}{_Z}  |  "
          f"Shortest: {_Y}{'—' if not gstats.total_events else _fns(gstats.shortest_ns)}{_Z}")
    print(f"  Max spread ever  : {_Y}{gstats.max_spread:.5f}{_Z} "
          f"({gstats.max_spread*100:.3f}¢)")
    print(f"  WS/HTTP ticks    : {_Y}{gstats.ws_ticks}/{gstats.http_ticks}{_Z} "
          f"({ws_pct}% WS)  |  Errors: {_R}{gstats.errors}{_Z}")

    # Currently open arbs
    if open_arbs:
        print(f"\n{_G}{_B}  ▶ LIVE ARBS ({len(open_arbs)}){_Z}")
        for cid, ev in list(open_arbs.items())[:10]:
            m = markets.get(cid)
            spread = m.spread() if m else ev.max_spread
            print(f"    {_G}●{_Z} {ev.question[:50]:<50}  "
                  f"running={_G}{_fns(ev.duration_ns())}{_Z}  "
                  f"spread={_G}{spread:.5f}{_Z}")

    # Top arb events by max spread (leaderboard)
    if gstats.history:
        print(f"\n{_Y}  TOP {TOP_ARB_COUNT} BY MAX SPREAD{_Z}")
        print(f"  {'Dur':>12}  {'MaxΔ':>8}  {'AvgΔ':>8}  {'Smpl':>5}  Question")
        print(f"  {'─'*12}  {'─'*8}  {'─'*8}  {'─'*5}  {'─'*40}")
        top = sorted(gstats.history,
                     key=lambda e: e.max_spread, reverse=True)[:TOP_ARB_COUNT]
        for ev in top:
            print(f"  {_fns(ev.duration_ns()):>12}  "
                  f"{ev.max_spread:>8.5f}  "
                  f"{ev.avg_spread:>8.5f}  "
                  f"{ev.samples:>5}  "
                  f"{ev.question[:42]}")

    # Recent history
    if gstats.history:
        recent = list(gstats.history)[-8:]
        print(f"\n{_C}  RECENT CLOSED ARBS{_Z}")
        print(f"  {'#':>4}  {'Dur':>12}  {'MaxΔ':>8}  Question")
        print(f"  {'─'*4}  {'─'*12}  {'─'*8}  {'─'*42}")
        for i, ev in enumerate(recent, gstats.total_events - len(recent) + 1):
            print(f"  {i:>4}  {_fns(ev.duration_ns()):>12}  "
                  f"{ev.max_spread:>8.5f}  {ev.question[:42]}")

    print(f"{_C}{'═'*70}{_Z}\n")


# ─────────────────────────────────────────────────────────────────────────────
#  ARB DETECTION LOOP  (hot path — runs on every state change)
# ─────────────────────────────────────────────────────────────────────────────

async def detector(state: State, gstats: GlobalStats):
    """
    Scans all markets every tick.
    Tracks per-market ArbEvent open/close lifecycle.
    Zero sleep in hot path — yields to event loop only.
    """
    last_stat   = time.monotonic()
    last_seen   = {}   # cid → (yes_ask, no_ask)

    while True:
        now_m = time.monotonic()

        # Take a snapshot of markets (avoid holding lock during processing)
        markets_snap = dict(state.markets)

        changed = False
        for cid, m in markets_snap.items():
            if not m.ready():
                continue

            ya, na = m.yes_ask, m.no_ask
            prev   = last_seen.get(cid)

            if prev == (ya, na):
                continue   # no change

            last_seen[cid] = (ya, na)
            changed = True

            spread = 1.0 - ya - na
            is_arb = spread >= MIN_ARB_SPREAD

            if is_arb:
                if cid in state.open_arbs:
                    state.open_arbs[cid].add(spread)
                    # Print compact tick only for sustained arbs
                    if state.open_arbs[cid].samples % 5 == 0:
                        _print_arb_tick(m, spread)
                else:
                    ev = ArbEvent(cid, m.question, spread)
                    state.open_arbs[cid] = ev
                    _print_arb_open(ev, ya, na)
            else:
                if cid in state.open_arbs:
                    ev = state.open_arbs.pop(cid)
                    ev.close()
                    _print_arb_close(ev, gstats.total_events + 1)
                    gstats.record(ev)

        # Also close arbs for markets that disappeared (resolved/removed)
        for cid in list(state.open_arbs.keys()):
            if cid not in markets_snap:
                ev = state.open_arbs.pop(cid)
                ev.close()
                gstats.record(ev)

        # Stats every STAT_INTERVAL_S seconds
        if now_m - last_stat >= STAT_INTERVAL_S:
            _print_stats(gstats, state.open_arbs, state.markets)
            last_stat = now_m

        # Yield without sleeping — let WS/HTTP tasks push new data
        await asyncio.sleep(0.001 if not changed else 0)


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    _print_header()

    state  = State()
    gstats = GlobalStats()

    connector = aiohttp.TCPConnector(
        limit              = 50,
        limit_per_host     = 20,
        ttl_dns_cache      = 600,
        use_dns_cache      = True,
        keepalive_timeout  = 60,
        enable_cleanup_closed = True,
    )
    # Disable gzip — saves CPU on every response
    session_headers = {"Accept-Encoding": "identity"}

    async with aiohttp.ClientSession(
            connector=connector, headers=session_headers) as session:

        print(f"{_C}  Launching tasks…{_Z}\n")

        await asyncio.gather(
            market_refresher(session, state, gstats),  # crawls all markets
            ws_manager(state, gstats),                  # WebSocket feed
            http_fallback(session, state, gstats),      # HTTP backup
            detector(state, gstats),                    # arb scanner
        )


if __name__ == "__main__":
    try:
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            print(f"{_C}  uvloop active ⚡⚡{_Z}")
        except ImportError:
            print(f"{_Y}  tip: pip install uvloop  for 2× faster event loop{_Z}")

        asyncio.run(main())

    except KeyboardInterrupt:
        print(f"\n{_Y}  Stopped.{_Z}\n")
        sys.exit(0)
