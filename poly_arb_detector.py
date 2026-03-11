"""
╔══════════════════════════════════════════════════════════════════╗
║   Polymarket  ACTIVE-MARKET  ARB DETECTOR  ·  µs PRECISION      ║
╠══════════════════════════════════════════════════════════════════╣
║  Fixes vs previous version:                                      ║
║  • Filters ACTIVE markets only (volume>0, liquidity>0, open)     ║
║  • Multiple WS connections to cover ALL tokens (not just 2000)   ║
║  • Lowers MIN_ARB_SPREAD to 0.001 to catch real arbs             ║
║  • Prints every non-zero spread tick so you can see data flow     ║
║  • HTTP /books polling for ALL markets as primary backup          ║
║  • Debug mode shows top spreads even when no arb threshold hit    ║
╚══════════════════════════════════════════════════════════════════╝

Install:  pip install aiohttp orjson uvloop
Run:      python poly_arb_detector.py
"""

import asyncio
import sys
import time
from collections import deque
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
        _JSON_LIB = "json"

import aiohttp

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG  — tune these
# ─────────────────────────────────────────────────────────────────────────────

GAMMA_BASE          = "https://gamma-api.polymarket.com"
CLOB_BASE           = "https://clob.polymarket.com"
WS_URL              = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

MIN_ARB_SPREAD      = 0.016    # 1.6¢ minimum arb spread
MIN_YES_ASK         = 0.02     # YES ask must be > 2¢ (above 2 ticks)
MIN_NO_ASK          = 0.02     # NO  ask must be > 2¢ (above 2 ticks)
SHOW_TOP_N          = 10       # print top-N spreads every stat interval
MIN_VOLUME_24H      = 1.0      # require at least $1 traded in last 24h
MIN_LIQUIDITY       = 0.0      # filter: min $ liquidity
REQUIRE_ACCEPTING   = True     # only markets with acceptingOrders:true
ACTIVE_ONLY         = True     # only markets with active=true from API

MARKET_PAGE_SIZE    = 500      # Gamma API page size
REFRESH_MARKETS_S   = 300      # re-crawl market list every 5 min
WS_TOKENS_PER_CONN  = 500      # tokens per WS connection
WS_PING_S           = 9.0      # WS keepalive
HTTP_POLL_S         = 2.0      # HTTP fallback poll interval
HTTP_BATCH_SIZE     = 100      # tokens per /books batch
STAT_INTERVAL_S     = 30.0     # stats print interval
TOP_ARB_COUNT       = 15       # leaderboard size

# ─────────────────────────────────────────────────────────────────────────────
#  ANSI
# ─────────────────────────────────────────────────────────────────────────────

_R = "\033[91m"; _G = "\033[92m"; _Y = "\033[93m"
_C = "\033[96m"; _M = "\033[95m"; _B = "\033[1m";  _Z = "\033[0m"

def _ts() -> str:
    dt = datetime.fromtimestamp(time.time(), tz=timezone.utc)
    return f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}.{dt.microsecond//1000:03d}"

def _fns(ns: int) -> str:
    if ns < 1_000:         return f"{ns}ns"
    if ns < 1_000_000:     return f"{ns/1e3:.1f}µs"
    if ns < 1_000_000_000: return f"{ns/1e6:.2f}ms"
    return f"{ns/1e9:.3f}s"

# ─────────────────────────────────────────────────────────────────────────────
#  DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

class Market:
    __slots__ = ("condition_id", "question", "slug", "yes_tok", "no_tok",
                 "yes_ask", "no_ask", "last_update_ns", "neg_risk",
                 "volume24h", "liquidity")

    def __init__(self, condition_id, question, slug,
                 yes_tok, no_tok, neg_risk=False,
                 volume24h=0.0, liquidity=0.0):
        self.condition_id   = condition_id
        self.question       = question[:72]
        self.slug           = slug
        self.yes_tok        = yes_tok
        self.no_tok         = no_tok
        self.neg_risk       = neg_risk
        self.volume24h      = volume24h
        self.liquidity      = liquidity
        self.yes_ask        = 0.0
        self.no_ask         = 0.0
        self.last_update_ns = 0

    def spread(self) -> float:
        return 1.0 - self.yes_ask - self.no_ask

    def ready(self) -> bool:
        return self.yes_ask > 0.0 and self.no_ask > 0.0


class ArbEvent:
    __slots__ = ("condition_id", "question", "start_ns", "end_ns",
                 "max_spread", "min_spread", "spread_sum", "samples", "avg_spread")

    def __init__(self, cid: str, question: str, spread: float):
        self.condition_id = cid
        self.question     = question[:55]
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
                 "markets_active", "markets_total", "history",
                 "ws_connections")

    def __init__(self):
        self.total_events    = 0
        self.total_ns        = 0
        self.longest_ns      = 0
        self.shortest_ns     = 10**18
        self.max_spread      = 0.0
        self.ws_ticks        = 0
        self.http_ticks      = 0
        self.errors          = 0
        self.markets_active  = 0
        self.markets_total   = 0
        self.ws_connections  = 0
        self.history: deque  = deque(maxlen=500)

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


class State:
    def __init__(self):
        self.markets: Dict[str, Market]   = {}   # cid → Market
        self.tok_to_cid: Dict[str, str]   = {}   # token_id → cid
        self.open_arbs: Dict[str, ArbEvent] = {}
        self.ws_dirty   = asyncio.Event()
        self.lock       = asyncio.Lock()

    def add_market(self, m: Market):
        self.markets[m.condition_id] = m
        self.tok_to_cid[m.yes_tok]   = m.condition_id
        self.tok_to_cid[m.no_tok]    = m.condition_id

    def remove_market(self, cid: str):
        m = self.markets.pop(cid, None)
        if m:
            self.tok_to_cid.pop(m.yes_tok, None)
            self.tok_to_cid.pop(m.no_tok, None)

    def update_ask(self, token_id: str, ask: float):
        cid = self.tok_to_cid.get(token_id)
        if not cid: return
        m = self.markets.get(cid)
        if not m: return
        if token_id == m.yes_tok: m.yes_ask = ask
        else:                     m.no_ask  = ask
        m.last_update_ns = time.perf_counter_ns()

    def all_tokens(self) -> List[str]:
        return list(self.tok_to_cid.keys())


# ─────────────────────────────────────────────────────────────────────────────
#  MARKET CRAWL  — active only with volume/liquidity filters
# ─────────────────────────────────────────────────────────────────────────────

_TO_SLOW = aiohttp.ClientTimeout(total=20, connect=5)
_TO_FAST = aiohttp.ClientTimeout(total=4,  connect=2)


def _parse_one(m_raw: dict) -> Optional[Market]:
    """Parse one market dict → Market or None if should be skipped."""
    # Must have exactly 2 clob token IDs
    toks = m_raw.get("clobTokenIds") or []
    if isinstance(toks, str):
        try: toks = _loads(toks)
        except: return None
    if not isinstance(toks, list) or len(toks) < 2:
        return None

    # Skip explicitly disabled order books
    if m_raw.get("enableOrderBook") is False:
        return None

    # active=true, closed=false
    if ACTIVE_ONLY:
        if m_raw.get("closed") is True:  return None
        if m_raw.get("active") is False: return None

    # Must be accepting orders RIGHT NOW (this is the key filter)
    if REQUIRE_ACCEPTING:
        if m_raw.get("acceptingOrders") is False:
            return None

    # Volume / liquidity — use whichever field is present
    try:
        vol24 = float(m_raw.get("volume24hr") or
                      m_raw.get("volume24hrClob") or
                      m_raw.get("volumeNum") or 0)
        liq   = float(m_raw.get("liquidity") or
                      m_raw.get("liquidityClob") or
                      m_raw.get("liquidityNum") or 0)
    except (ValueError, TypeError):
        vol24 = liq = 0.0

    if vol24 < MIN_VOLUME_24H: return None
    if liq   < MIN_LIQUIDITY:  return None

    cid = (m_raw.get("conditionId") or m_raw.get("condition_id")
           or m_raw.get("id") or "")
    if not cid: return None

    question = (m_raw.get("question") or m_raw.get("title") or "")
    slug     = m_raw.get("slug", "")
    neg_risk = bool(m_raw.get("negRisk") or m_raw.get("neg_risk", False))

    return Market(
        condition_id = cid,
        question     = question,
        slug         = slug,
        yes_tok      = str(toks[0]),
        no_tok       = str(toks[1]),
        neg_risk     = neg_risk,
        volume24h    = vol24,
        liquidity    = liq,
    )


async def crawl_gamma(session: aiohttp.ClientSession,
                      gstats: GlobalStats) -> List[Market]:
    """
    Paginate Gamma /markets with active+closed filters.
    Probes param sets so we always find what works.
    """
    # Params to probe — stop at first 200
    probes = [
        {"active": "true", "closed": "false",
         "limit": str(MARKET_PAGE_SIZE), "offset": "0"},
        {"active": "true", "closed": "false", "limit": str(MARKET_PAGE_SIZE)},
        {"limit": str(MARKET_PAGE_SIZE)},
    ]

    working: Optional[dict] = None
    for p in probes:
        try:
            async with session.get(f"{GAMMA_BASE}/markets",
                                   params=p, timeout=_TO_SLOW) as r:
                body = await r.read()
                if r.status == 200:
                    test = _loads(body)
                    if isinstance(test, list) or (
                            isinstance(test, dict) and
                            ("data" in test or "markets" in test)):
                        working = p
                        print(f"{_G}  Gamma params OK: {p}{_Z}")
                        break
                else:
                    print(f"{_Y}  Gamma probe {p} → {r.status}{_Z}")
        except Exception as e:
            print(f"{_Y}  Gamma probe error: {e}{_Z}")

    if working is None:
        return []

    markets: List[Market] = []
    offset = 0

    while True:
        p = dict(working)
        p["offset"] = str(offset)
        try:
            async with session.get(f"{GAMMA_BASE}/markets",
                                   params=p, timeout=_TO_SLOW) as r:
                if r.status != 200:
                    print(f"{_R}  Gamma page offset={offset} → {r.status}{_Z}")
                    break
                raw = _loads(await r.read())
        except Exception as e:
            print(f"{_R}  Gamma page error: {e}{_Z}")
            break

        data = raw if isinstance(raw, list) else (
               raw.get("data") or raw.get("markets") or [])

        page_total = 0
        for m_raw in data:
            page_total += 1
            m = _parse_one(m_raw)
            if m:
                markets.append(m)

        gstats.markets_total = offset + page_total
        page_lim = int(working.get("limit", MARKET_PAGE_SIZE))

        if len(data) < page_lim:
            break
        offset += page_lim

    return markets


async def crawl_clob(session: aiohttp.ClientSession) -> List[Market]:
    """CLOB /markets cursor pagination — fallback."""
    markets: List[Market] = []
    cursor  = ""
    while True:
        params = {"next_cursor": cursor} if cursor else {}
        try:
            async with session.get(f"{CLOB_BASE}/markets",
                                   params=params, timeout=_TO_SLOW) as r:
                if r.status != 200:
                    break
                raw = _loads(await r.read())
        except Exception as e:
            print(f"{_R}  CLOB markets error: {e}{_Z}")
            break

        if isinstance(raw, dict):
            data   = raw.get("data") or []
            cursor = raw.get("next_cursor", "")
        elif isinstance(raw, list):
            data   = raw
            cursor = ""
        else:
            break

        for m_raw in data:
            m = _parse_one(m_raw)
            if m:
                markets.append(m)

        if not cursor or cursor == "LTE=":
            break

    return markets


async def crawl_all(session: aiohttp.ClientSession,
                    gstats: GlobalStats) -> List[Market]:
    markets = await crawl_gamma(session, gstats)
    if not markets:
        print(f"{_Y}  Gamma returned 0 → trying CLOB /markets…{_Z}")
        markets = await crawl_clob(session)

    # Deduplicate
    seen: Dict[str, Market] = {}
    for m in markets:
        seen[m.condition_id] = m
    return list(seen.values())


# ─────────────────────────────────────────────────────────────────────────────
#  MARKET REFRESHER
# ─────────────────────────────────────────────────────────────────────────────

async def market_refresher(session: aiohttp.ClientSession,
                           state: State, gstats: GlobalStats):
    first = True
    while True:
        t0 = time.monotonic()
        print(f"{_C}  ↻  Crawling markets "
              f"(active={ACTIVE_ONLY}, "
              f"vol≥{MIN_VOLUME_24H}, liq≥{MIN_LIQUIDITY})…{_Z}")

        fresh = await crawl_all(session, gstats)
        elapsed = time.monotonic() - t0

        async with state.lock:
            old_cids = set(state.markets)
            new_cids = {m.condition_id for m in fresh}
            added    = new_cids - old_cids
            removed  = old_cids - new_cids

            for m in fresh:
                state.add_market(m)
            for cid in removed:
                state.remove_market(cid)

            gstats.markets_active = len(state.markets)

        print(f"{_G}  ✓  {len(fresh)} active markets  "
              f"+{len(added)}  -{len(removed)}  ({elapsed:.1f}s){_Z}")

        if added or first:
            state.ws_dirty.set()
            first = False

        await asyncio.sleep(REFRESH_MARKETS_S)


# ─────────────────────────────────────────────────────────────────────────────
#  WEBSOCKET MANAGER  — multiple connections, each covering a shard of tokens
# ─────────────────────────────────────────────────────────────────────────────

async def _ws_connection(ws_id: int, tokens: List[str],
                         state: State, gstats: GlobalStats):
    """One WS connection covering `tokens`."""

    sub_msg = _dumps({
        "type": "market",
        "assets_ids": tokens,
        "custom_feature_enabled": True,
    })
    tok_set = set(tokens)

    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(
                    WS_URL,
                    heartbeat=WS_PING_S,
                    compress=False,
                    max_msg_size=0,
                ) as ws:
                    await ws.send_str(sub_msg)
                    gstats.ws_connections += 1
                    # only log first time each shard connects (not every reconnect)
                    if gstats.ws_connections == 1 or ws_id == 0:
                        print(f"{_C}  WS#{ws_id} up ({len(tokens)} tokens)  "
                              f"total={gstats.ws_connections}{_Z}")

                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            break
                        raw = msg.data
                        if raw == "PONG":
                            continue
                        try:
                            parsed = _loads(raw)
                        except Exception:
                            continue

                        # WS sometimes sends a JSON array (list of events)
                        # normalise to a flat list of event dicts
                        if isinstance(parsed, list):
                            events = parsed
                        elif isinstance(parsed, dict):
                            events = [parsed]
                        else:
                            continue

                        for ev in events:
                            if not isinstance(ev, dict):
                                continue

                            etype = ev.get("event_type")

                            if etype == "best_bid_ask":
                                ask_raw = ev.get("best_ask")
                                if ask_raw is None: continue
                                state.update_ask(
                                    ev.get("asset_id", ""), float(ask_raw))
                                gstats.ws_ticks += 1

                            elif etype == "book":
                                asks = ev.get("asks")
                                if asks:
                                    state.update_ask(
                                        ev.get("asset_id", ""),
                                        float(asks[0]["price"]))

                            elif etype == "price_change":
                                for pc in ev.get("price_changes") or []:
                                    if not isinstance(pc, dict): continue
                                    ask_raw = pc.get("best_ask")
                                    if ask_raw is None: continue
                                    state.update_ask(
                                        pc.get("asset_id", ""),
                                        float(ask_raw))
                                gstats.ws_ticks += 1

                            elif etype == "market_resolved":
                                cid = state.tok_to_cid.get(
                                    ev.get("winning_asset_id", ""), "")
                                if cid:
                                    async with state.lock:
                                        state.remove_market(cid)

                    gstats.ws_connections -= 1

        except Exception as exc:
            print(f"{_R}  WS#{ws_id} error: {exc}{_Z}")
            gstats.ws_connections -= 1

        await asyncio.sleep(1.0)


async def ws_manager(state: State, gstats: GlobalStats):
    """
    Waits for ws_dirty signal, then spawns one WS task per shard.
    Each shard covers WS_TOKENS_PER_CONN tokens.
    Old tasks are cancelled and replaced on refresh.
    """
    tasks: List[asyncio.Task] = []

    while True:
        await state.ws_dirty.wait()
        state.ws_dirty.clear()

        # Cancel old WS tasks
        for t in tasks:
            t.cancel()
        tasks.clear()

        all_toks = state.all_tokens()
        if not all_toks:
            continue

        # Shard tokens across connections
        shards = [all_toks[i:i + WS_TOKENS_PER_CONN]
                  for i in range(0, len(all_toks), WS_TOKENS_PER_CONN)]

        print(f"{_C}  Sharding {len(all_toks)} tokens → "
              f"{len(shards)} WS connections{_Z}")

        for i, shard in enumerate(shards):
            t = asyncio.create_task(
                _ws_connection(i, shard, state, gstats),
                name=f"ws_{i}"
            )
            tasks.append(t)

        await asyncio.sleep(1.0)


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP FALLBACK — batch /books poll for all markets
# ─────────────────────────────────────────────────────────────────────────────

async def http_fallback(session: aiohttp.ClientSession,
                        state: State, gstats: GlobalStats):
    """
    Round-robins through all markets every HTTP_POLL_S seconds.
    Calls POST /books in batches of HTTP_BATCH_SIZE tokens.
    Writes to state for any token — WS and HTTP both feed the same state.
    """
    while True:
        await asyncio.sleep(HTTP_POLL_S)

        all_toks = state.all_tokens()
        if not all_toks:
            continue

        for i in range(0, min(len(all_toks), 1000), HTTP_BATCH_SIZE):
            batch = all_toks[i:i + HTTP_BATCH_SIZE]
            body  = [{"token_id": t} for t in batch]
            try:
                async with session.post(f"{CLOB_BASE}/books",
                                        json=body,
                                        timeout=_TO_FAST) as r:
                    if r.status != 200:
                        gstats.errors += 1
                        continue
                    data = _loads(await r.read())
            except Exception:
                gstats.errors += 1
                continue

            if not isinstance(data, list):
                continue

            for j, book in enumerate(data):
                if j >= len(batch): break
                asks = book.get("asks")
                if asks:
                    try:
                        state.update_ask(batch[j], float(asks[0]["price"]))
                    except Exception:
                        pass

            gstats.http_ticks += 1
            await asyncio.sleep(0)   # yield between batches


# ─────────────────────────────────────────────────────────────────────────────
#  DISPLAY  — arb events only, zero noise
# ─────────────────────────────────────────────────────────────────────────────

def _print_header():
    print(f"\n{_C}{_B}{'═'*68}{_Z}")
    print(f"{_B}   Polymarket ARB DETECTOR  ·  arb≥{MIN_ARB_SPREAD:.3f}"
          f"  YES/NO≥{MIN_YES_ASK:.3f}  µs clock{_Z}")
    print(f"{_C}{'═'*68}{_Z}\n")


def _print_arb_open(ev: ArbEvent, yes: float, no: float):
    # Single compact line on open
    print(f"{_G}🔥 {_ts()}  OPEN   "
          f"Δ={ev.max_spread:.4f} ({ev.max_spread*100:.2f}¢)  "
          f"YES={yes:.4f} NO={no:.4f}  "
          f"{ev.question[:50]}{_Z}")


def _print_arb_update(ev: ArbEvent, spread: float):
    # Tick line when arb spread changes while open
    print(f"{_G}   {_ts()}  TICK   "
          f"Δ={spread:.4f} ({spread*100:.2f}¢)  "
          f"max={ev.max_spread:.4f}  "
          f"t={ev.duration_ns()//1_000_000}ms  "
          f"{ev.question[:50]}{_Z}")


def _print_arb_close(ev: ArbEvent, n: int):
    dur_ms = ev.duration_ns() / 1_000_000
    color  = _G if dur_ms < 500 else _Y if dur_ms < 2000 else _R
    print(f"{color}✓ #{n} {_ts()}  CLOSE  "
          f"dur={dur_ms:.1f}ms  "
          f"Δmax={ev.max_spread:.4f} ({ev.max_spread*100:.2f}¢)  "
          f"avg={ev.avg_spread:.4f}  "
          f"n={ev.samples}  "
          f"{ev.question[:46]}{_Z}")


def _print_stats(gstats: GlobalStats,
                 open_arbs: Dict[str, ArbEvent],
                 markets: Dict[str, Market],
                 top_spreads: list):
    total_ticks = gstats.ws_ticks + gstats.http_ticks
    ws_pct = 100 * gstats.ws_ticks // total_ticks if total_ticks else 0
    avg_ms = gstats.avg_ns() / 1e6

    print(f"\n{_C}── STATS {_ts()} "
          f"│ markets={gstats.markets_active} "
          f"│ WS-conn={gstats.ws_connections} "
          f"│ arbs={gstats.total_events} "
          f"│ avg={avg_ms:.1f}ms "
          f"│ max-Δ={gstats.max_spread:.4f} "
          f"│ ticks={total_ticks} ({ws_pct}%WS) "
          f"│ err={gstats.errors} ──{_Z}")

    if open_arbs:
        print(f"{_G}  LIVE ({len(open_arbs)}): " +
              "  ".join(
                  f"{ev.question[:30]} Δ={ev.max_spread:.4f} "
                  f"t={ev.duration_ns()//1_000_000}ms"
                  for ev in list(open_arbs.values())[:5]
              ) + f"{_Z}")

    if top_spreads:
        print(f"{_Y}  TOP SPREADS: " +
              "  ".join(
                  f"{m.question[:24]}={s:.4f}"
                  for s, m, ya, na in top_spreads[:5]
                  if m
              ) + f"{_Z}")

    if gstats.history:
        best = max(gstats.history, key=lambda e: e.max_spread)
        fastest = min(gstats.history, key=lambda e: e.duration_ns())
        print(f"{_C}  BEST: Δ={best.max_spread:.4f} {best.question[:40]}  "
              f"FASTEST: {fastest.duration_ns()//1_000_000}ms "
              f"{fastest.question[:30]}{_Z}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
#  ARB DETECTION LOOP
# ─────────────────────────────────────────────────────────────────────────────

async def detector(state: State, gstats: GlobalStats):
    last_stat = time.monotonic()
    last_seen: Dict[str, Tuple[float, float]] = {}
    top_buf:   Dict[str, Tuple[float, float, float]] = {}

    while True:
        now_m   = time.monotonic()
        markets = state.markets

        for cid, m in markets.items():
            if not m.ready():
                continue

            ya, na = m.yes_ask, m.no_ask

            # ── Hard filter: both sides must have real liquidity ──────────
            if ya < MIN_YES_ASK or na < MIN_NO_ASK:
                if cid in state.open_arbs:
                    ev = state.open_arbs.pop(cid)
                    ev.close()
                    _print_arb_close(ev, gstats.total_events + 1)
                    gstats.record(ev)
                continue

            if last_seen.get(cid) == (ya, na):
                continue

            last_seen[cid] = (ya, na)
            spread = 1.0 - ya - na
            top_buf[cid] = (spread, ya, na)

            is_arb = spread >= MIN_ARB_SPREAD

            if is_arb:
                if cid in state.open_arbs:
                    prev_spread = state.open_arbs[cid].max_spread
                    state.open_arbs[cid].add(spread)
                    # print tick only if spread changed meaningfully
                    if abs(spread - prev_spread) >= 0.001:
                        _print_arb_update(state.open_arbs[cid], spread)
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

        # Close arbs for removed markets
        for cid in list(state.open_arbs):
            if cid not in markets:
                ev = state.open_arbs.pop(cid)
                ev.close()
                gstats.record(ev)

        # Stats — one compact line every interval
        if now_m - last_stat >= STAT_INTERVAL_S:
            top_list = sorted(
                ((s, markets.get(cid), ya, na)
                 for cid, (s, ya, na) in top_buf.items()
                 if markets.get(cid) is not None),
                key=lambda x: x[0],
                reverse=True
            )[:SHOW_TOP_N]
            _print_stats(gstats, state.open_arbs, markets, top_list)
            last_stat = now_m

        await asyncio.sleep(0)


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    _print_header()

    state  = State()
    gstats = GlobalStats()

    connector = aiohttp.TCPConnector(
        limit              = 100,
        limit_per_host     = 30,
        ttl_dns_cache      = 600,
        use_dns_cache      = True,
        keepalive_timeout  = 60,
        enable_cleanup_closed = True,
    )
    async with aiohttp.ClientSession(
            connector     = connector,
            headers       = {"Accept-Encoding": "identity"}) as session:

        print(f"{_C}  Spawning tasks…{_Z}\n")
        await asyncio.gather(
            market_refresher(session, state, gstats),
            ws_manager(state, gstats),
            http_fallback(session, state, gstats),
            detector(state, gstats),
        )


if __name__ == "__main__":
    try:
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            print(f"{_C}  uvloop ⚡⚡{_Z}")
        except ImportError:
            print(f"{_Y}  tip: pip install uvloop  for 2× faster event loop{_Z}")

        asyncio.run(main())

    except KeyboardInterrupt:
        print(f"\n{_Y}  Stopped.{_Z}\n")
        sys.exit(0)
