"""Market metadata and live state + Level 5 Intelligence."""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, List, Tuple

from utils import BPS, ZERO, ONE, clamp


def _D(x) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


@dataclass
class Market:
    market_id: int
    name: str
    status: str
    tick: Decimal
    step: Decimal
    tiers: list
    min_notional: Decimal
    min_size: Decimal
    max_size: Decimal
    mark: Decimal
    is_outside_rth: bool
    funding_rate: Decimal = Decimal(0)
    next_funding_time: float = 0.0

    @classmethod
    def from_api(cls, d: dict) -> "Market":
        return cls(
            market_id=int(d["marketId"]),
            name=d["marketDisplayName"],
            status=str(d.get("status", "ONLINE")).upper(),
            tick=Decimal(str(d["tickSize"])),
            step=Decimal(str(d["stepSize"])),
            tiers=list(d.get("tickTiers") or []),
            min_notional=Decimal(str(d.get("minOrderNotional") or "0")),
            min_size=Decimal(str(d.get("minOrderSize") or "0")),
            max_size=Decimal(str(d.get("maxOrderSize") or "0")),
            mark=Decimal(str(d.get("markPrice") or "0")),
            is_outside_rth=bool(d.get("isOutsideRth")),
            funding_rate=Decimal(str(d.get("fundingRate") or d.get("funding_rate") or "0")),
            next_funding_time=float(d.get("nextFundingTime") or d.get("next_funding_time") or 0.0),
        )

    def tick_for(self, price: Decimal) -> Decimal:
        for t in self.tiers:
            up = t.get("upToPrice")
            if up is None or price < Decimal(str(up)):
                return Decimal(str(t["tick"]))
        return self.tick


@dataclass
class VenueState:
    venue: str
    bid: Decimal
    ask: Decimal
    bid_sz: Decimal
    ask_sz: Decimal
    ts: float
    bids: tuple = ()          # ((px, sz), ...) best first - optional depth snapshot
    asks: tuple = ()
    depth_ts: float = 0.0

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def obi(self) -> Decimal:
        """Depth-weighted (1/rank) notional imbalance when depth is known, else top-of-book."""
        if self.bids and self.asks:
            b = a = 0.0
            for k, (px, sz) in enumerate(self.bids[:10]):
                b += float(px * sz) / (k + 1)
            for k, (px, sz) in enumerate(self.asks[:10]):
                a += float(px * sz) / (k + 1)
            tot = b + a
            return Decimal(str(round((b - a) / tot, 6))) if tot > 0 else ZERO
        total = self.bid_sz + self.ask_sz
        if total <= 0:
            return ZERO
        return (self.bid_sz - self.ask_sz) / total


class _Basis:
    __slots__ = ("value", "last_ts", "first_ts", "n")

    def __init__(self, v: float, ts: float):
        self.value, self.last_ts, self.first_ts, self.n = v, ts, ts, 1


class CrossVenueTracker:
    """External-venue intelligence (Binance / Bybit ...).

    Signals (all staleness-gated):
      * basis-adjusted lead/lag divergence  (external mid vs Arcus mid, minus rolling USDT/USD basis)
      * per-venue price velocity            (never mixes venues in one series)
      * depth-weighted order-book imbalance
      * aggressive trade-flow imbalance (USD)
      * forced-liquidation pressure (USD)
      * cross-venue dispersion
    """

    def __init__(self, cfg=None):
        g = lambda k, d: getattr(cfg, k, d) if cfg is not None else d
        self.stale_s = float(g("cross_stale_s", 2.0))
        self.basis_tau_s = float(g("cross_basis_tau_s", 45.0))
        self.warmup_s = float(g("cross_warmup_s", 10.0))
        self.max_shift_bps = float(g("cross_max_shift_bps", 4.0))
        self.flow_k_usd = float(g("cross_flow_k_usd", 20000.0))
        self.weights = dict(g("cross_weights", {}) or {})
        self.venues: dict = {}
        self._history: deque = deque()          # (now, venue, mid) kept for compatibility
        self._vhist: dict = {}                   # venue -> deque[(now, mid)]
        self._basis: dict = {}                   # venue -> _Basis
        self._trades: deque = deque()            # (now, venue, side, usd)
        self._liqs: deque = deque()              # (now, venue, side, usd)  side = FORCED order side
        self._last_now = 0.0
        self._cache: dict = {}

    # ------------------------------------------------------------------ ingest
    def update_venue(self, venue: str, bid: Decimal, ask: Decimal,
                     bid_sz: Decimal, ask_sz: Decimal, now: float) -> None:
        old = self.venues.get(venue)
        st = VenueState(venue, bid, ask, bid_sz, ask_sz, now)
        if old is not None and old.bids:
            st.bids, st.asks, st.depth_ts = old.bids, old.asks, old.depth_ts
        self.venues[venue] = st
        self._touch(now)
        mid = st.mid
        self._history.append((now, venue, mid))
        while self._history and now - self._history[0][0] > 60.0:
            self._history.popleft()
        h = self._vhist.setdefault(venue, deque())
        h.append((now, mid))
        while h and now - h[0][0] > 30.0:
            h.popleft()

    def update_depth(self, venue: str, bids: list, asks: list, now: float) -> None:
        st = self.venues.get(venue)
        try:
            b = tuple((_D(r[0]), _D(r[1])) for r in bids[:10])
            a = tuple((_D(r[0]), _D(r[1])) for r in asks[:10])
        except Exception:
            return
        if not b or not a:
            return
        if st is None:
            st = VenueState(venue, b[0][0], a[0][0], b[0][1], a[0][1], now)
            self.venues[venue] = st
        st.bids, st.asks, st.depth_ts = b, a, now
        self._touch(now)

    def update_trade(self, venue: str, side: str, size: Decimal, price: Decimal, now: float) -> None:
        self._trades.append((now, venue, side.upper(), float(size * price)))
        while self._trades and now - self._trades[0][0] > 30.0:
            self._trades.popleft()
        self._touch(now)

    def update_liquidation(self, venue: str, side: str, size: Decimal, price: Decimal, now: float) -> None:
        """side = side of the FORCED order (SELL = a long was liquidated -> downward pressure)."""
        self._liqs.append((now, venue, side.upper(), float(size * price)))
        while self._liqs and now - self._liqs[0][0] > 60.0:
            self._liqs.popleft()
        self._touch(now)

    def drop_venue(self, venue: str) -> None:
        """Called on disconnect so a dead feed can never keep skewing quotes."""
        self.venues.pop(venue, None)
        self._vhist.pop(venue, None)
        self._cache.clear()

    def observe_local(self, local_mid: Decimal, now: float) -> None:
        """Learn the slow USDT-vs-USD basis between each venue and Arcus (EWMA, freezes on dislocations)."""
        lm = float(local_mid)
        if lm <= 0:
            return
        for name, st in self.venues.items():
            if now - st.ts > self.stale_s:
                continue
            sample = (float(st.mid) - lm) / lm * 1e4
            b = self._basis.get(name)
            if b is None:
                self._basis[name] = _Basis(sample, now)
                continue
            dt = now - b.last_ts
            if dt <= 0:
                continue
            a = min(1.0 - math.exp(-dt / self.basis_tau_s), 0.05)
            if abs(sample - b.value) > 3.0:      # genuine lead/dislocation: barely absorb it
                a *= 0.1
            b.value += a * (sample - b.value)
            b.last_ts = now
            b.n += 1
        self._cache.clear()

    def _touch(self, now: float) -> None:
        if now > self._last_now:
            self._last_now = now
        self._cache.clear()

    # ------------------------------------------------------------------ helpers
    def _now(self, now: Optional[float]) -> float:
        return self._last_now if now is None else now

    def fresh(self, now: Optional[float] = None) -> list:
        n = self._now(now)
        return [v for v in self.venues.values() if n - v.ts <= self.stale_s and v.mid > ZERO]

    def _w(self, venue: str) -> float:
        return float(self.weights.get(venue.lower(), 1.0))

    def warmed(self, venue: str, now: float) -> bool:
        b = self._basis.get(venue)
        return b is not None and b.n >= 20 and (now - b.first_ts) >= self.warmup_s

    # ------------------------------------------------------------------ signals
    def cross_fair_value(self, now: Optional[float] = None) -> Optional[Decimal]:
        fr = self.fresh(now)
        if not fr:
            return None
        return sum(v.mid for v in fr) / Decimal(str(len(fr)))

    def venue_divergences(self, local_mid: Optional[Decimal], now: Optional[float] = None) -> list:
        """[(venue, basis-adjusted divergence bps, weight)] for fresh, basis-warmed venues.
        Positive = external venue is ABOVE Arcus (Arcus likely to rise)."""
        if local_mid is None or local_mid <= ZERO:
            return []
        n = self._now(now)
        lm = float(local_mid)
        out = []
        for v in self.fresh(n):
            if not self.warmed(v.venue, n):
                continue
            d = (float(v.mid) - lm) / lm * 1e4 - self._basis[v.venue].value
            out.append((v.venue, d, self._w(v.venue)))
        return out

    def lead_lag_divergence_bps(self, local_mid: Optional[Decimal], now: Optional[float] = None) -> Decimal:
        key = ("div", now, local_mid)
        hit = self._cache.get(key)
        if hit is not None:
            return hit
        rows = self.venue_divergences(local_mid, now)
        if not rows:
            return ZERO
        tw = sum(w for _, _, w in rows)
        d = sum(x * w for _, x, w in rows) / tw if tw > 0 else 0.0
        d = max(-self.max_shift_bps, min(self.max_shift_bps, d))
        res = Decimal(str(round(d, 4)))
        self._cache[key] = res
        return res

    def cross_velocity_bps(self, window_s: float, now: float) -> Decimal:
        """Mean per-venue price change over window_s (venues are never mixed in one series)."""
        vals = []
        for name, h in self._vhist.items():
            st = self.venues.get(name)
            if st is None or now - st.ts > self.stale_s:
                continue
            first = None
            for t, m in reversed(h):
                if now - t > window_s:
                    break
                first = m
            last = h[-1][1] if h else None
            if first is None or last is None or first == ZERO or len([1 for t, _ in h if now - t <= window_s]) < 2:
                continue
            vals.append(((last - first) / first * BPS, self._w(name)))
        if not vals:
            return ZERO
        tw = sum(w for _, w in vals)
        return sum(v * Decimal(str(w)) for v, w in vals) / Decimal(str(tw))

    def cross_dispersion_bps(self, now: Optional[float] = None) -> Decimal:
        mids = [v.mid for v in self.fresh(now)]
        if len(mids) < 2:
            return ZERO
        avg_mid = sum(mids) / Decimal(str(len(mids)))
        if avg_mid <= ZERO:
            return ZERO
        return (max(mids) - min(mids)) / avg_mid * BPS

    def cross_obi(self, now: Optional[float] = None) -> Decimal:
        fr = self.fresh(now)
        if not fr:
            return ZERO
        return sum(v.obi for v in fr) / Decimal(str(len(fr)))

    def cross_tfi(self, window_s: float, now: float) -> Decimal:
        """USD aggressive-flow imbalance across venues, shrunk toward 0 when volume is thin."""
        buy = sell = 0.0
        for t, _, side, usd in reversed(self._trades):
            if now - t > window_s:
                break
            if side in ("BUY", "BID"):
                buy += usd
            else:
                sell += usd
        tot = buy + sell
        if tot <= 0:
            return ZERO
        return Decimal(str(round((buy - sell) / tot * tot / (tot + self.flow_k_usd), 6)))

    def liq_pressure_usd(self, window_s: float, now: float) -> Tuple[float, float]:
        """(forced-sell USD [longs liquidated], forced-buy USD [shorts liquidated]) in window."""
        down = up = 0.0
        for t, _, side, usd in reversed(self._liqs):
            if now - t > window_s:
                break
            if side in ("SELL", "ASK"):
                down += usd
            else:
                up += usd
        return down, up

    def pull_decision(self, local_mid: Optional[Decimal], now: float, pull_bps: float,
                      vel_pull_bps: float, liq_usd: float) -> Tuple[bool, bool, str]:
        """(block_buy, block_sell, reason): pull the quote an external venue says is stale/about to be run over."""
        block_buy = block_sell = False
        why = []
        rows = self.venue_divergences(local_mid, now)
        if rows:
            ds = [d for _, d, _ in rows]
            wd = sum(d * w for _, d, w in rows) / sum(w for _, _, w in rows)
            need = 0.4 * pull_bps
            if wd <= -pull_bps and all(d <= -need for d in ds):
                block_buy = True
                why.append(f"ext below arcus {wd:.1f}bps")
            elif wd >= pull_bps and all(d >= need for d in ds):
                block_sell = True
                why.append(f"ext above arcus {wd:+.1f}bps")
        vel = float(self.cross_velocity_bps(1.0, now))
        if vel <= -vel_pull_bps:
            block_buy = True
            why.append(f"ext vel {vel:.1f}bps/1s")
        elif vel >= vel_pull_bps:
            block_sell = True
            why.append(f"ext vel {vel:+.1f}bps/1s")
        down, up = self.liq_pressure_usd(5.0, now)
        if liq_usd > 0:
            if down >= liq_usd:
                block_buy = True
                why.append(f"long liqs ${down:,.0f}")
            if up >= liq_usd:
                block_sell = True
                why.append(f"short liqs ${up:,.0f}")
        return block_buy, block_sell, ", ".join(why)


class MarketData:
    HISTORY_S = 120.0

    def __init__(self, cfg):
        self.cfg = cfg
        self.info: Optional[Market] = None
        self.info_ts = 0.0
        self.bid: Optional[Decimal] = None
        self.ask: Optional[Decimal] = None
        self.bid_sz: Optional[Decimal] = None
        self.ask_sz: Optional[Decimal] = None
        self.ts = 0.0
        self.jump_until = 0.0
        self._hist: deque = deque()
        self._trades: deque = deque()
        self._tfi_cache: dict = {}
        self._tfi_now = None
        self._vol_ewma = ZERO
        self._book_depth_bids: list = []
        self._book_depth_asks: list = []
        self.cross = CrossVenueTracker(cfg)
        self.funding_rate: Decimal = ZERO
        self.next_funding_time: float = 0.0

    def clear_book(self) -> None:
        self.bid = self.ask = self.bid_sz = self.ask_sz = None
        self._book_depth_bids.clear()
        self._book_depth_asks.clear()

    def update(self, bid: Decimal, ask: Decimal, bid_sz: Optional[Decimal],
               ask_sz: Optional[Decimal], now: float) -> None:
        prev = self.mid
        self.bid, self.ask, self.bid_sz, self.ask_sz, self.ts = bid, ask, bid_sz, ask_sz, now
        mid = self.mid
        if prev and mid and bid < ask:
            if abs(mid - prev) / prev * BPS >= self.cfg.jump_bps:
                self.jump_until = now + self.cfg.jump_cooldown_s
        if bid < ask and self.cross.venues:
            self.cross.observe_local(mid, now)
        if bid < ask:
            self._hist.append((now, mid))
            while self._hist and now - self._hist[0][0] > self.HISTORY_S:
                self._hist.popleft()
            move = self.move_bps(self.cfg.vol_window_s, now)
            self._vol_ewma = move if self._vol_ewma == 0 else self._vol_ewma * Decimal("0.9") + move * Decimal("0.1")

    def on_trade(self, side: str, size: Decimal, price: Decimal, now: float) -> None:
        self._trades.append((now, side.upper(), size, price))
        self._tfi_cache.clear()
        self._tfi_now = None
        while self._trades and now - self._trades[0][0] > 60.0:
            self._trades.popleft()

    def on_depth(self, bids: list, asks: list, now: float) -> None:
        try:
            self._book_depth_bids = [(_D(r[0]), _D(r[1])) for r in bids]
            self._book_depth_asks = [(_D(r[0]), _D(r[1])) for r in asks]
        except Exception:
            self._book_depth_bids = bids
            self._book_depth_asks = asks

    def update_cross_venue(self, venue: str, bid: Decimal, ask: Decimal,
                           bid_sz: Decimal, ask_sz: Decimal, now: float) -> None:
        self.cross.update_venue(venue, bid, ask, bid_sz, ask_sz, now)
        if self.mid is not None and now - self.ts <= self.cross.stale_s:
            self.cross.observe_local(self.mid, now)

    def update_cross_depth(self, venue: str, bids: list, asks: list, now: float) -> None:
        self.cross.update_depth(venue, bids, asks, now)

    def update_cross_trade(self, venue: str, side: str, size: Decimal, price: Decimal, now: float) -> None:
        self.cross.update_trade(venue, side, size, price, now)

    def update_cross_liq(self, venue: str, side: str, size: Decimal, price: Decimal, now: float) -> None:
        self.cross.update_liquidation(venue, side, size, price, now)

    @property
    def mid(self) -> Optional[Decimal]:
        return (self.bid + self.ask) / 2 if self.bid is not None and self.ask is not None else None

    @property
    def micro(self) -> Optional[Decimal]:
        if self.bid is None or self.ask is None:
            return None
        if self.bid_sz and self.ask_sz and (self.bid_sz + self.ask_sz) > 0:
            return (self.bid * self.ask_sz + self.ask * self.bid_sz) / (self.bid_sz + self.ask_sz)
        return self.mid

    @property
    def obi(self) -> Decimal:
        if self.bid_sz is None or self.ask_sz is None:
            return ZERO
        total = self.bid_sz + self.ask_sz
        if total <= 0:
            return ZERO
        return (self.bid_sz - self.ask_sz) / total

    def trade_flow_imbalance(self, window_s: float, now: float) -> Decimal:
        if self._tfi_now != now:
            self._tfi_cache.clear()
            self._tfi_now = now
        else:
            hit = self._tfi_cache.get(window_s)
            if hit is not None:
                return hit
        buy_vol = ZERO
        sell_vol = ZERO
        for t, side, sz, _ in reversed(self._trades):
            if now - t > window_s:
                break
            if side in ("BUY", "BID"):
                buy_vol += sz
            else:
                sell_vol += sz
        total = buy_vol + sell_vol
        res = ZERO if total <= 0 else (buy_vol - sell_vol) / total
        self._tfi_cache[window_s] = res
        return res

    @property
    def spread_bps(self) -> Decimal:
        m = self.mid
        return (self.ask - self.bid) / m * BPS if m else ZERO

    def jump_active(self, now: float) -> bool:
        return now < self.jump_until

    def _window(self, window_s: float, now: float):
        return [m for t, m in self._hist if now - t <= window_s]

    def ret_bps(self, window_s: float, now: float) -> Decimal:
        newest = oldest = None
        n = 0
        for t, m in reversed(self._hist):
            if now - t > window_s:
                break
            if newest is None:
                newest = m
            oldest = m
            n += 1
        if n < 2 or oldest == 0:
            return ZERO
        return (newest - oldest) / oldest * BPS

    def move_bps(self, window_s: float, now: float) -> Decimal:
        newest = oldest = hi = lo = None
        n = 0
        for t, m in reversed(self._hist):
            if now - t > window_s:
                break
            if newest is None:
                newest = hi = lo = m
            else:
                if m > hi:
                    hi = m
                elif m < lo:
                    lo = m
            oldest = m
            n += 1
        if n < 2:
            return ZERO
        if oldest == 0:
            return ZERO
        return (hi - lo) / newest * BPS

    @property
    def vol_bps(self) -> Decimal:
        return self._vol_ewma

    def detect_regime(self, now: float, tox_bps: Decimal) -> str:
        if tox_bps >= self.cfg.regime_toxic_threshold_bps:
            return "REGIME_D_TOXIC"
        tfi = abs(self.trade_flow_imbalance(10.0, now))
        obi = abs(self.obi)
        if tfi >= self.cfg.regime_flow_threshold or obi >= self.cfg.regime_flow_threshold:
            return "REGIME_C_TREND"
        if self.vol_bps >= self.cfg.regime_vol_threshold_bps:
            return "REGIME_B_HIGH_VOL"
        return "REGIME_A_QUIET"

    def queue_ahead(self, side: str, price: Decimal) -> Decimal:
        """Calculates resting queue depth ahead of (and at) our quote price on the given side."""
        if side in ("BUY", "BID"):
            if not self._book_depth_bids:
                return self.bid_sz or Decimal("1")
            ahead = ZERO
            for row in self._book_depth_bids:
                p, sz = _D(row[0]), _D(row[1])
                if p >= price:
                    ahead += sz
                else:
                    break
            return ahead if ahead > ZERO else (self.bid_sz or Decimal("1"))
        else:
            if not self._book_depth_asks:
                return self.ask_sz or Decimal("1")
            ahead = ZERO
            for row in self._book_depth_asks:
                p, sz = _D(row[0]), _D(row[1])
                if p <= price:
                    ahead += sz
                else:
                    break
            return ahead if ahead > ZERO else (self.ask_sz or Decimal("1"))

    def consumption_rate(self, side: str, window_s: float, now: float) -> Decimal:
        """Aggressive trade volume hitting the given book side per second."""
        vol = ZERO
        target_trade_side = "SELL" if side in ("BUY", "BID") else "BUY"
        for t, s, sz, _ in reversed(self._trades):
            if now - t > window_s:
                break
            if s == target_trade_side:
                vol += sz
        return vol / Decimal(str(max(0.1, window_s)))

    def liquidity_fragility(self, side: str, now: float) -> Decimal:
        """Ratio of aggressive counter-flow volume (1s) to available resting depth (L1-L5).
        A fragility value > 0.5 indicates resting liquidity is being consumed rapidly (imminent level sweep)."""
        cons = self.consumption_rate(side, 1.0, now)
        depth_list = self._book_depth_bids[:5] if side in ("BUY", "BID") else self._book_depth_asks[:5]
        depth = sum(_D(r[1]) for r in depth_list) if depth_list else ZERO
        if depth <= ZERO:
            depth = (self.bid_sz if side in ("BUY", "BID") else self.ask_sz) or Decimal("1")
        return cons / max(Decimal("0.01"), depth)

    def multi_depth_obi(self, levels: int = 5) -> Decimal:
        """Volume-weighted order book imbalance across the top N book levels."""
        if not self._book_depth_bids or not self._book_depth_asks:
            return self.obi
        bid_v = sum(_D(r[1]) for r in self._book_depth_bids[:levels])
        ask_v = sum(_D(r[1]) for r in self._book_depth_asks[:levels])
        tot = bid_v + ask_v
        if tot <= ZERO:
            return ZERO
        return (bid_v - ask_v) / tot

    def trade_flow_acceleration(self, now: float) -> Decimal:
        """Measures change in aggressive order flow: TFI(1s) - TFI(5s).
        Positive when buying is accelerating; negative when selling is accelerating."""
        tfi_1s = self.trade_flow_imbalance(1.0, now)
        tfi_5s = self.trade_flow_imbalance(5.0, now)
        return tfi_1s - tfi_5s

    def is_exhaustion(self, side: str, now: float) -> bool:
        """Detects whether aggressive flow on this side has exhausted / decelerated,
        signaling a safe mean-reversion liquidity provision opportunity."""
        if not getattr(self.cfg, "enable_absorption_mode", True):
            return False
        accel = self.trade_flow_acceleration(now)
        ret = self.ret_bps(3.0, now)
        if side in ("BUY", "BID"):
            # Sell flow was heavy but is decelerating (accel > 0) and price drop has stalled
            tfi_10s = self.trade_flow_imbalance(10.0, now)
            return bool(tfi_10s < Decimal("-0.30") and accel > Decimal("0.30") and ret > Decimal("-0.8"))
        else:
            # Buy flow was heavy but is decelerating (accel < 0) and price rise has stalled
            tfi_10s = self.trade_flow_imbalance(10.0, now)
            return bool(tfi_10s > Decimal("0.30") and accel < Decimal("-0.30") and ret < Decimal("0.8"))

    def reference_basis_bps(self) -> Decimal:
        """Basis in basis points between current perp mid and external reference / mark price."""
        if not self.mid or not self.info or not self.info.mark or self.info.mark <= ZERO:
            return ZERO
        return (self.mid - self.info.mark) / self.info.mark * BPS

    def tfi_horizon(self, horizon_s: float, now: float) -> Decimal:
        return self.trade_flow_imbalance(horizon_s, now)

    def trade_rates(self, window_s: float, now: float) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
        """Returns (buy_qty_per_s, sell_qty_per_s, buy_usd_per_s, sell_usd_per_s)."""
        buy_qty, sell_qty = ZERO, ZERO
        buy_usd, sell_usd = ZERO, ZERO
        for t, s, sz, px in reversed(self._trades):
            if now - t > window_s:
                break
            if s in ("BUY", "BID"):
                buy_qty += sz
                buy_usd += sz * px
            else:
                sell_qty += sz
                sell_usd += sz * px
        dt = Decimal(str(max(0.1, window_s)))
        return (buy_qty / dt, sell_qty / dt, buy_usd / dt, sell_usd / dt)

    def spread_ticks(self, m: Optional[Market] = None) -> Decimal:
        if not self.bid or not self.ask or not self.mid:
            return ZERO
        tick = m.tick_for(self.mid) if m else Decimal("0.1")
        return (self.ask - self.bid) / tick if tick > ZERO else ZERO

    def micro_spread_bps(self) -> Decimal:
        if not self.micro or not self.mid or self.mid <= ZERO:
            return ZERO
        return (self.micro - self.mid) / self.mid * BPS

    def depth_concentration(self, side: str, levels: int = 5) -> Decimal:
        depth_list = self._book_depth_bids[:levels] if side in ("BUY", "BID") else self._book_depth_asks[:levels]
        if not depth_list:
            return Decimal("1.0")
        l0_sz = _D(depth_list[0][1])
        tot_sz = sum(_D(r[1]) for r in depth_list)
        return (l0_sz / tot_sz) if tot_sz > ZERO else Decimal("1.0")

    def get_microstructure_snapshot(self, m: Optional[Market], now: float) -> dict:
        """Gathers complete high-frequency microstructure feature set for models and dataset logging."""
        b_rate, s_rate, b_usd, s_usd = self.trade_rates(1.0, now)
        return {
            "ts": now,
            "bid": str(self.bid or "0"),
            "ask": str(self.ask or "0"),
            "mid": str(self.mid or "0"),
            "spread_bps": float(self.spread_bps),
            "spread_ticks": float(self.spread_ticks(m)),
            "micro": str(self.micro or "0"),
            "micro_spread_bps": float(self.micro_spread_bps()),
            "obi_l1": float(self.obi),
            "obi_l5": float(self.multi_depth_obi(5)),
            "obi_l10": float(self.multi_depth_obi(10)),
            "tfi_250ms": float(self.tfi_horizon(0.25, now)),
            "tfi_500ms": float(self.tfi_horizon(0.50, now)),
            "tfi_1s": float(self.tfi_horizon(1.0, now)),
            "tfi_2s": float(self.tfi_horizon(2.0, now)),
            "tfi_5s": float(self.tfi_horizon(5.0, now)),
            "tfi_10s": float(self.tfi_horizon(10.0, now)),
            "tfi_accel": float(self.trade_flow_acceleration(now)),
            "buy_rate_usd": float(b_usd),
            "sell_rate_usd": float(s_usd),
            "vol_bps": float(self.vol_bps),
            "bid_fragility": float(self.liquidity_fragility("BUY", now)),
            "ask_fragility": float(self.liquidity_fragility("SELL", now)),
            "bid_exhaustion": self.is_exhaustion("BUY", now),
            "ask_exhaustion": self.is_exhaustion("SELL", now),
            "basis_bps": float(self.reference_basis_bps()),
            "funding_rate": float(self.funding_rate),
        }
