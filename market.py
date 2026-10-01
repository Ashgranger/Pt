"""Market metadata and live state + Level 5 Intelligence."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, List, Tuple

from utils import BPS, ZERO, ONE, clamp


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

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal("2")

    @property
    def obi(self) -> Decimal:
        total = self.bid_sz + self.ask_sz
        if total <= 0:
            return ZERO
        return (self.bid_sz - self.ask_sz) / total


class CrossVenueTracker:
    """Tracks cross-exchange market states (Binance, Bybit, OKX, Coinbase, etc.)
    and generates lead/lag, price velocity, dispersion, and cross-venue OBI features.
    """
    def __init__(self):
        self.venues: dict[str, VenueState] = {}
        self._history: deque = deque()

    def update_venue(self, venue: str, bid: Decimal, ask: Decimal,
                     bid_sz: Decimal, ask_sz: Decimal, now: float) -> None:
        st = VenueState(venue, bid, ask, bid_sz, ask_sz, now)
        self.venues[venue] = st
        self._history.append((now, venue, st.mid))
        while self._history and now - self._history[0][0] > 60.0:
            self._history.popleft()

    def prune_stale(self, now: float, max_age_s: float) -> None:
        stale = [name for name, state in self.venues.items() if now - state.ts > max_age_s]
        for name in stale:
            self.venues.pop(name, None)

    def cross_fair_value(self) -> Optional[Decimal]:
        if not self.venues:
            return None
        mids = [v.mid for v in self.venues.values() if v.mid > ZERO]
        if not mids:
            return None
        return sum(mids) / Decimal(str(len(mids)))

    def cross_velocity_bps(self, window_s: float, now: float) -> Decimal:
        """Rate of change of the external benchmark mid price over window_s."""
        if not self._history:
            return ZERO
        recent = [m for t, v, m in self._history if now - t <= window_s]
        if len(recent) < 2 or recent[0] == ZERO:
            return ZERO
        return (recent[-1] - recent[0]) / recent[0] * BPS

    def cross_dispersion_bps(self) -> Decimal:
        """Measures price disagreement across venues in bps (max mid - min mid / avg mid)."""
        if len(self.venues) < 2:
            return ZERO
        mids = [v.mid for v in self.venues.values() if v.mid > ZERO]
        if len(mids) < 2:
            return ZERO
        avg_mid = sum(mids) / Decimal(str(len(mids)))
        if avg_mid <= ZERO:
            return ZERO
        return (max(mids) - min(mids)) / avg_mid * BPS

    def cross_obi(self) -> Decimal:
        """Consolidated average order book imbalance across external venues."""
        if not self.venues:
            return ZERO
        obis = [v.obi for v in self.venues.values()]
        return sum(obis) / Decimal(str(len(obis)))

    def lead_lag_divergence_bps(self, local_mid: Optional[Decimal]) -> Decimal:
        """Divergence between external benchmark fair value and local mid price."""
        if local_mid is None or local_mid <= ZERO:
            return ZERO
        cf = self.cross_fair_value()
        if cf is None:
            return ZERO
        return (cf - local_mid) / local_mid * BPS


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
        self._vol_ewma = ZERO
        self._book_depth_bids: list = []
        self._book_depth_asks: list = []
        self.cross = CrossVenueTracker()
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
        if bid < ask:
            self._hist.append((now, mid))
            while self._hist and now - self._hist[0][0] > self.HISTORY_S:
                self._hist.popleft()
            move = self.move_bps(self.cfg.vol_window_s, now)
            self._vol_ewma = move if self._vol_ewma == 0 else self._vol_ewma * Decimal("0.9") + move * Decimal("0.1")

    def on_trade(self, side: str, size: Decimal, price: Decimal, now: float) -> None:
        self._trades.append((now, side.upper(), size, price))
        while self._trades and now - self._trades[0][0] > 60.0:
            self._trades.popleft()

    def on_depth(self, bids: list, asks: list, now: float) -> None:
        self._book_depth_bids = bids
        self._book_depth_asks = asks

    def update_cross_venue(self, venue: str, bid: Decimal, ask: Decimal,
                           bid_sz: Decimal, ask_sz: Decimal, now: float) -> None:
        self.cross.update_venue(venue, bid, ask, bid_sz, ask_sz, now)

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
        buy_vol = ZERO
        sell_vol = ZERO
        for t, side, sz, _ in self._trades:
            if now - t <= window_s:
                if side in ("BUY", "BID"):
                    buy_vol += sz
                else:
                    sell_vol += sz
        total = buy_vol + sell_vol
        if total <= 0:
            return ZERO
        return (buy_vol - sell_vol) / total

    @property
    def spread_bps(self) -> Decimal:
        m = self.mid
        return (self.ask - self.bid) / m * BPS if m else ZERO

    def jump_active(self, now: float) -> bool:
        return now < self.jump_until

    def _window(self, window_s: float, now: float):
        return [m for t, m in self._hist if now - t <= window_s]

    def ret_bps(self, window_s: float, now: float) -> Decimal:
        w = self._window(window_s, now)
        if len(w) < 2 or w[0] == 0:
            return ZERO
        return (w[-1] - w[0]) / w[0] * BPS

    def move_bps(self, window_s: float, now: float) -> Decimal:
        w = self._window(window_s, now)
        if len(w) < 2 or w[0] == 0:
            return ZERO
        return (max(w) - min(w)) / w[-1] * BPS

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
                p, sz = Decimal(str(row[0])), Decimal(str(row[1]))
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
                p, sz = Decimal(str(row[0])), Decimal(str(row[1]))
                if p <= price:
                    ahead += sz
                else:
                    break
            return ahead if ahead > ZERO else (self.ask_sz or Decimal("1"))

    def consumption_rate(self, side: str, window_s: float, now: float) -> Decimal:
        """Aggressive trade volume hitting the given book side per second."""
        vol = ZERO
        target_trade_side = "SELL" if side in ("BUY", "BID") else "BUY"
        for t, s, sz, _ in self._trades:
            if now - t <= window_s:
                if s == target_trade_side:
                    vol += sz
        return vol / Decimal(str(max(0.1, window_s)))

    def liquidity_fragility(self, side: str, now: float) -> Decimal:
        """Ratio of aggressive counter-flow volume (1s) to available resting depth (L1-L5).
        A fragility value > 0.5 indicates resting liquidity is being consumed rapidly (imminent level sweep)."""
        cons = self.consumption_rate(side, 1.0, now)
        depth_list = self._book_depth_bids[:5] if side in ("BUY", "BID") else self._book_depth_asks[:5]
        depth = sum(Decimal(str(r[1])) for r in depth_list) if depth_list else ZERO
        if depth <= ZERO:
            depth = (self.bid_sz if side in ("BUY", "BID") else self.ask_sz) or Decimal("1")
        return cons / max(Decimal("0.01"), depth)

    def multi_depth_obi(self, levels: int = 5) -> Decimal:
        """Volume-weighted order book imbalance across the top N book levels."""
        if not self._book_depth_bids or not self._book_depth_asks:
            return self.obi
        bid_v = sum(Decimal(str(r[1])) for r in self._book_depth_bids[:levels])
        ask_v = sum(Decimal(str(r[1])) for r in self._book_depth_asks[:levels])
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
        for t, s, sz, px in self._trades:
            if now - t <= window_s:
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
        l0_sz = Decimal(str(depth_list[0][1]))
        tot_sz = sum(Decimal(str(r[1])) for r in depth_list)
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
