"""Order book of *our own* orders with Multi-Pair Individual Order Management."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Optional, Dict, Tuple, List

from exchange import Exchange
from market import Market
from signer import Signer
from utils import BUY, SELL, fmt, bps_diff, q_down, q_up

log = logging.getLogger("orders")
GTT_DAYS = 40


@dataclass
class Order:
    order_id: str
    pair_index: int
    side: str
    price: Decimal
    qty: Decimal
    remaining: Decimal
    good_til_us: int
    created: float
    last_action: float
    filled_any: bool = False
    cancelling_since: Optional[float] = None
    is_taker: bool = False
    is_reduce_only: bool = False
    ev_bps: Decimal = Decimal(0)
    quote_mid: Optional[Decimal] = None
    est_px: Optional[Decimal] = None


class OrderManager:
    def __init__(self, cfg, ex: Exchange, signer: Signer, get_market: Callable[[], Market],
                 on_fill: Callable[[str, Decimal, Decimal, Order], None]):
        self.cfg, self.ex, self.signer = cfg, ex, signer
        self.get_market = get_market
        self.on_fill = on_fill
        
        self.orders: dict[str, Order] = {}
        self._last_taker: dict = {}
        self.pair_slots: dict[Tuple[int, str], str] = {}
        self._unmatched: dict[str, tuple] = {}
        self.reject_until = {BUY: 0.0, SELL: 0.0}
        self._reject_n = {BUY: 0, SELL: 0}
        self._actions: deque = deque()
        self.paused_until = 0.0
        self._recently_closed: deque = deque(maxlen=200)
        self._consec_errors = 0
        self.last_place_ts = 0.0
        self.maybe_orders = True
        self.n_place = self.n_modify = self.n_cancel = self.n_reject = self.n_actions = 0

    def side_orders(self, side: str) -> list[Order]:
        return sorted((o for o in self.orders.values() if o.side == side), key=lambda o: o.price, reverse=(side == BUY))

    def get_order_by_slot(self, pair_index: int, side: str) -> Optional[Order]:
        oid = self.pair_slots.get((pair_index, side))
        return self.orders.get(oid) if oid else None

    def open_qty(self, side: str) -> Decimal:
        return sum((o.remaining for o in self.orders.values() if o.side == side), Decimal(0))

    def describe(self, now: float) -> str:
        if not self.orders:
            return "none"
        desc = []
        for (pair_idx, side), oid in sorted(self.pair_slots.items()):
            o = self.orders.get(oid)
            if o:
                desc.append(f"L{pair_idx}{'B' if side == BUY else 'S'} {fmt(o.remaining)}@{fmt(o.price)}")
        return " | ".join(desc) if desc else "none"

    def _budget(self, now: float) -> bool:
        while self._actions and now - self._actions[0] > 60:
            self._actions.popleft()
        if len(self._actions) >= self.cfg.max_actions_per_min:
            return False
        self._actions.append(now)
        self.n_actions += 1
        return True

    @staticmethod
    def _ok(resp: dict) -> bool:
        return resp.get("status") in (200, 202) and "error" not in resp

    def _error(self, what: str, resp: dict, now: float) -> None:
        err = resp.get("error")
        log.warning("%s failed: status=%s %s", what, resp.get("status"), json.dumps(err)[:300])
        self._consec_errors += 1
        retry = err.get("retryAfterMs") if isinstance(err, dict) else None
        retry = retry or resp.get("retryAfterMs")
        if resp.get("status") == 429 and retry:
            self.paused_until = now + float(retry) / 1000 + 0.1
        if self._consec_errors >= 8:
            log.error("too many consecutive errors - pausing 30s")
            self.paused_until = now + 30
            self._consec_errors = 0

    def _backoff(self, side: str, now: float) -> None:
        self._reject_n[side] += 1
        self.reject_until[side] = now + min(0.25 * 2 ** (self._reject_n[side] - 1), 4.0)

    async def place(self, pair_index: int, side: str, px: Decimal, qty: Decimal, now: float,
                    time_in_force: str = "ALO", reduce_only: bool = False,
                    quote_mid: Optional[Decimal] = None,
                    est_px: Optional[Decimal] = None) -> Optional[Order]:
        if now < self.paused_until or not self._budget(now):
            return None
        m = self.get_market()
        tick = m.tick_for(px) if hasattr(m, "tick_for") else m.tick
        is_taker = (time_in_force == "IOC")
        if is_taker:
            px = q_up(px, tick) if side == BUY else q_down(px, tick)
        else:
            px = q_down(px, tick) if side == BUY else q_up(px, tick)
        qty = q_down(qty, m.step)
        if qty < m.min_size or (m.min_notional > 0 and px * qty < m.min_notional):
            return None
        good_til = int(time.time() * 1_000_000) + GTT_DAYS * 86_400 * 1_000_000
        req = self.signer.place(m, side, px, qty, good_til,
                                time_in_force=time_in_force, reduce_only=reduce_only)
        self.maybe_orders = True
        self.last_place_ts = now
        resp = await self.ex.write(req)
        res = resp.get("result") or {}
        if not self._ok(resp) or not res.get("orderId") or str(res.get("status")).upper() == "REJECTED":
            self._error(f"place L{pair_index} {side}", resp if not self._ok(resp) else
                        {"status": resp.get("status"), "error": res}, now)
            if not is_taker:
                self._backoff(side, now)
            return None
        self._consec_errors = 0
        self.n_place += 1
        oid = str(res["orderId"])
        o = Order(oid, pair_index, side, px, qty, qty, good_til, now, now,
                  is_taker=is_taker, is_reduce_only=reduce_only, quote_mid=quote_mid, est_px=est_px)
        self.orders[oid] = o
        if not is_taker:
            self.pair_slots[(pair_index, side)] = oid
        log.info("PLACE L%d %s %s @ %s (taker=%s)", pair_index, side, fmt(qty), fmt(px), is_taker)
        early = self._unmatched.pop(oid, None)
        if early:
            self._apply(o, early[1], now)
        return o

    async def modify(self, o: Order, px: Decimal, now: float, urgent: bool = False,
                     reduce_only: Optional[bool] = None) -> bool:
        if now < self.paused_until or not self._budget(now):
            if urgent:
                await self.cancel(o, now)
            return False
        m = self.get_market()
        tick = m.tick_for(px) if hasattr(m, "tick_for") else m.tick
        px = q_down(px, tick) if o.side == BUY else q_up(px, tick)
        r_only = o.is_reduce_only if reduce_only is None else reduce_only
        req = self.signer.modify(m, o.order_id, o.side, px, o.qty, o.good_til_us, reduce_only=r_only)
        resp = await self.ex.write(req)
        if not self._ok(resp):
            self._error(f"modify L{o.pair_index} {o.side}", resp, now)
            await self.cancel(o, now)
            return False
        self._consec_errors = 0
        self.n_modify += 1
        log.info("MODIFY L%d %s %s -> %s (reduce_only=%s)", o.pair_index, o.side, fmt(o.price), fmt(px), r_only)
        o.price, o.last_action, o.is_reduce_only = px, now, r_only
        return True

    async def cancel(self, o: Order, now: float) -> None:
        if o.cancelling_since is not None and now - o.cancelling_since < 5:
            return
        o.cancelling_since = now
        self._recently_closed.append((now, o.order_id))
        self._budget(now)
        resp = await self.ex.write(self.signer.cancel(self.get_market(), o.order_id))
        if self.cfg.dry_run or "ORDER_NOT_FOUND" in json.dumps(resp):
            self._remove_order(o.order_id)
        elif not self._ok(resp):
            o.cancelling_since = None
            self._error(f"cancel L{o.pair_index} {o.side}", resp, now)
        else:
            self.n_cancel += 1

    def _remove_order(self, order_id: str) -> None:
        o = self.orders.pop(order_id, None)
        if o:
            self._recently_closed.append((time.time(), order_id))
            slot = (o.pair_index, o.side)
            if self.pair_slots.get(slot) == order_id:
                self.pair_slots.pop(slot, None)

    async def _gather(self, coros) -> None:
        coros = list(coros)
        if not coros:
            return
        if len(coros) == 1:
            await coros[0]
            return
        for r in await asyncio.gather(*coros, return_exceptions=True):
            if isinstance(r, asyncio.CancelledError):
                raise r
            if isinstance(r, Exception):
                log.warning("parallel order action failed: %r", r)

    async def cancel_side(self, side: str, now: float) -> None:
        coros = []
        for slot, oid in list(self.pair_slots.items()):
            if slot[1] == side:
                o = self.orders.get(oid)
                if o:
                    coros.append(self.cancel(o, now))
        await self._gather(coros)

    async def cancel_all(self, force: bool = False) -> None:
        if not force and not self.orders and not self.maybe_orders:
            return
        m = self.get_market()
        now = time.time()
        await self._gather([self.cancel(o, now) for o in list(self.orders.values())])

        if (force or self.maybe_orders) and not self.cfg.dry_run and getattr(self.ex, "is_connected", False):
            try:
                res = await self.ex.get("orders", {"address": self.cfg.address, "accountIndex": self.cfg.account_index,
                                                    "marketId": m.market_id})
                if res and "openOrders" in res:
                    reqs = []
                    for r in res["openOrders"]:
                        if isinstance(r, dict):
                            r_mkt = r.get("marketId")
                            if r_mkt is not None and int(r_mkt) != m.market_id:
                                continue
                            oid = str(r.get("orderId") or r.get("id"))
                            if oid and oid != "None" and oid not in self.orders:
                                reqs.append(self.ex.write(self.signer.cancel(m, oid)))
                    await self._gather(reqs)
            except Exception:
                pass

        self.orders.clear()
        self.pair_slots.clear()
        self.maybe_orders = False

    async def sync_quotes(self, targets: list, now: float, blocked_sides: Optional[set] = None) -> None:
        active_slots = set()
        groups: dict = {}
        for t in targets:
            slot = (t.pair_index, t.side)
            groups.setdefault(slot, []).append(t)
            if not getattr(t, "is_taker", False):
                active_slots.add(slot)

        async def run_group(ts: list) -> None:
            for t in ts:  # same-slot targets stay strictly sequential
                await self._sync_one(t, now)

        coros = [run_group(ts) for ts in groups.values()]
        for slot, oid in list(self.pair_slots.items()):
            if slot not in active_slots:
                o = self.orders.get(oid)
                if o:
                    coros.append(self.cancel(o, now))
        await self._gather(coros)

    async def _sync_one(self, t, now: float) -> None:
        slot = (t.pair_index, t.side)
        existing = self.get_order_by_slot(t.pair_index, t.side)

        if getattr(t, "is_taker", False):
            last = self._last_taker.get(t.side)
            if last is not None and now - last < 1.5:
                return            # previous IOC still settling: never double-send a taker
            self._last_taker[t.side] = now
            if existing:
                await self.cancel(existing, now)
                self.pair_slots.pop(slot, None)
            await self.place(t.pair_index, t.side, t.price, t.qty, now,
                             time_in_force="IOC", reduce_only=True,
                             quote_mid=getattr(t, "quote_mid", None),
                             est_px=getattr(t, "est_px", None))
            return

        is_exit = bool(getattr(t, "is_exit_quote", False))
        if existing is None:
            if now >= self.reject_until[t.side]:
                o_new = await self.place(t.pair_index, t.side, t.price, t.qty, now,
                                         time_in_force="ALO", reduce_only=is_exit,
                                         quote_mid=getattr(t, "quote_mid", None))
                if o_new:
                    o_new.ev_bps = getattr(t, "expected_value_bps", Decimal(0))
            return

        drift = abs(bps_diff(t.price, existing.price))
        is_advancing = (t.side == BUY and t.price > existing.price) or (t.side == SELL and t.price < existing.price)
        is_retreating = not is_advancing

        should_modify = False
        urgent = False

        if getattr(existing, "is_reduce_only", False) != is_exit:
            should_modify = True
            urgent = True

        m = self.get_market()
        tick_bps = (m.tick / existing.price) * Decimal("10000") if existing.price > 0 else Decimal("0.1")
        eff_retreat = min(self.cfg.retreat_bps, tick_bps * Decimal("0.9"))
        eff_requote = min(self.cfg.requote_bps, tick_bps * Decimal("0.9"))

        if is_retreating and (drift >= eff_retreat or abs(t.price - existing.price) >= m.tick):
            should_modify = True
            urgent = True
        elif is_advancing and (drift >= eff_requote or abs(t.price - existing.price) >= m.tick) and (now - existing.last_action >= (getattr(self.cfg, "touch_min_requote_s", self.cfg.min_requote_s) if existing.pair_index == 0 else self.cfg.min_requote_s)):
            queue_reset_cost = getattr(self.cfg, "queue_reset_cost_bps", Decimal("0.20"))
            ev_gain = getattr(t, "expected_value_bps", Decimal(0)) - getattr(existing, "ev_bps", Decimal(0))
            if ev_gain >= queue_reset_cost or drift >= (self.cfg.requote_bps * Decimal("1.5")) or abs(t.price - existing.price) >= m.tick:
                should_modify = True

        if should_modify:
            if await self.modify(existing, t.price, now, urgent=urgent, reduce_only=is_exit):
                existing.ev_bps = getattr(t, "expected_value_bps", Decimal(0))
                existing.quote_mid = getattr(t, "quote_mid", None)

    def on_update(self, c, now: float) -> None:
        if not isinstance(c, dict) or not c.get("orderId"):
            return
        mkt_id = c.get("marketId")
        if mkt_id is not None:
            try:
                if int(mkt_id) != self.get_market().market_id:
                    return
            except Exception:
                pass
        oid = str(c["orderId"])
        o = self.orders.get(oid)
        if o is None:
            self._unmatched[oid] = (now, c)
            if len(self._unmatched) > 200:
                self._unmatched = {k: v for k, v in self._unmatched.items() if v[0] > now - 30}
            return
        self._apply(o, c, now)

    def _apply(self, o: Order, c: dict, now: float) -> None:
        if c.get("cancelReason") == "MODIFY_CANCELED":
            return
        state = str(c.get("state") or "").upper()
        status = str(c.get("status") or "").upper()
        filled = (state == "FILLED" or status == "FILLED")
        px = o.price
        try:
            if c.get("price"):
                px = Decimal(str(c["price"]))
            # takers: record the real execution price when the exchange reports it, not our limit
            for k in ("avgPrice", "averagePrice", "avgFillPrice", "lastFillPrice", "fillPrice"):
                v = c.get(k)
                if v and Decimal(str(v)) > 0:
                    px = Decimal(str(v))
                    break
        except Exception:
            pass
        if o.is_taker and (filled or state == "PARTIALLY_FILLED"):
            real = any(c.get(k) and Decimal(str(c[k])) > 0
                       for k in ("avgPrice", "averagePrice", "avgFillPrice", "lastFillPrice", "fillPrice"))
            # Always dump the raw update once per taker event so the true execution-price field can be confirmed.
            log.info("TAKER_RAW limit=%s est=%s real_px_field=%s raw=%s", fmt(o.price), fmt(o.est_px) if o.est_px else "-",
                     real, json.dumps(c, default=str)[:500])
            if not real and o.est_px and getattr(self.cfg, "taker_fill_price_mode", "est") == "est":
                px = o.est_px   # exchange sent no execution price: book the book-walk estimate, NOT our far-through limit
        fill_qty = Decimal(0)
        rem = c.get("remainingSize")
        if rem is not None:
            try:
                new_rem = Decimal(str(rem))
                if new_rem < o.remaining:
                    fill_qty = o.remaining - new_rem
                o.remaining = new_rem
            except Exception:
                rem = None
        if filled and rem is None:
            fill_qty, o.remaining = o.remaining, Decimal(0)
        if fill_qty > 0:
            o.filled_any = True
            self.on_fill(o.side, fill_qty, px, o)
        if state == "OPEN" or status == "OPEN":
            self._reject_n[o.side] = 0

        # IOC terminal handling per Arcus documentation
        is_ioc_done = o.is_taker and (state in ("PARTIALLY_FILLED", "FILLED", "CANCELED", "EXPIRED", "REJECTED") or
                                     status in ("PARTIALLY_FILLED", "FILLED", "CANCELED", "MARGIN_CANCELED", "REJECTED") or
                                     o.remaining == Decimal(0))
        if filled or is_ioc_done:
            self._remove_order(o.order_id)
        elif state in ("CANCELED", "REJECTED") or status in ("CANCELED", "MARGIN_CANCELED", "REJECTED"):
            reason = c.get("rejectionReason") or c.get("cancelReason") or ""
            if (state == "REJECTED" or status == "REJECTED") and not o.is_taker:
                self.n_reject += 1
                self._backoff(o.side, now)
            log.info("ORDER L%d %s %s %s", o.pair_index, o.side, state or status, reason)
            self._remove_order(o.order_id)

    async def reconcile(self, rows: list, now: float) -> None:
        m = self.get_market()
        market_rows = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            r_mkt = r.get("marketId")
            if r_mkt is not None:
                try:
                    if int(r_mkt) != m.market_id:
                        continue
                except (ValueError, TypeError):
                    pass
            market_rows.append(r)

        open_ids = {str(r.get("orderId") or r.get("id")) for r in market_rows}
        for o in list(self.orders.values()):
            if o.order_id not in open_ids and now - o.last_action > 5 and o.cancelling_since is None:
                log.warning("dropping ghost L%d %s order %s", o.pair_index, o.side, o.order_id)
                self._remove_order(o.order_id)
        mine = set(self.orders)
        if now - self.last_place_ts < 3:
            return
        recent_closed = {oid for ts, oid in self._recently_closed if now - ts < 15.0}
        reqs = []
        for oid in (open_ids - mine - recent_closed):
            if oid and oid != "None":
                self._recently_closed.append((now, oid))
                log.warning("cancelling orphan order %s for market %s (id %d)", oid, m.name, m.market_id)
                reqs.append(self.ex.write(self.signer.cancel(m, oid)))
        await self._gather(reqs)
