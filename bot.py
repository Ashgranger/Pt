"""Level 7 Market Maker for Arcus Perpetuals."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from decimal import Decimal
from typing import Any, Optional

from config import Config
from exchange import Exchange
from feeds import CrossFeedManager
from market import Market, MarketData
from signer import Signer
from ledger import Ledger, Fill
from engine import MarketMakingEngine, QuoteTarget
from orders import OrderManager, Order
from utils import BPS, BUY, SELL, ZERO, ONE, Fatal, fmt

log = logging.getLogger("bot")


def extract_positions(c: Any) -> list:
    if isinstance(c, list):
        return [r for r in c if isinstance(r, dict)]
    if isinstance(c, dict):
        if "positions" in c:
            p = c["positions"]
            if isinstance(p, dict):
                return [r for r in p.values() if isinstance(r, dict)]
            if isinstance(p, list):
                return [r for r in p if isinstance(r, dict)]
            return []
        if "marketId" in c:
            return [c]
    return []


from datetime import datetime as _dt
try:
    from zoneinfo import ZoneInfo as _ZI
    _ET = _ZI("America/New_York")
except Exception:  # pragma: no cover
    _ET = None


def et_hhmmss(ts: float) -> str:
    """US Eastern wall-clock for a unix timestamp (handles EST/EDT)."""
    try:
        return _dt.fromtimestamp(ts, _ET).strftime("%H:%M:%S") if _ET else ""
    except Exception:
        return ""


def in_et_windows(spec: str, ts: float) -> bool:
    """spec like '09:30-09:50,15:50-16:00' (ET). Empty = never."""
    if not spec or not _ET:
        return False
    cur = _dt.fromtimestamp(ts, _ET)
    mins = cur.hour * 60 + cur.minute
    for part in spec.split(","):
        try:
            a, z = part.strip().split("-")
            ah, am = a.split(":"); zh, zm = z.split(":")
            a_m, z_m = int(ah) * 60 + int(am), int(zh) * 60 + int(zm)
            if (a_m <= mins < z_m) if a_m <= z_m else (mins >= a_m or mins < z_m):   # a>z = wraps midnight
                return True
        except Exception:
            continue
    return False


class MarketMaker:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.now = time.monotonic
        self.stop_evt = asyncio.Event()
        self.restart_requested = False
        self._pos_seq = -1
        self._resync_req = False
        self._last_pos_resync = 0.0
        self._ro_n = 0
        self._ro_first = 0.0
        self._ro_fill_ts = None
        self._ro_fix_ts = 0.0
        self._ro_stage = 0
        self._idle_since = None
        self._tick_quote_path = False
        self._tick_paused_cfg = False
        self._last_blocked = (False, False)
        self._last_targets_n = 0

        self.ex = Exchange(cfg, self._on_channel)
        self.md = MarketData(cfg)
        self.signer = Signer(cfg.signing_key, cfg.address, cfg.account_index)
        self.ledger = Ledger(cfg)
        self.ledger.on_markout_cb = self._journal_markout
        self.md.own_provider = self._own_resting
        self._loss_pause_until = 0.0
        self._pnl_base = ZERO
        self.engine = MarketMakingEngine(cfg)
        self.om = OrderManager(cfg, self.ex, self.signer, self._get_market, self._on_fill)
        self.om.on_reduce_only_reject = self._on_reduce_only_reject
        self.ex.on_degraded = lambda: setattr(self, '_resync_req', True)

        self._recent_fills: deque = deque()
        self._burst_blocked_until = {BUY: 0.0, SELL: 0.0}
        self._trend_blocked_until = {BUY: 0.0, SELL: 0.0}

        self._last_heartbeat = 0.0
        self._last_reconcile = 0.0
        self._last_pause_log = {"rth": 0.0, "spread": 0.0, "oracle": 0.0, "jump": 0.0}
        self._last_status = 0.0
        self._last_info_fetch = 0.0
        self._last_logged_realized: Decimal = ZERO
        self._tick_lock = asyncio.Lock()
        self._dirty_evt = asyncio.Event()
        self._bg_tasks: dict = {}
        self._last_ext_wake = 0.0
        self._cross_feeds = None
        self._last_cross_log = 0.0
        self._dms_armed = False
        self._dms_fail = 0
        self._dms_ok_until = 0.0
        self._dms_blocked_until = 0.0
        self._files: dict = {}
        self._tick_on_trades = os.getenv("TICK_ON_TRADES", "1").strip().lower() in ("1", "true", "yes", "on")

    def _get_market(self) -> Market:
        if not self.md.info:
            raise Fatal("Market metadata not yet loaded")
        return self.md.info

    def _on_channel(self, channel: str, contents: Any, is_snapshot: bool) -> None:
        now = self.now()
        if channel == "bbo":
            if not isinstance(contents, dict):
                return
            bb, ba = contents.get("bestBid"), contents.get("bestAsk")
            if not bb or not ba:
                return
            try:
                bid = Decimal(str(bb["price"]))
                ask = Decimal(str(ba["price"]))
                bid_sz = Decimal(str(bb["size"])) if "size" in bb else None
                ask_sz = Decimal(str(ba["size"])) if "size" in ba else None
                self.md.update(bid, ask, bid_sz, ask_sz, now)
                if self.md.mid:
                    self.ledger.process_markouts(self.md.mid, now)
                self._dirty_evt.set()
            except Exception:
                pass

        elif channel == "trades":
            if isinstance(contents, list):
                for tr in contents:
                    self._handle_trade(tr, now)
            elif isinstance(contents, dict):
                self._handle_trade(contents, now)
            if self._tick_on_trades:
                self._dirty_evt.set()

        elif channel in ("l2Orderbook", "l2OrderbookUpdates", "orderBook"):
            if isinstance(contents, dict):
                bids = contents.get("bids") or []
                asks = contents.get("asks") or []
                self.md.on_depth(bids, asks, now)

        elif channel in ("external_bbo", "cross_venue"):
            if isinstance(contents, dict):
                venue = contents.get("venue", "EXTERNAL")
                bid = Decimal(str(contents["bid"]))
                ask = Decimal(str(contents["ask"]))
                bid_sz = Decimal(str(contents.get("bid_size", "1")))
                ask_sz = Decimal(str(contents.get("ask_size", "1")))
                self.md.update_cross_venue(venue, bid, ask, bid_sz, ask_sz, now)
                self._dirty_evt.set()

        elif channel == "orders":
            if isinstance(contents, list):
                for row in contents:
                    self.om.on_update(row, now)
            elif isinstance(contents, dict):
                self.om.on_update(contents, now)
            self._dirty_evt.set()

        elif channel == "positions":
            m = self.md.info
            if m:
                target_mid = self.md.mid or m.mark
                rows = [r for r in extract_positions(contents) if int(r.get("marketId", -1)) == m.market_id]
                seq_in = contents.get("lastSequenceId") if isinstance(contents, dict) else None
                try:
                    seq_in = int(seq_in) if seq_in is not None else None
                except (TypeError, ValueError):
                    seq_in = None
                if is_snapshot:
                    self._pos_seq = seq_in if seq_in is not None else -1       # snapshot = new baseline
                elif seq_in is not None:
                    if seq_in < self._pos_seq:
                        return                                                  # stale/out-of-order delta
                    self._pos_seq = seq_in
                if rows:
                    r = rows[0]
                    side = str(r.get("side", "FLAT")).upper()
                    sz = Decimal(str(r.get("size", "0")))
                    # size is already SIGNED (+long/-short) on Arcus; abs() by side makes this right for either form
                    # (it used to be negated twice, booking every short as a long).
                    ex_pos = abs(sz) if side in ("LONG", "BUY") else (-abs(sz) if side in ("SHORT", "SELL") else ZERO)
                elif is_snapshot:
                    ex_pos = ZERO        # a snapshot omits flat markets: no row == flat (this never cleared stale longs before)
                else:
                    return
                if is_snapshot:
                    self._apply_exchange_position(ex_pos, now, target_mid, m, "snapshot")
                else:
                    self.ledger.reconcile(ex_pos, now, target_mid, m.min_notional)

        elif channel in ("funding", "funding_rate", "fundingRate"):
            if isinstance(contents, dict):
                r = Decimal(str(contents.get("rate") or contents.get("fundingRate") or "0"))
                if self.md.info:
                    self.md.info.funding_rate = r
                self.md.funding_rate = r
                pmt = Decimal(str(contents.get("payment") or contents.get("fundingPayment") or "0"))
                if pmt != ZERO:
                    self.ledger.apply_funding(pmt)

    def _wake_throttled(self) -> None:
        """External feeds can emit hundreds of msgs/s; wake the quote loop at most every 20ms."""
        t = self.now()
        if t - self._last_ext_wake >= 0.02:
            self._last_ext_wake = t
            self._dirty_evt.set()

    def on_external_venue_bbo(self, venue: str, bid: Decimal, ask: Decimal,
                              bid_sz: Decimal = Decimal("1"), ask_sz: Decimal = Decimal("1")) -> None:
        if bid >= ask:
            return
        self.md.update_cross_venue(venue, bid, ask, bid_sz, ask_sz, self.now())
        self._wake_throttled()

    def on_external_depth(self, venue: str, bids: list, asks: list) -> None:
        self.md.update_cross_depth(venue, bids, asks, self.now())

    def on_external_trade(self, venue: str, side: str, size: Decimal, price: Decimal) -> None:
        self.md.update_cross_trade(venue, side, size, price, self.now())

    def on_external_liq(self, venue: str, side: str, size: Decimal, price: Decimal) -> None:
        self.md.update_cross_liq(venue, side, size, price, self.now())
        log.warning("LIQUIDATION %s forced-%s %s @ %s (~$%s)", venue, side, size, price,
                    f"{float(size * price):,.0f}")
        self._dirty_evt.set()

    def on_external_disconnect(self, venue: str) -> None:
        self.md.cross.drop_venue(venue)
        log.warning("Cross-venue feed %s disconnected - its signals are disabled until it reconnects", venue)

    def _handle_trade(self, tr: dict, now: float) -> None:
        try:
            side = str(tr.get("side") or tr.get("orderSide") or "BUY").upper()
            sz = Decimal(str(tr.get("size") or tr.get("quantity") or "0"))
            px = Decimal(str(tr.get("price") or "0"))
            if sz > 0:
                self.md.on_trade(side, sz, px, now)
        except Exception:
            pass

    def _on_fill(self, side: str, qty: Decimal, price: Decimal, o: Order) -> None:
        now = self.now()
        m = self.md.info
        mid = getattr(o, "quote_mid", None) or self.md.mid or price
        min_notional = m.min_notional if m else Decimal("5")
        
        is_maker = not getattr(o, "is_taker", False)
        fill = self.ledger.on_fill(side, qty, price, mid, now, min_notional, is_maker=is_maker)
        current_mid = self.md.mid or price
        log.info("FILL L%d %s %s @ %s | edge=%sbps pos=%s pnl=$%s ET=%s",
                 o.pair_index, side, fmt(qty), fmt(price), fmt(fill.edge_bps),
                 fmt(self.ledger.position), fmt(self.ledger.total_pnl(current_mid)),
                 et_hhmmss(time.time()))
        try:
            self._fill_ctx = {
                "level": o.pair_index, "et": et_hhmmss(time.time()),
                "obi": float(self.md.obi), "tfi": float(self.md.trade_flow_imbalance(10.0, now)),
                "spr_bps": float(self.md.spread_bps), "taker": bool(getattr(o, "is_taker", False)),
            }
        except Exception:
            self._fill_ctx = {"level": o.pair_index, "et": et_hhmmss(time.time())}

        self._recent_fills.append((now, side))
        while self._recent_fills and now - self._recent_fills[0][0] > self.cfg.burst_window_s:
            self._recent_fills.popleft()

        l = self.ledger.learner if (self.cfg.enable_online_learning and hasattr(self.ledger, 'learner')) else None
        burst_limit = l.burst_fills if l else self.cfg.burst_fills
        burst_cooldown = l.burst_cooldown_s if l else self.cfg.burst_cooldown_s
        sweep_limit = l.sweep_guard_fills if l else self.cfg.sweep_guard_fills
        sweep_window = l.sweep_guard_window_s if l else self.cfg.sweep_guard_window_s

        same_side = sum(1 for _, s in self._recent_fills if s == side)
        if same_side >= burst_limit:
            self._burst_blocked_until[side] = now + burst_cooldown
            log.warning("BURST GUARD: %d %s fills in %.1fs -> pulling %s for %.1fs",
                        same_side, side, self.cfg.burst_window_s, side, burst_cooldown)
            asyncio.create_task(self.om.cancel_side(side, now))

        rapid_fills = sum(1 for t, s in self._recent_fills if s == side and (now - t) <= sweep_window)
        if rapid_fills >= sweep_limit:
            self._burst_blocked_until[side] = max(self._burst_blocked_until[side], now + burst_cooldown)
            log.warning("SWEEP GUARD: %d %s fills in <=%.1fs -> emergency cancel %s",
                        rapid_fills, side, sweep_window, side)
            asyncio.create_task(self.om.cancel_side(side, now))

        self._journal(fill)
        self._dirty_evt.set()

    def _fp(self, path: str):
        fp = self._files.get(path)
        if fp is None or fp.closed:
            fp = open(path, "a", buffering=1)
            self._files[path] = fp
        return fp

    def _close_files(self) -> None:
        for fp in self._files.values():
            try:
                fp.close()
            except Exception:
                pass
        self._files.clear()

    def _spawn_bg(self, name: str, coro_fn, now: float) -> None:
        """Run slow network housekeeping off the quoting loop (one in flight per name)."""
        t = self._bg_tasks.get(name)
        if t is not None and not t.done():
            return
        self._bg_tasks[name] = asyncio.create_task(coro_fn(now))

    def _journal(self, f: Fill) -> None:
        if not self.cfg.journal_path or self.cfg.journal_path == os.devnull:
            return
        row = {
            "ts": f.ts, "side": f.side, "qty": fmt(f.qty), "price": fmt(f.price),
            "mid": fmt(f.mid), "edge_bps": fmt(f.edge_bps), "pos": fmt(f.position),
            "realized_delta": fmt(f.realized_delta), "total_realized": fmt(self.ledger.realized),
            "fees": fmt(self.ledger.fees)
        }
        row.update(getattr(self, "_fill_ctx", None) or {})
        try:
            self._fp(self.cfg.journal_path).write(json.dumps(row) + "\n")
        except Exception:
            pass

    def _own_resting(self):
        """Our RESTING maker orders old enough to be on the book and not being cancelled."""
        now = self.now()
        age = self.cfg.own_order_min_age_s
        return [(o.side, o.price, o.remaining) for o in self.om.orders.values()
                if not o.is_taker and o.cancelling_since is None and o.remaining > 0 and (now - o.created) >= age]

    def _journal_markout(self, f: Fill, horizon: float, m_bps) -> None:
        """Per-fill markout rows (join to fills on fill_ts) for offline conditional-EV fitting."""
        if not self.cfg.journal_path or self.cfg.journal_path == os.devnull:
            return
        try:
            self._fp(self.cfg.journal_path).write(json.dumps({
                "type": "markout", "fill_ts": f.ts, "side": f.side,
                "h": horizon, "markout_bps": round(float(m_bps), 4)}) + "\n")
        except Exception:
            pass

    def _log_quote_opportunity(self, targets: list[QuoteTarget], now: float) -> None:
        if not getattr(self.cfg, "enable_quote_dataset", False) or not self.cfg.quote_dataset_path:
            return
        if self.cfg.quote_dataset_path == os.devnull:
            return
        if now - getattr(self, "_last_ds_ts", -1e9) < self.cfg.quote_dataset_min_s:
            return
        self._last_ds_ts = now
        try:
            fp0 = self._fp(self.cfg.quote_dataset_path)
            if self.cfg.quote_dataset_max_mb > 0 and fp0.tell() > self.cfg.quote_dataset_max_mb * 1024 * 1024:
                fp0.close()
                self._files.pop(self.cfg.quote_dataset_path, None)
                os.replace(self.cfg.quote_dataset_path, self.cfg.quote_dataset_path + ".1")  # keep one old chunk
            snapshot = self.md.get_microstructure_snapshot(self.md.info, now)
            record = {
                "ts": now,
                "snapshot": snapshot,
                "position": float(self.ledger.position),
                "avg_cost": float(self.ledger.avg_cost),
                "unrealized_pnl": float(self.ledger.unrealized(self.md.mid or Decimal(0))),
                "realized_pnl": float(self.ledger.realized),
                "quotes": [
                    {
                        "pair_index": q.pair_index,
                        "side": q.side,
                        "price": float(q.price),
                        "qty": float(q.qty),
                        "ev_bps": float(q.expected_value_bps),
                        "p_fill": float(q.fill_probability),
                        "is_exit": q.is_exit_quote,
                        "is_taker": getattr(q, "is_taker", False)
                    }
                    for q in targets
                ]
            }
            self._fp(self.cfg.quote_dataset_path).write(json.dumps(record) + chr(10))
        except Exception:
            pass

    async def tick(self) -> None:
        async with self._tick_lock:
            now = self.now()
            self._tick_quote_path = False
            self.ledger.last_now = now
            self.ledger.current_now = now
            if hasattr(self.ledger, "learner") and (now - getattr(self, "_last_decay_call", 0.0) >= 1.0):
                self._last_decay_call = now
                self.ledger.learner.tick_decay(now)
                self.ledger.learner.flush()
            m = self.md.info
            if not m:
                return

            mid = self.md.mid
            if not mid or not self.md.bid or not self.md.ask:
                return

            if (self.cfg.dms_required and self.cfg.dms_enabled and not self.cfg.dry_run
                    and now > self._dms_ok_until):
                await self.om.cancel_all()
                if now - self._last_pause_log["oracle"] > 30.0:
                    self._last_pause_log["oracle"] = now
                    log.error("DMS_REQUIRED=1 but dead man's switch is not armed - quoting paused")
                return

            tot_pnl = self.ledger.total_pnl(mid)
            if self.cfg.session_loss_action == "pause_day":
                wall = time.time()
                until = getattr(self, "_loss_pause_until", 0.0)
                if until and wall >= until:                       # new UTC day: fresh loss budget
                    self._loss_pause_until = 0.0
                    self._pnl_base = tot_pnl
                    self.ledger.dyn_pnl_base = tot_pnl
                    log.warning("Daily loss pause over - resuming quoting with a fresh loss budget")
                if not self._loss_pause_until and (tot_pnl - getattr(self, "_pnl_base", ZERO)) <= -self.cfg.session_max_loss_usd:
                    self._loss_pause_until = (int(wall // 86400) + 1) * 86400.0
                    log.error("DAILY LOSS LIMIT HIT ($%s from day start, limit -$%s) - no new adds until 00:00 UTC; unwinds continue",
                              fmt(tot_pnl - getattr(self, "_pnl_base", ZERO)), fmt(self.cfg.session_max_loss_usd))
            elif tot_pnl <= -self.cfg.session_max_loss_usd:
                log.error("SESSION MAX LOSS BREACHED ($%s <= -$%s) - HALTING",
                          fmt(tot_pnl), fmt(self.cfg.session_max_loss_usd))
                await self.om.cancel_all(force=True)
                self.stop_evt.set()
                return

            if self.md.spread_bps > self.cfg.max_market_spread_bps:
                await self.om.cancel_all(force=True)
                if now - self._last_pause_log["spread"] > 30.0:
                    self._last_pause_log["spread"] = now
                    log.warning("Market spread (%sbps) exceeds MAX_MARKET_SPREAD_BPS (%sbps) - Quoting paused",
                                fmt(self.md.spread_bps), fmt(self.cfg.max_market_spread_bps))
                return

            if m.mark and abs(mid - m.mark) / m.mark * BPS > self.cfg.max_oracle_dev_bps:
                await self.om.cancel_all()
                if now - self._last_pause_log["oracle"] > 30.0:
                    self._last_pause_log["oracle"] = now
                    log.warning("Mid price (%s) deviates from Oracle mark (%s) by > %sbps - Quoting paused",
                                fmt(mid), fmt(m.mark), fmt(self.cfg.max_oracle_dev_bps))
                return

            self._rth_unwind_only = False
            if m.is_outside_rth and not self.cfg.quote_outside_rth:
                if abs(self.ledger.position) < m.min_size:
                    await self.om.cancel_all()
                    if now - self._last_pause_log["rth"] > 30.0:
                        self._last_pause_log["rth"] = now
                        log.warning("%s is outside Regular Trading Hours (9:30 AM - 4:00 PM EDT) - Quoting paused. Set QUOTE_OUTSIDE_RTH=1 in .env to trade outside RTH.",
                                    m.name)
                    return
                # open position outside RTH: keep managing the unwind (no new adds) instead of leaving it unmanaged
                self._rth_unwind_only = True

            pos_usd = self.ledger.position * mid
            if self.md.jump_active(now):
                if pos_usd == ZERO:
                    await self.om.cancel_all()
                    if now - self._last_pause_log["jump"] > 30.0:
                        self._last_pause_log["jump"] = now
                        log.warning("Price jump detected - Quoting paused for %ss cooldown", self.cfg.jump_cooldown_s)
                    return
                else:
                    if pos_usd > ZERO:
                        self._trend_blocked_until[BUY] = now + self.cfg.jump_cooldown_s
                    else:
                        self._trend_blocked_until[SELL] = now + self.cfg.jump_cooldown_s

            buy_blocked = (now < self._burst_blocked_until[BUY] or now < self._trend_blocked_until[BUY])
            sell_blocked = (now < self._burst_blocked_until[SELL] or now < self._trend_blocked_until[SELL])

            l = self.ledger.learner if (self.cfg.enable_online_learning and hasattr(self.ledger, 'learner')) else None
            trend_pull = l.trend_pull_bps if l else self.cfg.trend_pull_bps

            ret_trend = self.md.ret_bps(self.cfg.trend_window_s, now)
            if ret_trend <= -trend_pull:
                self._trend_blocked_until[BUY] = now + self.cfg.trend_hold_s
                buy_blocked = True
            elif ret_trend >= trend_pull:
                self._trend_blocked_until[SELL] = now + self.cfg.trend_hold_s
                sell_blocked = True

            if self.cfg.enable_cross_exchange and self.md.cross.venues:
                cb, cs, why = self.md.cross.pull_decision(
                    mid, now, self.cfg.cross_pull_bps, self.cfg.cross_vel_pull_bps, self.cfg.cross_liq_usd)
                if cb:
                    self._trend_blocked_until[BUY] = now + self.cfg.cross_pull_hold_s
                    buy_blocked = True
                if cs:
                    self._trend_blocked_until[SELL] = now + self.cfg.cross_pull_hold_s
                    sell_blocked = True
                if (cb or cs) and now - self._last_cross_log > 5.0:
                    self._last_cross_log = now
                    log.info("CROSS PULL %s | %s", "BIDS" if cb and not cs else ("ASKS" if cs and not cb else "BOTH"), why)

            pos_usd = self.ledger.position * mid
            if self.cfg.enable_online_learning and self.md.mid:
                ret_5s = self.md.ret_bps(5.0, now)
                tfi = self.md.trade_flow_imbalance(10.0, now)
                self.ledger.learner.on_flow_correlation(self.md.obi, tfi, ret_5s)

            if self.md.move_bps(self.cfg.vol_window_s, now) >= self.cfg.vol_pause_bps:
                # Volatility spike: pause ADDING sides, never pause UNWIND sides
                if pos_usd >= 0:
                    buy_blocked = True
                if pos_usd <= 0:
                    sell_blocked = True

            if getattr(self, "_loss_pause_until", 0.0) or getattr(self, "_rth_unwind_only", False) or in_et_windows(self.cfg.et_pause_windows, time.time()):
                # ET blackout (e.g. US open): stop ADDING, never block unwinds
                if pos_usd >= 0:
                    buy_blocked = True
                if pos_usd <= 0:
                    sell_blocked = True

            if self.cfg.oracle_guard and m.mark and mid and (self.now() - self.md.info_ts) <= max(15.0, 3 * self.cfg.market_refresh_s):
                dev = (m.mark - mid) / mid * BPS          # >0: oracle above the book -> price likely to rise
                if dev >= self.cfg.oracle_guard_bps and pos_usd <= 0:
                    sell_blocked = True                    # don't SELL (add) below the oracle
                elif dev <= -self.cfg.oracle_guard_bps and pos_usd >= 0:
                    buy_blocked = True                     # don't BUY (add) above the oracle
                if abs(dev) >= self.cfg.oracle_guard_bps and now - self._last_pause_log.get("oguard", 0.0) > 30.0:
                    self._last_pause_log["oguard"] = now
                    log.info("ORACLE_GUARD mark_dev=%+.2fbps -> blocking %s adds", float(dev), "SELL" if dev > 0 else "BUY")

            self._tick_quote_path = True
            self._tick_paused_cfg = bool(getattr(self, "_loss_pause_until", 0.0) or getattr(self, "_rth_unwind_only", False)
                                         or in_et_windows(self.cfg.et_pause_windows, time.time()))
            self._last_blocked = (buy_blocked, sell_blocked)
            existing_slots = set(self.om.pair_slots.keys())
            targets = self.engine.generate_ladder_quotes(
                m, self.md, self.ledger, now, buy_blocked, sell_blocked, existing_slots=existing_slots
            )
            self._last_targets_n = len(targets)
            self._log_quote_opportunity(targets, now)

            blocked_sides = set()
            if buy_blocked:
                blocked_sides.add(BUY)
            if sell_blocked:
                blocked_sides.add(SELL)
            await self.om.sync_quotes(targets, now, blocked_sides=blocked_sides)

    def _apply_exchange_position(self, ex_pos: Decimal, now: float, mid: Decimal, m, src: str) -> bool:
        """Adopt an authoritative exchange position immediately (snapshot / REST), unless a fill is still in flight."""
        tol = (m.min_notional / mid) * Decimal("0.25") if mid else Decimal("1e-8")
        if abs(ex_pos - self.ledger.position) <= tol:
            self.ledger._mismatch = 0
            return False
        if now - self.ledger.last_fill_ts < 3.0:
            self._resync_req = True          # a fill just landed - re-check over REST in a moment
            return False
        log.warning("POSITION RESYNC (%s): local %s -> exchange %s", src, fmt(self.ledger.position), fmt(ex_pos))
        self.ledger.force_position(ex_pos, now, mid, m.min_notional)
        self._dirty_evt.set()
        return True

    async def _resync_position(self, reason: str, confirm: bool = True) -> Optional[bool]:
        """REST ground truth. Returns True if corrected, False if already in sync, None if REST unavailable."""
        m = self.md.info
        if not m:
            return None
        self._last_pos_resync = self.now()
        a = await self.ex.fetch_position(self.cfg.address, self.cfg.account_index, m.market_id)
        if a is None:
            return None
        mid = self.md.mid or m.mark
        tol = (m.min_notional / mid) * Decimal("0.25") if mid else Decimal("1e-8")
        if abs(a - self.ledger.position) <= tol:
            return False
        if confirm:                       # guard against reading REST a hair before/after a WS fill
            await asyncio.sleep(1.0)
            b = await self.ex.fetch_position(self.cfg.address, self.cfg.account_index, m.market_id)
            if b is None or abs(b - a) > tol:
                return None
            a = b
        return self._apply_exchange_position(a, self.now(), mid, m, "REST/" + reason)

    async def _pos_resync_loop(self, now: float) -> None:
        S = float(getattr(self.cfg, "pos_resync_s", 0.0) or 0.0)
        due = S > 0 and (now - self._last_pos_resync) >= S
        if not (due or self._resync_req):
            return
        self._resync_req = False
        try:
            await self._resync_position("periodic")
        except Exception:
            log.exception("position resync failed")

    def _on_reduce_only_reject(self, side: str, now: float) -> None:
        """Exchange rejected our reduce-only order with REDUCE_ONLY_WOULD_INCREASE: it says that order would GROW the
        exchange position, i.e. the exchange is flat or on the other side. If our local book position disagrees we are
        desynced (missed fill / bad snapshot) and would otherwise retry the same doomed unwind forever until restart."""
        pos = self.ledger.position
        if pos == ZERO or not ((side == SELL and pos > ZERO) or (side == BUY and pos < ZERO)):
            return
        fill_ts = getattr(self.ledger, "last_fill_ts", None)
        if self._ro_fill_ts != fill_ts or now - self._ro_first > 60.0 or self._ro_n == 0:
            self._ro_n, self._ro_first, self._ro_fill_ts = 0, now, fill_ts
        self._ro_n += 1
        if self._ro_n < 3:
            return
        n, self._ro_n = self._ro_n, 0
        asyncio.create_task(self._ro_repair(side, now, n))

    async def _ro_repair(self, side: str, now: float, n: int = 3) -> None:
        r = None
        try:
            r = await self._resync_position("reduce-only rejects", confirm=False)
        except Exception:
            log.exception("reduce-only REST repair failed")
        if r is not None:                    # REST answered: either corrected, or local already agrees with exchange
            return
        m = self.md.info
        mid = self.md.mid or (m.mark if m else None)
        pos = self.ledger.position
        if not m or not mid or pos == ZERO:
            return
        if now - self._ro_fix_ts > 120.0:
            self._ro_stage = 0
        if self._ro_stage == 0:
            new_pos, how = -pos, "mirrored (sign error)"
        else:
            new_pos, how = ZERO, "set flat (exchange is flat)"
        log.error("POSITION DESYNC (REST unavailable): %d reduce-only %s orders rejected while local pos=%s -> local position %s to %s",
                  n, side, fmt(pos), how, fmt(new_pos))
        self.ledger.force_position(new_pos, now, mid, m.min_notional)
        self._ro_stage += 1
        self._ro_fix_ts = now
        self._dirty_evt.set()

    def _stall_check(self, now: float) -> None:
        """Self-heal: if the bot is flat, has no resting orders and is NOT intentionally paused, yet has not
        been able to quote for STALL_RESTART_S, restart the process in place (same effect as a manual restart,
        which is the only thing that has cleared this stall). Normal gaps between orders are <5 min."""
        S = float(getattr(self.cfg, "stall_restart_s", 0.0) or 0.0)
        m = self.md.info
        if S <= 0 or not m or self.restart_requested:
            return
        busy = (not self._tick_quote_path) or self._tick_paused_cfg or bool(self.om.orders) \
            or abs(self.ledger.position) >= m.min_size
        if busy or self._idle_since is None:
            self._idle_since = now
            return
        if now - self._idle_since < S:
            return
        try:
            regime = self.md.detect_regime(now, self.ledger.tox_bps)
            l = self.ledger.learner if hasattr(self.ledger, "learner") else None
            pm = (float(l.predict_markout(BUY, regime, 0, self.cfg.queue_horizon_s)),
                  float(l.predict_markout(SELL, regime, 0, self.cfg.queue_horizon_s))) if l else None
        except Exception:
            regime, pm = "?", None
        log.error("STALL WATCHDOG: flat, no resting orders and quoting allowed for %.0fs (blocked buy/sell=%s, last targets=%d, "
                  "regime=%s, pred_markout buy/sell=%s) - restarting process to clear stuck in-memory state",
                  now - self._idle_since, self._last_blocked, self._last_targets_n, regime, pm)
        self.restart_requested = True
        self.stop_evt.set()

    async def _heartbeat(self, now: float) -> None:
        """Arm/refresh the exchange-side dead man's switch (scheduleCancel) for our market."""
        cfg = self.cfg
        if cfg.dry_run or not cfg.dms_enabled or not self.md.info:
            return
        interval = min(cfg.heartbeat_s, cfg.dms_ttl_s / 3.0)
        if now - self._last_heartbeat < interval:
            return
        if time.time() < getattr(self, "_dms_blocked_until", 0.0):
            return          # exchange said the daily trigger quota is used up: don't hammer it until 00:00 UTC
        self._last_heartbeat = now
        try:
            deadline_us = int((time.time() + cfg.dms_ttl_s) * 1_000_000)
            resp = await self.ex.write(self.signer.schedule_cancel(self.md.info, deadline_us))
            ok = (isinstance(resp, dict) and resp.get("status") in (200, 202)
                  and not resp.get("error"))
            if ok:
                if not self._dms_armed:
                    log.info("Dead man's switch ARMED (market %s, ttl %.0fs, refresh every %.1fs)",
                             self.md.info.name, cfg.dms_ttl_s, interval)
                self._dms_armed = True
                self._dms_fail = 0
                self._dms_ok_until = now + cfg.dms_ttl_s
                return
            self._dms_fail += 1
            err_txt = json.dumps(resp.get("error") if isinstance(resp, dict) else resp).lower()
            if "limit reached" in err_txt or "daily_limit" in err_txt or "trigger limit" in err_txt:
                wall = time.time()
                self._dms_blocked_until = (int(wall // 86400) + 1) * 86400.0 + 5.0
                self._dms_armed = False
                log.warning("DEAD MAN'S SWITCH daily trigger quota is used up (the switch FIRED 10x today = every time the bot "
                            "stopped refreshing for DMS_TTL_S). Not retrying until 00:00 UTC. %s",
                            "Quoting will pause when the switch expires (DMS_REQUIRED=1)." if cfg.dms_required else
                            "Quoting continues WITHOUT exchange-side protection (set DMS_REQUIRED=1 to pause instead).")
                return
            if self._dms_fail in (1, 3) or self._dms_fail % 12 == 0:
                log.error("DEAD MAN'S SWITCH NOT ARMED (attempt %d): status=%s %s",
                          self._dms_fail, resp.get("status") if isinstance(resp, dict) else None,
                          json.dumps(resp.get("error") if isinstance(resp, dict) else resp)[:300])
        except Exception as e:
            self._dms_fail += 1
            log.error("dead man's switch refresh error: %s", e)

    async def _graceful_cancel(self) -> None:
        """Cancel everything while still connected; disarm the switch only if the book is verified empty
        (otherwise leave it armed so the exchange pulls anything we missed)."""
        for _t in self._bg_tasks.values():
            if not _t.done():
                _t.cancel()
        try:
            await self.om.cancel_all(force=True)
        except Exception as e:
            log.warning("graceful cancel_all error: %s", e)
        if self.cfg.dry_run or not self.md.info:
            return
        try:
            await asyncio.sleep(0.4)
            res = await self.ex.get("orders", {"address": self.cfg.address, "accountIndex": self.cfg.account_index,
                                                "marketId": self.md.info.market_id}, timeout=3.0)
            rows = [r for r in (res or {}).get("openOrders", []) if isinstance(r, dict)
                    and r.get("marketId") in (None, self.md.info.market_id)] if res is not None else None
            if rows == []:
                await self._disarm_dms()
            elif rows is None:
                log.warning("could not verify empty book on shutdown - leaving dead man's switch armed")
            else:
                log.warning("%d order(s) still open on shutdown - leaving dead man's switch armed", len(rows))
        except Exception as e:
            log.warning("shutdown verification error: %s", e)

    async def _disarm_dms(self) -> None:
        cfg = self.cfg
        if cfg.dry_run or not cfg.dms_enabled or not self.md.info or not self._dms_armed:
            return
        try:
            await self.ex.write(self.signer.schedule_cancel(self.md.info, None))
            self._dms_armed = False
            log.info("Dead man's switch disarmed")
        except Exception:
            pass

    async def _refresh_market_info(self, now: float) -> None:
        """markPrice / isOutsideRth / status / funding were only read ONCE at connect, so the oracle guard and the
        QUOTE_OUTSIDE_RTH pause ran on a frozen snapshot. Refresh the volatile fields in place (tick/step untouched)."""
        if self.cfg.market_refresh_s <= 0 or not self.md.info:
            return
        if now - getattr(self, "_last_mkt_refresh", -1e9) < self.cfg.market_refresh_s:
            return
        self._last_mkt_refresh = now
        try:
            rows = await self.ex.fetch_markets(self.cfg.market)
            if not rows:
                return
            fresh = Market.from_api(rows[0])
            info = self.md.info
            if fresh.mark and fresh.mark > ZERO:
                info.mark = fresh.mark
            info.is_outside_rth = fresh.is_outside_rth
            info.status = fresh.status
            if fresh.funding_rate is not None:
                info.funding_rate = fresh.funding_rate
            if fresh.next_funding_time:
                info.next_funding_time = fresh.next_funding_time
            if fresh.is_outside_rth != getattr(self, "_last_rth_flag", fresh.is_outside_rth):
                log.info("Market isOutsideRth changed -> %s", fresh.is_outside_rth)
            self._last_rth_flag = fresh.is_outside_rth
            self.md.info_ts = now
        except Exception as e:
            if now - getattr(self, "_last_mkt_err", -1e9) > 60.0:
                self._last_mkt_err = now
                log.warning("market info refresh failed: %s", e)

    async def _reconcile(self, now: float) -> None:
        if now - self._last_reconcile < self.cfg.reconcile_s:
            return
        self._last_reconcile = now
        try:
            m = self.md.info
            if not m:
                return
            res = await self.ex.get("orders", {"address": self.cfg.address, "accountIndex": self.cfg.account_index,
                                                "marketId": m.market_id})
            if res and "openOrders" in res:
                await self.om.reconcile(res["openOrders"], now)
        except Exception:
            pass

    def _status_log(self, now: float) -> None:
        if now - self._last_status < self.cfg.status_s:
            return
        self._last_status = now
        mid = self.md.mid or Decimal("0")
        regime = self.md.detect_regime(now, self.ledger.tox_bps)
        try:
            mk = self._get_market().mark
            dev = ((mk - mid) / mid * BPS) if (mk and mid) else None
        except Exception:
            mk, dev = None, None
        log.info("STATUS | %s | mid=%s spr=%.3fbps obi=%.2f vol=%.3fbps mark=%s mark_dev=%sbps | pos=%s unreal=$%.4f pnl=$%.4f | orders: %s",
                 regime, fmt(mid), float(self.md.spread_bps), float(self.md.obi), float(self.md.vol_bps),
                 fmt(mk) if mk else "-", ("%+.2f" % float(dev)) if dev is not None else "-",
                 fmt(self.ledger.position), float(self.ledger.unrealized(mid)),
                 float(self.ledger.total_pnl(mid)), self.om.describe(now))
        if self.cfg.enable_cross_exchange and self.cfg.cross_feed:
            cr = self.md.cross
            fr = cr.fresh(now)
            div = cr.lead_lag_divergence_bps(self.md.mid, now)
            down, up = cr.liq_pressure_usd(30.0, now)
            log.info("CROSS | venues=%s | div=%+.2fbps vel3s=%+.2fbps obi=%+.2f tfi5s=%+.2f disp=%.2fbps | liq30s sell=$%.0f buy=$%.0f",
                     ",".join(v.venue for v in fr) or "NONE (feeds down - signals off)",
                     float(div), float(cr.cross_velocity_bps(3.0, now)), float(cr.cross_obi(now)),
                     float(cr.cross_tfi(5.0, now)), float(cr.cross_dispersion_bps(now)), down, up)
        if self.cfg.enable_online_learning:
            s = self.ledger.learner.get_summary()
            p = s["params"]
            realized_delta = self.ledger.realized - self._last_logged_realized
            self._last_logged_realized = self.ledger.realized
            inv_pnl = self.ledger.inventory_pnl(mid)
            reason = s.get("last_change_reason", "none") or "none"

            m1s = (f"{float(Ledger.raw_mean_bps(self.ledger.markouts_1s)):+.2f}bps") if self.ledger.markouts_1s else "0.00bps"
            m5s = (f"{float(Ledger.raw_mean_bps(self.ledger.markouts_5s)):+.2f}bps") if self.ledger.markouts_5s else "0.00bps"
            m_avg = (f"{float(Ledger.raw_mean_bps(self.ledger.markouts)):+.2f}bps") if self.ledger.markouts else "0.00bps"
            wr = f"{s['win_rate']:.1f}%"
            afr = f"{s['adverse_fill_rate']:.1f}%"
            pnl_delta = ("+$" if realized_delta >= 0 else "-$") + f"{abs(float(realized_delta)):.2f}"
            inv_pnl_str = ("+$" if inv_pnl >= 0 else "-$") + f"{abs(float(inv_pnl)):.2f}"
            cap_spr = f"${float(self.ledger.spread_capture):.2f} (avg {float(self.ledger.avg_edge_bps):.2f}bps)"
            vol_str = f"${float(self.ledger.volume_usd):.2f}"
            fills_str = f"{self.ledger.n_fills} ({self.ledger.n_buys}B/{self.ledger.n_sells}S)"

            log.info("LEARN [updates=%d tox=%d] | edge=%.2f-%.2fbps skew=%.2fbps spacing=%.2fbps mult=%.2f vol_k=%.2f tox_mult=%.2f min_ev=%.2fbps obi_a=%.2f tfi_b=%.2f kappa=%.2f | markout_1s=%s markout_5s=%s avg_markout=%s | win_rate=%s adverse_fill_rate=%s | realized_pnl_delta=%s inventory_pnl=%s | capture_spread=%s volume=%s fills=%s",
                     s["total_updates"], s["toxic_fills"],
                     float(p["min_edge_bps"]), float(p["max_edge_bps"]), float(p["skew_bps"]),
                     float(p["level_spacing_bps"]), float(p["level_size_mult"]), float(p["vol_k"]),
                     float(p["tox_mult"]), float(p["min_ev_bps"]), float(p["obi_alpha"]),
                     float(p["tfi_beta"]), float(p["fill_prob_kappa"]),
                     m1s, m5s, m_avg, wr, afr, pnl_delta, inv_pnl_str, cap_spr, vol_str, fills_str)

    async def run(self) -> None:
        if self.cfg.enable_online_learning and hasattr(self.ledger, "learner"):
            self.ledger.learner.save_interval = 1.0  # keep disk I/O off the hot path
        log.info("Connecting to %s Arcus WS (%s)...", self.cfg.env_name, self.ex.ws_url)
        raw_markets = await self.ex.fetch_markets(self.cfg.market)
        self.md.info = Market.from_api(raw_markets[0])
        self.md.info_ts = self.now()
        log.info("Market loaded: %s (ID %d) tick=%s step=%s min_notional=$%s",
                 self.md.info.name, self.md.info.market_id, self.md.info.tick,
                 self.md.info.step, self.md.info.min_notional)

        try:
            import websockets
            from websockets.exceptions import ConnectionClosed
        except ImportError:
            class ConnectionClosed(Exception):
                pass
            if not hasattr(self.ex, "ws") or self.ex.ws is None:
                log.error("websockets package not available; install via pip install websockets")
                return

        if self.cfg.enable_cross_exchange and self.cfg.cross_feed and self._cross_feeds is None:
            self._cross_feeds = CrossFeedManager(self.cfg, self)
            self._cross_feeds.start()

        reconnect_delay = 1.0
        max_reconnect_delay = 15.0

        while not self.stop_evt.is_set():
            reader_task = None
            try:
                log.info("Connecting to Arcus WebSocket (%s)...", self.ex.ws_url)
                async with websockets.connect(
                    self.ex.ws_url,
                    ping_interval=self.cfg.ws_ping_interval_s,
                    ping_timeout=self.cfg.ws_ping_timeout_s,
                    open_timeout=20,
                    max_size=2**23,
                    close_timeout=5,
                    compression=None
                ) as ws:
                    self.ex.ws = ws
                    self.ex.last_rx = time.monotonic()
                    self._pos_seq = -1
                    self.ledger._mismatch = 0
                    reader_task = asyncio.create_task(self.ex.reader())

                    await self.ex.subscribe("bbo", self.cfg.market)
                    await self.ex.subscribe("l2Orderbook", self.cfg.market)
                    await self.ex.subscribe("trades", self.cfg.market)
                    await self.ex.subscribe("orders", self.cfg.address)
                    await self.ex.subscribe("userFills", self.cfg.address)
                    await self.ex.subscribe("positions", self.cfg.address)

                    reconnect_delay = 1.0
                    self._last_heartbeat = 0.0
                    await self._heartbeat(self.now())  # arm before any quote is placed

                    if self.om.maybe_orders:
                        await self.om.cancel_all()

                    # We may have been blind (resting orders keep resting on disconnect - no cancel-on-disconnect).
                    # Settle our position against REST truth BEFORE quoting again.
                    try:
                        await asyncio.sleep(1.0)       # let the market info + position snapshots land
                        r = await asyncio.wait_for(self._resync_position("reconnect"), timeout=12.0)
                        log.info("Reconnect position check: %s", {True: "corrected", False: "in sync", None: "REST unavailable"}[r])
                    except Exception as e:
                        log.warning("Reconnect position check failed: %s", e)
                    log.info("Subscribed to data feeds. Level 7 MM Engine active.")

                    while not self.stop_evt.is_set() and self.ex.is_connected:
                        now = self.now()
                        self._spawn_bg("heartbeat", self._heartbeat, now)
                        self._spawn_bg("reconcile", self._reconcile, now)
                        self._spawn_bg("posresync", self._pos_resync_loop, now)
                        if time.monotonic() - self.ex.last_rx > self.cfg.ws_stale_s:
                            log.warning("No WebSocket frame for %.0fs - forcing reconnect", time.monotonic() - self.ex.last_rx)
                            await ws.close()
                            break
                        self._spawn_bg("mktinfo", self._refresh_market_info, now)
                        self._status_log(now)

                        await self.tick()
                        self._stall_check(self.now())

                        try:
                            await asyncio.wait_for(self._dirty_evt.wait(), timeout=self.cfg.loop_s)
                            self._dirty_evt.clear()
                        except asyncio.TimeoutError:
                            pass

                    if self.stop_evt.is_set():
                        await self._graceful_cancel()  # must happen while the socket is still open

            except (ConnectionClosed, ConnectionResetError, BrokenPipeError, OSError) as e:
                log.warning("WebSocket connection dropped (%s). Reconnecting in %.1fs...", e, reconnect_delay)
            except Exception as e:
                log.error("Error in bot run loop: %s", e, exc_info=True)
            finally:
                for _t in self._bg_tasks.values():
                    if not _t.done():
                        _t.cancel()
                if reader_task and not reader_task.done():
                    reader_task.cancel()
                    try:
                        await reader_task
                    except (asyncio.CancelledError, Exception):
                        pass
                self.ex.ws = None

            if not self.stop_evt.is_set():
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

        log.info("Stopping bot - cancelling resting orders for market %s...", self.md.info.name if self.md.info else self.cfg.market)
        if self.cfg.enable_online_learning:
            self.ledger.learner.save()
            log.info("Saved online learning state to %s", self.cfg.learning_state_path)
        try:
            await self.om.cancel_all()
        except Exception:
            pass
        if self._cross_feeds is not None:
            try:
                await self._cross_feeds.stop()
            except Exception:
                pass
        self._close_files()
