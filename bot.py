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


class MarketMaker:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.now = time.monotonic
        self.stop_evt = asyncio.Event()

        self.ex = Exchange(cfg, self._on_channel)
        self.md = MarketData(cfg)
        self.signer = Signer(cfg.signing_key, cfg.address, cfg.account_index)
        self.ledger = Ledger(cfg)
        self.engine = MarketMakingEngine(cfg)
        self.om = OrderManager(cfg, self.ex, self.signer, self._get_market, self._on_fill)

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
            rows = extract_positions(contents)
            m = self.md.info
            if m:
                target_mid = self.md.mid or m.mark
                for r in rows:
                    if int(r.get("marketId", -1)) == m.market_id:
                        side = str(r.get("side", "FLAT")).upper()
                        sz = Decimal(str(r.get("size", "0")))
                        signed_pos = sz if side == "LONG" else (-sz if side == "SHORT" else ZERO)
                        self.ledger.reconcile(signed_pos, now, target_mid, m.min_notional)

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
        log.info("FILL L%d %s %s @ %s | edge=%sbps pos=%s pnl=$%s",
                 o.pair_index, side, fmt(qty), fmt(price), fmt(fill.edge_bps),
                 fmt(self.ledger.position), fmt(self.ledger.total_pnl(current_mid)))

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
        try:
            self._fp(self.cfg.journal_path).write(json.dumps(row) + "\n")
        except Exception:
            pass

    def _log_quote_opportunity(self, targets: list[QuoteTarget], now: float) -> None:
        if not getattr(self.cfg, "enable_quote_dataset", False) or not self.cfg.quote_dataset_path:
            return
        if self.cfg.quote_dataset_path == os.devnull:
            return
        try:
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
            if tot_pnl <= -self.cfg.session_max_loss_usd:
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

            if m.is_outside_rth and not self.cfg.quote_outside_rth:
                await self.om.cancel_all()
                if now - self._last_pause_log["rth"] > 30.0:
                    self._last_pause_log["rth"] = now
                    log.warning("%s is outside Regular Trading Hours (9:30 AM - 4:00 PM EDT) - Quoting paused. Set QUOTE_OUTSIDE_RTH=1 in .env to trade outside RTH.",
                                m.name)
                return

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

            existing_slots = set(self.om.pair_slots.keys())
            targets = self.engine.generate_ladder_quotes(
                m, self.md, self.ledger, now, buy_blocked, sell_blocked, existing_slots=existing_slots
            )
            self._log_quote_opportunity(targets, now)

            blocked_sides = set()
            if buy_blocked:
                blocked_sides.add(BUY)
            if sell_blocked:
                blocked_sides.add(SELL)
            await self.om.sync_quotes(targets, now, blocked_sides=blocked_sides)

    async def _heartbeat(self, now: float) -> None:
        """Arm/refresh the exchange-side dead man's switch (scheduleCancel) for our market."""
        cfg = self.cfg
        if cfg.dry_run or not cfg.dms_enabled or not self.md.info:
            return
        interval = min(cfg.heartbeat_s, cfg.dms_ttl_s / 3.0)
        if now - self._last_heartbeat < interval:
            return
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
        log.info("STATUS | %s | mid=%s spr=%sbps obi=%s vol=%sbps | pos=%s unreal=$%s pnl=$%s | orders: %s",
                 regime, fmt(mid), fmt(self.md.spread_bps), fmt(self.md.obi), fmt(self.md.vol_bps),
                 fmt(self.ledger.position), fmt(self.ledger.unrealized(mid)),
                 fmt(self.ledger.total_pnl(mid)), self.om.describe(now))
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

            m1s = (f"{float(self.ledger.avg_markout_1s_bps):+.2f}bps") if self.ledger.markouts_1s else "0.00bps"
            m5s = (f"{float(self.ledger.avg_markout_5s_bps):+.2f}bps") if self.ledger.markouts_5s else "0.00bps"
            m_avg = (f"{float(self.ledger.avg_markout_bps):+.2f}bps") if self.ledger.markouts else "0.00bps"
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
                    ping_interval=15,
                    ping_timeout=20,
                    max_size=2**23,
                    close_timeout=5,
                    compression=None
                ) as ws:
                    self.ex.ws = ws
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

                    log.info("Subscribed to data feeds. Level 7 MM Engine active.")

                    while not self.stop_evt.is_set() and self.ex.is_connected:
                        now = self.now()
                        self._spawn_bg("heartbeat", self._heartbeat, now)
                        self._spawn_bg("reconcile", self._reconcile, now)
                        self._status_log(now)

                        await self.tick()

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
