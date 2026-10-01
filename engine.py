"""Level 7 Quantitative Market-Making Engine."""
from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, List, Tuple

from config import Config
from market import Market, MarketData
from ledger import Ledger
from utils import BPS, BUY, SELL, ZERO, ONE, clamp, q_down, q_up, fmt


@dataclass
class QuoteTarget:
    pair_index: int
    side: str
    price: Decimal
    qty: Decimal
    expected_value_bps: Decimal
    fill_probability: float
    is_exit_quote: bool
    is_taker: bool = False


class MarketMakingEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def compute_fair_value(self, md: MarketData, now: float, ledger: Optional[Ledger] = None) -> Decimal:
        base_mid = md.mid
        if base_mid is None:
            return ZERO

        if not self.cfg.enable_orderbook_intel:
            return md.micro if (self.cfg.use_micro and md.micro) else base_mid

        micro = md.micro if md.micro else base_mid
        half_spr = (md.ask - md.bid) / Decimal("2") if (md.bid and md.ask) else ZERO

        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        alpha = l.obi_alpha if l else self.cfg.obi_alpha
        beta = l.tfi_beta if l else self.cfg.tfi_beta

        obi = md.obi
        obi_shift = half_spr * obi * alpha

        tfi = md.trade_flow_imbalance(10.0, now)
        tfi_shift = half_spr * tfi * beta

        cross_shift = ZERO
        cross_obi_shift = ZERO
        if self.cfg.enable_cross_exchange and md.cross.venues:
            cross_div = md.cross.lead_lag_divergence_bps(base_mid)
            if cross_div != ZERO:
                cross_shift = base_mid * (cross_div / BPS) * self.cfg.cross_lead_lag_weight
            cross_obi = md.cross.cross_obi()
            cross_obi_shift = half_spr * cross_obi * Decimal("0.5")

        basis_shift = ZERO
        if hasattr(md, "reference_basis_bps"):
            basis = md.reference_basis_bps()
            if basis != ZERO:
                basis_shift = -base_mid * (basis / BPS) * Decimal("0.05")

        fair_val = micro + obi_shift + tfi_shift + cross_shift + cross_obi_shift + basis_shift
        if md.bid and md.ask and md.bid < md.ask:
            fair_val = clamp(fair_val, md.bid, md.ask)
        return fair_val

    def fill_probability(self, distance_bps: Decimal, ledger: Optional[Ledger] = None) -> float:
        d = float(max(ZERO, distance_bps))
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        kappa = float(l.fill_prob_kappa if l else self.cfg.fill_prob_kappa)
        return math.exp(-kappa * d)

    def expected_adverse_move(self, side: str, md: MarketData, ledger: Ledger, now: float) -> Decimal:
        base_tox = ledger.side_tox_bps(side)
        
        ret_5s = md.ret_bps(self.cfg.trend_window_s, now)
        momentum_risk = ZERO
        if side == BUY and ret_5s < 0:
            momentum_risk = abs(ret_5s) * self.cfg.trend_widen
        elif side == SELL and ret_5s > 0:
            momentum_risk = ret_5s * self.cfg.trend_widen

        tfi = md.trade_flow_imbalance(10.0, now)
        flow_risk = ZERO
        if side == BUY and tfi < Decimal("-0.2"):
            flow_risk = abs(tfi) * Decimal("1.5")
        elif side == SELL and tfi > Decimal("0.2"):
            flow_risk = tfi * Decimal("1.5")

        cross_risk = ZERO
        if self.cfg.enable_cross_exchange and md.cross.venues:
            cross_velo = md.cross.cross_velocity_bps(3.0, now)
            if side == BUY and cross_velo < -self.cfg.cross_velocity_threshold_bps:
                cross_risk = abs(cross_velo) * Decimal("2.0")
            elif side == SELL and cross_velo > self.cfg.cross_velocity_threshold_bps:
                cross_risk = cross_velo * Decimal("2.0")

        total_adverse = base_tox + momentum_risk + flow_risk + cross_risk
        return total_adverse

    def compute_reservation_price(self, fair_value: Decimal, position_usd: Decimal,
                                  vol_bps: Decimal, ledger: Optional[Ledger] = None,
                                  target_inventory_usd: Decimal = ZERO) -> Decimal:
        if self.cfg.max_position_usd <= 0:
            return fair_value
        net_pos = position_usd - target_inventory_usd
        q = clamp(net_pos / self.cfg.max_position_usd, Decimal("-1"), Decimal("1"))
        
        q_eff = Decimal(str(math.copysign(math.pow(abs(float(q)), 1.3), float(q))))
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        skew_rate = l.skew_bps if l else self.cfg.skew_bps
        gamma_rate = l.gamma_risk_aversion if l else self.cfg.gamma_risk_aversion

        inv_skew_bps = q_eff * skew_rate
        if vol_bps > 0:
            inv_skew_bps += q_eff * vol_bps * gamma_rate

        res_price = fair_value * (ONE - inv_skew_bps / BPS)
        return res_price
    def compute_target_inventory_usd(self, md: MarketData, now: float, ledger: Optional[Ledger] = None) -> Decimal:
        """Computes optimal target inventory based on short-term alpha and funding carry."""
        if not self.cfg.enable_smart_inventory_mgmt:
            return ZERO
        
        alpha_bps = ZERO
        obi = md.obi
        tfi = md.trade_flow_imbalance(2.0, now)
        flow_signal = Decimal("0.6") * obi + Decimal("0.4") * tfi
        alpha_bps += flow_signal * Decimal("1.5")

        if self.cfg.enable_cross_exchange and md.cross.venues and md.mid:
            div = md.cross.lead_lag_divergence_bps(md.mid)
            alpha_bps += div * Decimal("0.5")

        if getattr(self.cfg, "enable_funding_carry", True) and md.info and getattr(md.info, "funding_rate", ZERO) != ZERO:
            funding_rate = md.info.funding_rate
            funding_bias = -funding_rate * getattr(self.cfg, "funding_weight", Decimal("0.5")) * BPS
            alpha_bps += funding_bias

        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        gamma = l.gamma_risk_aversion if l else self.cfg.gamma_risk_aversion
        denom = max(Decimal("0.05"), gamma * (ONE + md.vol_bps / Decimal("10.0")))
        target_usd = (alpha_bps / denom) * (self.cfg.order_usd / Decimal("10.0"))

        max_target = self.cfg.max_position_usd * Decimal("0.35")
        return clamp(target_usd, -max_target, max_target)

    def calculate_vwap_cross_cost(self, side: str, qty: Decimal, md: MarketData) -> Tuple[Decimal, Decimal]:
        """Calculates actual VWAP price and crossing cost in bps by walking the L2 book."""
        mid = md.mid
        if not mid or mid <= ZERO or qty <= ZERO:
            return (mid or ZERO, ZERO)

        depth = md._book_depth_bids if side == SELL else md._book_depth_asks
        if not depth:
            touch = md.bid if side == SELL else md.ask
            if not touch:
                return (mid, ZERO)
            slip = abs(touch - mid) / mid * BPS
            return (touch, slip + self.cfg.taker_fee_bps)

        rem = qty
        total_cost = ZERO
        for row in depth:
            px, sz = Decimal(str(row[0])), Decimal(str(row[1]))
            filled = min(rem, sz)
            total_cost += filled * px
            rem -= filled
            if rem <= ZERO:
                break

        if rem > ZERO:
            worst_px = Decimal(str(depth[-1][0]))
            penalty = Decimal("1.005") if side == BUY else Decimal("0.995")
            total_cost += rem * worst_px * penalty

        vwap = total_cost / qty
        crossing_slip_bps = abs(vwap - mid) / mid * BPS
        total_cross_bps = crossing_slip_bps + self.cfg.taker_fee_bps
        return (vwap, total_cross_bps)

    def queue_fill_probability(self, side: str, price: Decimal, md: MarketData, now: float,
                               horizon_s: float = 2.0, ledger: Optional[Ledger] = None) -> float:
        """Queue-Aware Fill Probability: Models queue depletion hazard rate lambda = consumption_rate / (queue_ahead + order_size)."""
        d = float(max(ZERO, (abs(md.mid - price) / md.mid * BPS) if md.mid else ZERO))
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        kappa = float(l.fill_prob_kappa if l else self.cfg.fill_prob_kappa)
        dist_penalty = math.exp(-kappa * d)

        if not getattr(self.cfg, "enable_queue_model", True):
            return dist_penalty

        q_ahead = float(md.queue_ahead(side, price)) if hasattr(md, "queue_ahead") else float(md.bid_sz or 1)
        cons = float(md.consumption_rate(side, 2.0, now)) if hasattr(md, "consumption_rate") else 0.0
        my_sz = float(self.cfg.order_usd / (price if price > 0 else Decimal(1)))

        denom = max(my_sz * 0.1, q_ahead + my_sz)
        lambda_fill = cons / denom
        p_queue = 1.0 - math.exp(-max(0.001, lambda_fill) * horizon_s)

        blended_p = 0.65 * p_queue + 0.35 * dist_penalty
        return float(max(0.02, min(0.98, blended_p)))


    def generate_ladder_quotes(
        self,
        m: Market,
        md: MarketData,
        ledger: Ledger,
        now: float,
        buy_blocked: bool,
        sell_blocked: bool,
        existing_slots: Optional[set] = None
    ) -> List[QuoteTarget]:
        if not md.bid or not md.ask or md.bid >= md.ask or not md.mid:
            return []

        mid = md.mid
        tick = m.tick_for(mid)
        step = m.step
        l = ledger.learner if (ledger and hasattr(ledger, "learner") and self.cfg.enable_online_learning) else None
        min_edge = l.min_edge_bps if l else self.cfg.min_edge_bps
        max_edge = l.max_edge_bps if l else self.cfg.max_edge_bps
        vol_k = l.vol_k if l else self.cfg.vol_k
        tox_mult = l.tox_mult if l else self.cfg.tox_mult
        tox_spread_mult = l.regime_toxic_spread_mult if l else self.cfg.regime_toxic_spread_mult
        spacing = l.level_spacing_bps if l else self.cfg.level_spacing_bps
        size_mult_base = l.level_size_mult if l else self.cfg.level_size_mult
        min_ev_base = l.min_ev_bps if l else self.cfg.min_ev_bps

        fair_val = self.compute_fair_value(md, now, ledger=ledger)
        pos_usd = ledger.position * mid
        target_inv_usd = self.compute_target_inventory_usd(md, now, ledger=ledger)
        res_price = self.compute_reservation_price(fair_val, pos_usd, md.vol_bps, ledger=ledger, target_inventory_usd=target_inv_usd)

        regime = md.detect_regime(now, ledger.tox_bps)
        is_toxic = (regime == "REGIME_D_TOXIC")
        
        base_edge_bps = min_edge + vol_k * md.vol_bps
        if is_toxic:
            base_edge_bps = base_edge_bps * tox_spread_mult

        if self.cfg.enable_cross_exchange and md.cross.venues:
            dispersion = md.cross.cross_dispersion_bps()
            if dispersion > Decimal("2.0"):
                dispersion_widen = (dispersion / Decimal("2.0")) * self.cfg.cross_dispersion_widen_mult
                base_edge_bps = base_edge_bps * (ONE + dispersion_widen / Decimal("10.0"))

        if self.cfg.guarantee_spread_capture:
            min_capture_edge = (Decimal("2.0") * self.cfg.maker_fee_bps) + Decimal("0.5")
            base_edge_bps = max(base_edge_bps, min_capture_edge)

        base_edge_bps = clamp(base_edge_bps, min_edge, max_edge)

        # Microstructure Flow & Asymmetric Quote Shading (Stoikov & Cartea-Jaimungal)
        obi = md.obi
        tfi = md.trade_flow_imbalance(10.0, now)
        flow_bias = Decimal("0.6") * obi + Decimal("0.4") * tfi
        half_spr = (md.ask - md.bid) / Decimal("2") if (md.bid and md.ask) else ZERO
        obi_alpha = l.obi_alpha if l else self.cfg.obi_alpha

        bid_asym_shift = max(ZERO, -flow_bias) * obi_alpha * half_spr
        ask_asym_shift = -max(ZERO, -flow_bias) * Decimal("0.3") * half_spr
        if flow_bias > 0:
            ask_asym_shift = flow_bias * obi_alpha * half_spr
            bid_asym_shift = -flow_bias * Decimal("0.3") * half_spr

        buy_tox = ledger.side_tox_bps(BUY)
        sell_tox = ledger.side_tox_bps(SELL)
        max_tox_addon = Decimal("2.5")
        buy_tox_penalty = min(max_tox_addon, buy_tox * tox_mult) if self.cfg.enable_online_learning else ZERO
        sell_tox_penalty = min(max_tox_addon, sell_tox * tox_mult) if self.cfg.enable_online_learning else ZERO

        quotes: List[QuoteTarget] = []
        stress_loss_limit = l.stress_loss_bps if l else self.cfg.stress_loss_bps
        max_hold_time = l.max_hold_s if l else self.cfg.max_hold_s

        is_stressed = (pos_usd != 0 and (
            ledger.hold_s(now) > max_hold_time or
            (ledger.unrealized(mid) / abs(pos_usd) * BPS < -stress_loss_limit)
        ))
        ret_5s = md.ret_bps(self.cfg.trend_window_s, now)
        trend_pull = l.trend_pull_bps if l else self.cfg.trend_pull_bps
        chasing_top = (ret_5s > Decimal("0.8") and (is_toxic or flow_bias > Decimal("0.3")))
        chasing_bottom = (ret_5s < Decimal("-0.8") and (is_toxic or flow_bias < Decimal("-0.3")))

                # Adverse flow detection for long (facing selling pressure) and short (facing buying pressure)
        has_adverse_selling = (tfi <= Decimal("-0.5") or (tfi <= Decimal("-0.2") and obi <= Decimal("-0.5")) or (obi <= Decimal("-0.7")) or (flow_bias <= Decimal("-0.4")))
        has_adverse_buying = (tfi >= Decimal("0.5") or (tfi >= Decimal("0.2") and obi >= Decimal("0.5")) or (obi >= Decimal("0.7")) or (flow_bias >= Decimal("0.4")))

        total_levels = 1 + max(0, self.cfg.extra_levels)

        remaining_buy_usd = max(ZERO, self.cfg.max_position_usd - pos_usd)
        remaining_sell_usd = max(ZERO, self.cfg.max_position_usd + pos_usd)

        for k in range(total_levels):
            k_spacing = Decimal(str(k)) * spacing
            if is_toxic:
                k_spacing = k_spacing * Decimal("2.0")
            level_edge = base_edge_bps + k_spacing
            
            size_mult = Decimal(str(math.pow(float(size_mult_base), k)))
            level_usd = max(self.cfg.order_usd * size_mult, m.min_notional)

            # --- BUY SIDE --- #
            is_unwind_buy = (pos_usd < 0)
            if is_unwind_buy:
                # UNWIND SHORT: Exit short with minimum profit target or breakeven shading
                if k == 0:
                    qty = q_down(abs(ledger.position), step)
                    unreal_bps = (ledger.avg_cost - mid) / ledger.avg_cost * BPS if (ledger.avg_cost and ledger.avg_cost > ZERO) else ZERO
                    pos_ratio = -pos_usd / self.cfg.max_position_usd if self.cfg.max_position_usd > ZERO else ZERO

                    has_adverse_flow = has_adverse_buying
                    adv_score = max(ZERO, tfi) * Decimal("1.5") + max(ZERO, obi) * Decimal("1.5")
                    if ret_5s > ZERO:
                        adv_score += ret_5s * Decimal("0.5")
                    if self.cfg.enable_cross_exchange and md.cross.venues:
                        cross_velo = md.cross.cross_velocity_bps(3.0, now)
                        if cross_velo > ZERO:
                            adv_score += cross_velo * Decimal("0.5")
                    adv_score += (sell_tox / Decimal("5.0"))

                    trigger_taker = False
                    if self.cfg.enable_smart_inventory_mgmt:
                        if unreal_bps < -self.cfg.stress_loss_bps:
                            trigger_taker = True
                        elif unreal_bps < -self.cfg.emergency_taker_loss_bps and (has_adverse_flow or adv_score >= self.cfg.emergency_taker_score_threshold):
                            trigger_taker = True
                        elif pos_ratio >= Decimal("0.80") and unreal_bps < -Decimal("3.0") and has_adverse_flow:
                            trigger_taker = True
                        elif ret_5s >= Decimal("3.0") and obi >= Decimal("0.70") and tfi >= Decimal("0.50") and unreal_bps < -self.cfg.taker_fee_bps:
                            trigger_taker = True

                    if trigger_taker and md.ask and qty >= m.min_size:
                        quotes.append(QuoteTarget(
                            pair_index=0, side=BUY, price=md.ask, qty=qty,
                            expected_value_bps=Decimal("-1.0"), fill_probability=1.0,
                            is_exit_quote=True, is_taker=True
                        ))
                    else:
                        min_profit_bps = max(self.cfg.exit_min_profit_bps, Decimal("1.0"))
                        hold_time = ledger.hold_s(now)
                        if hold_time <= 5.0:
                            dynamic_min_profit_bps = min_profit_bps
                        else:
                            decay = max(Decimal("0.0"), Decimal("1.0") - Decimal(str((hold_time - 5.0) / 25.0)))
                            dynamic_min_profit_bps = max(min_profit_bps * decay, Decimal("0.0"))
                        if self.cfg.enable_smart_inventory_mgmt and (has_adverse_flow or pos_ratio >= Decimal("0.70") or is_stressed or hold_time > 20.0):
                            # Aggressive maker scratch: join best bid (0 maker fee)
                            cand_px = md.bid
                            if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick:
                                cand_px = md.bid + tick
                        else:
                            min_profit_px = ledger.avg_cost * (ONE - dynamic_min_profit_bps / BPS) if (ledger.avg_cost and ledger.avg_cost > ZERO) else md.bid
                            cand_px = min(md.bid, min_profit_px)
                            if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.bid + tick) <= min_profit_px:
                                cand_px = md.bid + tick

                        cand_px = min(cand_px, md.ask - tick)
                        cand_px = q_down(cand_px, tick)
                        if md.ask and cand_px >= md.ask:
                            cand_px = md.ask - tick
                        if cand_px > ZERO and qty >= m.min_size:
                            quotes.append(QuoteTarget(
                                pair_index=0, side=BUY, price=cand_px, qty=qty,
                                expected_value_bps=Decimal("1.0"), fill_probability=0.9,
                                is_exit_quote=True
                            ))
            else:
                # ADDING LONG: Quote as long as inventory has room and side is not blocked
                severe_sell_pressure = (flow_bias < Decimal("-0.50") or (is_toxic and md.obi < Decimal("-0.55")) or has_adverse_selling)
                toxic_extra_level = (is_toxic and k > 0)
                # INVENTORY ROTATION & ANTI-CHASING: If already long, suppress touch L0 buy!
                already_long = (pos_usd >= self.cfg.order_usd * Decimal("0.5"))
                suppress_buy = (already_long and (k == 0 or has_adverse_selling or severe_sell_pressure)) or (chasing_top and k == 0)
                if getattr(self.cfg, "enable_onesided_touch", True) and k == 0 and is_toxic and flow_bias <= Decimal("-0.40"):
                    suppress_buy = True
                can_add = (not buy_blocked) and (not severe_sell_pressure) and (not toxic_extra_level) and (not suppress_buy) and (remaining_buy_usd >= level_usd)
                if can_add:
                    if k == 0 and getattr(self.cfg, "enable_selective_touch", True):
                        fragility = md.liquidity_fragility(BUY, now) if hasattr(md, "liquidity_fragility") else ZERO
                        is_fragile = bool(fragility >= getattr(self.cfg, "fragility_threshold", Decimal("0.60")))
                        is_exh = md.is_exhaustion(BUY, now) if hasattr(md, "is_exhaustion") else False

                        touch_px = (md.bid + tick) if (self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and not is_toxic and flow_bias >= Decimal("-0.2")) else md.bid
                        model_px = (res_price - bid_asym_shift) * (ONE - (level_edge + buy_tox_penalty) / BPS)
                        model_px = q_down(min(model_px, md.ask - tick), tick)

                        cand_options = [touch_px, md.bid - tick, model_px]
                        best_cand_px = cand_options[0]
                        best_ev = Decimal("-999999")
                        best_p_fill = 0.5

                        for c_px in set(cand_options):
                            c_px = min(c_px, md.ask - tick)
                            c_px = q_down(c_px, tick)
                            if c_px <= ZERO:
                                continue
                            c_cap = (fair_val - c_px) / fair_val * BPS
                            c_adv = self.expected_adverse_move(BUY, md, ledger, now)
                            if is_exh:
                                c_adv = max(ZERO, c_adv - Decimal("0.8"))
                            if is_fragile and c_px >= md.bid:
                                c_adv += Decimal("2.0") * fragility

                            c_p = self.queue_fill_probability(BUY, c_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                            fee_bps = self.cfg.maker_fee_bps
                            skew_rate = l.skew_bps if l else self.cfg.skew_bps
                            inv_cost_bps = max(ZERO, (pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate
                            pred_m = l.predict_markout(BUY, regime, k, self.cfg.queue_horizon_s) if (l and hasattr(l, "predict_markout")) else ZERO
                            c_ev = Decimal(str(c_p)) * (c_cap + pred_m - c_adv) - fee_bps - inv_cost_bps

                            if c_ev > best_ev:
                                best_ev = c_ev
                                best_cand_px = c_px
                                best_p_fill = c_p

                        cand_px = best_cand_px
                        p_fill = best_p_fill
                        ev_bps = best_ev
                    else:
                        if k == 0 and self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and not is_toxic and flow_bias >= Decimal("-0.2"):
                            cand_px = md.bid + tick
                        else:
                            cand_px = (res_price - bid_asym_shift) * (ONE - (level_edge + buy_tox_penalty) / BPS)
                        cand_px = min(cand_px, md.ask - tick)
                        cand_px = q_down(cand_px, tick)
                        if md.ask and cand_px >= md.ask:
                            cand_px = md.ask - tick
                        dist_bps = (md.ask - cand_px) / mid * BPS
                        p_fill = self.queue_fill_probability(BUY, cand_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                        capture_bps = (fair_val - cand_px) / fair_val * BPS
                        adv_bps = self.expected_adverse_move(BUY, md, ledger, now)
                        fee_bps = self.cfg.maker_fee_bps
                        skew_rate = l.skew_bps if l else self.cfg.skew_bps
                        inv_cost_bps = max(ZERO, (pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate
                        ev_bps = Decimal(str(p_fill)) * (capture_bps - adv_bps) - fee_bps - inv_cost_bps

                    if cand_px > ZERO:
                        qty = q_down(level_usd / cand_px, step)
                        if qty >= m.min_size:
                            min_ev = max(ZERO, min_ev_base - self.cfg.ev_hysteresis_bps) if (existing_slots and (k, BUY) in existing_slots) else min_ev_base
                            if (not self.cfg.enable_adaptive_ev) or (ev_bps >= min_ev):
                                quotes.append(QuoteTarget(
                                    pair_index=k, side=BUY, price=cand_px, qty=qty,
                                    expected_value_bps=ev_bps, fill_probability=p_fill,
                                    is_exit_quote=False
                                ))
                                remaining_buy_usd -= (qty * cand_px)

            # --- SELL SIDE --- #
            is_unwind_sell = (pos_usd > 0)
            if is_unwind_sell:
                # UNWIND LONG: Exit long with minimum profit target or breakeven shading
                if k == 0:
                    qty = q_down(abs(ledger.position), step)
                    unreal_bps = (mid - ledger.avg_cost) / ledger.avg_cost * BPS if (ledger.avg_cost and ledger.avg_cost > ZERO) else ZERO
                    pos_ratio = pos_usd / self.cfg.max_position_usd if self.cfg.max_position_usd > ZERO else ZERO

                    has_adverse_flow = has_adverse_selling
                    adv_score = max(ZERO, -tfi) * Decimal("1.5") + max(ZERO, -obi) * Decimal("1.5")
                    if ret_5s < ZERO:
                        adv_score += abs(ret_5s) * Decimal("0.5")
                    if self.cfg.enable_cross_exchange and md.cross.venues:
                        cross_velo = md.cross.cross_velocity_bps(3.0, now)
                        if cross_velo < ZERO:
                            adv_score += abs(cross_velo) * Decimal("0.5")
                    adv_score += (buy_tox / Decimal("5.0"))

                    trigger_taker = False
                    if self.cfg.enable_smart_inventory_mgmt:
                        if unreal_bps < -self.cfg.stress_loss_bps:
                            trigger_taker = True
                        elif unreal_bps < -self.cfg.emergency_taker_loss_bps and (has_adverse_flow or adv_score >= self.cfg.emergency_taker_score_threshold):
                            trigger_taker = True
                        elif pos_ratio >= Decimal("0.80") and unreal_bps < -Decimal("3.0") and has_adverse_flow:
                            trigger_taker = True
                        elif ret_5s <= -Decimal("3.0") and obi <= Decimal("-0.70") and tfi <= Decimal("-0.50") and unreal_bps < -self.cfg.taker_fee_bps:
                            trigger_taker = True

                    if trigger_taker and md.bid and qty >= m.min_size:
                        quotes.append(QuoteTarget(
                            pair_index=0, side=SELL, price=md.bid, qty=qty,
                            expected_value_bps=Decimal("-1.0"), fill_probability=1.0,
                            is_exit_quote=True, is_taker=True
                        ))
                    else:
                        min_profit_bps = max(self.cfg.exit_min_profit_bps, Decimal("1.0"))
                        hold_time = ledger.hold_s(now)
                        if hold_time <= 5.0:
                            dynamic_min_profit_bps = min_profit_bps
                        else:
                            decay = max(Decimal("0.0"), Decimal("1.0") - Decimal(str((hold_time - 5.0) / 25.0)))
                            dynamic_min_profit_bps = max(min_profit_bps * decay, Decimal("0.0"))
                        if self.cfg.enable_smart_inventory_mgmt and (has_adverse_flow or pos_ratio >= Decimal("0.70") or is_stressed or hold_time > 20.0):
                            # Aggressive maker scratch: join best ask (0 maker fee)
                            cand_px = md.ask
                            if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick:
                                cand_px = md.ask - tick
                        else:
                            min_profit_px = ledger.avg_cost * (ONE + dynamic_min_profit_bps / BPS) if (ledger.avg_cost and ledger.avg_cost > ZERO) else md.ask
                            cand_px = max(md.ask, min_profit_px)
                            if self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and (md.ask - tick) >= min_profit_px:
                                cand_px = md.ask - tick

                        cand_px = max(cand_px, md.bid + tick)
                        cand_px = q_up(cand_px, tick)
                        if md.bid and cand_px <= md.bid:
                            cand_px = md.bid + tick
                        if cand_px > ZERO and qty >= m.min_size:
                            quotes.append(QuoteTarget(
                                pair_index=0, side=SELL, price=cand_px, qty=qty,
                                expected_value_bps=Decimal("1.0"), fill_probability=0.9,
                                is_exit_quote=True
                            ))
            else:
                # ADDING SHORT: Quote as long as inventory has room and side is not blocked
                severe_buy_pressure = (flow_bias > Decimal("0.50") or (is_toxic and md.obi > Decimal("0.55")) or has_adverse_buying)
                toxic_extra_level = (is_toxic and k > 0)
                # INVENTORY ROTATION & ANTI-CHASING: If already short, suppress touch L0 sell!
                already_short = (-pos_usd >= self.cfg.order_usd * Decimal("0.5"))
                suppress_sell = (already_short and (k == 0 or has_adverse_buying or severe_buy_pressure)) or (chasing_bottom and k == 0)
                if getattr(self.cfg, "enable_onesided_touch", True) and k == 0 and is_toxic and flow_bias >= Decimal("0.40"):
                    suppress_sell = True
                can_add = (not sell_blocked) and (not severe_buy_pressure) and (not toxic_extra_level) and (not suppress_sell) and (remaining_sell_usd >= level_usd)
                if can_add:
                    if k == 0 and getattr(self.cfg, "enable_selective_touch", True):
                        fragility = md.liquidity_fragility(SELL, now) if hasattr(md, "liquidity_fragility") else ZERO
                        is_fragile = bool(fragility >= getattr(self.cfg, "fragility_threshold", Decimal("0.60")))
                        is_exh = md.is_exhaustion(SELL, now) if hasattr(md, "is_exhaustion") else False

                        touch_px = (md.ask - tick) if (self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and not is_toxic and flow_bias <= Decimal("0.2")) else md.ask
                        model_px = (res_price + ask_asym_shift) * (ONE + (level_edge + sell_tox_penalty) / BPS)
                        model_px = q_up(max(model_px, md.bid + tick), tick)

                        cand_options = [touch_px, md.ask + tick, model_px]
                        best_cand_px = cand_options[0]
                        best_ev = Decimal("-999999")
                        best_p_fill = 0.5

                        for c_px in set(cand_options):
                            c_px = max(c_px, md.bid + tick)
                            c_px = q_up(c_px, tick)
                            if c_px <= ZERO:
                                continue
                            c_cap = (c_px - fair_val) / fair_val * BPS
                            c_adv = self.expected_adverse_move(SELL, md, ledger, now)
                            if is_exh:
                                c_adv = max(ZERO, c_adv - Decimal("0.8"))
                            if is_fragile and c_px <= md.ask:
                                c_adv += Decimal("2.0") * fragility

                            c_p = self.queue_fill_probability(SELL, c_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                            fee_bps = self.cfg.maker_fee_bps
                            skew_rate = l.skew_bps if l else self.cfg.skew_bps
                            inv_cost_bps = max(ZERO, -(pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate
                            pred_m = l.predict_markout(SELL, regime, k, self.cfg.queue_horizon_s) if (l and hasattr(l, "predict_markout")) else ZERO
                            c_ev = Decimal(str(c_p)) * (c_cap + pred_m - c_adv) - fee_bps - inv_cost_bps

                            if c_ev > best_ev:
                                best_ev = c_ev
                                best_cand_px = c_px
                                best_p_fill = c_p

                        cand_px = best_cand_px
                        p_fill = best_p_fill
                        ev_bps = best_ev
                    else:
                        if k == 0 and self.cfg.penny and (md.ask - md.bid) > Decimal("2") * tick and not is_toxic and flow_bias <= Decimal("0.2"):
                            cand_px = md.ask - tick
                        else:
                            cand_px = (res_price + ask_asym_shift) * (ONE + (level_edge + sell_tox_penalty) / BPS)
                        cand_px = max(cand_px, md.bid + tick)
                        cand_px = q_up(cand_px, tick)
                        if md.bid and cand_px <= md.bid:
                            cand_px = md.bid + tick
                        dist_bps = (cand_px - md.bid) / mid * BPS
                        p_fill = self.queue_fill_probability(SELL, cand_px, md, now, self.cfg.queue_horizon_s, ledger=ledger)
                        capture_bps = (cand_px - fair_val) / fair_val * BPS
                        adv_bps = self.expected_adverse_move(SELL, md, ledger, now)
                        fee_bps = self.cfg.maker_fee_bps
                        skew_rate = l.skew_bps if l else self.cfg.skew_bps
                        inv_cost_bps = max(ZERO, -(pos_usd - target_inv_usd) / self.cfg.max_position_usd) * skew_rate
                        ev_bps = Decimal(str(p_fill)) * (capture_bps - adv_bps) - fee_bps - inv_cost_bps

                    if cand_px > ZERO:
                        qty = q_down(level_usd / cand_px, step)
                        if qty >= m.min_size:
                            min_ev = max(ZERO, min_ev_base - self.cfg.ev_hysteresis_bps) if (existing_slots and (k, SELL) in existing_slots) else min_ev_base
                            if (not self.cfg.enable_adaptive_ev) or (ev_bps >= min_ev):
                                quotes.append(QuoteTarget(
                                    pair_index=k, side=SELL, price=cand_px, qty=qty,
                                    expected_value_bps=ev_bps, fill_probability=p_fill,
                                    is_exit_quote=False
                                ))
                                remaining_sell_usd -= (qty * cand_px)

        return quotes
