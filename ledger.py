"""Fill-driven accounting + Level 6 Online Learning of Adverse Selection."""
from __future__ import annotations

import time
import math

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, List, Tuple

import json
import logging
import os
import tempfile
from typing import Dict, Any
from utils import BPS, BUY, SELL, ZERO, ONE, clamp, fmt

log = logging.getLogger("ledger")


@dataclass
class Fill:
    ts: float
    side: str
    qty: Decimal
    price: Decimal
    mid: Decimal
    edge_bps: Decimal
    position: Decimal
    realized_delta: Decimal



class ConditionalMarkoutModel:
    """Empirical Bayesian conditional markout prediction model.
    Learns E[markout | side, regime, level, horizon] with shrinkage toward prior.
    """
    def __init__(self, prior_weight: int = 5):
        self.prior_weight = prior_weight
        self.history: dict[tuple, list[float]] = {}
        self.priors = {
            ("BUY", "REGIME_A_QUIET", 0): Decimal("0.5"),
            ("SELL", "REGIME_A_QUIET", 0): Decimal("0.5"),
            ("BUY", "REGIME_B_HIGH_VOL", 0): Decimal("-0.5"),
            ("SELL", "REGIME_B_HIGH_VOL", 0): Decimal("-0.5"),
            ("BUY", "REGIME_C_TREND", 0): Decimal("-1.0"),
            ("SELL", "REGIME_C_TREND", 0): Decimal("-1.0"),
            ("BUY", "REGIME_D_TOXIC", 0): Decimal("-2.5"),
            ("SELL", "REGIME_D_TOXIC", 0): Decimal("-2.5"),
        }

    def record(self, side: str, regime: str, level: int, horizon: float, markout_bps: float) -> None:
        key = (side, regime, level, round(horizon, 1))
        if key not in self.history:
            self.history[key] = []
        self.history[key].append(markout_bps)
        if len(self.history[key]) > 100:
            self.history[key].pop(0)

    def predict(self, side: str, regime: str, level: int, horizon: float = 2.0) -> Decimal:
        key = (side, regime, level, round(horizon, 1))
        samples = self.history.get(key, [])
        prior = self.priors.get((side, regime, level), Decimal("0.0"))

        n = len(samples)
        if n == 0:
            return prior

        sample_mean = Decimal(str(sum(samples) / n))
        w = Decimal(str(n)) / (Decimal(str(n)) + Decimal(str(self.prior_weight)))
        return w * sample_mean + (Decimal("1") - w) * prior

class OnlineLearner:
    """Level 7+ Autonomous Online Learning Engine.
    Dynamically modulates ALL market making environment parameters, microstructural
    asymmetry margins, risk penalties, and inventory aversion based on real-time execution feedback."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.enabled = bool(getattr(cfg, "enable_online_learning", False))
        self.state_path = getattr(cfg, "learning_state_path", "learning_state.json")

        # Base configuration defaults (loaded from environment)
        self.base = {
            "min_edge_bps": Decimal(str(cfg.min_edge_bps)),
            "max_edge_bps": Decimal(str(cfg.max_edge_bps)),
            "skew_bps": Decimal(str(cfg.skew_bps)),
            "level_spacing_bps": Decimal(str(cfg.level_spacing_bps)),
            "level_size_mult": Decimal(str(cfg.level_size_mult)),
            "vol_k": Decimal(str(cfg.vol_k)),
            "tox_mult": Decimal(str(cfg.tox_mult)),
            "min_ev_bps": Decimal(str(cfg.min_ev_bps)),
            "obi_alpha": Decimal(str(cfg.obi_alpha)),
            "tfi_beta": Decimal(str(cfg.tfi_beta)),
            "fill_prob_kappa": Decimal(str(cfg.fill_prob_kappa)),
            "gamma_risk_aversion": Decimal(str(cfg.gamma_risk_aversion)),
            "regime_toxic_spread_mult": Decimal(str(cfg.regime_toxic_spread_mult)),
            "trend_pull_bps": Decimal(str(cfg.trend_pull_bps)),
            "trend_widen": Decimal(str(cfg.trend_widen)),
            "exit_min_profit_bps": Decimal(str(cfg.exit_min_profit_bps)),
            "stress_loss_bps": Decimal(str(cfg.stress_loss_bps)),
            "max_hold_s": Decimal(str(cfg.max_hold_s)),
            "burst_fills": Decimal(str(cfg.burst_fills)),
            "burst_cooldown_s": Decimal(str(cfg.burst_cooldown_s)),
            "sweep_guard_fills": Decimal(str(cfg.sweep_guard_fills)),
            "sweep_guard_window_s": Decimal(str(cfg.sweep_guard_window_s)),
        }

        self._last_decay_ts: Optional[float] = None
        self._last_feedback_ts: float = 0.0
        self._uncommitted_updates: int = 0
        self.markout_model = ConditionalMarkoutModel(prior_weight=int(getattr(cfg, "empirical_prior_weight", 5)))

        # Hard mathematical & safety bounds [min_val, max_val]
        self.bounds = {
            "min_edge_bps": (max(Decimal("0.2"), self.base["min_edge_bps"]), Decimal("6.0")),
            "max_edge_bps": (Decimal("2.0"), Decimal("20.0")),
            "skew_bps": (Decimal("0.5"), Decimal("35.0")),
            "level_spacing_bps": (Decimal("1.0"), Decimal("15.0")),
            "level_size_mult": (Decimal("0.10"), Decimal("0.95")),
            "vol_k": (Decimal("0.1"), Decimal("4.0")),
            "tox_mult": (Decimal("0.2"), Decimal("2.5")),
            "min_ev_bps": (Decimal("0.05"), Decimal("0.40")),
            "obi_alpha": (Decimal("0.05"), Decimal("1.5")),
            "tfi_beta": (Decimal("0.05"), Decimal("2.0")),
            "fill_prob_kappa": (Decimal("0.05"), Decimal("1.5")),
            "gamma_risk_aversion": (Decimal("0.01"), Decimal("1.5")),
            "regime_toxic_spread_mult": (Decimal("1.1"), Decimal("4.0")),
            "trend_pull_bps": (Decimal("0.5"), Decimal("10.0")),
            "trend_widen": (Decimal("0.2"), Decimal("4.0")),
            "exit_min_profit_bps": (Decimal("0.5"), Decimal("10.0")),
            "stress_loss_bps": (Decimal("10.0"), Decimal("60.0")),
            "max_hold_s": (Decimal("60.0"), Decimal("1200.0")),
            "burst_fills": (Decimal("2"), Decimal("5")),
            "burst_cooldown_s": (Decimal("10.0"), Decimal("90.0")),
            "sweep_guard_fills": (Decimal("2"), Decimal("4")),
            "sweep_guard_window_s": (Decimal("0.5"), Decimal("2.5")),
        }

        # Current live parameters initialized to base values
        self.params: Dict[str, Decimal] = dict(self.base)

        # Performance & learning statistics
        self.n_markouts = 0
        self.n_toxic = 0
        self.n_benign = 0
        self.n_fills = 0
        self.total_learned_updates = 0
        self.cumulative_spread_captured = Decimal("0")
        self.last_change_reason: str = "none"
        self.last_changes: List[str] = []
        self.ledger: Any = None

        if self.enabled:
            self.load()

    # --- Property Accessors for Engine & Bot --- #
    @property
    def min_edge_bps(self) -> Decimal:
        return self.params["min_edge_bps"] if self.enabled else self.base["min_edge_bps"]

    @property
    def max_edge_bps(self) -> Decimal:
        return self.params["max_edge_bps"] if self.enabled else self.base["max_edge_bps"]

    @property
    def skew_bps(self) -> Decimal:
        return self.params["skew_bps"] if self.enabled else self.base["skew_bps"]

    @property
    def level_spacing_bps(self) -> Decimal:
        return self.params["level_spacing_bps"] if self.enabled else self.base["level_spacing_bps"]

    @property
    def level_size_mult(self) -> Decimal:
        return self.params["level_size_mult"] if self.enabled else self.base["level_size_mult"]

    @property
    def vol_k(self) -> Decimal:
        return self.params["vol_k"] if self.enabled else self.base["vol_k"]

    @property
    def tox_mult(self) -> Decimal:
        return self.params["tox_mult"] if self.enabled else self.base["tox_mult"]

    @property
    def min_ev_bps(self) -> Decimal:
        return self.params["min_ev_bps"] if self.enabled else self.base["min_ev_bps"]

    @property
    def obi_alpha(self) -> Decimal:
        return self.params["obi_alpha"] if self.enabled else self.base["obi_alpha"]

    @property
    def tfi_beta(self) -> Decimal:
        return self.params["tfi_beta"] if self.enabled else self.base["tfi_beta"]

    @property
    def fill_prob_kappa(self) -> Decimal:
        return self.params["fill_prob_kappa"] if self.enabled else self.base["fill_prob_kappa"]

    @property
    def gamma_risk_aversion(self) -> Decimal:
        return self.params["gamma_risk_aversion"] if self.enabled else self.base["gamma_risk_aversion"]

    @property
    def regime_toxic_spread_mult(self) -> Decimal:
        return self.params["regime_toxic_spread_mult"] if self.enabled else self.base["regime_toxic_spread_mult"]

    @property
    def trend_pull_bps(self) -> Decimal:
        return self.params["trend_pull_bps"] if self.enabled else self.base["trend_pull_bps"]

    @property
    def trend_widen(self) -> Decimal:
        return self.params["trend_widen"] if self.enabled else self.base["trend_widen"]

    @property
    def exit_min_profit_bps(self) -> Decimal:
        return self.params["exit_min_profit_bps"] if self.enabled else self.base["exit_min_profit_bps"]

    @property
    def stress_loss_bps(self) -> Decimal:
        return self.params["stress_loss_bps"] if self.enabled else self.base["stress_loss_bps"]

    @property
    def max_hold_s(self) -> float:
        return float(self.params["max_hold_s"]) if self.enabled else float(self.base["max_hold_s"])

    @property
    def burst_fills(self) -> int:
        return int(self.params["burst_fills"]) if self.enabled else int(self.base["burst_fills"])

    @property
    def burst_cooldown_s(self) -> float:
        return float(self.params["burst_cooldown_s"]) if self.enabled else float(self.base["burst_cooldown_s"])

    @property
    def sweep_guard_fills(self) -> int:
        return int(self.params["sweep_guard_fills"]) if self.enabled else int(self.base["sweep_guard_fills"])

    @property
    def sweep_guard_window_s(self) -> float:
        return float(self.params["sweep_guard_window_s"]) if self.enabled else float(self.base["sweep_guard_window_s"])

    @property
    def win_rate(self) -> float:
        total = self.n_benign + self.n_toxic
        return (float(self.n_benign) / float(total) * 100.0) if total > 0 else 0.0

    @property
    def adverse_fill_rate(self) -> float:
        total = self.n_benign + self.n_toxic
        return (float(self.n_toxic) / float(total) * 100.0) if total > 0 else 0.0

    def _log_param_diff(self, reason: str, old_params: Dict[str, Decimal], context: Optional[Dict[str, str]] = None) -> None:
        name_map = {
            "level_spacing_bps": "spacing",
            "obi_alpha": "obi_a",
            "tfi_beta": "tfi_b",
            "min_edge_bps": "min_edge",
            "max_edge_bps": "max_edge",
            "skew_bps": "skew",
            "level_size_mult": "mult",
            "vol_k": "vol_k",
            "tox_mult": "tox_mult",
            "min_ev_bps": "min_ev",
            "fill_prob_kappa": "kappa",
            "gamma_risk_aversion": "gamma",
            "regime_toxic_spread_mult": "toxic_mult",
            "trend_pull_bps": "trend_pull",
            "trend_widen": "trend_widen",
            "exit_min_profit_bps": "exit_profit",
            "stress_loss_bps": "stress_loss",
            "max_hold_s": "max_hold",
            "burst_cooldown_s": "burst_cd",
            "sweep_guard_fills": "sweep_fills",
            "burst_fills": "burst_fills",
        }

        diffs = []
        for k, v in self.params.items():
            old_v = old_params.get(k, v)
            if abs(v - old_v) >= Decimal("0.005"):
                short_name = name_map.get(k, k)
                diffs.append((short_name, old_v, v))

        if not diffs:
            return

        self.last_change_reason = reason
        self.last_changes = [f"{name}: {float(old):.2f} -> {float(new):.2f}" for name, old, new in diffs]

        if log.isEnabledFor(logging.DEBUG):
            lines = ["LEARN"]
            for name, old, new in diffs:
                lines.append(f"{name + ':':<8} {float(old):.2f} -> {float(new):.2f}")
            lines.append(f"{'reason:':<8} {reason}")
            if context:
                for k, val in context.items():
                    lines.append(f"{k}: {val}")
            nl = chr(10)
            log.debug(nl + nl.join(lines))

    def tick_decay(self, now: float) -> None:
        """Gradually relaxes learned parameters toward base config during idle/no-fill periods."""
        if not self.enabled:
            return
        if self._last_decay_ts is None:
            self._last_decay_ts = now
            return
        # Only decay if at least 10s of quiet time without active fills or markouts
        if self._last_feedback_ts > 0 and (now - self._last_feedback_ts < 10.0):
            self._last_decay_ts = now
            return
        dt = now - self._last_decay_ts
        if dt <= 0:
            return
        self._last_decay_ts = now

        old_params = dict(self.params)
        decay_factor = Decimal(str(math.exp(-dt / 60.0)))
        for k in self.params:
            if k in self.base:
                diff = self.params[k] - self.base[k]
                self.params[k] = self.base[k] + diff * decay_factor
        self._clamp_all()
        if any(abs(self.params[k] - old_params.get(k, self.params[k])) >= Decimal("0.05") for k in self.params):
            self._log_param_diff("idle_decay", old_params, {"idle_s": f"{dt:.1f}s"})

    decay_idle = tick_decay

    def _clamp_all(self) -> None:
        for k in self.params:
            if k in self.bounds:
                lo, hi = self.bounds[k]
                self.params[k] = clamp(self.params[k], lo, hi)
        if self.params["max_edge_bps"] < self.params["min_edge_bps"] + Decimal("2.0"):
            self.params["max_edge_bps"] = min(self.bounds["max_edge_bps"][1], self.params["min_edge_bps"] + Decimal("2.0"))

    def on_markout(self, m_bps: Decimal, side: str, tox_bps: Decimal, horizon: float = 5.0,
                   now: Optional[float] = None, regime: str = "REGIME_A_QUIET") -> None:
        """Adapts edges, spreads, spacing, EV cutoffs, and adverse defenses based on markout evaluation."""
        if not self.enabled:
            return

        self._last_feedback_ts = now if now is not None else time.time()
        self.markout_model.record(side, regime, 0, horizon, float(m_bps))
        self.n_markouts += 1
        self.total_learned_updates += 1
        old_params = dict(self.params)
        h_str = f"{int(horizon)}s" if horizon else "markout"

        if m_bps < 0:
            # Adverse selection detected (toxic fill where price moved against us)
            self.n_toxic += 1
            severity = min(Decimal("3.0"), abs(m_bps) / Decimal("5.0"))
            reason = f"negative_{h_str}_markout"

            # 1. Widen quoting edges & ladder defenses (bounded safely)
            self.params["min_edge_bps"] += Decimal("0.20") * severity
            self.params["max_edge_bps"] += Decimal("0.40") * severity
            self.params["level_spacing_bps"] += Decimal("0.15") * severity
            self.params["level_size_mult"] -= Decimal("0.02") * severity

            # 2. Sharpen toxicity scaling, momentum penalties & EV hurdle
            self.params["tox_mult"] += Decimal("0.05") * severity
            self.params["min_ev_bps"] += Decimal("0.02") * severity
            self.params["regime_toxic_spread_mult"] += Decimal("0.03") * severity
            self.params["trend_widen"] += Decimal("0.03") * severity
            self.params["vol_k"] += Decimal("0.02") * severity

            # 3. Increase order-book / flow sensitivity to protect against informed flow
            self.params["obi_alpha"] += Decimal("0.02") * severity
            self.params["tfi_beta"] += Decimal("0.03") * severity

            # 4. Increase exit profit expectation to recover adverse costs
            self.params["exit_min_profit_bps"] += Decimal("0.05") * severity

            # 5. Tighten burst protection
            self.params["burst_cooldown_s"] += Decimal("1.5") * severity
        else:
            # Profitable, benign markout
            self.n_benign += 1
            decay = Decimal("0.10")
            reason = f"positive_{h_str}_markout"

            self.params["min_edge_bps"] -= (self.params["min_edge_bps"] - self.base["min_edge_bps"]) * decay
            self.params["max_edge_bps"] -= (self.params["max_edge_bps"] - self.base["max_edge_bps"]) * decay
            self.params["level_spacing_bps"] -= (self.params["level_spacing_bps"] - self.base["level_spacing_bps"]) * decay
            self.params["level_size_mult"] += (self.base["level_size_mult"] - self.params["level_size_mult"]) * decay
            self.params["tox_mult"] -= (self.params["tox_mult"] - self.base["tox_mult"]) * decay
            self.params["min_ev_bps"] -= (self.params["min_ev_bps"] - self.base["min_ev_bps"]) * decay
            self.params["regime_toxic_spread_mult"] -= (self.params["regime_toxic_spread_mult"] - self.base["regime_toxic_spread_mult"]) * decay
            self.params["vol_k"] -= (self.params["vol_k"] - self.base["vol_k"]) * decay
            self.params["trend_widen"] -= (self.params["trend_widen"] - self.base["trend_widen"]) * decay
            self.params["obi_alpha"] -= (self.params["obi_alpha"] - self.base["obi_alpha"]) * decay
            self.params["tfi_beta"] -= (self.params["tfi_beta"] - self.base["tfi_beta"]) * decay

        self._clamp_all()
        ctx = {f"markout_{h_str}": f"{float(m_bps):+.2f}bps"}
        self._log_param_diff(reason, old_params, ctx)
        self.save()


    def predict_markout(self, side: str, regime: str, level: int = 0, horizon: float = 2.0) -> Decimal:
        """Empirical prediction of expected post-fill markout in basis points conditional on market state."""
        if not getattr(self.cfg, "enable_empirical_learner", True):
            return ZERO
        return self.markout_model.predict(side, regime, level, horizon)

    def on_fill(self, side: str, price: Decimal, mid: Decimal, pos_usd: Decimal, hold_s: float,
                now: Optional[float] = None) -> None:
        """Adapts inventory skew, risk aversion, and fill-probability kappa upon execution."""
        if not self.enabled:
            return
        self._last_feedback_ts = now if now is not None else time.time()

        self.n_fills += 1
        self.total_learned_updates += 1
        old_params = dict(self.params)
        reason = "fill_inventory"

        # 1. Fill distance calibration for fill probability model P(fill) = exp(-kappa * dist)
        if mid and mid > 0:
            dist_bps = abs(price - mid) / mid * BPS
            if dist_bps > Decimal("1.5"):
                self.params["fill_prob_kappa"] -= Decimal("0.005")
            elif dist_bps < Decimal("0.2"):
                self.params["fill_prob_kappa"] += Decimal("0.002")

        # 2. Inventory holding duration & skew adaptation
        max_pos = Decimal(str(self.cfg.max_position_usd))
        pos_ratio = abs(pos_usd) / max_pos if max_pos > 0 else ZERO

        ctx = {"hold_s": f"{hold_s:.1f}s", "pos_ratio": f"{float(pos_ratio):.2f}"}

        if hold_s > 45.0 or pos_ratio > Decimal("0.5"):
            self.params["skew_bps"] += Decimal("0.35")
            self.params["gamma_risk_aversion"] += Decimal("0.02")
            reason = "inventory_stagnant"
        elif pos_ratio < Decimal("0.15"):
            self.params["skew_bps"] -= (self.params["skew_bps"] - self.base["skew_bps"]) * Decimal("0.05")
            self.params["gamma_risk_aversion"] -= (self.params["gamma_risk_aversion"] - self.base["gamma_risk_aversion"]) * Decimal("0.05")
            reason = "inventory_rebalanced"

        self._clamp_all()
        self._log_param_diff(reason, old_params, ctx)
        self.save()

    def on_flow_correlation(self, obi: Decimal, tfi: Decimal, ret_bps: Decimal) -> None:
        """Adapts order-book and trade-flow imbalance weights based on forward price prediction accuracy."""
        if not self.enabled:
            return

        old_params = dict(self.params)
        reason = "flow_correlation"

        if abs(obi) > Decimal("0.2") and abs(ret_bps) > Decimal("0.1"):
            if (obi > 0 and ret_bps > 0) or (obi < 0 and ret_bps < 0):
                self.params["obi_alpha"] += Decimal("0.02")
                reason = "flow_predictive_obi"
            else:
                self.params["obi_alpha"] -= Decimal("0.02")
                reason = "flow_divergent_obi"

        if abs(tfi) > Decimal("0.2") and abs(ret_bps) > Decimal("0.1"):
            if (tfi > 0 and ret_bps > 0) or (tfi < 0 and ret_bps < 0):
                self.params["tfi_beta"] += Decimal("0.02")
                reason = "flow_predictive_tfi"
            else:
                self.params["tfi_beta"] -= Decimal("0.02")
                reason = "flow_divergent_tfi"

        self._clamp_all()
        self._log_param_diff(reason, old_params, {"obi": f"{float(obi):.2f}", "ret_bps": f"{float(ret_bps):.2f}"})

    def on_spread_turnover(self, realized_bps: Decimal) -> None:
        """Adapts exit profit target based on realized turnover profitability."""
        if not self.enabled:
            return
        old_params = dict(self.params)
        reason = "spread_turnover"
        if realized_bps < Decimal("0.5"):
            self.params["exit_min_profit_bps"] += Decimal("0.15")
            reason = "turnover_low_margin"
        elif realized_bps > Decimal("2.0"):
            self.params["exit_min_profit_bps"] -= (self.params["exit_min_profit_bps"] - self.base["exit_min_profit_bps"]) * Decimal("0.05")
            reason = "turnover_healthy_margin"
        self._clamp_all()
        self._log_param_diff(reason, old_params, {"realized_bps": f"{float(realized_bps):.2f}"})
        self.save()

    def get_summary(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "total_updates": self.total_learned_updates,
            "markouts": self.n_markouts,
            "toxic_fills": self.n_toxic,
            "benign_fills": self.n_benign,
            "fills": self.n_fills,
            "win_rate": self.win_rate,
            "adverse_fill_rate": self.adverse_fill_rate,
            "last_change_reason": self.last_change_reason,
            "params": {k: f"{v:.4f}" for k, v in self.params.items()},
        }

    def save(self, path: Optional[str] = None) -> bool:
        """Persists learned parameters atomically to disk."""
        target_path = path or self.state_path
        if not target_path or target_path == os.devnull:
            return False

        data = {
            "version": 1,
            "market": getattr(self.cfg, "market", "UNKNOWN"),
            "enabled": self.enabled,
            "total_updates": self.total_learned_updates,
            "n_markouts": self.n_markouts,
            "n_toxic": self.n_toxic,
            "n_benign": self.n_benign,
            "n_fills": self.n_fills,
            "last_change_reason": self.last_change_reason,
            "params": {k: str(v) for k, v in self.params.items()},
        }

        try:
            dir_name = os.path.dirname(os.path.abspath(target_path)) or "."
            fd, tmp_file = tempfile.mkstemp(dir=dir_name, prefix="learning_tmp_")
            with os.fdopen(fd, "w") as fp:
                json.dump(data, fp, indent=2)
            os.replace(tmp_file, target_path)
            return True
        except Exception:
            return False

    def load(self, path: Optional[str] = None) -> bool:
        """Loads previously saved learned parameters."""
        target_path = path or self.state_path
        if not os.path.exists(target_path):
            return False

        try:
            with open(target_path, "r") as fp:
                data = json.load(fp)

            if isinstance(data, dict) and "params" in data:
                saved_market = data.get("market")
                current_market = getattr(self.cfg, "market", None)
                if saved_market and current_market and saved_market != current_market:
                    log.info("Learned state was for %s, current market is %s — resetting to base configuration",
                             saved_market, current_market)
                    return False
                for k, v in data["params"].items():
                    if k in self.params:
                        self.params[k] = Decimal(str(v))
                if "min_edge_bps" in self.params:
                    self.params["min_edge_bps"] = max(self.params["min_edge_bps"], self.base["min_edge_bps"])
                self.n_markouts = int(data.get("n_markouts", 0))
                self.n_toxic = int(data.get("n_toxic", 0))
                self.n_benign = int(data.get("n_benign", 0))
                self.n_fills = int(data.get("n_fills", 0))
                self.total_learned_updates = int(data.get("total_updates", 0))
                self.last_change_reason = str(data.get("last_change_reason", "none"))
                self._clamp_all()
                return True
        except Exception:
            pass
        return False

class Ledger:
    def __init__(self, cfg):
        self.cfg = cfg
        self.position = ZERO
        self.avg_cost = ZERO
        self.realized = ZERO
        self.fees = ZERO
        self.spread_capture = ZERO
        self.spread_edge_bps_sum = ZERO
        self.volume_usd = ZERO
        self.n_fills = 0
        self.n_buys = 0
        self.n_sells = 0
        self.fills: deque = deque(maxlen=500)
        self.opened_ts: Optional[float] = None
        self.last_fill_ts = -1e9
        
        self._pending_markouts: list = []
        self.markouts: deque = deque(maxlen=cfg.markout_window * 2)
        self.markouts_buy: deque = deque(maxlen=cfg.markout_window)
        self.markouts_sell: deque = deque(maxlen=cfg.markout_window)
        self.markouts_1s: deque = deque(maxlen=cfg.markout_window * 2)
        self.markouts_5s: deque = deque(maxlen=cfg.markout_window * 2)
        self.latest_markout_1s: Optional[Decimal] = None
        self.latest_markout_5s: Optional[Decimal] = None
        self.markouts_500ms: deque = deque(maxlen=cfg.markout_window * 2)
        self.markouts_2s: deque = deque(maxlen=cfg.markout_window * 2)
        self.latest_markout_500ms: Optional[Decimal] = None
        self.latest_markout_2s: Optional[Decimal] = None
        self.funding_pnl: Decimal = ZERO
        self._mismatch = 0
        self.last_now: float = time.time()
        self.learner = OnlineLearner(cfg)
        self.learner.ledger = self

    def is_flat(self, mid: Decimal, min_notional: Decimal) -> bool:
        return abs(self.position * mid) < max(min_notional, Decimal(1))

    def hold_s(self, now: float) -> float:
        return now - self.opened_ts if self.opened_ts is not None else 0.0

    def unrealized(self, mark: Decimal) -> Decimal:
        return (mark - self.avg_cost) * self.position if self.position != 0 else ZERO

    def total_pnl(self, mark: Decimal) -> Decimal:
        return self.realized + self.unrealized(mark) + getattr(self, "funding_pnl", ZERO)

    def apply_funding(self, pmt: Decimal) -> None:
        """Applies a funding payment (+ for received, - for paid)."""
        self.funding_pnl += pmt

    def inventory_pnl(self, mark: Decimal) -> Decimal:
        return self.total_pnl(mark) - self.spread_capture

    @property
    def avg_edge_bps(self) -> Decimal:
        return (self.spread_edge_bps_sum / self.n_fills) if self.n_fills else ZERO

    def on_fill(self, side: str, qty: Decimal, price: Decimal, mid: Decimal, now: float,
                min_notional: Decimal, is_maker: bool = True) -> Fill:
        signed = qty if side == BUY else -qty
        was_flat = self.is_flat(mid, min_notional)
        realized_delta = ZERO
        if self.position == 0 or (self.position > 0) == (signed > 0):
            total = abs(self.position) + qty
            self.avg_cost = (self.avg_cost * abs(self.position) + price * qty) / total
            self.position += signed
        else:
            closed = min(abs(self.position), qty)
            direction = 1 if self.position > 0 else -1
            realized_delta = (price - self.avg_cost) * closed * direction
            new_pos = self.position + signed
            if new_pos == 0:
                self.avg_cost = ZERO
            elif (new_pos > 0) != (self.position > 0):
                self.avg_cost = price
            self.position = new_pos
        fee_rate = self.cfg.maker_fee_bps if is_maker else self.cfg.taker_fee_bps
        fee = qty * price * fee_rate / BPS
        realized_delta -= fee
        self.fees += fee
        self.realized += realized_delta

        edge = (mid - price) if side == BUY else (price - mid)
        edge_bps = edge / mid * BPS if mid else ZERO
        if is_maker:
            self.spread_capture += edge * qty
            self.spread_edge_bps_sum += edge_bps
        self.volume_usd += qty * price
        self.n_fills += 1
        self.n_buys += (side == BUY)
        self.n_sells += (side != BUY)
        self.last_fill_ts = now

        now_flat = self.is_flat(mid, min_notional)
        if was_flat and not now_flat:
            self.opened_ts = now
        elif now_flat:
            self.opened_ts = None

        self.last_now = now
        f = Fill(now, side, qty, price, mid, edge_bps, self.position, realized_delta)
        self.fills.append(f)
        self._pending_markouts.append((now + 0.5, f, 0.5))
        self._pending_markouts.append((now + 1.0, f, 1.0))
        self._pending_markouts.append((now + 2.0, f, 2.0))
        self._pending_markouts.append((now + 5.0, f, 5.0))
        h = float(getattr(self.cfg, "markout_horizon_s", 5.0))
        if abs(h - 1.0) > 0.05 and abs(h - 5.0) > 0.05:
            self._pending_markouts.append((now + h, f, h))
        self._pending_markouts.sort(key=lambda x: x[0])
        self.learner.on_fill(side, price, mid, self.position * mid, self.hold_s(now), now=now)
        return f

    def _now(self) -> float:
        if hasattr(self, "current_now") and self.current_now is not None:
            return float(self.current_now)
        if hasattr(self, "last_now") and self.last_now is not None:
            return float(self.last_now)
        return time.time()

    def _calc_weighted_markout(self, buf: deque) -> Decimal:
        if not buf:
            return ZERO
        weighted_sum = ZERO
        weight_total = ZERO
        now = self._now()
        for item in buf:
            if isinstance(item, tuple):
                ts, val = item
                dt = max(0.0, float(now) - float(ts))
                if dt >= 60.0:
                    continue
                w = Decimal(str(math.exp(-dt / 25.0)))
            else:
                val = item
                w = ONE
            weighted_sum += val * w
            weight_total += w
        if weight_total < Decimal("0.01"):
            return ZERO
        return weighted_sum / weight_total

    def process_markouts(self, mid: Decimal, now: float) -> None:
        self.last_now = now
        h_cfg = float(getattr(self.cfg, "markout_horizon_s", 5.0))
        while self._pending_markouts and self._pending_markouts[0][0] <= now:
            _, f, horizon = self._pending_markouts.pop(0)
            m = (mid - f.price) if f.side == BUY else (f.price - mid)
            m_bps = m / f.price * BPS

            if abs(horizon - 0.5) < 0.05:
                self.markouts_500ms.append((now, m_bps))
                self.latest_markout_500ms = m_bps
            elif abs(horizon - 1.0) < 0.05:
                self.markouts_1s.append((now, m_bps))
                self.latest_markout_1s = m_bps
            elif abs(horizon - 2.0) < 0.05:
                self.markouts_2s.append((now, m_bps))
                self.latest_markout_2s = m_bps
            elif abs(horizon - 5.0) < 0.05:
                self.markouts_5s.append((now, m_bps))
                self.latest_markout_5s = m_bps

            self.markouts.append((now, m_bps))
            if f.side == BUY:
                self.markouts_buy.append((now, m_bps))
            else:
                self.markouts_sell.append((now, m_bps))

            regime = "REGIME_D_TOXIC" if self.tox_bps >= Decimal("1.5") else "REGIME_A_QUIET"
            if hasattr(self, "bot") and hasattr(self.bot, "md"):
                regime = self.bot.md.detect_regime(now, self.tox_bps)

            if abs(horizon - h_cfg) < 0.05 or (abs(h_cfg - 1.0) > 0.05 and abs(horizon - 5.0) < 0.05):
                self.learner.on_markout(m_bps, f.side, self.tox_bps, horizon=horizon, now=now, regime=regime)
            else:
                self.learner.markout_model.record(f.side, regime, 0, horizon, float(m_bps))

    @property
    def avg_markout_1s_bps(self) -> Decimal:
        return self._calc_weighted_markout(self.markouts_1s)

    @property
    def avg_markout_5s_bps(self) -> Decimal:
        return self._calc_weighted_markout(self.markouts_5s)

    @property
    def avg_markout_bps(self) -> Decimal:
        return self._calc_weighted_markout(self.markouts)

    @property
    def tox_bps(self) -> Decimal:
        if not self.markouts:
            return ZERO
        return max(ZERO, -self.avg_markout_bps)

    def side_tox_bps(self, side: str) -> Decimal:
        buf = self.markouts_buy if side == BUY else self.markouts_sell
        if not buf:
            return self.tox_bps
        avg_m = self._calc_weighted_markout(buf)
        return max(ZERO, -avg_m)

    def reconcile(self, ex_pos: Decimal, now: float, mid: Decimal, min_notional: Decimal) -> bool:
        tol = (min_notional / mid) * Decimal("0.25") if mid else Decimal("1e-8")
        if abs(ex_pos - self.position) <= tol:
            self._mismatch = 0
            return False
        if now - self.last_fill_ts < 4.0:
            return False
        self._mismatch += 1
        if self._mismatch < 2:
            return False
        old = self.position
        self.position = ex_pos
        self._mismatch = 0
        if old == 0 or (old > 0) != (ex_pos > 0) or self.avg_cost == 0:
            self.avg_cost = mid
        self.opened_ts = None if self.is_flat(mid, min_notional) else now
        return True
