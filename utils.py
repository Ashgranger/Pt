"""Small shared helpers (decimal maths, canonical JSON, logging)."""
from __future__ import annotations

import json
import logging
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any

BPS = Decimal("10000")
ZERO = Decimal("0")
ONE = Decimal("1")
BUY, SELL = "BUY", "SELL"


class Fatal(Exception):
    """Unrecoverable config / market problem - do not reconnect."""


def q_down(x: Decimal, unit: Decimal) -> Decimal:
    return (x / unit).to_integral_value(rounding=ROUND_DOWN) * unit


def q_up(x: Decimal, unit: Decimal) -> Decimal:
    return (x / unit).to_integral_value(rounding=ROUND_UP) * unit


def to_int(value: Decimal, unit: Decimal) -> int:
    """Exact decimal -> integer ticks/quantums (the signed payload needs exactness)."""
    n = value / unit
    if n != n.to_integral_value():
        raise ValueError(f"{value} is not a multiple of {unit}")
    return int(n)


def fmt(d: Decimal) -> str:
    return format(d.normalize(), "f")


def canonical(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def clamp(x: Decimal, lo: Decimal, hi: Decimal) -> Decimal:
    return max(lo, min(hi, x))


def bps_diff(a: Decimal, b: Decimal) -> Decimal:
    """(a - b) / b in basis points."""
    return (a - b) / b * BPS if b else ZERO


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S"
    )
