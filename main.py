#!/usr/bin/env python3
"""Arcus perp market maker (Level 7 Quantitative Engine).

  python main.py                 # paper-trade (DRY_RUN=1): real market data, simulated fills, no orders
  python main.py --live          # send real post-only limit orders
  python main.py scan            # rank markets by spread vs movement (pick where to quote)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from utils import Fatal, setup_logging

log = logging.getLogger("main")


def main() -> None:
    ap = argparse.ArgumentParser(description="Arcus perp market maker (Level 7 Engine)")
    ap.add_argument("cmd", nargs="?", default="run", choices=["run", "scan"])
    ap.add_argument("--live", action="store_true", help="send real orders (overrides DRY_RUN=1)")
    ap.add_argument("--env", choices=["mainnet", "testnet"], help="override ARCUS_ENV")
    ap.add_argument("--market", help="override MARKET")
    ap.add_argument("--env-file", default=".env")
    ap.add_argument("--seconds", type=float, default=30, help="scan sampling time")
    args = ap.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv(args.env_file)
    except ImportError:
        pass
    if args.env:
        os.environ["ARCUS_ENV"] = args.env
    if args.market:
        os.environ["MARKET"] = args.market
    if args.live:
        os.environ["DRY_RUN"] = "0"

    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    from config import Config
    try:
        cfg = Config.from_env()
    except Fatal as e:
        sys.exit(f"config error: {e}")

    if args.cmd == "scan":
        from scan import scan
        asyncio.run(scan(cfg, args.seconds))
        return

    from bot import MarketMaker
    log.info("env=%s market=%s %s | order=$%s max_pos=$%s min_edge=%sbps skew=%sbps ladder_levels=%d "
             "min_ev=%sbps tox_mult=%s", cfg.env_name, cfg.market,
             "PAPER (no orders sent)" if cfg.dry_run else "LIVE", cfg.order_usd, cfg.max_position_usd,
             cfg.min_edge_bps, cfg.skew_bps, cfg.extra_levels, cfg.min_ev_bps, cfg.tox_mult)
    if not cfg.dry_run and cfg.env_name == "mainnet":
        log.warning("LIVE ON MAINNET - real funds. Only post-only limit orders. Ctrl+C cancels all and exits.")

    async def _main() -> None:
        bot = MarketMaker(cfg)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, bot.stop_evt.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(bot.stop_evt.set))
        await bot.run()

    asyncio.run(_main())


if __name__ == "__main__":
    main()
