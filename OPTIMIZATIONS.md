# Latency optimizations (strategy logic unchanged)

- orders.py: place/modify/cancel for all ladder slots now sent concurrently (asyncio.gather) instead of one-by-one; same-slot actions stay ordered. cancel_all/cancel_side/orphan cancels also parallel.
- bot.py: heartbeat + reconcile (network calls up to 4-8s) run as background tasks, no longer block quoting.
- bot.py: trades channel now wakes the quote loop immediately (set TICK_ON_TRADES=0 to disable).
- bot.py: journal / quote-dataset use persistent line-buffered file handles (no open/close per write).
- ledger.py: learner state saved at most 1x/sec while live (flushed on stop); tests/library keep immediate save. Weighted-markout memoized per tick.
- market.py: trade/price window scans iterate newest->oldest and stop early; TFI cached per tick; depth parsed to Decimal once.
- exchange.py: orjson used if installed. WS permessage-deflate disabled. main.py: uvloop used if installed.

Optional extra speed:  pip install uvloop orjson
