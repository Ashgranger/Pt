# Latency optimizations (strategy logic unchanged)

- orders.py: place/modify/cancel for all ladder slots now sent concurrently (asyncio.gather) instead of one-by-one; same-slot actions stay ordered. cancel_all/cancel_side/orphan cancels also parallel.
- bot.py: heartbeat + reconcile (network calls up to 4-8s) run as background tasks, no longer block quoting.
- bot.py: trades channel now wakes the quote loop immediately (set TICK_ON_TRADES=0 to disable).
- bot.py: journal / quote-dataset use persistent line-buffered file handles (no open/close per write).
- ledger.py: learner state saved at most 1x/sec while live (flushed on stop); tests/library keep immediate save. Weighted-markout memoized per tick.
- market.py: trade/price window scans iterate newest->oldest and stop early; TFI cached per tick; depth parsed to Decimal once.
- exchange.py: orjson used if installed. WS permessage-deflate disabled. main.py: uvloop used if installed.
- DEAD MAN'S SWITCH: Arcus has no cancel-on-disconnect. The invalid `heartbeat` post was replaced by the real `scheduleCancel` (market-scoped, ttl DMS_TTL_S=30, refreshed every <=ttl/3, armed on each (re)connect, signed legacy-scheme). DMS_REQUIRED=1 pauses quoting if it cannot be armed.
- Shutdown now cancels orders while the socket is still open, verifies empty book, then disarms.

Optional extra speed:  pip install uvloop orjson
- CROSS-VENUE FEEDS (feeds.py): Binance USDT-M (bookTicker, depth10@100ms, aggTrade, forceOrder) and Bybit v5 linear (orderbook.50 snapshot+delta, publicTrade, liquidation). Signals: basis-adjusted lead/lag (removes USDT-vs-USD offset), per-venue velocity, depth-weighted OBI, external aggressor flow, liquidation pressure, dispersion; all staleness-gated and cleared on disconnect. Used in fair value, expected adverse move, and a hard "pull the stale side" guard (consensus across venues required). See CROSS_* in .env.example.
