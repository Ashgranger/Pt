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
- FIX Bybit feed: liquidation topic is now `allLiquidation.<SYM>` (legacy `liquidation.` fallback), subscribed in a SEPARATE request so a rejected optional topic can't starve price data; bad symbol disables the feed instead of reconnect-looping.
- FIX single-venue pulls need 1.5x the velocity threshold; emergency taker stop floored at 4 ticks; duplicate taker IOCs suppressed for 1.5s; taker fills use exchange avg price when reported.

## Inventory-bleed patch
- ledger.py: learner bounds for STRESS_LOSS_BPS / MAX_HOLD_S now follow your env (were hard floors of 10bps / 60s, silently overriding lower values).
- bot.py: ET_PAUSE_WINDOWS (e.g. 09:30-09:45) blocks ADDING sides only; unwinds continue.
- bot.py: FILL log shows ET=; journal rows carry level, ET, obi, tfi, spread; per-fill markout rows (type=markout) written for offline fitting.
- bot.py: status/LEARN markouts now plain means of last N (previously a 60s-decayed average that printed 0.00 whenever idle - display only, learner was unaffected).
- analyze_journal.py: conditional markout report (level / ET hour / book lean / flow / spread).
- .env.nvda_patched: EXTRA_LEVELS=0, MAX_POSITION_USD=1000, queue/fragility/one-sided ON, tighter exits.

## Taker phantom-loss fix
- Cause: emergency/stress taker IOCs were sent 0.15% (15bps) through the book and the ledger booked the fill at that LIMIT price (the exchange update carried no execution-price field). Every taker fill in the log shows fill px == limit, edge -15..-18bps. That one entry jumped inventory_pnl by about -$0.6 and fed -11bps markouts to the learner (tox_mult up, edge widened, bot stopped quoting).
- Fix: TAKER_SLIP_BPS (default 4) sets the limit offset; TAKER_FILL_PRICE_MODE=est books the order-book-walk (VWAP) price when the exchange sends no avg price; TAKER_RAW log line dumps the raw update so the true field can be confirmed. Set TAKER_FILL_PRICE_MODE=limit for the old behavior.
- Test: test_17.
