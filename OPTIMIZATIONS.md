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
- Added TAKER_WHY log line (rule, unreal bps, thresholds, hold) whenever a taker exit fires, so the trigger is never a mystery.
- Verified against exchange history: the 06:11:40 exit really closed at about 234.58 (value $239.39, fee $0.05, closed PnL -$0.08), not at the 234.05 the bot booked (-$0.60).

## Adverse-OBI exit (from the 09:56 short)
- Cause of the session loss: L0+L1 both sold in one second (3.2 sh, $755) at 09:56:37 as the market flipped quiet->trend; book then sat 0.9+ bid-heavy for 100s while the passive buy exit waited; one 13c gap hit the 6bps stop (-$0.5 total).
- ADV_OBI_EXIT=1: taker exit when obi leans against the position >= ADV_OBI_THRESH for ADV_OBI_SECS and unreal < -ADV_OBI_LOSS_BPS. Default off. Based on ONE event - validate with TAKER_WHY logs.

## Multi-day run hardening
- Logs now carry the date (MM-DD HH:MM:SS); LOG_FILE enables size-rotated file logging.
- Quote dataset throttled to 1 write/s and capped (rotates to .1 at QUOTE_DATASET_MAX_MB).
- SESSION_LOSS_ACTION=pause_day: the old default HALTs and cancels orders but leaves any open position unmanaged (HALT_EXIT is declared but never used). pause_day keeps unwinding, blocks adds until 00:00 UTC, then resets the budget.
- run_forever.sh restarts the process if it dies.

## Own-order exclusion (EXCLUDE_OWN_ORDERS=1/0, default 0 in code, 1 in env_nvda_patched)
- Was NOT implemented before: OBI, micro-price, queue_ahead, fragility, depth OBI and the taker book-walk all counted our own resting size.
- Now subtracts our resting maker orders (older than OWN_ORDER_MIN_AGE_S, not cancelling, not takers) per price level, clamped at 0. If our order is alone at the touch, the next external level is used for the touch size.
- Prices (bid/ask/mid) are untouched; only sizes change. Dataset snapshots add obi_l1_raw, own_bid_at_touch, own_ask_at_touch.
- Not covered: trade-flow (TFI) still includes trades against our own orders (real flow).

## Why it bled (log l__11, 10-04 16:20 -> 10-05 04:59, 207 fills, $62k volume)
- Net -$3.7 = spread capture +$2.66 (0.43bps) and inventory loss -$6.37 (-1.03bps) => -0.6bps of volume.
- 69 round trips: win rate 32%, avg win +$0.016, avg loss -$0.125 (needs ~89% wins to break even at that ratio).
- Mean signed mid move after our fills: -0.33bps @10s, -0.49 @30s, -0.58 @60s. Half-spread earned is ~0.21bps => adverse selection > edge.
- TIME OF DAY decides it: ET 18:00-21:59 lost -$3.43 of the -$3.48 attributable (-0.46..-1.12 bps/hr); ET 09:00-17:59 was ~flat (-$0.05 on ~$21k).
- Fix: QUOTE_OUTSIDE_RTH=0 (existing switch, pauses quoting outside 09:30-16:00 ET). ET_PAUSE_WINDOWS now wraps midnight (e.g. 18:00-09:30).
## Dynamic sizing (ENABLE_DYNAMIC_SIZING=1/0, default 0 in code)
- m = clamp(inventory * edge * vol * drawdown, DYN_SIZE_MIN, 1), adding quotes only; unwinds keep position size; logs DYNSIZE.

## Markout-aware exit urgency (engine.py, both unwind blocks)
- Context: the ConditionalMarkoutModel (empirical Bayes, ledger.py) already predicted E[markout|side,regime,level] to gate new quotes (ev_bps = p_fill*(capture+pred_m-adv) - fee - inv_cost) but was never consulted when deciding how urgently to EXIT an existing position - that decision (adv_score vs EMERGENCY_TAKER_SCORE_THRESHOLD) only looked at live tfi/obi/ret_5s/cross_velo/realized side_tox.
- Added: adv_score now also adds max(0, predict_markout(<closing side>, regime, 0, QUEUE_HORIZON_S)) * 0.5 on both sides (BUY-side prediction for short-covers, SELL-side prediction for long-exits). Sign check: markout is stored as (mid_future-fill_price) for BUY / (fill_price-mid_future) for SELL, so a positive prediction means the market is expected to keep moving further against the position we're trying to close - this can only ADD urgency (clamped at 0), never relax an existing trigger. In regimes with a negative prior (TREND/TOXIC/HIGH_VOL) it only fires once real same-regime fills have pushed the empirical mean positive, not off the prior alone.
- Rationale: directly reuses the model that's already being trained on live fills instead of adding a new unvalidated signal; same function/args pattern already used for the entry-side EV calc.
- Verified: all 48 tests in test_level7.py still pass unmodified (incl. test_16 emergency taker, test_18 adverse_obi_persist_exit, test_23 empirical_markout_model_predictions).
- Not yet done: no fresh live/paper run exists with this change - treat as unvalidated until a session's worth of fills confirms it actually reduces avg loss size.
