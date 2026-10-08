"""Comprehensive Test Suite for Level 7 Market Maker Engine."""
import asyncio
import os
import sys
import unittest
from decimal import Decimal as D

import sim
from market import Market
from utils import BUY, SELL, fmt


class TestLevel7MarketMaker(unittest.IsolatedAsyncioTestCase):

    async def test_01_multi_ladder_placement(self):
        """Test that multiple quotes (ladder pairs) are maintained simultaneously and tracked individually."""
        bot, s, clock = sim.make(EXTRA_LEVELS=1, ORDER_USD=20, MAX_POSITION_USD=100, SKEW_BPS=0)
        await sim.step(bot, s, clock, "80000.0", "80100.0")

        buy_orders = bot.om.side_orders(BUY)
        sell_orders = bot.om.side_orders(SELL)

        self.assertGreaterEqual(len(buy_orders), 1, "Should have at least 1 buy order")
        self.assertGreaterEqual(len(sell_orders), 1, "Should have at least 1 sell order")
        
        for o in bot.om.orders.values():
            self.assertIn(o.pair_index, [0, 1])
            self.assertIn(o.side, [BUY, SELL])
            self.assertGreater(o.price, D(0))
            self.assertGreater(o.remaining, D(0))
        print("✓ test_01_multi_ladder_placement passed: Multiple ladder pairs placed and tracked individually.")

    async def test_02_orderbook_intelligence_microprice(self):
        """Test Level 5 Order-Book Intelligence: heavy buy pressure shifts fair value and protects the ask."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ENABLE_ORDERBOOK_INTEL=1, USE_MICRO=1)
        
        await sim.step(bot, s, clock, "80000.0", "80100.0", bsz="1", asz="1")
        initial_ask = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(initial_ask, "Initial ask should be present in balanced market")
        initial_ask_price = initial_ask.price

        # Heavy bid pressure arrives: bid size = 10, ask size = 0.5 (microprice pumps toward ask)
        await sim.step(bot, s, clock, "80000.0", "80100.0", bsz="10", asz="0.5")
        
        new_ask = bot.om.get_order_by_slot(0, SELL)
        if new_ask is None:
            print("✓ test_02_orderbook_intelligence passed: Toxic ask pulled completely (EV < 0 protection).")
        else:
            self.assertGreater(new_ask.price, initial_ask_price, "Ask should reprice higher to protect against toxic buying")
            print(f"✓ test_02_orderbook_intelligence passed: Ask lifted from {initial_ask.price} to {new_ask.price}.")

    async def test_03_adaptive_ev_filter(self):
        """Test Level 4 Adaptive Market Making: quote only when EV > min_ev_bps."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, MIN_EV_BPS="1.0", ENABLE_ADAPTIVE_EV="1",
                                 MIN_EDGE_BPS="5", PENNY="0")
        
        await sim.step(bot, s, clock, "80000.0", "80000.1")
        self.assertEqual(s.rejects, 0, "No post-only crossing rejects should occur")
        print("✓ test_03_adaptive_ev_filter passed: Zero crossing rejects with adaptive EV guard.")

    async def test_04_online_learning_toxicity(self):
        """Test Level 6 Online Learning: markout tracking measures toxicity and widens edge."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, MARKOUT_HORIZON_S=1.0, TOX_MULT=2.0, ENABLE_ONLINE_LEARNING=1)
        await sim.step(bot, s, clock, "80000.0", "80050.0")

        fill = bot.ledger.on_fill(BUY, D("0.0003"), D("80000.0"), D("80025.0"), clock.t, D("5"))
        
        clock.t += 1.5
        bot.ledger.process_markouts(D("79900.0"), clock.t)
        
        self.assertGreater(bot.ledger.tox_bps, D(0), "Toxicity should be learned from adverse markout")
        side_tox = bot.ledger.side_tox_bps(BUY)
        self.assertGreater(side_tox, D(10), "Buy side toxicity should be elevated")
        print(f"✓ test_04_online_learning_toxicity passed: Learned buy-side toxicity = {side_tox:.2f} bps.")

    async def test_05_spread_capture_roundtrip(self):
        """Test that roundtrip fills capture positive spread without rejects."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100)
        await sim.step(bot, s, clock, "80000.0", "80080.0")

        # Taker hits our bid
        s.taker(SELL)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertGreater(bot.ledger.position, D(0), "Should be long after bid fill")
        
        # Quoting exit at the ask
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        s.taker(BUY)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        
        self.assertTrue(bot.ledger.is_flat(D("80040.0"), D("5.0")), f"Should be flat after closing ask fill, got {bot.ledger.position}")
        self.assertGreater(bot.ledger.realized, D(0), "Realized PnL from spread capture should be positive")
        self.assertEqual(s.rejects, 0, "Zero post-only rejects")
        print(f"✓ test_05_spread_capture_roundtrip passed: Realized PnL = ${bot.ledger.realized:.4f}.")




    async def test_06_toxic_regime_and_sweep_guard(self):
        """Test Level 7 Toxic Regime protection and preemptive Sweep Guard."""
        bot, s, clock = sim.make(EXTRA_LEVELS=2, ORDER_USD=20, MAX_POSITION_USD=200,
                                 REGIME_TOXIC_SPREAD_MULT="1.5", SWEEP_GUARD_FILLS=2)
        # 1. Normal step
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        self.assertGreaterEqual(len(bot.om.side_orders(BUY)), 2)

        # 2. Simulate sweep fills (2 fills in <= 1.0s)
        f1 = bot.om.get_order_by_slot(0, BUY)
        bot._on_fill(BUY, f1.remaining, f1.price, f1)
        f2 = bot.om.get_order_by_slot(1, BUY)
        bot._on_fill(BUY, f2.remaining, f2.price, f2)
        await asyncio.sleep(0.01)
        self.assertGreater(bot._burst_blocked_until[BUY], clock.t, "BUY side should be sweep blocked")

        # 3. Test toxic regime OBI suppression
        bot.ledger.markouts.append(D("-5.0"))
        self.assertEqual(bot.md.detect_regime(clock.t, bot.ledger.tox_bps), "REGIME_D_TOXIC")
        # Unblock and test step under toxic sell dump (OBI < -0.4)
        bot._burst_blocked_until[BUY] = 0.0
        await sim.step(bot, s, clock, "80000.0", "80100.0", bsz="0.1", asz="10.0")
        buy_orders = bot.om.side_orders(BUY)
        self.assertEqual(len(buy_orders), 0, "BUY orders should be suppressed during toxic sell dump")
        print("✓ test_06_toxic_regime_and_sweep_guard passed: Toxic OBI protection and Sweep Guard active.")


    async def test_07_online_learning_full_adaptation_and_persistence(self):
        """Test Level 6+ Online Learning: adapt edge, spacing, sizing, skew, EV, and persist state."""
        test_path = 'test_learning_state_unit.json'
        if os.path.exists(test_path):
            os.remove(test_path)

        bot, s, clock = sim.make(EXTRA_LEVELS=2, ORDER_USD=20, MAX_POSITION_USD=200,
                                 ENABLE_ONLINE_LEARNING=1, MARKOUT_HORIZON_S=1.0,
                                 LEARNING_STATE_PATH=test_path)
        learner = bot.ledger.learner
        base_edge = learner.min_edge_bps
        base_spacing = learner.level_spacing_bps
        base_skew = learner.skew_bps

        # 1. Fill and adverse markout -> adapts edge, spacing, tox_mult, min_ev
        fill = bot.ledger.on_fill(BUY, D('0.0003'), D('80000.0'), D('80025.0'), clock.t, D('5'))
        clock.t += 2.0
        bot.ledger.process_markouts(D('79900.0'), clock.t) # -12.5 bps adverse markout

        self.assertGreater(learner.min_edge_bps, base_edge, "min_edge should widen on adverse markout")
        self.assertGreater(learner.level_spacing_bps, base_spacing, "ladder spacing should widen")
        self.assertGreater(learner.tox_mult, D('1.0'), "tox_mult should increase")
        self.assertGreater(learner.min_ev_bps, D('0.2'), "min_ev should increase")

        # 2. Inventory holding duration -> adapts skew_bps & gamma_risk_aversion
        learner.on_fill(BUY, D('80000.0'), D('80000.0'), D('150.0'), 60.0)
        self.assertGreater(learner.skew_bps, base_skew, "skew should adapt higher on prolonged inventory")

        # 3. Flow correlation -> adapts obi_alpha & tfi_beta
        base_obi = learner.obi_alpha
        learner.on_flow_correlation(D('0.5'), D('0.5'), D('1.0'))
        self.assertGreater(learner.obi_alpha, base_obi, "obi_alpha should adapt higher on predictive flow")

        # 4. Persistence verification
        self.assertTrue(os.path.exists(test_path), "Learning state file must be persisted to disk")

        # 5. Cold reload verification
        bot2, _, _ = sim.make(EXTRA_LEVELS=2, ORDER_USD=20, MAX_POSITION_USD=200,
                              ENABLE_ONLINE_LEARNING=1, LEARNING_STATE_PATH=test_path)
        self.assertEqual(bot2.ledger.learner.min_edge_bps, learner.min_edge_bps)
        self.assertEqual(bot2.ledger.learner.skew_bps, learner.skew_bps)
        self.assertEqual(bot2.ledger.learner.total_learned_updates, learner.total_learned_updates)

        if os.path.exists(test_path):
            os.remove(test_path)
        print("✓ test_07_online_learning_full_adaptation_and_persistence passed: All parameters adapted and persisted.")


    async def test_08_profitable_unwind_and_no_loss_selling(self):
        """Test that unwinding inventory guarantees minimum profit and does not sell at a loss."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100,
                                 EXIT_MIN_PROFIT_BPS="1.5", STRESS_LOSS_BPS="20.0",
                                 MIN_REQUOTE_S="0.1", JUMP_BPS="20.0")
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        s.taker(SELL) # fills long at 80000.1
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        # Market drops below entry price
        clock.t += 0.5
        await sim.step(bot, s, clock, "79980.0", "80000.0")
        ask_order = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(ask_order)
        self.assertGreaterEqual(ask_order.price, D('80012.0'), "Bot must quote exit at or above min_profit_px")
        print("✓ test_08_profitable_unwind_and_no_loss_selling passed: Bot preserves profit and prevents loss selling.")


    async def test_09_anti_double_buying_and_chasing_top(self):
        """Test that bot suppresses L0 re-bids when already long and prevents chasing tops in toxic surge."""
        bot, s, clock = sim.make(EXTRA_LEVELS=2, ORDER_USD=20, MAX_POSITION_USD=200)
        await sim.step(bot, s, clock, '80000.0', '80100.0')

        # 1. Fill long
        s.taker(SELL)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertGreater(bot.ledger.position, D(0))

        # While already long, L0 touch BUY must be suppressed (focus on unwinding at profit)
        quotes = bot.engine.generate_ladder_quotes(bot._get_market(), bot.md, bot.ledger, clock.t, False, False)
        buy_slots = [q.pair_index for q in quotes if q.side == BUY]
        self.assertNotIn(0, buy_slots, "L0 BUY must be suppressed when already long")

        # 2. Anti-chasing top in toxic regime
        bot.ledger.position = D(0)
        bot.ledger.markouts.append(D('-5.0'))
        self.assertEqual(bot.md.detect_regime(clock.t, bot.ledger.tox_bps), 'REGIME_D_TOXIC')

        # Simulate upward price surge in history (ret_5s > 1.0 bps)
        bot.md._hist.append((clock.t - 4.0, D('79900.0')))
        bot.md._hist.append((clock.t, D('80100.0')))
        self.assertGreater(bot.md.ret_bps(bot.cfg.trend_window_s, clock.t), D('1.0'))

        quotes_chase = bot.engine.generate_ladder_quotes(bot._get_market(), bot.md, bot.ledger, clock.t, False, False)
        chase_buy_slots = [q.pair_index for q in quotes_chase if q.side == BUY]
        self.assertNotIn(0, chase_buy_slots, "L0 BUY must be suppressed during toxic price surge")
        print("✓ test_09_anti_double_buying_and_chasing_top passed: Single inventory control and anti-chasing active.")


    async def test_10_rth_and_no_redundant_cancel_all(self):
        """Test that outside RTH does not spam cancelAllOrders when flat and respects QUOTE_OUTSIDE_RTH."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, QUOTE_OUTSIDE_RTH="0")
        m_hood = Market(18, 'HOOD-USD', 'ONLINE', D('0.01'), D('0.0000001'), [], D('5'), D('0.0000001'), D('10000'), D('118.575'), True)
        bot.md.info = m_hood
        bot.md.info_ts = clock.t
        bot.md.bid = D('118.56')
        bot.md.ask = D('118.59')

        s.posts.clear()
        for _ in range(10):
            await bot.tick()
            clock.t += 0.25

        self.assertEqual(len(s.posts), 0, "Should not send cancelAllOrders when no orders exist")

        # Quoting allowed when enabled
        bot2, s2, clock2 = sim.make(EXTRA_LEVELS=0, QUOTE_OUTSIDE_RTH="1")
        bot2.md.info = m_hood
        bot2.md.info_ts = clock2.t
        await sim.step(bot2, s2, clock2, "118.56", "118.59")
        self.assertGreaterEqual(len(bot2.om.orders), 1)
        print("✓ test_10_rth_and_no_redundant_cancel_all passed: RTH pause clean and no spam cancel-all.")


    async def test_11_adverse_fill_avoidance_and_full_env_learning(self):
        """Test that order book imbalance suppresses adverse fills and full-env learner adapts."""
        bot, s, clock = sim.make(EXTRA_LEVELS=1, ORDER_USD=20, MAX_POSITION_USD=200,
                                 ENABLE_ONLINE_LEARNING=1)

        # 1. Neutral step
        await sim.step(bot, s, clock, "80000.0", "80080.0", bsz="1", asz="1")
        self.assertIsNotNone(bot.om.get_order_by_slot(0, BUY))

        # 2. Severe sell pressure (bsz=0.1, asz=10 -> OBI ~ -0.98) suppresses touch buy
        clock.t += 1.0
        await sim.step(bot, s, clock, "80000.0", "80080.0", bsz="0.1", asz="10.0")
        self.assertIsNone(bot.om.get_order_by_slot(0, BUY), "Touch BUY must be suppressed under severe sell pressure")

        # 3. Verify all env parameters present in learner
        l = bot.ledger.learner
        for param in ["min_edge_bps", "max_edge_bps", "skew_bps", "level_spacing_bps",
                      "level_size_mult", "vol_k", "tox_mult", "min_ev_bps",
                      "obi_alpha", "tfi_beta", "fill_prob_kappa", "gamma_risk_aversion",
                      "regime_toxic_spread_mult", "trend_pull_bps", "trend_widen",
                      "exit_min_profit_bps", "stress_loss_bps", "max_hold_s",
                      "burst_fills", "burst_cooldown_s"]:
            self.assertIn(param, l.params)
            self.assertIsNotNone(getattr(l, param))

        print("✓ test_11_adverse_fill_avoidance_and_full_env_learning passed: Adverse fills avoided and full env learned.")

    async def test_12_learning_decay_and_paralysis_recovery(self):
        """Test that online learner and toxicity smoothly decay to prevent indefinite quote paralysis."""
        bot, s, clock = sim.make(EXTRA_LEVELS=1, ORDER_USD=20, MAX_POSITION_USD=200,
                                 ENABLE_ONLINE_LEARNING=1, MARKOUT_HORIZON_S=1.0)

        # 1. Neutral step
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        initial_edge = bot.ledger.learner.min_edge_bps

        # 2. Simulate adverse markout on a round trip
        fill1 = bot.ledger.on_fill(BUY, D("0.00025"), D("80000.0"), D("80040.0"), clock.t, D("5"))
        fill2 = bot.ledger.on_fill(SELL, D("0.00025"), D("79950.0"), D("79950.0"), clock.t, D("5"))
        clock.t += 1.5
        bot.ledger.process_markouts(D("79900.0"), clock.t)

        widened_edge = bot.ledger.learner.min_edge_bps
        self.assertGreater(widened_edge, initial_edge, "Adverse markout should widen edge")
        self.assertGreater(bot.ledger.tox_bps, D("0"), "Toxicity should be positive")

        # 3. Fast-forward time by 100 seconds without fills (calm market)
        for _ in range(20):
            clock.t += 5.0
            bot.ledger.current_now = clock.t
            bot.ledger.learner.tick_decay(clock.t)
            await bot.tick()

        recovered_edge = bot.ledger.learner.min_edge_bps
        recovered_tox = bot.ledger.tox_bps
        self.assertLess(recovered_edge, widened_edge, "Learned edge must mean-revert toward base")
        self.assertEqual(recovered_tox, D("0"), "Toxicity must decay to 0 after extended calm")

        # 4. Verify quotes actively participate near the touch
        b0 = bot.om.get_order_by_slot(0, BUY)
        s0 = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(b0, "Touch BUY should be active when flat")
        self.assertIsNotNone(s0, "Touch SELL should be active when flat")
        print("✓ test_12_learning_decay_and_paralysis_recovery passed: Decay and active quote recovery verified.")

    async def test_13_cross_exchange_lead_lag_and_stale_quote_defense(self):
        """Test Cross-Exchange Intelligence: External leader venue surge immediately elevates adverse move and pulls vulnerable quote."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ENABLE_CROSS_EXCHANGE=1, CROSS_VELOCITY_THRESHOLD_BPS="1.0")
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        ask0 = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(ask0, "Initial ask should be placed in balanced market")

        # External leader venue (Binance Futures) surges up +25 bps
        bot.on_external_venue_bbo("BINANCE", D("80050.0"), D("80055.0"))
        clock.t += 0.5
        bot.on_external_venue_bbo("BINANCE", D("80250.0"), D("80260.0"))
        
        velo = bot.md.cross.cross_velocity_bps(3.0, clock.t)
        self.assertGreater(velo, D("10.0"), "External velocity should be strongly positive")
        
        adv_sell = bot.engine.expected_adverse_move(SELL, bot.md, bot.ledger, clock.t)
        self.assertGreater(adv_sell, D("10.0"), "Expected adverse move on SELL should spike")

        await sim.step(bot, s, clock, "80000.0", "80100.0")
        new_ask = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNone(new_ask, "Vulnerable ask should be pulled completely to prevent stale-quote sniping")
        print("✓ test_13_cross_exchange_lead_lag_and_stale_quote_defense passed: Vulnerable quote pulled ahead of external surge.")

    async def test_14_cross_exchange_dispersion_and_spread_capture(self):
        """Test that cross-exchange dispersion widens quoted spread during venue disagreement, capturing higher volatility premium."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ENABLE_CROSS_EXCHANGE=1, CROSS_DISPERSION_WIDEN_MULT="2.0",
                                 MIN_EDGE_BPS="5", MAX_EDGE_BPS="40", PENNY="0")
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        b0 = bot.om.get_order_by_slot(0, BUY)
        a0 = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(b0)
        self.assertIsNotNone(a0)
        normal_spread = a0.price - b0.price

        # External venues disagree: Binance at 79900, Bybit at 80200 (>30 bps dispersion)
        bot.on_external_venue_bbo("BINANCE", D("79900.0"), D("79910.0"))
        bot.on_external_venue_bbo("BYBIT", D("80190.0"), D("80200.0"))
        disp = bot.md.cross.cross_dispersion_bps()
        self.assertGreater(disp, D("20.0"), "Cross-venue dispersion should be elevated")

        await sim.step(bot, s, clock, "80000.0", "80100.0")
        b1 = bot.om.get_order_by_slot(0, BUY)
        a1 = bot.om.get_order_by_slot(0, SELL)
        if b1 and a1:
            widened_spread = a1.price - b1.price
            self.assertGreaterEqual(widened_spread, normal_spread, "Spread should widen during venue disagreement")
            print(f"✓ test_14_cross_exchange_dispersion_and_spread_capture passed: Spread widened from {normal_spread} to {widened_spread}.")
        else:
            print("✓ test_14_cross_exchange_dispersion_and_spread_capture passed: High dispersion protected quotes.")

    async def test_15_smart_inventory_fast_breakeven_unwind(self):
        """Test Smart Inventory Management: When adverse flow arrives (TFI < -0.5, OBI < -0.5), bot accelerates unwind down to breakeven maker."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100,
                                 EXIT_MIN_PROFIT_BPS="1.5", ENABLE_SMART_INVENTORY_MGMT=1,
                                 MIN_REQUOTE_S="0.1")
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        s.taker(SELL)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertGreater(bot.ledger.position, D(0))

        # Adverse flow arrives (selling pressure)
        clock.t += 0.5
        s.push_trade(SELL, "1.0", "80000.0")
        await sim.step(bot, s, clock, "79990.0", "80010.0", bsz="0.2", asz="1.5")

        ask_order = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(ask_order)
        self.assertLess(ask_order.price, D("80010.0"), "Ask should be shaded down to breakeven maker under adverse flow")
        print("✓ test_15_smart_inventory_fast_breakeven_unwind passed: Flow-accelerated breakeven unwind active.")

    async def test_17_taker_fill_booked_at_book_price_not_far_limit(self):
        """Regression: IOC taker exits were booked at their far-through limit (-15..-18bps phantom loss)."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100,
                                 EXIT_MIN_PROFIT_BPS="1.5", ENABLE_SMART_INVENTORY_MGMT=1,
                                 MIN_REQUOTE_S="0.1", EMERGENCY_TAKER_LOSS_BPS="6.0")
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        s.taker(SELL)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        clock.t += 0.5
        s.push_trade(SELL, "2.0", "79900.0")
        await sim.step(bot, s, clock, "79900.0", "79920.0", bsz="0.1", asz="2.0")
        f = bot.ledger.fills[-1]
        self.assertEqual(bot.ledger.position, D(0))
        self.assertGreaterEqual(f.price, D("79899.0"), "taker must be booked near the touch (bid 79900), not at its limit")
        self.assertGreater(f.edge_bps, D("-3"))

    async def test_18_adverse_obi_persist_exit(self):
        """Book leaning against an open long for a few seconds + small loss -> early taker exit, only when enabled."""
        async def run(enabled):
            bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100,
                                     EXIT_MIN_PROFIT_BPS="1.5", ENABLE_SMART_INVENTORY_MGMT=1,
                                     MIN_REQUOTE_S="0.1", STRESS_LOSS_BPS="50", EMERGENCY_TAKER_LOSS_BPS="50",
                                     ADV_OBI_EXIT=enabled, ADV_OBI_SECS="3", ADV_OBI_LOSS_BPS="2.0")
            await sim.step(bot, s, clock, "80000.0", "80080.0")
            s.taker(SELL)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            for _ in range(8):
                await sim.step(bot, s, clock, "79970.0", "79990.0", bsz="0.1", asz="5.0", dt=1.0)
            return bot.ledger.position
        self.assertEqual(await run("1"), D(0), "persistent adverse book + loss must flatten via taker")
        self.assertNotEqual(await run("0"), D(0), "rule is off by default")

    async def test_19_daily_loss_pause_keeps_running_and_unwinds(self):
        """SESSION_LOSS_ACTION=pause_day: breach must NOT stop the bot, only block new adds."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100,
                                 SESSION_MAX_LOSS_USD="0.001", SESSION_LOSS_ACTION="pause_day",
                                 ENABLE_SMART_INVENTORY_MGMT=1, MIN_REQUOTE_S="0.1")
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        s.taker(SELL)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        for _ in range(6):
            await sim.step(bot, s, clock, "79900.0", "79920.0", bsz="0.1", asz="2.0", dt=1.0)
        self.assertFalse(bot.stop_evt.is_set(), "pause_day must not halt the process")
        self.assertGreater(bot._loss_pause_until, 0.0)
        # default behavior unchanged
        bot2, s2, clock2 = sim.make(SESSION_MAX_LOSS_USD="0.001", ORDER_USD=20, MAX_POSITION_USD=100)
        await sim.step(bot2, s2, clock2, "80000.0", "80080.0")
        s2.taker(SELL)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        for _ in range(6):
            await sim.step(bot2, s2, clock2, "79900.0", "79920.0", bsz="0.1", asz="2.0", dt=1.0)
        self.assertTrue(bot2.stop_evt.is_set(), "default 'halt' still stops")

    async def test_20_exclude_own_orders_from_book_metrics(self):
        from market import MarketData
        def mk(flag):
            bot, s, clock = sim.make(EXCLUDE_OWN_ORDERS=flag)
            md = MarketData(bot.cfg)
            md.update(D("100.00"), D("100.02"), D("3"), D("1"), 1.0)
            md.on_depth([["100.00", "3"], ["99.98", "5"]], [["100.02", "1"], ["100.04", "2"]], 1.0)
            md.own_provider = lambda: [(BUY, D("100.00"), D("2"))]
            return md
        off, on = mk("0"), mk("1")
        self.assertEqual(off.obi, D("0.5"))                  # (3-1)/(3+1): unchanged when switch is off
        self.assertEqual(on.obi, D("0"))                     # (1-1)/(1+1): our 2 removed
        self.assertEqual(on.top_size("BUY"), D("1"))
        self.assertEqual(on.raw_obi, D("0.5"))
        self.assertLess(on.micro, off.micro)                 # no longer pulled up by our own bid
        self.assertEqual(on.queue_ahead(BUY, D("100.00")), D("1"))
        self.assertEqual(off.queue_ahead(BUY, D("100.00")), D("3"))
        # our order alone at the touch -> next external level used, never negative / zero-division
        on.own_provider = lambda: [(BUY, D("100.00"), D("3"))]
        self.assertEqual(on.top_size("BUY"), D("5"))
        self.assertGreaterEqual(on.multi_depth_obi(5), D("-1"))
        # unwinding/just-placed orders are not counted: provider is the filter (see bot._own_resting)
        on.own_provider = lambda: []
        self.assertEqual(on.obi, D("0.5"))

    async def test_21_own_resting_filters_young_and_cancelling_and_taker(self):
        bot, s, clock = sim.make(EXCLUDE_OWN_ORDERS="1", OWN_ORDER_MIN_AGE_S="0.3")
        from orders import Order
        now = bot.now()
        mkd = lambda oid, created, **kw: Order(order_id=oid, pair_index=0, side=BUY, price=D("100"), qty=D("1"),
                                               remaining=D("1"), good_til_us=0, created=created, last_action=created, **kw)
        bot.om.orders.clear()
        bot.om.orders["a"] = mkd("a", now - 5)
        bot.om.orders["young"] = mkd("young", now - 0.05)
        bot.om.orders["cx"] = mkd("cx", now - 5, cancelling_since=now - 1)
        bot.om.orders["tk"] = mkd("tk", now - 5, is_taker=True)
        self.assertEqual(len(bot._own_resting()), 1)

    async def test_22_dynamic_sizing_multiplier(self):
        bot, s, clock = sim.make(ENABLE_DYNAMIC_SIZING="1", MAX_POSITION_USD="1000", SESSION_MAX_LOSS_USD="3",
                                 DYN_SIZE_MIN="0.25", DYN_INV_CAP_FRAC="0.6", DYN_INV_MIN="0.25")
        await sim.step(bot, s, clock, "100.00", "100.02")
        eng, md, led = bot.engine, bot.md, bot.ledger
        now = clock.t
        for _ in range(6):
            led.markouts_buy.append((now, D("0.5")))
            led.markouts_sell.append((now, D("-0.5")))
        flat = eng.dynamic_size_mult(BUY, D("0"), md, led, "REGIME_A_QUIET", now)
        self.assertEqual(flat, D("1"), "positive edge + flat inventory -> full size")
        self.assertEqual(eng.dynamic_size_mult(SELL, D("0"), md, led, "REGIME_A_QUIET", now), D("0.25"),
                         "non-positive realized edge -> floor size")
        half = eng.dynamic_size_mult(BUY, D("300"), md, led, "REGIME_A_QUIET", now)   # f=0.3 of cap 0.6 -> halfway
        self.assertTrue(D("0.55") < half < D("0.7"), half)
        self.assertEqual(eng.dynamic_size_mult(BUY, D("-300"), md, led, "REGIME_A_QUIET", now), D("1"),
                         "the side that REDUCES inventory is not shrunk by the inventory schedule")
        self.assertEqual(eng.dynamic_size_mult(BUY, D("900"), md, led, "REGIME_A_QUIET", now), D("0.25"))
        bot_off, s2, c2 = sim.make(ENABLE_DYNAMIC_SIZING="0")
        self.assertEqual(bot_off.engine.dynamic_size_mult(SELL, D("900"), bot_off.md, bot_off.ledger, "REGIME_D_TOXIC", 0.0), D("1"))

    async def test_23_et_window_wraps_midnight(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        import bot as botmod
        et = ZoneInfo("America/New_York")
        at = lambda h, m: datetime(2026, 10, 5, h, m, tzinfo=et).timestamp()
        self.assertTrue(botmod.in_et_windows("18:00-09:30", at(20, 15)))
        self.assertTrue(botmod.in_et_windows("18:00-09:30", at(3, 0)))
        self.assertFalse(botmod.in_et_windows("18:00-09:30", at(12, 0)))
        self.assertTrue(botmod.in_et_windows("09:30-09:45,18:00-09:30", at(9, 35)))

    async def test_24_outside_rth_keeps_unwinding_open_position(self):
        bot, s, clock = sim.make(QUOTE_OUTSIDE_RTH="0", EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100,
                                 EXIT_MIN_PROFIT_BPS="1.5", MIN_REQUOTE_S="0.1")
        mk = bot._get_market()
        orig = mk.is_outside_rth
        try:
            await sim.step(bot, s, clock, "80000.0", "80080.0")
            s.taker(SELL)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await sim.step(bot, s, clock, "80000.0", "80080.0", dt=1.0)
            self.assertNotEqual(bot.ledger.position, D(0), "setup: should hold a position")
            mk.is_outside_rth = True
            for _ in range(3):
                await sim.step(bot, s, clock, "80000.0", "80080.0", dt=1.0)
            live = [o for o in bot.om.orders.values() if not o.is_taker]
            self.assertTrue([o for o in live if o.side == SELL], "unwind order must stay live outside RTH")
            self.assertFalse([o for o in live if o.side == BUY], "no new adds outside RTH")
        finally:
            mk.is_outside_rth = orig

    async def test_25_dms_quota_exhausted_stops_retrying(self):
        bot, s, clock = sim.make()
        calls = []
        async def fake_write(payload):
            calls.append(payload)
            return {"status": 429, "error": {"type": "APIError", "message": "schedule cancel trigger limit reached (10 per UTC day)"}}
        bot.ex.write = fake_write
        object.__setattr__(bot.cfg, "dry_run", False) if hasattr(bot.cfg, "__dataclass_fields__") else None
        bot.signer.schedule_cancel = lambda info, dl: {"dl": dl}
        bot.md.info = bot.md.info or object()
        await bot._heartbeat(100.0)
        await bot._heartbeat(200.0)
        await bot._heartbeat(300.0)
        self.assertEqual(len(calls), 1, "must stop retrying once the daily quota error is returned")
        self.assertGreater(bot._dms_blocked_until, 0.0)

    async def test_25_market_info_refresh_updates_mark_and_rth(self):
        bot, s, clock = sim.make(MARKET_REFRESH_S="1")
        info = bot._get_market()
        o_mark, o_rth, o_st = info.mark, info.is_outside_rth, info.status
        try:
            raw = {"marketId": 1, "marketDisplayName": info.name, "status": "ONLINE", "tickSize": str(info.tick),
                   "stepSize": str(info.step), "minOrderNotional": str(info.min_notional), "minOrderSize": str(info.min_size),
                   "maxOrderSize": str(info.max_size), "markPrice": "123.45", "isOutsideRth": True}
            async def fake(market=None):
                return [raw]
            bot.ex.fetch_markets = fake
            await bot._refresh_market_info(100.0)
            self.assertEqual(info.mark, D("123.45"))
            self.assertTrue(info.is_outside_rth)
            self.assertEqual(info.tick, bot._get_market().tick)
        finally:
            info.mark, info.is_outside_rth, info.status = o_mark, o_rth, o_st

    async def test_26b_oracle_guard_blocks_wrong_side(self):
        async def run(flag, mark):
            bot, s, clock = sim.make(ORACLE_GUARD=flag, ORACLE_GUARD_BPS="3.0", EXTRA_LEVELS=0, MIN_REQUOTE_S="0.1")
            info = bot._get_market()
            old = info.mark
            try:
                info.mark = D(mark)
                for _ in range(3):
                    bot.md.info_ts = clock.t
                    await sim.step(bot, s, clock, "80000.0", "80080.0", dt=1.0)
                live = [o for o in bot.om.orders.values() if not o.is_taker]
                return any(o.side == BUY for o in live), any(o.side == SELL for o in live)
            finally:
                info.mark = old
        self.assertEqual(await run("1", "80500"), (True, False), "oracle far above book -> no SELL adds")
        self.assertEqual(await run("1", "79500"), (False, True), "oracle far below book -> no BUY adds")
        self.assertEqual(await run("0", "80500"), (True, True), "off by default")

    async def test_27_dust_position_does_not_lock_quoting(self):
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100, MIN_REQUOTE_S="0.1")
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        m = bot._get_market()
        bot.ledger.position = m.min_size / D(10)            # untradeable dust
        bot.ledger.avg_cost = D("80100")                    # looks like a deep unrealized loss
        for _ in range(4):
            await sim.step(bot, s, clock, "80000.0", "80080.0", dt=1.0)
        live = [o for o in bot.om.orders.values() if not o.is_taker]
        self.assertTrue([o for o in live if o.side == BUY] and [o for o in live if o.side == SELL],
                        "dust must be treated as flat -> normal two-sided quoting")
        self.assertFalse([o for o in bot.om.orders.values() if o.is_taker], "no taker exit for dust")

    async def test_28_taker_exit_switch_and_aggressive_maker_exit(self):
        async def run(**env):
            base = dict(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100, EXIT_MIN_PROFIT_BPS="1.5",
                        ENABLE_SMART_INVENTORY_MGMT=1, MIN_REQUOTE_S="0.1", EMERGENCY_TAKER_LOSS_BPS="6.0")
            base.update(env)
            bot, s, clock = sim.make(**base)
            await sim.step(bot, s, clock, "80000.0", "80080.0")
            s.taker(SELL)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            clock.t += 0.5
            s.push_trade(SELL, "2.0", "79900.0")
            await sim.step(bot, s, clock, "79900.0", "79920.0", bsz="0.1", asz="2.0")
            takers = [f for f in bot.ledger.fills if f.side == SELL and f.price <= D("79900.5")]
            live = [o for o in bot.om.orders.values() if not o.is_taker and o.side == SELL]
            return bot, takers, live
        # 1) default: taker exit still works (position flattened immediately)
        bot, _, _ = await run()
        self.assertEqual(bot.ledger.position, D(0))
        # 2) button off: no taker; a post-only reduce-only SELL sits at the best ask
        bot, _, live = await run(ENABLE_TAKER_EXITS="0")
        self.assertNotEqual(bot.ledger.position, D(0), "no taker exit when disabled")
        self.assertTrue(live, "aggressive maker exit must be resting")
        self.assertTrue(all(o.price == D("79920.0") for o in live), [str(o.price) for o in live])
        self.assertTrue(all(getattr(o, "is_reduce_only", False) for o in live))
        # 3) disabled but catastrophe hard stop reached (loss ~11bps >= 5) -> taker allowed
        bot, _, _ = await run(ENABLE_TAKER_EXITS="0", TAKER_HARD_STOP_BPS="5")
        self.assertEqual(bot.ledger.position, D(0))
        # 4) maker-first: loss within stress+slack -> maker, no taker
        bot, _, live = await run(MAKER_EXIT_FIRST="1", STRESS_LOSS_BPS="20", MAKER_EXIT_SLACK_BPS="2")
        self.assertNotEqual(bot.ledger.position, D(0))
        self.assertTrue(live)

    async def test_16_emergency_taker_cut_on_adverse_cascade(self):
        """Test Emergency Taker Cut: When adverse loss and flow exceed threshold, bot fires IOC taker order to cut loss."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100,
                                 EXIT_MIN_PROFIT_BPS="1.5", ENABLE_SMART_INVENTORY_MGMT=1,
                                 MIN_REQUOTE_S="0.1", EMERGENCY_TAKER_LOSS_BPS="6.0")
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        s.taker(SELL)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertGreater(bot.ledger.position, D(0))

        # Severe price dump with persistent aggressive selling
        clock.t += 0.5
        s.push_trade(SELL, "2.0", "79900.0")
        await sim.step(bot, s, clock, "79900.0", "79920.0", bsz="0.1", asz="2.0")

        self.assertEqual(bot.ledger.position, D(0), "Long position should be liquidated via taker order")
        print("✓ test_16_emergency_taker_cut_on_adverse_cascade passed: Emergency Taker Cut liquidated position.")




    async def test_17_severe_selling_pressure_maker_scratch_and_taker_cut(self):
        """Test User Real Log Scenario:
        1. Long position held at cost 80000.0.
        2. Market drops to 79992.0 (loss ~1.0 bps) with severe selling pressure (OBI=-0.8, TFI=-1.0).
        3. Bot joins best ask as aggressive maker scratch at 0% maker fee, rather than hanging above market.
        4. When price cascades further to 79940.0 (loss > 6.0 bps), emergency taker IOC fires to cut loss.
        """
        bot, s, clock = sim.make(EXTRA_LEVELS=2, ORDER_USD=20, MAX_POSITION_USD=100,
                                 EXIT_MIN_PROFIT_BPS="1.5", ENABLE_SMART_INVENTORY_MGMT=1,
                                 MIN_REQUOTE_S="0.1", EMERGENCY_TAKER_LOSS_BPS="6.0")
        # 1. Fill Long at 80000.0
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        s.taker(SELL)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertGreater(bot.ledger.position, D(0))
        entry_cost = bot.ledger.avg_cost

        # 2. Market drops 1 bps below entry cost with severe selling pressure
        clock.t += 0.5
        s.push_trade(SELL, "1.0", "79992.0")
        await sim.step(bot, s, clock, "79988.0", "79996.0", bsz="0.1", asz="0.9")

        # Check: Unwind ask must join best ask (79996.0 or penny 79995.9), NOT hang above entry_cost
        ask_order = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(ask_order)
        self.assertLessEqual(ask_order.price, D("79996.0"), "Unwind ask must join top of book as maker scratch")
        self.assertLess(ask_order.price, entry_cost, "Under severe selling pressure, ask must not be held above cost")

        # Check: All BUY levels must be suppressed (no falling knife accumulation)
        buy_order_l0 = bot.om.get_order_by_slot(0, BUY)
        buy_order_l1 = bot.om.get_order_by_slot(1, BUY)
        self.assertIsNone(buy_order_l0, "L0 BUY must be suppressed during severe selling pressure")
        self.assertIsNone(buy_order_l1, "L1 BUY must be suppressed during severe selling pressure")

        # 3. Market cascades down past 6.0 bps loss
        clock.t += 0.5
        s.push_trade(SELL, "2.0", "79940.0")
        await sim.step(bot, s, clock, "79930.0", "79940.0", bsz="0.05", asz="2.0")

        # Check: Emergency Taker Cut executed, position is flat
        self.assertEqual(bot.ledger.position, D(0), "Position must be liquidated via emergency taker order")
        print("✓ test_17_severe_selling_pressure_maker_scratch_and_taker_cut passed: Scratch & Taker cut verified.")



    async def test_18_reduce_only_enforcement_on_maker_unwind(self):
        """Test Critical Bug Fix: Unwind maker orders MUST be marked reduce_only=True in both place and modify."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100)
        # 1. Flat market: entry order must NOT be reduce_only
        await sim.step(bot, s, clock, "80000.0", "80080.0")
        buy_order = bot.om.get_order_by_slot(0, BUY)
        self.assertIsNotNone(buy_order)
        self.assertFalse(buy_order.is_reduce_only, "Entry order must NOT be reduce-only")

        # 2. Fill Long -> Unwind order must be marked reduce_only
        s.taker(SELL)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await bot.tick()
        self.assertGreater(bot.ledger.position, D(0))

        # Check placed exit ask
        ask_order = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(ask_order)
        self.assertTrue(ask_order.is_reduce_only, "Exit ask order MUST be reduce_only=True")

        # 3. Market moves, modifying the exit ask: modify request must retain reduce_only=True
        clock.t += 0.5
        await sim.step(bot, s, clock, "80020.0", "80100.0")
        ask_order2 = bot.om.get_order_by_slot(0, SELL)
        self.assertIsNotNone(ask_order2)
        self.assertTrue(ask_order2.is_reduce_only, "Modified exit ask order MUST retain reduce_only=True")
        print("✓ test_18_reduce_only_enforcement_on_maker_unwind passed: Reduce-only strictly enforced on placement and modification.")

    async def test_19_queue_aware_fill_probability_and_selective_touch(self):
        """Test Queue-Aware Fill Probability & Selective-Touch:
        When top-of-book depth is heavy and fragility is high, bot quotes 1-tick back instead of blindly taking toxic touch."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ENABLE_SELECTIVE_TOUCH=1, ENABLE_QUEUE_MODEL=1)
        await sim.step(bot, s, clock, "80000.0", "80010.0")
        # Setup book depth: bids=[(80000, 100)], asks=[(80010, 0.5)] -> 100 contracts ahead of us at 80000
        bot.md.on_depth([[D("80000.0"), D("100.0")]], [[D("80010.0"), D("0.5")]], clock.t)
        
        # Balanced two-way flow with large resting queue ahead (100 contracts)
        bot.md.on_trade(BUY, D("6.0"), D("80010.0"), clock.t)
        bot.md.on_trade(SELL, D("5.0"), D("80000.0"), clock.t)
        fragility = bot.md.liquidity_fragility(BUY, clock.t)
        self.assertGreater(fragility, D("0.01"), "Fragility should be measurable")

        # Queue fill probability: with 100 contracts ahead, fill probability is realistic (not 1.0)
        p_fill_touch = bot.engine.queue_fill_probability(BUY, D("80000.0"), bot.md, clock.t)
        self.assertLess(p_fill_touch, 0.90, "Queue fill probability should be bounded by queue drainage")

        # Bot generates quotes under selective touch candidate selection
        quotes = bot.engine.generate_ladder_quotes(bot._get_market(), bot.md, bot.ledger, clock.t, False, False)
        buy_quotes = [q for q in quotes if q.side == BUY]
        self.assertGreaterEqual(len(buy_quotes), 1, "Should generate at least one buy quote candidate")
        self.assertGreater(buy_quotes[0].expected_value_bps, D("-10"))
        print("✓ test_19_queue_aware_fill_probability_and_selective_touch passed: Queue dynamics and selective candidates verified.")

    async def test_20_liquidity_fragility_and_absorption_exhaustion(self):
        """Test Liquidity Fragility & Trade Flow Exhaustion Detection:
        Detects when aggressive selling halts and depth absorbs the pressure."""
        bot, s, clock = sim.make()
        # 1. High selling volume in recent past (10s), but calm in last 1s -> deceleration
        for i in range(10):
            bot.md.on_trade(SELL, D("1.0"), D("80000.0"), clock.t - 8.0 + i * 0.5)
        # Stalled price drop
        bot.md._hist.append((clock.t - 3.0, D("80000.0")))
        bot.md._hist.append((clock.t, D("79998.0"))) # only -0.25 bps move

        is_exh = bot.md.is_exhaustion(BUY, clock.t)
        self.assertTrue(is_exh, "Exhaustion should trigger when severe selling momentum decelerates and price holds")
        print("✓ test_20_liquidity_fragility_and_absorption_exhaustion passed: Flow exhaustion and absorption detected.")

    async def test_21_alpha_and_funding_aware_inventory_target(self):
        """Test Alpha and Funding-Aware Inventory Target:
        Positive funding rate (longs pay shorts) causes bot to set negative inventory target, favoring short inventory."""
        bot, s, clock = sim.make(ENABLE_SMART_INVENTORY_MGMT=1, ENABLE_FUNDING_CARRY=1)
        # Positive funding rate: longs pay shorts (e.g. 5 bps per interval)
        bot.md.info.funding_rate = D("0.0005") # +5 bps
        await sim.step(bot, s, clock, "80000.0", "80020.0")

        q_target = bot.engine.compute_target_inventory_usd(bot.md, clock.t, bot.ledger)
        self.assertLess(q_target, D(0), "Target inventory should be negative (short) when funding pays shorts")

        # Check reservation price with target inventory:
        # Flat position with negative target inventory -> effective position is net positive -> reservation price skews lower
        res_price = bot.engine.compute_reservation_price(D("80010.0"), D(0), D("2.0"), bot.ledger, target_inventory_usd=q_target)
        self.assertLess(res_price, D("80010.0"), "When target is short, reservation price shifts lower to incentivize selling / discourage buying")
        print("✓ test_21_alpha_and_funding_aware_inventory_target passed: Target inventory adapts to funding carry and alpha.")

    async def test_22_multi_horizon_markout_and_funding_pnl(self):
        """Test Multi-Horizon Markout (500ms, 1s, 2s, 5s) and Funding Cashflow PnL Integration."""
        bot, s, clock = sim.make()
        # 1. Fill Buy
        fill = bot.ledger.on_fill(BUY, D("0.00025"), D("80000.0"), D("80010.0"), clock.t, D("5"))
        
        # Advance 0.5s -> 500ms markout evaluated
        clock.t += 0.5
        bot.ledger.process_markouts(D("80020.0"), clock.t)
        self.assertIsNotNone(bot.ledger.latest_markout_500ms)
        self.assertEqual(bot.ledger.latest_markout_500ms, D("2.5")) # (80020 - 80000) / 80000 * 10000 = +2.5 bps

        # Advance to 2.0s -> 1s and 2s markout evaluated
        clock.t += 1.5
        bot.ledger.process_markouts(D("80030.0"), clock.t)
        self.assertIsNotNone(bot.ledger.latest_markout_1s)
        self.assertIsNotNone(bot.ledger.latest_markout_2s)
        self.assertEqual(bot.ledger.latest_markout_2s, D("3.75"))

        # 2. Apply funding payment
        init_pnl = bot.ledger.total_pnl(D("80030.0"))
        bot.ledger.apply_funding(D("0.05")) # received /usr/bin/bash.05 funding
        new_pnl = bot.ledger.total_pnl(D("80030.0"))
        self.assertEqual(new_pnl, init_pnl + D("0.05"), "Total PnL must include cumulative funding cashflow")
        print("✓ test_22_multi_horizon_markout_and_funding_pnl passed: Multi-horizon markouts and funding cashflows verified.")


    async def test_23_empirical_markout_model_predictions(self):
        """Test Empirical Bayesian Markout Model: Learns E[markout | side, regime, level] with shrinkage toward prior."""
        bot, s, clock = sim.make()
        learner = bot.ledger.learner

        # Prior prediction in quiet regime: +0.5 bps
        p_quiet = learner.predict_markout(BUY, "REGIME_A_QUIET", level=0, horizon=2.0)
        self.assertEqual(p_quiet, D("0.5"))

        # Prior prediction in toxic regime: -2.5 bps
        p_toxic = learner.predict_markout(BUY, "REGIME_D_TOXIC", level=0, horizon=2.0)
        self.assertEqual(p_toxic, D("-2.5"))

        # Record 10 adverse fills (-5.0 bps each) in TOXIC regime
        for _ in range(10):
            learner.markout_model.record(BUY, "REGIME_D_TOXIC", 0, 2.0, -5.0)

        # Updated prediction should adapt toward -5.0 via shrinkage (10/(10+5) * -5.0 + 5/15 * -2.5 = -4.17 bps)
        p_updated = learner.predict_markout(BUY, "REGIME_D_TOXIC", level=0, horizon=2.0)
        self.assertLess(p_updated, D("-3.5"), "Empirical markout model must adapt downward with adverse observations")
        print("✓ test_23_empirical_markout_model_predictions passed: Bayesian shrinkage and empirical learning verified.")

    async def test_24_vwap_cross_cost_calculation(self):
        """Test Order Book Walk for Taker Cross Cost:
        Walks actual L2 depth to compute VWAP crossing price and slippage."""
        bot, s, clock = sim.make(TAKER_FEE_BPS="2.2")
        await sim.step(bot, s, clock, "80000.0", "80010.0")

        # Setup 2-tier ask book: 0.1 BTC @ 80010, 0.2 BTC @ 80020
        bot.md.on_depth([[D("80000.0"), D("1.0")]],
                        [[D("80010.0"), D("0.1")], [D("80020.0"), D("0.2")]], clock.t)

        # Need to buy 0.2 BTC: 0.1 filled @ 80010, 0.1 filled @ 80020 -> VWAP = 80015
        vwap, cross_cost_bps = bot.engine.calculate_vwap_cross_cost(BUY, D("0.2"), bot.md)
        self.assertEqual(vwap, D("80015.0"))
        # Crossing cost = slip vs mid(80005) = (80015 - 80005)/80005 * 10000 + 2.2 bps = 1.25 + 2.2 = 3.45 bps
        self.assertGreater(cross_cost_bps, D("3.0"))
        print("✓ test_24_vwap_cross_cost_calculation passed: Exact L2 depth VWAP walking verified.")

    async def test_25_onesided_touch_suppression(self):
        """Test One-Sided Touch:
        When extreme toxic flow is directed at ask side, touch ask is suppressed to avoid steamroller fills."""
        bot, s, clock = sim.make(EXTRA_LEVELS=1, ENABLE_ONESIDED_TOUCH=1)
        await sim.step(bot, s, clock, "80000.0", "80010.0")

        # Simulate massive aggressive buying burst: TFI = +0.8, OBI = +0.8, regime = TOXIC
        for _ in range(5):
            bot.md.on_trade(BUY, D("2.0"), D("80010.0"), clock.t)
        bot.ledger.markouts.append(D("-5.0")) # triggers toxic regime

        quotes = bot.engine.generate_ladder_quotes(bot._get_market(), bot.md, bot.ledger, clock.t, False, False)
        # Touch ask (level 0 SELL) must be suppressed under one-sided touch
        touch_sells = [q for q in quotes if q.side == SELL and q.pair_index == 0]
        self.assertEqual(len(touch_sells), 0, "Touch SELL must be suppressed during aggressive buying steam")
        print("✓ test_25_onesided_touch_suppression passed: One-sided touch protection active.")

    async def test_26_quote_opportunity_dataset_logging(self):
        """Test Quote Opportunity Dataset Logger:
        Logs complete microstructure snapshot and candidate actions to JSONL."""
        test_log = "test_quote_opportunities.jsonl"
        if os.path.exists(test_log):
            os.remove(test_log)

        bot, s, clock = sim.make(ENABLE_QUOTE_DATASET=1, QUOTE_DATASET_PATH=test_log)
        await sim.step(bot, s, clock, "80000.0", "80010.0")
        await bot.tick()

        self.assertTrue(os.path.exists(test_log), "Quote opportunity log file must be created")
        with open(test_log) as f:
            lines = f.readlines()
        self.assertGreater(len(lines), 0, "At least one quote opportunity record must be logged")

        import json
        rec = json.loads(lines[0])
        self.assertIn("snapshot", rec)
        self.assertIn("quotes", rec)
        self.assertIn("spread_ticks", rec["snapshot"])
        self.assertIn("obi_l1", rec["snapshot"])

        if os.path.exists(test_log):
            os.remove(test_log)
        print("✓ test_26_quote_opportunity_dataset_logging passed: High-frequency quote opportunity dataset verified.")

    async def test_27_positive_spread_capture_and_no_mid_crossing(self):
        """Verify that ladder quotes never cross mid, maintain monotonic depth, and capture strictly positive spread."""
        bot, s, clock = sim.make(
            MARKET="PUMP-USD",
            MIN_EDGE_BPS="3.5",
            MAX_EDGE_BPS="20.0",
            EXTRA_LEVELS=2,
            LEVEL_SPACING_BPS="3.0",
            ENABLE_SELECTIVE_TOUCH=1,
            ORDER_USD=50,
            MAX_POSITION_USD=180
        )
        m_pump = sim.Market(10, "PUMP-USD", "ONLINE", D("0.000001"), D("1"), [], D("5"), D("1"), D("1000000"), D("0.005025"), False)
        bot.md.info = m_pump
        bot.md.info_ts = clock.t
        bot.md.update(D("0.005000"), D("0.005050"), D("100000"), D("100000"), clock.t)

        quotes = bot.engine.generate_ladder_quotes(m_pump, bot.md, bot.ledger, clock.t, False, False)
        mid = bot.md.mid

        buy_quotes = [q for q in quotes if q.side == BUY]
        sell_quotes = [q for q in quotes if q.side == SELL]

        self.assertGreaterEqual(len(buy_quotes), 1)
        self.assertGreaterEqual(len(sell_quotes), 1)

        # 1. Verify every BUY quote is strictly below mid, and every SELL quote is strictly above mid
        for q in buy_quotes:
            self.assertLess(q.price, mid, f"BUY quote {q.price} must be strictly less than mid {mid}")
        for q in sell_quotes:
            self.assertGreater(q.price, mid, f"SELL quote {q.price} must be strictly greater than mid {mid}")

        # 2. Verify ladder depth monotonicity
        for i in range(len(buy_quotes) - 1):
            self.assertLess(buy_quotes[i+1].price, buy_quotes[i].price, "BUY ladder must descend into the book")
        for i in range(len(sell_quotes) - 1):
            self.assertGreater(sell_quotes[i+1].price, sell_quotes[i].price, "SELL ladder must ascend into the book")

        # 3. Simulate a fill at L0 BUY and verify positive spread capture
        l0_buy = buy_quotes[0]
        bot.ledger.on_fill(BUY, l0_buy.qty, l0_buy.price, mid, clock.t, True)
        self.assertGreater(bot.ledger.spread_capture, D("0"), "Spread capture must be strictly positive")
        self.assertGreater(bot.ledger.avg_edge_bps, D("0"), "Average edge bps must be strictly positive")

        print(f"✓ test_27_positive_spread_capture_and_no_mid_crossing passed: All quotes strictly respect mid and capture positive spread.")


    async def test_28_arcus_taker_tif_and_good_til_signing(self):
        """Verify Arcus documentation specification: TIF_IOC=2, FOK=1, GTT=0, ALO=3 and valid goodTilTime signing."""
        from signer import Signer
        s = Signer('11' * 32, '0xAbCdEf0123456789aBcDeF0123456789AbCdEf01', 0)
        self.assertEqual(s.TIF_GTT, 0, 'Arcus TIF GTT must be 0')
        self.assertEqual(s.TIF_FOK, 1, 'Arcus TIF FOK must be 1')
        self.assertEqual(s.TIF_IOC, 2, 'Arcus TIF IOC must be 2')
        self.assertEqual(s.TIF_ALO, 3, 'Arcus TIF ALO must be 3')

        m = sim.Market(1, 'BTC-USD', 'ONLINE', D('0.1'), D('0.0001'), [], D('5'), D('0.0001'), D('100000'), D('80000.0'), False)
        good_til = 1750000000000000
        req = s.place(m, BUY, D('80010.0'), D('0.001'), good_til, time_in_force='IOC', reduce_only=True)
        self.assertEqual(req['payload']['timeInForce'], 'IOC')
        self.assertEqual(req['payload']['orderType'], 'LIMIT')
        self.assertTrue(req['payload']['reduceOnly'])
        self.assertEqual(req['payload']['goodTilTime'], str(good_til))

        # Check binary typed message
        import json
        msg = s._typed(s.OP_PLACE, 123456789, m.market_id, g=good_til * 1000, p=800100, q=10, r=1, s=0, t=s.TIF_IOC)
        msg_dict = json.loads(msg)
        self.assertEqual(msg_dict['t'], 2, 'Signed IOC message t must be 2 (not 1 / FOK)')
        self.assertEqual(msg_dict['r'], 1, 'Signed message r must be 1 for reduce_only')
        self.assertEqual(msg_dict['g'], good_til * 1000, 'Signed message g must be nanoseconds')
        print('✓ test_28_arcus_taker_tif_and_good_til_signing passed: Arcus TIF=2 and goodTilTime verified.')

    async def test_29_order_manager_taker_slot_isolation_and_terminal_cleanup(self):
        """Verify that IOC taker orders never occupy pair_slots and are cleaned up immediately on terminal states."""
        bot, s, clock = sim.make(EXTRA_LEVELS=1, ORDER_USD=20, MAX_POSITION_USD=100)
        await sim.step(bot, s, clock, '80000.0', '80080.0')

        self.assertIn((0, SELL), bot.om.pair_slots)

        from engine import QuoteTarget
        taker_target = QuoteTarget(pair_index=0, side=SELL, price=D('79990.0'), qty=D('0.001'),
                                   expected_value_bps=D('-2.0'), fill_probability=1.0,
                                   is_exit_quote=True, is_taker=True, quote_mid=D('80040.0'))

        await bot.om.sync_quotes([taker_target], clock.t)
        self.assertNotIn((0, SELL), bot.om.pair_slots, 'IOC taker order must NOT occupy pair_slots!')

        # Verify no phantom errors on subsequent sync_quotes
        await bot.om.sync_quotes([], clock.t)
        self.assertEqual(bot.om._consec_errors, 0, 'No 404 errors or ghost modifications after IOC taker order')
        print('✓ test_29_order_manager_taker_slot_isolation_and_terminal_cleanup passed: Taker isolation and clean lifecycle verified.')

    async def test_30_taker_order_depth_slippage_and_execution_pricing(self):
        """Verify that taker orders cross the order book with slippage buffer instead of being stuck at passive touch."""
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ORDER_USD=20, MAX_POSITION_USD=100,
                                 ENABLE_SMART_INVENTORY_MGMT=1, EMERGENCY_TAKER_LOSS_BPS='6.0')
        m = sim.Market(1, 'BTC-USD', 'ONLINE', D('0.1'), D('0.0001'), [], D('5'), D('0.0001'), D('100000'), D('80000.0'), False)
        bot.md.info = m
        bot.ledger.position = D('0.001')
        bot.ledger.avg_cost = D('80000.0')
        # Price drops to 79900 (loss > 6 bps)
        bot.md.update(D('79900.0'), D('79920.0'), D('0.1'), D('2.0'), clock.t)
        quotes = bot.engine.generate_ladder_quotes(m, bot.md, bot.ledger, clock.t, False, False)
        t_quotes = [q for q in quotes if getattr(q, 'is_taker', False)]
        self.assertEqual(len(t_quotes), 1, 'Emergency taker cut should generate exactly 1 taker quote')
        t_q = t_quotes[0]
        self.assertEqual(t_q.side, SELL)
        self.assertLess(t_q.price, bot.md.bid, 'SELL taker order must cross below bid to guarantee execution')
        print(f'✓ test_30_taker_order_depth_slippage_and_execution_pricing passed: Taker price {t_q.price} crosses bid {bot.md.bid}.')

    async def test_31_spread_capture_strictly_positive_across_unwind_and_ladder(self):
        """Verify all maker quotes strictly maintain positive spread capture and positive edge bps upon fill."""
        bot, s, clock = sim.make(EXTRA_LEVELS=2, ORDER_USD=20, MAX_POSITION_USD=100, GUARANTEE_SPREAD_CAPTURE=1)
        m = sim.Market(1, 'BTC-USD', 'ONLINE', D('0.1'), D('0.0001'), [], D('5'), D('0.0001'), D('100000'), D('80000.0'), False)
        bot.md.info = m

        for pos in [D('-0.0005'), D('0'), D('0.0005')]:
            bot.ledger.position = pos
            bot.ledger.avg_cost = D('80000.0')
            bot.md.update(D('79980.0'), D('80020.0'), D('10'), D('10'), clock.t)
            mid = bot.md.mid
            quotes = bot.engine.generate_ladder_quotes(m, bot.md, bot.ledger, clock.t, False, False)
            for q in quotes:
                if not getattr(q, 'is_taker', False):
                    if q.side == BUY:
                        self.assertLess(q.price, mid, f'BUY quote {q.price} must be < mid {mid}')
                        edge = mid - q.price
                        self.assertGreater(edge, D(0), f'BUY edge must be > 0, got {edge}')
                    else:
                        self.assertGreater(q.price, mid, f'SELL quote {q.price} must be > mid {mid}')
                        edge = q.price - mid
                        self.assertGreater(edge, D(0), f'SELL edge must be > 0, got {edge}')

        print('✓ test_31_spread_capture_strictly_positive_across_unwind_and_ladder passed: Positive spread capture verified.')

    async def test_32_two_sided_quoting_and_positive_spread_on_funding_markets(self):
        """Verify that on markets with positive funding rate, the bot quotes both sides symmetrically when flat,
        does not accumulate negative inventory bias, and locks in strictly positive spread on roundtrip fills."""
        from orders import Order
        bot, s, clock = sim.make(ORDER_USD=20, MAX_POSITION_USD=100, ENABLE_SMART_INVENTORY_MGMT=1, ENABLE_FUNDING_CARRY=1)
        m = sim.Market(1, "BTC-USD", "ONLINE", D("0.1"), D("0.0001"), [], D("5"), D("0.0001"), D("100000"), D("80000.0"), False)
        m.funding_rate = D("0.0005") # 5 bps positive funding rate
        bot.md.info = m

        # 1. Flat state: Bot must quote both bids and asks
        bot.md.update(D("80000.0"), D("80010.0"), D("1.0"), D("1.0"), clock.t)
        quotes = bot.engine.generate_ladder_quotes(m, bot.md, bot.ledger, clock.t, False, False)
        bids = [q for q in quotes if q.side == BUY]
        asks = [q for q in quotes if q.side == SELL]
        self.assertGreater(len(bids), 0, "Bot must place bids when flat despite positive funding rate")
        self.assertGreater(len(asks), 0, "Bot must place asks when flat despite positive funding rate")

        # 2. Fill ask: Bot enters short
        sell_q = asks[0]
        bot._on_fill(SELL, sell_q.qty, sell_q.price, Order("s1", 0, SELL, sell_q.price, sell_q.qty, sell_q.qty, 0, clock.t, clock.t, quote_mid=D("80005.0")))
        self.assertEqual(bot.ledger.position, -sell_q.qty)

        # 3. Unwind quote must be at profitable touch
        clock.t += 1.0
        bot.md.update(D("80000.0"), D("80010.0"), D("1.0"), D("1.0"), clock.t)
        unwind_quotes = bot.engine.generate_ladder_quotes(m, bot.md, bot.ledger, clock.t, False, False)
        unwind_buys = [q for q in unwind_quotes if q.side == BUY]
        self.assertGreater(len(unwind_buys), 0, "Must place unwind buy quote")
        self.assertLessEqual(unwind_buys[0].price, D("80000.0"), "Unwind quote must not cross bid")

        # 4. Fill unwind buy: Locks in positive spread and PnL
        buy_q = unwind_buys[0]
        bot._on_fill(BUY, buy_q.qty, buy_q.price, Order("b1", 0, BUY, buy_q.price, buy_q.qty, buy_q.qty, 0, clock.t, clock.t, quote_mid=D("80005.0")))
        self.assertEqual(bot.ledger.position, D(0), "Bot must be flat after roundtrip")
        self.assertGreater(bot.ledger.realized, D(0), "Realized PnL must be strictly positive")
        self.assertGreater(bot.ledger.spread_capture, D(0), "Spread capture must be strictly positive")
        print("✓ test_32_two_sided_quoting_and_positive_spread_on_funding_markets passed: Balanced quoting and positive spread verified.")

    async def test_33_ultra_thin_liquid_market_spread_capture_and_touch_quoting(self):
        """Verify that on ultra-thin liquid markets (e.g. 0.1 bps spread), the bot quotes directly at the touch,
        does not offset quotes 100+ ticks away, and completes round-trips capturing positive spread."""
        from orders import Order
        bot, s, clock = sim.make(ORDER_USD=20, MAX_POSITION_USD=100, EXTRA_LEVELS=1)
        m = sim.Market(1, "BTC-USD", "ONLINE", D("0.1"), D("0.0001"), [], D("5"), D("0.0001"), D("100000"), D("80000.0"), False)
        bot.md.info = m

        bot.md.update(D("80000.0"), D("80000.8"), D("10.0"), D("10.0"), clock.t)
        quotes = bot.engine.generate_ladder_quotes(m, bot.md, bot.ledger, clock.t, False, False)
        
        bids = [q for q in quotes if q.side == BUY]
        asks = [q for q in quotes if q.side == SELL]
        self.assertGreaterEqual(len(bids), 1, "Must quote bids on liquid market")
        self.assertGreaterEqual(len(asks), 1, "Must quote asks on liquid market")
        
        # Level 0 must be at the touch
        self.assertEqual(bids[0].price, D("80000.0"), "L0 bid must be at touch 80000.0")
        self.assertEqual(asks[0].price, D("80000.8"), "L0 ask must be at touch 80000.8")
        
        # Fill buy at bid:
        bot._on_fill(BUY, bids[0].qty, bids[0].price, Order("b1", 0, BUY, bids[0].price, bids[0].qty, bids[0].qty, 0, clock.t, clock.t, quote_mid=D("80000.4")))
        
        # Check unwind sell quote at ask:
        clock.t += 0.5
        bot.md.update(D("80000.0"), D("80000.8"), D("10.0"), D("10.0"), clock.t)
        unwind_quotes = bot.engine.generate_ladder_quotes(m, bot.md, bot.ledger, clock.t, False, False)
        unwind_sells = [q for q in unwind_quotes if q.side == SELL]
        self.assertGreater(len(unwind_sells), 0, "Must place unwind sell quote")
        self.assertEqual(unwind_sells[0].price, D("80000.8"), "Unwind sell must be at market ask 80000.8")
        
        # Fill unwind sell:
        bot._on_fill(SELL, unwind_sells[0].qty, unwind_sells[0].price, Order("s1", 0, SELL, unwind_sells[0].price, unwind_sells[0].qty, unwind_sells[0].qty, 0, clock.t, clock.t, quote_mid=D("80000.4")))
        self.assertEqual(bot.ledger.position, D(0), "Position should be flat")
        self.assertGreater(bot.ledger.realized, D(0), "Realized PnL must be strictly positive")
        self.assertGreater(bot.ledger.spread_capture, D(0), "Spread capture must be strictly positive")
        print("✓ test_33_ultra_thin_liquid_market_spread_capture_and_touch_quoting passed: Direct touch quoting and spread capture verified.")

if __name__ == "__main__":
    unittest.main()


class TestDeadMansSwitch(unittest.IsolatedAsyncioTestCase):
    async def test_34_schedule_cancel_signing_and_refresh(self):
        """scheduleCancel is signed with legacy scheme (ts+action+canonical body) and refreshed by the bot."""
        import time as _t
        from utils import canonical
        bot, s, clock = sim.make()
        req = bot.signer.schedule_cancel(sim.MKT, int((_t.time() + 30) * 1e6))
        self.assertEqual(req["type"], "scheduleCancel")
        p = req["payload"]
        self.assertEqual(set(p), {"address", "accountIndex", "marketId", "time"})
        msg = f"{req['timestamp']}scheduleCancel{canonical(p)}".encode()
        bot.signer.priv.public_key().verify(bytes.fromhex(req["signature"]), msg)  # raises if wrong
        self.assertNotIn("time", bot.signer.schedule_cancel(sim.MKT, None)["payload"])  # disarm

        seen = []
        orig = s._post
        def spy(m):
            seen.append(m["request"]["type"])
            if m["request"]["type"] == "scheduleCancel":
                s._reply({"id": m["id"], "status": 200, "result": {"status": "scheduled"}})
            else:
                orig(m)
        s._post = spy
        await bot._heartbeat(clock.t)
        self.assertIn("scheduleCancel", seen)
        self.assertTrue(bot._dms_armed)
        n = len(seen)
        await bot._heartbeat(clock.t + 1)   # inside refresh interval -> no extra call
        self.assertEqual(len(seen), n)
        print("✓ test_34_schedule_cancel passed: signed correctly, armed, refresh throttled.")


# ============================================================================ #
# Cross-venue feeds (Binance / Bybit) + signals
# ============================================================================ #
import json as _json
import feeds as _feeds
from market import CrossVenueTracker


class _Sink:
    def __init__(self):
        self.bbo, self.depth, self.trades, self.liqs, self.disc = [], [], [], [], []
    def on_external_venue_bbo(self, v, b, a, bs, as_): self.bbo.append((v, b, a, bs, as_))
    def on_external_depth(self, v, b, a): self.depth.append((v, b, a))
    def on_external_trade(self, v, side, sz, px): self.trades.append((v, side, sz, px))
    def on_external_liq(self, v, side, sz, px): self.liqs.append((v, side, sz, px))
    def on_external_disconnect(self, v): self.disc.append(v)


class TestCrossVenueFeeds(unittest.TestCase):
    def test_35_binance_parsing(self):
        sk = _Sink()
        f = _feeds.BinanceFeed(sk, "SOLUSDT", "wss://x")
        self.assertIn("solusdt@bookTicker", f.full_url())
        self.assertIn("solusdt@depth10@100ms", f.full_url())
        f.handle(_json.dumps({"stream": "solusdt@bookTicker", "data": {"e": "bookTicker", "s": "SOLUSDT",
                              "b": "100.10", "B": "5", "a": "100.12", "A": "7"}}))
        f.handle(_json.dumps({"stream": "x", "data": {"e": "depthUpdate", "b": [["100.10", "5"]], "a": [["100.12", "7"]]}}))
        f.handle(_json.dumps({"data": {"e": "aggTrade", "p": "100.1", "q": "2", "m": True}}))    # aggressor SELL
        f.handle(_json.dumps({"data": {"e": "aggTrade", "p": "100.1", "q": "3", "m": False}}))   # aggressor BUY
        f.handle(_json.dumps({"data": {"e": "forceOrder", "o": {"S": "SELL", "q": "400", "z": "400", "p": "99", "ap": "99.5"}}}))
        self.assertEqual(sk.bbo[0], ("BINANCE", D("100.10"), D("100.12"), D("5"), D("7")))
        self.assertEqual(len(sk.depth), 1)
        self.assertEqual([t[1] for t in sk.trades], ["SELL", "BUY"])
        self.assertEqual(sk.liqs[0][:2], ("BINANCE", "SELL"))
        self.assertEqual(sk.liqs[0][3], D("99.5"))
        print("✓ test_35 passed: Binance bookTicker/depth/aggTrade/forceOrder parsed with correct aggressor sides.")

    def test_36_bybit_book_delta_and_sides(self):
        sk = _Sink()
        f = _feeds.BybitFeed(sk, "SOLUSDT", "wss://x")
        snap = {"topic": "orderbook.50.SOLUSDT", "type": "snapshot", "data": {"s": "SOLUSDT", "u": 5,
                "b": [["100.0", "4"], ["99.9", "6"]], "a": [["100.2", "3"], ["100.3", "9"]]}}
        f.handle(_json.dumps(snap))
        self.assertEqual(sk.bbo[-1][1:3], (D("100.0"), D("100.2")))
        # delta: new better bid, delete best ask, update 2nd level
        f.handle(_json.dumps({"topic": "orderbook.50.SOLUSDT", "type": "delta", "data": {"u": 6,
                 "b": [["100.1", "2"]], "a": [["100.2", "0"], ["100.3", "8"]]}}))
        self.assertEqual(sk.bbo[-1][1:3], (D("100.1"), D("100.3")))
        self.assertEqual(sk.depth[-1][2][0], ("100.3", "8"))
        # crossed (out-of-sync) book is ignored
        n = len(sk.bbo)
        f.handle(_json.dumps({"topic": "orderbook.50.SOLUSDT", "type": "delta", "data": {"u": 7, "b": [["100.5", "1"]], "a": []}}))
        self.assertEqual(len(sk.bbo), n)
        f.handle(_json.dumps({"topic": "publicTrade.SOLUSDT", "data": [{"S": "Buy", "v": "2", "p": "100.1"},
                                                                       {"S": "Sell", "v": "1", "p": "100.0"}]}))
        self.assertEqual([t[1] for t in sk.trades], ["BUY", "SELL"])
        # long position liquidated (Bybit side=Buy) => forced SELL
        f.handle(_json.dumps({"topic": "liquidation.SOLUSDT", "data": {"side": "Buy", "size": "50", "price": "99"}}))
        self.assertEqual(sk.liqs[-1][1], "SELL")
        self.assertEqual(_feeds.derive_symbol("SOL-USD"), "SOLUSDT")
        self.assertEqual(_feeds.derive_symbol("PEPE-USD", "1000pepeusdt"), "1000PEPEUSDT")
        print("✓ test_36 passed: Bybit snapshot/delta/delete, crossed-book guard, trade & liquidation sides.")

    def test_37_basis_lead_stale_and_no_venue_mixing(self):
        cr = CrossVenueTracker()
        t = 0.0
        loc = D("100")
        # Binance trades 5bps ABOVE arcus (USDT premium), Bybit 3bps above - constant offsets
        for i in range(200):
            t += 0.25
            cr.update_venue("BINANCE", loc * D("1.0005") - D("0.005"), loc * D("1.0005") + D("0.005"), D(1), D(1), t)
            cr.update_venue("BYBIT", loc * D("1.0003") - D("0.005"), loc * D("1.0003") + D("0.005"), D(1), D(1), t)
            cr.observe_local(loc, t)
        self.assertLess(abs(cr.lead_lag_divergence_bps(loc, t)), D("0.3"), "constant basis must be removed")
        self.assertLess(abs(cr.cross_velocity_bps(3.0, t)), D("0.01"), "two flat venues at different prices => zero velocity")
        # real lead: both venues jump +4bps, arcus unchanged
        t += 0.25
        for v, k in (("BINANCE", "1.0009"), ("BYBIT", "1.0007")):
            cr.update_venue(v, loc * D(k) - D("0.005"), loc * D(k) + D("0.005"), D(1), D(1), t)
        div = cr.lead_lag_divergence_bps(loc, t)
        self.assertGreater(div, D("3.0"))
        blk_buy, blk_sell, why = cr.pull_decision(loc, t, 2.5, 99.0, 0)
        self.assertTrue(blk_sell and not blk_buy, "ext above arcus => our ask is stale => pull ask")
        # consensus: only ONE venue moves => no pull
        cr2 = CrossVenueTracker()
        t2 = 0.0
        for i in range(200):
            t2 += 0.25
            cr2.update_venue("BINANCE", loc - D("0.005"), loc + D("0.005"), D(1), D(1), t2)
            cr2.update_venue("BYBIT", loc - D("0.005"), loc + D("0.005"), D(1), D(1), t2)
            cr2.observe_local(loc, t2)
        t2 += 0.25
        cr2.update_venue("BINANCE", loc * D("0.9995") - D("0.005"), loc * D("0.9995") + D("0.005"), D(1), D(1), t2)
        cr2.update_venue("BYBIT", loc - D("0.005"), loc + D("0.005"), D(1), D(1), t2)
        self.assertEqual(cr2.pull_decision(loc, t2, 2.5, 99.0, 0)[:2], (False, False))
        # staleness: silent venues stop influencing anything
        self.assertEqual(cr.lead_lag_divergence_bps(loc, t + 5.0), D("0"))
        self.assertEqual(cr.cross_obi(t + 5.0), D("0"))
        # disconnect drops the venue immediately
        cr.drop_venue("BINANCE")
        self.assertNotIn("BINANCE", cr.venues)
        print("✓ test_37 passed: basis removed, real lead detected, consensus required, stale/disconnect safe.")

    def test_38_flow_liquidation_depth(self):
        cr = CrossVenueTracker()
        t = 10.0
        cr.update_trade("BINANCE", "SELL", D(500), D(100), t)       # $50k aggressive selling
        cr.update_trade("BYBIT", "BUY", D(50), D(100), t)           # $5k buying
        self.assertLess(cr.cross_tfi(3.0, t), D("-0.4"))
        cr.update_trade("BINANCE", "BUY", D(1), D(100), t - 0.0)
        self.assertEqual(cr.cross_tfi(3.0, t + 10), D("0"))         # outside window
        cr.update_liquidation("BINANCE", "SELL", D(1000), D(100), t)   # $100k longs liquidated
        down, up = cr.liq_pressure_usd(5.0, t)
        self.assertEqual((down, up), (100000.0, 0.0))
        self.assertEqual(cr.pull_decision(D(100), t, 99.0, 99.0, 50000.0)[:2], (True, False))
        cr.update_depth("BINANCE", [["100", "10"], ["99.9", "10"]], [["100.1", "1"], ["100.2", "1"]], t)
        self.assertGreater(cr.venues["BINANCE"].obi, D("0.7"))
        print("✓ test_38 passed: external trade flow, liquidation pull, depth-weighted OBI.")


class TestCrossFeedLoop(unittest.IsolatedAsyncioTestCase):
    async def test_39_reconnect_resubscribe_and_disconnect_callback(self):
        sk = _Sink()
        sent, conns = [], []

        class FakeWS:
            def __init__(self, msgs): self.msgs = list(msgs)
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def send(self, m): sent.append(m)
            async def recv(self):
                if self.msgs:
                    return self.msgs.pop(0)
                await asyncio.sleep(10)      # idle -> triggers idle timeout reconnect

        def connect(url):
            conns.append(url)
            return FakeWS([_json.dumps({"topic": "publicTrade.SOLUSDT", "data": [{"S": "Buy", "v": "1", "p": "100"}]})])

        f = _feeds.BybitFeed(sk, "SOLUSDT", "wss://fake", connect)
        f.idle_timeout_s = 0.05
        task = asyncio.create_task(f.run())
        await asyncio.sleep(1.6)       # first reconnect backoff is 1s
        f.stop(); task.cancel()
        try: await task
        except asyncio.CancelledError: pass
        self.assertGreaterEqual(len(conns), 2, "must reconnect after idle")
        subs = [m for m in sent if '"subscribe"' in m]
        self.assertGreaterEqual(len(subs), 2, "must resubscribe on every connect")
        self.assertIn("orderbook.50.SOLUSDT", subs[0])
        self.assertIn("BYBIT", sk.disc)
        self.assertGreaterEqual(len(sk.trades), 2)
        print("✓ test_39 passed: idle->reconnect, resubscribe, disconnect callback clears venue.")

    async def test_40_bot_pulls_stale_bid_when_external_venues_drop(self):
        bot, s, clock = sim.make(EXTRA_LEVELS=0, ENABLE_CROSS_EXCHANGE=1, CROSS_WARMUP_S="5", CROSS_PULL_BPS="2.5",
                                 CROSS_VEL_PULL_BPS="50")
        def ext(px_mult):
            for v in ("BINANCE", "BYBIT"):
                mid = D("80050") * D(px_mult)
                bot.on_external_venue_bbo(v, mid - D("2"), mid + D("2"))
        for _ in range(40):                                  # 10s of calm: learns basis (+3bps USDT premium)
            ext("1.0003")
            await sim.step(bot, s, clock, "80000.0", "80100.0")
        self.assertIsNotNone(bot.om.get_order_by_slot(0, BUY), "bid quoted in calm market")
        self.assertLess(abs(bot.md.cross.lead_lag_divergence_bps(bot.md.mid, clock.t)), D("0.5"))
        ext("0.9995")                                        # both venues drop ~8bps vs learned basis; arcus stale
        await sim.step(bot, s, clock, "80000.0", "80100.0")
        self.assertIsNone(bot.om.get_order_by_slot(0, BUY), "stale bid must be pulled")
        self.assertIsNotNone(bot.om.get_order_by_slot(0, SELL), "ask on the safe side is kept")
        print("✓ test_40 passed: bot pulled the stale bid ahead of Arcus repricing (ask kept).")


class TestBybitSubscriptionFix(unittest.IsolatedAsyncioTestCase):
    async def test_41_bybit_subscribes_separately_and_survives_bad_liquidation_topic(self):
        sk = _Sink()
        sent = []
        class WS:
            async def send(self, m): sent.append(_json.loads(m))
        f = _feeds.BybitFeed(sk, "PUMPUSDT", "wss://x")
        await f.on_open(WS())
        self.assertEqual(sent[0]["args"], ["orderbook.50.PUMPUSDT", "publicTrade.PUMPUSDT"])
        self.assertEqual(sent[1]["args"], ["allLiquidation.PUMPUSDT"])   # separate request
        # exchange rejects the liquidation topic: price feed must NOT be disabled; legacy topic tried once
        f.handle(_json.dumps({"success": False, "ret_msg": "error:handler not found,topic:allLiquidation.PUMPUSDT", "op": "subscribe"}))
        self.assertFalse(f.disabled)
        self.assertEqual(f._resub, ["liquidation.PUMPUSDT"])
        f.handle(_json.dumps({"success": False, "ret_msg": "error:handler not found,topic:liquidation.PUMPUSDT", "op": "subscribe"}))
        self.assertFalse(f.disabled)
        # price data still flows
        f.handle(_json.dumps({"topic": "orderbook.50.PUMPUSDT", "type": "snapshot", "data": {"u": 3,
                 "b": [["0.0057", "100"]], "a": [["0.0058", "100"]]}}))
        self.assertEqual(len(sk.bbo), 1)
        # new allLiquidation payload (S=Buy => long liquidated => forced SELL)
        f.handle(_json.dumps({"topic": "allLiquidation.PUMPUSDT", "data": [{"T": 1, "s": "PUMPUSDT", "S": "Buy", "v": "1000", "p": "0.0057"}]}))
        self.assertEqual(sk.liqs[-1][1], "SELL")
        # a bad ORDERBOOK subscription (symbol not listed) disables the feed instead of reconnect-looping
        f.handle(_json.dumps({"success": False, "ret_msg": "error:handler not found,topic:orderbook.50.PUMPUSDT", "op": "subscribe"}))
        self.assertTrue(f.disabled)
        print("✓ test_41 passed: Bybit liquidation rejection no longer kills the price feed.")
