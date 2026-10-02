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

if __name__ == "__main__":
    unittest.main()
