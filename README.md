# Arcus Level 8+ Institutional Market Maker: Tight-Spread & Alpha-Driven Engine

A high-frequency quantitative market-making system engineered for tight-spread perpetual DEXes and limit order books.

---

## Architecture Overview

```
                               ┌─────────────────────────────────┐
                               │   Market Ingestion & Ticks/L2   │
                               └────────────────┬────────────────┘
                                                │
                 ┌──────────────────────────────┴──────────────────────────────┐
                 ▼                                                             ▼
┌─────────────────────────────────┐                           ┌─────────────────────────────────┐
│   Expanded Microstructure State │                           │   Cross-Exchange & Mark Alpha   │
│ - Multi-horizon TFI (0.25s-10s) │                           │ - CEX Lead-Lag (Binance/Bybit)  │
│ - Multi-depth OBI (L1, L5, L10) │                           │ - Reference Basis Reversion     │
│ - Liquidity Fragility (1s flow) │                           │ - Funding Carry Direction       │
│ - Flow Acceleration/Decel       │                           └────────────────┬────────────────┘
└────────────────┬────────────────┘                                            │
                 │                                                             │
                 └──────────────────────────────┬──────────────────────────────┘
                                                ▼
                               ┌─────────────────────────────────┐
                               │    Alpha-Aware Target Inv       │
                               │  q_target = f(Alpha, Funding)   │
                               │  ResPrice = f(Fair, q - q_targ) │
                               └────────────────┬────────────────┘
                                                │
                                                ▼
                               ┌─────────────────────────────────┐
                               │  Selective-Touch Candidate Eval │
                               │  - Touch vs 1-Tick vs Model     │
                               │  - Queue Hazard Fill Prob P(H)  │
                               │  - Empirical E[Markout|State]   │
                               │  - One-Sided Steam Suppression  │
                               └────────────────┬────────────────┘
                                                │
                 ┌──────────────────────────────┴──────────────────────────────┐
                 ▼                                                             ▼
┌─────────────────────────────────┐                           ┌─────────────────────────────────┐
│   Dynamic Maker Scratch Unwind  │                           │   Quote Opportunity Dataset     │
│ - Time-decay target profit      │                           │ - State + Candidate Matrix      │
│ - Zero maker fee queue priority │                           │ - Logs to quote_opps.jsonl      │
│ - L2 VWAP Taker Book Walking    │                           │ - Walk-forward model training   │
└─────────────────────────────────┘                           └─────────────────────────────────┘
```

---

## Key Modules & Implementations

### 1. Empirical Bayesian Conditional Markout Model (`ledger.py`)
Separates fill probability from post-fill returns:
* Predicts $E[\text{Markout} \mid \text{Side}, \text{Regime}, \text{Level}, \text{Horizon}]$.
* Applies Empirical Bayes shrinkage toward theoretical priors when sample sizes are small:
  $$\hat{\mu} = \frac{N}{N + N_0} \bar{X} + \frac{N_0}{N + N_0} \mu_{\text{prior}}$$
* Tracks multi-horizon post-fill markouts at $250\text{ms}, 500\text{ms}, 1\text{s}, 2\text{s}, 5\text{s}, 10\text{s},$ and $30\text{s}$.

### 2. Expanded Microstructure Feature Pipeline (`market.py`)
Computes high-frequency microstructure signals:
* **Order Book Imbalance**: $\text{OBI}_{\text{L1}}$, $\text{OBI}_{\text{L5}}$, $\text{OBI}_{\text{L10}}$.
* **Trade Flow Imbalance**: Rolling multi-horizon $\text{TFI}$ at $250\text{ms}, 500\text{ms}, 1\text{s}, 2\text{s}, 5\text{s},$ and $10\text{s}$.
* **Flow Acceleration**: $\text{TFI}(1s) - \text{TFI}(5s)$ to distinguish surging momentum from decelerating flows.
* **Depth Concentration**: Ratio of Level 0 touch depth to top-5 aggregate depth.
* **Microprice Spread**: $(P_{\text{micro}} - P_{\text{mid}}) / P_{\text{mid}} \times 10{,}000$ (bps).
* **Reference Basis**: $(P_{\text{mid}} - P_{\text{mark}}) / P_{\text{mark}} \times 10{,}000$ (bps).

### 3. Liquidity Fragility Signal (`market.py` & `engine.py`)
$$\text{Fragility} = \frac{\text{Aggressive Counter-Volume}_{1s}}{\text{Resting Depth}_{L1-L5}}$$
When fragility exceeds `FRAGILITY_THRESHOLD` ($0.60$), the resting level is being actively consumed. The engine pulls touch quotes or shifts back 1 tick.

### 4. Alpha-Aware & Funding-Aware Inventory Target (`engine.py`)
Inventory is no longer a purely mean-reverting variable:
$$q_{\text{target}} = \text{clamp}\left(\frac{\alpha_{\text{short-term}} + \text{Funding Carry}}{\gamma \cdot (1 + \sigma / 10)}, \; -q_{\text{max\_safe}}, \; +q_{\text{max\_safe}}\right)$$
* In bullish regimes, positive inventory is tolerated.
* When funding is positive (longs pay shorts), target inventory shifts short to harvest funding yield.
* Reservation pricing skews relative to $(q - q_{\text{target}})$.

### 5. One-Sided Touch Protection (`engine.py`)
When `ENABLE_ONESIDED_TOUCH=1` and toxic flow is detected:
* If aggressive buying surges ($\text{TFI} \ge 0.45$, flow bias $\ge 0.40$), touch asks are suppressed.
* If aggressive selling surges ($\text{TFI} \le -0.45$, flow bias $\le -0.40$), touch bids are suppressed.

### 6. Reversal & Absorption Mode (`market.py` & `engine.py`)
Toggled via `ENABLE_ABSORPTION_MODE=1`:
* **Momentum State**: Selling accelerates, book thins $\rightarrow$ avoid buying.
* **Exhaustion State**: Selling volume was heavy but decelerates ($\text{TFI}_{\text{accel}} > 0.3$), opposite depth holds, price drop stalls $\rightarrow$ provides bid liquidity at favorable spreads.

### 7. Quote Opportunity Dataset Logger (`bot.py`)
When `ENABLE_QUOTE_DATASET=1`, writes every quoting decision to `quote_opportunities.jsonl`:
* Full microstructure state snapshot.
* Inventory, position, and PnL.
* Evaluated candidate quotes, predicted fill probabilities, predicted markouts, and net EVs.
* Execution decisions (PLACE, MODIFY, CANCEL, NO_QUOTE).

### 8. L2 VWAP Order Book Walking for Taker Exits (`engine.py`)
Replaces heuristic crossing assumptions:
* Walks the live order book depth level-by-level to calculate exact VWAP and slippage.
* Compares expected loss from waiting against actual crossing cost + taker fees.

---

## Automated Verification Suite

Run all 26 unit and simulation tests:
```bash
python3 -m unittest test_level7.py
```
Run live or simulated trading:
```bash
python3 main.py --mode sim
python3 main.py --mode live
```
