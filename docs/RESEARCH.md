# Strategy Research Report — v3.3 audit → TPMR

**Data:** NIFTY 50 index, 1-minute bars, 3-Apr-2017 → 6-Nov-2026 (888,302 bars; public dataset
`technovusin/nifty50-historical-data`). **Costs:** 0.04 % of notional round-trip (STT + exchange +
brokerage + ~1 pt slippage/side on futures). **Validation:** parameters chosen on 2017–2022 only,
**2023–2026 held out**. "Win" = trade net of costs > 0.

All numbers below are reproducible with the scripts in `research/` (`NIFTY_DATA_DIR=data/raw`).

---

## 1. Verdict on the original v3.3 logic: **SCRAPPED**

`research/replica_v33.py` replays the live code exactly as written: 5-minute scan cadence,
yfinance-style forming bars, the 16-minute anchor window, all filters, snapshot-price exits, ratchet
stops and overnight carry.

| Metric | v3.3 replica |
|---|---|
| Trades (9.5 yrs) | 388 |
| **Win rate (net)** | **40.7 %** |
| Profit factor | 1.11 |
| Expectancy | +0.05 R / trade |
| Worst year | 2025: −1,423 pts (25.6 % win) |

**Tweak phase** (`research/tweak_ema_cross.py`, 8,352 variants: ADX level/slope, HTF, separation,
extension, time window × 9 SL/TP geometries × BE × overnight × signal-exit):

* Every 70–80 % win variant gets there through **payoff geometry** (TP 1·ATR vs SL 2.5–3·ATR,
  break-even win rate 71–75 %). All of them **lose money out of sample** (OOS PF 0.71–0.87).
* 13 of 8,352 variants pass ≥60 % win with PF > 1.1 on both halves. That is roughly the
  false-discovery count expected from a search this size, and they are economically thin
  (≈ +100 pts/yr).
* The crossover carries a weak trend edge, but it only shows up at 20–35 % win rates.

**Structural findings**

1. NIFTY intraday 15-minute signals, whether trend-following *or* mean-reverting
   (`research/new_intraday_mr.py`, 1,620 variants), **do not survive realistic costs**. The edge
   per trade is smaller than ~10 pts of friction.
2. The exploitable edge sits at the **multi-day horizon**: short-term oversold closes inside an
   intermediate uptrend revert. Shorts fail in every configuration, which is consistent with the
   index's structural upward drift and the asymmetric reflexivity of Indian equity flows (domestic
   SIP buying absorbs dips).
3. **Stops hurt mean reversion.** Any stop tighter than ~8·ATR lowers both win rate and PF, because
   it sells the very dislocation being bought. Tail risk is handled by position size, a
   catastrophic stop and a time stop.
4. A feature-based ML confidence score has **no walk-forward skill** (Brier 0.195 vs 0.186 for the
   base rate) on ~100 trades, so confidence is reported as an honest Bayesian setup win-rate.

---

## 2. Replacement model: NIFTY Trend-Pullback Mean Reversion (TPMR)

**Decision once per day at 15:25 IST**, using the live price as today's close:

| | Rule |
|---|---|
| Regime | price > SMA(50) of daily closes |
| Setup A | RSI(2) (Wilder) < 10 |
| Setup B | IBS = (C − L)/(H − L) < 0.15, i.e. closing near the day's low |
| Entry | BUY at 15:25 if regime and (A or B) |
| Exit | at a later 15:25, price > previous day's high; or 10-day time stop |
| Catastrophic stop | 8 × ATR(14) intraday (never triggered 2017–2026) |
| Position | one at a time, long only |

### Results (`python -m nifty_algo.backtest --history full`)

| Metric | Full 2017–2026 | In-sample 2017–22 | **Out-of-sample 2023–26** |
|---|---|---|---|
| Trades | 153 (16.7/yr) | 92 | 61 |
| **Win rate** | **72.5 %** (95 % CI 65.0–79.0) | 70.7 % | **75.4 %** |
| Profit factor | 1.57 | 1.51 | 1.62 |
| Avg win / loss | +150.5 / −253.5 pts | | |
| Expectancy | +39.6 pts/trade | | |
| Max drawdown | −1,932 pts | | |
| Avg holding | 3.7 days | | |

By setup: **RSI** 78.6 % (PF 3.51) · **IBS+RSI** 90.0 % (PF 4.67) · **IBS** 70.5 % (PF 1.38).

**Robustness checks passed**

* Broad parameter plateau: RSI(2) thresholds 10–20 and trend SMAs 50/100/200 give 64–78 %
  OOS win; it is not a single lucky spike.
* Doubling costs to 0.08 %: win 69.5 %, PF 1.43.
* Executing at the **next day's open** instead of 15:25: win 68.9 %, PF 1.83.
* Live engine tick-by-tick replay matches the backtest **trade-for-trade, to the paisa** (57/58).
  The one difference is the 2025 Diwali Muhurat session, where the live engine correctly refused
  stale data.

### Honest risk framing

* Payoff is **negatively skewed**: an average loss is ~1.7× an average win. A 72 % win rate is
  needed just to keep PF ≈ 1.6, so a regime where the win rate drops to ~63 % is breakeven. The
  dashboard raises an *edge-degradation alert* if the live win rate falls below the backtest 95 %
  lower bound.
* Losing years happen: 2017 (−317), 2018 (−132), 2022 (−229), with a 1,932-pt peak-to-trough.
  Per lot (65 units) that is roughly ₹1.26 L, so size capital for at least 2× that.
* ~17 trades/yr means statistical confirmation from live trading takes 1–2 years. Paper-trade first.
* Validated **only on the NIFTY index**. The index is not tradable directly: execute via
  NIFTY futures (basis and rollover not modelled; the strategy is long-only, so the basis is a
  small, mostly constant drag) or deep-ITM options.
