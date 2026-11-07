# Nifty Algo Engine v4.0: NIFTY Trend-Pullback Mean Reversion (TPMR)

A rule-based, self-running NIFTY signal engine with a full decision journal, Telegram alerts,
calibrated confidence % and a read-only dashboard.

> **Research verdict:** the original v3.3 EMA-crossover logic scored **40.7 % win** over 9.5 years and
> was scrapped. Its replacement, TPMR, scores **72.5 % win (75.4 % out-of-sample), PF 1.57**.
> Full evidence: [`docs/RESEARCH.md`](docs/RESEARCH.md).

## Strategy (one decision per day, 15:25 IST)

* **BUY** when price > SMA50(daily) **and** (RSI(2) < 10 **or** IBS < 0.15)
* **SELL** at a later 15:25 when price > previous day's high, or after 10 days
* Disaster stop 8×ATR (insurance only) · one position at a time · long only

## Architecture

```
nifty_algo/
  strategy.py    pure signal/exit rules, shared by backtest AND live engine (no drift possible)
  indicators.py  Wilder RSI/ATR, SMA, IBS
  feeds.py       Yahoo v8 chart API: timeouts, retry+backoff, circuit breaker, cache, validation, yfinance fallback
  engine.py      idempotent state machine: MONITOR (5 min) / DECISION (15:25) / EOD
  journal.py     SQLite (WAL): decisions, positions, events, state; single writer
  confidence.py  Bayesian setup win-rate (backtest prior + live updates)
  runner.py      single-instance process (file lock), signal-safe loop, CLI
  backtest.py    backtester on 1-minute history -> summary, trades, priors.json
  replay.py      drives the LIVE engine tick-by-tick on history (parity + dry-run)
algo_dashboard.py  Streamlit UI, read-only on the journal (never fetches data)
research/          every study behind the verdict (replica, tweak grids, new-model search)
```

**What was fixed relative to v3.3:**

* **Hung servers and request floods:** v3.3 pulled 18 symbols × 15 days of 15m bars every 5 minutes
  through a Streamlit thread. v4 makes about 2 small requests per cycle, with hard timeouts and a
  circuit breaker.
* **Overlapping scans:** one OS process with an exclusive file lock, and sequential ticks.
* **Delayed or duplicate execution:** each decision is written once per date (a DB unique key). After
  a restart the engine resumes from the journal.
* **Lost state:** v3.3 kept its database in `/tmp` on Streamlit Cloud. v4 uses a persistent
  `data/algo_journal.db`.
* **"Is the logic working?":** every 15:25 decision is logged with all features and reasons
  (NO_TRADE / ENTRY / HOLD / EXIT / BLOCKED_RISK / SKIPPED_STALE / NO_SESSION). The dashboard
  compares the live win rate with the backtest confidence interval.

## Quick start

```bash
pip install -r requirements.txt
python scripts/fetch_history.py                          # ~60 MB of 1-min NIFTY history
python -m nifty_algo.backtest --write-priors             # reproduce results + confidence priors
python -m pytest -q tests                                # 18 tests incl. live-vs-backtest parity
export TELEGRAM_TOKEN=... TELEGRAM_CHAT_ID=...           # optional alerts
python -m nifty_algo.runner                              # start the engine (keep it running)
streamlit run algo_dashboard.py                          # dashboard (separate process)
```

Other commands: `python -m nifty_algo.runner --status` · `--once` · `--reset-risk` ·
`python -m nifty_algo.replay --start 2025-01-01` (full offline dry-run).

**Hosting:** run the engine on an always-on Linux box or VM (`deploy/*.service` systemd units).
Streamlit Community Cloud sleeps idle apps and wipes `/tmp`, so it cannot host the engine.

Config: `nifty_algo/config.py` (or env `NIFTY_ALGO_<FIELD>` / JSON via `--config`). Verify
`lot_size` against the current NSE circular.

## Status / next steps toward live deployment

1. **Paper-trade** (the current mode) for at least 3 months and compare with the dashboard's backtest band.
2. Add broker order routing (Kite Connect, Upstox, etc.) behind `Engine._close` / the entry path, and
   map signals to the NIFTY futures contract (rollover rule) or deep-ITM calls.
3. Validated on NIFTY only. Do not apply it to other symbols without re-running the research.
