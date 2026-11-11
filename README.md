# Nifty Algo Engine v5: paper-trading NIFTY TPMR + BTC low-vol breakout

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

## Live paper trading (NIFTY TPMR + BTC breakout)

| Piece | Where it runs | Why |
|---|---|---|
| Engine (`nifty_algo.runner`) | **GitHub Actions** (`.github/workflows/algo-engine.yml`) | free for public repos, never sleeps |
| State (SQLite journal) | branch **`algo-state`** (`nifty.db`, `btc.db`, `status.json`) | survives every run; single-commit snapshot |
| Dashboard (`algo_dashboard.py`) | Streamlit Cloud | read-only; downloads state every 60 s |
| Alerts | Telegram, sent by the engine | BUY / EXIT / break-even / skipped signal / failures |

Schedule (UTC cron, IST in brackets): `13 9 * * 1-5` (14:43, NIFTY session loop to 15:40), `41 9 * * 1-5`
(15:11, backup), `17 * * * *` (hourly: BTC daily decision after 00:00 UTC, stop/target checks, late NIFTY
decision from the 15:25 snapshot if the session job was missed). Runs are serialized (`concurrency`).

**One-time setup**
1. Merge this branch into `main` (scheduled workflows only run from the default branch).
2. GitHub → Settings → Secrets and variables → Actions → add `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID`
   (same values as in the Streamlit secrets; GitHub cannot read those).
3. Actions → *algo-engine* → *Run workflow* → mode `test-telegram` (you should get a test alert), then `once`.
4. Streamlit Cloud keeps serving `algo_dashboard.py` from `main`; no secrets needed for it.

**Local commands**
```bash
pip install -r requirements-engine.txt
python -m nifty_algo.runner --once            # one tick of both engines
python -m nifty_algo.runner --test-telegram   # needs TELEGRAM_TOKEN / TELEGRAM_CHAT_ID env vars
python -m nifty_algo.runner                   # run forever (VPS alternative to GitHub Actions)
python -m pytest -q tests                     # 22 tests incl. live-vs-backtest parity for both strategies
```

## Status / next steps toward live deployment

1. **Paper-trade** (the current mode) for at least 3 months and compare with the dashboard's backtest band.
2. Add broker order routing (Kite Connect, Upstox, etc.) behind `Engine._close` / the entry path, and
   map signals to the NIFTY futures contract (rollover rule) or deep-ITM calls.
3. Validated on NIFTY only. Do not apply it to other symbols without re-running the research.
