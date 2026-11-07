import json
import os
from dataclasses import replace
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from nifty_algo.config import IST, Config
from nifty_algo.confidence import confidence
from nifty_algo.engine import Engine
from nifty_algo.feeds import CircuitBreaker, FeedError, Snapshot, parse_chart_json, validate_today
from nifty_algo.indicators import atr, ibs, rsi
from nifty_algo.journal import Journal
from nifty_algo.notify import Notifier
from nifty_algo.strategy import PositionState, compute_features, evaluate_entry, evaluate_exit

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ------------------------------------------------------------------ indicators
def test_rsi_extremes_and_range():
    up = pd.Series(np.arange(1, 50, dtype=float))
    assert rsi(up, 2).iloc[-1] == pytest.approx(100.0)
    dn = pd.Series(np.arange(50, 1, -1, dtype=float))
    assert rsi(dn, 2).iloc[-1] == pytest.approx(0.0, abs=1e-9)
    rnd = pd.Series(100 + np.cumsum(np.random.default_rng(1).normal(0, 1, 500)))
    r = rsi(rnd, 2).dropna()
    assert ((r >= 0) & (r <= 100)).all()


def test_rsi_wilder_reference():
    # hand-computed Wilder RSI(2): diffs +1,-2,+1 -> up rma 1,.5,.75 ; dn rma 0,1,.5 -> RSI 60
    c = pd.Series([10.0, 11.0, 9.0, 10.0])
    r = rsi(c, 2)
    up = 1.0; dn = 0.0                      # first diff seeds the ewm
    up = .5 * 0 + .5 * up; dn = .5 * 2 + .5 * dn
    up = .5 * 1 + .5 * up; dn = .5 * 0 + .5 * dn
    assert r.iloc[-1] == pytest.approx(100 - 100 / (1 + up / dn))


def test_ibs_and_atr():
    h, l, c = pd.Series([10.0, 12.0]), pd.Series([8.0, 10.0]), pd.Series([9.0, 10.5])
    assert ibs(h, l, c).tolist() == pytest.approx([0.5, 0.25])
    a = atr(h, l, c, 2)
    assert a.iloc[0] == pytest.approx(2.0) and a.iloc[1] == pytest.approx(2.5)  # TR2 = max(2, 3, 1) = 3


# ------------------------------------------------------------------ feed
def _chart_payload(ts, o, h, l, c):
    return {"chart": {"result": [{"meta": {"gmtoffset": 19800}, "timestamp": ts,
                                  "indicators": {"quote": [{"open": o, "high": h, "low": l, "close": c, "volume": [0] * len(ts)}]}}],
                      "error": None}}


def test_parse_chart_json_ist_and_nulls():
    t0 = int(pd.Timestamp("2026-11-06 03:45", tz="UTC").timestamp())  # 09:15 IST
    js = _chart_payload([t0, t0 + 60, t0 + 120], [1, None, 3], [2, 2, 4], [0.5, 1, 2], [1.5, 1.5, 3.5])
    df = parse_chart_json(js)
    assert list(df.index.strftime("%H:%M")) == ["09:15", "09:17"]  # null row dropped
    assert df.Close.tolist() == [1.5, 3.5]


def test_parse_chart_json_error():
    with pytest.raises(FeedError):
        parse_chart_json({"chart": {"error": {"code": "Not Found"}, "result": None}})


def test_circuit_breaker():
    t = [0.0]
    cb = CircuitBreaker(threshold=2, cooldown_sec=10, clock=lambda: t[0])
    cb.failure(); assert cb.allow()
    cb.failure(); assert not cb.allow()
    t[0] = 11; assert cb.allow()


def test_validate_today_rejects_implausible():
    daily = pd.DataFrame({"Open": [100.0], "High": [101.0], "Low": [99.0], "Close": [100.0]})
    with pytest.raises(FeedError):
        validate_today(pd.Series({"Open": 100, "High": 130, "Low": 99, "Close": 125}), daily)


# ------------------------------------------------------------------ strategy
def _trend_with_dip(n=120):
    """Steady uptrend; the last day closes at its low after a sharp 3-day dip."""
    c = 20000 + np.arange(n) * 20.0
    c[-3:] = c[-4] - np.array([80, 160, 260])
    d = pd.DataFrame({"Open": c + 10, "High": c + 40, "Low": c - 40, "Close": c},
                     index=pd.bdate_range("2025-01-01", periods=n))
    d.iloc[-1, d.columns.get_loc("Low")] = d.Close.iloc[-1] - 1  # IBS ~ 0
    return d


def test_entry_signal_in_uptrend_dip():
    p = Config().strategy
    f = compute_features(_trend_with_dip(), p)
    dec = evaluate_entry(f.iloc[-1], p, len(f))
    assert dec.take and set(dec.setups) == {"RSI", "IBS"}


def test_no_entry_below_trend():
    p = Config().strategy
    d = _trend_with_dip()
    d[["Open", "High", "Low", "Close"]] = d[["Open", "High", "Low", "Close"]].iloc[::-1].values  # downtrend
    f = compute_features(d, p)
    dec = evaluate_entry(f.iloc[-1], p, len(f))
    assert not dec.take and any("regime" in r for r in dec.reasons)


def test_exit_rules():
    p = Config().strategy
    row = pd.Series({"Close": 101.0, "prev_high": 100.0})
    assert evaluate_exit(row, PositionState(95, 10, None, 1), p).startswith("TARGET")
    row = pd.Series({"Close": 99.0, "prev_high": 100.0})
    assert evaluate_exit(row, PositionState(95, 10, None, 3), p) is None
    assert evaluate_exit(row, PositionState(95, 10, None, p.max_hold_days), p).startswith("TIME")


def test_confidence_uses_setup_then_pooled():
    pri = {"ALL": {"wins": 70, "losses": 30}, "RSI": {"wins": 30, "losses": 5}, "IBS+RSI": {"wins": 3, "losses": 0}}
    c1 = confidence("RSI", pri)
    assert c1["basis"] == "RSI" and 75 < c1["pct"] < 90 and c1["low90_pct"] < c1["pct"]
    c2 = confidence("IBS+RSI", pri)
    assert c2["basis"] == "ALL"
    c3 = confidence("RSI", pri, {"RSI": {"wins": 0, "losses": 10}})
    assert c3["pct"] < c1["pct"]  # live losses pull confidence down


# ------------------------------------------------------------------ engine lifecycle
class ScriptedFeed:
    """Serves a daily history plus a scripted 'today' bar per date."""
    def __init__(self, daily, today_bars, as_of_lag_sec=60):
        self.daily, self.today_bars, self.lag = daily, today_bars, as_of_lag_sec
        self.calls = 0

    def snapshot(self, now, session_open_min, cutoff_min=None):
        self.calls += 1
        d = pd.Timestamp(now.date())
        hist = self.daily[self.daily.index < d]
        tb = self.today_bars.get(now.date())
        if tb is None:
            return Snapshot(hist, None, None, None)
        return Snapshot(hist, pd.Series(tb, name=d), float(tb["Close"]), now - timedelta(seconds=self.lag))


def _engine(tmp_path, feed, **cfg_over):
    cfg = replace(Config(), db_path=str(tmp_path / "j.db"), lock_path=str(tmp_path / "l"),
                  priors_path=os.path.join(ROOT, "nifty_algo", "priors.json"), **cfg_over)
    return Engine(cfg, Journal(cfg.db_path), feed, Notifier())


def _at(d, hh, mm, ss=5):
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=IST)


def _scenario():
    hist = _trend_with_dip(121)
    entry_day = hist.index[-1].date()
    daily = hist.iloc[:-1]
    eb = hist.iloc[-1][["Open", "High", "Low", "Close"]].to_dict()
    exit_day = (hist.index[-1] + pd.offsets.BDay(1)).date()
    # next day: rally above previous day's high
    xb = {"Open": eb["Close"] + 20, "High": eb["High"] + 200, "Low": eb["Close"] + 10, "Close": eb["High"] + 150}
    full = pd.concat([daily, pd.DataFrame([eb], index=[pd.Timestamp(entry_day)])])
    return full, {entry_day: eb, exit_day: xb}, entry_day, exit_day


def test_full_lifecycle_entry_hold_exit_and_idempotency(tmp_path):
    daily, bars, d0, d1 = _scenario()
    feed = ScriptedFeed(daily, bars)
    eng = _engine(tmp_path, feed)
    assert eng.tick(_at(d0, 15, 20)) == []                       # before decision window: nothing
    assert "decision" in eng.tick(_at(d0, 15, 25))
    pos = eng.j.open_position()
    assert pos and pos["setup"] in ("RSI", "IBS", "IBS+RSI") and pos["confidence"] > 50
    assert any("BUY" in m for m in eng.n.sent)
    # repeated ticks in the window must not create a second decision/position
    assert "decision" not in eng.tick(_at(d0, 15, 26))
    with eng.j.conn() as c:
        assert c.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 1
    # EOD summary once
    assert "eod" in eng.tick(_at(d0, 15, 40))
    assert "eod" not in eng.tick(_at(d0, 15, 45))
    # restart: brand-new engine object on the same DB resumes the open position
    eng2 = _engine(tmp_path, feed)
    assert eng2.j.open_position()["id"] == pos["id"]
    assert "monitor" in eng2.tick(_at(d1, 10, 0))
    assert "decision" in eng2.tick(_at(d1, 15, 25))
    closed = eng2.j.closed_positions()
    assert len(closed) == 1 and closed[0]["exit_reason"].startswith("TARGET") and closed[0]["net_pts"] > 0
    assert eng2.j.open_position() is None


def test_stale_data_retries_then_skips(tmp_path):
    daily, bars, d0, _ = _scenario()
    feed = ScriptedFeed(daily, bars, as_of_lag_sec=3600)
    eng = _engine(tmp_path, feed)
    eng.tick(_at(d0, 15, 25))
    assert not eng.j.decision_exists(d0.isoformat(), "DECISION")      # retry pending
    eng.tick(_at(d0, 15, 29))
    with eng.j.conn() as c:
        assert c.execute("SELECT action FROM decisions").fetchone()[0] == "SKIPPED_STALE"
    assert eng.j.open_position() is None


def test_holiday_records_no_session(tmp_path):
    daily, bars, d0, _ = _scenario()
    feed = ScriptedFeed(daily, {})
    eng = _engine(tmp_path, feed)
    eng.tick(_at(d0, 15, 29))
    with eng.j.conn() as c:
        assert c.execute("SELECT action FROM decisions").fetchone()[0] == "NO_SESSION"


def test_disaster_stop_in_monitor(tmp_path):
    daily, bars, d0, d1 = _scenario()
    crash = {"Open": 1000.0, "High": 1000.0, "Low": 900.0, "Close": 900.0}
    feed = ScriptedFeed(daily, {d0: bars[d0], d1: crash})
    eng = _engine(tmp_path, feed)
    eng.tick(_at(d0, 15, 25))
    assert eng.j.open_position() is not None
    # crash next day is implausible (>15%) -> validated in real feed; scripted feed bypasses validation,
    # so the monitor must still enforce the disaster stop
    eng.tick(_at(d1, 9, 20))
    c = eng.j.closed_positions()
    assert len(c) == 1 and c[0]["exit_reason"].startswith("DISASTER")


def test_risk_governor_blocks_after_losses(tmp_path):
    daily, bars, d0, _ = _scenario()
    eng = _engine(tmp_path, ScriptedFeed(daily, bars), max_consecutive_losses=2)
    for i in range(2):
        eng.j.insert_position(symbol="X", side="LONG", lots=1, lot_size=65, entry_ts="", entry_date=f"2020-01-0{i+1}",
                              entry_price=100, entry_atr=1, disaster_stop=None, setup="IBS", confidence=70,
                              confidence_low=60, features="{}")
        p = eng.j.open_position()
        eng.j.update_position(p["id"], status="CLOSED", net_pts=-50.0)
    eng.tick(_at(d0, 15, 25))
    with eng.j.conn() as c:
        assert c.execute("SELECT action FROM decisions").fetchone()[0] == "BLOCKED_RISK"
    eng.reset_risk()
    assert eng._risk_block() is None


def test_journal_refuses_second_open_position(tmp_path):
    j = Journal(str(tmp_path / "x.db"))
    kw = dict(symbol="X", side="LONG", lots=1, lot_size=65, entry_ts="", entry_date="2020-01-01", entry_price=1,
              entry_atr=1, disaster_stop=None, setup="IBS", confidence=1, confidence_low=1, features="{}")
    j.insert_position(**kw)
    with pytest.raises(RuntimeError):
        j.insert_position(**kw)


# ------------------------------------------------------------------ parity on real data (optional)
DATA = os.path.join(ROOT, "data", "raw")


@pytest.mark.skipif(not os.path.isdir(DATA), reason="historical data not downloaded")
def test_live_engine_matches_backtest(tmp_path):
    from nifty_algo.backtest import load_minutes, run_backtest
    from nifty_algo.replay import replay
    cfg = replace(Config(), db_path=str(tmp_path / "r.db"))
    m1 = load_minutes(DATA)
    m1 = m1[m1.index >= "2023-06-01"]
    bt = run_backtest(m1, cfg, "full")
    bt = bt[pd.to_datetime(bt.entry_date) >= "2024-07-01"]
    eng = replay(m1, cfg, "2024-06-01", "2025-06-30", cfg.db_path)
    lv = pd.DataFrame(eng.j.closed_positions())
    lv = lv[pd.to_datetime(lv.entry_date) >= "2024-07-01"]
    bt = bt[pd.to_datetime(bt.exit_date) <= pd.to_datetime(lv.exit_date).max()]
    assert len(bt) == len(lv) > 5
    assert np.allclose(bt.entry.values, lv.entry_price.values) and np.allclose(bt.exit.values, lv.exit_price.values)
