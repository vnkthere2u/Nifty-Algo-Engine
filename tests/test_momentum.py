import os, sys
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from nifty_algo.config import IST
from nifty_algo.journal import Journal
from nifty_algo.momentum import MomentumEngine, MomParams, rank_table, clean_series, last_weekday_of_month
from nifty_algo.notify import Notifier

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _panel(n_days=620, n_stocks=25, seed=3, crash_from=None):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2024-01-01", periods=n_days)
    drifts = np.linspace(-0.0005, 0.0015, n_stocks)
    C = pd.DataFrame({f"S{i:02d}": 100 * np.exp(np.cumsum(drifts[i] + rng.normal(0, 0.012, n_days))) for i in range(n_stocks)}, index=idx)
    n = pd.Series(20000 * np.exp(np.cumsum(0.0006 + rng.normal(0, 0.008, n_days))), index=idx)
    if crash_from is not None:
        k = idx.get_loc(pd.Timestamp(crash_from))
        n.iloc[k:] = n.iloc[k - 1] * np.exp(np.cumsum(np.full(n_days - k, -0.01)))
    return C, n


def _eng(tmp_path):
    return MomentumEngine(Journal(str(tmp_path / "m.db")), feed=None, notifier=Notifier())


def _at(d, hh=16, mm=30):
    d = pd.Timestamp(d)
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=IST)


def test_clean_series_backadjusts_demerger():
    c = pd.Series([100, 101, 102, 60, 61.0], index=pd.bdate_range("2026-01-01", periods=5))
    out = clean_series(c, c)
    assert out.pct_change().abs().max() < 0.05


def test_last_weekday_of_month():
    assert last_weekday_of_month("2026-11-17") == pd.Timestamp("2026-11-30")
    assert last_weekday_of_month("2026-05-05") == pd.Timestamp("2026-05-29")      # 31-May-2026 is a Sunday


def test_lifecycle_decide_fill_and_regime_exit(tmp_path):
    C, n = _panel()
    eng = _eng(tmp_path)
    mid = C.index[C.index.month == 4][-1] - pd.Timedelta(days=10)                 # some day in Apr-2025... any mid-month
    mid = C.index[C.index <= pd.Timestamp("2025-04-15")][-1]
    eng.process(C[C.index <= mid], n, _at(mid))
    assert eng.j.get("mom_last_decision") == "2025-03" and not eng.j.get("mom_pending")
    me = C.index[(C.index.year == 2025) & (C.index.month == 4)][-1]               # April month-end close
    eng.process(C[C.index <= me], n, _at(me))
    pend = eng.j.get("mom_pending")
    assert len(pend) == 10 and all(o["action"] == "BUY" for o in pend)
    nxt = C.index[C.index > me][0]
    eng.process(C[C.index <= nxt], n, _at(nxt))
    pos = eng.j.open_positions()
    assert len(pos) == 10 and all(p["entry_price"] == pytest.approx(C.at[nxt, p["symbol"]]) for p in pos)
    view = eng.j.get("mom_view")
    assert len(view["holdings"]) == 10 and view["ranking"][0]["rank"] == 1
    # later: NIFTY collapses -> next month-end decision sells everything
    C2, n2 = _panel(crash_from="2025-05-02")
    me2 = C2.index[(C2.index.year == 2025) & (C2.index.month == 5)][-1]
    eng.process(C2[C2.index <= me2], n2, _at(me2))
    pend = eng.j.get("mom_pending")
    assert len(pend) == 10 and all(o["action"] == "SELL" for o in pend)
    nxt2 = C2.index[C2.index > me2][0]
    eng.process(C2[C2.index <= nxt2], n2, _at(nxt2))
    assert eng.j.open_positions() == [] and len(eng.j.closed_positions()) == 10


def test_intraday_partial_bar_is_ignored(tmp_path):
    C, n = _panel()
    eng = _eng(tmp_path)
    me = C.index[(C.index.year == 2025) & (C.index.month == 4)][-1]
    eng.j.set("mom_last_decision", "2025-03")
    eng.process(C[C.index <= me], n, _at(me, 12, 0))          # month-end, but session still open
    assert not eng.j.get("mom_pending")
    eng.process(C[C.index <= me], n, _at(me, 16, 0))          # after the close
    assert len(eng.j.get("mom_pending")) == 10


DATA = os.path.join(ROOT, "data", "yahoo100")


@pytest.mark.skipif(not os.path.isdir(DATA), reason="research data not downloaded")
def test_live_engine_matches_research_backtest(tmp_path):
    sys.path.insert(0, os.path.join(ROOT, "research6")); sys.path.insert(0, os.path.join(ROOT, "research5"))
    from momentum import panel, backtest
    C, nifty = panel()
    r, w = backtest(C, nifty, top=10, lb=252, skip=21, vol_adj=True, regime=True)
    eng = _eng(tmp_path)
    month_ends = C.index.to_series().groupby(C.index.to_period("M")).max()
    month_ends = month_ends[(month_ends >= "2023-01-01") & (month_ends < C.index[-1])]
    eng.j.set("mom_last_decision", "2022-12")
    checked = 0
    for me in month_ends:
        nxt = C.index[C.index > me][0]
        eng.process(C[C.index <= me], nifty, _at(me, 16, 0))
        eng.process(C[C.index <= nxt], nifty, _at(nxt, 16, 0))
        live = sorted(p["symbol"] for p in eng.j.open_positions())
        bt = sorted(w.loc[nxt][w.loc[nxt] > 0].index)
        assert live == bt, f"{me.date()}: live {live} vs backtest {bt}"
        checked += 1
    assert checked >= 30
