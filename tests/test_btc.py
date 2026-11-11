import os, sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from nifty_algo.btc import BtcEngine, BtcParams, features, parse_kraken, to_inr, step_bar, open_pos
from nifty_algo.journal import Journal
from nifty_algo.notify import Notifier

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UTC = timezone.utc


def test_parse_kraken_and_inr():
    js = {"error": [], "result": {"XXBTZUSD": [[1767225600, "100", "110", "90", "105", "0", "1", 1],
                                               [1767312000, "105", "120", "100", "118", "0", "1", 1]], "last": 1}}
    d = parse_kraken(js)
    assert list(d.Close) == [105.0, 118.0] and d.index[0] == pd.Timestamp("2026-01-01")
    fx = pd.Series([88.0], index=[pd.Timestamp("2025-12-31")])
    inr = to_inr(d, fx)
    assert inr.Close.tolist() == [105 * 88.0, 118 * 88.0]


def test_step_bar_rules():
    p = BtcParams()
    ps = open_pos("2026-01-01", 100.0, 5.0, p)          # SL 95, TP 110, hard 90
    assert open_pos("x", 100.0, 11.0, p) is None         # >10% risk skipped
    assert step_bar(ps, 100, 104, 96, 94, first=True, completed=False, p=p) is None   # close-stop waits for completed bar
    assert step_bar(ps, 100, 104, 96, 94, first=True, completed=True, p=p) == (94, "SL")
    ps = open_pos("2026-01-01", 100.0, 5.0, p)
    assert step_bar(ps, 100, 106, 99, 104, first=True, completed=True, p=p) is None and ps.sl == pytest.approx(100.7)
    assert step_bar(ps, 99, 101, 98, 100, first=False, completed=False, p=p) == (99, "BE")   # gap below BE stop -> open
    ps = open_pos("2026-01-01", 100.0, 5.0, p)
    assert step_bar(ps, 112, 113, 111, 112, first=False, completed=False, p=p) == (112, "TP")  # gap above target


DATA = os.path.join(ROOT, "data", "yahoo", "BTC_1d.pkl")


@pytest.mark.skipif(not os.path.exists(DATA), reason="research data not downloaded")
def test_live_engine_matches_research_backtest(tmp_path):
    sys.path.insert(0, os.path.join(ROOT, "research2"))
    from data2 import load
    from signals import features as rfeat, TRENDS, SETUPS, STOPS
    from sim import run
    d = load("BTC")
    f = rfeat(d)
    sig = (SETUPS["BO_C20"][0](f) & TRENDS["T3"](f) & f.sma200.notna()).fillna(False)
    ref = run(f, sig.values, STOPS["A3.5"](f).values, 0.006, R=2.0, be_R=1.0, atr=f.atr.values, close_stop=True)
    start = d.index[260]
    ref = ref[(ref.entry_date > start) & (ref.reason != "OPEN")]

    eng = BtcEngine(Journal(str(tmp_path / "b.db")), feed=None, notifier=Notifier())
    for k in range(260, len(d)):
        day = d.index[k]
        # mid-day run: today's bar seen as partial (full-day values), all prior days completed
        eng.process(d.iloc[:k + 1], datetime(day.year, day.month, day.day, 12, tzinfo=UTC))
    # final run the next day to complete the last bar
    nxt = d.index[-1] + timedelta(days=1)
    eng.process(d, datetime(nxt.year, nxt.month, nxt.day, 1, tzinfo=UTC))
    live = pd.DataFrame(eng.j.closed_positions())
    assert len(live) == len(ref) >= 20
    assert np.allclose(live.entry_price.values, ref.entry.values)
    assert np.allclose(live.exit_price.values, ref.exit.values)
    assert ((live.net_inr > 0).values == (ref.ret > 0).values).all()
