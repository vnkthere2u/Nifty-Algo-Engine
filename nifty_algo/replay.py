"""Historical replay: drives the LIVE engine tick-by-tick with a feed that serves exactly what
Yahoo would have shown at each instant.  Used for parity tests (live == backtest) and for
dry-running the whole system end-to-end without a network.

    python -m nifty_algo.replay --data data/raw --start 2024-01-01 --end 2026-11-06 --db data/replay.db
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from .bars import daily_snapshot
from .config import IST, Config, load_config
from .engine import Engine
from .feeds import Snapshot
from .journal import Journal
from .notify import Notifier


class HistoricalFeed:
    def __init__(self, m1: pd.DataFrame, session_close_min: int = 930):
        self.m1 = m1
        self.full_daily = daily_snapshot(m1, session_close_min)[["Open", "High", "Low", "Close"]]
        self.t = m1.index.values
        self.days = {d: (a, b) for d, a, b in self._day_bounds()}

    def _day_bounds(self):
        dn = self.m1.index.normalize().values
        starts = np.r_[0, np.flatnonzero(dn[1:] != dn[:-1]) + 1]
        ends = np.r_[starts[1:], len(dn)]
        for a, b in zip(starts, ends):
            yield pd.Timestamp(dn[a]).date(), a, b

    def snapshot(self, now: datetime, session_open_min: int, cutoff_min=None) -> Snapshot:
        n = now.astimezone(IST).replace(tzinfo=None)
        today = n.date()
        daily = self.full_daily[self.full_daily.index.date < today]
        if today not in self.days:
            return Snapshot(daily=daily, today=None, last_price=None, as_of=None)
        a, b = self.days[today]
        x = self.m1.iloc[a:b]
        mins = x.index.hour * 60 + x.index.minute
        lim = n.hour * 60 + n.minute  # bars starting at or before 'now' (Yahoo shows the forming minute)
        keep = (mins >= session_open_min) & (mins <= lim)
        if cutoff_min is not None:
            keep &= mins < cutoff_min
        x = x[keep]
        if x.empty:
            return Snapshot(daily=daily, today=None, last_price=None, as_of=None)
        tb = pd.Series({"Open": x.Open.iloc[0], "High": x.High.max(), "Low": x.Low.min(), "Close": x.Close.iloc[-1]},
                       name=pd.Timestamp(today))
        return Snapshot(daily=daily, today=tb, last_price=float(x.Close.iloc[-1]),
                        as_of=x.index[-1].to_pydatetime().replace(tzinfo=IST))


def replay(m1: pd.DataFrame, cfg: Config, start: str, end: str, db_path: str, step_min: int = 5) -> Engine:
    if os.path.exists(db_path):
        os.remove(db_path)
    feed = HistoricalFeed(m1, cfg.session_close_min)
    eng = Engine(cfg, Journal(db_path), feed, Notifier())
    days = sorted(d for d in feed.days if pd.Timestamp(start).date() <= d <= pd.Timestamp(end).date())
    for d in days:
        base = datetime(d.year, d.month, d.day, tzinfo=IST)
        for mins in range(cfg.session_open_min, cfg.session_close_min + 11, step_min):
            eng.tick(base + timedelta(minutes=mins, seconds=5))
    return eng


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data/raw")
    ap.add_argument("--config", default=None)
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2099-01-01")
    ap.add_argument("--db", default="data/replay.db")
    a = ap.parse_args()
    from .backtest import load_minutes
    cfg = load_config(a.config)
    eng = replay(load_minutes(a.data), cfg, a.start, a.end, a.db)
    tr = pd.DataFrame(eng.j.closed_positions())
    print(tr[["entry_date", "entry_price", "exit_date", "exit_price", "setup", "confidence", "exit_reason", "net_pts"]].to_string())
    print(eng.stats())


if __name__ == "__main__":
    main()
