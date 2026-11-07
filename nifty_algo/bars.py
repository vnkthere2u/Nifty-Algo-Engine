"""Bar construction shared by backtest and live engine."""
from __future__ import annotations

import pandas as pd

FEATURE_WINDOW = 250  # daily rows fed to compute_features (identical in backtest and live)


def daily_snapshot(m1: pd.DataFrame, cutoff_min: int) -> pd.DataFrame:
    """Aggregate 1-minute bars into one row per day using minutes that START before cutoff_min
    (IST minutes after midnight).  cutoff 15:25 -> the 15:24 bar is the last one, i.e. the price
    known a few seconds after 15:25:00 when the live decision runs."""
    mins = m1.index.hour * 60 + m1.index.minute
    x = m1[(mins >= 9 * 60 + 15) & (mins < cutoff_min)]
    agg = x.groupby(x.index.normalize()).agg(
        Open=("Open", "first"), High=("High", "max"), Low=("Low", "min"), Close=("Close", "last"),
        last_ts=("Close", lambda s: s.index[-1]))
    agg.index.name = "date"
    return agg
