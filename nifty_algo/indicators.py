"""Indicator primitives (Wilder smoothing matches TradingView)."""
from __future__ import annotations

import numpy as np
import pandas as pd


def rma(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


def sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()


def rsi(close: pd.Series, n: int) -> pd.Series:
    d = close.diff()
    up = rma(d.clip(lower=0.0), n)
    dn = rma((-d).clip(lower=0.0), n)
    rs = up / dn.replace(0.0, np.nan)
    out = 100.0 - 100.0 / (1.0 + rs)
    # all-up window -> RSI 100; all-down -> 0
    out = out.where(~((dn == 0) & (up > 0)), 100.0)
    return out


def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    pc = close.shift(1)
    tr = pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    return rma(tr, n)


def ibs(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    rng = (high - low).replace(0.0, np.nan)
    return ((close - low) / rng).fillna(0.5)
