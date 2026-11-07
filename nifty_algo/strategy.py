"""TPMR strategy logic - pure functions, shared verbatim by the backtester and the live engine.

Daily frame convention: one row per trading day, OHLC measured from the open up to the
decision snapshot (15:25 IST).  The live engine appends today's partial row built from
intraday data, so live and backtest see exactly the same numbers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from .config import StrategyParams
from .indicators import atr, ibs, rsi, sma

MIN_HISTORY = 60  # rows needed before the first valid signal


def compute_features(daily: pd.DataFrame, p: StrategyParams) -> pd.DataFrame:
    """daily: index = date, columns Open/High/Low/Close. Returns a copy with features."""
    d = daily[["Open", "High", "Low", "Close"]].astype(float).copy()
    d["rsi"] = rsi(d["Close"], p.rsi_len)
    d["sma_trend"] = sma(d["Close"], p.trend_sma)
    d["sma5"] = sma(d["Close"], 5)
    d["atr"] = atr(d["High"], d["Low"], d["Close"], p.atr_len)
    d["ibs"] = ibs(d["High"], d["Low"], d["Close"])
    d["prev_high"] = d["High"].shift(1)
    d["trend_dist_atr"] = (d["Close"] - d["sma_trend"]) / d["atr"]
    return d


@dataclass
class EntryDecision:
    take: bool
    setups: list = field(default_factory=list)
    reasons: list = field(default_factory=list)
    features: dict = field(default_factory=dict)

    @property
    def setup_key(self) -> str:
        if not self.setups:
            return "NONE"
        return "+".join(sorted(self.setups))


def _features_dict(row: pd.Series) -> dict:
    keys = ["Open", "High", "Low", "Close", "rsi", "sma_trend", "sma5", "atr", "ibs", "prev_high", "trend_dist_atr"]
    return {k: (None if pd.isna(row.get(k)) else round(float(row.get(k)), 4)) for k in keys}


def evaluate_entry(row: pd.Series, p: StrategyParams, history_len: int) -> EntryDecision:
    feats = _features_dict(row)
    reasons = []
    if history_len < MIN_HISTORY or pd.isna(row["sma_trend"]) or pd.isna(row["atr"]):
        return EntryDecision(False, [], ["insufficient history"], feats)
    if row["Close"] <= row["sma_trend"]:
        reasons.append(f"regime: close {row['Close']:.2f} <= SMA{p.trend_sma} {row['sma_trend']:.2f}")
    setups = []
    if row["rsi"] < p.rsi_entry:
        setups.append("RSI")
    if row["ibs"] < p.ibs_entry:
        setups.append("IBS")
    if not setups:
        reasons.append(f"no setup: RSI{p.rsi_len}={row['rsi']:.1f} (need <{p.rsi_entry}), "
                       f"IBS={row['ibs']:.2f} (need <{p.ibs_entry})")
    take = not reasons
    return EntryDecision(take, setups, reasons if not take else [f"setup {'+'.join(setups)} in uptrend"], feats)


@dataclass
class PositionState:
    entry_price: float
    entry_atr: float
    entry_date: object
    days_held: int  # completed decision days since entry (0 on entry day)


def disaster_stop_price(entry_price: float, entry_atr: float, p: StrategyParams) -> Optional[float]:
    if p.disaster_stop_atr <= 0:
        return None
    return entry_price - p.disaster_stop_atr * entry_atr


def evaluate_exit(row: pd.Series, pos: PositionState, p: StrategyParams) -> Optional[str]:
    """Called once per decision snapshot on days AFTER the entry day."""
    c = float(row["Close"])
    if p.exit_above_prev_high and not pd.isna(row["prev_high"]) and c > row["prev_high"]:
        return "TARGET: close > prev high"
    if p.close_stop_atr > 0 and c <= pos.entry_price - p.close_stop_atr * pos.entry_atr:
        return "STOP: closing-basis stop"
    if p.max_hold_days > 0 and pos.days_held >= p.max_hold_days:
        return f"TIME: held {pos.days_held} days"
    return None
