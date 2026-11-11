"""Clean, INR-denominated daily data for the research universe."""
import os
import numpy as np
import pandas as pd

D = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "yahoo")

NSE_STOCKS = ["HDFCBANK", "SBIN", "RELIANCE", "INFY", "TCS", "ITC", "TMPV", "TATASTEEL", "LT",
              "BHARTIARTL", "SUNPHARMA", "VEDL"]
INDICES = ["NIFTY", "BANKNIFTY"]
COMMOD = ["GOLD", "SILVER", "CRUDE"]
CRYPTO = ["BTC"]
UNIVERSE = INDICES + NSE_STOCKS + COMMOD + CRYPTO
GROUP = {**{s: "INDEX" for s in INDICES}, **{s: "STOCK" for s in NSE_STOCKS},
         **{s: "COMMODITY" for s in COMMOD}, "BTC": "CRYPTO"}
# round-trip cost as fraction of notional (no leverage), excluding income tax
COST = {"INDEX": 0.0025, "STOCK": 0.0035, "COMMODITY": 0.0015, "CRYPTO": 0.0060}
# demergers: (date, symbol) -> back-adjust prior prices by Open_t / Close_{t-1}
CORP_ACTIONS = [("TMPV", "2025-10-14"), ("VEDL", "2026-04-30")]
START = {"BTC": "2014-09-17"}
DEFAULT_START = "2007-01-01"


def _raw(name):
    return pd.read_pickle(os.path.join(D, f"{name}_1d.pkl"))[["Open", "High", "Low", "Close"]].astype(float)


def _fix_ohlc(d):
    d = d.copy()
    d["High"] = d[["Open", "High", "Low", "Close"]].max(axis=1)
    d["Low"] = d[["Open", "High", "Low", "Close"]].min(axis=1)
    return d


def load(name):
    if name in ("GOLD", "SILVER", "CRUDE"):
        src = {"GOLD": "GLD", "SILVER": "SLV", "CRUDE": "CRUDE"}[name]
        d = _raw(src)
        fx = _raw("USDINR")["Close"]
        fx = fx[(fx > 30) & (fx < 150)]
        fx = fx.reindex(d.index.union(fx.index)).ffill().reindex(d.index)
        d = d.mul(fx, axis=0)
    elif name == "BTC":
        d = _raw("BTC")
        fx = _raw("USDINR")["Close"]
        fx = fx[(fx > 30) & (fx < 150)]
        fx = fx.reindex(d.index.union(fx.index)).ffill().reindex(d.index)
        d = d.mul(fx, axis=0)
    else:
        d = _raw(name)
    for sym, dt in CORP_ACTIONS:
        if sym == name and pd.Timestamp(dt) in d.index:
            i = d.index.get_loc(pd.Timestamp(dt))
            f = d["Open"].iloc[i] / d["Close"].iloc[i - 1]
            d.iloc[:i] = d.iloc[:i] * f
    d = d[d.index >= START.get(name, DEFAULT_START)]
    if name == "CRUDE":  # April-2020 negative-price episode makes % P&L meaningless
        d = d[(d.index < "2020-03-15") | (d.index > "2020-06-30")]
    d = d.dropna()
    d = d[(d > 0).all(axis=1)]
    return _fix_ohlc(d)
