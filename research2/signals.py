"""Feature + setup library (all computed on completed daily bars; no look-ahead)."""
import numpy as np, pandas as pd

def rma(s, n): return s.ewm(alpha=1 / n, adjust=False).mean()

def features(d):
    f = d.copy()
    c, h, l = f.Close, f.High, f.Low
    pc = c.shift()
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    f["atr"] = rma(tr, 14)
    for n in (20, 50, 200):
        f[f"sma{n}"] = c.rolling(n).mean(); f[f"ema{n}"] = c.ewm(span=n, adjust=False).mean()
    dlt = c.diff(); up = rma(dlt.clip(lower=0), 2); dn = rma(-dlt.clip(upper=0), 2)
    f["rsi2"] = 100 - 100 / (1 + up / dn.replace(0, np.nan)); f["rsi2"] = f.rsi2.fillna(100)
    for n in (20, 55): f[f"hh{n}"] = h.rolling(n).max()           # includes today
    f["hh252"] = h.rolling(252).max()
    f["ll10"] = l.rolling(10).min()
    sd = c.rolling(20).std(); f["bbu"] = f.sma20 + 2 * sd; f["bbw"] = 4 * sd / f.sma20
    f["bbw_pct"] = f.bbw.rolling(120).rank(pct=True)
    rng = h - l
    f["nr7"] = rng <= rng.rolling(7).min()
    f["inside"] = (h <= h.shift()) & (l >= l.shift())
    f["dn3"] = (c < pc) & (pc < pc.shift()) & (pc.shift() < pc.shift(2))
    return f

TRENDS = {
    "T1": lambda f: f.Close > f.sma200,
    "T2": lambda f: (f.sma50 > f.sma200) & (f.Close > f.sma50),
    "T3": lambda f: (f.ema20 > f.ema50) & (f.ema50 > f.ema200),
}

# name -> (signal fn, entry_type, trigger fn)
SETUPS = {
    "PB_RSI5":  (lambda f: f.rsi2 < 5, 0, None),
    "PB_RSI10": (lambda f: f.rsi2 < 10, 0, None),
    "PB_RSI20": (lambda f: f.rsi2 < 20, 0, None),
    "PB_EMA20": (lambda f: (f.Low <= f.ema20) & (f.Close > f.ema20), 0, None),
    "PB_DN3":   (lambda f: f.dn3, 0, None),
    "BO_C20":   (lambda f: f.Close > f.hh20.shift(), 0, None),
    "BO_C55":   (lambda f: f.Close > f.hh55.shift(), 0, None),
    "BO_STOP20": (lambda f: f.Close < f.hh20, 1, lambda f: f.hh20 * 1.001),
    "BO_STOP55": (lambda f: f.Close < f.hh55, 1, lambda f: f.hh55 * 1.001),
    "SQZ":      (lambda f: (f.bbw_pct.rolling(5).min() < 0.2) & (f.Close > f.bbu), 0, None),
    "NR7_IB":   (lambda f: f.nr7 & f.inside, 1, lambda f: f.High * 1.001),
    "MOM52_PB": (lambda f: (f.Close >= 0.93 * f.hh252) & (f.rsi2 < 20), 0, None),
}

STOPS = {
    "A1.5": lambda f: 1.5 * f.atr,
    "A2.5": lambda f: 2.5 * f.atr,
    "A3.5": lambda f: 3.5 * f.atr,
    "SWING": lambda f: (f.Close - f.ll10 + 0.2 * f.atr).clip(lower=0.5 * f.atr),
}
TRAILS = {"none": (0.0, 1.0, 0.0), "BE1": (1.0, 1.0, 0.0), "BE1+CH3": (1.0, 1.5, 3.0)}
