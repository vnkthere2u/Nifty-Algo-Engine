"""Win-rate-only objective (>75%), long only. Two routes: (A) payoff geometry, (B) mean-reversion with signal exits.
Daily bars 2016-2024 for 86 NIFTY-100 stocks; decide at close, act at next open; cost 0.35% round trip."""
import numpy as np, pandas as pd
from multiprocessing import Pool
from numba import njit
from core import Ctx, stocks, CUT
COST = 0.0035

@njit(cache=True)
def sim(o, h, l, c, sig, xflag, sl_d, tp_d, maxhold, cat):
    n = len(c); out = np.zeros((n, 4)); k = 0; i = 0
    while i < n - 1:
        if not sig[i]: i += 1; continue
        j = i + 1; e = o[j]
        sl = e - sl_d[i] if sl_d[i] > 0 else e * (1 - cat)
        tp = e + tp_d[i] if tp_d[i] > 0 else 1e18
        xp = 0.0
        while True:
            if j >= n: j = n - 1; xp = c[j]; break
            if j > i + 1 and o[j] <= sl: xp = o[j]; break
            if j > i + 1 and o[j] >= tp: xp = o[j]; break
            if l[j] <= sl: xp = sl; break
            if h[j] >= tp: xp = tp; break
            if xflag[j] and j > i + 1 - 1:   # signal exit at close of day j (decided at close)
                xp = c[j]; break
            if maxhold > 0 and j - (i + 1) >= maxhold: xp = c[j]; break
            j += 1
        out[k, 0] = i + 1; out[k, 1] = j; out[k, 2] = e; out[k, 3] = xp; k += 1; i = j
    return out[:k]

def rsi(c, n):
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean(); dn = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))

RULES = {}
def work(sym):
    d = Ctx(sym).d.copy(); c = d.Close
    d["sma5"] = c.rolling(5).mean(); d["sma10"] = c.rolling(10).mean(); d["r2"] = rsi(c, 2)
    up = c > d.sma200; z = np.zeros(len(d)); no = np.zeros(len(d), bool)
    rows = []
    def run(name, sig, xflag, sl_d=z, tp_d=z, mh=0, cat=0.99):
        r = sim(d.Open.values, d.High.values, d.Low.values, c.values, sig.fillna(False).values.astype(np.bool_),
                xflag.fillna(False).values.astype(np.bool_), np.nan_to_num(sl_d), np.nan_to_num(tp_d), mh, cat)
        for a, b, e, x in r:
            rows.append(dict(sym=sym, rule=name, t=d.index[int(a)], days=int(b - a), ret=x / e - 1 - COST))
    every = pd.Series(True, index=d.index) & d.sma200.notna()
    A = d.atr.values
    # (A) geometry only, entry every day (no edge): target small vs stop
    for sl, tp in ((1, 1.5), (2, 1), (3, 1), (3, 0.5), (5, 0.5)):
        run(f"A_geom SL{sl}/TP{tp} ATR", every, pd.Series(no, index=d.index), sl * A, tp * A)
    # (B) mean reversion, exit on strength, no fixed target
    run("B1 RSI2<10 & >SMA200, exit C>SMA5, no stop", (d.r2 < 10) & up, c > d.sma5)
    run("B2 RSI2<5 & >SMA200, exit C>SMA5, no stop", (d.r2 < 5) & up, c > d.sma5)
    run("B3 RSI2<10 & >SMA200, exit C>prevHigh", (d.r2 < 10) & up, c > d.High.shift())
    run("B4 RSI2<10 & >SMA200, exit C>SMA5, stop 3ATR", (d.r2 < 10) & up, c > d.sma5, 3 * A)
    run("B5 RSI2<10 & >SMA200, exit C>SMA5, max 10d", (d.r2 < 10) & up, c > d.sma5, mh=10)
    run("B6 3 down closes & >SMA200, exit C>SMA5", (c < c.shift()) & (c.shift() < c.shift(2)) & (c.shift(2) < c.shift(3)) & up, c > d.sma5)
    return rows

if __name__ == "__main__":
    with Pool(4) as p:
        T = pd.DataFrame([r for rows in p.map(work, stocks()) for r in rows])
    T["win"] = T.ret > 0; T["per"] = np.where(T.t < CUT, "IS", "OOS")
    def agg(g):
        w = g[g.win]; l = g[~g.win]; yrs = 8.85
        return pd.Series(dict(trades=len(g), per_stock_yr=round(len(g) / 86 / yrs, 1), win=round(100 * g.win.mean(), 1),
            avg_win=round(100 * w.ret.mean(), 2), avg_loss=round(100 * l.ret.mean(), 2),
            payoff=round(w.ret.mean() / -l.ret.mean(), 2), pf=round(w.ret.sum() / -l.ret.sum(), 2),
            exp=round(100 * g.ret.mean(), 2), worst=round(100 * g.ret.min(), 1), days=round(g.days.mean(), 1),
            inr_yr_per_stock=round(g.ret.sum() * 1e5 / 86 / yrs)))
    pd.set_option("display.width", 250)
    print(T.groupby("rule").apply(agg).to_string())
    print("\nIS vs OOS win% / PF:")
    x = T.groupby(["rule", "per"]).apply(lambda g: pd.Series(dict(win=round(100 * g.win.mean(), 1), pf=round(g.ret[g.win].sum() / -g.ret[~g.win].sum(), 2)))).unstack()
    print(x.to_string())
    T.to_pickle("../data/r3_wr75.pkl")
