"""Same ceiling test with WEEKLY ATR geometry (SL=1 wATR, TP=1.5 wATR), entries at each week's last close."""
import itertools, numpy as np, pandas as pd
from multiprocessing import Pool
from core import Ctx, simulate, stocks, CUT, load5, atr, rsi
from strategies import _frame, _bar_at

def work(sym):
    ctx = Ctx(sym); d = ctx.d
    w = d.resample("W-FRI").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna()
    w["atr"] = atr(w); w["ema10"] = w.Close.ewm(span=10, adjust=False).mean(); w["ema30"] = w.Close.ewm(span=30, adjust=False).mean()
    w["rsi2"] = rsi(w.Close); w["hh52"] = w.High.rolling(52).max()
    w["last_day"] = d.Close.resample("W-FRI").apply(lambda s: s.index[-1] if len(s) else pd.NaT)
    w = w.dropna(subset=["last_day"])
    di = np.searchsorted(ctx.days.values, w.last_day.values.astype("datetime64[ns]"))
    ok = di + 1 < len(ctx.days); w, di = w[ok], di[ok]
    bars = _bar_at(ctx, di, 1000)
    s = _frame(bars, 1, np.zeros(len(bars)), _bar_at(ctx, di + 1, 1000), w.atr.values, False)
    f = pd.DataFrame({"bar": bars,
        "wtrend": ((w.Close > w.ema10).astype(int) + (w.ema10 > w.ema30).astype(int)).values,
        "wrsi": pd.cut(w.rsi2, [-1, 10, 30, 70, 90, 101], labels=False).values,
        "w52": pd.cut(w.Close / w.hh52, [0, 0.8, 0.9, 0.97, 2], labels=False).values,
        "wret": pd.cut(w.Close.pct_change(), [-1, -0.03, 0, 0.03, 1], labels=False).values})
    t = simulate(ctx, s, be_on=False)
    t["bar"] = ctx.idx_at(t.entry_time)  # entry bar; map back via day
    t["wk"] = t.entry_time.dt.normalize()
    f["wk"] = ctx.days[np.minimum(di + 1, len(ctx.days) - 1)]
    t = t.merge(f.drop(columns="bar"), on="wk", how="left"); t["sym"] = sym
    t["risk_pct"] = t.atr / t.entry
    return t

if __name__ == "__main__":
    with Pool(4) as p:
        A = pd.concat(p.map(work, stocks()))
    A["win"] = (A.reason == "TP").astype(int); A["is"] = A.entry_time < CUT
    A.to_pickle("../data/r3_ceiling_weekly.pkl")
    print("trades", len(A), " median stop % of price:", round(100 * A.risk_pct.median(), 1),
          " skipped if >10%: handled in sim; median days held:", (A.exit_time - A.entry_time).dt.days.median())
    print("base TP-hit IS/OOS:", A.groupby("is").win.mean().round(3).to_dict(), " exp% IS/OOS:", (100 * A.groupby("is").ret.mean()).round(2).to_dict())
    res = []
    for f1, f2 in itertools.combinations(["wtrend", "wrsi", "w52", "wret"], 2):
        g = A.groupby([f1, f2, "is"]).agg(n=("win", "size"), w=("win", "mean"), e=("ret", "mean")).unstack("is")
        g.columns = [f"{a}_{'IS' if b else 'OOS'}" for a, b in g.columns]
        g = g[(g.n_IS >= 300) & (g.n_OOS >= 100)]
        for k, row in g.iterrows(): res.append(dict(cond=f"{f1}={k[0]:.0f} & {f2}={k[1]:.0f}", **row.to_dict()))
    R = pd.DataFrame(res).sort_values("w_IS", ascending=False)
    pd.set_option("display.width", 250); print(R.head(10).round(3).to_string(index=False))
    for f1 in ["wtrend", "wrsi", "w52", "wret"]:
        print(f1, A.groupby([f1, "is"]).win.mean().unstack().round(3).to_dict())
