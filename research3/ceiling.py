"""What is the best achievable win rate for SL=1ATR / TP=1.5ATR given observable conditions?
Systematic entries (every day; intraday at 09:30/11:00/13:30 with 15m ATR; swing at each close with daily ATR),
tagged with features known at entry; 2-feature bucket search on IS, verified OOS."""
import itertools, numpy as np, pandas as pd
from multiprocessing import Pool
from core import Ctx, simulate, stocks, CUT, load5
from strategies import _frame, _bar_at

def mkt():
    n5 = load5("NIFTY_50"); nd = n5.resample("1D").agg({"Close": "last"}).dropna()
    nd["n_up200"] = (nd.Close > nd.Close.rolling(200).mean()).astype(int); nd["n_ret1"] = nd.Close.pct_change()
    vx = load5("INDIA_VIX").resample("1D").agg({"Close": "last"}).dropna(); nd["vix"] = vx.Close.reindex(nd.index).ffill()
    return nd, n5

def work(sym):
    nd, n5 = mkt(); ctx = Ctx(sym); d = ctx.d; out = []
    prev = d.shift(1)
    feats_day = pd.DataFrame({
        "trend": ((prev.Close > prev.ema20).astype(int) + (prev.ema20 > prev.ema50).astype(int) + (prev.Close > prev.sma200).astype(int)),
        "rsi2": pd.cut(prev.rsi2, [-1, 10, 30, 70, 90, 101], labels=False),
        "gap": pd.cut(d.Open / prev.Close - 1, [-1, -0.01, -0.003, 0.003, 0.01, 1], labels=False),
        "atrp": pd.qcut((prev.atr / prev.Close).rank(pct=True), 3, labels=False),
        "d52": pd.cut(prev.Close / prev.hh252, [0, 0.8, 0.9, 0.97, 2], labels=False),
        "n_up": nd.n_up200.shift(1).reindex(d.index).values,
        "n_r1": pd.cut(nd.n_ret1.shift(1).reindex(d.index), [-1, -0.01, 0, 0.01, 1], labels=False).values,
        "vix": pd.cut(nd.vix.shift(1).reindex(d.index), [0, 13, 17, 22, 100], labels=False).values,
    }, index=d.index)
    nfo = n5.Open.groupby(n5.index.normalize()).transform("first")
    for tm in (555 + 15, 660, 810):   # 09:30, 11:00, 13:30 bar closes
        days = np.arange(len(ctx.days)); bars = _bar_at(ctx, days, tm - 5)
        ok = ctx.mins[bars] == tm - 5; bars, days = bars[ok], days[ok]
        dop = pd.Series(ctx.b.Open.values).groupby(ctx.day_id).transform("first").values
        ret_open = ctx.b.Close.values[bars] / dop[bars] - 1
        nifty_now = n5.Close.reindex(ctx.t[bars]).values; nifty_open = nfo.reindex(ctx.t[bars]).values
        rs = ret_open - (nifty_now / nifty_open - 1)
        s = _frame(bars, 0, ctx.b.Close.values[bars], bars, ctx.a15.atr.values[bars], True)
        s["ro"] = pd.cut(ret_open, [-1, -0.01, -0.003, 0.003, 0.01, 1], labels=False)
        s["rs"] = pd.cut(rs, [-1, -0.005, 0, 0.005, 1], labels=False)
        s["vw"] = (ctx.b.Close.values[bars] > ctx.vwap[bars]).astype(int)
        t = simulate(ctx, s.iloc[:, :6].assign(), be_on=False)
        t["bar"] = ctx.idx_at(t.entry_time) - 1
        t = t.merge(s[["bar", "ro", "rs", "vw"]], on="bar", how="left")
        fd = feats_day.reindex(t.entry_time.dt.normalize()).reset_index(drop=True)
        t = pd.concat([t.reset_index(drop=True), fd], axis=1); t["mode"] = f"I{tm}"; out.append(t)
    # swing: every day close, buy next open (level 0 => next-bar open), daily ATR
    days = np.arange(len(ctx.days) - 1); bars = _bar_at(ctx, days, 1000)
    s = _frame(bars, 1, np.zeros(len(bars)), _bar_at(ctx, days + 1, 1000), d.atr.reindex(ctx.days).values[days], False)
    t = simulate(ctx, s, be_on=False)
    fd = feats_day.shift(-1).reindex(t.entry_time.dt.normalize() - pd.Timedelta(days=0)).reset_index(drop=True)
    sd = feats_day.reindex(t.entry_time.dt.normalize()).reset_index(drop=True)
    t = pd.concat([t.reset_index(drop=True), sd], axis=1); t["mode"] = "SWING"; out.append(t)
    r = pd.concat(out); r["sym"] = sym
    return r

if __name__ == "__main__":
    with Pool(4) as p:
        A = pd.concat(p.map(work, stocks()))
    A["win"] = (A.reason == "TP").astype(int); A["is"] = A.entry_time < CUT
    A.to_pickle("../data/r3_ceiling.pkl")
    F = ["trend", "rsi2", "gap", "atrp", "d52", "n_up", "n_r1", "vix", "ro", "rs", "vw"]
    print("base TP-hit rate by mode:\n", A.groupby(["mode", "is"]).win.mean().unstack().round(3))
    res = []
    for mode, M in A.groupby("mode"):
        fs = [f for f in F if M[f].notna().any()]
        for f1, f2 in itertools.combinations(fs, 2):
            g = M.groupby([f1, f2, "is"]).agg(n=("win", "size"), w=("win", "mean"), e=("ret", "mean")).unstack("is")
            g.columns = [f"{a}_{'IS' if b else 'OOS'}" for a, b in g.columns]
            g = g[(g.n_IS >= 300) & (g.n_OOS >= 100)]
            for k, row in g.iterrows():
                res.append(dict(mode=mode, cond=f"{f1}={k[0]:.0f} & {f2}={k[1]:.0f}", **row.to_dict()))
    R = pd.DataFrame(res).sort_values("w_IS", ascending=False)
    pd.set_option("display.width", 250)
    for mode in R["mode"].unique():
        print(f"\n== {mode}: top-5 IS buckets (verified OOS) ==")
        print(R[R["mode"] == mode].head(5).round(3).to_string(index=False))
    print("\nMAX OOS TP-hit among top-20 IS buckets per mode:", R.groupby("mode").head(20).groupby("mode").w_OOS.max().round(3).to_dict())
