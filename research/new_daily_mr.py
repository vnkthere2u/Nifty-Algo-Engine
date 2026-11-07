"""Daily trend-filtered mean reversion on NIFTY. Signal uses price at 15:25 (executable live),
fill at 15:26 open; exits evaluated each day at 15:25."""
import itertools, numpy as np, pandas as pd
from engine_fast import Market, rsi, atr, split_stats

mk = Market()
m1 = mk.m1
snap = m1[(m1.index.hour * 60 + m1.index.minute) <= 925]           # day state as of 15:25
d = snap.resample("1D").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()
d["t_sig"] = d.index + pd.Timedelta(hours=15, minutes=26)
for n in (2, 3, 4): d[f"rsi{n}"] = rsi(d.Close, n)
for n in (5, 10): d[f"sma{n}"] = d.Close.rolling(n).mean()
for n in (50, 100, 200): d[f"sma{n}"] = d.Close.rolling(n).mean()
d["atr"] = atr(d.High, d.Low, d.Close, 14)
d["dn3"] = (d.Close < d.Close.shift()).rolling(3).sum() == 3
d = d.iloc[200:]
rows = []
for rn, thr, trend, exitma, slm, mh in itertools.product((2, 3, 4), (5, 10, 15, 20, 25), (0, 50, 100, 200),
                                                        (5, 10), (0, 2.0, 3.0), (0, 5, 10)):
    for side in (1, -1):
        if side == 1:
            m = d[f"rsi{rn}"] < thr
            if trend: m &= d.Close > d[f"sma{trend}"]
            ex = d.Close > d[f"sma{exitma}"]
        else:
            m = d[f"rsi{rn}"] > 100 - thr
            if trend: m &= d.Close < d[f"sma{trend}"]
            ex = d.Close < d[f"sma{exitma}"]
        s = d[m]
        if len(s) < 25: continue
        flags = mk.minute_flags(d.t_sig.values - np.timedelta64(1, "m"), ex.values)  # exit at 15:26 open
        tr = mk.run(s.t_sig.values - np.timedelta64(1, "m"), np.full(len(s), side),
                    slm * s.atr.values, np.zeros(len(s)),
                    xl=flags if side == 1 else None, xs=flags if side == -1 else None,
                    max_hold=mh * 375, allow_overnight=True)
        a, o = split_stats(tr)
        rows.append(dict(side=side, rsi=rn, thr=thr, trend=trend, exitma=exitma, sl=slm, mh=mh,
                         is_n=a["n"], is_win=a["win"], is_pf=a["pf"], is_pts=a["tot_pts"], is_dd=a["maxdd_pts"],
                         oos_n=o.get("n", 0), oos_win=o.get("win"), oos_pf=o.get("pf"), oos_pts=o.get("tot_pts"), oos_dd=o.get("maxdd_pts")))
R = pd.DataFrame(rows); R.to_csv("../data/grid_daily_mr.csv", index=False)
for side in (1, -1):
    q = R[(R.side == side) & (R.is_n >= 30)]
    print(f"\n=== side {side}: top IS by win (PF>1.3) ===")
    print(q[q.is_pf > 1.3].sort_values(["is_win", "is_pf"], ascending=False).head(12).to_string())
