"""Grid-search tweaks of the v3.3 EMA5/EMA39 crossover family. IS=2017-2022, OOS=2023+."""
import itertools, numpy as np, pandas as pd
from engine_fast import Market, ema, atr, adx, split_stats, stats

mk = Market()
b = mk.b15.copy()
b["e5"], b["e39"] = ema(b.Close, 5), ema(b.Close, 39)
b["atr"] = atr(b.High, b.Low, b.Close)
b["adx"], _, _ = adx(b.High, b.Low, b.Close)
h = mk.m1.resample("60min", origin="start_day", offset="15min").agg({"Close": "last"}).dropna()
h["he"] = ema(h.Close, 39)
# last COMPLETED hourly bar visible at 15m bar end
b["end"] = b.index + pd.Timedelta(minutes=15)
hh = h.copy(); hh["hend"] = hh.index + pd.Timedelta(minutes=60)
b = pd.merge_asof(b.sort_values("end"), hh[["hend", "he"]].sort_values("hend"), left_on="end", right_on="hend")
b.index = b["end"] - pd.Timedelta(minutes=15)
up = (b.e5 > b.e39) & (b.e5.shift() <= b.e39.shift())
dn = (b.e5 < b.e39) & (b.e5.shift() >= b.e39.shift())
b["sig"] = np.where(up, 1, np.where(dn, -1, 0))
b["adx_up"] = b.adx >= b.adx.shift() - 1
b["sep"] = (b.e5 - b.e39).abs() / b.atr
b["ext"] = (b.Close - b.e39).abs() / b.atr
b["htf_ok"] = np.where(b.sig == 1, b.Close > b.he, b.Close < b.he)
b["tmin"] = b.index.hour * 60 + b.index.minute
b = b.iloc[200:]
xl = mk.minute_flags(b.end.values, (b.e5 < b.e39).values)   # opposite regime -> exit long
xs = mk.minute_flags(b.end.values, (b.e5 > b.e39).values)
S = b[b.sig != 0]
rows = []
grid = itertools.product([0, 20, 25, 30], [False, True], [False, True], [0, .15], [2.5, 99],
                         [False, True],
                         [(1.5, 3.75), (1.5, 2.25), (1.5, 1.5), (1.5, 1.0), (2, 1), (2.5, 1), (1, 1), (1, 2), (3, 1)],
                         [0, .5], [False, True], [False, True])
for amin, arise, htf, sepm, extm, tw, (slm, tpm), be, ovn, sx in grid:
    m = (S.adx > amin) & (S.sep >= sepm) & (S.ext <= extm)
    if arise: m &= S.adx_up
    if htf: m &= S.htf_ok
    if tw: m &= (S.tmin >= 570) & (S.tmin <= 855)
    s = S[m]
    if len(s) < 60: continue
    tr = mk.run(s.end.values, s.sig.values, slm * s.atr.values, tpm * s.atr.values,
                xl=xl if sx else None, xs=xs if sx else None, be_trig=be, be_off=.1,
                allow_overnight=ovn)
    a, o = split_stats(tr)
    rows.append(dict(adx=amin, rise=arise, htf=htf, sep=sepm, ext=extm, tw=tw, sl=slm, tp=tpm, be=be,
                     ovn=ovn, sx=sx, is_n=a["n"], is_win=a["win"], is_pf=a["pf"], is_pts=a["tot_pts"],
                     oos_n=o.get("n", 0), oos_win=o.get("win"), oos_pf=o.get("pf"), oos_pts=o.get("tot_pts")))
R = pd.DataFrame(rows)
R.to_csv("../data/grid_ema_cross.csv", index=False)
print("combos:", len(R))
q = R[(R.is_pf > 1.15) & (R.is_n >= 100)].sort_values("is_win", ascending=False)
print("\nTop by IS win-rate (IS PF>1.15):"); print(q.head(15).to_string())
print("\nTop by IS PF:"); print(R[R.is_n >= 100].sort_values("is_pf", ascending=False).head(10).to_string())
print("\nBest OOS win among IS PF>1.0:", R[(R.is_pf>1.0)].oos_win.max())
