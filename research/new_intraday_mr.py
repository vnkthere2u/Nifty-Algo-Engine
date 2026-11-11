"""Intraday 15m pullback-in-trend mean reversion. Flat by 15:20. IS 2017-22 / OOS 2023+."""
import itertools, numpy as np, pandas as pd
from engine_fast import Market, rsi, atr, ema, split_stats

mk = Market()
b = mk.b15.copy()
b["end"] = b.index + pd.Timedelta(minutes=15)
b["atr"] = atr(b.High, b.Low, b.Close)
for n in (2, 3): b[f"rsi{n}"] = rsi(b.Close, n)
b["e5"] = ema(b.Close, 5); b["e20"] = ema(b.Close, 20); b["e200"] = ema(b.Close, 200)
b["tmin"] = b.index.hour * 60 + b.index.minute
# daily regime known before today's open: prior-day close vs prior-day SMA50/20
dly = mk.daily.copy()
dly["up50"] = dly.Close > dly.Close.rolling(50).mean()
dly["up20"] = dly.Close > dly.Close.rolling(20).mean()
reg = dly[["up50", "up20"]].shift(1)          # yesterday's state
b["day"] = b.index.normalize()
b = b.join(reg, on="day")
b = b.iloc[300:].dropna(subset=["up50"])
rows = []
grid = itertools.product((2, 3), (5, 10, 15), ("none", "e200", "d50", "d20", "e200+d50"),
                         ("e5", "rsi50", "rsi70"), (0, 1.5, 2.5), (0, 8, 16), (1, -1))
for rn, thr, regime, exitk, slm, mhb, side in grid:
    if side == 1:
        m = b[f"rsi{rn}"] < thr
        if "e200" in regime: m &= b.Close > b.e200
        if "d50" in regime: m &= b.up50.astype(bool)
        if regime == "d20": m &= b.up20.astype(bool)
    else:
        m = b[f"rsi{rn}"] > 100 - thr
        if "e200" in regime: m &= b.Close < b.e200
        if "d50" in regime: m &= ~b.up50.astype(bool)
        if regime == "d20": m &= ~b.up20.astype(bool)
    m &= (b.tmin >= 570) & (b.tmin <= 870)
    s = b[m]
    if len(s) < 100: continue
    if exitk == "e5": ex = (b.Close > b.e5) if side == 1 else (b.Close < b.e5)
    elif exitk == "rsi50": ex = (b[f"rsi{rn}"] > 50) if side == 1 else (b[f"rsi{rn}"] < 50)
    else: ex = (b[f"rsi{rn}"] > 70) if side == 1 else (b[f"rsi{rn}"] < 30)
    fl = mk.minute_flags(b.end.values, ex.values)
    tr = mk.run(s.end.values, np.full(len(s), side), slm * s.atr.values, np.zeros(len(s)),
                xl=fl if side == 1 else None, xs=fl if side == -1 else None,
                max_hold=mhb * 15, allow_overnight=False, eod_min=920)
    a, o = split_stats(tr)
    rows.append(dict(side=side, rsi=rn, thr=thr, regime=regime, exit=exitk, sl=slm, mhb=mhb,
                     is_n=a["n"], is_win=a["win"], is_pf=a["pf"], is_pts=a["tot_pts"], is_avg=a["avg_pts"],
                     oos_n=o.get("n", 0), oos_win=o.get("win"), oos_pf=o.get("pf"), oos_pts=o.get("tot_pts"), oos_avg=o.get("avg_pts")))
R = pd.DataFrame(rows); R.to_csv("../data/grid_intraday_mr.csv", index=False)
print("combos", len(R))
for side in (1, -1):
    q = R[(R.side == side) & (R.is_pf > 1.1)]
    print(f"\n=== side {side}: top IS win (PF>1.1) ===")
    print(q.sort_values(["is_win"], ascending=False).head(12).to_string())
    print(f"--- side {side}: top IS PF ---")
    print(R[R.side == side].sort_values("is_pf", ascending=False).head(6).to_string())
