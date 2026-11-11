import itertools, numpy as np, pandas as pd
from engine_fast import Market, rsi, atr, split_stats, stats
mk = Market(); m1 = mk.m1
snap = m1[(m1.index.hour * 60 + m1.index.minute) <= 925]
d = snap.resample("1D").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()
d["t"] = d.index + pd.Timedelta(hours=15, minutes=25)
d["t_next_open"] = d.index.to_series().shift(-1).values + np.timedelta64(555, "m")
d["rsi2"] = rsi(d.Close, 2)
for n in (5, 10, 50): d[f"sma{n}"] = d.Close.rolling(n).mean()
d["atr"] = atr(d.High, d.Low, d.Close, 14)
d["ibs"] = (d.Close - d.Low) / (d.High - d.Low).replace(0, np.nan)
d["prevH"] = d.High.shift()
d = d.iloc[60:].dropna(subset=["sma50"]).copy()
up = d.Close > d.sma50
cfgs = {
 "U1: (RSI2<10 | IBS<0.15) >SMA50, exit C>SMA5": (((d.rsi2 < 10) | (d.ibs < .15)) & up, d.Close > d.sma5),
 "U2: (RSI2<10 | IBS<0.15) >SMA50, exit C>prevH": (((d.rsi2 < 10) | (d.ibs < .15)) & up, d.Close > d.prevH),
 "U3: (RSI2<15 | IBS<0.15) >SMA50, exit C>prevH|C>SMA5": (((d.rsi2 < 15) | (d.ibs < .15)) & up, (d.Close > d.prevH) | (d.Close > d.sma5)),
 "U4: (RSI2<10 | IBS<0.15) >SMA50, exit C>prevH|C>SMA5": (((d.rsi2 < 10) | (d.ibs < .15)) & up, (d.Close > d.prevH) | (d.Close > d.sma5)),
 "R1: RSI2<10 >SMA50 exit C>SMA5": ((d.rsi2 < 10) & up, d.Close > d.sma5),
}
def go(m, ex, slm=0, mh=0, cost=0.0004, delay=False):
    s = d[m]
    tt = s.t_next_open.values if delay else s.t.values
    fl = mk.minute_flags((d.t_next_open.values if delay else d.t.values), ex.values)
    ok = ~pd.isna(tt)
    return mk.run(tt[ok], np.ones(ok.sum(), int), slm * s.atr.values[ok], np.zeros(ok.sum()), xl=fl,
                  max_hold=mh * 375, allow_overnight=True, cost_pct=cost)
pd.set_option("display.width", 250)
rows = []
for name, (m, ex) in cfgs.items():
    for slm, mh, cost, delay in [(0,0,.0004,False),(4,10,.0004,False),(3,7,.0004,False),(0,0,.0008,False),(0,0,.0004,True)]:
        tr = go(m, ex, slm, mh, cost, delay); a, o = split_stats(tr); f = stats(tr)
        rows.append(dict(cfg=name[:3], sl=slm, mh=mh, cost=cost, delay=delay, n=f["n"], yr=f["per_yr"], win=f["win"], pf=f["pf"],
                         avg=f["avg_pts"], tot=f["tot_pts"], dd=f["maxdd_pts"], is_win=a["win"], oos_win=o["win"], is_pf=a["pf"], oos_pf=o["pf"]))
print(pd.DataFrame(rows).to_string())
tr = go(*cfgs["U4: (RSI2<10 | IBS<0.15) >SMA50, exit C>prevH|C>SMA5"], 4, 10)
tr["yr"] = pd.to_datetime(tr.entry_time).dt.year
tr["days"] = (pd.to_datetime(tr.exit_time) - pd.to_datetime(tr.entry_time)).dt.days
print(tr.groupby("yr").agg(n=("net","size"), win=("net", lambda x: round((x>0).mean()*100,1)), pts=("net","sum"), worst=("net","min")).to_string())
print("reasons", tr.reason.value_counts().to_dict(), "avg hold days", tr.days.mean().round(2))
print("avg win", tr.net[tr.net>0].mean().round(1), "avg loss", tr.net[tr.net<=0].mean().round(1))
