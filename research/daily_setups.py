import itertools, numpy as np, pandas as pd
from engine_fast import Market, rsi, atr, split_stats, stats

mk = Market(); m1 = mk.m1
snap = m1[(m1.index.hour * 60 + m1.index.minute) <= 925]
d = snap.resample("1D").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()
d["t"] = d.index + pd.Timedelta(hours=15, minutes=25)
d["rsi2"] = rsi(d.Close, 2); d["rsi3"] = rsi(d.Close, 3)
for n in (5, 10, 20, 50, 100, 200): d[f"sma{n}"] = d.Close.rolling(n).mean()
d["atr"] = atr(d.High, d.Low, d.Close, 14)
d["ibs"] = (d.Close - d.Low) / (d.High - d.Low).replace(0, np.nan)
d["dn"] = (d.Close < d.Close.shift()).astype(int)
d["dn_run"] = d.dn.groupby((d.dn == 0).cumsum()).cumsum()
sd = d.Close.rolling(20).std(); d["bbl"] = d.sma20 - 2 * sd
d["prevH"] = d.High.shift()
d = d.iloc[200:].copy()
up50, up200 = d.Close > d.sma50, d.Close > d.sma200
setups = {
  "RSI2<10 & >SMA50": (d.rsi2 < 10) & up50,
  "RSI2<15 & >SMA50": (d.rsi2 < 15) & up50,
  "RSI2<10 & >SMA200": (d.rsi2 < 10) & up200,
  "RSI3<20 & >SMA50": (d.rsi3 < 20) & up50,
  "IBS<0.2 & <SMA5 & >SMA200": (d.ibs < .2) & (d.Close < d.sma5) & up200,
  "IBS<0.15 & >SMA50": (d.ibs < .15) & up50,
  "3 down closes & >SMA200": (d.dn_run >= 3) & up200,
  "3 down closes & >SMA50": (d.dn_run >= 3) & up50,
  "Close<BB(20,2) & >SMA200": (d.Close < d.bbl) & up200,
  "Close<SMA10-1ATR & >SMA200": (d.Close < d.sma10 - d.atr) & up200,
}
setups["UNION A (RSI2<10|3dn|IBS) >SMA200"] = ((d.rsi2 < 10) | (d.dn_run >= 3) | ((d.ibs < .2) & (d.Close < d.sma5))) & up200
setups["UNION B (RSI2<15 | 3dn&RSI2<25) >SMA50"] = ((d.rsi2 < 15) | ((d.dn_run >= 3) & (d.rsi2 < 25))) & up50
exits = {"C>SMA5": d.Close > d.sma5, "C>prevH": d.Close > d.prevH, "RSI2>70": d.rsi2 > 70,
         "C>SMA10": d.Close > d.sma10}
rows = []
for (sn, m), (xn, ex), (slm, mh) in itertools.product(setups.items(), exits.items(), [(0, 0), (4, 10), (3, 10)]):
    s = d[m]
    fl = mk.minute_flags(d.t.values, ex.values)
    tr = mk.run(s.t.values, np.ones(len(s), int), slm * s.atr.values, np.zeros(len(s)), xl=fl,
                max_hold=mh * 375, allow_overnight=True)
    a, o = split_stats(tr); f = stats(tr)
    rows.append(dict(setup=sn, exit=xn, sl=slm, mh=mh, n=f["n"], per_yr=f["per_yr"], win=f["win"], pf=f["pf"],
                     avg=f["avg_pts"], dd=f["maxdd_pts"], is_win=a["win"], is_pf=a["pf"], oos_n=o["n"],
                     oos_win=o["win"], oos_pf=o["pf"]))
R = pd.DataFrame(rows); R.to_csv("../data/grid_daily_setups.csv", index=False)
pd.set_option("display.width", 250)
print(R[(R.sl == 0)].sort_values(["oos_win"], ascending=False).to_string())
print(R[(R.sl == 4)].sort_values(["oos_win"], ascending=False).head(15).to_string())
