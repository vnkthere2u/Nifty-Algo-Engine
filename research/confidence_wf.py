"""Walk-forward confidence model for the daily MR candidate (U2). Logistic regression (numpy, L2),
trained on all trades closed BEFORE the test year, applied to that year. Checks calibration + gating."""
import numpy as np, pandas as pd
from engine_fast import Market, rsi, atr, stats
mk = Market(); m1 = mk.m1
snap = m1[(m1.index.hour * 60 + m1.index.minute) <= 925]
d = snap.resample("1D").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()
d["t"] = d.index + pd.Timedelta(hours=15, minutes=25)
d["rsi2"] = rsi(d.Close, 2)
for n in (5, 50, 200): d[f"sma{n}"] = d.Close.rolling(n).mean()
d["atr"] = atr(d.High, d.Low, d.Close, 14)
d["ibs"] = (d.Close - d.Low) / (d.High - d.Low).replace(0, np.nan)
d["prevH"] = d.High.shift()
d["f_trend"] = (d.Close - d.sma50) / d.atr           # trend strength
d["f_stretch"] = (d.sma5 - d.Close) / d.atr          # pullback depth
d["f_rsi"] = d.rsi2 / 100
d["f_ibs"] = d.ibs
d["f_vol"] = d.atr / d.Close * 100                   # vol regime
d["f_ret5"] = d.Close.pct_change(5) * 100
d["f_lt"] = (d.Close > d.sma200).astype(float)
d = d.dropna(subset=["sma50"]).iloc[5:]
m = ((d.rsi2 < 10) | (d.ibs < .15)) & (d.Close > d.sma50)
s = d[m]
fl = mk.minute_flags(d.t.values, (d.Close > d.prevH).values)
F = ["f_trend", "f_stretch", "f_rsi", "f_ibs", "f_vol", "f_ret5", "f_lt"]
tr = mk.run(s.t.values, np.ones(len(s), int), np.zeros(len(s)), np.zeros(len(s)), xl=fl, allow_overnight=True,
            meta={k: s[k].values for k in F})
tr["y"] = (tr.net > 0).astype(float); tr["yr"] = pd.to_datetime(tr.entry_time).dt.year

def fit(X, y, lam=1.0, it=3000, lr=0.1):
    mu, sd = X.mean(0), X.std(0) + 1e-9; Z = (X - mu) / sd
    w = np.zeros(Z.shape[1]); b = np.log(y.mean() / (1 - y.mean()))
    for _ in range(it):
        p = 1 / (1 + np.exp(-(Z @ w + b)))
        w -= lr * (Z.T @ (p - y) / len(y) + lam * w / len(y)); b -= lr * (p - y).mean()
    return mu, sd, w, b
def pred(mod, X):
    mu, sd, w, b = mod; return 1 / (1 + np.exp(-(((X - mu) / sd) @ w + b)))

tr["conf"] = np.nan
for yr in range(2020, 2027):
    trn = tr[pd.to_datetime(tr.exit_time).dt.year < yr]; tst = tr.yr == yr
    mod = fit(trn[F].values, trn.y.values, lam=5.0)
    # shrink toward prior base rate (Bayesian blend by sample size)
    base = trn.y.mean(); k = len(trn) / (len(trn) + 40)
    tr.loc[tst, "conf"] = k * pred(mod, tr.loc[tst, F].values) + (1 - k) * base
wf = tr.dropna(subset=["conf"]).copy()
print("walk-forward trades", len(wf), "win", round(wf.y.mean() * 100, 1), "Brier", round(((wf.conf - wf.y) ** 2).mean(), 4),
      "vs base-rate Brier", round(((wf.y.mean() - wf.y) ** 2).mean(), 4))
wf["bucket"] = pd.cut(wf.conf, [0, .6, .7, .75, .8, 1.0])
print(wf.groupby("bucket", observed=True).agg(n=("y", "size"), pred=("conf", "mean"), actual=("y", "mean"), pts=("net", "sum")).round(3))
for th in (0, .65, .7, .72, .75, .78):
    g = wf[wf.conf >= th]; st = stats(g)
    print(f"gate>={th:.2f}: n={st['n']} /yr={st['per_yr']} win={st['win']} pf={st['pf']} avg={st['avg_pts']} tot={st['tot_pts']} dd={st['maxdd_pts']}")
mod = fit(tr[F].values, tr.y.values, lam=5.0)
print("full-sample coefficients (z-scored):", dict(zip(F, np.round(mod[2], 3))))
