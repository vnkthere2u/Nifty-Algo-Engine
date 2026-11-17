import numpy as np, pandas as pd, sys, os
from momentum import *
sys.path.insert(0, "../research5"); from fetch import N50
C, nifty = panel()
R = C.pct_change().fillna(0)
# equal-weight buy & hold of the same universe (daily rebalanced EW of available stocks) -- same survivorship bias
avail = C.notna() & C.shift(1).notna()
ew = (R * avail).sum(axis=1) / avail.sum(axis=1).replace(0, np.nan)
ew = ew.fillna(0); ew = ew[ew.index >= START]
nup = (nifty > nifty.rolling(200).mean()).shift(1).fillna(False)
ew_reg = (ew * nup.reindex(ew.index)).fillna(0)
cfg = dict(top=10, lb=252, vol_adj=True, regime=True)
r, w = backtest(C, nifty, **cfg)
pd.set_option("display.width", 250)
rows = []
for lbl, x in (("NIFTY index", nifty.pct_change().fillna(0)[lambda s: s.index >= START]), ("EW same 98 stocks", ew),
               ("EW same 98 + NIFTY regime", ew_reg), ("MOMENTUM top10/12m/vol-adj/regime", r)):
    rows.append(dict(strategy=lbl, **{f"IS_{k}": v for k, v in stats(x[x.index < CUT]).items()},
                     **{f"OOS_{k}": v for k, v in stats(x[x.index >= CUT]).items()}))
print(pd.DataFrame(rows).to_string(index=False))
# NIFTY-50-only universe
C50 = C[[c for c in C.columns if c in set(N50)]]
r50, _ = backtest(C50, nifty, **cfg)
print("\nMomentum on NIFTY 50 names only:", "IS", stats(r50[r50.index < CUT]), "OOS", stats(r50[r50.index >= CUT]))
# yearly returns
y = pd.DataFrame({"momentum": (1 + r).resample("YE").prod() - 1, "nifty": (1 + nifty.pct_change().fillna(0)[nifty.index >= START]).resample("YE").prod() - 1,
                  "EW98": (1 + ew).resample("YE").prod() - 1})
print("\nCalendar-year returns %:\n", (100 * y).round(1).T.to_string())
# holdings history concentration: how often top contributors were recent index entrants
T = trade_list(C, w)
print("\nTop 10 contributing stocks (sum of trade returns):\n", T.groupby("sym").ret.sum().sort_values(ascending=False).head(10).round(2).to_string())
# today's picks (as of last close) + regime
last = C.index[-1]
mom = C.shift(21) / C.shift(252) - 1; vol = R.rolling(252).std() * np.sqrt(252); sc = (mom / vol)
elig = (C > C.rolling(200).mean()) & C.shift(252).notna()
s = sc.loc[last].where(elig.loc[last]).dropna().sort_values(ascending=False)
print(f"\nAs of {last.date()}: NIFTY {nifty.iloc[-1]:.0f} vs 200DMA {nifty.rolling(200).mean().iloc[-1]:.0f} -> regime {'ON' if nifty.iloc[-1] > nifty.rolling(200).mean().iloc[-1] else 'OFF (cash)'}")
top = s.head(12)
print(pd.DataFrame({"score": top.round(2), "12-1m return %": (100 * mom.loc[last, top.index]).round(1), "vol %": (100 * vol.loc[last, top.index]).round(1),
                    "close": C.loc[last, top.index].round(2), "200DMA": C.rolling(200).mean().loc[last, top.index].round(2)}).to_string())
# current portfolio per last month-end rebalance
held = w.iloc[-1][w.iloc[-1] > 0].index.tolist()
print("\nHeld since last month-end rebalance:", held)
