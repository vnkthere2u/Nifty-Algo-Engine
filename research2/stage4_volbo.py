"""Generalisation test: trend breakout gated by LOW relative volatility (the mechanism behind the BTC edge)."""
import itertools, numpy as np, pandas as pd
from data2 import UNIVERSE, GROUP, COST, load
from signals import features, TRENDS, SETUPS, STOPS, TRAILS
from sim import run
from pooled import pm, CUT
F = {s: features(load(s)) for s in UNIVERSE}
for f in F.values():
    f["atrp"] = f.atr / f.Close
    f["atrp_rank"] = f.atrp.rolling(252).rank(pct=True)
rows = []
for tn, sn, vr, stn, R, trn, cs in itertools.product(["T1", "T2", "T3"], ["BO_C20", "BO_C55", "BO_STOP20", "SQZ"],
                                                  [1.0, 0.5, 0.33, 0.2], ["A2.5", "A3.5"], (1.5, 2.0, 3.0),
                                                  ["none", "BE1"], (False, True)):
    sf, et, tg = SETUPS[sn]; be, ts, ta = TRAILS[trn]; allt = []
    for s, f in F.items():
        m = sf(f) & TRENDS[tn](f) & f.sma200.notna() & (f.atrp_rank <= vr)
        t = run(f, m.fillna(False).values, STOPS[stn](f).values, COST[GROUP[s]], R=R,
                trig=None if tg is None else tg(f).values, entry_type=et, be_R=be, trail_start_R=ts,
                trail_atr=ta, atr=f.atr.values, close_stop=cs)
        t["sym"] = s; t["grp"] = GROUP[s]; allt.append(t)
    T = pd.concat(allt)
    for g, G in [("ALL", T)] + list(T.groupby("grp")):
        i, o = pm(G[G.entry_date < CUT]), pm(G[G.entry_date >= CUT])
        rows.append(dict(trend=tn, setup=sn, volrank=vr, stop=stn, R=R, trail=trn, close_stop=cs, grp=g,
                         **{f"is_{k}": v for k, v in i.items()}, **{f"oos_{k}": v for k, v in o.items()}))
P = pd.DataFrame(rows); P.to_pickle("../data/screen_volbo.pkl")
pd.set_option("display.width", 260)
cols = ['grp','trend','setup','volrank','stop','R','trail','close_stop','is_n','is_win','is_payoff','is_pf','oos_n','oos_win','oos_payoff','oos_pf','oos_exp']
mn = {'ALL': 150, 'STOCK': 100, 'INDEX': 25, 'COMMODITY': 40, 'CRYPTO': 20}
for g in mn:
    Q = P[(P.grp == g) & (P.is_n >= mn[g])]
    ok = Q[(Q.is_win >= 60) & (Q.is_payoff >= 1.5) & (Q.is_pf >= 1.5)]
    both = ok[(ok.oos_win >= 60) & (ok.oos_payoff >= 1.5) & (ok.oos_pf >= 1.5)]
    print(f"\n### {g}: configs={len(Q)} IS-pass={len(ok)} IS+OOS-pass={len(both)}")
    show = both if len(both) else (ok if len(ok) else Q[Q.is_payoff >= 1.5].sort_values('is_win', ascending=False))
    print(show.sort_values('is_pf', ascending=False)[cols].head(6).to_string())
print("\nMedian effect of vol filter (ALL):")
print(P[P.grp == 'ALL'].groupby('volrank')[['is_n', 'is_win', 'is_payoff', 'is_pf', 'oos_win', 'oos_payoff', 'oos_pf']].median())
