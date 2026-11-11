"""Stage-3 levers (pooled, universal rules): closing-basis stop, market regime, relative strength."""
import itertools, time, numpy as np, pandas as pd
from data2 import UNIVERSE, GROUP, COST, load
from signals import features, TRENDS, SETUPS, STOPS, TRAILS
from sim import run
from pooled import pm, CUT
F = {s: features(load(s)) for s in UNIVERSE}
nf = F["NIFTY"]
for s, f in F.items():
    if GROUP[s] == "STOCK":
        n = nf.reindex(f.index).ffill()
        f["mkt_ok"] = (n.Close > n.sma200).values
        f["rs_ok"] = (f.Close.pct_change(63) > n.Close.pct_change(63)).values
    else:
        f["mkt_ok"] = True; f["rs_ok"] = True
SET = ["PB_RSI5", "PB_RSI10", "PB_RSI20", "MOM52_PB", "PB_EMA20", "BO_C20", "BO_C55", "SQZ"]
rows = []; t0 = time.time()
for tn, sn, stn, R, trn, cs, reg in itertools.product(["T1", "T2", "T3"], SET, ["A2.5", "A3.5", "SWING"],
                                                      (1.5, 2.0), ["none", "BE1"], (False, True),
                                                      ("off", "mkt", "mkt+rs")):
    sf, et, tg = SETUPS[sn]; be, ts, ta = TRAILS[trn]; allt = []
    for s, f in F.items():
        m = sf(f) & TRENDS[tn](f) & f.sma200.notna()
        if reg != "off": m &= f.mkt_ok.astype(bool)
        if reg == "mkt+rs": m &= f.rs_ok.astype(bool)
        t = run(f, m.fillna(False).values, STOPS[stn](f).values, COST[GROUP[s]], R=R,
                trig=None if tg is None else tg(f).values, entry_type=et, be_R=be, trail_start_R=ts,
                trail_atr=ta, atr=f.atr.values, close_stop=cs)
        t["sym"] = s; t["grp"] = GROUP[s]; allt.append(t)
    T = pd.concat(allt)
    for g, G in [("ALL", T)] + list(T.groupby("grp")):
        if reg != "off" and g != "STOCK": continue
        i, o = pm(G[G.entry_date < CUT]), pm(G[G.entry_date >= CUT])
        rows.append(dict(trend=tn, setup=sn, stop=stn, R=R, trail=trn, close_stop=cs, regime=reg, grp=g,
                         **{f"is_{k}": v for k, v in i.items()}, **{f"oos_{k}": v for k, v in o.items()}))
print("done", round(time.time() - t0), "s")
pd.DataFrame(rows).to_pickle("../data/screen_stage3.pkl")
