"""Universal-rule screen: same config applied to every symbol; metrics pooled per group and overall."""
import itertools, time, numpy as np, pandas as pd
from data2 import UNIVERSE, GROUP, COST, load
from signals import features, TRENDS, SETUPS, STOPS, TRAILS
from sim import run
CUT = pd.Timestamp("2023-01-01")
F = {s: features(load(s)) for s in UNIVERSE} if __name__ == "__main__" else {}
def pm(t):
    if len(t) == 0: return dict(n=0)
    t = t[t.reason != "OPEN"]; w = t.ret > 0
    aw, al = t.ret[w].mean(), -t.ret[~w].mean()
    return dict(n=len(t), win=round(100*w.mean(),1), payoff=round(aw/al,2) if al>0 else np.inf,
                pf=round(t.ret[w].sum()/-t.ret[~w].sum(),2) if (~w).any() else np.inf,
                exp=round(100*t.ret.mean(),2), worst=round(100*t.ret.min(),1))
if __name__ == "__main__":
  rows = []; t0 = time.time()
  for (tn, tf), (sn, (sf, et, tg)), (stn, stf), R, (trn, (be, ts, ta)) in itertools.product(
          TRENDS.items(), SETUPS.items(), STOPS.items(), (1.5, 2.0, 3.0), TRAILS.items()):
      allt = []
      for s, f in F.items():
          sig = (sf(f) & tf(f) & f.sma200.notna()).fillna(False).values
          t = run(f, sig, stf(f).values, COST[GROUP[s]], R=R, trig=None if tg is None else tg(f).values,
                  entry_type=et, be_R=be, trail_start_R=ts, trail_atr=ta, atr=f.atr.values)
          t["sym"] = s; t["grp"] = GROUP[s]; allt.append(t)
      T = pd.concat(allt)
      base = dict(trend=tn, setup=sn, stop=stn, R=R, trail=trn)
      for g, G in [("ALL", T)] + list(T.groupby("grp")):
          i, o = pm(G[G.entry_date < CUT]), pm(G[G.entry_date >= CUT])
          rows.append(dict(**base, grp=g, **{f"is_{k}": v for k, v in i.items()}, **{f"oos_{k}": v for k, v in o.items()},
                           n_sym_pos_is=int((G[G.entry_date < CUT].groupby("sym").ret.mean() > 0).sum())))
  print("done", round(time.time()-t0), "s")
  pd.DataFrame(rows).to_pickle("../data/screen_pooled.pkl")
