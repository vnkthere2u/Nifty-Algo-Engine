"""Stage-2 baseline screen: every setup x trend x stop x R x trail on every symbol.
Selection uses IN-SAMPLE (<2023) only; OOS columns are stored but not used for ranking."""
import itertools, time, numpy as np, pandas as pd
from data2 import UNIVERSE, GROUP, COST, load
from signals import features, TRENDS, SETUPS, STOPS, TRAILS
from sim import run, metrics
CUT = pd.Timestamp("2023-01-01")
rows = []; t0 = time.time()
for sym in UNIVERSE:
    f = features(load(sym)); cost = COST[GROUP[sym]]
    valid = f.sma200.notna() & f.atr.notna()
    for (tn, tf), (sn, (sf, et, tg)), (stn, stf), R, (trn, (be, ts, ta)) in itertools.product(
            TRENDS.items(), SETUPS.items(), STOPS.items(), (1.5, 2.0, 3.0), TRAILS.items()):
        sig = (sf(f) & tf(f) & valid).fillna(False).values
        t = run(f, sig, stf(f).values, cost, R=R, trig=None if tg is None else tg(f).values, entry_type=et,
                be_R=be, trail_start_R=ts, trail_atr=ta, atr=f.atr.values)
        ti, to = t[t.entry_date < CUT], t[t.entry_date >= CUT]
        mi = metrics(ti, years=(min(CUT, f.index[-1]) - f.index[200]).days / 365.25)
        mo = metrics(to, years=(f.index[-1] - CUT).days / 365.25)
        rows.append(dict(sym=sym, grp=GROUP[sym], trend=tn, setup=sn, stop=stn, R=R, trail=trn,
                         **{f"is_{k}": v for k, v in mi.items()}, **{f"oos_{k}": v for k, v in mo.items()}))
    print(sym, len(rows), round(time.time() - t0), "s", flush=True)
pd.DataFrame(rows).to_pickle("../data/screen_daily.pkl")
