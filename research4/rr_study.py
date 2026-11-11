"""Bhadra breakout: search target/stop geometry with reward >= risk. Same engine as bhadra_bt."""
import itertools, numpy as np, pandas as pd
from bhadra_bt import *

def sim(ev, base, c15, tp_mode, tp_val, sl_mode, exit_mode):
    bt = base.index.values
    o, h, l, c = (base[x].values for x in ("Open", "High", "Low", "Close"))
    bmin = (base.index.hour * 60 + base.index.minute).values
    bday = base.index.normalize().values
    out = []
    for r in ev.itertuples():
        if r.cstart not in c15.index:
            continue
        rng = c15.loc[r.cstart]; H, L = rng.High, rng.Low
        later = c15[(c15.index >= r.cend) & (c15.index.normalize() == r.day)]
        sig = None
        for ts, k in later.iterrows():
            if ts + pd.Timedelta(minutes=15) > r.day + pd.Timedelta(minutes=915):
                break
            if k.Close > H: sig = (1, ts, k.Close); break
            if k.Close < L: sig = (-1, ts, k.Close); break
        if sig is None:
            continue
        d, ts, e = sig
        sl = (L if d == 1 else H) if sl_mode == "range" else (H + L) / 2
        risk = d * (e - sl)
        if risk <= 0:
            continue
        reward = tp_val * risk if tp_mode == "R" else tp_val
        if reward < risk:                     # constraint: risk must not exceed reward
            continue
        tp = e + d * reward
        j = np.searchsorted(bt, (ts + pd.Timedelta(minutes=15)).to_datetime64())
        res = None; k = j
        while k < len(bt):
            nd = bday[k] != np.datetime64(r.day)
            if exit_mode == "eod" and (nd or bmin[k] >= 925):
                res, xp = "EOD", (c[k - 1] if nd else o[k]); break
            if k > j and bday[k] != bday[k - 1]:
                if (d == 1 and o[k] <= sl) or (d == -1 and o[k] >= sl): res, xp = "SL", o[k]; break
                if (d == 1 and o[k] >= tp) or (d == -1 and o[k] <= tp): res, xp = "TP", o[k]; break
            if (l[k] <= sl) if d == 1 else (h[k] >= sl): res, xp = "SL", sl; break
            if (h[k] >= tp) if d == 1 else (l[k] <= tp): res, xp = "TP", tp; break
            k += 1
        if res is None:
            continue
        g = d * (xp - e)
        out.append(dict(day=r.day, dir=d, res=res, risk=risk, reward=reward, net=g - COST_PCT * e))
    return pd.DataFrame(out)

def stats(t):
    if len(t) == 0: return dict(n=0)
    w = t.net > 0
    return dict(n=len(t), tp_hit=round(100 * (t.res == "TP").mean(), 1), win=round(100 * w.mean(), 1),
                eod=round(100 * (t.res == "EOD").mean(), 1), rr=round((t.reward / t.risk).mean(), 2),
                avg_net=round(t.net.mean(), 1), total=round(t.net.sum()),
                pf=round(t.net[w].sum() / -t.net[~w].sum(), 2) if (~w).any() else np.inf)

bh = pd.read_csv(os.path.join(ROOT, "data", "bhadra_periods.csv"), parse_dates=["start", "end"])
inst = {"NIFTY": (load_nifty_1m(), (30, 50, 75, 100)), "BANKNIFTY": (load_bank_5m(), (60, 100, 150, 200))}
rows = []
for name, (base, pts) in inst.items():
    c15 = candles15(base); days = pd.DatetimeIndex(np.unique(base.index.normalize()))
    ev = events(bh[(bh.end >= base.index[0]) & (bh.end <= base.index[-1])], set(days))
    pool = [d for d in days if d not in set(ev.day)]; rng = np.random.default_rng(11)
    ctl_ev = []
    for rep in range(5):
        pick = rng.choice(len(pool), size=len(ev), replace=False); cev = ev.copy()
        cev["day"] = [pool[i] for i in pick]
        cev["cstart"] = [d + (cs - cs.normalize()) for d, cs in zip(cev.day, ev.cstart)]
        cev["cend"] = cev.cstart + pd.Timedelta(minutes=15); ctl_ev.append(cev)
    ctl_ev = pd.concat(ctl_ev, ignore_index=True)
    grid = [("R", k) for k in (1.0, 1.25, 1.5, 2.0)] + [("pts", p) for p in pts]
    for (tm, tv), sm, xm in itertools.product(grid, ("range", "mid"), ("eod", "hold")):
        b = stats(sim(ev, base, c15, tm, tv, sm, xm)); cc = stats(sim(ctl_ev, base, c15, tm, tv, sm, xm))
        rows.append(dict(inst=name, target=f"{tv}{'R' if tm == 'R' else 'pts'}", stop=sm, exit=xm,
                         **{k: v for k, v in b.items()}, ctl_win=cc.get("win"), ctl_pf=cc.get("pf")))
R = pd.DataFrame(rows); R.to_csv(os.path.join(ROOT, "data", "bhadra_rr_study.csv"), index=False)
pd.set_option("display.width", 250); pd.set_option("display.max_rows", 100)
print(R.sort_values(["inst", "win"], ascending=[True, False]).to_string(index=False))
