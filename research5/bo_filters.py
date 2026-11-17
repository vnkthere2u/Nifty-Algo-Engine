"""Universal filters on the breakout (same rules for every stock), pooled; IS 2010-21 vs OOS 2022+."""
import itertools, numpy as np, pandas as pd
from bt import *
from nifty_algo.btc import BtcParams
import nifty_algo.btc as B

nifty = pd.read_pickle(f"{D}/NSEI.pkl")
F = {}
for path in sorted(glob.glob(f"{D}/*.pkl")):
    s = os.path.basename(path)[:-4]
    if s == "NSEI": continue
    d = load(s)
    if len(d) < 600: continue
    f = feats(d, nifty)
    n = nifty.Close.reindex(f.index).ffill()
    f["rs126"] = f.Close.pct_change(126) - n.pct_change(126)
    f["nifty_up200"] = n > n.rolling(200).mean()
    F[s] = f

def run(target_r, regime, rs, cost=COST):
    rows = []
    for s, f in F.items():
        trend = (f.ema20 > f.ema50) & (f.ema50 > f.ema200) & f.sma200.notna()
        sig = trend & (f.Close > f.hh20p)
        if regime: sig &= f.nifty_up & f.nifty_up200
        if rs: sig &= f.rs126 > 0
        p = BtcParams(cost_pct=cost, target_r=target_r)
        o, h, l, c = (f[k].values for k in ("Open", "High", "Low", "Close")); atr = f.atr.values
        sv = sig.fillna(False).values; i = 0; n = len(c)
        while i < n - 1:
            if not sv[i]: i += 1; continue
            ps = B.open_pos("x", o[i + 1], p.stop_atr * atr[i], p)
            if ps is None: i += 1; continue
            j = i + 1; hit = None
            while j < n:
                hit = B.step_bar(ps, o[j], h[j], l[j], c[j], first=(j == i + 1), completed=True, p=p)
                if hit: break
                j += 1
            if not hit: break
            rows.append((s, f.index[i + 1], hit[0] / ps.entry - 1 - cost)); i = j
    return pd.DataFrame(rows, columns=["sym", "entry_date", "ret"])

out = []
for tr, rg, rs in itertools.product((2.0, 1.5), (False, True), (False, True)):
    T = run(tr, rg, rs)
    a, b = score(T[T.entry_date < CUT]), score(T[T.entry_date >= CUT])
    T2 = run(tr, rg, rs, 2 * COST); b2 = score(T2[T2.entry_date >= CUT])
    yrs = T.groupby(T.entry_date.dt.year).ret.agg(lambda x: (x > 0).mean() * 100)
    out.append(dict(target=f"{tr}R", nifty_regime=rg, rel_strength=rs, n_is=a["n"], win_is=round(a["win"], 1), pf_is=round(a["pf"], 2),
                    n_oos=b["n"], win_oos=round(b["win"], 1), pf_oos=round(b["pf"], 2), pf_oos_2xcost=round(b2["pf"], 2),
                    avg_win=round(score(T)["avg_win"], 2), avg_loss=round(score(T)["avg_loss"], 2),
                    worst_year_win=round(yrs.min(), 1), trades_per_yr=round(len(T) / 16.8, 1)))
pd.set_option("display.width", 250)
print(pd.DataFrame(out).to_string(index=False))
