import numpy as np, pandas as pd
from multiprocessing import Pool
from core import Ctx, simulate, score, stocks, CUT
from strategies import _frame, _swing, _bar_at

def passes(r):
    return (r.get("is_win", 0) >= 60 and r.get("oos_win", 0) >= 60 and r.get("is_pf", 0) >= 1.5 and r.get("oos_pf", 0) >= 1.5
            and r.get("is_pnl_inr", 0) > 0 and r.get("oos_pnl_inr", 0) > 0 and r.get("full_maxdd_pct", -99) >= -10
            and r.get("full_n", 0) >= 30 and r.get("oos_n", 0) >= 10)

def null_work(sym):
    """Random-entry controls: swing-type (daily ATR, buy at random day's next open) and intraday-type."""
    ctx = Ctx(sym); rows = []
    rng = np.random.default_rng(abs(hash(sym)) % 2**32)
    for rep in range(10):
        for kind, rate in (("NULL_SWING", 0.06), ("NULL_INTRA", 0.04)):
            if kind == "NULL_SWING":
                mask = pd.Series(rng.random(len(ctx.d)) < rate, index=ctx.d.index)
                s = _swing(ctx, mask, ctx.d.Close * 0.0)   # level 0 -> fills at next open
            else:
                days = np.flatnonzero(rng.random(len(ctx.days)) < rate * 3)
                mins = rng.integers(600, 860, len(days)) // 5 * 5
                bars = np.array([_bar_at(ctx, np.array([d]), m)[0] for d, m in zip(days, mins)], dtype=np.int64)
                s = _frame(bars, 0, ctx.b.Close.values[bars], bars, ctx.a15.atr.values[bars], True)
            for be in (False, True):
                t = simulate(ctx, s, be_on=be)
                r = dict(sym=sym, strat=f"{kind}_{rep}", be=be)
                for lbl, sub in (("is", t[t.entry_time < CUT]), ("oos", t[t.entry_time >= CUT]), ("full", t)):
                    for k, v in score(sub).items(): r[f"{lbl}_{k}"] = v
                rows.append(r)
    return rows

if __name__ == "__main__":
    S = pd.read_pickle("../data/r3_screen.pkl")
    S["pass"] = [passes(r) for r in S.to_dict("records")]
    print("REAL configs:", len(S), " passes:", int(S["pass"].sum()), f"({100*S['pass'].mean():.2f}%)")
    with Pool(4) as p:
        N = pd.DataFrame([r for rows in p.map(null_work, stocks()) for r in rows])
    N["pass"] = [passes(r) for r in N.to_dict("records")]
    print("NULL configs:", len(N), " passes:", int(N["pass"].sum()), f"({100*N['pass'].mean():.2f}%)")
    print("NULL pooled win%:", round(N.full_win.mean(), 1), " NULL median PF:", round(N.full_pf.median(), 2))
    S.to_pickle("../data/r3_screen_scored.pkl"); N.to_pickle("../data/r3_null.pkl")
    pd.set_option("display.width", 250)
    cols = ["sym","strat","filt","be","is_n","is_win","is_tp_pct","is_be_pct","is_pf","oos_n","oos_win","oos_tp_pct","oos_be_pct","oos_pf","full_maxdd_pct","full_inr_yr","full_per_yr"]
    print(S[S["pass"]].sort_values("oos_pf", ascending=False)[cols].to_string())
