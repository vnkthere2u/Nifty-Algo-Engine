"""Run every (stock x base strategy x filter x break-even) config; store IS / OOS / FULL scores."""
import os, sys, time, traceback
from multiprocessing import Pool
import numpy as np
import pandas as pd
from core import Ctx, simulate, score, stocks, load5, CUT
from strategies import BASE

FILTERS = ["none", "nifty", "vix", "both"]
_MKT = None


def market():
    n5 = load5("NIFTY_50")
    nd = n5.resample("1D").agg({"Close": "last"}).dropna()
    nd["ok"] = nd.Close > nd.Close.rolling(200).mean()
    vx = load5("INDIA_VIX").resample("1D").agg({"Close": "last"}).dropna()
    vok = (vx.Close < 20)
    return {"n5": n5, "nifty_ok": nd.ok, "vix_ok": vok}


def regime_mask(ctx, sig, kind):
    if kind == "none" or len(sig) == 0:
        return np.ones(len(sig), bool)
    day = ctx.day[sig.bar.values]
    # intraday signals: use previous day's regime; swing signals (known at close): same day
    ref = np.where(sig.intra.values, (day - pd.Timedelta(days=1)), day)
    out = np.ones(len(sig), bool)
    for key in (["nifty_ok"] if kind == "nifty" else ["vix_ok"] if kind == "vix" else ["nifty_ok", "vix_ok"]):
        s = _MKT[key].astype(float)
        v = s.reindex(pd.DatetimeIndex(ref), method="ffill").values
        out &= (v == 1.0)
    return out


def work(sym):
    global _MKT
    if _MKT is None:
        _MKT = market()
    rows, trades = [], {}
    try:
        ctx = Ctx(sym, _MKT)
        for name, fn in BASE.items():
            base = fn(ctx)
            for flt in FILTERS:
                sig = base[regime_mask(ctx, base, flt)] if len(base) else base
                for be in (False, True):
                    t = simulate(ctx, sig, be_on=be)
                    key = (sym, name, flt, be)
                    r = dict(sym=sym, strat=name, filt=flt, be=be)
                    if len(t):
                        trades[key] = t
                        for lbl, sub in (("is", t[t.entry_time < CUT]), ("oos", t[t.entry_time >= CUT]), ("full", t)):
                            for k, v in score(sub).items():
                                r[f"{lbl}_{k}"] = v
                    rows.append(r)
    except Exception:
        traceback.print_exc()
    return rows, trades


if __name__ == "__main__":
    syms = stocks() if len(sys.argv) < 2 else sys.argv[1:]
    t0 = time.time(); allrows, alltr = [], {}
    with Pool(min(8, os.cpu_count())) as p:
        for i, (rows, tr) in enumerate(p.imap_unordered(work, syms)):
            allrows += rows; alltr.update(tr)
            print(f"{i+1}/{len(syms)} {rows[0]['sym'] if rows else '?'} {round(time.time()-t0)}s", flush=True)
    pd.DataFrame(allrows).to_pickle("../data/r3_screen.pkl")
    pd.to_pickle(alltr, "../data/r3_trades.pkl")
    print("done", round(time.time() - t0), "s")
