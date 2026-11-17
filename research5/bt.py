"""Strategies A-E on NIFTY 50 + Next 50 (daily, long only, Rs 1L/trade, 10% hard stop, 0.35% costs)."""
import glob, os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from nifty_algo.btc import BtcParams, open_pos, step_bar

D = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "yahoo100")
CUT, START, COST = pd.Timestamp("2022-01-01"), "2010-01-01", 0.0035
REAL_CRASH = {"CGPOWER"}   # big gaps that were real events, not corporate actions

def load(sym):
    d = pd.read_pickle(f"{D}/{sym}.pkl")[["Open", "High", "Low", "Close"]].astype(float)
    if sym not in REAL_CRASH:
        g = d.Open / d.Close.shift() - 1
        for t in g[g.abs() > 0.30].index:                      # demerger / bad tick: back-adjust history
            i = d.index.get_loc(t); f = d.Open.iloc[i] / d.Close.iloc[i - 1]
            d.iloc[:i] = d.iloc[:i] * f
    d = d[d.index >= START]
    d["High"] = d[["Open", "High", "Low", "Close"]].max(axis=1); d["Low"] = d[["Open", "High", "Low", "Close"]].min(axis=1)
    return d

def feats(d, nifty):
    f = d.copy(); c = f.Close; pc = c.shift()
    tr = pd.concat([f.High - f.Low, (f.High - pc).abs(), (f.Low - pc).abs()], axis=1).max(axis=1)
    f["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean()
    dl = c.diff(); up = dl.clip(lower=0).ewm(alpha=.5, adjust=False).mean(); dn = (-dl.clip(upper=0)).ewm(alpha=.5, adjust=False).mean()
    f["rsi2"] = (100 - 100 / (1 + up / dn.replace(0, np.nan))).fillna(100)
    f["ibs"] = ((c - f.Low) / (f.High - f.Low).replace(0, np.nan)).fillna(.5)
    for n in (5, 50, 200): f[f"sma{n}"] = c.rolling(n).mean()
    for n in (20, 50, 200): f[f"ema{n}"] = c.ewm(span=n, adjust=False).mean()
    f["hh20p"] = f.High.rolling(20).max().shift(1)
    f["atrp_rank"] = (f.atr / c).rolling(252).rank(pct=True)
    n = nifty.Close.reindex(f.index).ffill()
    f["nifty_up"] = n > n.rolling(50).mean()
    f["dn3"] = (c < pc) & (pc < pc.shift()) & (pc.shift() < pc.shift(2))
    return f

def sim_mr(f, sig, exit_kind, cost=COST, hard=0.10, maxhold=10):
    o, h, l, c = f.Open.values, f.High.values, f.Low.values, f.Close.values
    xs = f.sma5.values; out = []; i = 0; n = len(c)
    while i < n - 1:
        if not sig[i]: i += 1; continue
        e = c[i]; hs = e * (1 - hard); j = i + 1; xp = None
        while j < n:
            if o[j] <= hs: xp = o[j]; break
            if l[j] <= hs: xp = hs; break
            if (exit_kind == "prevhigh" and c[j] > h[j - 1]) or (exit_kind == "sma5" and c[j] > xs[j]) or j - i >= maxhold:
                xp = c[j]; break
            j += 1
        if xp is None: break
        out.append((f.index[i], f.index[j], e, xp, xp / e - 1 - cost)); i = j
    return out

def sim_bo(f, sig, cost=COST):
    p = BtcParams(cost_pct=cost); o, h, l, c = (f[k].values for k in ("Open", "High", "Low", "Close"))
    atr = f.atr.values; out = []; i = 0; n = len(c)
    while i < n - 1:
        if not sig[i]: i += 1; continue
        ps = open_pos("x", o[i + 1], p.stop_atr * atr[i], p)
        if ps is None: i += 1; continue
        j = i + 1; hit = None
        while j < n:
            hit = step_bar(ps, o[j], h[j], l[j], c[j], first=(j == i + 1), completed=True, p=p)
            if hit: break
            j += 1
        if not hit: break
        out.append((f.index[i + 1], f.index[j], ps.entry, hit[0], hit[0] / ps.entry - 1 - cost)); i = j
    return out

def signals(f):
    up50 = f.Close > f.sma50
    tp = ((f.rsi2 < 10) | (f.ibs < .15)) & up50
    trend = (f.ema20 > f.ema50) & (f.ema50 > f.ema200) & f.sma200.notna()
    bo = trend & (f.Close > f.hh20p)
    return {
        "A_TPMR": (tp & f.sma50.notna(), "mr", "prevhigh"),
        "B_TPMR+regime": (tp & f.nifty_up & (f.Close > f.sma200), "mr", "prevhigh"),
        "C_BTC_breakout": (bo, "bo", None),
        "D_breakout+squeeze": (bo & (f.atrp_rank < .30), "bo", None),
        "E_3down_uptrend": (f.dn3 & (f.Close > f.sma200), "mr", "sma5"),
    }

def run_all(cost=COST, null=False, seed=0):
    nifty = pd.read_pickle(f"{D}/NSEI.pkl")
    rows = []; rng = np.random.default_rng(seed)
    for path in sorted(glob.glob(f"{D}/*.pkl")):
        sym = os.path.basename(path)[:-4]
        if sym == "NSEI": continue
        d = load(sym)
        if len(d) < 600: continue
        f = feats(d, nifty)
        for name, (sig, kind, ek) in signals(f).items():
            s = sig.fillna(False).values
            if null:   # same number of entries, random dates
                k = s.sum(); s = np.zeros(len(s), bool); s[rng.choice(np.arange(200, len(s) - 1), size=min(k, len(s) - 201), replace=False)] = True
            tr = sim_mr(f, s, ek, cost) if kind == "mr" else sim_bo(f, s, cost)
            for t in tr: rows.append((sym, name) + t)
    return pd.DataFrame(rows, columns=["sym", "strat", "entry_date", "exit_date", "entry", "exit", "ret"])

def score(t):
    if len(t) == 0: return dict(n=0)
    w = t.ret > 0
    return dict(n=len(t), win=100 * w.mean(), pf=t.ret[w].sum() / max(1e-9, -t.ret[~w].sum()),
                avg_win=100 * t.ret[w].mean() if w.any() else 0, avg_loss=100 * t.ret[~w].mean() if (~w).any() else 0,
                worst=100 * t.ret.min(), exp=100 * t.ret.mean())

def table(T):
    out = []
    for (sym, st), g in T.groupby(["sym", "strat"]):
        a, b, f = score(g[g.entry_date < CUT]), score(g[g.entry_date >= CUT]), score(g)
        out.append(dict(sym=sym, strat=st, n=f["n"], n_oos=b.get("n", 0), win_is=a.get("win"), win_oos=b.get("win"),
                        pf_is=a.get("pf"), pf_oos=b.get("pf"), worst=f.get("worst"), exp=f.get("exp"),
                        avg_win=f.get("avg_win"), avg_loss=f.get("avg_loss")))
    return pd.DataFrame(out)

def passes(r):
    return (r.n >= 30 and r.n_oos >= 10 and (r.win_is or 0) >= 60 and (r.win_oos or 0) >= 60
            and (r.pf_is or 0) >= 1.5 and (r.pf_oos or 0) >= 1.5)
