"""S1: cross-sectional momentum rotation on NIFTY 100 (current constituents), monthly rebalance.
Decision at month-end close, executed at the NEXT day's close (1-day lag, conservative).
Costs 0.175% per side (0.35% round trip). Regime: hold only while NIFTY > its 200-day SMA."""
import glob, os, sys, itertools
import numpy as np, pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "research5"))
from bt import load, D

START, CUT = pd.Timestamp("2012-01-01"), pd.Timestamp("2022-01-01")
SIDE_COST = 0.00175

def panel():
    closes = {}
    for p in sorted(glob.glob(f"{D}/*.pkl")):
        s = os.path.basename(p)[:-4]
        if s == "NSEI": continue
        d = load(s)
        if len(d) > 300: closes[s] = d.Close
    C = pd.DataFrame(closes).sort_index()
    n = pd.read_pickle(f"{D}/NSEI.pkl").Close.reindex(C.index).ffill()
    return C, n

def backtest(C, nifty, top=10, lb=252, skip=21, vol_adj=True, regime=True, trend_filter=True, side_cost=SIDE_COST):
    R = C.pct_change().fillna(0.0)
    mom = C.shift(skip) / C.shift(lb) - 1
    vol = R.rolling(252).std() * np.sqrt(252)
    score = mom / vol if vol_adj else mom
    elig = C.notna() & C.shift(lb).notna()
    if trend_filter: elig &= C > C.rolling(200).mean()
    nup = nifty > nifty.rolling(200).mean()
    month_end = C.index.to_series().groupby(C.index.to_period("M")).max().values
    w = pd.DataFrame(0.0, index=C.index, columns=C.columns)
    cur = pd.Series(0.0, index=C.columns); trades = []; open_at = {}
    rebal = set(pd.DatetimeIndex(month_end))
    dates = C.index
    for k in range(1, len(dates)):
        t, prev = dates[k], dates[k - 1]
        if prev in rebal and prev >= START - pd.Timedelta(days=40):
            sc = score.loc[prev].where(elig.loc[prev]).dropna()
            pick = list(sc.sort_values(ascending=False).index[:top]) if (not regime or nup.loc[prev]) else []
            new = pd.Series(0.0, index=C.columns)
            if pick: new[pick] = 1.0 / top          # cash for empty slots (conservative)
            for s in set(cur[cur > 0].index) - set(pick):            # closed positions
                trades.append((s, open_at.pop(s), t, C.at[open_at_close.pop(s), s] if False else None))
            for s in set(pick) - set(cur[cur > 0].index):
                open_at[s] = t
            cur = new
        w.loc[t] = cur
    port = (w.shift(1).fillna(0) * R).sum(axis=1)                    # weights set on day t earn from t+1
    turn = w.diff().abs().sum(axis=1).fillna(0)
    port = port - turn.shift(1).fillna(0) * side_cost
    return port[port.index >= START], w

def trade_list(C, w, side_cost=SIDE_COST):
    """Each continuous holding of a stock = one trade; return from entry close to exit close, net of costs."""
    out = []
    held = w > 0
    for s in C.columns:
        h = held[s].values; idx = C.index
        i = 0
        while i < len(h):
            if h[i]:
                j = i
                while j + 1 < len(h) and h[j + 1]: j += 1
                e, x = C[s].iloc[i], C[s].iloc[min(j + 1, len(h) - 1)]
                out.append((s, idx[i], idx[min(j + 1, len(h) - 1)], x / e - 1 - 2 * side_cost))
                i = j + 1
            else:
                i += 1
    return pd.DataFrame(out, columns=["sym", "entry", "exit", "ret"])

def stats(r):
    eq = (1 + r).cumprod(); yrs = (r.index[-1] - r.index[0]).days / 365.25
    cagr = eq.iloc[-1] ** (1 / yrs) - 1; dd = (eq / eq.cummax() - 1).min()
    sh = r.mean() / r.std() * np.sqrt(252) if r.std() > 0 else 0
    m = (1 + r).resample("ME").prod() - 1
    return dict(cagr=round(100 * cagr, 1), maxdd=round(100 * dd, 1), sharpe=round(sh, 2), mar=round(cagr / -dd, 2) if dd < 0 else np.inf,
                month_win=round(100 * (m > 0).mean(), 1))

if __name__ == "__main__":
    C, nifty = panel()
    nr = nifty.pct_change().fillna(0); nr = nr[nr.index >= START]
    pd.set_option("display.width", 250)
    print("universe:", C.shape[1], "stocks;", C.index[0].date(), "->", C.index[-1].date())
    print("NIFTY (price index) IS/OOS:", stats(nr[nr.index < CUT]), stats(nr[nr.index >= CUT]))
    rows = []
    for top, lb, va, rg in itertools.product((5, 10, 15), (126, 252), (True, False), (True, False)):
        r, w = backtest(C, nifty, top, lb, 21, va, rg)
        a, b = stats(r[r.index < CUT]), stats(r[r.index >= CUT])
        r2, _ = backtest(C, nifty, top, lb, 21, va, rg, side_cost=2 * SIDE_COST)
        b2 = stats(r2[r2.index >= CUT])
        T = trade_list(C, w); Tw = T.ret > 0
        rows.append(dict(top=top, lookback=lb, vol_adj=va, regime=rg, **{f"is_{k}": v for k, v in a.items()},
                         **{f"oos_{k}": v for k, v in b.items()}, oos_cagr_2xcost=b2["cagr"],
                         trades=len(T), trade_win=round(100 * Tw.mean(), 1),
                         trade_pf=round(T.ret[Tw].sum() / -T.ret[~Tw].sum(), 2)))
    R = pd.DataFrame(rows); R.to_csv("../data/r6_momentum_grid.csv", index=False)
    print(R.to_string(index=False))
