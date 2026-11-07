"""Long-only, no-leverage, one-position-per-symbol daily simulator (numba).

Order model
  entry_type 0 (MARKET): signal on bar i close -> buy at open of bar i+1.
  entry_type 1 (STOP):   signal on bar i close -> buy-stop at trigger[i] valid on bar i+1 only;
                         fill at max(open, trigger) if high >= trigger, else cancelled.
Risk
  stop distance D (price) fixed at entry: SL = E - D, TP = E + R*D  (R >= 1.5 => planned RR >= 1:1.5).
  trades with D/E > max_risk (10%) are skipped.
Gap realism
  at each bar's open: open <= SL -> exit at OPEN (loss larger than plan); open >= TP -> exit at OPEN.
  intrabar: SL checked before TP (worst case). Trailing/BE updates use the bar CLOSE/HIGH and
  apply from the NEXT bar (no intrabar look-ahead).
No time limit: a trade lives until SL/TP/trailing stop is hit (or data ends -> marked to market).
"""
import numpy as np
import pandas as pd
from numba import njit


@njit(cache=True)
def simulate(o, h, l, c, sig, trig, dist, atr, entry_type, R, be_R, trail_start_R, trail_atr, max_risk, be_lock, close_stop):
    n = len(c)
    out = np.zeros((n, 8))  # entry_i, exit_i, entry_px, exit_px, reason, risk_pct, mfe_R, init_D
    k = 0
    i = 0
    while i < n - 1:
        if not sig[i]:
            i += 1
            continue
        j = i + 1
        if entry_type == 1:
            if h[j] < trig[i]:
                i += 1
                continue
            e = max(o[j], trig[i])
        else:
            e = o[j]
        D = dist[i]
        if D <= 0 or D / e > max_risk:
            i += 1
            continue
        sl = e - D
        tp = e + R * D
        a = atr[i]
        hh = e
        reason = 0
        xp = 0.0
        first = True
        while True:
            if j >= n:
                j = n - 1
                xp = c[j]
                reason = 9
                break
            hard = e * (1.0 - max_risk)          # absolute risk cap, always intraday
            act = hard if (close_stop and sl < e) else sl   # closing-basis mode: only cap is intraday
            if not first:
                if o[j] <= act:
                    xp = o[j]; reason = 1; break
                if o[j] >= tp:
                    xp = o[j]; reason = 2; break
            if l[j] <= act:
                xp = act; reason = 1; break
            if h[j] >= tp:
                xp = tp; reason = 2; break
            if close_stop and sl < e and c[j] <= sl:
                xp = c[j]; reason = 1; break
            # end-of-bar stop management (effective next bar)
            if h[j] > hh:
                hh = h[j]
            if be_R > 0 and hh - e >= be_R * D and sl < e:
                sl = e * (1.0 + be_lock)
            if trail_atr > 0 and hh - e >= trail_start_R * D:
                ns = hh - trail_atr * a
                if ns > sl:
                    sl = ns
            first = False
            j += 1
        if reason == 1 and sl >= e:
            reason = 3  # trailing / break-even stop
        out[k, 0] = i + 1; out[k, 1] = j; out[k, 2] = e; out[k, 3] = xp; out[k, 4] = reason
        out[k, 5] = D / e; out[k, 6] = (hh - e) / D; out[k, 7] = D
        k += 1
        i = j  # next signal can occur on the exit bar's close
    return out[:k]


REASON = {1: "SL", 2: "TP", 3: "TRAIL", 9: "OPEN"}


def run(df, sig, dist, cost, R=2.0, trig=None, entry_type=0, be_R=0.0, trail_start_R=1.0, trail_atr=0.0,
        max_risk=0.10, atr=None, close_stop=False):
    o, h, l, c = (df[x].values.astype(np.float64) for x in ("Open", "High", "Low", "Close"))
    trig = np.zeros(len(c)) if trig is None else np.nan_to_num(np.asarray(trig, np.float64))
    atr = np.zeros(len(c)) if atr is None else np.nan_to_num(np.asarray(atr, np.float64))
    r = simulate(o, h, l, c, np.asarray(sig, np.bool_), trig, np.nan_to_num(np.asarray(dist, np.float64)),
                 atr, entry_type, float(R), float(be_R), float(trail_start_R), float(trail_atr), float(max_risk),
                 float(cost + 0.001), bool(close_stop))
    if len(r) == 0:
        return pd.DataFrame(columns=["entry_date", "exit_date", "entry", "exit", "reason", "risk_pct", "ret", "R_mult"])
    idx = df.index
    t = pd.DataFrame({"entry_date": idx[r[:, 0].astype(int)], "exit_date": idx[r[:, 1].astype(int)],
                      "entry": r[:, 2], "exit": r[:, 3], "reason": [REASON[int(x)] for x in r[:, 4]],
                      "risk_pct": r[:, 5], "mfe_R": r[:, 6]})
    t["ret"] = t.exit / t.entry - 1 - cost                      # net % return on the ₹1L position
    t["R_mult"] = (t.exit - t.entry - cost * t.entry) / r[:, 7]
    t["days"] = (t.exit_date - t.entry_date).dt.days
    return t


def metrics(t, capital=100_000.0, years=None):
    if t is None or len(t) == 0:
        return dict(n=0)
    closed = t[t.reason != "OPEN"]
    if len(closed) == 0:
        return dict(n=0)
    w = closed.ret > 0
    aw = closed.ret[w].mean() if w.any() else 0.0
    al = -closed.ret[~w].mean() if (~w).any() else np.nan
    pnl = closed.ret * capital
    eq = capital + pnl.cumsum()
    dd = (eq - eq.cummax()).min()
    yrs = years or max((closed.exit_date.max() - closed.entry_date.min()).days / 365.25, 0.25)
    return dict(n=len(closed), per_yr=round(len(closed) / yrs, 1), win=round(100 * w.mean(), 1),
                payoff=round(aw / al, 2) if al and al > 0 else np.inf,
                pf=round(pnl[w].sum() / -pnl[~w].sum(), 2) if (~w).any() and pnl[~w].sum() < 0 else np.inf,
                exp_pct=round(100 * closed.ret.mean(), 2), avg_win_pct=round(100 * aw, 2),
                avg_loss_pct=round(100 * (al if al == al else 0), 2),
                worst_pct=round(100 * closed.ret.min(), 2), pnl_inr=round(pnl.sum()),
                inr_per_yr=round(pnl.sum() / yrs), maxdd_inr=round(dd), maxdd_pct=round(100 * dd / capital, 1),
                avg_days=round(closed.days.mean(), 1), max_risk_pct=round(100 * closed.risk_pct.max(), 1))
