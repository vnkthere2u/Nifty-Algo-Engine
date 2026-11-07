"""Fast event-driven research engine (numba) on 1-minute NIFTY bars.

Signals are produced on COMPLETED bars (15m or daily) and filled at the open of the
first 1-minute bar after the signal bar closes.  Exits are simulated minute by minute:
gap-aware stop/target, worst-case ordering inside a minute (stop before target),
break-even ratchet, max-hold, end-of-day square-off and signal-based exits.
"""
import numpy as np
import pandas as pd
from numba import njit
from load_data import load_1m, resample_session

COST_PCT = 0.0004  # round-trip: STT + exchange + brokerage + ~1pt slippage per side on futures


# ---------------------------------------------------------------- indicators
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def rma(s, n):
    return s.ewm(alpha=1 / n, adjust=False).mean()


def atr(h, l, c, n=14):
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return rma(tr, n)


def adx(h, l, c, n=14):
    up, dn = h.diff(), -l.diff()
    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=h.index)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=h.index)
    a = atr(h, l, c, n)
    pdi = 100 * rma(pdm, n) / a
    mdi = 100 * rma(mdm, n) / a
    dx = (100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)).fillna(0)
    return rma(dx, n), pdi, mdi


def rsi(c, n):
    d = c.diff()
    up = rma(d.clip(lower=0), n)
    dn = rma(-d.clip(upper=0), n)
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))


# ---------------------------------------------------------------- simulator
@njit(cache=True)
def _simulate(entry_idx, direction, sl_dist, tp_dist, mo, mh, ml, mc, day_id, mins,
              xl, xs, max_hold, be_trig, be_off, eod_min, allow_overnight):
    n_sig = len(entry_idx)
    out_e = np.full(n_sig, -1); out_x = np.full(n_sig, -1)
    out_ep = np.zeros(n_sig); out_xp = np.zeros(n_sig); out_r = np.zeros(n_sig, np.int64)
    busy_until = -1
    nm = len(mo)
    for k in range(n_sig):
        i = entry_idx[k]
        if i <= busy_until or i >= nm:
            continue
        d = direction[k]
        e = mo[i]
        sl = e - d * sl_dist[k] if sl_dist[k] > 0 else np.nan
        tp = e + d * tp_dist[k] if tp_dist[k] > 0 else np.nan
        be_done = False
        j = i
        reason = 0
        xp = 0.0
        while True:
            if j >= nm:
                j = nm - 1; xp = mc[j]; reason = 9; break
            # new day gap / overnight handling
            if j > i and day_id[j] != day_id[j - 1]:
                if not allow_overnight:
                    j -= 1; xp = mc[j]; reason = 5; break
                o = mo[j]
                if not np.isnan(sl) and ((d == 1 and o <= sl) or (d == -1 and o >= sl)):
                    xp = o; reason = 1; break
                if not np.isnan(tp) and ((d == 1 and o >= tp) or (d == -1 and o <= tp)):
                    xp = o; reason = 2; break
            # signal exit at this minute's open
            if j > i and ((d == 1 and xl[j]) or (d == -1 and xs[j])):
                xp = mo[j]; reason = 3; break
            # end-of-day square-off
            if not allow_overnight and mins[j] >= eod_min:
                xp = mo[j]; reason = 5; break
            if max_hold > 0 and j - i >= max_hold:
                xp = mo[j]; reason = 4; break
            hi, lo = mh[j], ml[j]
            if d == 1:
                if not np.isnan(sl) and lo <= sl:
                    xp = min(sl, mo[j]) if j > i else sl; reason = 1; break
                if not np.isnan(tp) and hi >= tp:
                    xp = tp; reason = 2; break
                if be_trig > 0 and not be_done and hi - e >= be_trig * sl_dist[k]:
                    nsl = e + be_off * sl_dist[k]
                    if np.isnan(sl) or nsl > sl:
                        sl = nsl
                    be_done = True
            else:
                if not np.isnan(sl) and hi >= sl:
                    xp = max(sl, mo[j]) if j > i else sl; reason = 1; break
                if not np.isnan(tp) and lo <= tp:
                    xp = tp; reason = 2; break
                if be_trig > 0 and not be_done and e - lo >= be_trig * sl_dist[k]:
                    nsl = e - be_off * sl_dist[k]
                    if np.isnan(sl) or nsl < sl:
                        sl = nsl
                    be_done = True
            j += 1
        out_e[k] = i; out_x[k] = j; out_ep[k] = e; out_xp[k] = xp; out_r[k] = reason
        busy_until = j
    return out_e, out_x, out_ep, out_xp, out_r


REASONS = {1: "SL", 2: "TP", 3: "SIGNAL", 4: "TIME", 5: "EOD", 9: "END"}


class Market:
    def __init__(self):
        m1 = load_1m()
        self.m1 = m1
        self.mt = m1.index.values
        self.mo = m1.Open.values.astype(np.float64)
        self.mh = m1.High.values.astype(np.float64)
        self.ml = m1.Low.values.astype(np.float64)
        self.mc = m1.Close.values.astype(np.float64)
        self.mins = (m1.index.hour * 60 + m1.index.minute).values.astype(np.int64)
        self.day_id = ((m1.index.normalize() - pd.Timestamp("2000-01-01")).days).values.astype(np.int64)
        self.b15 = resample_session(m1, 15)
        self.b5 = resample_session(m1, 5)
        dd = m1.resample("1D").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"})
        self.daily = dd.dropna()

    def entry_index(self, bar_end_times):
        """First minute at or after the bar end (signal bar close)."""
        return np.searchsorted(self.mt, np.asarray(bar_end_times, dtype="datetime64[ns]"))

    def minute_flags(self, bar_end_times, flags):
        """Map a boolean on completed bars to 'exit at open of first minute after bar end'."""
        out = np.zeros(len(self.mt), dtype=np.bool_)
        idx = self.entry_index(np.asarray(bar_end_times)[np.asarray(flags, bool)])
        idx = idx[idx < len(out)]
        out[idx] = True
        return out

    def run(self, sig_times_end, direction, sl_dist, tp_dist, xl=None, xs=None, max_hold=0,
            be_trig=0.0, be_off=0.0, eod_min=920, allow_overnight=False, cost_pct=COST_PCT,
            meta=None):
        order = np.argsort(np.asarray(sig_times_end))
        st = np.asarray(sig_times_end)[order]
        ei = self.entry_index(st)
        nm = len(self.mt)
        if xl is None: xl = np.zeros(nm, np.bool_)
        if xs is None: xs = np.zeros(nm, np.bool_)
        e, x, ep, xp, r = _simulate(ei.astype(np.int64), np.asarray(direction, np.int64)[order],
                                    np.asarray(sl_dist, np.float64)[order], np.asarray(tp_dist, np.float64)[order],
                                    self.mo, self.mh, self.ml, self.mc, self.day_id, self.mins,
                                    xl, xs, int(max_hold), float(be_trig), float(be_off), int(eod_min),
                                    bool(allow_overnight))
        ok = e >= 0
        d = np.asarray(direction)[order][ok]
        tr = pd.DataFrame({
            "entry_time": self.mt[e[ok]], "exit_time": self.mt[np.minimum(x[ok], nm - 1)],
            "dir": d, "entry": ep[ok], "exit": xp[ok],
            "sl_dist": np.asarray(sl_dist)[order][ok],
            "reason": [REASONS[int(v)] for v in r[ok]]})
        if meta is not None:
            for k_, v_ in meta.items():
                tr[k_] = np.asarray(v_)[order][ok]
        tr["gross"] = tr.dir * (tr.exit - tr.entry)
        tr["net"] = tr.gross - cost_pct * tr.entry
        tr["R"] = tr.net / tr.sl_dist.replace(0, np.nan)
        return tr


def stats(tr, label=""):
    if len(tr) == 0:
        return dict(label=label, n=0)
    w = tr.net > 0
    gp, gl = tr.net[w].sum(), -tr.net[~w].sum()
    yrs = max((pd.Timestamp(tr.entry_time.max()) - pd.Timestamp(tr.entry_time.min())).days / 365.25, 0.5)
    eq = tr.net.cumsum()
    dd = (eq - eq.cummax()).min()
    return dict(label=label, n=len(tr), per_yr=round(len(tr) / yrs, 1), win=round(w.mean() * 100, 1),
                pf=round(gp / gl, 2) if gl > 0 else np.inf, avg_pts=round(tr.net.mean(), 1),
                tot_pts=round(tr.net.sum()), maxdd_pts=round(dd), avgR=round(tr.R.mean(), 3))


def split_stats(tr, cut="2023-01-01", label=""):
    t = pd.to_datetime(tr.entry_time)
    a = stats(tr[t < cut], label + "|IS"); b = stats(tr[t >= cut], label + "|OOS")
    return a, b
