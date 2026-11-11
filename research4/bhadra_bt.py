"""Backtest: Bhadra-end 15-minute range breakout on NIFTY and BANK NIFTY.

Rules (as specified):
  * Bhadra (Vishti karana) periods computed from DE421 (bhadra.py).
  * Keep only periods whose END falls on a trading day inside market hours (needs a complete
    15-min candle before the end: end >= 09:30, and a later candle to trade: end < 15:30).
  * Range candle = the last complete 15-min candle ending at or before Bhadra end
    (end 12:35 -> candle 12:15-12:30). Mark its high/low.
  * First later 15-min candle that CLOSES above the high -> LONG (SL = range low);
    closes below the low -> SHORT (SL = range high). One trade per Bhadra.
  * TP = +100 pts NIFTY / +200 pts BANK NIFTY from entry.
Variants: entry at breakout-candle close (default) or next-candle open; exit at 15:25 (default)
or hold until TP/SL (overnight, gaps filled at the open). Control: same mechanics on random
non-Bhadra days at the same candle times.
"""
import os, sys
import numpy as np
import pandas as pd

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
COST_PCT = 0.0004            # futures round trip incl. slippage (as fraction of price)


def load_nifty_1m():
    sys.path.insert(0, os.path.join(ROOT, "research"))
    os.environ.setdefault("NIFTY_DATA_DIR", os.path.join(ROOT, "data", "raw"))
    from load_data import load_1m
    return load_1m()


def load_bank_5m():
    b = pd.read_pickle(os.path.join(ROOT, "data", "m5", "NIFTY_BANK.pkl"))[["Open", "High", "Low", "Close"]]
    b = b[b.index >= "2016-01-01"]
    m = b.index.hour * 60 + b.index.minute
    return b[(m >= 555) & (m <= 925)]


def candles15(base):
    return base.resample("15min", origin="start_day", offset="15min").agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()


def events(bhadra, trading_days):
    ev = []
    for s, e in zip(bhadra.start, bhadra.end):
        d = e.normalize()
        if d not in trading_days:
            continue
        em = e.hour * 60 + e.minute + e.second / 60
        if em < 570 or em >= 930:           # need candle 09:15-09:30 complete; nothing to trade after 15:30
            continue
        cend = 555 + int((em - 555) // 15) * 15
        if cend >= 930:
            continue
        ev.append(dict(bhadra_start=s, bhadra_end=e, day=d, cstart=d + pd.Timedelta(minutes=cend - 15),
                       cend=d + pd.Timedelta(minutes=cend)))
    return pd.DataFrame(ev)


def simulate(ev, base, c15, tp_pts, entry_mode="close", exit_mode="eod"):
    bt = base.index.values
    o, h, l, c = (base[x].values for x in ("Open", "High", "Low", "Close"))
    bmin = (base.index.hour * 60 + base.index.minute).values
    bday = base.index.normalize().values
    out = []
    for r in ev.itertuples():
        if r.cstart not in c15.index:
            continue
        rng = c15.loc[r.cstart]
        H, L = rng.High, rng.Low
        later = c15[(c15.index >= r.cend) & (c15.index.normalize() == r.day)]
        sig = None
        for ts, k in later.iterrows():
            if ts + pd.Timedelta(minutes=15) > r.day + pd.Timedelta(minutes=915):   # last entry at 15:15
                break
            if k.Close > H:
                sig = (1, ts, k.Close); break
            if k.Close < L:
                sig = (-1, ts, k.Close); break
        base_row = dict(day=r.day, bhadra_end=r.bhadra_end, range_candle=r.cstart.strftime("%H:%M"),
                        H=H, L=L, width=H - L)
        if sig is None:
            out.append({**base_row, "result": "NO_BREAKOUT"}); continue
        d, ts, cl = sig
        t_entry = ts + pd.Timedelta(minutes=15)          # breakout candle close time
        j = np.searchsorted(bt, t_entry.to_datetime64())
        if j >= len(bt):
            continue
        if entry_mode == "close":
            e = cl
        else:
            if bday[j] != np.datetime64(r.day) or bmin[j] >= 925:
                out.append({**base_row, "result": "NO_FILL"}); continue
            e = o[j]
        sl = L if d == 1 else H
        tp = e + d * tp_pts
        if (d == 1 and e <= sl) or (d == -1 and e >= sl):
            out.append({**base_row, "result": "INVALID"}); continue
        res, xp, xt = None, None, None
        k = j
        while k < len(bt):
            new_day = bday[k] != np.datetime64(r.day)
            if new_day and exit_mode == "eod":
                res, xp, xt = "EOD", c[k - 1], bt[k - 1]; break
            if exit_mode == "eod" and bmin[k] >= 925:
                res, xp, xt = "EOD", o[k], bt[k]; break
            if new_day and k > j and bday[k] != bday[k - 1]:       # overnight gap
                if (d == 1 and o[k] <= sl) or (d == -1 and o[k] >= sl):
                    res, xp, xt = "SL", o[k], bt[k]; break
                if (d == 1 and o[k] >= tp) or (d == -1 and o[k] <= tp):
                    res, xp, xt = "TP", o[k], bt[k]; break
            hit_sl = (l[k] <= sl) if d == 1 else (h[k] >= sl)
            hit_tp = (h[k] >= tp) if d == 1 else (l[k] <= tp)
            if hit_sl:
                res, xp, xt = "SL", sl, bt[k]; break
            if hit_tp:
                res, xp, xt = "TP", tp, bt[k]; break
            k += 1
        if res is None:
            res, xp, xt = "OPEN", c[-1], bt[-1]
        gross = d * (xp - e)
        out.append({**base_row, "result": res, "dir": "LONG" if d == 1 else "SHORT", "entry_time": t_entry,
                    "entry": e, "sl": sl, "tp": tp, "risk_pts": abs(e - sl), "exit": xp, "exit_time": pd.Timestamp(xt),
                    "gross_pts": gross, "net_pts": gross - COST_PCT * e})
    return pd.DataFrame(out)


def summarize(t, label):
    tr = t[t.result.isin(["TP", "SL", "EOD"])]
    if len(tr) == 0:
        return dict(label=label, events=len(t), trades=0)
    w = tr.net_pts > 0
    gp, gl = tr.net_pts[w].sum(), -tr.net_pts[~w].sum()
    return dict(label=label, events=len(t), no_breakout=int((t.result == "NO_BREAKOUT").sum()), trades=len(tr),
                tp_hit_pct=round(100 * (tr.result == "TP").mean(), 1), sl_pct=round(100 * (tr.result == "SL").mean(), 1),
                eod_pct=round(100 * (tr.result == "EOD").mean(), 1), win_pct=round(100 * w.mean(), 1),
                avg_risk_pts=round(tr.risk_pts.mean(), 1), avg_net_pts=round(tr.net_pts.mean(), 1),
                total_net_pts=round(tr.net_pts.sum()), pf=round(gp / gl, 2) if gl > 0 else np.inf,
                long_win=round(100 * (tr[tr.dir == "LONG"].net_pts > 0).mean(), 1),
                short_win=round(100 * (tr[tr.dir == "SHORT"].net_pts > 0).mean(), 1),
                per_year=round(len(tr) / max((tr.day.max() - tr.day.min()).days / 365.25, 0.5), 1))
