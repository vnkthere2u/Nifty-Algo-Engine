"""
Faithful replay of algo_dashboard.py v3.3 decision logic on NIFTY 1-minute history.

Simulates the live daemon exactly as written:
  * a scan every 5 minutes, 09:15-15:35 IST (signals_live window)
  * each scan sees yfinance-style 15m bars where the LAST bar is still forming
    (built from 1-minute data up to the scan instant)
  * indicators on the forming bar are a one-step Wilder/EMA update from the last
    completed bar (identical to recomputing the whole series)
  * anchor -> 16-minute evaluation window -> entry / expiry, all filters as coded
  * open-trade management on the snapshot price only (no high/low), ratchet stops
  * no end-of-day square-off (positions carry overnight, as in the live code)
"""
import numpy as np
import pandas as pd
from load_data import load_1m, resample_session

A5, A39 = 2 / 6, 2 / 40
N = 14


def precompute_15m(b):
    h, l, c = b.High.values, b.Low.values, b.Close.values
    n = len(c)
    ema5 = np.empty(n); ema39 = np.empty(n); atr = np.empty(n)
    pdm_s = np.empty(n); mdm_s = np.empty(n); adx = np.empty(n)
    ema5[0] = ema39[0] = c[0]; atr[0] = h[0] - l[0]; pdm_s[0] = mdm_s[0] = 0.0; adx[0] = 0.0
    for i in range(1, n):
        ema5[i] = A5 * c[i] + (1 - A5) * ema5[i - 1]
        ema39[i] = A39 * c[i] + (1 - A39) * ema39[i - 1]
        tr = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
        atr[i] = (tr + (N - 1) * atr[i - 1]) / N
        up, dn = h[i] - h[i - 1], l[i - 1] - l[i]
        pdm = up if (up > dn and up > 0) else 0.0
        mdm = dn if (dn > up and dn > 0) else 0.0
        pdm_s[i] = (pdm + (N - 1) * pdm_s[i - 1]) / N
        mdm_s[i] = (mdm + (N - 1) * mdm_s[i - 1]) / N
        pdi, mdi = 100 * pdm_s[i] / atr[i], 100 * mdm_s[i] / atr[i]
        dx = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) > 0 else 0.0
        adx[i] = (dx + (N - 1) * adx[i - 1]) / N
    d = dict(o=b.Open.values, h=h, l=l, c=c, ema5=ema5, ema39=ema39, atr=atr,
             pdm_s=pdm_s, mdm_s=mdm_s, adx=adx)
    # hourly bins (resample('1h').last() on 15m bars) and EMA39 over completed bin closes
    hour_key = b.index.floor("h")
    d["hour_key"] = hour_key.values
    hc = b.Close.groupby(hour_key).last()
    he = np.empty(len(hc)); he[0] = hc.values[0]
    for i in range(1, len(hc)):
        he[i] = A39 * hc.values[i] + (1 - A39) * he[i - 1]
    d["hbin_keys"] = hc.index.values
    d["hbin_ema"] = he
    d["start"] = b.index.values
    return d


def partial_update(d, i, O, H, L, C):
    """Indicators of a forming bar whose previous completed bar is i."""
    ema5 = A5 * C + (1 - A5) * d["ema5"][i]
    ema39 = A39 * C + (1 - A39) * d["ema39"][i]
    tr = max(H - L, abs(H - d["c"][i]), abs(L - d["c"][i]))
    atr = (tr + (N - 1) * d["atr"][i]) / N
    up, dn = H - d["h"][i], d["l"][i] - L
    pdm = up if (up > dn and up > 0) else 0.0
    mdm = dn if (dn > up and dn > 0) else 0.0
    ps = (pdm + (N - 1) * d["pdm_s"][i]) / N
    ms = (mdm + (N - 1) * d["mdm_s"][i]) / N
    pdi, mdi = 100 * ps / atr, 100 * ms / atr
    dx = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) > 0 else 0.0
    adx = (dx + (N - 1) * d["adx"][i]) / N
    return dict(c=C, ema5=ema5, ema39=ema39, atr=atr, adx=adx)


def run(cost_pct=0.0004, verbose=True):
    m1 = load_1m()
    b15 = resample_session(m1, 15)
    d = precompute_15m(b15)
    starts = d["start"]
    mt = m1.index.values
    mo, mh, ml, mc = m1.Open.values, m1.High.values, m1.Low.values, m1.Close.values
    hb_keys, hb_ema = d["hbin_keys"], d["hbin_ema"]

    def row_completed(i):
        return dict(c=d["c"][i], ema5=d["ema5"][i], ema39=d["ema39"][i], atr=d["atr"][i],
                    adx=d["adx"][i], start=starts[i], hour=d["hour_key"][i])

    def htf_ema(row, curr):
        """EMA39_1H value seen on `row` in the live df (bin close = latest available price)."""
        k = np.searchsorted(hb_keys, row["hour"])
        if row["hour"] == curr["hour"]:
            prev_e = hb_ema[k - 1] if k > 0 else curr["c"]
            return A39 * curr["c"] + (1 - A39) * prev_e
        return hb_ema[k]

    days = np.unique(m1.index.normalize().values)
    trades, open_tr = [], None
    anchor, proc_sid, last_closed = None, None, None
    stat = dict(anchors=0, blocked_evals=0)

    for day in days:
        for mins in range(555, 940, 5):
            T = day + np.timedelta64(mins, "m")
            # --- build snapshot -------------------------------------------------
            # completed 15m bars: start + 15m <= T
            ic = np.searchsorted(starts, T - np.timedelta64(15, "m"), side="right") - 1
            if ic < 3:
                continue
            # forming bar: starts[ic+1] <= T  (same day, within session)
            curr = None
            if ic + 1 < len(starts) and starts[ic + 1] <= T and mins <= 929:
                s = starts[ic + 1]
                a = np.searchsorted(mt, s); bpos = np.searchsorted(mt, T)
                if bpos < len(mt) and mt[bpos] == T:
                    O = mo[a]
                    H = max(mh[a:bpos].max(), mo[bpos]) if bpos > a else mo[bpos]
                    L = min(ml[a:bpos].min(), mo[bpos]) if bpos > a else mo[bpos]
                    C = mo[bpos]
                    curr = partial_update(d, ic, O, H, L, C)
                    curr["start"] = s; curr["hour"] = d["hour_key"][ic + 1]
            if curr is None:
                # after 15:30 or data gap: last row is the latest completed bar
                curr = row_completed(ic); last = row_completed(ic - 1); prev = row_completed(ic - 2)
            else:
                last = row_completed(ic); prev = row_completed(ic - 1)
            # data freshness (age of last row start <= 3600s)
            if (T - curr["start"]) > np.timedelta64(3600, "s"):
                continue
            lp = curr["c"]
            otc = 1 if open_tr is not None else 0
            is_15m = (mins % 15 == 0)

            # --- open-trade manager ---------------------------------------------
            if open_tr is not None:
                t = open_tr; av = t["atr"]
                closed = None
                if t["dir"] == 1:
                    if lp >= t["tp"]: closed = "TP"
                    elif lp <= t["sl"]: closed = "SL" if t["sl"] < t["entry"] else "BE/TRAIL"
                    if not closed:
                        pr = (lp - t["entry"]) / av; sr = (t["sl"] - t["entry"]) / av
                        if pr >= 3 and sr < 2: t["sl"] = t["entry"] + 2 * av
                        elif pr >= 2 and sr < 1: t["sl"] = t["entry"] + av
                        elif pr >= 1 and sr < .25: t["sl"] = t["entry"] + .25 * av
                else:
                    if lp <= t["tp"]: closed = "TP"
                    elif lp >= t["sl"]: closed = "SL" if t["sl"] > t["entry"] else "BE/TRAIL"
                    if not closed:
                        pr = (t["entry"] - lp) / av; sr = (t["entry"] - t["sl"]) / av
                        if pr >= 3 and sr < 2: t["sl"] = t["entry"] - 2 * av
                        elif pr >= 2 and sr < 1: t["sl"] = t["entry"] - av
                        elif pr >= 1 and sr < .25: t["sl"] = t["entry"] - .25 * av
                if closed:
                    t.update(exit_time=T, exit=lp, reason=closed)
                    trades.append(t); open_tr = None
                    last_closed = (closed, T)

            # --- anchor creation ------------------------------------------------
            sid = last["start"]
            if anchor is None and is_15m and proc_sid != sid:
                is_long = prev["ema5"] <= prev["ema39"] and last["ema5"] > last["ema39"]
                is_short = prev["ema5"] >= prev["ema39"] and last["ema5"] < last["ema39"]
                if is_long or is_short:
                    anchor = dict(start=T, dir=1 if is_long else -1, atr=last["atr"], sid=sid)
                    stat["anchors"] += 1

            # --- anchor evaluation ----------------------------------------------
            if anchor is not None:
                me = (T - anchor["start"]) / np.timedelta64(1, "m")
                if me <= 16:
                    evc = last if me >= 14 else curr
                    p_adx = prev["adx"] if me >= 14 else last["adx"]
                    l_adx = evc["adx"]
                    htf_bull = evc["c"] > htf_ema(evc, curr)
                    rej = []
                    if last_closed and last_closed[0] == "SL" and (T - last_closed[1]) < np.timedelta64(15, "m"):
                        rej.append("cooldown")
                    if otc: rej.append("open")
                    if l_adx <= 20: rej.append("adx_weak")
                    elif l_adx < p_adx - 1: rej.append("adx_falling")
                    if abs(evc["ema5"] - evc["ema39"]) < 0.15 * anchor["atr"]: rej.append("tangled")
                    if htf_bull != (anchor["dir"] == 1): rej.append("htf")
                    if abs(evc["c"] - evc["ema39"]) > 2.5 * anchor["atr"]: rej.append("overext")
                    if not rej:
                        e = evc["c"]; a_ = anchor["atr"]; dr = anchor["dir"]
                        open_tr = dict(dir=dr, entry_time=T, entry=e, atr=a_,
                                       sl=e - dr * 1.5 * a_, tp=e + dr * 3.75 * a_, adx=l_adx, me=me)
                        proc_sid = anchor["sid"]; anchor = None
                    else:
                        stat["blocked_evals"] += 1
                        if me >= 14:
                            proc_sid = anchor["sid"]; anchor = None
                else:
                    proc_sid = anchor["sid"]; anchor = None

    tr = pd.DataFrame(trades)
    tr["gross_pts"] = tr["dir"] * (tr["exit"] - tr["entry"])
    tr["cost_pts"] = cost_pct * tr["entry"]
    tr["net_pts"] = tr["gross_pts"] - tr["cost_pts"]
    tr["R"] = tr["net_pts"] / (1.5 * tr["atr"])
    tr["hold_days"] = (tr["exit_time"] - tr["entry_time"]) / pd.Timedelta(days=1)
    if verbose:
        report(tr, "v3.3 REPLICA", stat)
    return tr


def report(tr, title, stat=None):
    n = len(tr)
    w = (tr.net_pts > 0).mean() * 100
    wg = (tr.gross_pts > 0).mean() * 100
    pf = tr.net_pts[tr.net_pts > 0].sum() / -tr.net_pts[tr.net_pts <= 0].sum()
    print(f"\n===== {title} =====")
    if stat: print(stat)
    print(f"trades={n}  win%(net)={w:.1f}  win%(gross)={wg:.1f}  PF={pf:.2f}  "
          f"avgR={tr.R.mean():.3f}  total_pts={tr.net_pts.sum():.0f}  avg_hold_days={tr.hold_days.mean():.2f}")
    print(tr.reason.value_counts().to_string())
    tr = tr.copy(); tr["yr"] = pd.to_datetime(tr.entry_time).dt.year
    g = tr.groupby("yr").agg(n=("net_pts", "size"), win=("net_pts", lambda x: (x > 0).mean() * 100),
                             pts=("net_pts", "sum"), avgR=("R", "mean"))
    print(g.round(2).to_string())
    for dname, sub in tr.groupby("dir"):
        print(f"dir {dname:+d}: n={len(sub)} win={(sub.net_pts>0).mean()*100:.1f}% pts={sub.net_pts.sum():.0f}")


if __name__ == "__main__":
    tr = run()
    tr.to_csv("../data/replica_v33_trades.csv", index=False)
