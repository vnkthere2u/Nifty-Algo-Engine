"""NIFTY-100 stock research core: clean 5-minute data, multi-timeframe features, event simulator.

Trade geometry (user spec): SL = 1 x ATR, TP = 1.5 x ATR (ATR of the strategy's timeframe).
Win = TP hit OR break-even exit (break-evens reported separately). Long only.
Execution on 5-minute bars, entries/exits intraday the moment conditions trigger.
"""
import os
import numpy as np
import pandas as pd
from numba import njit

D = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "m5")
INDICES = {"NIFTY_50", "NIFTY_BANK", "NIFTY_CONSUMPTION", "NIFTY_ENERGY", "INDIA_VIX", "INDIA VIX"}
LISTING = {"IRCTC": "2019-10-14", "IRFC": "2021-01-29", "HDFCLIFE": "2017-11-17", "SBILIFE": "2017-10-03",
           "LICI": "2022-05-17", "JIOFIN": "2023-08-21"}
START = "2016-01-01"
CUT = pd.Timestamp("2022-01-01")       # in-sample < CUT <= out-of-sample
COST_INTRA, COST_DELIV = 0.0012, 0.0035


def stocks():
    return sorted(os.path.basename(f)[:-4] for f in os.listdir(D)
                  if f.endswith(".pkl") and f[:-4] not in INDICES)


def load5(sym):
    b = pd.read_pickle(os.path.join(D, f"{sym}.pkl"))
    b = b[b.index >= max(pd.Timestamp(START), pd.Timestamp(LISTING.get(sym, START)))]
    b = b[(b[["Open", "High", "Low", "Close"]] > 0).all(axis=1)]
    med = b.Close.rolling(301, center=True, min_periods=50).median()
    for c in ("Open", "High", "Low", "Close"):
        b = b[(b[c] / med - 1).abs() <= 0.15]
        med = med.reindex(b.index)
    return b


def rma(s, n):
    return s.ewm(alpha=1 / n, adjust=False).mean()


def atr(df, n=14):
    pc = df.Close.shift()
    tr = pd.concat([df.High - df.Low, (df.High - pc).abs(), (df.Low - pc).abs()], axis=1).max(axis=1)
    return rma(tr, n)


def rsi(c, n=2):
    d = c.diff()
    up, dn = rma(d.clip(lower=0), n), rma(-d.clip(upper=0), n)
    return (100 - 100 / (1 + up / dn.replace(0, np.nan))).fillna(100)


def resample(b, rule):
    return b.resample(rule, origin="start_day", offset="15min").agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna(subset=["Close"])


def daily(b):
    d = b.resample("1D").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna()
    d["atr"] = atr(d)
    for n in (20, 50):
        d[f"ema{n}"] = d.Close.ewm(span=n, adjust=False).mean()
    d["sma50"], d["sma200"] = d.Close.rolling(50).mean(), d.Close.rolling(200).mean()
    d["rsi2"] = rsi(d.Close)
    d["hh252"] = d.High.rolling(252).max()
    d["hh20"], d["ll20"] = d.High.rolling(20).max(), d.Low.rolling(20).min()
    d["vol20"] = d.Volume.rolling(20).mean()
    rng = d.High - d.Low
    d["nr7"] = rng <= rng.rolling(7).min()
    d["inside"] = (d.High <= d.High.shift()) & (d.Low >= d.Low.shift())
    d["trend_up"] = (d.Close > d.ema20) & (d.ema20 > d.ema50)
    return d


class Ctx:
    """All arrays a strategy needs, aligned to the 5-minute grid of one stock."""

    def __init__(self, sym, market=None):
        b = load5(sym)
        self.sym, self.b = sym, b
        self.t = b.index
        self.day = b.index.normalize()
        self.mins = (b.index.hour * 60 + b.index.minute).values
        dcodes, self.day_id = np.unique(self.day.values, return_inverse=True)
        self.days = pd.DatetimeIndex(dcodes)
        self.d = daily(b)
        # previous-day daily features broadcast to each 5m bar
        prev = self.d.shift(1).reindex(self.days)
        self.prev = prev.iloc[self.day_id].set_index(b.index)
        # 15-minute bars & features; value available at close of the 15m bar's last 5m bar
        m15 = resample(b, "15min")
        m15["atr"] = atr(m15)
        m15["ema20"], m15["ema50"] = m15.Close.ewm(span=20, adjust=False).mean(), m15.Close.ewm(span=50, adjust=False).mean()
        m15["rsi2"] = rsi(m15.Close)
        m15["end"] = m15.index + pd.Timedelta(minutes=10)      # start of last 5m bar inside
        self.m15 = m15
        h1 = resample(b, "60min")
        h1["ema20"] = h1.Close.ewm(span=20, adjust=False).mean()
        h1["end"] = h1.index + pd.Timedelta(minutes=55)
        # last COMPLETED 15m/1h values as-of each 5m bar close
        self.a15 = self._asof(m15, ["atr", "ema20", "ema50", "rsi2", "Close", "Low", "High", "Open"])
        self.a1h = self._asof(h1, ["ema20", "Close"])
        # intraday VWAP
        tp = (b.High + b.Low + b.Close) / 3
        pv = (tp * b.Volume).groupby(self.day_id).cumsum()
        vv = b.Volume.groupby(self.day_id).cumsum().replace(0, np.nan)
        self.vwap = (pv / vv).values
        self.market = market

    def _asof(self, tf, cols):
        x = tf[["end"] + cols].copy()
        x = x.set_index("end")
        x = x[~x.index.duplicated(keep="last")]
        return x.reindex(self.t, method="ffill")

    def idx_at(self, times):
        return np.searchsorted(self.t.values, np.asarray(times, dtype="datetime64[ns]"))


@njit(cache=True)
def _sim(o, h, l, c, day_id, mins, s_bar, s_kind, s_level, s_valid, s_atr, s_intra, be_on, eod_min,
         cost_i, cost_d, max_risk):
    n, m = len(c), len(s_bar)
    out = np.zeros((m, 9))
    k = 0
    busy = -1
    for q in range(m):
        i = s_bar[q]
        if i <= busy or i + 1 >= n:
            continue
        j = i + 1
        if s_intra[q] and (day_id[j] != day_id[i] or mins[j] >= eod_min):
            continue
        # ---- entry
        e = -1.0
        if s_kind[q] == 0:
            e = o[j]
        else:
            lv = s_level[q]
            while j <= s_valid[q] and j < n:
                if s_intra[q] and mins[j] >= eod_min:
                    break
                if h[j] >= lv:
                    e = max(o[j], lv)
                    break
                j += 1
            if e < 0:
                continue
        A = s_atr[q]
        if A <= 0 or A / e > max_risk:
            continue
        sl = e - A
        tp = e + 1.5 * A
        ent = j
        be_done = False
        reason = 0
        xp = 0.0
        while True:
            if j >= n:
                j = n - 1; xp = c[j]; reason = 9; break
            if j > ent and day_id[j] != day_id[j - 1]:
                if s_intra[q]:
                    j -= 1; xp = c[j]; reason = 4; break
                if o[j] <= sl:
                    xp = o[j]; reason = 1; break
                if o[j] >= tp:
                    xp = o[j]; reason = 2; break
            if s_intra[q] and mins[j] >= eod_min and j > ent:
                xp = o[j]; reason = 4; break
            if l[j] <= sl:
                xp = sl; reason = 1; break
            if h[j] >= tp:
                xp = tp; reason = 2; break
            if be_on and not be_done and h[j] >= e + A:
                sl = e * (1.0 + (cost_i if s_intra[q] else cost_d) + 0.0005)
                be_done = True
            j += 1
        if reason == 1 and sl > e:
            reason = 3
        same_day = day_id[j] == day_id[ent]
        cost = cost_i if same_day else cost_d
        out[k, 0] = ent; out[k, 1] = j; out[k, 2] = e; out[k, 3] = xp; out[k, 4] = reason
        out[k, 5] = A; out[k, 6] = cost; out[k, 7] = q; out[k, 8] = 1.0 if same_day else 0.0
        k += 1
        busy = j
    return out[:k]


REASON = {1: "SL", 2: "TP", 3: "BE", 4: "TIME", 9: "OPEN"}


def simulate(ctx, sig, be_on=False, eod_min=920):
    """sig: DataFrame with columns bar, kind, level, valid, atr, intra (one row per signal)."""
    if sig is None or len(sig) == 0:
        return pd.DataFrame()
    sig = sig.sort_values("bar")
    b = ctx.b
    r = _sim(b.Open.values, b.High.values, b.Low.values, b.Close.values, ctx.day_id.astype(np.int64),
             ctx.mins.astype(np.int64), sig.bar.values.astype(np.int64), sig.kind.values.astype(np.int64),
             sig.level.values.astype(np.float64), sig.valid.values.astype(np.int64), sig.atr.values.astype(np.float64),
             sig.intra.values.astype(np.bool_), be_on, eod_min, COST_INTRA, COST_DELIV, 0.10)
    if len(r) == 0:
        return pd.DataFrame()
    t = pd.DataFrame({"entry_time": ctx.t[r[:, 0].astype(int)], "exit_time": ctx.t[r[:, 1].astype(int)],
                      "entry": r[:, 2], "exit": r[:, 3], "reason": [REASON[int(x)] for x in r[:, 4]],
                      "atr": r[:, 5], "cost": r[:, 6], "intraday": r[:, 8] > 0})
    t["ret"] = t.exit / t.entry - 1 - t.cost
    # drawdown basis: SL losses booked at the planned stop (gap slippage ignored, per user rule)
    t["ret_plan"] = np.where(t.reason == "SL", -t.atr / t.entry - t.cost, t.ret)
    return t[t.reason != "OPEN"]


def score(t, cap=100_000.0):
    if t is None or len(t) == 0:
        return dict(n=0)
    n = len(t)
    tp, be, sl, tm = [(t.reason == r).sum() for r in ("TP", "BE", "SL", "TIME")]
    wins = tp + be
    g = t.ret[t.ret > 0].sum(); ls = -t.ret[t.ret <= 0].sum()
    eq = (t.ret_plan * cap).cumsum()
    dd = (eq - np.maximum.accumulate(np.r_[0, eq.values])[1:]).min()
    yrs = max((t.exit_time.max() - t.entry_time.min()).days / 365.25, 0.25)
    return dict(n=n, win=round(100 * wins / n, 1), tp_pct=round(100 * tp / n, 1), be_pct=round(100 * be / n, 1),
                time_pct=round(100 * tm / n, 1), pf=round(g / ls, 2) if ls > 0 else np.inf,
                exp_pct=round(100 * t.ret.mean(), 3), pnl_inr=round(t.ret.sum() * cap),
                inr_yr=round(t.ret.sum() * cap / yrs), maxdd_pct=round(100 * min(dd, 0) / cap, 1),
                per_yr=round(n / yrs, 1), intraday_share=round(100 * t.intraday.mean(), 0))
