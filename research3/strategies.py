"""Signal generators. Each returns a DataFrame: bar, kind(0=market next bar,1=buy-stop), level, valid, atr, intra.
'bar' is the 5-minute bar at whose CLOSE the signal is known (no look-ahead)."""
import numpy as np
import pandas as pd


def _frame(bar, kind, level, valid, atr, intra):
    s = pd.DataFrame({"bar": np.asarray(bar, np.int64), "kind": kind, "level": np.asarray(level, float),
                      "valid": np.asarray(valid, np.int64), "atr": np.asarray(atr, float), "intra": intra})
    return s[np.isfinite(s.atr) & (s.atr > 0) & np.isfinite(s.level)]


def _day_pos(ctx):
    return pd.Series(np.arange(len(ctx.t)), index=ctx.t).groupby(ctx.day_id).cumcount().values


def _bar_at(ctx, day_idx, minute):
    """index of last bar of day `day_idx` with start minute <= minute."""
    tm = ctx.days[day_idx] + pd.to_timedelta(minute, unit="m")
    return np.searchsorted(ctx.t.values, tm.values, side="right") - 1


# ------------------------------------------------------------------ intraday
def i1_orb(ctx, k=3, rv=1.5):
    b, pos = ctx.b, _day_pos(ctx)
    sig = np.flatnonzero(pos == k - 1)
    sig = sig[ctx.mins[sig] == 555 + 5 * (k - 1)]
    g = pd.DataFrame({"h": b.High.values, "v": b.Volume.values, "d": ctx.day_id, "p": pos})
    g = g[g.p < k].groupby("d").agg(orh=("h", "max"), orv=("v", "sum"))
    g["avg"] = g.orv.shift(1).rolling(20).mean()
    d = ctx.day_id[sig]
    orh, orv, avg = g.orh.reindex(d).values, g.orv.reindex(d).values, g.avg.reindex(d).values
    ok = ctx.prev.trend_up.values[sig].astype(bool) & (orv >= rv * avg)
    sig = sig[ok]
    return _frame(sig, 1, orh[ok] * 1.0005, _bar_at(ctx, ctx.day_id[sig], 775), ctx.a15.atr.values[sig], True)


def i2_vwap_pb(ctx):
    b, m = ctx.b, ctx.mins
    v = ctx.vwap
    at10 = pd.Series(np.where(m == 595, (b.Close.values > v).astype(float), np.nan)).groupby(ctx.day_id).transform("max").values
    cond = ((m >= 600) & (m <= 840) & (b.Low.values <= v) & (b.Close.values > v) & (b.Close.values > b.Open.values)
            & (at10 == 1) & ctx.prev.trend_up.values.astype(bool))
    idx = np.flatnonzero(cond)
    first = pd.Series(idx).groupby(ctx.day_id[idx]).first().values
    return _frame(first, 0, b.Close.values[first], first, ctx.a15.atr.values[first], True)


def i3_gap(ctx, mode="cont"):
    b, pos = ctx.b, _day_pos(ctx)
    sig = np.flatnonzero((pos == 2) & (ctx.mins == 565))
    st = sig - 2
    o = b.Open.values[st]; hi = np.maximum.reduce([b.High.values[st + q] for q in range(3)])
    lo = np.minimum.reduce([b.Low.values[st + q] for q in range(3)]); cl = b.Close.values[sig]
    pc = ctx.prev.Close.values[sig]
    gap = o / pc - 1
    green = (cl > o) & ((cl - lo) >= 0.5 * (hi - lo))
    ok = green & ((gap >= 0.01) if mode == "cont" else (gap <= -0.01))
    sig = sig[ok]
    return _frame(sig, 1, hi[ok] * 1.0005, _bar_at(ctx, ctx.day_id[sig], 655), ctx.a15.atr.values[sig], True)


def i4_ema_pb(ctx):
    a, h1, p, m = ctx.a15, ctx.a1h, ctx.prev, ctx.mins
    last5 = ((m - 555) % 15 == 10) & (m >= 580) & (m <= 865)
    cond = (last5 & (a.ema20.values > a.ema50.values) & (h1.Close.values > h1.ema20.values)
            & (p.Close.values > p.ema50.values) & (a.Low.values <= a.ema20.values) & (a.Close.values > a.ema20.values))
    idx = np.flatnonzero(cond)
    return _frame(idx, 0, ctx.b.Close.values[idx], idx, a.atr.values[idx], True)


def i5_rsi_15m(ctx):
    a, p, m = ctx.a15, ctx.prev, ctx.mins
    last5 = ((m - 555) % 15 == 10) & (m >= 580) & (m <= 865)
    idx = np.flatnonzero(last5 & (a.rsi2.values < 10) & (p.Close.values > p.sma50.values))
    return _frame(idx, 0, ctx.b.Close.values[idx], idx, a.atr.values[idx], True)


def i6_rs_leader(ctx, thr=0.0075):
    if ctx.market is None:
        return _frame([], 1, [], [], [], True)
    b, pos = ctx.b, _day_pos(ctx)
    sig = np.flatnonzero(ctx.mins == 625)
    day_open = pd.Series(b.Open.values).groupby(ctx.day_id).transform("first").values
    mh = pd.Series(b.High.values).groupby(ctx.day_id).cummax().values
    nf = ctx.market["n5"].reindex(ctx.t)
    nopen = nf.Open.groupby(ctx.day_id).transform("first").values
    rs = (b.Close.values / day_open - 1) - (nf.Close.values / nopen - 1)
    ok = (rs[sig] >= thr) & (ctx.prev.Close.values[sig] > ctx.prev.ema50.values[sig])
    sig = sig[ok]
    return _frame(sig, 1, mh[sig] * 1.0005, _bar_at(ctx, ctx.day_id[sig], 835), ctx.a15.atr.values[sig], True)


# ------------------------------------------------------------------ swing (daily setup, intraday trigger next day)
def _swing(ctx, mask, level):
    d = ctx.d
    days = np.flatnonzero(mask.reindex(ctx.days).fillna(False).values)
    days = days[days + 1 < len(ctx.days)]
    sig = _bar_at(ctx, days, 1000)                  # last bar of signal day
    valid = _bar_at(ctx, days + 1, 1000)            # buy-stop valid through next session
    lv = level.reindex(ctx.days).values[days]
    at = d.atr.reindex(ctx.days).values[days]
    return _frame(sig, 1, lv, valid, at, False)


def d1_trend_pb(ctx):
    d = ctx.d
    m = (d.ema50 > d.sma200) & (d.Close > d.sma200) & (d.Low.rolling(3).min() <= d.ema20) & (d.Close > d.ema20)
    return _swing(ctx, m, d.High * 1.001)


def d2_base_bo(ctx):
    d = ctx.d
    m = (d.Close >= 0.95 * d.hh252) & ((d.hh20 - d.ll20) / d.Close <= 0.12)
    return _swing(ctx, m, d.hh20 * 1.001)


def d3_nr7_ib(ctx):
    d = ctx.d
    m = d.nr7 & d.inside & (d.Close > d.ema50) & (d.ema50 > d.sma200)
    return _swing(ctx, m, d.High * 1.001)


def d4_rsi2(ctx):
    d = ctx.d
    return _swing(ctx, (d.rsi2 < 10) & (d.Close > d.sma200), d.Close * 1.001)


def d5_post_gap(ctx):
    d = ctx.d
    gap = d.Open / d.Close.shift() - 1
    m = (gap >= 0.03) & ((d.Close - d.Low) >= 0.5 * (d.High - d.Low)) & (d.Volume >= 2 * d.vol20.shift())
    return _swing(ctx, m, d.High * 1.001)


BASE = {
    "I1_ORB15_rv1.0": lambda c: i1_orb(c, 3, 1.0), "I1_ORB15_rv1.5": lambda c: i1_orb(c, 3, 1.5),
    "I1_ORB30_rv1.0": lambda c: i1_orb(c, 6, 1.0), "I1_ORB30_rv1.5": lambda c: i1_orb(c, 6, 1.5),
    "I2_VWAP_PB": i2_vwap_pb,
    "I3_GAP_CONT": lambda c: i3_gap(c, "cont"), "I3_GAP_FILL": lambda c: i3_gap(c, "fill"),
    "I4_EMA20_PB_15m": i4_ema_pb, "I5_RSI2_15m": i5_rsi_15m,
    "I6_RS_0.75": lambda c: i6_rs_leader(c, 0.0075), "I6_RS_1.25": lambda c: i6_rs_leader(c, 0.0125),
    "D1_TREND_PB": d1_trend_pb, "D2_BASE_BO": d2_base_bo, "D3_NR7_IB": d3_nr7_ib,
    "D4_RSI2": d4_rsi2, "D5_POST_GAP": d5_post_gap,
}
