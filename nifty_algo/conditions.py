"""Live condition snapshots for the dashboard: every rule the strategy checks, its current value,
the required value, pass/fail, and the price levels at which entry/exit would trigger.
Computed by the engine on every run and stored in the journal (kv key 'conditions')."""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from .bars import FEATURE_WINDOW
from .indicators import rsi
from .strategy import compute_features, evaluate_entry


def _pct(level, price):
    return None if level is None or price in (None, 0) else round(100 * (level / price - 1), 2)


def _rule(name, current, required, ok, note=""):
    return {"rule": name, "current": current, "required": required, "ok": bool(ok), "note": note}


def _rsi_trigger_price(closes: pd.Series, n: int, threshold: float) -> float | None:
    """Highest closing price for today at which RSI(n) would be below `threshold` (bisection; RSI rises with price)."""
    base = closes.iloc[:-1]

    def r(p):
        return float(rsi(pd.concat([base, pd.Series([p])], ignore_index=True), n).iloc[-1])

    last = float(closes.iloc[-1])
    lo, hi = last * 0.7, last * 1.3
    if r(lo) >= threshold:
        return None
    if r(hi) < threshold:
        return hi
    for _ in range(40):
        mid = (lo + hi) / 2
        if r(mid) < threshold:
            lo = mid
        else:
            hi = mid
    return lo


# ---------------------------------------------------------------- NIFTY
def nifty_conditions(cfg, snap, position: dict | None, now: datetime) -> dict:
    p = cfg.strategy
    daily = snap.daily
    if snap.today is not None:
        frame = pd.concat([daily.tail(FEATURE_WINDOW - 1), snap.today.to_frame().T]).astype(float)
        source = f"live session ({snap.as_of:%H:%M} IST)" if snap.as_of else "live session"
        session = "open" if snap.as_of and (now - snap.as_of).total_seconds() < 900 else "closed (last session)"
    else:
        frame = daily.tail(FEATURE_WINDOW).astype(float)
        source, session = f"last completed day {frame.index[-1]:%d-%b}", "closed"
    f = compute_features(frame, p)
    row = f.iloc[-1]
    price = float(row.Close)
    H, L = float(row.High), float(row.Low)
    ibs_px = L + p.ibs_entry * (H - L)
    rsi_px = _rsi_trigger_price(frame.Close, p.rsi_len, p.rsi_entry)
    entry_px = max([x for x in (ibs_px, rsi_px) if x is not None], default=None)
    dec = evaluate_entry(row, p, len(frame))
    rules = [
        _rule(f"Trend: price > SMA{p.trend_sma}", round(price, 2), f"> {row.sma_trend:,.2f}", price > row.sma_trend,
              f"{_pct(price, row.sma_trend):+.2f}% vs SMA"),
        _rule(f"Setup A: RSI({p.rsi_len}) < {p.rsi_entry:g}", round(float(row.rsi), 1), f"< {p.rsi_entry:g}", row.rsi < p.rsi_entry,
              f"true if price ≤ {rsi_px:,.2f}" if rsi_px else "not reachable within ±30%"),
        _rule(f"Setup B: IBS < {p.ibs_entry:g}", round(float(row.ibs), 2), f"< {p.ibs_entry:g}", row.ibs < p.ibs_entry,
              f"true if price ≤ {ibs_px:,.2f} (day range {L:,.2f}–{H:,.2f})"),
    ]
    levels = [
        {"level": f"SMA{p.trend_sma} (trend floor)", "price": round(float(row.sma_trend), 2), "dist_pct": _pct(row.sma_trend, price)},
        {"level": "Entry trigger (RSI or IBS met)", "price": round(entry_px, 2) if entry_px else None, "dist_pct": _pct(entry_px, price)},
        {"level": "IBS trigger", "price": round(ibs_px, 2), "dist_pct": _pct(ibs_px, price)},
        {"level": "RSI(2) trigger", "price": round(rsi_px, 2) if rsi_px else None, "dist_pct": _pct(rsi_px, price)},
    ]
    pos = None
    if position:
        exit_lvl = float(row.prev_high) if not pd.isna(row.prev_high) else None
        pos = {"entry": position["entry_price"], "entry_date": position["entry_date"],
               "pnl_pct": round(100 * (price / position["entry_price"] - 1), 2),
               "days_held": int(position.get("days_held") or 0), "time_stop_days": p.max_hold_days}
        levels = [
            {"level": "EXIT target: 15:25 price > previous day's high", "price": round(exit_lvl, 2) if exit_lvl else None, "dist_pct": _pct(exit_lvl, price)},
            {"level": "Entry price", "price": round(position["entry_price"], 2), "dist_pct": _pct(position["entry_price"], price)},
            {"level": "Disaster stop (8×ATR)", "price": round(position["disaster_stop"], 2) if position.get("disaster_stop") else None,
             "dist_pct": _pct(position.get("disaster_stop"), price)},
        ]
        rules = [_rule("Exit: price > previous day's high", round(price, 2), f"> {exit_lvl:,.2f}" if exit_lvl else "n/a",
                       exit_lvl is not None and price > exit_lvl, "checked at 15:25 on days after entry"),
                 _rule("Time stop", f"day {pos['days_held']}", f"≥ {p.max_hold_days} days", pos["days_held"] >= p.max_hold_days)]
    verdict = ("IN POSITION — " + ("exit condition met now" if rules[0]["ok"] else "holding")) if position else \
        ("ENTRY CONDITIONS MET NOW (BUY if still true at 15:25)" if dec.take else "no entry: " + "; ".join(dec.reasons))
    return {"updated": now.isoformat(timespec="seconds"), "price": round(price, 2), "source": source, "session": session,
            "day_high": round(H, 2), "day_low": round(L, 2), "verdict": verdict, "rules": rules, "levels": levels,
            "position": pos, "decision_time": "15:25 IST"}


# ---------------------------------------------------------------- BTC
def btc_conditions(f: pd.DataFrame, today: pd.Timestamp, pos, pending: dict | None, p, now: datetime) -> dict:
    live = today in f.index
    row = f.loc[today] if live else f.iloc[-1]
    price = float(row.Close)
    risk = p.stop_atr * float(row.atr) / price
    trend = bool(row.ema_f > row.ema_m > row.ema_s)
    bo = float(row.hh_prev)
    rules = [
        _rule("Trend: EMA20 > EMA50 > EMA200", f"{row.ema_f:,.0f} / {row.ema_m:,.0f} / {row.ema_s:,.0f}", "fast > mid > slow", trend),
        _rule("Breakout: close > prior 20-day high", f"{price:,.0f}", f"> {bo:,.0f}", price > bo, f"{_pct(bo, price):+.2f}% away"),
        _rule("Volatility gate: stop (3.5×ATR) ≤ 10%", f"{100 * risk:.1f}%", "≤ 10%", risk <= p.max_risk,
              "skips trades in chaotic markets"),
    ]
    levels = [{"level": "Breakout level (prior 20-day high)", "price": round(bo), "dist_pct": _pct(bo, price)},
              {"level": "EMA20", "price": round(float(row.ema_f)), "dist_pct": _pct(row.ema_f, price)},
              {"level": "EMA50", "price": round(float(row.ema_m)), "dist_pct": _pct(row.ema_m, price)},
              {"level": "EMA200", "price": round(float(row.ema_s)), "dist_pct": _pct(row.ema_s, price)}]
    position = None
    if pos is not None:
        be_trig = pos.entry + p.be_r * pos.D
        position = {"entry": round(pos.entry), "entry_date": pos.entry_date, "pnl_pct": round(100 * (price / pos.entry - 1), 2),
                    "break_even_active": pos.sl >= pos.entry}
        levels = [{"level": "Target (2R)", "price": round(pos.tp), "dist_pct": _pct(pos.tp, price)},
                  {"level": "Break-even trigger (+1R)", "price": round(be_trig), "dist_pct": _pct(be_trig, price)},
                  {"level": "Entry", "price": round(pos.entry), "dist_pct": _pct(pos.entry, price)},
                  {"level": "Stop (daily-close basis)" + (" — at break-even" if pos.sl >= pos.entry else ""),
                   "price": round(pos.sl), "dist_pct": _pct(pos.sl, price)},
                  {"level": "Hard stop (intraday, −10%)", "price": round(pos.hard), "dist_pct": _pct(pos.hard, price)}]
        rules = [_rule("Target hit (intraday)", f"{price:,.0f}", f"≥ {pos.tp:,.0f}", price >= pos.tp),
                 _rule("Stop: UTC daily close ≤ stop", f"{price:,.0f}", f"≤ {pos.sl:,.0f}", price <= pos.sl, "judged at the daily close"),
                 _rule("Break-even armed", "yes" if pos.sl >= pos.entry else "no", f"high ≥ {be_trig:,.0f}", pos.sl >= pos.entry)]
        verdict = "IN POSITION"
    elif pending:
        verdict = f"SIGNAL on {pending['signal_day']} — paper BUY at the {pending['fill_day']} UTC open"
    else:
        met = all(r["ok"] for r in rules)
        verdict = "ENTRY CONDITIONS MET NOW (BUY at next UTC open if still true at the 05:30 IST close)" if met else \
            "no entry: " + "; ".join(r["rule"] for r in rules if not r["ok"])
    return {"updated": now.isoformat(timespec="seconds"), "price": round(price), "usdinr": round(float(row.get("usdinr", np.nan)), 2)
            if "usdinr" in row and not pd.isna(row.get("usdinr")) else None,
            "source": "today's UTC bar so far" if live else "last completed UTC day", "verdict": verdict,
            "rules": rules, "levels": levels, "position": position, "decision_time": "05:30 IST (00:00 UTC daily close)"}
