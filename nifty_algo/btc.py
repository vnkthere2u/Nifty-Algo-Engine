"""BTC low-volatility trend breakout (validated in research2/, registered in strategies/registry.json).

Rules (daily bars, UTC days, prices in INR = BTC-USD x USD/INR):
  signal at a completed day's close: EMA20 > EMA50 > EMA200 and Close > highest High of the previous 20 days
  entry : next day's open;  D = 3.5 x ATR14 (signal day);  skip if D > 10 % of entry (volatility gate)
  stops : closing-basis stop at entry - D (exit at that day's close) + hard intraday stop at -10 %
  target: entry + 2D (intraday);  break-even: once the high reaches entry + 1D, stop -> entry x 1.007
  gaps  : opening beyond stop/target fills at the open
The bar-step function below is a line-by-line port of research2/sim.py so live == backtest.
"""
from __future__ import annotations

import json
import logging
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
import requests

from .confidence import confidence
from .config import IST, state_dir
from .feeds import FeedError, YahooFeed
from .journal import Journal
from .notify import Notifier, esc

log = logging.getLogger("nifty_algo.btc")
UTC = timezone.utc


@dataclass(frozen=True)
class BtcParams:
    ema_fast: int = 20
    ema_mid: int = 50
    ema_slow: int = 200
    breakout_lookback: int = 20
    atr_len: int = 14
    stop_atr: float = 3.5
    target_r: float = 2.0
    be_r: float = 1.0
    max_risk: float = 0.10
    cost_pct: float = 0.006          # round trip (Indian exchange fees + slippage); tax excluded
    capital: float = 100000.0

    @property
    def be_lock(self) -> float:
        return self.cost_pct + 0.001


# backtest record 2015-2026 (research2): 23 wins / 4 losses
BTC_PRIORS = {"ALL": {"wins": 23, "losses": 4}, "BREAKOUT": {"wins": 23, "losses": 4}}


# ------------------------------------------------------------------ indicators / signal
def features(d: pd.DataFrame, p: BtcParams = BtcParams()) -> pd.DataFrame:
    f = d[["Open", "High", "Low", "Close"]].astype(float).copy()
    pc = f.Close.shift()
    tr = pd.concat([f.High - f.Low, (f.High - pc).abs(), (f.Low - pc).abs()], axis=1).max(axis=1)
    f["atr"] = tr.ewm(alpha=1 / p.atr_len, adjust=False).mean()
    for n, k in ((p.ema_fast, "ema_f"), (p.ema_mid, "ema_m"), (p.ema_slow, "ema_s")):
        f[k] = f.Close.ewm(span=n, adjust=False).mean()
    f["sma_s"] = f.Close.rolling(p.ema_slow).mean()          # warm-up guard (research required 200 bars)
    f["hh_prev"] = f.High.rolling(p.breakout_lookback).max().shift(1)
    f["signal"] = ((f.ema_f > f.ema_m) & (f.ema_m > f.ema_s) & (f.Close > f.hh_prev) & f.sma_s.notna()).astype(bool)
    return f


@dataclass
class Pos:
    entry_date: str
    entry: float
    D: float
    sl: float
    tp: float
    hh: float
    first_done: bool = False   # entry day's completed bar processed

    @property
    def hard(self) -> float:
        return self.entry * (1 - BtcParams().max_risk)


def open_pos(entry_date: str, e: float, D: float, p: BtcParams) -> Optional[Pos]:
    if D <= 0 or D / e > p.max_risk:
        return None
    return Pos(entry_date, e, D, e - D, e + p.target_r * D, e)


def step_bar(ps: Pos, o, h, l, c, first: bool, completed: bool, p: BtcParams):
    """Port of research2/sim.py inner loop for ONE bar. Returns (exit_price, reason) or None.
    For a partial (today's) bar only intraday-observable checks run; close-stop and BE update
    wait for the completed bar, exactly as in the backtest."""
    act = ps.hard if ps.sl < ps.entry else ps.sl
    if not first:
        if o <= act:
            return o, ("SL" if act < ps.entry else "BE")
        if o >= ps.tp:
            return o, "TP"
    if l <= act:
        return act, ("SL" if act < ps.entry else "BE")
    if h >= ps.tp:
        return ps.tp, "TP"
    if not completed:
        return None
    if ps.sl < ps.entry and c <= ps.sl:
        return c, "SL"
    ps.hh = max(ps.hh, h)
    if p.be_r > 0 and ps.hh - ps.entry >= p.be_r * ps.D and ps.sl < ps.entry:
        ps.sl = ps.entry * (1 + p.be_lock)
    return None


# ------------------------------------------------------------------ data
class BtcFeed:
    """Daily BTC in INR. Yahoo BTC-USD x INR=X; Kraken XBTUSD as BTC fallback."""

    def __init__(self):
        self.btc = YahooFeed("BTC-USD", cache_ttl_sec=60)
        self.fx = YahooFeed("INR=X", cache_ttl_sec=600)

    @staticmethod
    def kraken_daily() -> pd.DataFrame:
        try:
            r = requests.get("https://api.kraken.com/0/public/OHLC", params={"pair": "XBTUSD", "interval": 1440}, timeout=(5, 15))
            r.raise_for_status()
            return parse_kraken(r.json())
        except FeedError:
            raise
        except Exception as e:
            raise FeedError(f"kraken fallback failed: {e}") from e

    def daily_inr(self) -> pd.DataFrame:
        try:
            usd = self.btc.daily("5y")
        except FeedError as e:
            log.warning("yahoo BTC failed (%s); trying Kraken", e)
            usd = self.kraken_daily()
        usd = usd[["Open", "High", "Low", "Close"]]
        fx = self.fx.daily("5y")["Close"]
        return to_inr(usd, fx)


def parse_kraken(js: dict) -> pd.DataFrame:
    if js.get("error"):
        raise FeedError(f"kraken error {js['error']}")
    res = js["result"]
    key = [k for k in res if k != "last"][0]
    a = np.array(res[key], dtype=float)
    idx = pd.to_datetime(a[:, 0], unit="s").normalize()
    return pd.DataFrame({"Open": a[:, 1], "High": a[:, 2], "Low": a[:, 3], "Close": a[:, 4]}, index=idx)


def to_inr(usd: pd.DataFrame, fx: pd.Series) -> pd.DataFrame:
    usd = usd.copy(); usd.index = pd.DatetimeIndex(usd.index).normalize()
    usd = usd[~usd.index.duplicated(keep="last")].sort_index()
    fx = fx.copy(); fx.index = pd.DatetimeIndex(fx.index).normalize()
    fx = fx[(fx > 30) & (fx < 200)]
    fx = fx[~fx.index.duplicated(keep="last")].sort_index()
    f = fx.reindex(usd.index.union(fx.index)).ffill().reindex(usd.index).bfill()
    d = usd.mul(f, axis=0).dropna()
    d["High"] = d[["Open", "High", "Low", "Close"]].max(axis=1)
    d["Low"] = d[["Open", "High", "Low", "Close"]].min(axis=1)
    d["usdinr"] = f.reindex(d.index).values
    return d


# ------------------------------------------------------------------ engine
class BtcEngine:
    name = "BTC"
    min_interval_sec = 240   # daily strategy: polling more often than every ~5 min adds nothing

    def __init__(self, journal: Journal, feed, notifier: Notifier | None = None, p: BtcParams = BtcParams()):
        self.j, self.feed, self.n, self.p = journal, feed, notifier or Notifier(), p

    # state ---------------------------------------------------------
    def _pos(self) -> Optional[Pos]:
        s = self.j.get("btc_pos")
        return Pos(**s) if s else None

    def _save_pos(self, ps: Optional[Pos]):
        self.j.set("btc_pos", asdict(ps) if ps else None)

    # main ----------------------------------------------------------
    def tick(self, now: datetime) -> list:
        now = now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
        self.j.set("heartbeat", {"ts": now.astimezone(IST).isoformat(timespec="seconds")})
        last = self.j.get("btc_last_tick")
        if last and (now - datetime.fromisoformat(last)).total_seconds() < self.min_interval_sec:
            return []
        self.j.set("btc_last_tick", now.isoformat(timespec="seconds"))
        try:
            d = self.feed.daily_inr()
            return self.process(d, now)
        except FeedError as e:
            self.j.event("WARN", "btc", f"data unavailable: {e}")
            return []
        except Exception as e:
            self.j.event("ERROR", "btc", f"{e}\n{traceback.format_exc()}")
            log.exception("btc tick failed")
            return []

    def process(self, d: pd.DataFrame, now: datetime) -> list:
        """Catch up all completed UTC days not yet processed, then check today's partial bar."""
        p = self.p
        today = pd.Timestamp(now.astimezone(UTC).date())
        f = features(d, p)
        done = []
        last = self.j.get("btc_last_day")
        comp = f[f.index < today]
        todo = comp[comp.index > pd.Timestamp(last)] if last else comp.tail(1)
        for day, r in todo.iterrows():
            ds = day.date().isoformat()
            pend = self.j.get("btc_pending")
            ps = self._pos()
            if pend and ps is None and pend["fill_day"] == ds:
                ps = self._enter(ds, float(r.Open), pend, now, late=True)
            if ps is not None:
                first = ps.entry_date == ds
                hit = step_bar(ps, r.Open, r.High, r.Low, r.Close, first=first, completed=True, p=p)
                if hit:
                    self._exit(ps, hit[0], hit[1], ds, now)
                    ps = None
                else:
                    self._save_pos(ps)
                    self._mark(ps, float(r.Close), float(r.High), float(r.Low))
            action, reasons = "HOLD" if ps else "NO_TRADE", []
            if ps is None:
                if bool(r.signal):
                    nxt = (day + timedelta(days=1)).date().isoformat()
                    risk = p.stop_atr * r.atr / r.Close
                    self.j.set("btc_pending", {"signal_day": ds, "fill_day": nxt, "D": float(p.stop_atr * r.atr),
                                               "close": float(r.Close)})
                    action = "SIGNAL"
                    reasons = [f"close {r.Close:,.0f} > 20d high {r.hh_prev:,.0f}; EMA20>EMA50>EMA200; "
                               f"stop distance {100 * risk:.1f}% ({'OK' if risk <= p.max_risk else 'too volatile, will skip'})"]
                else:
                    trend = r.ema_f > r.ema_m > r.ema_s
                    reasons = [f"no signal: trend {'up' if trend else 'not aligned'}, close {r.Close:,.0f} vs 20d high {r.hh_prev:,.0f}"]
            else:
                reasons = [f"holding: close {r.Close:,.0f}, stop {ps.sl:,.0f}{' (BE)' if ps.sl >= ps.entry else ''}, target {ps.tp:,.0f}"]
            self.j.record_decision(ds, "DAILY", float(r.Close), action, "BREAKOUT", None, None, reasons, _clean(r))
            self.j.set("btc_last_day", ds)
            done.append(f"day {ds}: {action}")
            if day == comp.index[-1]:
                self._daily_summary(ds, r, ps, action)

        # today's partial bar: open pending entry at today's open, intraday stop/target checks
        if today in f.index:
            r = f.loc[today]
            ds = today.date().isoformat()
            pend = self.j.get("btc_pending")
            ps = self._pos()
            if pend and ps is None and pend["fill_day"] == ds:
                ps = self._enter(ds, float(r.Open), pend, now, late=False)
                if ps:
                    done.append("entry")
            if ps is not None:
                hit = step_bar(ps, r.Open, r.High, r.Low, r.Close, first=(ps.entry_date == ds), completed=False, p=p)
                if hit:
                    self._exit(ps, hit[0], hit[1], ds, now)
                    done.append("exit")
                else:
                    self._mark(ps, float(r.Close), float(r.High), float(r.Low))
                    self._be_alert(ps, float(r.High))
        return done

    # actions --------------------------------------------------------
    def _enter(self, ds, e, pend, now, late) -> Optional[Pos]:
        p = self.p
        self.j.set("btc_pending", None)
        ps = open_pos(ds, e, pend["D"], p)
        if ps is None:
            self.j.event("INFO", "btc", f"signal {pend['signal_day']} skipped: stop {100 * pend['D'] / e:.1f}% > 10% cap")
            self.n.send(f"⚪ <b>BTC signal skipped</b> ({pend['signal_day']})\nStop distance {100 * pend['D'] / e:.1f}% exceeds the 10% risk cap.")
            return None
        conf = confidence("BREAKOUT", BTC_PRIORS, self._live_counts())
        self.j.insert_position(symbol="BTC-INR", side="LONG", lots=1, lot_size=1, entry_ts=now.isoformat(timespec="seconds"),
                               entry_date=ds, entry_price=e, entry_atr=pend["D"], disaster_stop=ps.hard, setup="BREAKOUT",
                               confidence=conf["pct"], confidence_low=conf["low90_pct"], features=json.dumps(pend),
                               last_price=e, last_price_ts=now.isoformat(timespec="seconds"))
        self._save_pos(ps)
        self.j.event("INFO", "btc", f"PAPER ENTRY BTC @ {e:,.0f} (signal {pend['signal_day']})")
        self.n.send(
            f"🟢 <b>BUY BTC</b> (paper){' — late fill at day open' if late else ''}\n"
            f"Entry (UTC {ds} open): ₹{e:,.0f}\n"
            f"Stop (daily-close basis): ₹{ps.sl:,.0f}  (−{100 * ps.D / e:.1f}%)\n"
            f"Hard stop (intraday): ₹{ps.hard:,.0f}  (−10%)\n"
            f"Target: ₹{ps.tp:,.0f}  (+{100 * 2 * ps.D / e:.1f}%)\n"
            f"Break-even move at ₹{e + ps.D:,.0f}\n"
            f"Confidence: <b>{conf['pct']}%</b> (90% floor {conf['low90_pct']}%)\n"
            f"Paper size: ₹{p.capital:,.0f}")
        return ps

    def _exit(self, ps: Pos, px, reason, ds, now):
        p = self.p
        pos = self.j.open_position()
        ret = px / ps.entry - 1 - p.cost_pct
        net_inr = ret * p.capital
        if pos:
            self.j.update_position(pos["id"], status="CLOSED", exit_ts=now.isoformat(timespec="seconds"), exit_date=ds,
                                   exit_price=float(px), exit_reason=reason, gross_pts=float(px - ps.entry),
                                   cost_pts=float(p.cost_pct * ps.entry), net_pts=float(ret * ps.entry), net_inr=float(net_inr),
                                   last_price=float(px))
        self._save_pos(None)
        self.j.event("INFO", "btc", f"PAPER EXIT BTC @ {px:,.0f} ({reason}) {100 * ret:+.2f}%")
        icon = "✅" if ret > 0 else ("⚪" if reason == "BE" else "🛑")
        self.n.send(f"{icon} <b>EXIT BTC</b> (paper) — {esc(reason)}\nExit ₹{px:,.0f} | Entry ₹{ps.entry:,.0f} ({ps.entry_date})\n"
                    f"Result: <b>{100 * ret:+.2f}%</b> = ₹{net_inr:+,.0f} on ₹{p.capital:,.0f}")

    def _mark(self, ps: Pos, last, hi, lo):
        pos = self.j.open_position()
        if not pos:
            return
        self.j.update_position(pos["id"], last_price=last, last_price_ts=datetime.now(IST).isoformat(timespec="seconds"),
                               mfe_pts=max(pos["mfe_pts"] or 0.0, hi - ps.entry), mae_pts=min(pos["mae_pts"] or 0.0, lo - ps.entry),
                               days_held=(pd.Timestamp(datetime.now(UTC).date()) - pd.Timestamp(ps.entry_date)).days)

    def _be_alert(self, ps: Pos, hi):
        if ps.sl < ps.entry and hi >= ps.entry + ps.D and not self.j.get("btc_be_alerted") == ps.entry_date:
            self.j.set("btc_be_alerted", ps.entry_date)
            self.n.send(f"🛡️ <b>BTC +1R reached</b> (₹{ps.entry + ps.D:,.0f}). Stop moves to break-even "
                        f"₹{ps.entry * (1 + self.p.be_lock):,.0f} at today's UTC close.")

    def _daily_summary(self, ds, r, ps, action):
        if self.j.get("btc_summary_day") == ds:
            return
        self.j.set("btc_summary_day", ds)
        if action == "SIGNAL":
            pend = self.j.get("btc_pending")
            self.n.send(f"📣 <b>BTC breakout signal</b> on {ds} close ₹{r.Close:,.0f}\n"
                        f"Paper BUY at next UTC open (05:30 IST). Stop distance {100 * pend['D'] / r.Close:.1f}%.")
        elif ps is None and not self.j.get("btc_quiet_summaries"):
            pass  # no message on no-signal days; the engine heartbeat is visible on the dashboard

    def _live_counts(self):
        out = {"ALL": {"wins": 0, "losses": 0}, "BREAKOUT": {"wins": 0, "losses": 0}}
        for t in self.j.closed_positions():
            k = "wins" if (t["net_inr"] or 0) > 0 else "losses"
            out["ALL"][k] += 1; out["BREAKOUT"][k] += 1
        return out


def _clean(r: pd.Series) -> dict:
    keep = ["Open", "High", "Low", "Close", "atr", "ema_f", "ema_m", "ema_s", "hh_prev", "signal"]
    out = {}
    for k in keep:
        v = r.get(k)
        out[k] = None if v is None or (isinstance(v, float) and np.isnan(v)) else (bool(v) if k == "signal" else round(float(v), 2))
    return out


def btc_db_path() -> str:
    return f"{state_dir()}/btc.db"
