"""Stock Tracker: monthly momentum rotation on NIFTY 100 (research6), run live as a paper strategy.

Rules (month-end close):
  * Regime: NIFTY close > its 200-day SMA, else hold cash (sell everything).
  * Eligible: close > own 200-day SMA and >= 1 year of history.
  * Score = (return from 252 to 21 trading days ago) / (252-day annualised volatility).
  * Hold the top 10, Rs 1 L each. Sell holdings that drop out of the top 10, buy new entrants.
  * Orders decided at the month's last close are paper-filled at the NEXT trading day's close
    (exactly as backtested). No fixed target; exits are only the monthly rank / trend / regime check.
Daily (after the close): refresh prices, mark holdings, rank everything, and publish a decision view
with warnings (holding below its 200-day SMA, out of the top 10 today, NIFTY below its 200-day SMA).
Warnings are for review; trades happen only at month-end, as tested.
"""
from __future__ import annotations

import logging
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

from .config import IST, state_dir
from .journal import Journal
from .notify import Notifier, esc

log = logging.getLogger("nifty_algo.momentum")

UNIVERSE = (
    "ADANIENT ADANIPORTS APOLLOHOSP ASIANPAINT AXISBANK BAJAJ-AUTO BAJFINANCE BAJAJFINSV BEL BHARTIARTL CIPLA "
    "COALINDIA DRREDDY EICHERMOT ETERNAL GRASIM HCLTECH HDFCBANK HDFCLIFE HINDALCO HINDUNILVR ICICIBANK INDIGO INFY "
    "ITC JIOFIN JSWSTEEL KOTAKBANK LT M&M MARUTI MAXHEALTH NESTLEIND NTPC ONGC POWERGRID RELIANCE SBILIFE SHRIRAMFIN "
    "SBIN SUNPHARMA TCS TATACONSUM TMPV TATASTEEL TECHM TITAN TRENT ULTRACEMCO WIPRO "
    "ABB ADANIENSOL ADANIGREEN ADANIPOWER AMBUJACEM BAJAJHLDNG BANKBARODA BHEL BOSCHLTD BPCL BRITANNIA CANBK CGPOWER "
    "CHOLAFIN DABUR DIVISLAB DLF DMART GAIL GODREJCP HAL HAVELLS HINDZINC HYUNDAI ICICIGI INDHOTEL IOC IRFC JINDALSTEL "
    "JSWENERGY LICI LODHA MOTHERSON NAUKRI PFC PIDILITIND PNB RECLTD SHREECEM SIEMENS SOLARINDS TATAPOWER TORNTPHARM "
    "TVSMOTOR UNITDSPR VBL VEDL ZYDUSLIFE"
).split()


@dataclass(frozen=True)
class MomParams:
    top: int = 10
    lookback: int = 252
    skip: int = 21
    trend_sma: int = 200
    regime_sma: int = 200
    capital: float = 100000.0
    cost_pct: float = 0.0035          # delivery round trip


# ------------------------------------------------------------------ data
class MomentumFeed:
    """Split/dividend-adjusted daily closes for the universe + NIFTY, from Yahoo (2 years)."""

    def __init__(self, symbols=UNIVERSE, rng: str = "2y"):
        self.symbols, self.rng = symbols, rng
        self.s = requests.Session()
        self.s.headers["User-Agent"] = "Mozilla/5.0"

    def _get(self, sym: str):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}"
        for a in range(3):
            try:
                r = self.s.get(url, params={"interval": "1d", "range": self.rng, "events": "div,splits",
                                            "includeAdjustedClose": "true"}, timeout=(5, 15))
                if r.status_code == 200:
                    res = (r.json()["chart"]["result"] or [None])[0]
                    if not res or not res.get("timestamp"):
                        return None
                    idx = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert("Asia/Kolkata").tz_localize(None).normalize()
                    q = res["indicators"]["quote"][0]
                    adj = res["indicators"].get("adjclose", [{}])[0].get("adjclose") or q["close"]
                    d = pd.DataFrame({"Open": q["open"], "Close": q["close"], "Adj": adj}, index=idx).dropna()
                    d = d[(d.Close > 0) & (d.Adj > 0)]
                    d = d[~d.index.duplicated(keep="last")]
                    return clean_series(d.Adj, d.Open * d.Adj / d.Close)
            except Exception:
                pass
            time.sleep(1 + a)
        return None

    def panel(self):
        closes = {}
        for s in self.symbols:
            x = self._get(s + ".NS")
            if x is not None and len(x) > 50:
                closes[s] = x
            time.sleep(0.15)
        n = self._get("^NSEI")
        if n is None or len(closes) < 50:
            raise RuntimeError(f"momentum data incomplete ({len(closes)} stocks, nifty={'ok' if n is not None else 'missing'})")
        return pd.DataFrame(closes).sort_index(), n


def clean_series(close: pd.Series, open_: pd.Series | None = None) -> pd.Series:
    """Back-adjust >30% overnight gaps (demergers / bad ticks) so they can't fake a crash or a rally."""
    c = close.copy()
    o = open_ if open_ is not None else c
    g = o / c.shift() - 1
    for t in g[g.abs() > 0.30].index:
        i = c.index.get_loc(t)
        c.iloc[:i] = c.iloc[:i] * (o.iloc[i] / c.iloc[i - 1])
    return c


# ------------------------------------------------------------------ ranking
def rank_table(C: pd.DataFrame, nifty: pd.Series, asof, p: MomParams = MomParams()):
    C = C[C.index <= asof]
    n = nifty[nifty.index <= asof]
    R = C.pct_change().fillna(0.0)          # identical to the validated backtest (missing day = 0% return)
    mom = C.shift(p.skip).iloc[-1] / C.shift(p.lookback).iloc[-1] - 1
    vol = R.tail(252).std() * np.sqrt(252)
    sma = C.rolling(p.trend_sma).mean().iloc[-1]
    close = C.iloc[-1]
    hist = C.notna().sum()
    elig = (close > sma) & (hist > p.lookback) & close.notna()
    score = (mom / vol).where(elig)
    t = pd.DataFrame({"score": score, "mom_pct": 100 * mom, "vol_pct": 100 * vol, "close": close, "sma200": sma,
                      "eligible": elig}).dropna(subset=["close"])
    t = t.sort_values("score", ascending=False, na_position="last")
    t["rank"] = np.where(t.score.notna(), np.arange(1, len(t) + 1), np.nan)
    nsma = n.rolling(p.regime_sma).mean().iloc[-1]
    regime = bool(n.iloc[-1] > nsma)
    return t, regime, float(n.iloc[-1]), float(nsma)


def last_weekday_of_month(d) -> pd.Timestamp:
    e = (pd.Timestamp(d) + pd.offsets.MonthEnd(0)).normalize()
    while e.weekday() >= 5:
        e -= pd.Timedelta(days=1)
    return e


# ------------------------------------------------------------------ engine
class MomentumEngine:
    name = "MOMENTUM"
    after_close_min = 15 * 60 + 45

    def __init__(self, journal: Journal, feed, notifier: Notifier | None = None, p: MomParams = MomParams()):
        self.j, self.feed, self.n, self.p = journal, feed, notifier or Notifier(), p

    def _due(self, now) -> bool:
        last = self.j.get("mom_last_run")
        if not last:
            return True
        last = datetime.fromisoformat(last)
        if last.date() < now.date():
            return now.hour * 60 + now.minute >= 9 * 60          # once each morning (catch-up) ...
        close_today = now.replace(hour=self.after_close_min // 60, minute=self.after_close_min % 60, second=0, microsecond=0)
        return now >= close_today > last                           # ... and once after today's close

    def tick(self, now: datetime) -> list:
        now = now.astimezone(IST) if now.tzinfo else now.replace(tzinfo=IST)
        self.j.set("heartbeat", {"ts": now.isoformat(timespec="seconds")})
        if not self._due(now):
            return []
        try:
            C, nifty = self.feed.panel()
            out = self.process(C, nifty, now)
            self.j.set("mom_last_run", now.isoformat(timespec="seconds"))
            return out
        except Exception as e:
            self.j.event("ERROR", "momentum", f"{e}\n{traceback.format_exc()}")
            log.exception("momentum tick failed")
            return []

    # -------------------------------------------------------------
    def _session_closed(self, C, now) -> bool:
        """Is the latest bar a completed session? (bar dated before today, or today after the close)."""
        last = C.index[-1].date()
        return last < now.date() or now.hour * 60 + now.minute >= self.after_close_min

    def process(self, C: pd.DataFrame, nifty: pd.Series, now: datetime) -> list:
        done = []
        closed_idx = C.index if self._session_closed(C, now) else C.index[:-1]
        # 1) fill pending orders at the close of the first trading day after the decision
        self._try_fill(C, closed_idx, now, done)
        # 2) month-end decision (catch-up safe)
        months = pd.Series(closed_idx, index=closed_idx).groupby(closed_idx.to_period("M")).max()
        last_dec = self.j.get("mom_last_decision")
        complete = [m for m, d in months.items()
                    if d < closed_idx[-1] or (d == closed_idx[-1] and pd.Timestamp(d) >= last_weekday_of_month(d))]
        if last_dec is None:                     # first start: no back-fill, begin with the next month-end
            prior = [m for m in complete if m < pd.Period(now.date(), "M")]
            self.j.set("mom_last_decision", str(prior[-1]) if prior else str(pd.Period(now.date(), "M") - 1))
            last_dec = self.j.get("mom_last_decision")
        for m in complete:
            if str(m) > last_dec and not (self.j.get("mom_pending") or []):
                self._decide(C, nifty, months[m], str(m), now)
                done.append(f"decision {m}")
                last_dec = str(m)
                self._try_fill(C, closed_idx, now, done)    # month-end was a holiday: fill bar may already exist
        # 3) daily decision view
        self._view(C, nifty, closed_idx[-1], now)
        done.append("view")
        return done

    def _try_fill(self, C, closed_idx, now, done):
        pend = self.j.get("mom_pending") or []
        if not pend:
            return
        after = closed_idx[closed_idx > pd.Timestamp(pend[0]["decision_date"])]
        if len(after):
            self._fill(pend, C, after[0], now)
            self.j.set("mom_pending", [])
            done.append(f"filled {len(pend)} orders on {after[0].date()}")

    def _decide(self, C, nifty, dday, month, now):
        p = self.p
        t, regime, nv, nsma = rank_table(C, nifty, dday, p)
        targets = list(t[t.score.notna()].index[:p.top]) if regime else []
        held = [x["symbol"] for x in self.j.open_positions()]
        sells = [s for s in held if s not in targets]
        buys = [s for s in targets if s not in held]
        holds = [s for s in held if s in targets]
        why = {}
        for s in sells:
            if not regime:
                why[s] = "NIFTY below 200-day avg: move to cash"
            elif s in t.index and not bool(t.at[s, "eligible"]):
                why[s] = "fell below its 200-day avg"
            else:
                r = t.at[s, "rank"] if s in t.index else None
                why[s] = f"dropped out of top {p.top} (rank {int(r)})" if r == r and r is not None else "not ranked"
        for s in buys:
            why[s] = f"new top {p.top} (rank {int(t.at[s, 'rank'])}, score {t.at[s, 'score']:.2f})"
        orders = [{"decision_date": str(dday.date()), "action": "SELL", "symbol": s, "why": why[s]} for s in sells] + \
                 [{"decision_date": str(dday.date()), "action": "BUY", "symbol": s, "why": why[s]} for s in buys]
        self.j.set("mom_pending", orders)
        self.j.set("mom_last_decision", month)
        reasons = [f"NIFTY {nv:,.0f} vs 200-day {nsma:,.0f}: regime {'ON' if regime else 'OFF (cash)'}"] + \
                  [f"{o['action']} {o['symbol']}: {o['why']}" for o in orders] + [f"HOLD {s}" for s in holds]
        self.j.record_decision(str(dday.date()), f"REBAL-{month}", nv, "REBALANCE", "MOMENTUM", None, None, reasons,
                               {"buys": buys, "sells": sells, "holds": holds, "regime": regime,
                                "top": t.head(15)[["rank", "score", "close"]].round(2).reset_index().rename(columns={"index": "sym"}).to_dict("records")})
        msg = (f"📊 <b>Stock Tracker — {month} rebalance</b> (decided on {dday:%d-%b} close)\n"
               f"NIFTY {nv:,.0f} vs 200-day {nsma:,.0f} → {'invest' if regime else 'stay in CASH'}\n")
        if buys: msg += "🟢 BUY: " + ", ".join(buys) + "\n"
        if sells: msg += "🔴 SELL: " + ", ".join(sells) + "\n"
        if holds: msg += "⚪ HOLD: " + ", ".join(holds) + "\n"
        if not (buys or sells or holds): msg += "No positions.\n"
        msg += "Paper fills at the next trading day's close; ₹1 L per stock."
        self.n.send(msg)

    def _fill(self, orders, C, fill_day, now):
        p = self.p
        ts = now.isoformat(timespec="seconds")
        opens = {x["symbol"]: x for x in self.j.open_positions()}
        for o in orders:
            s = o["symbol"]
            px = float(C.at[fill_day, s]) if s in C.columns and not pd.isna(C.at[fill_day, s]) else None
            if px is None:
                self.j.event("WARN", "momentum", f"no price for {s} on {fill_day.date()}; order skipped")
                continue
            if o["action"] == "SELL" and s in opens:
                pos = opens[s]
                ret = px / pos["entry_price"] - 1 - p.cost_pct
                self.j.update_position(pos["id"], status="CLOSED", exit_ts=ts, exit_date=str(fill_day.date()), exit_price=px,
                                       exit_reason=o["why"], gross_pts=px - pos["entry_price"], net_pts=ret * pos["entry_price"],
                                       net_inr=ret * p.capital, last_price=px)
            elif o["action"] == "BUY" and s not in opens:
                self.j.insert_position(allow_multiple=True, symbol=s, side="LONG", lots=1, lot_size=1, entry_ts=ts,
                                       entry_date=str(fill_day.date()), entry_price=px, entry_atr=None, disaster_stop=None,
                                       setup="MOMENTUM", confidence=None, confidence_low=None, features=o["why"],
                                       last_price=px, last_price_ts=ts)
        self.j.event("INFO", "momentum", f"filled {len(orders)} orders at {fill_day.date()} close")
        self.n.send(f"✅ <b>Stock Tracker</b>: paper orders filled at {fill_day:%d-%b} close: " +
                    ", ".join(f"{o['action']} {o['symbol']}" for o in orders))

    def _view(self, C, nifty, asof, now):
        p = self.p
        t, regime, nv, nsma = rank_table(C, nifty, asof, p)
        hold_rows, warns = [], []
        for pos in self.j.open_positions():
            s = pos["symbol"]
            last = float(C[s].dropna().iloc[-1]) if s in C.columns else pos["last_price"]
            row = t.loc[s] if s in t.index else None
            rank = None if row is None or row["rank"] != row["rank"] else int(row["rank"])
            sma = None if row is None else float(row["sma200"])
            status = "OK"
            if sma is not None and last < sma:
                status = "⚠️ below 200-day avg (sell at month-end if still below)"
            elif rank is None or rank > p.top:
                status = f"⚠️ rank {rank or '—'} (sell at month-end if outside top {p.top})"
            if status != "OK":
                warns.append(f"{s}: {status}")
            ret = last / pos["entry_price"] - 1
            self.j.update_position(pos["id"], last_price=last, last_price_ts=now.isoformat(timespec="seconds"))
            hold_rows.append({"symbol": s, "entry_date": pos["entry_date"], "entry": pos["entry_price"], "last": last,
                              "ret_pct": round(100 * ret, 2), "pnl": round(ret * p.capital), "rank": rank,
                              "exit_level": round(sma, 2) if sma else None, "status": status})
        if not regime:
            warns.insert(0, f"NIFTY {nv:,.0f} is below its 200-day avg {nsma:,.0f}: month-end decision would be CASH")
        held = {r["symbol"] for r in hold_rows}
        top = t.head(15).copy()
        ranking = [{"rank": int(r["rank"]) if r["rank"] == r["rank"] else None, "symbol": s,
                    "score": None if pd.isna(r["score"]) else round(float(r["score"]), 2),
                    "mom_pct": round(float(r["mom_pct"]), 1), "close": round(float(r["close"]), 2),
                    "sma200": round(float(r["sma200"]), 2), "held": s in held} for s, r in top.iterrows()]
        would_buy = [x["symbol"] for x in ranking if x["rank"] and x["rank"] <= p.top and not x["held"]] if regime else []
        would_sell = [r["symbol"] for r in hold_rows if r["status"] != "OK"] if regime else [r["symbol"] for r in hold_rows]
        nxt = last_weekday_of_month(now.date())
        if now.date() > nxt.date():
            nxt = last_weekday_of_month(now.date() + timedelta(days=7))
        self.j.set("mom_view", {
            "updated": now.isoformat(timespec="seconds"), "asof": str(asof.date()), "regime": regime,
            "nifty": round(nv, 2), "nifty_sma200": round(nsma, 2), "next_rebalance": str(nxt.date()),
            "holdings": hold_rows, "ranking": ranking, "warnings": warns,
            "if_rebalanced_today": {"buy": would_buy, "sell": would_sell},
            "pending": self.j.get("mom_pending") or []})
        if hold_rows and self._session_closed(C, now) and self.j.get("mom_summary_day") != str(asof.date()):
            self.j.set("mom_summary_day", str(asof.date()))
            tot = sum(r["pnl"] for r in hold_rows)
            best = max(hold_rows, key=lambda r: r["ret_pct"]); worst = min(hold_rows, key=lambda r: r["ret_pct"])
            self.n.send(f"📊 <b>Stock Tracker daily update — {asof.date()}</b>\n{len(hold_rows)} holdings · open P&L <b>₹{tot:+,.0f}</b>\n"
                        f"Best {best['symbol']} {best['ret_pct']:+.1f}% · worst {worst['symbol']} {worst['ret_pct']:+.1f}%\n"
                        f"{len(warns)} watch item(s) · next rebalance {nxt.date()}")
        new_warn = sorted(set(warns) - set(self.j.get("mom_warned") or []))
        if new_warn and hold_rows:
            self.n.send("⚠️ <b>Stock Tracker watch</b> (no action until month-end):\n" + "\n".join(esc(w) for w in new_warn))
        self.j.set("mom_warned", warns)


def momentum_db_path() -> str:
    return f"{state_dir()}/momentum.db"
