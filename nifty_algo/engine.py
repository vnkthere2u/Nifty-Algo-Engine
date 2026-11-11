"""Live/paper trading engine for TPMR.

tick(now) is the only entry point; it is idempotent and safe to call as often as you like:
  * MONITOR  (every `monitor_every_sec` inside the session, only when a position is open):
             mark-to-market, MFE/MAE, intraday disaster stop.
  * DECISION (once per trading day, 15:25-15:29 IST): exit check for an open position,
             otherwise entry check -> signal + confidence + journal + alert.
  * EOD      (once per day after the close): daily summary.
All state lives in the SQLite journal, so a restart resumes exactly where it stopped.
"""
from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timedelta

import pandas as pd

from .bars import FEATURE_WINDOW
from .confidence import confidence, load_priors
from .config import IST, Config
from .feeds import FeedError
from .journal import Journal
from .notify import Notifier, esc
from .strategy import PositionState, compute_features, disaster_stop_price, evaluate_entry, evaluate_exit

log = logging.getLogger("nifty_algo.engine")


def _ist(now: datetime) -> datetime:
    return now.astimezone(IST) if now.tzinfo else now.replace(tzinfo=IST)


def _mins(now: datetime) -> int:
    return now.hour * 60 + now.minute


class Engine:
    def __init__(self, cfg: Config, journal: Journal, feed, notifier: Notifier | None = None):
        self.cfg, self.j, self.feed = cfg, journal, feed
        self.n = notifier or Notifier()
        self.priors = load_priors(cfg.priors_path)

    # ================================================================ dispatcher
    def tick(self, now: datetime) -> list:
        now = _ist(now)
        done = []
        self.j.set("heartbeat", {"ts": now.isoformat(timespec="seconds")})
        m = _mins(now)
        for name, due, fn in (
            ("monitor", self._monitor_due(now, m), self.monitor),
            ("decision", self._decision_due(now, m), self.decide),
            ("eod", self._eod_due(now, m), self.eod),
        ):
            if not due:
                continue
            try:
                fn(now)
                done.append(name)
            except FeedError as e:
                self.j.event("WARN", name, f"data unavailable: {e}")
                log.warning("%s: data unavailable: %s", name, e)
            except Exception as e:  # never let one phase kill the loop
                self.j.event("ERROR", name, f"{e}\n{traceback.format_exc()}")
                log.exception("%s failed", name)
        return done

    def _monitor_due(self, now, m) -> bool:
        if not (self.cfg.session_open_min <= m <= self.cfg.session_close_min):
            return False
        if self.j.open_position() is None:
            return False
        last = self.j.get("last_monitor_ts")
        return last is None or (now - datetime.fromisoformat(last)).total_seconds() >= self.cfg.monitor_every_sec - 5

    def _decision_due(self, now, m) -> bool:
        return (self.cfg.decision_min <= m <= self.cfg.late_decision_until_min
                and not self.j.decision_exists(now.date().isoformat(), "DECISION"))

    def _eod_due(self, now, m) -> bool:
        return m >= self.cfg.session_close_min + 5 and self.j.get("eod_done") != now.date().isoformat() \
            and self.j.decision_exists(now.date().isoformat(), "DECISION")

    # ================================================================ phases
    def monitor(self, now: datetime):
        pos = self.j.open_position()
        if pos is None:
            return
        snap = self.feed.snapshot(now, self.cfg.session_open_min, None)
        self.j.set("last_monitor_ts", now.isoformat(timespec="seconds"))
        if snap.today is None or snap.last_price is None:
            return
        if not self._fresh(snap, now):
            self.j.event("WARN", "monitor", f"stale intraday data (as_of={snap.as_of})")
            return
        px = snap.last_price
        mfe = max(pos["mfe_pts"] or 0.0, float(snap.today.High) - pos["entry_price"]) if snap.today is not None else pos["mfe_pts"]
        mae = min(pos["mae_pts"] or 0.0, float(snap.today.Low) - pos["entry_price"]) if snap.today is not None else pos["mae_pts"]
        # only today's extremes after entry are meaningful on the entry day
        if pos["entry_date"] == now.date().isoformat():
            mfe = max(pos["mfe_pts"] or 0.0, px - pos["entry_price"])
            mae = min(pos["mae_pts"] or 0.0, px - pos["entry_price"])
        self.j.update_position(pos["id"], last_price=px, last_price_ts=snap.as_of.isoformat(), mfe_pts=mfe, mae_pts=mae)
        ds = pos["disaster_stop"]
        if ds is not None and px <= ds:
            self._close(pos, px, now, f"DISASTER STOP {ds:.2f}")

    def decide(self, now: datetime):
        trade_date = now.date().isoformat()
        snap = self.feed.snapshot(now, self.cfg.session_open_min, self.cfg.decision_min)
        if snap.today is None:
            # no intraday bars today: exchange holiday / weekend
            if _mins(now) >= self.cfg.decision_deadline_min or now.weekday() >= 5:
                self.j.record_decision(trade_date, "DECISION", None, "NO_SESSION", "NONE", None, None,
                                       ["no intraday data for today (holiday/weekend)"], {})
            return
        # the snapshot is complete when its last bar is within 6 minutes of the 15:25 cutoff
        cutoff = now.replace(hour=self.cfg.decision_min // 60, minute=self.cfg.decision_min % 60, second=0, microsecond=0)
        complete = snap.as_of is not None and 0 < (cutoff - snap.as_of).total_seconds() <= 360
        if not complete:
            msg = f"incomplete snapshot as_of={snap.as_of} at {now:%H:%M:%S}"
            if _mins(now) >= self.cfg.decision_deadline_min + 60:
                self.j.record_decision(trade_date, "DECISION", snap.last_price, "SKIPPED_STALE", "NONE", None, None, [msg], {})
                self.n.send(f"⚠️ <b>{esc(self.cfg.name)}</b> decision SKIPPED - {esc(msg)}")
            else:
                self.j.event("WARN", "decision", msg + " (will retry)")
            return
        self._late = _mins(now) > self.cfg.decision_deadline_min

        frame = pd.concat([snap.daily.tail(FEATURE_WINDOW - 1), snap.today.to_frame().T]).astype(float)
        feats = compute_features(frame, self.cfg.strategy)
        row = feats.iloc[-1]
        price = float(snap.today.Close)
        self.j.upsert_daily_bar(trade_date, float(snap.today.Open), float(snap.today.High), float(snap.today.Low), price)

        pos = self.j.open_position()
        if pos is not None:
            self._decide_exit(now, trade_date, pos, row, price)
        else:
            self._decide_entry(now, trade_date, row, len(frame), price)

    def _decide_exit(self, now, trade_date, pos, row, price):
        if pos["entry_date"] >= trade_date:
            self.j.record_decision(trade_date, "DECISION", price, "HOLD", pos["setup"], None, None,
                                   ["entry day"], _clean(row))
            return
        days = int(pos["days_held"] or 0) + 1
        self.j.update_position(pos["id"], days_held=days, last_price=price, last_price_ts=now.isoformat(timespec="seconds"))
        st = PositionState(entry_price=pos["entry_price"], entry_atr=pos["entry_atr"],
                           entry_date=pos["entry_date"], days_held=days)
        reason = evaluate_exit(row, st, self.cfg.strategy)
        if reason:
            if self.j.record_decision(trade_date, "DECISION", price, "EXIT", pos["setup"], None, None, [reason], _clean(row)):
                self._close(pos, price, now, reason)
        else:
            unreal = price - pos["entry_price"]
            self.j.record_decision(trade_date, "DECISION", price, "HOLD", pos["setup"], None, None,
                                   [f"day {days}: close {price:.2f} <= prev high {row['prev_high']:.2f}; "
                                    f"unrealised {unreal:+.1f} pts"], _clean(row))

    def _decide_entry(self, now, trade_date, row, hist_len, price):
        dec = evaluate_entry(row, self.cfg.strategy, hist_len)
        if not dec.take:
            self.j.record_decision(trade_date, "DECISION", price, "NO_TRADE", dec.setup_key, None, None,
                                   dec.reasons, dec.features)
            return
        blocked = self._risk_block()
        conf = confidence(dec.setup_key, self.priors, self._live_counts())
        if blocked:
            self.j.record_decision(trade_date, "DECISION", price, "BLOCKED_RISK", dec.setup_key, conf["pct"],
                                   conf["low90_pct"], [blocked], dec.features)
            self.n.send(f"⛔ <b>{esc(self.cfg.name)}</b> BUY signal blocked by risk governor: {esc(blocked)}")
            return
        if not self.j.record_decision(trade_date, "DECISION", price, "ENTRY", dec.setup_key, conf["pct"],
                                      conf["low90_pct"], dec.reasons, dec.features):
            return  # another process already decided today
        p = self.cfg.strategy
        atr_v = float(row["atr"])
        ds = disaster_stop_price(price, atr_v, p)
        self.j.insert_position(symbol=self.cfg.symbol, side="LONG", lots=self.cfg.lots, lot_size=self.cfg.lot_size,
                               entry_ts=now.isoformat(timespec="seconds"), entry_date=trade_date, entry_price=price,
                               entry_atr=atr_v, disaster_stop=ds, setup=dec.setup_key, confidence=conf["pct"],
                               confidence_low=conf["low90_pct"], features=json.dumps(dec.features),
                               last_price=price, last_price_ts=now.isoformat(timespec="seconds"))
        self.j.event("INFO", "decision", f"ENTRY {self.cfg.name} @ {price:.2f} setup={dec.setup_key} conf={conf['pct']}%")
        late = getattr(self, "_late", False)
        msg = (f"🟢 <b>BUY {esc(self.cfg.name)}</b> (paper) @ {price:,.2f}"
               f"{' — LATE ALERT, paper-filled at the 15:25 price' if late else ''}\n"
               f"Setup: <b>{esc(dec.setup_key)}</b> | RSI{p.rsi_len}={row['rsi']:.1f} | IBS={row['ibs']:.2f}\n"
               f"Trend: {row['trend_dist_atr']:+.1f} ATR above SMA{p.trend_sma}\n"
               f"Confidence: <b>{conf['pct']}%</b> (90% floor {conf['low90_pct']}%, basis {esc(conf['basis'])})\n"
               f"Exit plan: 15:25 close &gt; prev-day high ({row['High']:,.2f} today), or {p.max_hold_days} days")
        if ds is not None:
            msg += f"\nDisaster stop: {ds:,.2f} ({p.disaster_stop_atr}×ATR)"
        self.n.send(msg)

    def eod(self, now: datetime):
        d = now.date().isoformat()
        with self.j.conn() as c:
            r = c.execute("SELECT action, price, setup, reasons FROM decisions WHERE trade_date=? AND phase='DECISION'", (d,)).fetchone()
        pos = self.j.open_position()
        s = self.stats()
        txt = (f"📒 <b>{esc(self.cfg.name)} EOD {d}</b>\nDecision: <b>{esc(r['action'])}</b> "
               f"@ {r['price'] if r['price'] is not None else '-'}\n")
        if pos:
            txt += f"Open: LONG from {pos['entry_price']:,.2f} ({pos['entry_date']}), day {pos['days_held']}, last {pos['last_price']:,.2f}\n"
        txt += f"Live record: {s['trades']} trades, win {s['win_pct']}%, net {s['net_pts']:+.1f} pts"
        self.n.send(txt)
        self.j.set("eod_done", d)

    # ================================================================ helpers
    def _fresh(self, snap, now) -> bool:
        if snap.as_of is None:
            return False
        age = (now - snap.as_of).total_seconds()
        return age <= self.cfg.max_data_age_sec

    def _close(self, pos, price, now, reason):
        gross = price - pos["entry_price"]
        cost = self.cfg.cost_pct * pos["entry_price"]
        net = gross - cost
        self.j.update_position(pos["id"], status="CLOSED", exit_ts=now.isoformat(timespec="seconds"),
                               exit_date=now.date().isoformat(), exit_price=price, exit_reason=reason,
                               gross_pts=gross, cost_pts=cost, net_pts=net,
                               net_inr=net / pos["entry_price"] * self.cfg.capital_per_trade, last_price=price)
        self.j.event("INFO", "trade", f"EXIT {self.cfg.name} @ {price:.2f} ({reason}) net {net:+.1f} pts")
        icon = "✅" if net > 0 else "🛑"
        self.n.send(f"{icon} <b>EXIT {esc(self.cfg.name)}</b> @ {price:,.2f}\n{esc(reason)}\n"
                    f"Entry {pos['entry_price']:,.2f} ({pos['entry_date']}) → net <b>{net:+.1f} pts</b> "
                    f"({100 * net / pos['entry_price']:+.2f}% = ₹{net / pos['entry_price'] * self.cfg.capital_per_trade:+,.0f} on ₹1 L)")

    def _live_counts(self) -> dict:
        out = {"ALL": {"wins": 0, "losses": 0}}
        for t in self.j.closed_positions():
            k = "wins" if (t["net_pts"] or 0) > 0 else "losses"
            out["ALL"][k] += 1
            out.setdefault(t["setup"], {"wins": 0, "losses": 0})[k] += 1
        return out

    def _risk_block(self) -> str | None:
        reset_at = self.j.get("risk_reset_id", 0)
        tr = [t for t in self.j.closed_positions() if t["id"] > reset_at]
        if not tr:
            return None
        streak = 0
        for t in reversed(tr):
            if (t["net_pts"] or 0) <= 0:
                streak += 1
            else:
                break
        if streak >= self.cfg.max_consecutive_losses:
            return f"{streak} consecutive losses (limit {self.cfg.max_consecutive_losses}); reset with --reset-risk"
        eq = pd.Series([t["net_pts"] or 0.0 for t in tr]).cumsum()
        dd = float((eq - eq.cummax().clip(lower=0)).min())
        if -dd >= self.cfg.max_drawdown_pts:
            return f"drawdown {dd:.0f} pts breaches limit {self.cfg.max_drawdown_pts:.0f}; reset with --reset-risk"
        return None

    def stats(self) -> dict:
        tr = self.j.closed_positions()
        n = len(tr)
        w = sum(1 for t in tr if (t["net_pts"] or 0) > 0)
        return {"trades": n, "win_pct": round(100 * w / n, 1) if n else 0.0,
                "net_pts": round(sum((t["net_pts"] or 0) for t in tr), 1)}

    def reset_risk(self):
        tr = self.j.closed_positions()
        self.j.set("risk_reset_id", tr[-1]["id"] if tr else 0)
        self.j.event("INFO", "risk", "risk governor reset by operator")


def _clean(row: pd.Series) -> dict:
    return {k: (None if pd.isna(v) else round(float(v), 4)) for k, v in row.items()}
