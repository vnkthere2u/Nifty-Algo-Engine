"""SQLite journal: the single source of truth for state, decisions, trades and logs.

Only the engine process writes.  The dashboard opens the DB read-only, so UI refreshes can
never block or corrupt the engine.  WAL mode + short transactions.
"""
from __future__ import annotations

import contextlib
import json
import os
import sqlite3
from datetime import datetime
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, level TEXT, component TEXT, message TEXT);
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, trade_date TEXT, phase TEXT, price REAL,
    action TEXT, setup TEXT, confidence REAL, confidence_low REAL, reasons TEXT, features TEXT,
    UNIQUE(trade_date, phase));
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, side TEXT, lots INTEGER, lot_size INTEGER,
    entry_ts TEXT, entry_date TEXT, entry_price REAL, entry_atr REAL, disaster_stop REAL,
    setup TEXT, confidence REAL, confidence_low REAL, features TEXT,
    status TEXT, days_held INTEGER DEFAULT 0, last_price REAL, last_price_ts TEXT,
    mfe_pts REAL DEFAULT 0, mae_pts REAL DEFAULT 0,
    exit_ts TEXT, exit_date TEXT, exit_price REAL, exit_reason TEXT,
    gross_pts REAL, cost_pts REAL, net_pts REAL, net_inr REAL);
CREATE INDEX IF NOT EXISTS ix_pos_status ON positions(status);
CREATE TABLE IF NOT EXISTS daily_bars (
    date TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL, snapshot_ts TEXT);
"""


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


class Journal:
    def __init__(self, path: str, readonly: bool = False):
        self.path = path
        self.readonly = readonly
        if not readonly:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            with self.conn() as c:
                c.executescript(SCHEMA)

    @contextlib.contextmanager
    def conn(self):
        if self.readonly:
            c = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=10)
        else:
            c = sqlite3.connect(self.path, timeout=30)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
        c.row_factory = sqlite3.Row
        try:
            yield c
            if not self.readonly:
                c.commit()
        except Exception:
            if not self.readonly:
                c.rollback()
            raise
        finally:
            c.close()

    # ---------------------------------------------------------------- kv / events
    def set(self, key: str, value) -> None:
        with self.conn() as c:
            c.execute("INSERT OR REPLACE INTO kv(key,value,updated_at) VALUES(?,?,?)",
                      (key, json.dumps(value, default=str), _now_iso()))

    def get(self, key: str, default=None):
        with self.conn() as c:
            r = c.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(r["value"]) if r else default

    def event(self, level: str, component: str, message: str) -> None:
        with self.conn() as c:
            c.execute("INSERT INTO events(ts,level,component,message) VALUES(?,?,?,?)",
                      (_now_iso(), level, component, message[:2000]))
            c.execute("DELETE FROM events WHERE id <= (SELECT MAX(id) FROM events) - 20000")

    # ---------------------------------------------------------------- decisions
    def record_decision(self, trade_date: str, phase: str, price: Optional[float], action: str, setup: str,
                        confidence: Optional[float], confidence_low: Optional[float], reasons: list, features: dict) -> bool:
        """Returns False if a decision for (trade_date, phase) already exists (idempotency)."""
        with self.conn() as c:
            try:
                c.execute("""INSERT INTO decisions(ts,trade_date,phase,price,action,setup,confidence,confidence_low,
                             reasons,features) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                          (_now_iso(), trade_date, phase, price, action, setup, confidence, confidence_low,
                           json.dumps(reasons), json.dumps(features, default=str)))
                return True
            except sqlite3.IntegrityError:
                return False

    def decision_exists(self, trade_date: str, phase: str) -> bool:
        with self.conn() as c:
            return c.execute("SELECT 1 FROM decisions WHERE trade_date=? AND phase=?", (trade_date, phase)).fetchone() is not None

    # ---------------------------------------------------------------- positions
    def open_position(self) -> Optional[dict]:
        with self.conn() as c:
            r = c.execute("SELECT * FROM positions WHERE status='OPEN' ORDER BY id DESC LIMIT 1").fetchone()
        return dict(r) if r else None

    def insert_position(self, **kw) -> int:
        kw.setdefault("status", "OPEN")
        cols = ",".join(kw.keys())
        q = ",".join("?" * len(kw))
        with self.conn() as c:
            if c.execute("SELECT 1 FROM positions WHERE status='OPEN'").fetchone():
                raise RuntimeError("refusing to open a second position")
            cur = c.execute(f"INSERT INTO positions({cols}) VALUES({q})", tuple(kw.values()))
            return int(cur.lastrowid)

    def update_position(self, pid: int, **kw) -> None:
        sets = ",".join(f"{k}=?" for k in kw)
        with self.conn() as c:
            c.execute(f"UPDATE positions SET {sets} WHERE id=?", (*kw.values(), pid))

    def closed_positions(self) -> list:
        with self.conn() as c:
            return [dict(r) for r in c.execute("SELECT * FROM positions WHERE status='CLOSED' ORDER BY id")]

    def upsert_daily_bar(self, date: str, o: float, h: float, l: float, cl: float) -> None:
        with self.conn() as c:
            c.execute("INSERT OR REPLACE INTO daily_bars VALUES(?,?,?,?,?,?)", (date, o, h, l, cl, _now_iso()))
