"""Run the engines inside the Streamlit app process (simple hosting: no GitHub Actions needed).

start_background() launches ONE daemon thread per process that ticks both engines every 30 s,
writes status.json, and sends a daily 'engine alive' Telegram ping at ~09:05 IST so you notice if
the app was asleep. Missed BTC days are caught up automatically when the app wakes; the NIFTY
15:25 decision needs the app awake at that time.
"""
from __future__ import annotations

import logging
import os
import threading
import time
import traceback
from datetime import datetime

import requests

from .config import IST, state_dir

log = logging.getLogger("nifty_algo.inapp")
STATE_RAW = "https://raw.githubusercontent.com/vnkthere2u/Nifty-Algo-Engine/algo-state"


def restore_state(dirpath: str):
    """First start on a fresh container: seed the journal from the last published state (if any)."""
    for name in ("nifty.db", "btc.db"):
        p = os.path.join(dirpath, name)
        if os.path.exists(p):
            continue
        try:
            r = requests.get(f"{STATE_RAW}/{name}", timeout=15)
            if r.status_code == 200 and r.content[:15] == b"SQLite format 3":
                with open(p, "wb") as fh:
                    fh.write(r.content)
        except Exception:
            pass


def _loop(health: dict):
    from .runner import build_engines, tick_all, write_status
    engines = build_engines(["nifty", "btc"])
    notifier = engines[0].n
    notifier.send("🚀 <b>Algo engine started</b> (in Streamlit app) " + datetime.now(IST).strftime("%d-%b %H:%M IST")
                  + "\nTracking NIFTY (15:25 IST decision) and BTC (daily close). You'll get a daily 'alive' ping ~09:05 IST.")
    while True:
        try:
            res = tick_all(engines)
            write_status(engines, res)
            health.update(last_tick=datetime.now(IST).isoformat(timespec="seconds"), ticks=health["ticks"] + 1, error=None)
            now = datetime.now(IST)
            if now.hour * 60 + now.minute >= 545 and health.get("alive_sent") != now.date().isoformat():
                health["alive_sent"] = now.date().isoformat()
                parts = []
                for e in engines:
                    pos = e.j.open_position()
                    name = getattr(e, "name", None) or e.cfg.name
                    parts.append(f"{name}: {'LONG from ' + format(pos['entry_price'], ',.2f') if pos else 'flat'}")
                notifier.send("✅ <b>Algo engine alive</b> " + now.strftime("%d-%b %H:%M IST") + "\n" + "\n".join(parts))
        except Exception as ex:
            health["error"] = f"{ex}"
            log.error("engine loop error: %s\n%s", ex, traceback.format_exc())
        time.sleep(30 - (time.time() % 30) + 1)


def start_background() -> dict:
    d = state_dir()
    restore_state(d)
    health = {"started": datetime.now(IST).isoformat(timespec="seconds"), "ticks": 0, "last_tick": None, "error": None,
              "state_dir": d}
    threading.Thread(target=_loop, args=(health,), daemon=True, name="AlgoEngine").start()
    return health
