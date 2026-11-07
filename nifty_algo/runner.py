"""Process entry point: single-instance, signal-safe scheduler loop.

    python -m nifty_algo.runner                # run forever (paper trading + alerts)
    python -m nifty_algo.runner --once         # one tick now (cron-friendly)
    python -m nifty_algo.runner --status       # print state and exit
    python -m nifty_algo.runner --reset-risk   # clear the risk-governor pause

Fixes vs v3.3: no Streamlit-hosted daemon thread, no overlapping scans (one process holds an
exclusive file lock; ticks run sequentially), wall-clock aligned wakeups, every failure
contained and journaled.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime

from .config import IST, load_config
from .engine import Engine
from .feeds import YahooFeed
from .journal import Journal
from .notify import Notifier

log = logging.getLogger("nifty_algo")
_STOP = False


def _handle_stop(signum, frame):
    global _STOP
    _STOP = True


class SingleInstance:
    def __init__(self, path: str):
        self.path = path
        self.fh = None

    def __enter__(self):
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        self.fh = open(self.path, "w")
        try:
            fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"another engine instance holds {self.path}; refusing to start")
        self.fh.write(str(os.getpid()))
        self.fh.flush()
        return self

    def __exit__(self, *a):
        try:
            fcntl.flock(self.fh, fcntl.LOCK_UN)
            self.fh.close()
        except Exception:
            pass


def _sleep_until_next(step_sec: int = 30):
    """Sleep to the next wall-clock multiple of step_sec (+2s), waking early on stop."""
    now = time.time()
    target = (int(now) // step_sec + 1) * step_sec + 2
    while not _STOP and time.time() < target:
        time.sleep(min(1.0, target - time.time()))


def build_engine(cfg_path: str | None = None) -> Engine:
    cfg = load_config(cfg_path)
    j = Journal(cfg.db_path)
    feed = YahooFeed(cfg.symbol)
    return Engine(cfg, j, feed, Notifier(cfg.telegram_token, cfg.telegram_chat_id))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--reset-risk", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    a = ap.parse_args(argv)
    logging.basicConfig(level=a.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    eng = build_engine(a.config)

    if a.status:
        print(json.dumps({"heartbeat": eng.j.get("heartbeat"), "open_position": eng.j.open_position(),
                          "live_stats": eng.stats(), "risk_block": eng._risk_block()}, indent=2, default=str))
        return
    if a.reset_risk:
        eng.reset_risk()
        print("risk governor reset")
        return

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    with SingleInstance(eng.cfg.lock_path):
        eng.j.event("INFO", "runner", f"engine started pid={os.getpid()} config={json.dumps(eng.cfg.to_dict(), default=str)}")
        if a.once:
            print(eng.tick(datetime.now(IST)))
            return
        eng.n.send(f"🚀 <b>{eng.cfg.name} engine online</b> (TPMR, paper mode)")
        while not _STOP:
            try:
                eng.tick(datetime.now(IST))
            except Exception as e:  # tick already contains errors; this is the last line of defence
                log.exception("tick crashed: %s", e)
            _sleep_until_next(30)
        eng.j.event("INFO", "runner", "engine stopped (signal)")


if __name__ == "__main__":
    sys.exit(main())
