"""Process entry point for the paper-trading engines (NIFTY TPMR + BTC breakout).

    python -m nifty_algo.runner --once                 # one tick of every engine (hourly cron)
    python -m nifty_algo.runner --until 15:40          # tick every 30 s until 15:40 IST (NIFTY session job)
    python -m nifty_algo.runner                        # run forever (VPS / always-on machine)
    python -m nifty_algo.runner --status               # print state and exit
    python -m nifty_algo.runner --test-telegram        # send a test alert
    python -m nifty_algo.runner --reset-risk           # clear the NIFTY risk-governor pause

Single instance (file lock), sequential ticks (no overlapping scans), every failure contained and journaled.
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

from .btc import BtcEngine, BtcFeed, btc_db_path
from .config import IST, load_config, state_dir
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
        self.path, self.fh = path, None

    def __enter__(self):
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        self.fh = open(self.path, "w")
        try:
            fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"another engine instance holds {self.path}; refusing to start")
        self.fh.write(str(os.getpid())); self.fh.flush()
        return self

    def __exit__(self, *a):
        try:
            fcntl.flock(self.fh, fcntl.LOCK_UN); self.fh.close()
        except Exception:
            pass


def build_engines(names, cfg_path=None) -> list:
    cfg = load_config(cfg_path)
    notifier = Notifier(cfg.telegram_token, cfg.telegram_chat_id)
    engines = []
    if "nifty" in names:
        engines.append(Engine(cfg, Journal(cfg.db_path), YahooFeed(cfg.symbol), notifier))
    if "btc" in names:
        engines.append(BtcEngine(Journal(btc_db_path()), BtcFeed(), notifier))
    return engines


def tick_all(engines, now=None):
    out = {}
    for e in engines:
        name = getattr(e, "name", None) or e.cfg.name
        try:
            out[name] = e.tick(now or datetime.now(IST))
        except Exception as ex:  # engines contain their own errors; last line of defence
            log.exception("%s tick crashed: %s", name, ex)
            out[name] = f"crashed: {ex}"
    return out


def _sleep(step=30):
    target = (int(time.time()) // step + 1) * step + 2
    while not _STOP and time.time() < target:
        time.sleep(min(1.0, target - time.time()))


def write_status(engines, result):
    st = {"updated": datetime.now(IST).isoformat(timespec="seconds"), "host": os.environ.get("ALGO_HOST", os.uname().nodename),
          "last_result": {k: (v if isinstance(v, (list, str)) else str(v)) for k, v in result.items()}}
    with open(os.path.join(state_dir(), "status.json"), "w") as fh:
        json.dump(st, fh, indent=2)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--strategies", default="nifty,btc")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--until", default=None, help="HH:MM IST")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--reset-risk", action="store_true")
    ap.add_argument("--test-telegram", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    a = ap.parse_args(argv)
    logging.basicConfig(level=a.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    engines = build_engines([s.strip().lower() for s in a.strategies.split(",")], a.config)

    if a.test_telegram:
        ok = engines[0].n.send("🔔 Algo engine test alert — Telegram is wired correctly.")
        print("sent" if ok else "NOT sent (check TELEGRAM_TOKEN / TELEGRAM_CHAT_ID)")
        return 0 if ok else 1
    if a.status:
        for e in engines:
            print(getattr(e, "name", "NIFTY"), json.dumps({"heartbeat": e.j.get("heartbeat"), "open": e.j.open_position()}, default=str, indent=1))
        return 0
    if a.reset_risk:
        for e in engines:
            if isinstance(e, Engine):
                e.reset_risk()
        print("risk governor reset"); return 0

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    lock = os.path.join(state_dir(), "engine.lock")
    with SingleInstance(lock):
        try:
            if a.once:
                res = tick_all(engines); print(json.dumps(res, default=str)); write_status(engines, res)
                return 0
            end = None
            if a.until:
                hh, mm = map(int, a.until.split(":"))
                end = datetime.now(IST).replace(hour=hh, minute=mm, second=0, microsecond=0)
            while not _STOP:
                res = tick_all(engines); write_status(engines, res)
                if end and datetime.now(IST) >= end:
                    break
                _sleep(30)
        finally:
            for e in engines:
                try:
                    e.j.checkpoint()
                except Exception:
                    pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
