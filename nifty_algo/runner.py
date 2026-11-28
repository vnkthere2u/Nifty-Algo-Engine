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
import subprocess
import sys
import time
from datetime import datetime, timedelta

from .btc import BtcEngine, BtcFeed, btc_db_path
from .config import IST, load_config, state_dir
from .engine import Engine
from .feeds import YahooFeed
from .journal import Journal
from .momentum import MomentumEngine, MomentumFeed, momentum_db_path
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
    if "momentum" in names:
        engines.append(MomentumEngine(Journal(momentum_db_path()), MomentumFeed(), notifier))
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


def publish(engines, cmd: str):
    """Fold each journal's WAL into its .db file, then run the publish command (e.g. push to the state branch)."""
    for e in engines:
        try:
            e.j.checkpoint()
        except Exception:
            pass
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
        log.info("publish: %s %s", r.returncode, (r.stdout or r.stderr).strip()[-200:])
    except Exception as ex:
        log.warning("publish failed: %s", ex)


def write_status(engines, result):
    st = {"updated": datetime.now(IST).isoformat(timespec="seconds"), "host": os.environ.get("ALGO_HOST", os.uname().nodename),
          "last_result": {k: (v if isinstance(v, (list, str)) else str(v)) for k, v in result.items()}}
    with open(os.path.join(state_dir(), "status.json"), "w") as fh:
        json.dump(st, fh, indent=2)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None)
    ap.add_argument("--strategies", default="nifty,btc,momentum")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--until", default=None, help="HH:MM IST (today); a late start never loops into the next day")
    ap.add_argument("--for", dest="for_min", type=float, default=None, help="loop for N minutes")
    ap.add_argument("--sync-every", type=float, default=0, help="minutes between --sync-cmd calls (0 = off)")
    ap.add_argument("--sync-cmd", default=None, help="command that publishes the state (e.g. bash deploy/state.sh push)")
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
            start = datetime.now(IST)
            end = None
            if a.until:
                hh, mm = map(int, a.until.split(":"))
                end = start.replace(hour=hh, minute=mm, second=0, microsecond=0)
                if end <= start or (end - start).total_seconds() > 3 * 3600:
                    end = start          # started too late/early (delayed trigger): one tick, then exit
            if a.for_min:
                fe = start + timedelta(minutes=a.for_min)
                end = min(end, fe) if end else fe
            last_sync = time.time()
            while not _STOP:
                res = tick_all(engines); write_status(engines, res)
                if a.sync_cmd and a.sync_every and time.time() - last_sync >= a.sync_every * 60:
                    publish(engines, a.sync_cmd)
                    last_sync = time.time()
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
