"""Download NIFTY 1-minute history (public dataset: github.com/technovusin/nifty50-historical-data)
into data/raw for backtesting/replay.   python scripts/fetch_history.py"""
import datetime as dt
import os
import sys
import time

import requests

BASE = "https://raw.githubusercontent.com/technovusin/nifty50-historical-data/HEAD/1min"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")


def fetch(url, path):
    for attempt in range(4):
        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 404:
                return False
            r.raise_for_status()
            with open(path, "wb") as fh:
                fh.write(r.content)
            return True
        except Exception as e:
            print("retry", url, e, file=sys.stderr)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"failed: {url}")


def main():
    os.makedirs(OUT, exist_ok=True)
    today = dt.date.today()
    targets = [(f"{BASE}/{y}/NIFTY50_1min_{y}.csv", f"NIFTY50_1min_{y}.csv") for y in range(2017, 2026)]
    for y in range(2026, today.year + 1):
        for m in range(1, 13):
            if dt.date(y, m, 1) > today:
                break
            targets.append((f"{BASE}/{y}/NIFTY50_1min_{y}-{m:02d}.csv", f"NIFTY50_1min_{y}-{m:02d}.csv"))
    for url, name in targets:
        ok = fetch(url, os.path.join(OUT, name))
        print(("ok   " if ok else "miss ") + name)


if __name__ == "__main__":
    main()
