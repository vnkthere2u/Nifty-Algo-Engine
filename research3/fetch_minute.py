"""Stream NIFTY-100 1-minute CSVs (public repo akshaj23/INTRADAY-PAIR-TRADING-NIFTY-100-STOCKS)
and store session-anchored 5-minute OHLCV bars (data/m5/<SYM>.pkl)."""
import io, os, sys, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor
import pandas as pd, requests

BASE = "https://raw.githubusercontent.com/akshaj23/INTRADAY-PAIR-TRADING-NIFTY-100-STOCKS/HEAD/"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "m5")

def sym_of(path):
    b = os.path.basename(path)
    return b.replace("_minute_data.csv", "").replace("_minute.csv", "")

def one(path):
    sym = sym_of(path)
    dst = os.path.join(OUT, f"{sym}.pkl")
    if os.path.exists(dst):
        return sym, "cached"
    for a in range(4):
        try:
            r = requests.get(BASE + urllib.parse.quote(path), timeout=300)
            r.raise_for_status(); break
        except Exception as e:
            time.sleep(2 ** a); err = e
    else:
        return sym, f"FAIL {err}"
    d = pd.read_csv(io.BytesIO(r.content))
    d.columns = [c.strip().lower() for c in d.columns]
    d["date"] = pd.to_datetime(d["date"].astype(str).str.slice(0, 19))
    d = d.drop_duplicates("date").set_index("date").sort_index()
    m = d.index.hour * 60 + d.index.minute
    d = d[(m >= 555) & (m <= 929)]
    if "volume" not in d: d["volume"] = 0
    b = d.resample("5min", origin="start_day", offset="15min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna(subset=["close"])
    b.columns = ["Open", "High", "Low", "Close", "Volume"]
    b.to_pickle(dst)
    return sym, f"{len(d)} 1m -> {len(b)} 5m  {b.index[0].date()}..{b.index[-1].date()}"

paths = [p.strip() for p in open("minute_files.txt") if p.strip() and not p.startswith("output/")]
with ThreadPoolExecutor(6) as ex:
    for sym, msg in ex.map(one, paths):
        print(f"{sym:20s} {msg}", flush=True)
