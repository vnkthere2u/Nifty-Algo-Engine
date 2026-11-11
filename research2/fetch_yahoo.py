"""Download daily (max) and 1h (730d) bars from Yahoo v8 chart API, split/dividend adjusted."""
import json, os, sys, time
import pandas as pd, requests

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "yahoo")
UNIVERSE = {
 "NIFTY": "^NSEI", "BANKNIFTY": "^NSEBANK",
 "HDFCBANK": "HDFCBANK.NS", "SBIN": "SBIN.NS", "RELIANCE": "RELIANCE.NS", "INFY": "INFY.NS", "TCS": "TCS.NS",
 "ITC": "ITC.NS", "TATAMOTORS": "TATAMOTORS.NS", "TMPV": "TMPV.NS", "TATASTEEL": "TATASTEEL.NS", "LT": "LT.NS",
 "BHARTIARTL": "BHARTIARTL.NS", "SUNPHARMA": "SUNPHARMA.NS", "VEDL": "VEDL.NS",
 "GOLD": "GC=F", "SILVER": "SI=F", "CRUDE": "CL=F", "BTC": "BTC-USD",
}
S = requests.Session(); S.headers["User-Agent"] = "Mozilla/5.0"

def chart(sym, interval, **params):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}"
    p = {"interval": interval, "events": "div,splits", "includeAdjustedClose": "true", **params}
    for a in range(4):
        r = S.get(url, params=p, timeout=30)
        if r.status_code == 200: break
        time.sleep(2 ** a)
    js = r.json()["chart"]
    if js.get("error") or not js["result"] or not js["result"][0].get("timestamp"):
        return None, js.get("error")
    res = js["result"][0]; q = res["indicators"]["quote"][0]
    tz = res["meta"].get("exchangeTimezoneName", "UTC")
    idx = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert(tz).tz_localize(None)
    df = pd.DataFrame({k.capitalize(): q[k] for k in ("open", "high", "low", "close", "volume")}, index=idx)
    if "adjclose" in res["indicators"]:
        df["Adj"] = res["indicators"]["adjclose"][0]["adjclose"]
    return df, res["meta"]

for name, sym in UNIVERSE.items():
    d, meta = chart(sym, "1d", period1=0, period2=int(time.time()))
    if d is None:
        print(f"{name:11s} {sym:14s} NO DATA {meta}"); continue
    d = d.dropna(subset=["Open", "High", "Low", "Close"])
    d = d[(d[["Open", "High", "Low", "Close"]] > 0).all(axis=1)]
    if "Adj" in d:  # back-adjust OHLC for splits/dividends
        f = (d["Adj"] / d["Close"]).fillna(1.0)
        for c in ("Open", "High", "Low", "Close"): d[c] = d[c] * f
    d.index = d.index.normalize(); d = d[~d.index.duplicated(keep="last")]
    d.to_pickle(f"{OUT}/{name}_1d.pkl")
    h, _ = chart(sym, "1h", range="730d")
    nh = 0
    if h is not None:
        h = h.dropna(subset=["Open", "High", "Low", "Close"]); h = h[(h[["Open","High","Low","Close"]] > 0).all(axis=1)]
        h.to_pickle(f"{OUT}/{name}_1h.pkl"); nh = len(h)
    print(f"{name:11s} {sym:14s} daily {len(d):5d} {d.index[0].date()} -> {d.index[-1].date()}  last {d.Close.iloc[-1]:.2f} | 1h {nh}")
    time.sleep(0.5)
