"""Daily (split/dividend-adjusted) Yahoo history for NIFTY 50 + NIFTY Next 50 (approx. 2025-26 constituents)."""
import os, time, pandas as pd, requests
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "yahoo100")
N50 = "ADANIENT ADANIPORTS APOLLOHOSP ASIANPAINT AXISBANK BAJAJ-AUTO BAJFINANCE BAJAJFINSV BEL BHARTIARTL CIPLA COALINDIA DRREDDY EICHERMOT ETERNAL GRASIM HCLTECH HDFCBANK HDFCLIFE HINDALCO HINDUNILVR ICICIBANK INDIGO INFY ITC JIOFIN JSWSTEEL KOTAKBANK LT M&M MARUTI MAXHEALTH NESTLEIND NTPC ONGC POWERGRID RELIANCE SBILIFE SHRIRAMFIN SBIN SUNPHARMA TCS TATACONSUM TMPV TATASTEEL TECHM TITAN TRENT ULTRACEMCO WIPRO".split()
NN50 = "ABB ADANIENSOL ADANIGREEN ADANIPOWER AMBUJACEM BAJAJHLDNG BANKBARODA BHEL BOSCHLTD BPCL BRITANNIA CANBK CGPOWER CHOLAFIN DABUR DIVISLAB DLF DMART GAIL GODREJCP HAL HAVELLS HINDZINC HYUNDAI ICICIGI ICICIPRULI INDHOTEL IOC IRFC JINDALSTEL JSWENERGY LICI LODHA LTIM MOTHERSON NAUKRI PFC PIDILITIND PNB RECLTD SHREECEM SIEMENS SOLARINDS TATAPOWER TORNTPHARM TVSMOTOR UNITDSPR VBL VEDL ZYDUSLIFE".split()
S = requests.Session(); S.headers["User-Agent"] = "Mozilla/5.0"

def get(sym):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}"
    for a in range(4):
        r = S.get(url, params={"interval": "1d", "period1": 1230768000, "period2": int(time.time()),
                               "events": "div,splits", "includeAdjustedClose": "true"}, timeout=30)
        if r.status_code == 200: break
        time.sleep(2 ** a)
    res = r.json()["chart"]["result"]
    if not res or not res[0].get("timestamp"): return None
    res = res[0]; q = res["indicators"]["quote"][0]
    idx = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert("Asia/Kolkata").tz_localize(None).normalize()
    d = pd.DataFrame({k.capitalize(): q[k] for k in ("open", "high", "low", "close", "volume")}, index=idx)
    d["Adj"] = res["indicators"]["adjclose"][0]["adjclose"]
    d = d.dropna(subset=["Open", "High", "Low", "Close", "Adj"]); d = d[(d[["Open", "High", "Low", "Close"]] > 0).all(axis=1)]
    f = d.Adj / d.Close
    for c in ("Open", "High", "Low", "Close"): d[c] = d[c] * f
    return d[~d.index.duplicated(keep="last")][["Open", "High", "Low", "Close", "Volume"]]

if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True); rows = []
    for grp, lst in (("N50", N50), ("NN50", NN50), ("IDX", ["^NSEI"])):
        for s in lst:
            try:
                d = get(s if s.startswith("^") else s + ".NS")
            except Exception as e:
                d = None
            if d is None or len(d) < 50:
                rows.append((grp, s, 0, None)); print("MISSING", s); continue
            d.to_pickle(os.path.join(OUT, f"{s.replace('^','')}.pkl"))
            g = (d.Open / d.Close.shift() - 1).abs()
            rows.append((grp, s, len(d), d.index[0].date(), ", ".join(f"{i.date()}:{v*100:+.0f}%" for i, v in (d.Open / d.Close.shift() - 1)[g > 0.18].items())[:80]))
            time.sleep(0.3)
    pd.DataFrame(rows).to_csv(os.path.join(OUT, "_universe.csv"), index=False)
    print(pd.DataFrame(rows).to_string())
