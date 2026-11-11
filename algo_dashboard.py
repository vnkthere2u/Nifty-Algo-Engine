"""
ALGO ENGINE — paper-trading dashboard (NIFTY TPMR + BTC low-vol breakout).

Read-only. The engine runs on GitHub Actions (never sleeps) and saves its journal to the
`algo-state` branch; this page downloads those files every minute. It never fetches market
data or runs strategy code, so it cannot hang, overlap scans, or delay execution.

    streamlit run algo_dashboard.py
Config (optional): env/secret ALGO_STATE_URL (raw URL of the state branch) or
NIFTY_ALGO_STATE_DIR (read local files instead, e.g. when the engine runs on the same machine).
"""
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

IST = timezone(timedelta(hours=5, minutes=30))
CAPITAL = 100_000.0


def _secret(key, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


STATE_URL = os.environ.get("ALGO_STATE_URL") or _secret(
    "ALGO_STATE_URL", "https://raw.githubusercontent.com/vnkthere2u/Nifty-Algo-Engine/algo-state")
LOCAL_DIR = os.environ.get("NIFTY_ALGO_STATE_DIR")

STRATS = {
    "NIFTY": {"file": "nifty.db", "title": "NIFTY 50 · Trend-Pullback Mean Reversion",
              "bt": {"win": 72.5, "ci": "65.0–79.0", "pf": 1.57, "n": 153, "per_yr": 16.7},
              "rules": "15:25 IST daily: price > SMA50 and (RSI(2) < 10 or IBS < 0.15) → BUY. "
                       "Exit at a later 15:25 when price > previous day's high, or after 10 days. Disaster stop 8×ATR."},
    "BTC": {"file": "btc.db", "title": "BTC · Low-volatility trend breakout",
            "bt": {"win": 85.2, "ci": "67–95", "pf": 9.94, "n": 27, "per_yr": 2.7},
            "rules": "Daily (UTC): EMA20 > EMA50 > EMA200 and close > prior 20-day high → BUY next open. "
                     "Stop 3.5×ATR (daily-close basis) + hard −10%; target 2R; break-even at +1R; skip if stop > 10%."},
}

st.set_page_config(page_title="Algo Engine", layout="wide", initial_sidebar_state="collapsed")
if st_autorefresh:
    st_autorefresh(interval=60_000, key="refresh")
st.markdown("""<style>
.block-container{padding:0.8rem 1rem 0.5rem!important;max-width:100%!important}
.mc{background:var(--secondary-background-color);border-radius:8px;padding:10px 14px;text-align:center;height:100%}
.mc-l{font-size:.70rem;opacity:.7;font-weight:600;text-transform:uppercase;letter-spacing:.6px}
.mc-v{font-size:1.35rem;font-weight:700;line-height:1.25}
.mc-s{font-size:.74rem;opacity:.75}
</style>""", unsafe_allow_html=True)


# ------------------------------------------------------------------ data access
@st.cache_data(ttl=60, show_spinner=False)
def fetch(name: str):
    """Return a local path to the state file (downloaded from the state branch), or None."""
    if LOCAL_DIR:
        p = os.path.join(LOCAL_DIR, name)
        return p if os.path.exists(p) else None
    try:
        r = requests.get(f"{STATE_URL}/{name}", timeout=15, headers={"Cache-Control": "no-cache"})
        if r.status_code != 200:
            return None
        fd, path = tempfile.mkstemp(suffix="_" + name)
        with os.fdopen(fd, "wb") as fh:
            fh.write(r.content)
        return path
    except Exception:
        return None


def q(path, sql, params=()):
    if not path:
        return pd.DataFrame()
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5) as c:
            return pd.read_sql_query(sql, c, params=params)
    except Exception:
        return pd.DataFrame()


def kv(path, key):
    df = q(path, "SELECT value FROM kv WHERE key=?", (key,))
    return json.loads(df.value.iloc[0]) if len(df) and df.value.iloc[0] is not None else None


def card(col, label, value, sub=""):
    col.markdown(f"<div class='mc'><div class='mc-l'>{label}</div><div class='mc-v'>{value}</div>"
                 f"<div class='mc-s'>{sub}</div></div>", unsafe_allow_html=True)


def inr(x):
    return f"₹{x:+,.0f}"


def load(sid):
    path = fetch(STRATS[sid]["file"])
    pos = q(path, "SELECT * FROM positions ORDER BY id")
    dec = q(path, "SELECT * FROM decisions ORDER BY id DESC LIMIT 400")
    ev = q(path, "SELECT ts, level, component, message FROM events ORDER BY id DESC LIMIT 200")
    closed = pos[pos.status == "CLOSED"].copy() if len(pos) else pos
    opn = pos[pos.status == "OPEN"].copy() if len(pos) else pos
    return dict(path=path, closed=closed, open=opn, dec=dec, ev=ev, hb=kv(path, "heartbeat"))


def stats(closed):
    if closed is None or len(closed) == 0:
        return dict(n=0, win=0.0, be=0, net=0.0)
    w = closed.net_inr > 0
    be = closed.exit_reason.fillna("").str.contains("BE").sum()
    return dict(n=len(closed), win=100 * w.mean(), be=int(be), net=float(closed.net_inr.sum()))


# ------------------------------------------------------------------ header
now = datetime.now(IST)
st.markdown("### ⚙️ Algo Engine — paper trading")
status_path = fetch("status.json")
status = json.load(open(status_path)) if status_path else None
data = {sid: load(sid) for sid in STRATS}

c = st.columns(4)
if status:
    upd = datetime.fromisoformat(status["updated"])
    age_min = (now - upd).total_seconds() / 60
    ok = age_min <= 90
    card(c[0], "Engine", "🟢 running" if ok else "🔴 no recent run", f"last run {upd:%d-%b %H:%M} IST ({age_min:.0f} min ago)")
else:
    card(c[0], "Engine", "⚪ no state yet", "waiting for the first GitHub Actions run")
allc = pd.concat([d["closed"].assign(strategy=s) for s, d in data.items() if len(d["closed"])], ignore_index=True) \
    if any(len(d["closed"]) for d in data.values()) else pd.DataFrame()
S = stats(allc)
unreal = 0.0
for d in data.values():
    for _, p in d["open"].iterrows():
        if pd.notna(p.last_price):
            unreal += (p.last_price / p.entry_price - 1) * CAPITAL
card(c[1], "Closed paper trades", S["n"], f"win {S['win']:.1f}% · {S['be']} break-even" if S["n"] else "none yet")
card(c[2], "Realised P&L", inr(S["net"]) if S["n"] else "₹0", "₹1 L notional per trade")
card(c[3], "Open P&L", inr(unreal), f"{sum(len(d['open']) for d in data.values())} open position(s)")

st.write("")
cols = st.columns(len(STRATS))
for col, (sid, meta) in zip(cols, STRATS.items()):
    d = data[sid]
    with col:
        st.markdown(f"**{meta['title']}**")
        k = st.columns(3)
        if len(d["open"]):
            p = d["open"].iloc[-1]
            ret = (p.last_price / p.entry_price - 1) if pd.notna(p.last_price) else 0.0
            card(k[0], "Position", f"LONG {100 * ret:+.2f}%", f"entry {p.entry_price:,.2f} · {inr(ret * CAPITAL)}")
        else:
            card(k[0], "Position", "FLAT", "waiting for setup")
        if len(d["dec"]):
            r = d["dec"].iloc[0]
            card(k[1], "Last decision", r.action, f"{r.trade_date}")
        else:
            card(k[1], "Last decision", "—", "")
        s = stats(d["closed"])
        card(k[2], "Paper record", f"{s['n']} trades", f"win {s['win']:.0f}% (bt {meta['bt']['win']}%) · {inr(s['net'])}" if s["n"] else f"backtest win {meta['bt']['win']}%")

st.write("")
tabs = st.tabs(["📈 Performance", "📂 Open positions", "📒 Trade ledger", "🧭 Signals & decisions", "🛠 Events", "📜 Rules"])

with tabs[0]:
    if len(allc):
        e = allc.sort_values("exit_ts")[["exit_date", "net_inr", "strategy"]].copy()
        e["Cumulative P&L (₹)"] = e.net_inr.cumsum()
        st.markdown("**Combined paper equity (closed trades)**")
        st.line_chart(e.set_index("exit_date")["Cumulative P&L (₹)"], height=260)
        rows = []
        for sid, d in data.items():
            s = stats(d["closed"]); bt = STRATS[sid]["bt"]
            flag = ""
            if s["n"] >= 15 and s["win"] < float(bt["ci"].split("–")[0]):
                flag = "⚠️ below backtest range"
            rows.append({"strategy": sid, "trades": s["n"], "win %": round(s["win"], 1), "break-evens": s["be"],
                         "net ₹": round(s["net"]), "backtest win %": bt["win"], "backtest 95% range": bt["ci"],
                         "backtest trades/yr": bt["per_yr"], "check": flag})
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    else:
        st.info("No closed paper trades yet. NIFTY averages ~17 trades/yr and BTC ~3/yr, so the first ones can take days to weeks.")

with tabs[1]:
    rows = []
    for sid, d in data.items():
        for _, p in d["open"].iterrows():
            ret = (p.last_price / p.entry_price - 1) if pd.notna(p.last_price) else 0.0
            rows.append({"strategy": sid, "entry date": p.entry_date, "entry": round(p.entry_price, 2),
                         "last": round(p.last_price, 2) if pd.notna(p.last_price) else None,
                         "last update": p.last_price_ts, "P&L %": round(100 * ret, 2), "P&L ₹": round(ret * CAPITAL),
                         "stop": round(p.disaster_stop, 2) if pd.notna(p.disaster_stop) else None,
                         "setup": p.setup, "confidence %": p.confidence, "days": p.days_held})
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    else:
        st.info("No open positions.")

with tabs[2]:
    if len(allc):
        led = allc[["strategy", "entry_date", "entry_price", "exit_date", "exit_price", "exit_reason", "setup",
                    "confidence", "net_inr"]].copy()
        led["return %"] = 100 * led.net_inr / CAPITAL
        st.dataframe(led.sort_values("exit_date", ascending=False), hide_index=True, width="stretch")
        st.download_button("Download ledger CSV", led.to_csv(index=False), "paper_ledger.csv", "text/csv")
    else:
        st.info("No closed trades yet.")

with tabs[3]:
    for sid, d in data.items():
        st.markdown(f"**{sid}**")
        dec = d["dec"]
        if len(dec):
            dec = dec.copy()
            dec["reasons"] = dec.reasons.apply(lambda s: " | ".join(json.loads(s or "[]")))
            st.dataframe(dec[["trade_date", "ts", "action", "setup", "price", "confidence", "reasons"]],
                         hide_index=True, width="stretch", height=260)
        else:
            st.caption("No decisions recorded yet.")

with tabs[4]:
    ev = pd.concat([d["ev"].assign(strategy=s) for s, d in data.items() if len(d["ev"])], ignore_index=True) \
        if any(len(d["ev"]) for d in data.values()) else pd.DataFrame()
    if len(ev):
        st.dataframe(ev.sort_values("ts", ascending=False), hide_index=True, width="stretch")
    else:
        st.info("No events.")
    if status:
        st.caption(f"Engine host: {status.get('host')} · last result: {status.get('last_result')}")

with tabs[5]:
    for sid, meta in STRATS.items():
        bt = meta["bt"]
        st.markdown(f"**{meta['title']}**  \n{meta['rules']}  \n"
                    f"Backtest: {bt['n']} trades, win {bt['win']}% (95% range {bt['ci']}%), profit factor {bt['pf']}, "
                    f"~{bt['per_yr']} trades/yr.")
    st.caption("Paper trading only. ₹1 L notional per trade, no leverage. NIFTY P&L uses futures-equivalent costs (0.04%); "
               "BTC uses 0.6% round trip. Taxes not deducted.")
