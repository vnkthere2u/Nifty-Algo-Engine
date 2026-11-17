"""
ALGO SIGNALS — simple paper-trading dashboard (read-only).

The engine runs on GitHub Actions and saves its journal to the `algo-state` branch; this page only
reads that data (refreshes every minute). It never runs strategy code or fetches market data.

    streamlit run algo_dashboard.py
Optional: env/secret ALGO_STATE_URL (raw URL of the state branch) or NIFTY_ALGO_STATE_DIR (local files).
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
    "NIFTY": {"file": "nifty.db", "name": "NIFTY 50", "rule": "Buys dips in an uptrend · checked daily at 15:25 IST",
              "bt_win": 72.5, "money": False},
    "BTC": {"file": "btc.db", "name": "Bitcoin", "rule": "Buys calm breakouts in an uptrend · checked daily at 05:30 IST",
            "bt_win": 85.2, "money": True},
}
SIGNAL_ACTIONS = ("ENTRY", "EXIT", "SIGNAL", "BLOCKED_RISK", "MISSED_NO_DATA", "SKIPPED_STALE")

st.set_page_config(page_title="Algo Signals", page_icon="📈", layout="centered")
if st_autorefresh:
    st_autorefresh(interval=60_000, key="refresh")


# ------------------------------------------------------------------ helpers
def money(x, sign=False):
    """Indian digit grouping: 7796701 -> 77,96,701."""
    if x is None or pd.isna(x):
        return "—"
    neg = x < 0
    s = f"{abs(round(x)):d}"
    if len(s) > 3:
        head, tail = s[:-3], s[-3:]
        parts = []
        while len(head) > 2:
            parts.insert(0, head[-2:]); head = head[:-2]
        if head:
            parts.insert(0, head)
        s = ",".join(parts) + "," + tail
    pre = "-" if neg else ("+" if sign and x > 0 else "")
    return f"{pre}₹{s}"


def price(sid, x):
    if x is None or pd.isna(x):
        return "—"
    return money(x) if STRATS[sid]["money"] else f"{x:,.2f}"


def ist(ts):
    if not ts or pd.isna(ts):
        return "—"
    t = datetime.fromisoformat(str(ts))
    t = t.astimezone(IST) if t.tzinfo else t
    return t.strftime("%d %b, %H:%M")


@st.cache_data(ttl=60, show_spinner=False)
def fetch(name):
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


def q(path, sql):
    if not path:
        return pd.DataFrame()
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5) as c:
            return pd.read_sql_query(sql, c)
    except Exception:
        return pd.DataFrame()


def kv(path, key):
    d = q(path, f"SELECT value FROM kv WHERE key='{key}'")
    return json.loads(d.value.iloc[0]) if len(d) and d.value.iloc[0] else None


# ------------------------------------------------------------------ load
data = {}
for sid, m in STRATS.items():
    p = fetch(m["file"])
    data[sid] = dict(pos=q(p, "SELECT * FROM positions ORDER BY id"),
                     dec=q(p, "SELECT * FROM decisions ORDER BY trade_date DESC, id DESC"),
                     cond=kv(p, "conditions"))
sp = fetch("status.json")
status = json.load(open(sp)) if sp else None
now = datetime.now(IST)

# ------------------------------------------------------------------ header
st.title("📈 Algo Signals")
if status:
    upd = datetime.fromisoformat(status["updated"])
    mins = (now - upd).total_seconds() / 60
    ok = mins <= 90
    ago = f"{mins:.0f} min ago" if mins < 120 else f"{mins / 60:.1f} h ago"
    st.markdown(f"{'🟢' if ok else '🔴'} **Engine {'running' if ok else 'not updating'}** · "
                f"last updated **{upd:%d %b, %H:%M} IST** ({ago}) · paper trading, ₹1 lakh per trade")
    if not ok:
        st.warning("No engine update for over 90 minutes. Check github.com/vnkthere2u/Nifty-Algo-Engine/actions.")
else:
    st.warning("Engine data not available yet.")

# ------------------------------------------------------------------ 1. what it's tracking
st.subheader("What it's tracking")
cols = st.columns(2)
for col, (sid, m) in zip(cols, STRATS.items()):
    d = data[sid]
    c = d["cond"] or {}
    opn = d["pos"][d["pos"].status == "OPEN"] if len(d["pos"]) else pd.DataFrame()
    with col.container(border=True):
        st.markdown(f"**{m['name']}**")
        st.caption(m["rule"])
        if len(opn):
            p = opn.iloc[-1]
            last = c.get("price", p.last_price)
            ret = last / p.entry_price - 1
            st.markdown(f"🔵 **In trade** since {p.entry_date} · {ret * 100:+.2f}% ({money(ret * CAPITAL, True)})")
            exit_lv = next((x for x in c.get("levels", []) if x["level"].startswith(("EXIT", "Target"))), None)
            if exit_lv and exit_lv.get("price"):
                st.markdown(f"Sells when price goes above **{price(sid, exit_lv['price'])}**")
        else:
            st.markdown("⚪ **Watching** — no open trade")
            trig = next((x for x in c.get("levels", []) if x["level"].startswith(("Entry trigger", "Breakout"))), None)
            if trig and trig.get("price"):
                word = "at or below" if sid == "NIFTY" else "above"
                st.markdown(f"Buys if price closes {word} **{price(sid, trig['price'])}** "
                            f"({trig['dist_pct']:+.1f}% from now)" + (" with trend up" if sid == "BTC" else ""))
        st.markdown(f"Current price: **{price(sid, c.get('price'))}**")

# ------------------------------------------------------------------ 2. open trades
st.subheader("Open trades")
rows = []
for sid, d in data.items():
    for _, p in (d["pos"][d["pos"].status == "OPEN"] if len(d["pos"]) else pd.DataFrame()).iterrows():
        last = (d["cond"] or {}).get("price", p.last_price)
        ret = last / p.entry_price - 1
        rows.append({"Market": STRATS[sid]["name"], "Bought on": p.entry_date, "Buy price": price(sid, p.entry_price),
                     "Now": price(sid, last), "Result": f"{ret * 100:+.2f}%", "₹ P&L": money(ret * CAPITAL, True)})
if rows:
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
else:
    st.info("No open trades right now.")

# ------------------------------------------------------------------ 3. recent signals
st.subheader("Recent signals")
sig = []
for sid, d in data.items():
    if len(d["dec"]):
        s = d["dec"][d["dec"].action.isin(SIGNAL_ACTIONS)]
        for _, r in s.iterrows():
            note = " · ".join(json.loads(r.reasons or "[]"))
            sig.append({"Date": r.trade_date, "Market": STRATS[sid]["name"],
                        "Signal": {"ENTRY": "🟢 BUY", "EXIT": "🔴 SELL", "SIGNAL": "📣 Signal",
                                   "BLOCKED_RISK": "⛔ Blocked", "MISSED_NO_DATA": "⚠️ Missed",
                                   "SKIPPED_STALE": "⚠️ Skipped"}.get(r.action, r.action),
                        "Price": price(sid, r.price), "Why": note[:90]})
if sig:
    st.dataframe(pd.DataFrame(sig).sort_values("Date", ascending=False).head(15), hide_index=True, width="stretch")
else:
    st.info("No signals yet. NIFTY averages ~17 trades a year, BTC ~3.")

# ------------------------------------------------------------------ 4. past performance
st.subheader("Past performance")
closed = []
for sid, d in data.items():
    if len(d["pos"]):
        for _, p in d["pos"][d["pos"].status == "CLOSED"].iterrows():
            closed.append({"sid": sid, "Market": STRATS[sid]["name"], "Bought": p.entry_date, "Buy": price(sid, p.entry_price),
                           "Sold": p.exit_date, "Sell": price(sid, p.exit_price),
                           "Result %": 100 * p.net_inr / CAPITAL, "₹ P&L": p.net_inr,
                           "Reason": str(p.exit_reason).split(":")[0].title()})
if closed:
    cl = pd.DataFrame(closed)
    wins = (cl["₹ P&L"] > 0).sum()
    m = st.columns(4)
    m[0].metric("Closed trades", len(cl))
    m[1].metric("Win rate", f"{100 * wins / len(cl):.0f}%")
    m[2].metric("Total P&L", money(cl["₹ P&L"].sum(), True))
    m[3].metric("Avg per trade", money(cl["₹ P&L"].mean(), True))
    show = cl.drop(columns="sid").sort_values("Sold", ascending=False).copy()
    show["Result %"] = show["Result %"].map(lambda x: f"{x:+.2f}%")
    show["₹ P&L"] = show["₹ P&L"].map(lambda x: money(x, True))
    st.dataframe(show, hide_index=True, width="stretch")
    st.caption("Backtest win rates for comparison: NIFTY 72.5%, BTC 85.2%. "
               "Results are after trading costs, before tax.")
else:
    st.info("No closed trades yet.")

# ------------------------------------------------------------------ daily checks (collapsed)
with st.expander("Daily checks (proof the engine ran)"):
    for sid, d in data.items():
        if len(d["dec"]):
            t = d["dec"].head(10).copy()
            t["Result"] = t.action.str.replace("_", " ").str.title()
            t["Detail"] = t.reasons.map(lambda s: " · ".join(json.loads(s or "[]"))[:80])
            st.markdown(f"**{STRATS[sid]['name']}**")
            st.dataframe(t[["trade_date", "Result", "Detail"]].rename(columns={"trade_date": "Date"}),
                         hide_index=True, width="stretch")
