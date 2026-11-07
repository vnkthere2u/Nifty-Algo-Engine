"""
ALGO ENGINE TERMINAL v4.0 - read-only dashboard for the NIFTY TPMR engine.

This UI never fetches market data and never runs strategy code: it only reads the SQLite
journal written by the engine process (`python -m nifty_algo.runner`).  Refreshing it can
therefore never hang, overlap, or delay execution.

    streamlit run algo_dashboard.py
"""
import json
import os
import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

from nifty_algo.config import IST, load_config

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:  # optional
    st_autorefresh = None

CFG = load_config(os.environ.get("NIFTY_ALGO_CONFIG"))
BT_SUMMARY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nifty_algo", "backtest_summary.json")
BT_TRADES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nifty_algo", "backtest_trades.csv")

st.set_page_config(page_title="Algo Engine", layout="wide", initial_sidebar_state="collapsed")
if st_autorefresh:
    st_autorefresh(interval=30_000, key="refresh")

st.markdown("""<style>
.block-container{padding:0.8rem 1rem 0.5rem!important;max-width:100%!important}
.mc{background:var(--secondary-background-color);border-radius:8px;padding:10px 14px;text-align:center}
.mc-l{font-size:.70rem;opacity:.7;font-weight:600;text-transform:uppercase;letter-spacing:.6px}
.mc-v{font-size:1.45rem;font-weight:700;line-height:1.2}
.mc-s{font-size:.72rem;opacity:.7}
</style>""", unsafe_allow_html=True)


def q(sql, params=()):
    if not os.path.exists(CFG.db_path):
        return pd.DataFrame()
    with sqlite3.connect(f"file:{CFG.db_path}?mode=ro", uri=True, timeout=5) as c:
        return pd.read_sql_query(sql, c, params=params)


def kv(key):
    df = q("SELECT value FROM kv WHERE key=?", (key,))
    return json.loads(df.value.iloc[0]) if len(df) else None


def card(col, label, value, sub=""):
    col.markdown(f"<div class='mc'><div class='mc-l'>{label}</div><div class='mc-v'>{value}</div>"
                 f"<div class='mc-s'>{sub}</div></div>", unsafe_allow_html=True)


st.markdown(f"### ⚙️ Algo Engine — {CFG.name} · Trend-Pullback Mean Reversion")

if not os.path.exists(CFG.db_path):
    st.warning("No journal yet. Start the engine: `python -m nifty_algo.runner` "
               f"(expected DB at `{CFG.db_path}`).")

# ---------------------------------------------------------------- status row
now = datetime.now(IST)
hb = kv("heartbeat")
hb_age = (now - datetime.fromisoformat(hb["ts"])).total_seconds() if hb else None
closed = q("SELECT * FROM positions WHERE status='CLOSED' ORDER BY id")
openp = q("SELECT * FROM positions WHERE status='OPEN' ORDER BY id DESC LIMIT 1")
last_dec = q("SELECT * FROM decisions ORDER BY id DESC LIMIT 1")
bt = json.load(open(BT_SUMMARY)) if os.path.exists(BT_SUMMARY) else {}

c = st.columns(6)
if hb_age is None:
    card(c[0], "Engine", "⚪ offline", "no heartbeat")
else:
    ok = hb_age < 120
    card(c[0], "Engine", "🟢 running" if ok else "🔴 stalled", f"heartbeat {int(hb_age)}s ago")
if len(last_dec):
    r = last_dec.iloc[0]
    card(c[1], "Last decision", r.action, f"{r.trade_date} @ {r.price if pd.notna(r.price) else '-'}")
else:
    card(c[1], "Last decision", "—", "waiting for 15:25 IST")
n = len(closed)
wins = int((closed.net_pts > 0).sum()) if n else 0
card(c[2], "Live trades", n, f"win {100 * wins / n:.1f}%" if n else "no closed trades")
card(c[3], "Live net", f"{closed.net_pts.sum():+,.0f} pts" if n else "0 pts",
     f"₹{closed.net_inr.sum():+,.0f}" if n else "")
if len(openp):
    p = openp.iloc[0]
    u = (p.last_price - p.entry_price) if pd.notna(p.last_price) else 0.0
    card(c[4], "Open position", f"LONG {u:+,.0f} pts", f"from {p.entry_price:,.2f} · day {int(p.days_held or 0)}")
else:
    card(c[4], "Open position", "FLAT", "")
card(c[5], "Backtest win rate", f"{bt.get('win_rate_pct', '—')}%",
     f"95% CI {bt.get('win_rate_95ci', ['-', '-'])[0]}–{bt.get('win_rate_95ci', ['-', '-'])[1]}% · n={bt.get('trades', '-')}")

st.write("")
t_today, t_pos, t_ledger, t_dec, t_perf, t_bt, t_log = st.tabs(
    ["📡 Today", "📂 Open position", "📒 Trade ledger", "🧭 Decision log", "📈 Live performance",
     "🧪 Backtest", "🛠 Events"])

with t_today:
    if len(last_dec):
        r = last_dec.iloc[0]
        st.markdown(f"**{r.trade_date} — {r.action}**  · setup `{r.setup}`"
                    + (f" · confidence **{r.confidence:.1f}%** (90% floor {r.confidence_low:.1f}%)" if pd.notna(r.confidence) else ""))
        for reason in json.loads(r.reasons or "[]"):
            st.markdown(f"- {reason}")
        f = json.loads(r.features or "{}")
        if f:
            keep = ["Close", "High", "Low", "prev_high", "rsi", "ibs", "sma_trend", "atr", "trend_dist_atr"]
            st.dataframe(pd.DataFrame([{k: f.get(k) for k in keep}]), hide_index=True, width="stretch")
    st.caption("Rules: at 15:25 IST, if price > SMA50(daily) and (RSI(2) < 10 or IBS < 0.15) → BUY. "
               "Exit at a later 15:25 when price > previous day's high, or after 10 days. "
               "Disaster stop 8×ATR (insurance only).")

with t_pos:
    if len(openp):
        st.dataframe(openp.drop(columns=["features"]).T.rename(columns={0: "value"}), width="stretch")
    else:
        st.info("No open position.")

with t_ledger:
    if n:
        show = closed[["id", "entry_date", "entry_price", "exit_date", "exit_price", "days_held", "setup",
                       "confidence", "exit_reason", "mfe_pts", "mae_pts", "net_pts", "net_inr"]]
        st.dataframe(show.iloc[::-1], hide_index=True, width="stretch")
        st.download_button("Download ledger CSV", closed.to_csv(index=False), "ledger.csv", "text/csv")
    else:
        st.info("No closed trades yet.")

with t_dec:
    d = q("SELECT trade_date, ts, action, setup, price, confidence, reasons FROM decisions ORDER BY id DESC LIMIT 500")
    if len(d):
        d["reasons"] = d.reasons.apply(lambda s: " | ".join(json.loads(s or "[]")))
        st.dataframe(d, hide_index=True, width="stretch")
    else:
        st.info("No decisions recorded yet.")

with t_perf:
    if n:
        eq = closed[["exit_date", "net_pts"]].copy()
        eq["Cumulative net (pts)"] = eq.net_pts.cumsum()
        st.markdown("**Live equity curve (closed trades, index points)**")
        st.line_chart(eq.set_index("exit_date")["Cumulative net (pts)"], height=260)
        exp_wr = bt.get("win_rate_pct")
        lo = bt.get("win_rate_95ci", [None])[0]
        live_wr = 100 * wins / n
        msg = f"Live win rate {live_wr:.1f}% over {n} trades vs backtest {exp_wr}% (95% CI lower bound {lo}%)."
        if n >= 15 and lo is not None and live_wr < lo:
            st.error("⚠️ Edge-degradation alert: " + msg)
        else:
            st.success(msg)
        by = closed.groupby("setup").agg(trades=("net_pts", "size"), win_pct=("net_pts", lambda x: round(100 * (x > 0).mean(), 1)),
                                         net_pts=("net_pts", "sum")).reset_index()
        st.dataframe(by, hide_index=True)
    else:
        st.info("Live performance appears after the first closed trade.")

with t_bt:
    if bt:
        cols = st.columns(5)
        card(cols[0], "Trades", bt["trades"], f"{bt['trades_per_year']}/yr")
        card(cols[1], "Win rate", f"{bt['win_rate_pct']}%", f"OOS 2023+: {bt['out_of_sample_2023_plus']['win_pct']}%")
        card(cols[2], "Profit factor", bt["profit_factor"], f"OOS {bt['out_of_sample_2023_plus']['pf']}")
        card(cols[3], "Expectancy", f"{bt['expectancy_pts']} pts", f"avg win {bt['avg_win_pts']} / loss {bt['avg_loss_pts']}")
        card(cols[4], "Max drawdown", f"{bt['max_drawdown_pts']} pts", f"worst trade {bt['worst_trade_pts']}")
        if os.path.exists(BT_TRADES):
            btt = pd.read_csv(BT_TRADES)
            btt["Cumulative net (pts)"] = btt.net_pts.cumsum()
            st.markdown("**Backtest equity curve 2017–2026 (index points, net of costs)**")
            st.line_chart(btt.set_index("exit_date")["Cumulative net (pts)"], height=260)
        yr = pd.DataFrame(bt["by_year"]).T
        yr.index.name = "year"
        st.dataframe(yr, width="stretch")
        st.dataframe(pd.DataFrame(bt["by_setup"]).T, width="stretch")
    else:
        st.info("Run `python -m nifty_algo.backtest --write-priors` to generate the backtest summary.")

with t_log:
    ev = q("SELECT ts, level, component, message FROM events ORDER BY id DESC LIMIT 300")
    st.dataframe(ev, hide_index=True, width="stretch") if len(ev) else st.info("No events.")
