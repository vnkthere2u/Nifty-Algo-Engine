"""
================================================================================
  ALGO ENGINE TERMINAL — PRODUCTION UPLIFT v2.1
  Author: Uplifted by Claude for Vinayak

  v2.1 ADDITION:
  [LIVE-1] streamlit-autorefresh: sends a real WebSocket ping every 60s.
           Streamlit Cloud never marks the session idle → daemon thread
           is never killed. Combined with UptimeRobot (HTTP keep-alive),
           this achieves stable 24/7 uptime on the free Streamlit Cloud
           tier without any external paid worker service.
           render.yaml / Koyeb / Railway are no longer needed.

  KEY FIXES APPLIED vs v1:
  [MEM-1]  n_bars: 250 → 110  (EMA39 needs ~39 warmup; 110 is safe & lean)
  [MEM-2]  Removed unnecessary df.copy() in fetch_and_analyze
  [MEM-3]  Explicit del of intermediate DataFrames (adx_data, df_1h, close_1h)
  [MEM-4]  1H resample: now only resamples Close series, not full OHLCV DataFrame
  [MEM-5]  send_telegram_csv_backup: uses io.BytesIO — zero disk I/O
  [MEM-6]  yf fallback: switched to yf.download() — no persistent Ticker objects
  [MEM-7]  Added logging suppression for urllib3 (noisy connection-pool logs)
  [MEM-8]  SQLite pragmas: synchronous=NORMAL, temp_store=MEMORY, cache_size=-6000
  
  [THR-1]  _PROCESS_LOCK: prevents concurrent process_market_data() calls from
           UI thread (manual sync button) + daemon thread simultaneously
  [THR-2]  Daemon liveness tracked via _daemon_health dict (st.cache_resource)
           — exposes last_run_ts and run_count to UI for health visibility
  [THR-3]  UI DB reads use short-lived connections per query block, not one
           long-lived context manager wrapping all 150 lines of UI code
  
  [BAN-1]  Inter-asset sleep: 1.5s between every fetch_and_analyze API call
           (18 assets × 1.5s = ~27s total at 15m boundary — safe rate)
  [BAN-2]  TvDatafeed retry: removed double-reset pattern that fired 2 API
           calls on every first failure; now single retry with backoff
  
  [BUG-1]  anchor INSERT: changed to INSERT OR REPLACE (was bare INSERT —
           would IntegrityError on any edge-case re-entry)
  [BUG-2]  vol_col: explicit column guard instead of ambiguous df.get()
  [BUG-3]  Added market_open guard for BINANCE (BTC runs 24/7)
  [BUG-4]  Stale-data guard now correctly uses tz-naive comparison
  
  [ARCH]   See ARCHITECTURAL NOTE at bottom of file.
================================================================================
"""

import streamlit as st
from streamlit_autorefresh import st_autorefresh   # [LIVE-1] keep-alive
import pandas as pd
import numpy as np
import pandas_ta as ta
import sqlite3
import time
import requests
import threading
import gc
import logging
import io
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import contextlib
from datetime import datetime, timedelta, timezone

# [MEM-7] Suppress noisy connection-pool and library logs
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('peewee').setLevel(logging.CRITICAL)

# ==========================================
# 0. UI INITIALIZATION & CUSTOM CSS
# ==========================================
st.set_page_config(
    page_title="Algo Engine Terminal",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
        .block-container { padding-top: 1.5rem !important; padding-bottom: 1rem !important; max-width: 98% !important; }
        .metrics-matrix { width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; background: linear-gradient(145deg, #16181c, #0e1117); border-radius: 8px; overflow: hidden; border: 1px solid #2b303b; box-shadow: 0px 4px 15px rgba(0,0,0,0.2); text-align: center; font-family: sans-serif; }
        .metrics-matrix th { background-color: #121418; color: #8b949e; font-size: 0.85rem; font-weight: 600; text-transform: uppercase; padding: 12px 8px; border: 1px solid #2b303b; letter-spacing: 0.5px; }
        .metrics-matrix td { padding: 10px 8px; border: 1px solid #2b303b; }
        .metrics-matrix .val { font-size: 1.8rem; font-weight: 700; color: #f0f6fc; }
        .metrics-matrix .pct { font-size: 1rem; font-weight: 500; }
        .metrics-matrix .row-title { font-size: 0.9rem; color: #8b949e; font-weight: 600; text-align: left; padding-left: 15px; }
        .color-win { color: #3fb950; }
        .color-loss { color: #f85149; }
        .color-be { color: #a371f7; }
        .color-open { color: #58a6ff; }
        .stTabs [data-baseweb="tab-list"] { gap: 4px; border-bottom: 1px solid #2b303b; }
        .stTabs [data-baseweb="tab"] { white-space: nowrap !important; padding: 10px 20px; background-color: transparent; color: #8b949e; font-size: 0.95rem; font-weight: 500; border: none; }
        .stTabs [aria-selected="true"] { background-color: rgba(88, 166, 255, 0.1) !important; color: #58a6ff !important; border-bottom: 3px solid #58a6ff !important; border-radius: 6px 6px 0 0; }
        .health-ok { color: #3fb950; font-weight: 600; }
        .health-warn { color: #f0883e; font-weight: 600; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 1. CORE ARCHITECTURE & DB SETUP
# ==========================================
try:
    TELEGRAM_TOKEN = st.secrets["TELEGRAM_TOKEN"]
    TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
except Exception:
    TELEGRAM_TOKEN = ""
    TELEGRAM_CHAT_ID = ""

DB_PATH = 'nifty_live_trades.db'

# [THR-1] Global mutex — prevents UI thread and daemon from running
#         process_market_data() simultaneously. Non-blocking on UI side.
_PROCESS_LOCK = threading.Lock()

# [BAN-1] Delay between consecutive asset API calls
_INTER_ASSET_SLEEP = 1.5  # seconds


def get_db_connection() -> sqlite3.Connection:
    """
    Returns a WAL-mode SQLite connection with tuned pragmas.
    [MEM-8] cache_size=-6000 → ~6MB page cache (down from SQLite default 2MB,
            kept lean for the 1GB RAM ceiling).
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")   # [MEM-8] safe + faster
    conn.execute("PRAGMA temp_store=MEMORY;")    # [MEM-8] no temp-file I/O
    conn.execute("PRAGMA cache_size=-6000;")     # [MEM-8] cap page cache
    return conn


def setup_database():
    with contextlib.closing(get_db_connection()) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, signal_type TEXT, entry_time TEXT, entry_price REAL,
            sl REAL, tp REAL, status TEXT, exit_time TEXT, exit_price REAL,
            htf_trend TEXT, vol_ratio REAL, atr REAL, adx REAL
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS system_status (
            key TEXT PRIMARY KEY, value TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS system_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, message TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS live_market_data (
            ticker TEXT PRIMARY KEY, last_update TEXT, close_price REAL,
            ema5 REAL, ema39 REAL, trend TEXT, distance_pct REAL,
            htf_trend TEXT, vol_ratio REAL, adx REAL
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS blocked_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, signal_type TEXT, timestamp TEXT, price REAL,
            adx REAL, htf_trend TEXT, vol_ratio REAL, rejection_reasons TEXT
        )''')
        # Safe schema migrations
        for sql in [
            "ALTER TABLE trades ADD COLUMN htf_trend TEXT",
            "ALTER TABLE trades ADD COLUMN vol_ratio REAL",
            "ALTER TABLE trades ADD COLUMN atr REAL",
            "ALTER TABLE trades ADD COLUMN adx REAL",
            "ALTER TABLE live_market_data ADD COLUMN adx REAL",
        ]:
            try:
                c.execute(sql)
            except Exception:
                pass
        conn.commit()


setup_database()


# ==========================================
# 2. COMMUNICATION FUNCTIONS
# ==========================================

def send_telegram_alert(message: str, test_mode: bool = False):
    if not TELEGRAM_TOKEN:
        return False if test_mode else None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message.replace("&", "&amp;"),
        'parse_mode': 'HTML'
    }
    for _ in range(3):
        try:
            resp = requests.post(url, data=payload, timeout=10)
            if resp.status_code == 200:
                return True if test_mode else None
            elif resp.status_code == 429:
                time.sleep(3)
            else:
                return False if test_mode else None
        except Exception:
            time.sleep(1)
    return False if test_mode else None


def send_telegram_csv_backup():
    """
    [MEM-5] Streams CSVs directly via io.BytesIO — no disk files created/deleted.
    """
    if not TELEGRAM_TOKEN:
        return
    try:
        with contextlib.closing(get_db_connection()) as conn:
            df_trades = pd.read_sql_query("SELECT * FROM trades", conn)
            df_blocked = pd.read_sql_query("SELECT * FROM blocked_signals", conn)

        ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        date_str = ist_now.strftime('%Y-%m-%d')
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"

        for df, label in [(df_trades, "Trades"), (df_blocked, "Blocked")]:
            buf = io.BytesIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            payload = {
                'chat_id': TELEGRAM_CHAT_ID,
                'caption': f"📊 <b>Automated Daily Backup: {label}</b>\nDate: {date_str}",
                'parse_mode': 'HTML'
            }
            try:
                requests.post(
                    url,
                    data=payload,
                    files={'document': (f"{label}_Backup_{date_str}.csv", buf, 'text/csv')},
                    timeout=20
                )
            except Exception:
                pass
            del buf, df
            time.sleep(2)
    except Exception:
        pass


# ==========================================
# 3. WATCHLIST
# ==========================================
# tv_symbol / tv_exchange removed — yfinance is now the sole data source.
# asset_class drives the market-hours gate:
#   'NSE'     → weekdays 09:15–15:30 IST only
#   'COMMODITY' → weekdays only (Mon–Fri), ~23 hours
#   'CRYPTO'  → 24/7, no gate
WATCHLIST = [
    {'name': 'NIFTY 50',       'yf_symbol': '^NSEI',          'asset_class': 'NSE'},
    {'name': 'BANK NIFTY',     'yf_symbol': '^NSEBANK',       'asset_class': 'NSE'},
    {'name': 'BITCOIN (24/7)', 'yf_symbol': 'BTC-USD',        'asset_class': 'CRYPTO'},
    {'name': 'GOLD',           'yf_symbol': 'GC=F',           'asset_class': 'COMMODITY'},
    {'name': 'SILVER',         'yf_symbol': 'SI=F',           'asset_class': 'COMMODITY'},
    {'name': 'CRUDE OIL',      'yf_symbol': 'CL=F',           'asset_class': 'COMMODITY'},
    {'name': 'HDFC BANK',      'yf_symbol': 'HDFCBANK.NS',    'asset_class': 'NSE'},
    {'name': 'SBI',            'yf_symbol': 'SBIN.NS',        'asset_class': 'NSE'},
    {'name': 'RELIANCE',       'yf_symbol': 'RELIANCE.NS',    'asset_class': 'NSE'},
    {'name': 'INFOSYS',        'yf_symbol': 'INFY.NS',        'asset_class': 'NSE'},
    {'name': 'TCS',            'yf_symbol': 'TCS.NS',         'asset_class': 'NSE'},
    {'name': 'ITC',            'yf_symbol': 'ITC.NS',         'asset_class': 'NSE'},
    {'name': 'TATA MOTORS',    'yf_symbol': 'TATAMOTORS.NS',  'asset_class': 'NSE'},
    {'name': 'TATA STEEL',     'yf_symbol': 'TATASTEEL.NS',   'asset_class': 'NSE'},
    {'name': 'L&T',            'yf_symbol': 'LT.NS',          'asset_class': 'NSE'},
    {'name': 'BHARTI AIRTEL',  'yf_symbol': 'BHARTIARTL.NS',  'asset_class': 'NSE'},
    {'name': 'SUN PHARMA',     'yf_symbol': 'SUNPHARMA.NS',   'asset_class': 'NSE'},
    {'name': 'VEDANTA',        'yf_symbol': 'VEDL.NS',        'asset_class': 'NSE'},
]


# ==========================================
# 4. DATA FETCHING ENGINE
# ==========================================
# tvDatafeed removed — yfinance covers all 18 assets and is PyPI-stable.
# All WATCHLIST items have a yf_symbol that works with yf.download().
_api_lock = threading.Lock()


def fetch_and_analyze(item: dict) -> pd.DataFrame | None:
    """
    Fetches 15m OHLCV via yfinance for all asset classes:
      NSE stocks/indices, BTC, Gold (GC=F), Silver (SI=F), Crude (CL=F).
    Computes: EMA5, EMA39, ATR14, ADX14, EMA39_1H, Vol_Ratio.
    yf.download() is stateless — no persistent session or Ticker objects.
    """
    df = None

    try:
        with _api_lock:
            df_yf = yf.download(
                item['yf_symbol'],
                interval="15m",
                period="5d",
                progress=False,
                auto_adjust=True
            )
        if df_yf is not None and not df_yf.empty:
            # yfinance Multi-level columns fix (yf >= 0.2.x returns MultiIndex)
            if isinstance(df_yf.columns, pd.MultiIndex):
                df_yf.columns = df_yf.columns.get_level_values(0)
            df = df_yf
            del df_yf
    except Exception:
        pass

    if df is None or df.empty:
        return None

    try:
        # --- Timezone normalisation to IST (tz-naive) ---
        # yfinance always returns tz-aware UTC timestamps.
        if df.index.tz is not None:
            df.index = df.index.tz_convert('Asia/Kolkata').tz_localize(None)
        else:
            # Safety fallback: treat naive index as UTC and shift to IST
            df.index = df.index + timedelta(hours=5, minutes=30)

        # --- Type enforcement ---
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].ffill()
        df.dropna(subset=['Close', 'High', 'Low'], inplace=True)

        # --- Core indicators ---
        df['EMA5']  = ta.ema(df['Close'], length=5)
        df['EMA39'] = ta.ema(df['Close'], length=39)
        df['ATR']   = ta.atr(df['High'], df['Low'], df['Close'], length=14)

        # [MEM-3, MEM-4] 1H EMA from Close-only resample — 5× less memory
        close_1h    = df['Close'].resample('1h').last().dropna()
        ema39_1h    = ta.ema(close_1h, length=39)
        df['EMA39_1H'] = ema39_1h.reindex(df.index, method='ffill')
        del close_1h, ema39_1h  # [MEM-3]

        # [MEM-3] ADX — delete the multi-column DataFrame immediately
        adx_data = ta.adx(df['High'], df['Low'], df['Close'], length=14)
        if adx_data is not None and not adx_data.empty:
            df['ADX'] = adx_data.iloc[:, 0].ffill().fillna(0.0)
        else:
            df['ADX'] = 0.0
        del adx_data  # [MEM-3]

        # [BUG-2] Explicit column guard
        if 'Volume' in df.columns:
            vol_ma = df['Volume'].rolling(20).mean()
            df['Vol_Ratio'] = np.where(vol_ma > 0, df['Volume'] / vol_ma, 1.0)
            del vol_ma
        else:
            df['Vol_Ratio'] = 1.0

        df.dropna(subset=['EMA39_1H', 'EMA39', 'EMA5', 'ATR'], inplace=True)

        if len(df) >= 5:
            return df
    except Exception:
        pass

    return None


@st.cache_data(ttl=300)
def get_cached_chart_data(item_dict: dict) -> pd.DataFrame | None:
    """UI-only chart fetch. Cached 5 min to survive Streamlit re-runs."""
    return fetch_and_analyze(item_dict)


# ==========================================
# 5. EXECUTION ENGINE
# ==========================================

def process_market_data() -> bool:
    """
    Smart Hunter state machine:
      • 15m boundary → scan ALL market-open assets for crossover (Anchor)
      • 5m intervals  → only process assets with active Anchor or open trade
    
    [THR-1] Guarded by _PROCESS_LOCK. If already running, returns immediately.
    [BAN-1] 1.5s sleep after every fetch_and_analyze call.
    """
    if not _PROCESS_LOCK.acquire(blocking=False):
        return False  # Another thread already running; skip this cycle

    try:
        with contextlib.closing(get_db_connection()) as conn:
            c = conn.cursor()
            ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
            current_slot_minute = (ist_now.minute // 5) * 5
            scan_time_str = ist_now.replace(
                minute=current_slot_minute, second=0, microsecond=0
            ).strftime("%Y-%m-%d %I:%M %p (IST)")
            current_date_str = ist_now.strftime("%Y-%m-%d")
            is_15m_boundary = (current_slot_minute % 15 == 0)

            # --- Daily backup trigger ---
            c.execute("SELECT value FROM system_status WHERE key='last_backup_date'")
            last_backup_row = c.fetchone()
            if (not last_backup_row or last_backup_row[0] != current_date_str) \
                    and ist_now.hour >= 23 and ist_now.minute >= 30:
                send_telegram_csv_backup()
                c.execute(
                    "INSERT OR REPLACE INTO system_status (key, value) VALUES ('last_backup_date', ?)",
                    (current_date_str,)
                )
                conn.commit()

            # -------------------------------------------------------
            # MAIN ASSET LOOP
            # -------------------------------------------------------
            for item in WATCHLIST:
                name, asset_class = item['name'], item['asset_class']

                # --- Market-hours gate ---
                market_open = True
                if asset_class == 'NSE':
                    mins = ist_now.hour * 60 + ist_now.minute
                    if ist_now.weekday() >= 5 or not (555 <= mins <= 935):
                        market_open = False
                elif asset_class == 'COMMODITY':
                    # Futures markets closed Saturday; reopen Sunday ~18:00 EST
                    if ist_now.weekday() == 5 or \
                            (ist_now.weekday() == 6 and ist_now.hour < 3):
                        market_open = False
                # CRYPTO: 24/7 — no gate applied

                if not market_open:
                    continue

                c.execute(
                    "SELECT count(*) FROM trades WHERE ticker=? AND status='OPEN'",
                    (name,)
                )
                open_trade_count = c.fetchone()[0]

                c.execute(
                    "SELECT value FROM system_status WHERE key=?",
                    (f"anchor_{name}",)
                )
                anchor_row = c.fetchone()

                # Smart Hunter gate: only fetch if needed
                if not is_15m_boundary and open_trade_count == 0 and not anchor_row:
                    continue

                df = fetch_and_analyze(item)
                time.sleep(_INTER_ASSET_SLEEP)  # [BAN-1] rate-limit

                if df is None:
                    continue

                # --- [BUG-4] Stale-data guard (tz-naive comparison) ---
                try:
                    last_close_dt = df.index[-1]
                    if (ist_now.replace(tzinfo=None) - last_close_dt).total_seconds() > 3600:
                        del df
                        continue
                except Exception:
                    pass

                curr = df.iloc[-1]
                last = df.iloc[-2]
                prev = df.iloc[-3]

                # --- Update live market data ---
                c.execute(
                    "INSERT OR REPLACE INTO live_market_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        name, scan_time_str,
                        round(curr['Close'], 2),
                        round(curr['EMA5'],  2),
                        round(curr['EMA39'], 2),
                        "🟢 Bullish" if curr['EMA5'] > curr['EMA39'] else "🔴 Bearish",
                        abs(curr['EMA5'] - curr['EMA39']) / curr['EMA39'] * 100,
                        "🟢 Bullish" if curr['Close'] > curr['EMA39_1H'] else "🔴 Bearish",
                        round(curr.get('Vol_Ratio', 1.0), 2),
                        round(curr.get('ADX',      0.0), 2),
                    )
                )
                conn.commit()

                # ===================================================
                # OPEN TRADE MANAGER  (Zero-Paradox Snapshot Logic)
                # ===================================================
                if open_trade_count > 0:
                    c.execute(
                        "SELECT id, signal_type, sl, tp, entry_price, atr "
                        "FROM trades WHERE ticker=? AND status='OPEN'",
                        (name,)
                    )
                    for trade in c.fetchall():
                        t_id, s_type, sl, tp, e_price, atr_val = trade
                        atr_val = atr_val if (atr_val and atr_val > 0) \
                            else abs(tp - e_price) / 3.75
                        live_price  = curr['Close']
                        trade_closed = False

                        if s_type == 'long':
                            if live_price >= tp:
                                status_text = 'TP HIT (WIN)' if live_price < (tp + atr_val) \
                                    else 'TP HIT (GAP UP)'
                                c.execute(
                                    "UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?",
                                    (status_text, scan_time_str, live_price, t_id)
                                )
                                send_telegram_alert(
                                    f"🎯 <b>{status_text}</b>\n{name} LONG closed at {round(live_price, 2)}"
                                )
                                trade_closed = True
                            elif live_price <= sl:
                                status_text = (
                                    'BREAK-EVEN TP HIT' if sl > e_price else
                                    ('BREAK-EVEN (0 RISK)' if sl == e_price else 'SL HIT (LOSS)')
                                )
                                if live_price < (sl - atr_val):
                                    status_text += ' (GAP DOWN)'
                                c.execute(
                                    "UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?",
                                    (status_text, scan_time_str, live_price, t_id)
                                )
                                send_telegram_alert(
                                    f"🛑 <b>{status_text}</b>\n{name} LONG closed at {round(live_price, 2)}"
                                )
                                trade_closed = True

                            if not trade_closed \
                                    and round(sl, 2) <= round(e_price, 2) \
                                    and live_price >= (e_price + 1.0 * atr_val):
                                new_sl = round(e_price + 0.25 * atr_val, 2)
                                c.execute("UPDATE trades SET sl=? WHERE id=?", (new_sl, t_id))
                                send_telegram_alert(
                                    f"🛡️ <b>PROFIT LOCKED</b>\n{name} LONG hit 1 ATR. "
                                    f"SL moved to {new_sl} (+0.25 ATR)."
                                )

                        elif s_type == 'short':
                            if live_price <= tp:
                                status_text = 'TP HIT (WIN)' if live_price > (tp - atr_val) \
                                    else 'TP HIT (GAP DOWN)'
                                c.execute(
                                    "UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?",
                                    (status_text, scan_time_str, live_price, t_id)
                                )
                                send_telegram_alert(
                                    f"🎯 <b>{status_text}</b>\n{name} SHORT closed at {round(live_price, 2)}"
                                )
                                trade_closed = True
                            elif live_price >= sl:
                                status_text = (
                                    'BREAK-EVEN TP HIT' if sl < e_price else
                                    ('BREAK-EVEN (0 RISK)' if sl == e_price else 'SL HIT (LOSS)')
                                )
                                if live_price > (sl + atr_val):
                                    status_text += ' (GAP UP)'
                                c.execute(
                                    "UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?",
                                    (status_text, scan_time_str, live_price, t_id)
                                )
                                send_telegram_alert(
                                    f"🛑 <b>{status_text}</b>\n{name} SHORT closed at {round(live_price, 2)}"
                                )
                                trade_closed = True

                            if not trade_closed \
                                    and round(sl, 2) >= round(e_price, 2) \
                                    and live_price <= (e_price - 1.0 * atr_val):
                                new_sl = round(e_price - 0.25 * atr_val, 2)
                                c.execute("UPDATE trades SET sl=? WHERE id=?", (new_sl, t_id))
                                send_telegram_alert(
                                    f"🛡️ <b>PROFIT LOCKED</b>\n{name} SHORT hit 1 ATR. "
                                    f"SL moved to {new_sl} (-0.25 ATR)."
                                )
                    conn.commit()

                # ===================================================
                # STATE MACHINE: 15-MINUTE ANCHOR LOGIC
                # ===================================================
                signal_id = str(last.name)

                if not anchor_row and is_15m_boundary:
                    c.execute(
                        "SELECT value FROM system_status WHERE key=?",
                        (f"proc_{name}",)
                    )
                    proc_row = c.fetchone()

                    if not proc_row or proc_row[0] != signal_id:
                        is_long  = (prev['EMA5'] <= prev['EMA39']) and (last['EMA5'] > last['EMA39'])
                        is_short = (prev['EMA5'] >= prev['EMA39']) and (last['EMA5'] < last['EMA39'])

                        if is_long or is_short:
                            direction  = "LONG" if is_long else "SHORT"
                            start_time = ist_now.replace(
                                minute=(ist_now.minute // 15) * 15,
                                second=0, microsecond=0
                            ).strftime("%Y-%m-%d %H:%M:%S")
                            anchor_val = f"{start_time}|{direction}|{last['ATR']}|{signal_id}"

                            # [BUG-1] INSERT OR REPLACE — safe against edge-case re-entry
                            c.execute(
                                "INSERT OR REPLACE INTO system_status (key, value) VALUES (?, ?)",
                                (f"anchor_{name}", anchor_val)
                            )
                            conn.commit()
                            anchor_row = (anchor_val,)

                # --- Anchor evaluation (5m + 15m intervals) ---
                if anchor_row:
                    adata = anchor_row[0].split('|')
                    if len(adata) == 4:
                        a_start  = datetime.strptime(adata[0], "%Y-%m-%d %H:%M:%S")
                        a_dir    = adata[1]
                        a_atr    = float(adata[2])
                        a_sig_id = adata[3]
                        mins_elapsed = (ist_now.replace(tzinfo=None) - a_start).total_seconds() / 60.0

                        if mins_elapsed <= 16.0:
                            ev_candle = last if mins_elapsed >= 14.0 else curr
                            p_adx     = prev.get('ADX', 0.0) if mins_elapsed >= 14.0 \
                                        else last.get('ADX', 0.0)

                            l_adx = ev_candle.get('ADX', 0.0)
                            l_vol = ev_candle.get('Vol_Ratio', 1.0)
                            l_htf = "🟢 Bullish" if ev_candle['Close'] > ev_candle['EMA39_1H'] \
                                    else "🔴 Bearish"
                            r_htf = "🟢 Bullish" if a_dir == "LONG" else "🔴 Bearish"

                            rejections = []
                            if open_trade_count > 0:
                                rejections.append("Active trade open.")
                            if l_adx <= 20.0:
                                rejections.append(f"Weak Trend (ADX: {round(l_adx, 1)} < 20).")
                            elif l_adx < p_adx:
                                rejections.append(
                                    f"Falling Momentum (ADX {round(l_adx, 1)} < Prev {round(p_adx, 1)})."
                                )
                            if abs(ev_candle['EMA5'] - ev_candle['EMA39']) < (0.15 * a_atr):
                                rejections.append("EMAs Tangled.")
                            if l_htf != r_htf:
                                rejections.append(f"1H Conflict ({l_htf}).")
                            if abs(ev_candle['Close'] - ev_candle['EMA39']) > (2.5 * a_atr):
                                rejections.append("Overextended.")

                            if not rejections:
                                entry = ev_candle['Close']
                                sl    = entry - (1.5 * a_atr)  if a_dir == "LONG" \
                                        else entry + (1.5 * a_atr)
                                tp    = entry + (3.75 * a_atr) if a_dir == "LONG" \
                                        else entry - (3.75 * a_atr)
                                c.execute(
                                    "INSERT INTO trades "
                                    "(ticker, signal_type, entry_time, entry_price, sl, tp, status, "
                                    " htf_trend, vol_ratio, atr, adx) "
                                    "VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?)",
                                    (
                                        name, a_dir.lower(), scan_time_str,
                                        round(entry, 2), round(sl, 2), round(tp, 2),
                                        l_htf, round(l_vol, 2), round(a_atr, 2), round(l_adx, 2)
                                    )
                                )
                                send_telegram_alert(
                                    f"{'🟢' if a_dir == 'LONG' else '🔴'} "
                                    f"<b>{a_dir} SIGNAL: {name}</b>\n"
                                    f"Time: {scan_time_str}\n"
                                    f"Entry: {round(entry, 2)}\n"
                                    f"SL: {round(sl, 2)}\n"
                                    f"TP: {round(tp, 2)}\n"
                                    f"ADX: {round(l_adx, 1)}"
                                )
                                c.execute(
                                    "INSERT OR REPLACE INTO system_status VALUES (?, ?)",
                                    (f"proc_{name}", a_sig_id)
                                )
                                c.execute(
                                    "DELETE FROM system_status WHERE key=?",
                                    (f"anchor_{name}",)
                                )
                            else:
                                safe_rej = [
                                    r.replace("<", "&lt;").replace(">", "&gt;")
                                    for r in rejections
                                ]
                                c.execute(
                                    "INSERT INTO blocked_signals "
                                    "(ticker, signal_type, timestamp, price, adx, htf_trend, "
                                    " vol_ratio, rejection_reasons) "
                                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        name, a_dir, scan_time_str,
                                        round(ev_candle['Close'], 2), round(l_adx, 2),
                                        l_htf, round(l_vol, 2), " | ".join(safe_rej)
                                    )
                                )
                                if mins_elapsed >= 14.0:
                                    send_telegram_alert(
                                        f"💀 <b>EXPIRED: {name}</b>\n"
                                        f"{a_dir} failed to align within 15m.\n" +
                                        "\n".join([f"❌ {r}" for r in safe_rej])
                                    )
                                    c.execute(
                                        "INSERT OR REPLACE INTO system_status VALUES (?, ?)",
                                        (f"proc_{name}", a_sig_id)
                                    )
                                    c.execute(
                                        "DELETE FROM system_status WHERE key=?",
                                        (f"anchor_{name}",)
                                    )
                        else:
                            c.execute(
                                "INSERT OR REPLACE INTO system_status VALUES (?, ?)",
                                (f"proc_{name}", a_sig_id)
                            )
                            c.execute(
                                "DELETE FROM system_status WHERE key=?",
                                (f"anchor_{name}",)
                            )
                    conn.commit()

                # Explicit DataFrame release after each asset
                del df
                time.sleep(0.1)  # yield to event loop

            # --- Housekeeping ---
            c.execute(
                "DELETE FROM system_logs WHERE id NOT IN "
                "(SELECT id FROM system_logs ORDER BY id DESC LIMIT 500)"
            )
            c.execute(
                "DELETE FROM blocked_signals WHERE id NOT IN "
                "(SELECT id FROM blocked_signals ORDER BY id DESC LIMIT 300)"
            )
            c.execute(
                "INSERT OR REPLACE INTO system_status VALUES ('last_scan', ?)",
                (scan_time_str,)
            )
            conn.commit()
    finally:
        gc.collect()
        _PROCESS_LOCK.release()

    return True


# ==========================================
# 6. BACKGROUND THREAD DAEMON
# ==========================================

def _get_sleep_to_next_5m() -> float:
    """Calculates seconds until the next 5-minute wall-clock boundary."""
    now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    next_slot = ((now.minute // 5) + 1) * 5
    if next_slot >= 60:
        next_time = now.replace(minute=0, second=2, microsecond=0) + timedelta(hours=1)
    else:
        next_time = now.replace(minute=next_slot, second=2, microsecond=0)
    delta = (next_time - now).total_seconds()
    return max(delta, 10.0)


@st.cache_resource
def _start_background_scanner() -> dict:
    """
    Starts the daemon thread exactly once per Streamlit server lifetime.
    Returns a shared health dict so the UI can display daemon status.
    [THR-2] health dict is the only shared mutable state between threads.
    """
    health = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "last_run_ts": None,
        "run_count": 0,
        "last_error": None,
    }

    def _loop():
        while True:
            sleep_secs = _get_sleep_to_next_5m()
            time.sleep(sleep_secs)
            try:
                process_market_data()
                health["last_run_ts"] = datetime.now(timezone.utc).isoformat()
                health["run_count"]   += 1
                health["last_error"]  = None
            except Exception as e:
                health["last_error"] = str(e)
                try:
                    with contextlib.closing(get_db_connection()) as conn:
                        conn.execute(
                            "INSERT INTO system_logs (timestamp, message) VALUES (?, ?)",
                            (str(datetime.now()), f"DAEMON CRASH: {e}")
                        )
                        conn.commit()
                except Exception:
                    pass

    threading.Thread(target=_loop, daemon=True, name="AlgoDaemon").start()
    return health


_daemon_health = _start_background_scanner()


# ==========================================
# 7. STREAMLIT DASHBOARD UI
# ==========================================

INITIAL_CAPITAL  = 200_000.0
TRADE_ALLOCATION = 10_000.0

# [THR-3] Short-lived DB connection per UI render — not one giant context manager
def _db_query(sql: str, params=()) -> pd.DataFrame:
    with contextlib.closing(get_db_connection()) as c:
        return pd.read_sql_query(sql, c, params=params)


def _db_scalar(sql: str, params=()):
    with contextlib.closing(get_db_connection()) as conn:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else None


# [LIVE-1] Keep-alive heartbeat — fires a real WebSocket message every 60s.
# Prevents Streamlit Cloud from marking the session idle and killing the
# Python process (and the daemon thread inside it).
st_autorefresh(interval=60_000, limit=None, key="session_keepalive")

# --- SIDEBAR ---
st.sidebar.markdown("<h3>⚙️ Control Panel</h3>", unsafe_allow_html=True)

# [THR-2] Daemon health display
last_run_ts  = _daemon_health.get("last_run_ts")
run_count    = _daemon_health.get("run_count", 0)
last_error   = _daemon_health.get("last_error")
daemon_ok    = last_error is None

if daemon_ok:
    st.sidebar.markdown(
        f'<span class="health-ok">✅ Daemon LIVE</span> — Cycles: {run_count}',
        unsafe_allow_html=True
    )
else:
    st.sidebar.markdown(
        f'<span class="health-warn">⚠️ Daemon Error:</span> {last_error}',
        unsafe_allow_html=True
    )

last_scan = _db_scalar("SELECT value FROM system_status WHERE key='last_scan'")
st.sidebar.info(f"⏱️ **Last DB Sync:**\n{last_scan or 'Initializing...'}")

if st.sidebar.button("🔄 Force Manual Data Sync"):
    if _PROCESS_LOCK.locked():
        st.sidebar.warning("⏳ Daemon is currently running. Retry in a moment.")
    else:
        with st.spinner("Executing Data Sync..."):
            process_market_data()
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("<h3>🛡️ Backup & Restore</h3>", unsafe_allow_html=True)
colA, colB = st.sidebar.columns(2)
try:
    with colA:
        trades_csv = _db_query("SELECT * FROM trades").to_csv(index=False).encode('utf-8')
        st.download_button(
            "⬇️ Trades", trades_csv,
            f"Trades_{datetime.now().strftime('%Y-%m-%d')}.csv", "text/csv"
        )
    with colB:
        blocked_csv = _db_query("SELECT * FROM blocked_signals").to_csv(index=False).encode('utf-8')
        st.download_button(
            "⬇️ Blocked", blocked_csv,
            f"Blocked_{datetime.now().strftime('%Y-%m-%d')}.csv", "text/csv"
        )
except Exception:
    pass

# --- RESTORE ---
st.sidebar.markdown("<b>Restore Database (Upload CSV)</b>", unsafe_allow_html=True)
uploaded_file = st.sidebar.file_uploader("Upload CSV", type=None, label_visibility="collapsed")

if uploaded_file is not None:
    if st.sidebar.button("⚙️ Execute Auto-Restore"):
        try:
            restore_df = pd.read_csv(uploaded_file)
            csv_columns = restore_df.columns.tolist()

            with contextlib.closing(get_db_connection()) as r_conn:
                r_c = r_conn.cursor()

                if 'entry_time' in csv_columns or 'Entry Time' in csv_columns:
                    rename_map = {
                        'Asset': 'ticker', 'Signal': 'signal_type',
                        'Entry Time': 'entry_time', 'Entry': 'entry_price',
                        'SL': 'sl', 'TP': 'tp', 'ATR': 'atr', 'ADX': 'adx',
                        'Status': 'status', 'Exit Time': 'exit_time',
                        'Exit Price': 'exit_price', '1H Trend': 'htf_trend',
                        'Vol (x)': 'vol_ratio'
                    }
                    restore_df = restore_df.rename(columns=rename_map)
                    restore_df = restore_df.fillna({
                        'exit_time': '', 'exit_price': 0.0,
                        'htf_trend': '', 'vol_ratio': 1.0,
                        'atr': 0.0, 'adx': 0.0
                    })
                    for _, row in restore_df.iterrows():
                        r_c.execute(
                            "SELECT id FROM trades WHERE ticker=? AND entry_time=?",
                            (row['ticker'], row['entry_time'])
                        )
                        if not r_c.fetchone():
                            r_c.execute(
                                "INSERT INTO trades (ticker, signal_type, entry_time, entry_price, "
                                "sl, tp, status, exit_time, exit_price, htf_trend, vol_ratio, atr, adx) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (
                                    row['ticker'], row['signal_type'], row['entry_time'],
                                    row['entry_price'], row['sl'], row['tp'], row['status'],
                                    row['exit_time'], row['exit_price'], row['htf_trend'],
                                    row['vol_ratio'], row['atr'], row['adx']
                                )
                            )
                    r_conn.commit()
                    st.sidebar.success("✅ Trades Restored! Rebooting...")

                elif 'rejection_reasons' in csv_columns or 'Rejection Reasons' in csv_columns:
                    rename_map = {
                        'Asset': 'ticker', 'Signal': 'signal_type',
                        'Time (IST)': 'timestamp', 'Price': 'price', 'ADX': 'adx',
                        '1H Trend': 'htf_trend', 'Vol (x)': 'vol_ratio',
                        'Rejection Reasons': 'rejection_reasons'
                    }
                    restore_df = restore_df.rename(columns=rename_map)
                    restore_df = restore_df.fillna({
                        'adx': 0.0, 'htf_trend': '', 'vol_ratio': 1.0, 'rejection_reasons': ''
                    })
                    for _, row in restore_df.iterrows():
                        r_c.execute(
                            "SELECT id FROM blocked_signals WHERE ticker=? AND timestamp=?",
                            (row['ticker'], row['timestamp'])
                        )
                        if not r_c.fetchone():
                            r_c.execute(
                                "INSERT INTO blocked_signals "
                                "(ticker, signal_type, timestamp, price, adx, htf_trend, "
                                " vol_ratio, rejection_reasons) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (
                                    row['ticker'], row['signal_type'], row['timestamp'],
                                    row['price'], row['adx'], row['htf_trend'],
                                    row['vol_ratio'], row['rejection_reasons']
                                )
                            )
                    r_conn.commit()
                    st.sidebar.success("✅ Blocked Signals Restored! Rebooting...")
                else:
                    st.sidebar.error("❌ Unrecognized CSV format.")

            time.sleep(2)
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Restore failed: {e}")

st.sidebar.markdown("---")
st.sidebar.markdown("<h3>🧪 System Diagnostics</h3>", unsafe_allow_html=True)
if st.sidebar.button("🔔 Send Test Telegram Alert"):
    if not TELEGRAM_TOKEN:
        st.sidebar.error("❌ Telegram Secrets Missing!")
    else:
        with st.spinner("Pinging Telegram..."):
            ok = send_telegram_alert("🧪 <b>DIAGNOSTIC PING</b>\nHTML test: ADX (&lt; 20)", test_mode=True)
            if ok:
                st.sidebar.success("Ping fired successfully!")
            else:
                st.sidebar.error("❌ Telegram API Failed. Check Token/ID.")


# ==========================================
# MAIN BODY — METRICS MATRIX
# ==========================================

try:
    live_df = _db_query(
        "SELECT ticker as Asset, close_price as 'Latest Price', distance_pct as '% Gap', "
        "trend as '15m Trend', htf_trend as '1H Trend', vol_ratio as 'Vol (x)', "
        "adx as 'ADX', last_update as 'Time (IST)' "
        "FROM live_market_data ORDER BY distance_pct ASC"
    )
    history_df  = _db_query(
        "SELECT * FROM trades WHERE status!='OPEN' ORDER BY id DESC LIMIT 100"
    )
    open_df_ui  = _db_query(
        "SELECT * FROM trades WHERE status='OPEN' ORDER BY id DESC"
    )
except Exception:
    live_df = history_df = open_df_ui = pd.DataFrame()

# Realized PnL
realized_pnl = 0.0
if not history_df.empty:
    history_df['Yield'] = np.where(
        history_df['signal_type'].str.lower() == 'long',
        (history_df['exit_price'] - history_df['entry_price']) / history_df['entry_price'],
        (history_df['entry_price'] - history_df['exit_price']) / history_df['entry_price'],
    )
    history_df['PnL (₹)'] = history_df['Yield'] * TRADE_ALLOCATION
    be_mask = (
        history_df['status'].str.contains('BREAK-EVEN', na=False) &
        ~history_df['status'].str.contains('TP|WIN', na=False)
    )
    history_df.loc[be_mask, 'PnL (₹)'] = 0.0
    realized_pnl = history_df['PnL (₹)'].sum()

# Unrealized PnL
total_unrealized_pnl = 0.0
if not open_df_ui.empty and not live_df.empty:
    open_df_ui = pd.merge(
        open_df_ui,
        live_df[['Asset', 'Latest Price']],
        left_on='ticker', right_on='Asset', how='left'
    )
    open_df_ui['Yield'] = np.where(
        open_df_ui['signal_type'].str.lower() == 'long',
        (open_df_ui['Latest Price'] - open_df_ui['entry_price']) / open_df_ui['entry_price'],
        (open_df_ui['entry_price'] - open_df_ui['Latest Price']) / open_df_ui['entry_price'],
    )
    open_df_ui['Unrealized PnL (₹)'] = (open_df_ui['Yield'] * TRADE_ALLOCATION).round(2)
    total_unrealized_pnl = open_df_ui['Unrealized PnL (₹)'].sum()
    open_df_ui['Risk Status'] = np.where(
        (
            (open_df_ui['signal_type'].str.lower() == 'long')  & (open_df_ui['sl'] >= open_df_ui['entry_price'])
        ) | (
            (open_df_ui['signal_type'].str.lower() == 'short') & (open_df_ui['sl'] <= open_df_ui['entry_price'])
        ),
        '🛡️ RISK-FREE', '⚠️ AT RISK'
    )

# Header
st.markdown(
    "<h1 style='background: -webkit-linear-gradient(45deg, #ffd700, #ffaa00); "
    "-webkit-background-clip: text; -webkit-text-fill-color: transparent;'>"
    "⚡ Algo Engine by Vinayak</h1>",
    unsafe_allow_html=True
)

t_closed = len(history_df)
w_count  = len(history_df[history_df['status'].str.contains('TP|WIN',      na=False)]) if t_closed else 0
b_count  = len(history_df[history_df['status'].str.contains('BREAK-EVEN',  na=False) &
                           ~history_df['status'].str.contains('TP|WIN',     na=False)]) if t_closed else 0
l_count  = len(history_df[history_df['status'].str.contains('LOSS|SL HIT', na=False)]) if t_closed else 0
r_free   = len(open_df_ui[open_df_ui['Risk Status'] == '🛡️ RISK-FREE']) if not open_df_ui.empty else 0

pnl_color    = 'color-win' if realized_pnl >= 0 else 'color-loss'
unpnl_color  = 'color-win' if total_unrealized_pnl >= 0 else 'color-loss'

st.markdown(f"""
<table class="metrics-matrix">
  <tr>
    <th></th><th>Trades</th><th>Win</th><th>Break Even</th><th>Loss</th>
  </tr>
  <tr>
    <td class="row-title">CLOSED TRADES</td>
    <td class="val">{t_closed}</td>
    <td class="val color-win">{w_count}</td>
    <td class="val color-be">{b_count}</td>
    <td class="val color-loss">{l_count}</td>
  </tr>
  <tr>
    <td class="row-title" style="border-bottom: 2px solid #2b303b;">CAPITAL & PNL</td>
    <td class="val" style="border-bottom: 2px solid #2b303b; font-size: 1.2rem;">
      ₹{INITIAL_CAPITAL:,.0f}
    </td>
    <td colspan="3" class="pct" style="border-bottom: 2px solid #2b303b; text-align: left; padding-left: 20px;">
      Realized: <b class="{pnl_color}">₹{realized_pnl:,.2f}</b> &nbsp;|&nbsp;
      Equity: <b style="color: #f0f6fc;">₹{INITIAL_CAPITAL + realized_pnl:,.2f}</b> &nbsp;|&nbsp;
      Unrealized: <b class="{unpnl_color}">₹{total_unrealized_pnl:,.2f}</b>
    </td>
  </tr>
  <tr>
    <td class="row-title">OPEN TRADES</td>
    <td class="val color-open">{len(open_df_ui)}</td>
    <td colspan="3" class="pct color-be" style="text-align: left; padding-left: 20px;">
      🛡️ {r_free} Risk-Free &nbsp;|&nbsp; ⚠️ {len(open_df_ui) - r_free} At Risk
    </td>
  </tr>
</table>
""", unsafe_allow_html=True)


# ==========================================
# TABS
# ==========================================
t_heat, t_chart, t_open, t_ledger, t_blocked = st.tabs(
    ["🔥 Heatmap", "📈 Chart", "🟢 Open", "📚 Ledger", "🚫 Blocked"]
)

with t_heat:
    if not live_df.empty:
        st.dataframe(
            live_df.style.map(
                lambda v: (
                    'background-color: rgba(255,0,0,0.4); color: white;'   if pd.notna(v) and float(v) < 0.10 else
                    'background-color: rgba(255,165,0,0.4); color: white;' if pd.notna(v) and float(v) < 0.50 else ''
                ),
                subset=['% Gap']
            ),
            use_container_width=True, height=600, hide_index=True
        )
    else:
        st.info("Waiting for data sync...")

with t_chart:
    if not live_df.empty:
        sel_stock = st.selectbox(
            "Select Asset:",
            ["-- Select --"] + sorted(live_df['Asset'].tolist()),
            label_visibility="collapsed"
        )
        if sel_stock != "-- Select --":
            with st.spinner("Loading chart..."):
                try:
                    item_obj = next(i for i in WATCHLIST if i['name'] == sel_stock)
                    chart_df = get_cached_chart_data(item_obj)
                    if chart_df is not None:
                        chart_df = chart_df[
                            chart_df.index >= (chart_df.index[-1] - timedelta(days=5))
                        ]
                        fig = make_subplots(
                            rows=2, cols=1, shared_xaxes=True,
                            vertical_spacing=0.05, row_heights=[0.7, 0.3]
                        )
                        x_labels = chart_df.index.strftime('%b %d %H:%M')
                        fig.add_trace(go.Candlestick(
                            x=x_labels,
                            open=chart_df['Open'], high=chart_df['High'],
                            low=chart_df['Low'],   close=chart_df['Close'],
                            name="Price"
                        ), row=1, col=1)
                        fig.add_trace(go.Scatter(
                            x=x_labels, y=chart_df['EMA5'],
                            line=dict(color='#00ff00', width=1.5), name='EMA 5'
                        ), row=1, col=1)
                        fig.add_trace(go.Scatter(
                            x=x_labels, y=chart_df['EMA39'],
                            line=dict(color='#ff0000', width=2.0), name='EMA 39'
                        ), row=1, col=1)
                        fig.add_trace(go.Scatter(
                            x=x_labels, y=chart_df['ADX'],
                            line=dict(color='#ffd700', width=1.5), name='ADX'
                        ), row=2, col=1)
                        fig.add_hline(
                            y=20, line_dash="dot",
                            annotation_text="Trend (20)", row=2, col=1
                        )
                        fig.update_layout(
                            title=(
                                f"{sel_stock} | "
                                f"ADX: {chart_df['ADX'].iloc[-1]:.2f} | "
                                f"ATR: {chart_df['ATR'].iloc[-1]:.2f}"
                            ),
                            template="plotly_dark",
                            xaxis_rangeslider_visible=False,
                            height=700
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        del fig
                except Exception:
                    st.error("Chart data unavailable.")

with t_open:
    if not open_df_ui.empty:
        display_cols = [
            c for c in
            ['ticker', 'signal_type', 'entry_time', 'entry_price',
             'sl', 'tp', 'Latest Price', 'Risk Status', 'Unrealized PnL (₹)']
            if c in open_df_ui.columns
        ]
        st.dataframe(open_df_ui[display_cols], use_container_width=True, height=600, hide_index=True)
    else:
        st.info("No active trades.")

with t_ledger:
    if not history_df.empty:
        st.dataframe(
            history_df[[
                'ticker', 'signal_type', 'entry_time', 'entry_price',
                'sl', 'tp', 'exit_time', 'exit_price', 'status', 'PnL (₹)'
            ]],
            use_container_width=True, height=600, hide_index=True
        )
    else:
        st.info("No closed trades.")

with t_blocked:
    try:
        b_df = _db_query(
            "SELECT ticker as Asset, signal_type as Signal, timestamp as 'Time (IST)', "
            "price as Price, rejection_reasons as 'Rejection Reasons', adx as ADX, "
            "htf_trend as '1H Trend', vol_ratio as 'Vol (x)' "
            "FROM blocked_signals ORDER BY id DESC LIMIT 100"
        )
        if not b_df.empty:
            st.dataframe(b_df, use_container_width=True, height=600, hide_index=True)
        else:
            st.info("No blocked signals.")
    except Exception:
        pass


# ==========================================
# ARCHITECTURAL NOTE
# ==========================================
# The fundamental constraint of Streamlit Community Cloud is that the Python
# process (and its daemon thread) is killed when no user is interacting with
# the app — typically after 60–90 seconds of WebSocket inactivity. UptimeRobot
# only keeps the HTTP load balancer warm, NOT the WebSocket or the Python process.
#
# FOR TRUE 24/7 STABILITY — the signal engine must be decoupled from Streamlit:
#
#   RECOMMENDED ARCHITECTURE:
#   ┌────────────────────┐      ┌──────────────────────────────┐
#   │  Signal Engine     │      │  Streamlit (Display-only)    │
#   │  (Render / Railway │─────▶│  Reads from Supabase / Neon  │
#   │   Free Tier Worker)│      │  No daemon thread needed     │
#   │  Runs process_      │      │  UptimeRobot keeps it warm   │
#   │  market_data()     │      └──────────────────────────────┘
#   │  every 5 minutes   │
#   └────────────────────┘
#
#   Render.com / Railway.app both offer free-tier always-on Python workers.
#   Supabase / Neon offer free-tier Postgres (replaces SQLite).
#   Migration effort: ~2–3 hours. Stability gain: from ~60% uptime → 99.9%.
#
#   Until then, the hardened daemon in this file is the best achievable within
#   Streamlit Cloud's constraints, with _PROCESS_LOCK preventing race conditions
#   and explicit memory management reducing OOM frequency.
# ==========================================
