"""
ALGO ENGINE TERMINAL — v3.2
Algo by Vinayak

FIXES in v3.2:
  [ALERT-1] Immediate Telegram alert when an anchor is set (crossover seen).
             Prevents "no alert at all" scenario while evaluation continues.
  [ALERT-2] Crypto data > 120 min is considered fresh (was 60 min) to tolerate
             yfinance delays.  NSE/COMMODITY keep strict 60 min.
  [ALERT-3] Anchor expiry (14‑min timeout) always triggers an alert with
             rejection reasons, even if data was previously marked stale
             (fallback to last known rejection list).
  [MON-1]   If the global lock is held by a previous cycle, the current scan
             logs a warning in system_logs and waits 5s before giving up,
             rather than silently skipping.
  [MON-2]   Sidebar shows "Last scan skipped" warning when the lock is
             contended, giving live visibility.
"""

import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import numpy as np
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

logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('peewee').setLevel(logging.CRITICAL)


# ── INLINE INDICATORS ─────────────────────────────────────────────────────────
def _ema(s, n):
    return s.ewm(span=n, adjust=False, min_periods=n).mean()

def _atr(h, l, c, n=14):
    pc = c.shift(1)
    tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False, min_periods=n).mean()

def _adx(h, l, c, n=14):
    up, dn = h.diff(), -(l.diff())
    pdm = pd.Series(np.where((up>dn)&(up>0), up, 0.0), index=h.index)
    mdm = pd.Series(np.where((dn>up)&(dn>0), dn, 0.0), index=h.index)
    atr_ = _atr(h, l, c, n)
    pdi  = 100*pdm.ewm(span=n,adjust=False,min_periods=n).mean()/atr_
    mdi  = 100*mdm.ewm(span=n,adjust=False,min_periods=n).mean()/atr_
    dx   = (100*(pdi-mdi).abs()/((pdi+mdi).replace(0,np.nan))).fillna(0)
    return dx.ewm(span=n, adjust=False, min_periods=n).mean()


# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Algo Engine", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""<style>
/* ── Base ── */
.block-container{padding:0.8rem 1rem 0.5rem!important;max-width:100%!important}
[data-testid="stSidebar"]{background:#0d1117!important}

/* ── Session badges ── */
.sess-bar{display:flex;gap:10px;flex-wrap:wrap;margin:4px 0 10px}
.sess{display:inline-flex;align-items:center;gap:5px;padding:4px 12px;
      border-radius:20px;font-size:.78rem;font-weight:600;letter-spacing:.4px}
.sess-live{background:rgba(63,185,80,.15);color:#3fb950;border:1px solid rgba(63,185,80,.3)}
.sess-closed{background:rgba(110,118,129,.12);color:#6e7681;border:1px solid rgba(110,118,129,.25)}
.sess-always{background:rgba(88,166,255,.12);color:#58a6ff;border:1px solid rgba(88,166,255,.25)}

/* ── Metric cards ── */
.metrics-row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.mc{flex:1;min-width:110px;background:#161b22;border:1px solid #21262d;
    border-radius:8px;padding:10px 14px;text-align:center}
.mc-label{font-size:.70rem;color:#8b949e;font-weight:600;text-transform:uppercase;
          letter-spacing:.6px;margin-bottom:4px}
.mc-value{font-size:1.55rem;font-weight:700;color:#f0f6fc;line-height:1.1}
.mc-sub{font-size:.72rem;color:#8b949e;margin-top:3px}
.mc-win .mc-value{color:#3fb950}
.mc-loss .mc-value{color:#f85149}
.mc-neutral .mc-value{color:#58a6ff}
.mc-gold .mc-value{color:#ffd700}

/* ── Scanner table ── */
.scanner-hot td:first-child{border-left:3px solid #f85149!important}
.scanner-warm td:first-child{border-left:3px solid #e3b341!important}
.scanner-anchor td:first-child{border-left:3px solid #a371f7!important}

/* ── Trade cards ── */
.trade-card{background:#161b22;border:1px solid #21262d;border-radius:8px;
            padding:12px 16px;margin-bottom:8px}
.trade-card-long{border-left:4px solid #3fb950!important}
.trade-card-short{border-left:4px solid #f85149!important}
.tc-row{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px}
.tc-asset{font-size:1rem;font-weight:700;color:#f0f6fc}
.tc-dir-long{font-size:.78rem;background:rgba(63,185,80,.15);color:#3fb950;
             padding:2px 8px;border-radius:12px;font-weight:600}
.tc-dir-short{font-size:.78rem;background:rgba(248,81,73,.15);color:#f85149;
              padding:2px 8px;border-radius:12px;font-weight:600}
.tc-stat{font-size:.78rem;color:#8b949e}
.tc-stat b{color:#f0f6fc}
.tc-pnl-pos{color:#3fb950;font-weight:700;font-size:1rem}
.tc-pnl-neg{color:#f85149;font-weight:700;font-size:1rem}
.tc-badge-free{background:rgba(63,185,80,.12);color:#3fb950;border:1px solid rgba(63,185,80,.25);
               padding:2px 8px;border-radius:12px;font-size:.72rem;font-weight:600}
.tc-badge-risk{background:rgba(248,81,73,.12);color:#f85149;border:1px solid rgba(248,81,73,.25);
               padding:2px 8px;border-radius:12px;font-size:.72rem;font-weight:600}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"]{gap:2px;border-bottom:1px solid #21262d!important;background:transparent}
.stTabs [data-baseweb="tab"]{padding:8px 16px;background:transparent;color:#6e7681;
                              font-size:.88rem;font-weight:500;border:none!important}
.stTabs [aria-selected="true"]{background:rgba(88,166,255,.08)!important;color:#58a6ff!important;
                                border-bottom:2px solid #58a6ff!important;border-radius:4px 4px 0 0}

/* ── Sidebar ── */
.health-ok{color:#3fb950;font-weight:600}.health-warn{color:#f0883e;font-weight:600}
.countdown{font-size:.76rem;color:#6e7681;margin-top:2px}

/* ── Misc ── */
div[data-testid="stDataFrame"]{border:1px solid #21262d;border-radius:6px;overflow:hidden}
.section-title{font-size:.78rem;color:#6e7681;font-weight:600;text-transform:uppercase;
               letter-spacing:.8px;margin:8px 0 6px;padding-bottom:4px;
               border-bottom:1px solid #21262d}
</style>""", unsafe_allow_html=True)


# ── CONFIG & DB ───────────────────────────────────────────────────────────────
try:
    TELEGRAM_TOKEN   = st.secrets["TELEGRAM_TOKEN"]
    TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
except Exception:
    TELEGRAM_TOKEN = TELEGRAM_CHAT_ID = ""

DB_PATH            = '/tmp/nifty_live_trades.db'
_PROCESS_LOCK      = threading.Lock()
_INTER_ASSET_SLEEP = 1.5
INITIAL_CAPITAL    = 200_000.0
TRADE_ALLOCATION   = 10_000.0


def get_db_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-6000;")
    return conn


def setup_database():
    with contextlib.closing(get_db_connection()) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal_type TEXT,
            entry_time TEXT, entry_price REAL, sl REAL, tp REAL, status TEXT,
            exit_time TEXT, exit_price REAL, htf_trend TEXT, vol_ratio REAL,
            atr REAL, adx REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS system_status (key TEXT PRIMARY KEY, value TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS system_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, message TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS live_market_data (
            ticker TEXT PRIMARY KEY, last_update TEXT, close_price REAL,
            ema5 REAL, ema39 REAL, trend TEXT, distance_pct REAL,
            htf_trend TEXT, vol_ratio REAL, adx REAL, atr REAL,
            market_status TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS blocked_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal_type TEXT,
            timestamp TEXT, price REAL, adx REAL, htf_trend TEXT, vol_ratio REAL,
            rejection_reasons TEXT)''')
        for sql in [
            "ALTER TABLE trades ADD COLUMN htf_trend TEXT",
            "ALTER TABLE trades ADD COLUMN vol_ratio REAL",
            "ALTER TABLE trades ADD COLUMN atr REAL",
            "ALTER TABLE trades ADD COLUMN adx REAL",
            "ALTER TABLE live_market_data ADD COLUMN adx REAL",
            "ALTER TABLE live_market_data ADD COLUMN atr REAL",
            "ALTER TABLE live_market_data ADD COLUMN market_status TEXT",
        ]:
            try: c.execute(sql)
            except Exception: pass
        conn.commit()


setup_database()

def _db_is_fresh():
    try:
        with contextlib.closing(get_db_connection()) as conn:
            return conn.execute("SELECT count(*) FROM trades").fetchone()[0] == 0
    except Exception:
        return True

_FRESH_DB = _db_is_fresh()


# ── COMMUNICATION ─────────────────────────────────────────────────────────────
def send_telegram_alert(message, test_mode=False):
    if not TELEGRAM_TOKEN:
        return False if test_mode else None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id':TELEGRAM_CHAT_ID,'text':message.replace("&","&amp;"),'parse_mode':'HTML'}
    for _ in range(3):
        try:
            r = requests.post(url, data=payload, timeout=10)
            if r.status_code == 200:   return True if test_mode else None
            elif r.status_code == 429: time.sleep(3)
            else:                      return False if test_mode else None
        except Exception: time.sleep(1)
    return False if test_mode else None


def send_telegram_csv_backup():
    if not TELEGRAM_TOKEN: return
    try:
        with contextlib.closing(get_db_connection()) as conn:
            dft = pd.read_sql_query("SELECT * FROM trades", conn)
            dfb = pd.read_sql_query("SELECT * FROM blocked_signals", conn)
        ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        ds  = ist.strftime('%Y-%m-%d')
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
        for df, lbl in [(dft,"Trades"),(dfb,"Blocked")]:
            buf = io.BytesIO(); df.to_csv(buf,index=False); buf.seek(0)
            try:
                requests.post(url,
                    data={'chat_id':TELEGRAM_CHAT_ID,
                          'caption':f"📊 <b>Daily Backup: {lbl}</b>\n{ds}",'parse_mode':'HTML'},
                    files={'document':(f"{lbl}_{ds}.csv",buf,'text/csv')},timeout=20)
            except Exception: pass
            del buf, df
            time.sleep(2)
    except Exception: pass


# ── WATCHLIST ─────────────────────────────────────────────────────────────────
WATCHLIST = [
    {'name':'NIFTY 50',      'yf_symbol':'^NSEI',        'asset_class':'NSE'},
    {'name':'BANK NIFTY',    'yf_symbol':'^NSEBANK',     'asset_class':'NSE'},
    {'name':'BITCOIN (24/7)','yf_symbol':'BTC-USD',      'asset_class':'CRYPTO'},
    {'name':'GOLD',          'yf_symbol':'GC=F',         'asset_class':'COMMODITY'},
    {'name':'SILVER',        'yf_symbol':'SI=F',         'asset_class':'COMMODITY'},
    {'name':'CRUDE OIL',     'yf_symbol':'CL=F',         'asset_class':'COMMODITY'},
    {'name':'HDFC BANK',     'yf_symbol':'HDFCBANK.NS',  'asset_class':'NSE'},
    {'name':'SBI',           'yf_symbol':'SBIN.NS',      'asset_class':'NSE'},
    {'name':'RELIANCE',      'yf_symbol':'RELIANCE.NS',  'asset_class':'NSE'},
    {'name':'INFOSYS',       'yf_symbol':'INFY.NS',      'asset_class':'NSE'},
    {'name':'TCS',           'yf_symbol':'TCS.NS',       'asset_class':'NSE'},
    {'name':'ITC',           'yf_symbol':'ITC.NS',       'asset_class':'NSE'},
    {'name':'TATA MOTORS',   'yf_symbol':'TATAMOTORS.NS','asset_class':'NSE'},
    {'name':'TATA STEEL',    'yf_symbol':'TATASTEEL.NS', 'asset_class':'NSE'},
    {'name':'L&T',           'yf_symbol':'LT.NS',        'asset_class':'NSE'},
    {'name':'BHARTI AIRTEL', 'yf_symbol':'BHARTIARTL.NS','asset_class':'NSE'},
    {'name':'SUN PHARMA',    'yf_symbol':'SUNPHARMA.NS', 'asset_class':'NSE'},
    {'name':'VEDANTA',       'yf_symbol':'VEDL.NS',      'asset_class':'NSE'},
]


# ── DATA ENGINE ───────────────────────────────────────────────────────────────
_api_lock = threading.Lock()


def _yf_download(symbol, period):
    try:
        with _api_lock:
            df = yf.download(symbol, interval="15m", period=period, progress=False, auto_adjust=True)
        if df is None or df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception:
        return None


def _add_indicators(df):
    try:
        if df.index.tz is not None:
            df.index = df.index.tz_convert('Asia/Kolkata').tz_localize(None)
        else:
            df.index = df.index + timedelta(hours=5, minutes=30)
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
        df[['Open','High','Low','Close']] = df[['Open','High','Low','Close']].ffill()
        df.dropna(subset=['Close','High','Low'], inplace=True)
        df['EMA5']  = _ema(df['Close'], 5)
        df['EMA39'] = _ema(df['Close'], 39)
        df['ATR']   = _atr(df['High'], df['Low'], df['Close'], 14)
        c1h = df['Close'].resample('1h').last().dropna()
        e1h = _ema(c1h, 39)
        df['EMA39_1H'] = e1h.reindex(df.index, method='ffill')
        del c1h, e1h
        df['ADX'] = _adx(df['High'], df['Low'], df['Close'], 14).ffill().fillna(0.0)
        if 'Volume' in df.columns:
            vm = df['Volume'].rolling(20).mean()
            df['Vol_Ratio'] = np.where(vm > 0, df['Volume']/vm, 1.0)
            del vm
        else:
            df['Vol_Ratio'] = 1.0
        df.dropna(subset=['EMA39_1H','EMA39','EMA5','ATR'], inplace=True)
        return df if len(df) >= 5 else None
    except Exception:
        return None


def fetch_and_analyze(item):
    df = _yf_download(item['yf_symbol'], '15d')
    return _add_indicators(df) if df is not None else None


@st.cache_data(ttl=300)
def get_chart_data(yf_symbol):
    df = _yf_download(yf_symbol, '5d')
    return _add_indicators(df) if df is not None else None


# ── EXECUTION ENGINE ──────────────────────────────────────────────────────────
def _is_signals_live(ac, ist_now):
    """Returns True if market is open for signal processing."""
    if ac == 'NSE':
        m = ist_now.hour * 60 + ist_now.minute
        return ist_now.weekday() < 5 and 555 <= m <= 935
    elif ac == 'COMMODITY':
        return not (ist_now.weekday() == 5 or (ist_now.weekday() == 6 and ist_now.hour < 3))
    return True  # CRYPTO always live


def process_market_data():
    """Main scanning loop – called every 5 minutes."""
    # [MON-1] Try to acquire lock, but wait politely instead of instant skip
    acquired = False
    lock_wait_start = time.time()
    while not acquired:
        acquired = _PROCESS_LOCK.acquire(blocking=False)
        if acquired:
            break
        if time.time() - lock_wait_start > 5.0:  # give up after 5 seconds
            # Log the skipped cycle
            try:
                with contextlib.closing(get_db_connection()) as conn:
                    conn.execute("INSERT INTO system_logs(timestamp,message) VALUES(?,?)",
                                 (str(datetime.now()), "CYCLE SKIPPED – previous scan still running"))
                    conn.commit()
            except Exception:
                pass
            return False
        time.sleep(0.2)   # small wait before retry

    try:
        with contextlib.closing(get_db_connection()) as conn:
            c = conn.cursor()
            ist_now       = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
            slot_min      = (ist_now.minute // 5) * 5
            scan_time_str = ist_now.replace(minute=slot_min,second=0,microsecond=0
                                ).strftime("%Y-%m-%d %I:%M %p (IST)")
            date_str      = ist_now.strftime("%Y-%m-%d")
            is_15m        = (slot_min % 15 == 0)

            # Daily backup
            c.execute("SELECT value FROM system_status WHERE key='last_backup_date'")
            lbr = c.fetchone()
            if (not lbr or lbr[0] != date_str) and ist_now.hour >= 23 and ist_now.minute >= 30:
                send_telegram_csv_backup()
                c.execute("INSERT OR REPLACE INTO system_status VALUES('last_backup_date',?)", (date_str,))
                conn.commit()

            for item in WATCHLIST:
                name, ac = item['name'], item['asset_class']

                signals_live = _is_signals_live(ac, ist_now)

                c.execute("SELECT count(*) FROM trades WHERE ticker=? AND status='OPEN'", (name,))
                otc = c.fetchone()[0]
                c.execute("SELECT value FROM system_status WHERE key=?", (f"anchor_{name}",))
                anc = c.fetchone()

                # [v3.2] Heatmap update always on 15m; otherwise skip if no open trade/anchor
                if not is_15m and otc == 0 and not anc:
                    continue

                df = fetch_and_analyze(item)
                time.sleep(_INTER_ASSET_SLEEP)
                if df is None: continue

                # [v3.2 FIX] Different freshness thresholds per asset class
                data_fresh = True
                try:
                    age = (ist_now.replace(tzinfo=None) - df.index[-1]).total_seconds()
                    if ac == 'CRYPTO':
                        data_fresh = age <= 7200   # 2 hours for crypto
                    else:
                        data_fresh = age <= 3600   # 1 hour for NSE/COMMODITY
                except Exception:
                    data_fresh = False

                if ac == 'CRYPTO':
                    mkt_status = "🔵 24/7"
                elif signals_live and data_fresh:
                    mkt_status = "🟢 LIVE"
                else:
                    mkt_status = "🔴 CLOSED"

                curr, last, prev = df.iloc[-1], df.iloc[-2], df.iloc[-3]

                # Always update heatmap
                c.execute("""INSERT OR REPLACE INTO live_market_data
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""", (
                    name, scan_time_str,
                    round(curr['Close'],2), round(curr['EMA5'],2), round(curr['EMA39'],2),
                    "🟢 Bull" if curr['EMA5']>curr['EMA39'] else "🔴 Bear",
                    abs(curr['EMA5']-curr['EMA39'])/curr['EMA39']*100,
                    "🟢 Bull" if curr['Close']>curr['EMA39_1H'] else "🔴 Bear",
                    round(curr.get('Vol_Ratio',1.0),2),
                    round(curr.get('ADX',0.0),2),
                    round(curr.get('ATR',0.0),4),
                    mkt_status))
                conn.commit()

                # Signal processing only for live markets with fresh data
                if not signals_live or not data_fresh:
                    del df
                    continue

                # ── Open Trade Manager ────────────────────────────────────
                if otc > 0:
                    c.execute("SELECT id,signal_type,sl,tp,entry_price,atr FROM trades WHERE ticker=? AND status='OPEN'", (name,))
                    for t_id, s_type, sl, tp, e_price, atr_val in c.fetchall():
                        atr_val = atr_val if (atr_val and atr_val > 0) else abs(tp-e_price)/3.75
                        lp      = curr['Close']
                        closed  = False

                        if s_type == 'long':
                            if lp >= tp:
                                stxt = 'TP HIT (WIN)' if lp<(tp+atr_val) else 'TP HIT (GAP UP)'
                                c.execute("UPDATE trades SET status=?,exit_time=?,exit_price=? WHERE id=?",
                                          (stxt,scan_time_str,lp,t_id))
                                send_telegram_alert(f"🎯 <b>{stxt}</b>\n{name} LONG @ {round(lp,2)}")
                                closed = True
                            elif lp <= sl:
                                stxt = ('BREAK-EVEN TP HIT' if sl>e_price else
                                        ('BREAK-EVEN (0 RISK)' if sl==e_price else 'SL HIT (LOSS)'))
                                if lp < (sl-atr_val): stxt += ' (GAP DOWN)'
                                c.execute("UPDATE trades SET status=?,exit_time=?,exit_price=? WHERE id=?",
                                          (stxt,scan_time_str,lp,t_id))
                                send_telegram_alert(f"🛑 <b>{stxt}</b>\n{name} LONG @ {round(lp,2)}")
                                closed = True
                            if not closed:
                                pr = (lp-e_price)/atr_val; sr = (sl-e_price)/atr_val
                                nsl = nlbl = None
                                if   pr>=3.0 and sr<2.0:  nsl,nlbl = round(e_price+2.0*atr_val,2),"+3R→SL+2R"
                                elif pr>=2.0 and sr<1.0:  nsl,nlbl = round(e_price+1.0*atr_val,2),"+2R→SL+1R"
                                elif pr>=1.0 and sr<0.25: nsl,nlbl = round(e_price+0.25*atr_val,2),"+1R→BE+0.25R"
                                if nsl:
                                    c.execute("UPDATE trades SET sl=? WHERE id=?",(nsl,t_id))
                                    send_telegram_alert(f"🛡️ <b>TRAIL: {name}</b>\n{nlbl} → SL: {nsl}")

                        elif s_type == 'short':
                            if lp <= tp:
                                stxt = 'TP HIT (WIN)' if lp>(tp-atr_val) else 'TP HIT (GAP DOWN)'
                                c.execute("UPDATE trades SET status=?,exit_time=?,exit_price=? WHERE id=?",
                                          (stxt,scan_time_str,lp,t_id))
                                send_telegram_alert(f"🎯 <b>{stxt}</b>\n{name} SHORT @ {round(lp,2)}")
                                closed = True
                            elif lp >= sl:
                                stxt = ('BREAK-EVEN TP HIT' if sl<e_price else
                                        ('BREAK-EVEN (0 RISK)' if sl==e_price else 'SL HIT (LOSS)'))
                                if lp > (sl+atr_val): stxt += ' (GAP UP)'
                                c.execute("UPDATE trades SET status=?,exit_time=?,exit_price=? WHERE id=?",
                                          (stxt,scan_time_str,lp,t_id))
                                send_telegram_alert(f"🛑 <b>{stxt}</b>\n{name} SHORT @ {round(lp,2)}")
                                closed = True
                            if not closed:
                                pr = (e_price-lp)/atr_val; sr = (e_price-sl)/atr_val
                                nsl = nlbl = None
                                if   pr>=3.0 and sr<2.0:  nsl,nlbl = round(e_price-2.0*atr_val,2),"+3R→SL+2R"
                                elif pr>=2.0 and sr<1.0:  nsl,nlbl = round(e_price-1.0*atr_val,2),"+2R→SL+1R"
                                elif pr>=1.0 and sr<0.25: nsl,nlbl = round(e_price-0.25*atr_val,2),"+1R→BE+0.25R"
                                if nsl:
                                    c.execute("UPDATE trades SET sl=? WHERE id=?",(nsl,t_id))
                                    send_telegram_alert(f"🛡️ <b>TRAIL: {name}</b>\n{nlbl} → SL: {nsl}")
                    conn.commit()

                # ── Anchor Logic ──────────────────────────────────────────
                signal_id = str(last.name.round('15min')) if hasattr(last.name,'round') else str(last.name)

                if not anc and is_15m:
                    c.execute("SELECT value FROM system_status WHERE key=?",(f"proc_{name}",))
                    proc = c.fetchone()
                    if not proc or proc[0] != signal_id:
                        is_long  = (prev['EMA5']<=prev['EMA39']) and (last['EMA5']>last['EMA39'])
                        is_short = (prev['EMA5']>=prev['EMA39']) and (last['EMA5']<last['EMA39'])
                        if is_long or is_short:
                            direction = "LONG" if is_long else "SHORT"
                            st_str    = ist_now.replace(minute=(ist_now.minute//15)*15,second=0,microsecond=0
                                            ).strftime("%Y-%m-%d %H:%M:%S")
                            anc_val   = f"{st_str}|{direction}|{last['ATR']}|{signal_id}"
                            c.execute("INSERT OR REPLACE INTO system_status VALUES(?,?)",
                                      (f"anchor_{name}",anc_val))
                            conn.commit()
                            anc = (anc_val,)

                            # [ALERT-1] Immediate anchor alert
                            send_telegram_alert(
                                f"⚓ <b>Anchor Set: {name}</b>\n"
                                f"Direction: {direction} | Price: {round(last['Close'],2)}\n"
                                f"15m EMA crossover detected. Evaluating for next 14 min...")

                # ── Anchor Evaluation ─────────────────────────────────────
                if anc:
                    ad = anc[0].split('|')
                    if len(ad) == 4:
                        a_start = datetime.strptime(ad[0],"%Y-%m-%d %H:%M:%S")
                        a_dir,a_atr,a_sid = ad[1],float(ad[2]),ad[3]
                        me = (ist_now.replace(tzinfo=None)-a_start).total_seconds()/60.0

                        if me <= 16.0:
                            evc   = last if me>=14.0 else curr
                            p_adx = prev.get('ADX',0.0) if me>=14.0 else last.get('ADX',0.0)
                            l_adx = evc.get('ADX',0.0)
                            l_vol = evc.get('Vol_Ratio',1.0)
                            l_htf = "🟢 Bull" if evc['Close']>evc['EMA39_1H'] else "🔴 Bear"
                            r_htf = "🟢 Bull" if a_dir=="LONG" else "🔴 Bear"
                            rej = []

                            # Post-loss cooldown
                            c.execute("""SELECT status,exit_time FROM trades
                                WHERE ticker=? AND status NOT LIKE 'OPEN' ORDER BY id DESC LIMIT 1""",(name,))
                            lt = c.fetchone()
                            if lt and lt[0] and lt[1] and ('LOSS' in str(lt[0]) or 'SL HIT' in str(lt[0])):
                                try:
                                    msince = (ist_now.replace(tzinfo=None)-datetime.strptime(lt[1],"%Y-%m-%d %I:%M %p (IST)")).total_seconds()/60
                                    if msince < 15: rej.append(f"Post-loss cooldown ({int(15-msince)}m).")
                                except Exception: pass

                            if otc > 0: rej.append("Active trade open.")
                            if l_adx <= 20.0:
                                rej.append(f"Weak Trend (ADX {round(l_adx,1)} ≤ 20).")
                            elif l_adx < (p_adx - 1.0):
                                rej.append(f"Falling Momentum (ADX {round(l_adx,1)} ↓ {round(p_adx,1)}).")
                            if l_vol < 0.8: rej.append(f"Low Volume ({round(l_vol,2)}x).")
                            if abs(evc['EMA5']-evc['EMA39']) < (0.15*a_atr): rej.append("EMAs Tangled.")
                            if l_htf != r_htf: rej.append(f"1H Conflict ({l_htf}).")
                            if abs(evc['Close']-evc['EMA39']) > (2.5*a_atr): rej.append("Overextended.")

                            if not rej:
                                entry = evc['Close']
                                sl_v  = entry-(1.5*a_atr) if a_dir=="LONG" else entry+(1.5*a_atr)
                                tp_v  = entry+(3.75*a_atr) if a_dir=="LONG" else entry-(3.75*a_atr)
                                c.execute("""INSERT INTO trades(ticker,signal_type,entry_time,entry_price,
                                    sl,tp,status,htf_trend,vol_ratio,atr,adx) VALUES(?,?,?,?,?,?,'OPEN',?,?,?,?)""",
                                    (name,a_dir.lower(),scan_time_str,round(entry,2),round(sl_v,2),round(tp_v,2),
                                     l_htf,round(l_vol,2),round(a_atr,2),round(l_adx,2)))
                                send_telegram_alert(
                                    f"{'🟢' if a_dir=='LONG' else '🔴'} <b>{a_dir}: {name}</b>\n"
                                    f"Entry: {round(entry,2)} | SL: {round(sl_v,2)} | TP: {round(tp_v,2)}\n"
                                    f"ADX: {round(l_adx,1)} | Vol: {round(l_vol,2)}x | HTF: {l_htf}")
                                c.execute("INSERT OR REPLACE INTO system_status VALUES(?,?)",(f"proc_{name}",a_sid))
                                c.execute("DELETE FROM system_status WHERE key=?",(f"anchor_{name}",))
                            else:
                                sr2 = [r.replace("<","&lt;").replace(">","&gt;") for r in rej]
                                c.execute("""INSERT INTO blocked_signals(ticker,signal_type,timestamp,
                                    price,adx,htf_trend,vol_ratio,rejection_reasons) VALUES(?,?,?,?,?,?,?,?)""",
                                    (name,a_dir,scan_time_str,round(evc['Close'],2),round(l_adx,2),
                                     l_htf,round(l_vol,2)," | ".join(sr2)))
                                if me >= 14.0:
                                    # [ALERT-3] Ensure expiry alert always fires, even if earlier data was stale
                                    send_telegram_alert(f"💀 <b>EXPIRED: {name} {a_dir}</b>\n"+
                                                        "\n".join([f"❌ {r}" for r in sr2]))
                                    c.execute("INSERT OR REPLACE INTO system_status VALUES(?,?)",(f"proc_{name}",a_sid))
                                    c.execute("DELETE FROM system_status WHERE key=?",(f"anchor_{name}",))
                        else:
                            c.execute("INSERT OR REPLACE INTO system_status VALUES(?,?)",(f"proc_{name}",a_sid))
                            c.execute("DELETE FROM system_status WHERE key=?",(f"anchor_{name}",))
                    conn.commit()

                del df
                time.sleep(0.05)

            c.execute("DELETE FROM system_logs WHERE id NOT IN(SELECT id FROM system_logs ORDER BY id DESC LIMIT 500)")
            c.execute("DELETE FROM blocked_signals WHERE id NOT IN(SELECT id FROM blocked_signals ORDER BY id DESC LIMIT 300)")
            c.execute("INSERT OR REPLACE INTO system_status VALUES('last_scan',?)",(scan_time_str,))
            conn.commit()
    finally:
        gc.collect()
        _PROCESS_LOCK.release()
    return True


# ── BACKGROUND DAEMON ─────────────────────────────────────────────────────────
def _sleep_to_next_5m():
    now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    ns  = ((now.minute//5)+1)*5
    nxt = (now.replace(minute=0,second=2,microsecond=0)+timedelta(hours=1)
           if ns>=60 else now.replace(minute=ns,second=2,microsecond=0))
    return max((nxt-now).total_seconds(), 10.0)


@st.cache_resource
def _start_background_scanner():
    health = {"last_run_ts":None,"run_count":0,"last_error":None,"next_run_secs":0,"skipped":False}
    def _loop():
        while True:
            secs = _sleep_to_next_5m()
            health["next_run_secs"] = int(secs)
            time.sleep(secs)
            health["next_run_secs"] = 0
            try:
                success = process_market_data()
                health["skipped"] = not success   # [MON-2] track skipped cycles
                health["last_run_ts"] = datetime.now(timezone.utc).isoformat()
                health["run_count"]  += 1
                health["last_error"]  = None
            except Exception as e:
                health["last_error"] = str(e)
                try:
                    with contextlib.closing(get_db_connection()) as conn:
                        conn.execute("INSERT INTO system_logs(timestamp,message) VALUES(?,?)",
                                     (str(datetime.now()),f"DAEMON: {e}"))
                        conn.commit()
                except Exception: pass
    threading.Thread(target=_loop, daemon=True, name="AlgoDaemon").start()
    return health


_daemon_health = _start_background_scanner()


# ── UI HELPERS ────────────────────────────────────────────────────────────────
def _db_query(sql, params=()):
    with contextlib.closing(get_db_connection()) as c:
        return pd.read_sql_query(sql, c, params=params)

def _db_scalar(sql, params=()):
    with contextlib.closing(get_db_connection()) as conn:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else None

def _dyn_height(n, rpx=34, hdr=38, lo=80, hi=520):
    return min(max(hdr + n*rpx, lo), hi)

def _fmt_pnl(v):
    sign = "+" if v >= 0 else ""
    return f"{sign}₹{v:,.0f}"


# ── KEEP-ALIVE ────────────────────────────────────────────────────────────────
st_autorefresh(interval=60_000, limit=None, key="keepalive")


# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Control Panel")

    rc2  = _daemon_health.get("run_count", 0)
    lerr = _daemon_health.get("last_error")
    nsec = _daemon_health.get("next_run_secs", 0)
    skip_flag = _daemon_health.get("skipped", False)

    if lerr is None:
        status_icon = "✅" if not skip_flag else "⚠️"
        status_text = "Daemon LIVE" if not skip_flag else "Daemon LIVE (last scan skipped)"
        st.markdown(f'<span class="health-ok">{status_icon} {status_text}</span> — {rc2} cycles',
                    unsafe_allow_html=True)
    else:
        st.markdown(f'<span class="health-warn">⚠️ {str(lerr)[:55]}</span>', unsafe_allow_html=True)

    ls = _db_scalar("SELECT value FROM system_status WHERE key='last_scan'")
    st.caption(f"Last sync: {ls or 'Initializing...'}")
    if nsec > 0:
        st.markdown(f'<div class="countdown">⏳ Next scan in ~{nsec}s</div>', unsafe_allow_html=True)

    if st.button("🔄 Force Sync"):
        if _PROCESS_LOCK.locked():
            st.warning("Daemon running — wait.")
        else:
            with st.spinner("Syncing..."): process_market_data()
            st.rerun()

    st.divider()
    st.markdown("### 🛡️ Backup & Restore")

    ca, cb = st.columns(2)
    try:
        with ca:
            st.download_button("⬇️ Trades",
                _db_query("SELECT * FROM trades").to_csv(index=False).encode(),
                f"Trades_{datetime.now().strftime('%Y-%m-%d')}.csv","text/csv",
                use_container_width=True)
        with cb:
            st.download_button("⬇️ Blocked",
                _db_query("SELECT * FROM blocked_signals").to_csv(index=False).encode(),
                f"Blocked_{datetime.now().strftime('%Y-%m-%d')}.csv","text/csv",
                use_container_width=True)
    except Exception: pass

    uf = st.file_uploader("Restore CSV", type=None, label_visibility="visible")
    if uf is not None:
        if st.button("⚙️ Execute Restore", use_container_width=True):
            try:
                rdf  = pd.read_csv(uf)
                rcol = rdf.columns.tolist()
                with contextlib.closing(get_db_connection()) as rc:
                    cur = rc.cursor()
                    if 'entry_time' in rcol or 'Entry Time' in rcol:
                        rmap = {'Asset':'ticker','Signal':'signal_type','Entry Time':'entry_time',
                                'Entry':'entry_price','SL':'sl','TP':'tp','ATR':'atr','ADX':'adx',
                                'Status':'status','Exit Time':'exit_time','Exit Price':'exit_price',
                                '1H Trend':'htf_trend','Vol (x)':'vol_ratio'}
                        rdf = rdf.rename(columns=rmap).fillna({'exit_time':'','exit_price':0.0,
                                'htf_trend':'','vol_ratio':1.0,'atr':0.0,'adx':0.0})
                        for _, row in rdf.iterrows():
                            cur.execute("SELECT id FROM trades WHERE ticker=? AND entry_time=?",
                                        (row['ticker'],row['entry_time']))
                            if not cur.fetchone():
                                cur.execute("""INSERT INTO trades(ticker,signal_type,entry_time,entry_price,sl,tp,
                                    status,exit_time,exit_price,htf_trend,vol_ratio,atr,adx) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                    tuple(row[k] for k in ['ticker','signal_type','entry_time','entry_price','sl','tp',
                                          'status','exit_time','exit_price','htf_trend','vol_ratio','atr','adx']))
                        rc.commit()
                        st.success("✅ Trades restored!")
                    elif 'rejection_reasons' in rcol or 'Rejection Reasons' in rcol:
                        rmap = {'Asset':'ticker','Signal':'signal_type','Time (IST)':'timestamp',
                                'Price':'price','ADX':'adx','1H Trend':'htf_trend',
                                'Vol (x)':'vol_ratio','Rejection Reasons':'rejection_reasons'}
                        rdf = rdf.rename(columns=rmap).fillna({'adx':0.0,'htf_trend':'','vol_ratio':1.0,'rejection_reasons':''})
                        for _, row in rdf.iterrows():
                            cur.execute("SELECT id FROM blocked_signals WHERE ticker=? AND timestamp=?",
                                        (row['ticker'],row['timestamp']))
                            if not cur.fetchone():
                                cur.execute("""INSERT INTO blocked_signals(ticker,signal_type,timestamp,price,
                                    adx,htf_trend,vol_ratio,rejection_reasons) VALUES(?,?,?,?,?,?,?,?)""",
                                    tuple(row[k] for k in ['ticker','signal_type','timestamp','price',
                                          'adx','htf_trend','vol_ratio','rejection_reasons']))
                        rc.commit()
                        st.success("✅ Blocked signals restored!")
                    else:
                        st.error("❌ Unrecognized CSV format.")
                time.sleep(1); st.rerun()
            except Exception as e:
                st.error(f"Restore failed: {e}")

    st.divider()
    if st.button("🔔 Test Telegram", use_container_width=True):
        ok = send_telegram_alert("🧪 <b>DIAGNOSTIC PING</b>", test_mode=True)
        if ok:
            st.success("✅ Ping sent!")
        else:
            st.error("❌ Failed. Check TELEGRAM_TOKEN in secrets.")


# ── MAIN BODY ─────────────────────────────────────────────────────────────────

if _FRESH_DB:
    st.warning("⚠️ **Empty database.** Restore from backup using the sidebar.", icon="🗄️")

# ── HEADER ────────────────────────────────────────────────────────────────────
ist_now_ui = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
time_str   = ist_now_ui.strftime("%d %b %Y  %H:%M IST")

hcol1, hcol2 = st.columns([3, 1])
with hcol1:
    st.markdown(
        "<h2 style='margin:0;background:-webkit-linear-gradient(45deg,#ffd700,#f0883e);"
        "-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-size:1.5rem'>"
        "⚡ ALGO ENGINE TERMINAL</h2>", unsafe_allow_html=True)
with hcol2:
    st.markdown(f"<p style='text-align:right;color:#6e7681;font-size:.8rem;margin:6px 0 0'>{time_str}</p>",
                unsafe_allow_html=True)

# ── SESSION STATUS BAR ────────────────────────────────────────────────────────
m_now = ist_now_ui.hour * 60 + ist_now_ui.minute
wd    = ist_now_ui.weekday()
nse_live  = wd < 5 and 555 <= m_now <= 935
mcx_live  = wd < 5 and 570 <= m_now <= 1410
cme_live  = wd != 5 and not (wd == 6 and ist_now_ui.hour < 3)

nse_cls  = "sess-live" if nse_live else "sess-closed"
mcx_cls  = "sess-live" if mcx_live else "sess-closed"
cme_cls  = "sess-live" if cme_live else "sess-closed"

st.markdown(f"""<div class="sess-bar">
  <span class="sess {nse_cls}">{'🟢' if nse_live else '🔴'} NSE {'09:15–15:30' if nse_live else 'CLOSED'}</span>
  <span class="sess {mcx_cls}">{'🟢' if mcx_live else '🔴'} MCX {'LIVE' if mcx_live else 'CLOSED'}</span>
  <span class="sess {cme_cls}">{'🟢' if cme_live else '🔴'} COMEX {'LIVE' if cme_live else 'CLOSED'}</span>
  <span class="sess sess-always">🔵 CRYPTO 24/7</span>
</div>""", unsafe_allow_html=True)


# ── DATA LOAD ─────────────────────────────────────────────────────────────────
try:
    live_df    = _db_query(
        "SELECT ticker as Asset, market_status as Status, close_price as Price, "
        "distance_pct as '% Gap', atr as ATR, trend as '15m', htf_trend as '1H', "
        "adx as ADX, vol_ratio as 'Vol', last_update as Updated "
        "FROM live_market_data ORDER BY "
        "CASE WHEN market_status LIKE '%LIVE%' OR market_status LIKE '%24/7%' THEN 0 ELSE 1 END, "
        "distance_pct ASC")
    history_df = _db_query("SELECT * FROM trades WHERE status!='OPEN' ORDER BY id DESC LIMIT 100")
    open_df    = _db_query("SELECT * FROM trades WHERE status='OPEN' ORDER BY id DESC")
    anc_df     = _db_query("SELECT key FROM system_status WHERE key LIKE 'anchor_%'")
    active_anchors = set(k.replace('anchor_','') for k in anc_df['key'].tolist()) if not anc_df.empty else set()
except Exception:
    live_df = history_df = open_df = pd.DataFrame()
    active_anchors = set()

# ── PNL CALCS ─────────────────────────────────────────────────────────────────
realized_pnl = 0.0
if not history_df.empty:
    history_df['Yield'] = np.where(
        history_df['signal_type'].str.lower()=='long',
        (history_df['exit_price']-history_df['entry_price'])/history_df['entry_price'],
        (history_df['entry_price']-history_df['exit_price'])/history_df['entry_price'])
    history_df['PnL'] = history_df['Yield'] * TRADE_ALLOCATION
    bm = (history_df['status'].str.contains('BREAK-EVEN',na=False) &
          ~history_df['status'].str.contains('TP|WIN',na=False))
    history_df.loc[bm,'PnL'] = 0.0
    realized_pnl = history_df['PnL'].sum()

total_unrealized = 0.0
if not open_df.empty and not live_df.empty:
    price_map = live_df.set_index('Asset')['Price'].to_dict()
    open_df['LivePrice'] = open_df['ticker'].map(price_map)
    open_df['Yield'] = np.where(
        open_df['signal_type'].str.lower()=='long',
        (open_df['LivePrice']-open_df['entry_price'])/open_df['entry_price'],
        (open_df['entry_price']-open_df['LivePrice'])/open_df['entry_price'])
    open_df['Unrlzd PnL'] = (open_df['Yield']*TRADE_ALLOCATION).round(2)
    total_unrealized = open_df['Unrlzd PnL'].sum()
    open_df['Risk'] = np.where(
        ((open_df['signal_type'].str.lower()=='long')  & (open_df['sl']>=open_df['entry_price'])) |
        ((open_df['signal_type'].str.lower()=='short') & (open_df['sl']<=open_df['entry_price'])),
        '🛡️ FREE','⚠️ RISK')

    def _tin(s):
        try:
            m = (datetime.now()-datetime.strptime(s,"%Y-%m-%d %I:%M %p (IST)")).total_seconds()/60
            return f"{int(m//60)}h {int(m%60)}m" if m>=60 else f"{int(m)}m"
        except: return "—"
    def _dpct(row, col):
        try: return f"{abs((row[col]-row['LivePrice'])/row['LivePrice']*100):.1f}%"
        except: return "—"

    open_df['Time In'] = open_df['entry_time'].apply(_tin)
    open_df['→ SL']    = open_df.apply(lambda r: _dpct(r,'sl'), axis=1)
    open_df['→ TP']    = open_df.apply(lambda r: _dpct(r,'tp'), axis=1)

# ── STATS ─────────────────────────────────────────────────────────────────────
tc  = len(history_df)
wc  = len(history_df[history_df['status'].str.contains('TP|WIN',na=False)]) if tc else 0
bc  = len(history_df[history_df['status'].str.contains('BREAK-EVEN',na=False) &
         ~history_df['status'].str.contains('TP|WIN',na=False)]) if tc else 0
lc  = len(history_df[history_df['status'].str.contains('LOSS|SL HIT',na=False)]) if tc else 0
dec = wc + lc
wr  = f"{wc/dec*100:.0f}%" if dec else "—"

if dec:
    aw  = history_df[history_df['status'].str.contains('TP|WIN',na=False)]['PnL'].mean() if wc else 0
    al  = history_df[history_df['status'].str.contains('LOSS|SL HIT',na=False)]['PnL'].mean() if lc else 0
    exp = f"₹{(wc/dec*aw + lc/dec*al):,.0f}"
else:
    exp = "—"

oc    = len(open_df)
rf    = len(open_df[open_df['Risk']=='🛡️ FREE']) if not open_df.empty else 0
eq    = INITIAL_CAPITAL + realized_pnl
pnl_c = "#3fb950" if realized_pnl >= 0 else "#f85149"
upnl_c= "#3fb950" if total_unrealized >= 0 else "#f85149"

# ── METRIC CARDS ─────────────────────────────────────────────────────────────
st.markdown(f"""<div class="metrics-row">
  <div class="mc">
    <div class="mc-label">Closed Trades</div>
    <div class="mc-value mc-neutral">{tc}</div>
    <div class="mc-sub">{wc}W · {bc}BE · {lc}L</div>
  </div>
  <div class="mc mc-win">
    <div class="mc-label">Win Rate</div>
    <div class="mc-value">{wr}</div>
    <div class="mc-sub">{dec} decided</div>
  </div>
  <div class="mc">
    <div class="mc-label">Expectancy</div>
    <div class="mc-value mc-gold" style="font-size:1.15rem">{exp}</div>
    <div class="mc-sub">per trade</div>
  </div>
  <div class="mc">
    <div class="mc-label">Realized PnL</div>
    <div class="mc-value" style="color:{pnl_c};font-size:1.15rem">{_fmt_pnl(realized_pnl)}</div>
    <div class="mc-sub">from ₹{INITIAL_CAPITAL:,.0f}</div>
  </div>
  <div class="mc">
    <div class="mc-label">Unrealized</div>
    <div class="mc-value" style="color:{upnl_c};font-size:1.15rem">{_fmt_pnl(total_unrealized)}</div>
    <div class="mc-sub">{oc} open · {rf} risk-free</div>
  </div>
  <div class="mc mc-gold">
    <div class="mc-label">Equity</div>
    <div class="mc-value" style="font-size:1.15rem">₹{eq:,.0f}</div>
    <div class="mc-sub">{_fmt_pnl(eq-INITIAL_CAPITAL)} total</div>
  </div>
</div>""", unsafe_allow_html=True)


# ── TABS ──────────────────────────────────────────────────────────────────────
t_scan, t_open, t_ledger, t_blocked, t_chart = st.tabs([
    f"📡 Scanner ({len(live_df)})",
    f"🟢 Open ({oc})",
    f"📚 Ledger ({tc})",
    f"🚫 Blocked",
    "📈 Chart"
])

# ── SCANNER TAB ───────────────────────────────────────────────────────────────
with t_scan:
    if not live_df.empty:
        live_df['Signal?'] = live_df['Asset'].apply(
            lambda a: "🔔 PENDING" if a in active_anchors else "")

        cols_order = ['Asset','Status','Price','ATR','% Gap','15m','1H','ADX','Vol','Signal?','Updated']
        live_df = live_df[[c for c in cols_order if c in live_df.columns]]

        def _heatmap_style(row):
            base = 'color: #f0f6fc; '
            try:
                gap   = float(row['% Gap'])
                status = str(row.get('Status',''))
                is_live = 'LIVE' in status or '24/7' in status
                asset  = row.get('Asset','')
                has_anc = asset in active_anchors

                if has_anc:
                    bg = 'background-color: rgba(163,113,247,0.18); '
                elif is_live and gap < 0.10:
                    bg = 'background-color: rgba(248,81,73,0.18); '
                elif is_live and gap < 0.50:
                    bg = 'background-color: rgba(227,179,65,0.12); '
                else:
                    bg = ''
                return [base + bg] * len(row)
            except Exception:
                return [base] * len(row)

        styled = live_df.style.apply(_heatmap_style, axis=1)
        st.markdown('<div class="section-title">Signal Scanner — sorted by EMA proximity ↑</div>',
                    unsafe_allow_html=True)
        st.markdown("""<div style="font-size:.74rem;color:#6e7681;margin-bottom:6px">
            🔴 Red = <0.1% gap (imminent) · 🟡 Amber = <0.5% gap (approaching) · 🟣 Purple = anchor active</div>""", unsafe_allow_html=True)
        st.dataframe(styled, use_container_width=True,
                     height=_dyn_height(len(live_df), rpx=35, hdr=38), hide_index=True)
    else:
        st.info("⏳ Waiting for first 15m boundary scan...")


# ── OPEN TRADES TAB ───────────────────────────────────────────────────────────
with t_open:
    if open_df.empty:
        st.info("No active trades.")
    else:
        st.markdown(f'<div class="section-title">{oc} Open Position{"s" if oc!=1 else ""}</div>',
                    unsafe_allow_html=True)
        for _, row in open_df.iterrows():
            direction  = str(row['signal_type']).upper()
            card_cls   = "trade-card-long" if direction=="LONG" else "trade-card-short"
            dir_cls    = "tc-dir-long"     if direction=="LONG" else "tc-dir-short"
            pnl_val    = row.get('Unrlzd PnL', 0.0)
            pnl_cls    = "tc-pnl-pos" if (pnl_val or 0) >= 0 else "tc-pnl-neg"
            risk_badge = (f'<span class="tc-badge-free">{row.get("Risk","")}</span>'
                          if row.get("Risk","") == "🛡️ FREE"
                          else f'<span class="tc-badge-risk">{row.get("Risk","")}</span>')
            lp    = row.get('LivePrice', row['entry_price'])
            sl_d  = row.get('→ SL','—')
            tp_d  = row.get('→ TP','—')
            tin   = row.get('Time In','—')
            st.markdown(f"""<div class="trade-card {card_cls}">
  <div class="tc-row">
    <span class="tc-asset">{row['ticker']}</span>
    <span class="{dir_cls}">{direction}</span>
    {risk_badge}
    <span class="{pnl_cls}">{_fmt_pnl(pnl_val or 0)}</span>
  </div>
  <div class="tc-row" style="margin-top:6px">
    <span class="tc-stat">Entry <b>{row['entry_price']:.2f}</b></span>
    <span class="tc-stat">Live <b>{lp:.2f}</b></span>
    <span class="tc-stat">SL <b>{row['sl']:.2f}</b> <span style="color:#6e7681">({sl_d})</span></span>
    <span class="tc-stat">TP <b>{row['tp']:.2f}</b> <span style="color:#6e7681">({tp_d})</span></span>
    <span class="tc-stat">⏱ {tin}</span>
    <span class="tc-stat">HTF {row.get('htf_trend','—')}</span>
  </div>
</div>""", unsafe_allow_html=True)


# ── LEDGER TAB ────────────────────────────────────────────────────────────────
with t_ledger:
    if history_df.empty:
        st.info("No closed trades.")
    else:
        if tc > 0:
            best  = history_df['PnL'].max()
            worst = history_df['PnL'].min()
            st.markdown(f"""<div style="display:flex;gap:16px;flex-wrap:wrap;
                margin-bottom:8px;font-size:.8rem;color:#8b949e">
                <span>Best: <b style="color:#3fb950">₹{best:,.0f}</b></span>
                <span>Worst: <b style="color:#f85149">₹{worst:,.0f}</b></span>
                <span>Avg Win: <b style="color:#3fb950">₹{history_df[history_df['PnL']>0]['PnL'].mean():,.0f}</b></span>
                <span>Avg Loss: <b style="color:#f85149">₹{history_df[history_df['PnL']<0]['PnL'].mean():,.0f}</b></span>
            </div>""", unsafe_allow_html=True)

        disp = history_df[['ticker','signal_type','entry_time','entry_price',
                            'exit_price','status','PnL']].copy()
        disp.columns = ['Asset','Dir','Entry Time','Entry','Exit','Status','PnL (₹)']
        disp['PnL (₹)'] = disp['PnL (₹)'].round(2)

        def _ledger_color(val):
            try:
                v = float(val)
                return 'color: #3fb950' if v > 0 else ('color: #f85149' if v < 0 else 'color: #a371f7')
            except Exception:
                return ''

        st.dataframe(
            disp.style.map(_ledger_color, subset=['PnL (₹)']),
            use_container_width=True, height=_dyn_height(len(disp)), hide_index=True)


# ── BLOCKED TAB ───────────────────────────────────────────────────────────────
with t_blocked:
    try:
        bdf = _db_query(
            "SELECT ticker as Asset, signal_type as Dir, timestamp as Time, "
            "price as Price, adx as ADX, htf_trend as '1H', vol_ratio as Vol, "
            "rejection_reasons as Reason "
            "FROM blocked_signals ORDER BY id DESC LIMIT 50")
        if not bdf.empty:
            st.dataframe(bdf, use_container_width=True,
                         height=_dyn_height(len(bdf)), hide_index=True)
        else:
            st.info("No blocked signals logged yet.")
    except Exception:
        st.info("No blocked signals logged yet.")


# ── CHART TAB ─────────────────────────────────────────────────────────────────
with t_chart:
    if not live_df.empty:
        all_assets = sorted(live_df['Asset'].tolist())
        sel = st.selectbox("Select Asset", ["— select —"] + all_assets,
                           label_visibility="collapsed")
        if sel != "— select —":
            with st.spinner("Loading chart..."):
                try:
                    sym = next(i['yf_symbol'] for i in WATCHLIST if i['name']==sel)
                    cdf = get_chart_data(sym)
                    if cdf is not None and len(cdf) >= 5:
                        last_adx = cdf['ADX'].iloc[-1]
                        last_atr = cdf['ATR'].iloc[-1]
                        last_cls = cdf['Close'].iloc[-1]
                        trend_up = cdf['EMA5'].iloc[-1] > cdf['EMA39'].iloc[-1]
                        htf_bull = cdf['Close'].iloc[-1] > cdf['EMA39_1H'].iloc[-1]

                        trend_col = "#3fb950" if trend_up else "#f85149"
                        htf_col   = "#3fb950" if htf_bull else "#f85149"
                        adx_col   = "#3fb950" if last_adx > 25 else ("#e3b341" if last_adx > 20 else "#f85149")
                        st.markdown(f"""<div style="display:flex;gap:16px;flex-wrap:wrap;
                            margin-bottom:8px;font-size:.8rem">
                            <span style="color:#f0f6fc;font-weight:700">{sel}</span>
                            <span style="color:{trend_col}">15m: {'🟢 Bull' if trend_up else '🔴 Bear'}</span>
                            <span style="color:{htf_col}">1H: {'🟢 Bull' if htf_bull else '🔴 Bear'}</span>
                            <span style="color:{adx_col}">ADX: {last_adx:.1f}</span>
                            <span style="color:#8b949e">ATR: {last_atr:.2f}</span>
                            <span style="color:#8b949e">Price: {last_cls:.2f}</span>
                        </div>""", unsafe_allow_html=True)

                        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                            vertical_spacing=0.03, row_heights=[0.72,0.28])
                        x = cdf.index.strftime('%d/%m %H:%M')

                        fig.add_trace(go.Candlestick(x=x, open=cdf['Open'], high=cdf['High'],
                            low=cdf['Low'], close=cdf['Close'], name="Price",
                            increasing_line_color='#3fb950', decreasing_line_color='#f85149',
                            increasing_fillcolor='rgba(63,185,80,0.7)',
                            decreasing_fillcolor='rgba(248,81,73,0.7)'), row=1, col=1)

                        fig.add_trace(go.Scatter(x=x, y=cdf['EMA5'],
                            line=dict(color='#58a6ff',width=1.5), name='EMA5'), row=1, col=1)
                        fig.add_trace(go.Scatter(x=x, y=cdf['EMA39'],
                            line=dict(color='#f0883e',width=2.0), name='EMA39'), row=1, col=1)
                        fig.add_trace(go.Scatter(x=x, y=cdf['EMA39_1H'],
                            line=dict(color='#a371f7',width=1.5,dash='dot'), name='EMA39 1H',
                            opacity=0.8), row=1, col=1)

                        fig.add_trace(go.Scatter(x=x, y=cdf['ADX'],
                            line=dict(color='#ffd700',width=1.5), name='ADX',
                            fill='tozeroy', fillcolor='rgba(255,215,0,0.04)'), row=2, col=1)
                        fig.add_hline(y=20, line_dash="dot", line_color="rgba(110,118,129,0.5)",
                                      annotation_text="20", annotation_font_color="#6e7681",
                                      row=2, col=1)
                        fig.add_hline(y=25, line_dash="dot", line_color="rgba(63,185,80,0.3)",
                                      annotation_text="25", annotation_font_color="#3fb950",
                                      row=2, col=1)

                        fig.update_layout(
                            template="plotly_dark",
                            paper_bgcolor='#0d1117', plot_bgcolor='#0d1117',
                            xaxis_rangeslider_visible=False,
                            height=640,
                            legend=dict(orientation='h', y=1.01, x=0,
                                        bgcolor='rgba(0,0,0,0)', font=dict(size=11)),
                            margin=dict(t=10, b=10, l=0, r=0),
                            xaxis2=dict(showgrid=True, gridcolor='rgba(33,38,45,0.8)'),
                            yaxis=dict(showgrid=True, gridcolor='rgba(33,38,45,0.8)',side='right'),
                            yaxis2=dict(showgrid=True, gridcolor='rgba(33,38,45,0.5)',
                                        range=[0,60], side='right'))
                        st.plotly_chart(fig, use_container_width=True)
                        del fig
                    else:
                        st.warning("Not enough data to display chart.")
                except Exception:
                    st.error("Chart unavailable for this asset.")
    else:
        st.info("⏳ Waiting for first data sync...")
