"""
================================================================================
  ALGO ENGINE TERMINAL — PRODUCTION v3.0
  Algo by Vinayak | Engineered for Streamlit Community Cloud (1GB RAM)

  v3.0 IMPROVEMENTS over v2.1:
  [L1] Progressive trailing stop — 3 levels (+1R→BE, +2R→+1R, +3R→+2R)
       instead of single level. Eliminates massive profit giveback.
  [L2] ADX velocity requires 1-point minimum drop to reject. Was rejecting
       on floating-point micro-artefacts like 45.2 vs 45.3.
  [L3] Vol_Ratio filter added (< 0.8x avg → reject). Volume now actually
       used as confirmation. Eliminates thin-air crossovers.
  [L4] signal_id rounded to nearest 15min. Prevents yfinance timestamp
       microsecond drift from generating duplicate signals on same candle.
  [L5] Post-loss cooldown: 15-minute moratorium after SL HIT per ticker.
  [R1] Chart uses 5d-only fetch, separate from 15d engine function.
       Saves ~66% chart memory.
  [R2] Dynamic dataframe heights based on row count.
  [R3] Architectural note block removed from production code.
  [U1] Win Rate % and Expectancy added to metrics matrix.
  [U2] ATR column added to heatmap (essential for position sizing).
  [U3] EMA39_1H dashed line overlay added to chart (HTF reference).
  [U4] Open trades: Time In Trade + Distance to SL/TP % columns.
  [U5] Next scan countdown added to sidebar.
================================================================================
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


# ==========================================
# INLINE INDICATORS  (replaces pandas_ta)
# ==========================================

def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False, min_periods=length).mean()

def _atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    pc = close.shift(1)
    tr = pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(span=length, adjust=False, min_periods=length).mean()

def _adx(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    up, down = high.diff(), -(low.diff())
    pdm   = pd.Series(np.where((up > down) & (up > 0),   up,   0.0), index=high.index)
    mdm   = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=high.index)
    atr_s = _atr(high, low, close, length)
    pdi   = 100 * pdm.ewm(span=length, adjust=False, min_periods=length).mean() / atr_s
    mdi   = 100 * mdm.ewm(span=length, adjust=False, min_periods=length).mean() / atr_s
    denom = (pdi + mdi).replace(0, np.nan)
    dx    = (100 * (pdi - mdi).abs() / denom).fillna(0)
    return dx.ewm(span=length, adjust=False, min_periods=length).mean()


# ==========================================
# 0. PAGE CONFIG & CSS
# ==========================================
st.set_page_config(page_title="Algo Engine Terminal", layout="wide", initial_sidebar_state="expanded")

st.markdown("""<style>
.block-container{padding-top:1.2rem!important;padding-bottom:0.5rem!important;max-width:99%!important}
.metrics-matrix{width:100%;border-collapse:collapse;margin:8px 0 16px;
  background:linear-gradient(145deg,#16181c,#0e1117);border-radius:8px;overflow:hidden;
  border:1px solid #2b303b;box-shadow:0 4px 15px rgba(0,0,0,.2);text-align:center;font-family:sans-serif}
.metrics-matrix th{background:#121418;color:#8b949e;font-size:.82rem;font-weight:600;
  text-transform:uppercase;padding:10px 6px;border:1px solid #2b303b;letter-spacing:.5px}
.metrics-matrix td{padding:8px 6px;border:1px solid #2b303b}
.metrics-matrix .val{font-size:1.65rem;font-weight:700;color:#f0f6fc}
.metrics-matrix .pct{font-size:.95rem;font-weight:500}
.metrics-matrix .row-title{font-size:.88rem;color:#8b949e;font-weight:600;text-align:left;padding-left:14px}
.color-win{color:#3fb950}.color-loss{color:#f85149}.color-be{color:#a371f7}.color-open{color:#58a6ff}
.health-ok{color:#3fb950;font-weight:600}.health-warn{color:#f0883e;font-weight:600}
.countdown{font-size:.78rem;color:#8b949e;margin-top:3px}
.stTabs [data-baseweb="tab-list"]{gap:3px;border-bottom:1px solid #2b303b}
.stTabs [data-baseweb="tab"]{white-space:nowrap!important;padding:9px 18px;background:transparent;
  color:#8b949e;font-size:.93rem;font-weight:500;border:none}
.stTabs [aria-selected="true"]{background:rgba(88,166,255,.1)!important;color:#58a6ff!important;
  border-bottom:3px solid #58a6ff!important;border-radius:6px 6px 0 0}
</style>""", unsafe_allow_html=True)


# ==========================================
# 1. CONFIG & DB
# ==========================================
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


def get_db_connection() -> sqlite3.Connection:
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
            htf_trend TEXT, vol_ratio REAL, adx REAL, atr REAL)''')
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
        ]:
            try: c.execute(sql)
            except Exception: pass
        conn.commit()


setup_database()

def _db_is_fresh() -> bool:
    try:
        with contextlib.closing(get_db_connection()) as conn:
            return conn.execute("SELECT count(*) FROM trades").fetchone()[0] == 0
    except Exception:
        return True

_FRESH_DB = _db_is_fresh()


# ==========================================
# 2. COMMUNICATION
# ==========================================

def send_telegram_alert(message: str, test_mode: bool = False):
    if not TELEGRAM_TOKEN: return False if test_mode else None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message.replace("&","&amp;"), 'parse_mode':'HTML'}
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
        ist_now  = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        date_str = ist_now.strftime('%Y-%m-%d')
        api_url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
        for df, lbl in [(dft,"Trades"),(dfb,"Blocked")]:
            buf = io.BytesIO(); df.to_csv(buf, index=False); buf.seek(0)
            try:
                requests.post(api_url,
                    data={'chat_id':TELEGRAM_CHAT_ID,
                          'caption':f"📊 <b>Daily Backup: {lbl}</b>\n{date_str}",'parse_mode':'HTML'},
                    files={'document':(f"{lbl}_{date_str}.csv", buf,'text/csv')},timeout=20)
            except Exception: pass
            del buf, df
            time.sleep(2)
    except Exception: pass


# ==========================================
# 3. WATCHLIST
# ==========================================
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


# ==========================================
# 4. DATA ENGINE
# ==========================================
_api_lock = threading.Lock()


def _yf_download(symbol: str, period: str) -> pd.DataFrame | None:
    try:
        with _api_lock:
            df = yf.download(symbol, interval="15m", period=period,
                             progress=False, auto_adjust=True)
        if df is None or df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception:
        return None


def _add_indicators(df: pd.DataFrame) -> pd.DataFrame | None:
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
            df['Vol_Ratio'] = np.where(vm > 0, df['Volume'] / vm, 1.0)
            del vm
        else:
            df['Vol_Ratio'] = 1.0
        df.dropna(subset=['EMA39_1H','EMA39','EMA5','ATR'], inplace=True)
        return df if len(df) >= 5 else None
    except Exception:
        return None


def fetch_and_analyze(item: dict) -> pd.DataFrame | None:
    """15d fetch for signal engine — gives 94+ 1H bars for NSE, 360+ for crypto."""
    df = _yf_download(item['yf_symbol'], '15d')
    return _add_indicators(df) if df is not None else None


@st.cache_data(ttl=300)
def get_chart_data(yf_symbol: str) -> pd.DataFrame | None:
    """[R1] 5d-only chart fetch. Separate from 15d engine function. Cached 5 min."""
    df = _yf_download(yf_symbol, '5d')
    return _add_indicators(df) if df is not None else None


# ==========================================
# 5. EXECUTION ENGINE
# ==========================================

def process_market_data() -> bool:
    if not _PROCESS_LOCK.acquire(blocking=False): return False
    try:
        with contextlib.closing(get_db_connection()) as conn:
            c = conn.cursor()
            ist_now          = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
            slot_min         = (ist_now.minute // 5) * 5
            scan_time_str    = ist_now.replace(minute=slot_min, second=0, microsecond=0
                                   ).strftime("%Y-%m-%d %I:%M %p (IST)")
            current_date_str = ist_now.strftime("%Y-%m-%d")
            is_15m           = (slot_min % 15 == 0)

            # Daily backup
            c.execute("SELECT value FROM system_status WHERE key='last_backup_date'")
            lbr = c.fetchone()
            if (not lbr or lbr[0] != current_date_str) and ist_now.hour >= 23 and ist_now.minute >= 30:
                send_telegram_csv_backup()
                c.execute("INSERT OR REPLACE INTO system_status VALUES('last_backup_date',?)", (current_date_str,))
                conn.commit()

            for item in WATCHLIST:
                name, ac = item['name'], item['asset_class']

                # Market hours gate
                market_open = True
                if ac == 'NSE':
                    m = ist_now.hour * 60 + ist_now.minute
                    if ist_now.weekday() >= 5 or not (555 <= m <= 935): market_open = False
                elif ac == 'COMMODITY':
                    if ist_now.weekday() == 5 or (ist_now.weekday() == 6 and ist_now.hour < 3):
                        market_open = False
                if not market_open: continue

                c.execute("SELECT count(*) FROM trades WHERE ticker=? AND status='OPEN'", (name,))
                otc = c.fetchone()[0]
                c.execute("SELECT value FROM system_status WHERE key=?", (f"anchor_{name}",))
                anc = c.fetchone()
                if not is_15m and otc == 0 and not anc: continue

                df = fetch_and_analyze(item)
                time.sleep(_INTER_ASSET_SLEEP)
                if df is None: continue

                try:
                    if (ist_now.replace(tzinfo=None) - df.index[-1]).total_seconds() > 3600:
                        del df; continue
                except Exception: pass

                curr, last, prev = df.iloc[-1], df.iloc[-2], df.iloc[-3]

                # Update heatmap — includes ATR
                c.execute("""INSERT OR REPLACE INTO live_market_data VALUES(?,?,?,?,?,?,?,?,?,?,?)""", (
                    name, scan_time_str,
                    round(curr['Close'],2), round(curr['EMA5'],2), round(curr['EMA39'],2),
                    "🟢 Bullish" if curr['EMA5']>curr['EMA39'] else "🔴 Bearish",
                    abs(curr['EMA5']-curr['EMA39'])/curr['EMA39']*100,
                    "🟢 Bullish" if curr['Close']>curr['EMA39_1H'] else "🔴 Bearish",
                    round(curr.get('Vol_Ratio',1.0),2),
                    round(curr.get('ADX',0.0),2),
                    round(curr.get('ATR',0.0),4)))
                conn.commit()

                # ── Open Trade Manager ────────────────────────────────────
                if otc > 0:
                    c.execute("SELECT id,signal_type,sl,tp,entry_price,atr FROM trades WHERE ticker=? AND status='OPEN'", (name,))
                    for t_id, s_type, sl, tp, e_price, atr_val in c.fetchall():
                        atr_val    = atr_val if (atr_val and atr_val > 0) else abs(tp-e_price)/3.75
                        lp         = curr['Close']
                        closed     = False

                        if s_type == 'long':
                            if lp >= tp:
                                stxt = 'TP HIT (WIN)' if lp < (tp+atr_val) else 'TP HIT (GAP UP)'
                                c.execute("UPDATE trades SET status=?,exit_time=?,exit_price=? WHERE id=?",
                                          (stxt, scan_time_str, lp, t_id))
                                send_telegram_alert(f"🎯 <b>{stxt}</b>\n{name} LONG @ {round(lp,2)}")
                                closed = True
                            elif lp <= sl:
                                stxt = ('BREAK-EVEN TP HIT' if sl>e_price else
                                        ('BREAK-EVEN (0 RISK)' if sl==e_price else 'SL HIT (LOSS)'))
                                if lp < (sl-atr_val): stxt += ' (GAP DOWN)'
                                c.execute("UPDATE trades SET status=?,exit_time=?,exit_price=? WHERE id=?",
                                          (stxt, scan_time_str, lp, t_id))
                                send_telegram_alert(f"🛑 <b>{stxt}</b>\n{name} LONG @ {round(lp,2)}")
                                closed = True
                            # [L1] 3-level progressive trail
                            if not closed:
                                pr = (lp-e_price)/atr_val; sr = (sl-e_price)/atr_val
                                nsl = nlbl = None
                                if   pr >= 3.0 and sr < 2.0: nsl,nlbl = round(e_price+2.0*atr_val,2), "+3R → SL to +2R"
                                elif pr >= 2.0 and sr < 1.0: nsl,nlbl = round(e_price+1.0*atr_val,2), "+2R → SL to +1R"
                                elif pr >= 1.0 and sr < 0.25:nsl,nlbl = round(e_price+0.25*atr_val,2),"+1R → SL to BE+0.25R"
                                if nsl:
                                    c.execute("UPDATE trades SET sl=? WHERE id=?",(nsl,t_id))
                                    send_telegram_alert(f"🛡️ <b>TRAIL: {name}</b>\n{nlbl} → SL: {nsl}")

                        elif s_type == 'short':
                            if lp <= tp:
                                stxt = 'TP HIT (WIN)' if lp>(tp-atr_val) else 'TP HIT (GAP DOWN)'
                                c.execute("UPDATE trades SET status=?,exit_time=?,exit_price=? WHERE id=?",
                                          (stxt, scan_time_str, lp, t_id))
                                send_telegram_alert(f"🎯 <b>{stxt}</b>\n{name} SHORT @ {round(lp,2)}")
                                closed = True
                            elif lp >= sl:
                                stxt = ('BREAK-EVEN TP HIT' if sl<e_price else
                                        ('BREAK-EVEN (0 RISK)' if sl==e_price else 'SL HIT (LOSS)'))
                                if lp > (sl+atr_val): stxt += ' (GAP UP)'
                                c.execute("UPDATE trades SET status=?,exit_time=?,exit_price=? WHERE id=?",
                                          (stxt, scan_time_str, lp, t_id))
                                send_telegram_alert(f"🛑 <b>{stxt}</b>\n{name} SHORT @ {round(lp,2)}")
                                closed = True
                            # [L1] 3-level progressive trail
                            if not closed:
                                pr = (e_price-lp)/atr_val; sr = (e_price-sl)/atr_val
                                nsl = nlbl = None
                                if   pr >= 3.0 and sr < 2.0: nsl,nlbl = round(e_price-2.0*atr_val,2), "+3R → SL to +2R"
                                elif pr >= 2.0 and sr < 1.0: nsl,nlbl = round(e_price-1.0*atr_val,2), "+2R → SL to +1R"
                                elif pr >= 1.0 and sr < 0.25:nsl,nlbl = round(e_price-0.25*atr_val,2),"+1R → SL to BE+0.25R"
                                if nsl:
                                    c.execute("UPDATE trades SET sl=? WHERE id=?",(nsl,t_id))
                                    send_telegram_alert(f"🛡️ <b>TRAIL: {name}</b>\n{nlbl} → SL: {nsl}")
                    conn.commit()

                # ── Anchor Logic ──────────────────────────────────────────
                # [L4] Round to 15min for stable dedup
                signal_id = str(last.name.round('15min')) if hasattr(last.name,'round') else str(last.name)

                if not anc and is_15m:
                    c.execute("SELECT value FROM system_status WHERE key=?", (f"proc_{name}",))
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
                                      (f"anchor_{name}", anc_val))
                            conn.commit()
                            anc = (anc_val,)

                # ── Anchor Evaluation ─────────────────────────────────────
                if anc:
                    ad = anc[0].split('|')
                    if len(ad) == 4:
                        a_start = datetime.strptime(ad[0], "%Y-%m-%d %H:%M:%S")
                        a_dir, a_atr, a_sid = ad[1], float(ad[2]), ad[3]
                        me = (ist_now.replace(tzinfo=None)-a_start).total_seconds()/60.0

                        if me <= 16.0:
                            evc   = last if me >= 14.0 else curr
                            p_adx = prev.get('ADX',0.0) if me>=14.0 else last.get('ADX',0.0)
                            l_adx = evc.get('ADX',0.0)
                            l_vol = evc.get('Vol_Ratio',1.0)
                            l_htf = "🟢 Bullish" if evc['Close']>evc['EMA39_1H'] else "🔴 Bearish"
                            r_htf = "🟢 Bullish" if a_dir=="LONG" else "🔴 Bearish"

                            rej = []

                            # [L5] Post-loss cooldown
                            c.execute("""SELECT status,exit_time FROM trades
                                WHERE ticker=? AND status NOT LIKE 'OPEN' ORDER BY id DESC LIMIT 1""", (name,))
                            lt = c.fetchone()
                            if lt and lt[0] and lt[1] and ('LOSS' in str(lt[0]) or 'SL HIT' in str(lt[0])):
                                try:
                                    dt_exit = datetime.strptime(lt[1], "%Y-%m-%d %I:%M %p (IST)")
                                    msince  = (ist_now.replace(tzinfo=None)-dt_exit).total_seconds()/60
                                    if msince < 15: rej.append(f"Post-loss cooldown ({int(15-msince)}m left).")
                                except Exception: pass

                            if otc > 0: rej.append("Active trade open.")

                            # [L2] ADX: require 1-point drop minimum
                            if l_adx <= 20.0:
                                rej.append(f"Weak Trend (ADX {round(l_adx,1)} ≤ 20).")
                            elif l_adx < (p_adx - 1.0):
                                rej.append(f"Falling Momentum (ADX {round(l_adx,1)} ↓ from {round(p_adx,1)}).")

                            # [L3] Volume filter
                            if l_vol < 0.8: rej.append(f"Low Volume ({round(l_vol,2)}x < 0.8x avg).")

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


# ==========================================
# 6. BACKGROUND DAEMON
# ==========================================

def _sleep_to_next_5m() -> float:
    now   = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    ns    = ((now.minute // 5) + 1) * 5
    nxt   = (now.replace(minute=0, second=2, microsecond=0)+timedelta(hours=1)
             if ns >= 60 else now.replace(minute=ns, second=2, microsecond=0))
    return max((nxt - now).total_seconds(), 10.0)


@st.cache_resource
def _start_background_scanner() -> dict:
    health = {"last_run_ts":None, "run_count":0, "last_error":None, "next_run_secs":0}

    def _loop():
        while True:
            secs = _sleep_to_next_5m()
            health["next_run_secs"] = int(secs)
            time.sleep(secs)
            health["next_run_secs"] = 0
            try:
                process_market_data()
                health["last_run_ts"] = datetime.now(timezone.utc).isoformat()
                health["run_count"]  += 1
                health["last_error"]  = None
            except Exception as e:
                health["last_error"] = str(e)
                try:
                    with contextlib.closing(get_db_connection()) as conn:
                        conn.execute("INSERT INTO system_logs(timestamp,message) VALUES(?,?)",
                                     (str(datetime.now()), f"DAEMON: {e}"))
                        conn.commit()
                except Exception: pass

    threading.Thread(target=_loop, daemon=True, name="AlgoDaemon").start()
    return health


_daemon_health = _start_background_scanner()


# ==========================================
# 7. UI HELPERS
# ==========================================

def _db_query(sql: str, params=()) -> pd.DataFrame:
    with contextlib.closing(get_db_connection()) as c:
        return pd.read_sql_query(sql, c, params=params)

def _db_scalar(sql: str, params=()):
    with contextlib.closing(get_db_connection()) as conn:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else None

def _dyn_height(n: int, rpx: int = 36, hdr: int = 40, lo: int = 80, hi: int = 560) -> int:
    """[R2] Dynamic height: avoids 600px tables for 2 rows."""
    return min(max(hdr + n * rpx, lo), hi)


# ==========================================
# 8. DASHBOARD RENDER
# ==========================================
st_autorefresh(interval=60_000, limit=None, key="keepalive")

if _FRESH_DB:
    st.warning("⚠️ **Empty database detected.** Use **Restore Database** in the sidebar "
               "with your latest backup CSV to recover trade history.", icon="🗄️")

# ── SIDEBAR ──────────────────────────────────────────────────────────────────
st.sidebar.markdown("<h3>⚙️ Control Panel</h3>", unsafe_allow_html=True)

rc2  = _daemon_health.get("run_count", 0)
lerr = _daemon_health.get("last_error")
nsec = _daemon_health.get("next_run_secs", 0)

if lerr is None:
    st.sidebar.markdown(f'<span class="health-ok">✅ Daemon LIVE</span> — {rc2} cycles',
                        unsafe_allow_html=True)
else:
    st.sidebar.markdown(f'<span class="health-warn">⚠️ {str(lerr)[:55]}</span>', unsafe_allow_html=True)

ls = _db_scalar("SELECT value FROM system_status WHERE key='last_scan'")
st.sidebar.info(f"⏱️ **Last Sync:** {ls or 'Initializing...'}")
if nsec > 0:
    st.sidebar.markdown(f'<div class="countdown">⏳ Next scan in ~{nsec}s</div>', unsafe_allow_html=True)

if st.sidebar.button("🔄 Force Manual Sync"):
    if _PROCESS_LOCK.locked():
        st.sidebar.warning("⏳ Daemon running. Wait.")
    else:
        with st.spinner("Syncing..."): process_market_data()
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("<h3>🛡️ Backup & Restore</h3>", unsafe_allow_html=True)
ca, cb = st.sidebar.columns(2)
try:
    with ca: st.download_button("⬇️ Trades",
                _db_query("SELECT * FROM trades").to_csv(index=False).encode(),
                f"Trades_{datetime.now().strftime('%Y-%m-%d')}.csv","text/csv")
    with cb: st.download_button("⬇️ Blocked",
                _db_query("SELECT * FROM blocked_signals").to_csv(index=False).encode(),
                f"Blocked_{datetime.now().strftime('%Y-%m-%d')}.csv","text/csv")
except Exception: pass

st.sidebar.markdown("<b>Restore (Upload CSV)</b>", unsafe_allow_html=True)
uf = st.sidebar.file_uploader("Upload CSV", type=None, label_visibility="collapsed")
if uf is not None and st.sidebar.button("⚙️ Execute Auto-Restore"):
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
                    cur.execute("SELECT id FROM trades WHERE ticker=? AND entry_time=?",(row['ticker'],row['entry_time']))
                    if not cur.fetchone():
                        cur.execute("""INSERT INTO trades(ticker,signal_type,entry_time,entry_price,sl,tp,
                            status,exit_time,exit_price,htf_trend,vol_ratio,atr,adx) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            tuple(row[k] for k in ['ticker','signal_type','entry_time','entry_price','sl','tp',
                                  'status','exit_time','exit_price','htf_trend','vol_ratio','atr','adx']))
                rc.commit()
                st.sidebar.success("✅ Trades Restored!")
            elif 'rejection_reasons' in rcol or 'Rejection Reasons' in rcol:
                rmap = {'Asset':'ticker','Signal':'signal_type','Time (IST)':'timestamp',
                        'Price':'price','ADX':'adx','1H Trend':'htf_trend',
                        'Vol (x)':'vol_ratio','Rejection Reasons':'rejection_reasons'}
                rdf = rdf.rename(columns=rmap).fillna({'adx':0.0,'htf_trend':'','vol_ratio':1.0,'rejection_reasons':''})
                for _, row in rdf.iterrows():
                    cur.execute("SELECT id FROM blocked_signals WHERE ticker=? AND timestamp=?",(row['ticker'],row['timestamp']))
                    if not cur.fetchone():
                        cur.execute("""INSERT INTO blocked_signals(ticker,signal_type,timestamp,price,
                            adx,htf_trend,vol_ratio,rejection_reasons) VALUES(?,?,?,?,?,?,?,?)""",
                            tuple(row[k] for k in ['ticker','signal_type','timestamp','price',
                                  'adx','htf_trend','vol_ratio','rejection_reasons']))
                rc.commit()
                st.sidebar.success("✅ Blocked Signals Restored!")
            else:
                st.sidebar.error("❌ Unrecognized CSV format.")
        time.sleep(1); st.rerun()
    except Exception as e:
        st.sidebar.error(f"Restore failed: {e}")

st.sidebar.markdown("---")
if st.sidebar.button("🔔 Test Telegram"):
    ok = send_telegram_alert("🧪 <b>DIAGNOSTIC PING</b>", test_mode=True)
    st.sidebar.success("✅ Sent!") if ok else st.sidebar.error("❌ Failed. Check secrets.")


# ── MAIN BODY ─────────────────────────────────────────────────────────────────
try:
    live_df    = _db_query(
        "SELECT ticker as Asset, close_price as Price, distance_pct as '% Gap', "
        "atr as ATR, trend as '15m Trend', htf_trend as '1H Trend', "
        "vol_ratio as 'Vol (x)', adx as ADX, last_update as 'Updated (IST)' "
        "FROM live_market_data ORDER BY distance_pct ASC")
    history_df = _db_query("SELECT * FROM trades WHERE status!='OPEN' ORDER BY id DESC LIMIT 100")
    open_df_ui = _db_query("SELECT * FROM trades WHERE status='OPEN' ORDER BY id DESC")
except Exception:
    live_df = history_df = open_df_ui = pd.DataFrame()

# PnL
realized_pnl = 0.0
if not history_df.empty:
    history_df['Yield'] = np.where(
        history_df['signal_type'].str.lower()=='long',
        (history_df['exit_price']-history_df['entry_price'])/history_df['entry_price'],
        (history_df['entry_price']-history_df['exit_price'])/history_df['entry_price'])
    history_df['PnL (₹)'] = history_df['Yield'] * TRADE_ALLOCATION
    bm = history_df['status'].str.contains('BREAK-EVEN',na=False) & ~history_df['status'].str.contains('TP|WIN',na=False)
    history_df.loc[bm,'PnL (₹)'] = 0.0
    realized_pnl = history_df['PnL (₹)'].sum()

total_unrealized_pnl = 0.0
if not open_df_ui.empty and not live_df.empty:
    open_df_ui = pd.merge(open_df_ui, live_df[['Asset','Price']].rename(columns={'Price':'Latest Price'}),
                          left_on='ticker', right_on='Asset', how='left')
    open_df_ui['Yield'] = np.where(
        open_df_ui['signal_type'].str.lower()=='long',
        (open_df_ui['Latest Price']-open_df_ui['entry_price'])/open_df_ui['entry_price'],
        (open_df_ui['entry_price']-open_df_ui['Latest Price'])/open_df_ui['entry_price'])
    open_df_ui['Unrlzd PnL (₹)'] = (open_df_ui['Yield']*TRADE_ALLOCATION).round(2)
    total_unrealized_pnl = open_df_ui['Unrlzd PnL (₹)'].sum()

    def _tin(s):  # [U4] time in trade
        try:
            m = (datetime.now()-datetime.strptime(s,"%Y-%m-%d %I:%M %p (IST)")).total_seconds()/60
            return f"{int(m//60)}h{int(m%60)}m" if m>=60 else f"{int(m)}m"
        except: return "—"
    def _dpct(row, col):  # [U4] distance to level
        try: return f"{abs((row[col]-row['Latest Price'])/row['Latest Price']*100):.1f}%"
        except: return "—"

    open_df_ui['Time In'] = open_df_ui['entry_time'].apply(_tin)
    open_df_ui['→ SL']    = open_df_ui.apply(lambda r: _dpct(r,'sl'), axis=1)
    open_df_ui['→ TP']    = open_df_ui.apply(lambda r: _dpct(r,'tp'), axis=1)
    open_df_ui['Status']  = np.where(
        ((open_df_ui['signal_type'].str.lower()=='long')  & (open_df_ui['sl']>=open_df_ui['entry_price'])) |
        ((open_df_ui['signal_type'].str.lower()=='short') & (open_df_ui['sl']<=open_df_ui['entry_price'])),
        '🛡️ FREE','⚠️ RISK')

# Counts & stats
tc   = len(history_df)
wc   = len(history_df[history_df['status'].str.contains('TP|WIN',na=False)]) if tc else 0
bc   = len(history_df[history_df['status'].str.contains('BREAK-EVEN',na=False) &
           ~history_df['status'].str.contains('TP|WIN',na=False)]) if tc else 0
lc   = len(history_df[history_df['status'].str.contains('LOSS|SL HIT',na=False)]) if tc else 0
rf   = len(open_df_ui[open_df_ui['Status']=='🛡️ FREE']) if not open_df_ui.empty else 0
dec  = wc + lc
wr   = f"{wc/dec*100:.0f}%" if dec else "—"
if dec:
    aw = history_df[history_df['status'].str.contains('TP|WIN',na=False)]['PnL (₹)'].mean() if wc else 0
    al = history_df[history_df['status'].str.contains('LOSS|SL HIT',na=False)]['PnL (₹)'].mean() if lc else 0
    exp_str = f"₹{(wc/dec*aw + lc/dec*al):,.0f}/trade"
else:
    exp_str = "—"

pc  = 'color-win' if realized_pnl >= 0 else 'color-loss'
upc = 'color-win' if total_unrealized_pnl >= 0 else 'color-loss'

st.markdown("<h1 style='background:-webkit-linear-gradient(45deg,#ffd700,#ffaa00);"
            "-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:4px'>"
            "⚡ Algo Engine by Vinayak</h1>", unsafe_allow_html=True)

st.markdown(f"""
<table class="metrics-matrix">
<tr><th></th><th>Total</th><th>Win</th><th>Break Even</th><th>Loss</th><th>Win Rate</th><th>Expectancy</th></tr>
<tr>
  <td class="row-title">CLOSED TRADES</td>
  <td class="val">{tc}</td><td class="val color-win">{wc}</td>
  <td class="val color-be">{bc}</td><td class="val color-loss">{lc}</td>
  <td class="val color-win">{wr}</td>
  <td class="val" style="font-size:1.05rem">{exp_str}</td>
</tr>
<tr>
  <td class="row-title" style="border-bottom:2px solid #2b303b">CAPITAL & PNL</td>
  <td class="val" style="border-bottom:2px solid #2b303b;font-size:1.05rem">₹{INITIAL_CAPITAL:,.0f}</td>
  <td colspan="5" class="pct" style="border-bottom:2px solid #2b303b;text-align:left;padding-left:16px">
    Realized: <b class="{pc}">₹{realized_pnl:,.2f}</b> &nbsp;|&nbsp;
    Equity: <b style="color:#f0f6fc">₹{INITIAL_CAPITAL+realized_pnl:,.2f}</b> &nbsp;|&nbsp;
    Unrealized: <b class="{upc}">₹{total_unrealized_pnl:,.2f}</b>
  </td>
</tr>
<tr>
  <td class="row-title">OPEN TRADES</td>
  <td class="val color-open">{len(open_df_ui)}</td>
  <td colspan="5" class="pct" style="text-align:left;padding-left:16px">
    🛡️ {rf} Risk-Free &nbsp;|&nbsp; ⚠️ {len(open_df_ui)-rf} At Risk
  </td>
</tr>
</table>""", unsafe_allow_html=True)


# ── TABS ──────────────────────────────────────────────────────────────────────
t_heat, t_open, t_ledger, t_blocked, t_chart = st.tabs(
    ["🔥 Heatmap","🟢 Open Trades","📚 Ledger","🚫 Blocked","📈 Chart"])

with t_heat:
    if not live_df.empty:
        st.dataframe(
            live_df.style.map(
                lambda v: ('background-color:rgba(220,38,38,.35);color:#fff' if pd.notna(v) and float(v)<0.10 else
                           'background-color:rgba(217,119,6,.35);color:#fff' if pd.notna(v) and float(v)<0.50 else ''),
                subset=['% Gap']),
            use_container_width=True, height=_dyn_height(len(live_df)), hide_index=True)
    else:
        st.info("⏳ Waiting for first data sync...")

with t_open:
    if not open_df_ui.empty:
        dc = [c for c in ['ticker','signal_type','entry_time','entry_price','sl','tp',
                           'Latest Price','→ SL','→ TP','Time In','Status','Unrlzd PnL (₹)']
              if c in open_df_ui.columns]
        st.dataframe(open_df_ui[dc], use_container_width=True,
                     height=_dyn_height(len(open_df_ui)), hide_index=True)
    else:
        st.info("No active trades.")

with t_ledger:
    if not history_df.empty:
        st.dataframe(
            history_df[['ticker','signal_type','entry_time','entry_price',
                         'sl','tp','exit_time','exit_price','status','PnL (₹)']],
            use_container_width=True, height=_dyn_height(len(history_df)), hide_index=True)
    else:
        st.info("No closed trades.")

with t_blocked:
    try:
        bdf = _db_query(
            "SELECT ticker as Asset, signal_type as Signal, timestamp as 'Time (IST)', "
            "price as Price, adx as ADX, htf_trend as '1H Trend', vol_ratio as 'Vol (x)', "
            "rejection_reasons as 'Rejection Reasons' "
            "FROM blocked_signals ORDER BY id DESC LIMIT 50")
        st.dataframe(bdf, use_container_width=True, height=_dyn_height(len(bdf)), hide_index=True) if not bdf.empty else st.info("No blocked signals.")
    except Exception: pass

with t_chart:
    # [R1] Uses get_chart_data (5d) not fetch_and_analyze (15d)
    if not live_df.empty:
        sel = st.selectbox("Select Asset:", ["— select —"]+sorted(live_df['Asset'].tolist()),
                           label_visibility="collapsed")
        if sel != "— select —":
            with st.spinner("Loading..."):
                try:
                    sym = next(i['yf_symbol'] for i in WATCHLIST if i['name']==sel)
                    cdf = get_chart_data(sym)
                    if cdf is not None:
                        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                            vertical_spacing=0.04, row_heights=[0.72,0.28])
                        x = cdf.index.strftime('%b %d %H:%M')
                        fig.add_trace(go.Candlestick(x=x, open=cdf['Open'], high=cdf['High'],
                            low=cdf['Low'], close=cdf['Close'], name="Price",
                            increasing_line_color='#3fb950', decreasing_line_color='#f85149'), row=1,col=1)
                        fig.add_trace(go.Scatter(x=x,y=cdf['EMA5'],
                            line=dict(color='#58a6ff',width=1.5),name='EMA 5'),row=1,col=1)
                        fig.add_trace(go.Scatter(x=x,y=cdf['EMA39'],
                            line=dict(color='#f0883e',width=2.0),name='EMA 39'),row=1,col=1)
                        # [U3] HTF reference line
                        fig.add_trace(go.Scatter(x=x,y=cdf['EMA39_1H'],
                            line=dict(color='#a371f7',width=1.5,dash='dash'),name='EMA 39 1H'),row=1,col=1)
                        fig.add_trace(go.Scatter(x=x,y=cdf['ADX'],
                            line=dict(color='#ffd700',width=1.5),name='ADX'),row=2,col=1)
                        fig.add_hline(y=20,line_dash="dot",line_color="#6e7681",
                                      annotation_text="20",row=2,col=1)
                        fig.update_layout(
                            title=dict(text=f"{sel}  ·  ADX: {cdf['ADX'].iloc[-1]:.1f}  ·  ATR: {cdf['ATR'].iloc[-1]:.2f}",
                                       font=dict(size=13)),
                            template="plotly_dark", xaxis_rangeslider_visible=False,
                            height=660, legend=dict(orientation='h',y=1.02),
                            margin=dict(t=55,b=15,l=5,r=5))
                        st.plotly_chart(fig, use_container_width=True)
                        del fig
                except Exception:
                    st.error("Chart unavailable.")
    else:
        st.info("⏳ Waiting for first data sync...")
