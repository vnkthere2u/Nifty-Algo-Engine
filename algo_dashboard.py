import streamlit as st
import pandas as pd
import numpy as np
import pandas_ta as ta
import sqlite3
import time
import requests
import threading
import os
import gc 
import logging
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import contextlib
from datetime import datetime, timedelta, timezone
from tvDatafeed import TvDatafeed, Interval

# Suppress messy tvDatafeed console warnings
logging.getLogger('tvDatafeed').setLevel(logging.CRITICAL)

# ==========================================
# 0. UI INITIALIZATION & CUSTOM CSS
# ==========================================
st.set_page_config(page_title="Algo Engine Terminal", layout="wide", initial_sidebar_state="expanded")

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
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 1. CORE ARCHITECTURE & DB SETUP
# ==========================================
try:
    TELEGRAM_TOKEN = st.secrets["TELEGRAM_TOKEN"]
    TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
except:
    TELEGRAM_TOKEN = ""
    TELEGRAM_CHAT_ID = ""

def get_db_connection():
    conn = sqlite3.connect('nifty_live_trades.db', check_same_thread=False, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def setup_database():
    with contextlib.closing(get_db_connection()) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal_type TEXT, entry_time TEXT, entry_price REAL, sl REAL, tp REAL, status TEXT, exit_time TEXT, exit_price REAL, htf_trend TEXT, vol_ratio REAL, atr REAL, adx REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS system_status (key TEXT PRIMARY KEY, value TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS system_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, message TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS live_market_data (ticker TEXT PRIMARY KEY, last_update TEXT, close_price REAL, ema5 REAL, ema39 REAL, trend TEXT, distance_pct REAL, htf_trend TEXT, vol_ratio REAL, adx REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS blocked_signals (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal_type TEXT, timestamp TEXT, price REAL, adx REAL, htf_trend TEXT, vol_ratio REAL, rejection_reasons TEXT)''')
        
        try: c.execute("ALTER TABLE trades ADD COLUMN htf_trend TEXT")
        except: pass
        try: c.execute("ALTER TABLE trades ADD COLUMN vol_ratio REAL")
        except: pass
        try: c.execute("ALTER TABLE trades ADD COLUMN atr REAL")
        except: pass
        try: c.execute("ALTER TABLE trades ADD COLUMN adx REAL")
        except: pass
        try: c.execute("ALTER TABLE live_market_data ADD COLUMN adx REAL")
        except: pass
        
        conn.commit()

setup_database()

def send_telegram_alert(message, test_mode=False):
    if not TELEGRAM_TOKEN: return False if test_mode else None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message.replace("&", "&amp;"), 'parse_mode': 'HTML'}
    for _ in range(3):
        try: 
            resp = requests.post(url, data=payload, timeout=10)
            if resp.status_code == 200: return True if test_mode else None
            elif resp.status_code == 429: time.sleep(3)
            else: return False if test_mode else None
        except Exception: time.sleep(1)

def send_telegram_csv_backup():
    if not TELEGRAM_TOKEN: return
    try:
        with contextlib.closing(get_db_connection()) as conn:
            df_trades = pd.read_sql_query("SELECT * FROM trades", conn)
            df_blocked = pd.read_sql_query("SELECT * FROM blocked_signals", conn)
            
        ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        date_str = ist_now.strftime('%Y-%m-%d')
        
        trades_filename = f"Trades_Backup_{date_str}.csv"
        df_trades.to_csv(trades_filename, index=False)
        blocked_filename = f"Blocked_Backup_{date_str}.csv"
        df_blocked.to_csv(blocked_filename, index=False)

        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
        payload_trades = {'chat_id': TELEGRAM_CHAT_ID, 'caption': f"📊 <b>Automated Daily Backup: Trades</b>\nDate: {date_str}", 'parse_mode': 'HTML'}
        with open(trades_filename, 'rb') as f: requests.post(url, data=payload_trades, files={'document': f}, timeout=15)
        os.remove(trades_filename)
        
        time.sleep(2)
        
        payload_blocked = {'chat_id': TELEGRAM_CHAT_ID, 'caption': f"🚫 <b>Automated Daily Backup: Blocked Signals</b>\nDate: {date_str}", 'parse_mode': 'HTML'}
        with open(blocked_filename, 'rb') as f: requests.post(url, data=payload_blocked, files={'document': f}, timeout=15)
        os.remove(blocked_filename)
    except Exception: pass

# ==========================================
# 2. DATA FETCHING ENGINE (FULL 18 ASSETS)
# ==========================================
WATCHLIST = [
    {'name': 'NIFTY 50', 'tv_symbol': 'NIFTY', 'tv_exchange': 'NSE', 'yf_symbol': '^NSEI'},
    {'name': 'BANK NIFTY', 'tv_symbol': 'BANKNIFTY', 'tv_exchange': 'NSE', 'yf_symbol': '^NSEBANK'},
    {'name': 'BITCOIN (24/7)', 'tv_symbol': 'BTCUSDT', 'tv_exchange': 'BINANCE', 'yf_symbol': 'BTC-USD'},
    {'name': 'GOLD', 'tv_symbol': 'XAUUSD', 'tv_exchange': 'OANDA', 'yf_symbol': 'GC=F'},
    {'name': 'SILVER', 'tv_symbol': 'XAGUSD', 'tv_exchange': 'OANDA', 'yf_symbol': 'SI=F'},
    {'name': 'CRUDE OIL', 'tv_symbol': 'WTICOUSD', 'tv_exchange': 'OANDA', 'yf_symbol': 'CL=F'},
    {'name': 'HDFC BANK', 'tv_symbol': 'HDFCBANK', 'tv_exchange': 'NSE', 'yf_symbol': 'HDFCBANK.NS'},
    {'name': 'SBI', 'tv_symbol': 'SBIN', 'tv_exchange': 'NSE', 'yf_symbol': 'SBIN.NS'},
    {'name': 'RELIANCE', 'tv_symbol': 'RELIANCE', 'tv_exchange': 'NSE', 'yf_symbol': 'RELIANCE.NS'},
    {'name': 'INFOSYS', 'tv_symbol': 'INFY', 'tv_exchange': 'NSE', 'yf_symbol': 'INFY.NS'},
    {'name': 'TCS', 'tv_symbol': 'TCS', 'tv_exchange': 'NSE', 'yf_symbol': 'TCS.NS'},
    {'name': 'ITC', 'tv_symbol': 'ITC', 'tv_exchange': 'NSE', 'yf_symbol': 'ITC.NS'},
    {'name': 'TATA MOTORS', 'tv_symbol': 'TATAMOTORS', 'tv_exchange': 'NSE', 'yf_symbol': 'TATAMOTORS.NS'},
    {'name': 'TATA STEEL', 'tv_symbol': 'TATASTEEL', 'tv_exchange': 'NSE', 'yf_symbol': 'TATASTEEL.NS'},
    {'name': 'L&T', 'tv_symbol': 'LT', 'tv_exchange': 'NSE', 'yf_symbol': 'LT.NS'},
    {'name': 'BHARTI AIRTEL', 'tv_symbol': 'BHARTIARTL', 'tv_exchange': 'NSE', 'yf_symbol': 'BHARTIARTL.NS'},
    {'name': 'SUN PHARMA', 'tv_symbol': 'SUNPHARMA', 'tv_exchange': 'NSE', 'yf_symbol': 'SUNPHARMA.NS'},
    {'name': 'VEDANTA', 'tv_symbol': 'VEDL', 'tv_exchange': 'NSE', 'yf_symbol': 'VEDL.NS'}
]

@st.cache_resource
def get_tv_connection():
    # Locks the socket connection so Streamlit UI refreshes don't spawn 100s of ghost connections
    return TvDatafeed()

def fetch_and_analyze(item):
    tv_conn = get_tv_connection()
    df = None
    try:
        df_tv = tv_conn.get_hist(symbol=item['tv_symbol'], exchange=item['tv_exchange'], interval=Interval.in_15_minute, n_bars=250)
        if df_tv is not None and not df_tv.empty: df = df_tv.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
    except Exception: pass 

    if (df is None or df.empty) and item['tv_exchange'] == 'NSE':
        try:
            df_yf = yf.Ticker(item['yf_symbol']).history(interval="15m", period="5d")
            if df_yf is not None and not df_yf.empty:
                df_yf.index = df_yf.index.tz_localize(None) if df_yf.index.tz is not None else df_yf.index
                df = df_yf
        except Exception: pass

    if df is not None and not df.empty:
        try:
            df = df.copy() 
            if df.index.tz is not None: df.index = df.index.tz_convert('Asia/Kolkata').tz_localize(None)
            else: df.index = df.index + timedelta(hours=5, minutes=30)
            
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
                
            df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].ffill()
            df.dropna(subset=['Close', 'High', 'Low'], inplace=True) 
            
            df['EMA5'] = ta.ema(df['Close'], length=5)
            df['EMA39'] = ta.ema(df['Close'], length=39)
            df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
            df_1h = df.resample('1h').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
            df['EMA39_1H'] = ta.ema(df_1h['Close'], length=39).reindex(df.index, method='ffill')
            
            adx_data = ta.adx(df['High'], df['Low'], df['Close'], length=14)
            df['ADX'] = adx_data.iloc[:, 0].ffill().fillna(0.0) if adx_data is not None and not adx_data.empty else 0.0
            df['Vol_Ratio'] = np.where(df.get('Volume', pd.Series(dtype=float)).rolling(20).mean() > 0, df['Volume'] / df['Volume'].rolling(20).mean(), 1.0) if 'Volume' in df.columns else 1.0 
            
            df.dropna(subset=['EMA39_1H', 'EMA39', 'EMA5', 'ATR'], inplace=True)
            if len(df) >= 5: return df
        except Exception: pass
    return None

@st.cache_data(ttl=300)
def get_cached_chart_data(item_dict):
    return fetch_and_analyze(item_dict)

# ==========================================
# 3. THE EXECUTION ENGINE
# ==========================================
def process_market_data():
    try:
        with contextlib.closing(get_db_connection()) as conn:
            c = conn.cursor()
            ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
            scan_time_str = ist_now.replace(minute=(ist_now.minute // 5) * 5, second=0, microsecond=0).strftime("%Y-%m-%d %I:%M %p (IST)")
            current_date_str = ist_now.strftime("%Y-%m-%d")
            
            c.execute("SELECT value FROM system_status WHERE key='last_backup_date'")
            last_backup_row = c.fetchone()
            last_backup_date = last_backup_row[0] if last_backup_row else ""
            
            if current_date_str != last_backup_date and ist_now.hour >= 23 and ist_now.minute >= 30:
                send_telegram_csv_backup()
                c.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES ('last_backup_date', ?)", (current_date_str,))
                conn.commit()

            for item in WATCHLIST:
                name, exchange = item['name'], item['tv_exchange']
                
                market_open = True
                if exchange == 'NSE':
                    if ist_now.weekday() >= 5 or not (555 <= (ist_now.hour * 60 + ist_now.minute) <= 935): market_open = False
                elif exchange in ['OANDA', 'TVC']:
                    if ist_now.weekday() == 5 or (ist_now.weekday() == 6 and ist_now.hour < 3): market_open = False 

                df = fetch_and_analyze(item)
                if df is None: continue
                
                try:
                    last_close_dt = df.index[-1]
                    if (ist_now.replace(tzinfo=None) - last_close_dt).total_seconds() > 3600: market_open = False
                except: pass

                curr, last, prev = df.iloc[-1], df.iloc[-2], df.iloc[-3]
                
                c.execute("INSERT OR REPLACE INTO live_market_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (name, scan_time_str, round(curr['Close'], 2), round(curr['EMA5'], 2), round(curr['EMA39'], 2), 
                           "🟢 Bullish" if curr['EMA5'] > curr['EMA39'] else "🔴 Bearish", abs(curr['EMA5']-curr['EMA39'])/curr['EMA39']*100, 
                           "🟢 Bullish" if curr['Close'] > curr['EMA39_1H'] else "🔴 Bearish", round(curr.get('Vol_Ratio', 1.0), 2), round(curr.get('ADX', 0.0), 2)))
                conn.commit()
                
                if not market_open: continue

                c.execute("SELECT id, signal_type, sl, tp, entry_price, atr FROM trades WHERE ticker=? AND status='OPEN'", (name,))
                for trade in c.fetchall():
                    t_id, s_type, sl, tp, e_price, atr_val = trade
                    atr_val = atr_val if atr_val > 0 else abs(tp - e_price)/3.75
                    
                    c_open, c_high, c_low, c_close = curr['Open'], curr['High'], curr['Low'], curr['Close']
                    trade_closed = False
                    
                    if s_type == 'long':
                        if c_open >= tp: c.execute("UPDATE trades SET status='TP HIT (GAP UP)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, c_open, t_id)); trade_closed = True
                        elif c_open <= sl: c.execute("UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?", ('BREAK-EVEN (GAP DOWN)' if sl==e_price else 'SL HIT (GAP)', scan_time_str, c_open, t_id)); trade_closed = True
                        elif c_high >= tp: c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, tp, t_id)); trade_closed = True
                        elif c_low <= sl: c.execute("UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?", ('BREAK-EVEN TP HIT' if sl>e_price else 'SL HIT', scan_time_str, sl, t_id)); trade_closed = True
                        
                        if not trade_closed and round(sl, 2) <= round(e_price, 2) and max(c_high, c_close) >= (e_price + 1.0 * atr_val):
                            c.execute("UPDATE trades SET sl=? WHERE id=?", (e_price + 0.25 * atr_val, t_id))
                            send_telegram_alert(f"🛡️ <b>PROFIT LOCKED</b>\n{name} LONG hit 1 ATR. SL moved to +0.25 ATR.")

                    elif s_type == 'short':
                        if c_open <= tp: c.execute("UPDATE trades SET status='TP HIT (GAP DOWN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, c_open, t_id)); trade_closed = True
                        elif c_open >= sl: c.execute("UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?", ('BREAK-EVEN (GAP UP)' if sl==e_price else 'SL HIT (GAP)', scan_time_str, c_open, t_id)); trade_closed = True
                        elif c_low <= tp: c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, tp, t_id)); trade_closed = True
                        elif c_high >= sl: c.execute("UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?", ('BREAK-EVEN TP HIT' if sl<e_price else 'SL HIT', scan_time_str, sl, t_id)); trade_closed = True
                        
                        if not trade_closed and round(sl, 2) >= round(e_price, 2) and min(c_low, c_close) <= (e_price - 1.0 * atr_val):
                            c.execute("UPDATE trades SET sl=? WHERE id=?", (e_price - 0.25 * atr_val, t_id))
                            send_telegram_alert(f"🛡️ <b>PROFIT LOCKED</b>\n{name} SHORT hit 1 ATR. SL moved to +0.25 ATR.")
                conn.commit()

                signal_id = str(last.name) 
                c.execute("SELECT value FROM system_status WHERE key=?", (f"proc_{name}",))
                proc_row = c.fetchone()
                
                if not proc_row or proc_row[0] != signal_id:
                    c.execute("SELECT value FROM system_status WHERE key=?", (f"anchor_{name}",))
                    anchor_row = c.fetchone()
                    
                    is_long = (prev['EMA5'] <= prev['EMA39']) and (last['EMA5'] > last['EMA39'])
                    is_short = (prev['EMA5'] >= prev['EMA39']) and (last['EMA5'] < last['EMA39'])
                    
                    if (is_long or is_short) and not anchor_row:
                        direction = "LONG" if is_long else "SHORT"
                        start_time = ist_now.replace(minute=(ist_now.minute // 15) * 15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
                        anchor_val = f"{start_time}|{direction}|{last['ATR']}|{signal_id}"
                        c.execute("INSERT INTO system_status (key, value) VALUES (?, ?)", (f"anchor_{name}", anchor_val))
                        conn.commit()
                        anchor_row = (anchor_val,)

                    if anchor_row:
                        adata = anchor_row[0].split('|')
                        if len(adata) == 4 and adata[3] == signal_id:
                            a_start, a_dir, a_atr = datetime.strptime(adata[0], "%Y-%m-%d %H:%M:%S"), adata[1], float(adata[2])
                            mins_elapsed = (ist_now.replace(tzinfo=None) - a_start).total_seconds() / 60.0
                            
                            if mins_elapsed <= 16.0:
                                ev_candle = last if mins_elapsed >= 14.0 else curr
                                p_adx = prev.get('ADX', 0.0) if mins_elapsed >= 14.0 else last.get('ADX', 0.0)
                                
                                l_adx, l_vol = ev_candle.get('ADX', 0.0), ev_candle.get('Vol_Ratio', 1.0)
                                l_htf = "🟢 Bullish" if ev_candle['Close'] > ev_candle['EMA39_1H'] else "🔴 Bearish"
                                r_htf = "🟢 Bullish" if a_dir == "LONG" else "🔴 Bearish"
                                
                                rejections = []
                                if len(open_trades) > 0: rejections.append("Active trade open.")
                                if not (l_adx > 20.0 and l_adx >= p_adx): rejections.append(f"Weak Trend (ADX: {round(l_adx,1)}).")
                                if abs(ev_candle['EMA5'] - ev_candle['EMA39']) < (0.15 * a_atr): rejections.append("EMAs Tangled.")
                                if l_htf != r_htf: rejections.append(f"1H Conflict ({l_htf}).")
                                if abs(ev_candle['Close'] - ev_candle['EMA39']) > (2.5 * a_atr): rejections.append("Overextended.")
                                
                                if not rejections:
                                    entry = ev_candle['Close']
                                    sl = entry - (1.5 * a_atr) if a_dir == "LONG" else entry + (1.5 * a_atr)
                                    tp = entry + (3.75 * a_atr) if a_dir == "LONG" else entry - (3.75 * a_atr)
                                    c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status, htf_trend, vol_ratio, atr, adx) VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?)", (name, a_dir.lower(), scan_time_str, round(entry, 2), round(sl, 2), round(tp, 2), l_htf, round(l_vol, 2), round(a_atr, 2), round(l_adx, 2)))
                                    send_telegram_alert(f"{'🟢' if a_dir=='LONG' else '🔴'} <b>{a_dir} SIGNAL: {name}</b>\nTime: {scan_time_str}\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}\nADX: {round(l_adx, 1)}")
                                    
                                    c.execute("INSERT OR REPLACE INTO system_status VALUES (?, ?)", (f"proc_{name}", signal_id))
                                    c.execute("DELETE FROM system_status WHERE key=?", (f"anchor_{name}",))
                                else:
                                    safe_rej = [r.replace("<", "&lt;").replace(">", "&gt;") for r in rejections]
                                    c.execute("INSERT INTO blocked_signals (ticker, signal_type, timestamp, price, adx, htf_trend, vol_ratio, rejection_reasons) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (name, a_dir, scan_time_str, round(ev_candle['Close'], 2), round(l_adx, 2), l_htf, round(l_vol, 2), " | ".join(safe_rej)))
                                    
                                    if mins_elapsed >= 14.0:
                                        send_telegram_alert(f"💀 <b>EXPIRED: {name}</b>\n{a_dir} failed to align within 15m.\n" + "\n".join([f"❌ {r}" for r in safe_rej]))
                                        c.execute("INSERT OR REPLACE INTO system_status VALUES (?, ?)", (f"proc_{name}", signal_id))
                                        c.execute("DELETE FROM system_status WHERE key=?", (f"anchor_{name}",))
                            else:
                                c.execute("INSERT OR REPLACE INTO system_status VALUES (?, ?)", (f"proc_{name}", signal_id))
                                c.execute("DELETE FROM system_status WHERE key=?", (f"anchor_{name}",))
                    conn.commit()
                time.sleep(1) 
                
            c.execute("DELETE FROM system_logs WHERE id NOT IN (SELECT id FROM system_logs ORDER BY id DESC LIMIT 500)")
            c.execute("DELETE FROM blocked_signals WHERE id NOT IN (SELECT id FROM blocked_signals ORDER BY id DESC LIMIT 300)")
            c.execute("INSERT OR REPLACE INTO system_status VALUES ('last_scan', ?)", (scan_time_str,))
            conn.commit()
    finally: gc.collect() 
    return True

# ==========================================
# 4. BACKGROUND THREADING DAEMON
# ==========================================
def get_sleep_time_to_next_5m():
    now = datetime.now()
    next_minute = ((now.minute // 5) + 1) * 5
    if next_minute >= 60: next_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else: next_time = now.replace(minute=next_minute, second=0, microsecond=0)
    seconds = (next_time - now).total_seconds()
    return seconds if seconds > 0 else 300

@st.cache_resource
def start_background_scanner():
    def background_loop():
        while True:
            time.sleep(get_sleep_time_to_next_5m())
            try: process_market_data()
            except Exception: pass
    threading.Thread(target=background_loop, daemon=True).start()
    return True

engine_running = start_background_scanner()

# ==========================================
# 5. STREAMLIT DASHBOARD UI
# ==========================================
with contextlib.closing(get_db_connection()) as ui_conn:
    ui_c = ui_conn.cursor()

    st.sidebar.markdown("<h3>⚙️ Control Panel</h3>", unsafe_allow_html=True)
    if engine_running: st.sidebar.success("✅ Background Daemon is LIVE")

    try:
        ui_c.execute("SELECT value FROM system_status WHERE key='last_scan'")
        last_scan_row = ui_c.fetchone()
        st.sidebar.info(f"⏱️ **Last Database Sync:**\n{last_scan_row[0] if last_scan_row else 'Initializing...'}")
    except: pass

    if st.sidebar.button("🔄 Force Manual Data Sync"):
        with st.spinner("Executing Data Sync..."):
            process_market_data()
            st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("<h3>🛡️ Backup & Restore</h3>", unsafe_allow_html=True)
    colA, colB = st.sidebar.columns(2)
    try:
        with colA: st.download_button("⬇️ Trades", pd.read_sql_query("SELECT * FROM trades", ui_conn).to_csv(index=False).encode('utf-8'), f"Trades_{datetime.now().strftime('%Y-%m-%d')}.csv", "text/csv")
        with colB: st.download_button("⬇️ Blocked", pd.read_sql_query("SELECT * FROM blocked_signals", ui_conn).to_csv(index=False).encode('utf-8'), f"Blocked_{datetime.now().strftime('%Y-%m-%d')}.csv", "text/csv")
    except: pass

    st.sidebar.markdown("---")
    if st.sidebar.button("🔔 Send Test Telegram Alert"):
        if not TELEGRAM_TOKEN: st.sidebar.error("❌ Telegram Secrets Missing!")
        else:
            with st.spinner("Pinging Telegram..."):
                if send_telegram_alert("🧪 <b>DIAGNOSTIC PING</b>\nTesting HTML: ADX (&lt; 20)", test_mode=True): st.sidebar.success("Ping fired successfully!")
                else: st.sidebar.error("❌ Telegram API Failed. Check Token/ID.")

    # Matrix Calculations
    INITIAL_CAPITAL, TRADE_ALLOCATION = 200000.00, 10000.00
    try:
        live_df = pd.read_sql_query("SELECT ticker as Asset, close_price as 'Latest Price', distance_pct as '% Gap', trend as '15m Trend', htf_trend as '1H Trend', vol_ratio as 'Vol (x)', adx as 'ADX', last_update as 'Time (IST)' FROM live_market_data ORDER BY distance_pct ASC", ui_conn)
        history_df = pd.read_sql_query("SELECT * FROM trades WHERE status!='OPEN' ORDER BY id DESC LIMIT 100", ui_conn)
        open_df_ui = pd.read_sql_query("SELECT * FROM trades WHERE status='OPEN' ORDER BY id DESC", ui_conn)
    except: live_df, history_df, open_df_ui = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    realized_pnl = 0.0
    if not history_df.empty:
        history_df['Yield'] = np.where(history_df['signal_type'].str.lower() == 'long', (history_df['exit_price'] - history_df['entry_price']) / history_df['entry_price'], (history_df['entry_price'] - history_df['exit_price']) / history_df['entry_price'])
        history_df['PnL (₹)'] = history_df['Yield'] * TRADE_ALLOCATION
        history_df.loc[history_df['status'].str.contains('BREAK-EVEN') & ~history_df['status'].str.contains('TP HIT'), 'PnL (₹)'] = 0.0
        realized_pnl = history_df['PnL (₹)'].sum()

    total_unrealized_pnl = 0.0
    if not open_df_ui.empty and not live_df.empty:
        open_df_ui = pd.merge(open_df_ui, live_df[['Asset', 'Latest Price']], left_on='ticker', right_on='Asset', how='left')
        open_df_ui['Yield'] = np.where(open_df_ui['signal_type'].str.lower() == 'long', (open_df_ui['Latest Price'] - open_df_ui['entry_price']) / open_df_ui['entry_price'], (open_df_ui['entry_price'] - open_df_ui['Latest Price']) / open_df_ui['entry_price'])
        open_df_ui['Unrealized PnL (₹)'] = (open_df_ui['Yield'] * TRADE_ALLOCATION).round(2)
        total_unrealized_pnl = open_df_ui['Unrealized PnL (₹)'].sum()
        open_df_ui['Risk Status'] = np.where(((open_df_ui['signal_type'].str.lower() == 'long') & (open_df_ui['sl'] >= open_df_ui['entry_price'])) | ((open_df_ui['signal_type'].str.lower() == 'short') & (open_df_ui['sl'] <= open_df_ui['entry_price'])), '🛡️ RISK-FREE', '⚠️ AT RISK')

    st.markdown("<h1 style='background: -webkit-linear-gradient(45deg, #ffd700, #ffaa00); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>⚡ Algo Engine by Vinayak</h1>", unsafe_allow_html=True)
    
    t_closed = len(history_df)
    w_count = len(history_df[history_df['status'].str.contains('TP|WIN', na=False)]) if t_closed else 0
    b_count = len(history_df[history_df['status'].str.contains('BREAK-EVEN', na=False) & ~history_df['status'].str.contains('TP|WIN', na=False)]) if t_closed else 0
    l_count = len(history_df[history_df['status'].str.contains('LOSS|SL HIT', na=False)]) if t_closed else 0
    r_free = len(open_df_ui[open_df_ui['Risk Status'] == '🛡️ RISK-FREE']) if not open_df_ui.empty else 0

    st.markdown(f"""
    <table class="metrics-matrix">
        <tr><th></th><th>Trades</th><th>Win</th><th>Break Even</th><th>Loss</th></tr>
        <tr><td class="row-title">CLOSED TRADES</td><td class="val">{t_closed}</td><td class="val color-win">{w_count}</td><td class="val color-be">{b_count}</td><td class="val color-loss">{l_count}</td></tr>
        <tr><td class="row-title" style="border-bottom: 2px solid #2b303b;">CAPITAL & PNL</td><td class="val" style="border-bottom: 2px solid #2b303b; font-size: 1.2rem;">₹{INITIAL_CAPITAL:,.0f}</td><td colspan="3" class="pct" style="border-bottom: 2px solid #2b303b; text-align: left; padding-left: 20px;">Realized: <b class="{'color-win' if realized_pnl > 0 else 'color-loss'}">₹{realized_pnl:,.2f}</b> | Equity: <b style="color: #f0f6fc;">₹{INITIAL_CAPITAL+realized_pnl:,.2f}</b> | Unrealized: <b class="{'color-win' if total_unrealized_pnl > 0 else 'color-loss'}">₹{total_unrealized_pnl:,.2f}</b></td></tr>
        <tr><td class="row-title">OPEN TRADES</td><td class="val color-open">{len(open_df_ui)}</td><td colspan="3" class="pct color-be" style="text-align: left; padding-left: 20px;">🛡️ {r_free} Risk-Free | ⚠️ {len(open_df_ui) - r_free} At Risk</td></tr>
    </table>
    """, unsafe_allow_html=True)

    t_heat, t_chart, t_open, t_ledger, t_blocked = st.tabs(["🔥 Heatmap", "📈 Chart", "🟢 Open", "📚 Ledger", "🚫 Blocked"])

    with t_heat:
        if not live_df.empty: st.dataframe(live_df.style.map(lambda v: 'background-color: rgba(255,0,0,0.4); color: white;' if pd.notna(v) and float(v)<0.10 else ('background-color: rgba(255,165,0,0.4); color: white;' if pd.notna(v) and float(v)<0.50 else ''), subset=['% Gap']), width="stretch", height=600, hide_index=True)
        else: st.info("Waiting for data sync...")

    with t_chart:
        if not live_df.empty:
            sel_stock = st.selectbox("Select Asset:", ["-- Select --"] + sorted(live_df['Asset'].tolist()), label_visibility="collapsed")
            if sel_stock != "-- Select --":
                with st.spinner("Loading chart..."):
                    try:
                        chart_df = get_cached_chart_data(next(i for i in WATCHLIST if i['name'] == sel_stock))
                        if chart_df is not None:
                            chart_df = chart_df[chart_df.index >= (chart_df.index[-1] - timedelta(days=5))]
                            fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05, row_heights=[0.7, 0.3])
                            fig.add_trace(go.Candlestick(x=chart_df.index.strftime('%b %d %H:%M'), open=chart_df['Open'], high=chart_df['High'], low=chart_df['Low'], close=chart_df['Close'], name="Price"), row=1, col=1)
                            fig.add_trace(go.Scatter(x=chart_df.index.strftime('%b %d %H:%M'), y=chart_df['EMA5'], line=dict(color='#00ff00', width=1.5), name='EMA 5'), row=1, col=1)
                            fig.add_trace(go.Scatter(x=chart_df.index.strftime('%b %d %H:%M'), y=chart_df['EMA39'], line=dict(color='#ff0000', width=2), name='EMA 39'), row=1, col=1)
                            fig.add_trace(go.Scatter(x=chart_df.index.strftime('%b %d %H:%M'), y=chart_df['ADX'], line=dict(color='#ffd700', width=1.5), name='ADX'), row=2, col=1)
                            fig.add_hline(y=20, line_dash="dot", annotation_text="Trend (20)", row=2, col=1)
                            fig.update_layout(title=f"{sel_stock} | Live ADX: {chart_df['ADX'].iloc[-1]:.2f} | Live ATR: {chart_df['ATR'].iloc[-1]:.2f}", template="plotly_dark", xaxis_rangeslider_visible=False, height=700)
                            st.plotly_chart(fig, use_container_width=True)
                    except: st.error("Chart data unavailable.")

    with t_open:
        if not open_df_ui.empty: st.dataframe(open_df_ui[['ticker', 'signal_type', 'entry_time', 'entry_price', 'sl', 'tp', 'Latest Price', 'Risk Status', 'Unrealized PnL (₹)']], width="stretch", height=600, hide_index=True)
        else: st.info("No active trades.")

    with t_ledger:
        if not history_df.empty: st.dataframe(history_df[['ticker', 'signal_type', 'entry_time', 'entry_price', 'sl', 'tp', 'exit_time', 'exit_price', 'status', 'PnL (₹)']], width="stretch", height=600, hide_index=True)
        else: st.info("No closed trades.")

    with t_blocked:
        try:
            b_df = pd.read_sql_query("SELECT ticker, signal_type, timestamp, price, rejection_reasons FROM blocked_signals ORDER BY id DESC LIMIT 100", ui_conn)
            if not b_df.empty: st.dataframe(b_df, width="stretch", height=600, hide_index=True)
            else: st.info("No blocked signals.")
        except: pass
