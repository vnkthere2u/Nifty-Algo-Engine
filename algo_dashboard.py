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
import traceback
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, timezone
from tvDatafeed import TvDatafeed, Interval

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
        .stDataFrame { margin-top: -15px; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 1. TELEGRAM ALERT & BACKUP SETUP
# ==========================================
try:
    TELEGRAM_TOKEN = st.secrets["TELEGRAM_TOKEN"]
    TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
except:
    TELEGRAM_TOKEN = ""
    TELEGRAM_CHAT_ID = ""

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN: return 
    safe_message = message.replace("&", "&amp;")
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': safe_message, 'parse_mode': 'HTML'}
    for attempt in range(3):
        try: 
            response = requests.post(url, data=payload, timeout=10)
            if response.status_code == 200: break
            elif response.status_code == 429: time.sleep(3); continue
            else:
                conn = get_db_connection()
                c = conn.cursor()
                ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
                err_msg = f"Telegram API Error {response.status_code}: {response.text}"
                c.execute("INSERT INTO system_logs (timestamp, message) VALUES (?, ?)", (ist_now.strftime("%Y-%m-%d %I:%M %p"), err_msg))
                conn.commit()
                conn.close()
                break 
        except Exception as e: 
            try:
                conn = get_db_connection()
                c = conn.cursor()
                ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
                c.execute("INSERT INTO system_logs (timestamp, message) VALUES (?, ?)", (ist_now.strftime("%Y-%m-%d %I:%M %p"), f"Telegram Crash: {str(e)}"))
                conn.commit()
                conn.close()
            except: pass
            time.sleep(1)

def send_telegram_csv_backup():
    if not TELEGRAM_TOKEN: return
    try:
        conn = get_db_connection()
        ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        date_str = ist_now.strftime('%Y-%m-%d')
        
        df_trades = pd.read_sql_query("SELECT * FROM trades", conn)
        trades_filename = f"Trades_Backup_{date_str}.csv"
        df_trades.to_csv(trades_filename, index=False)
        
        df_blocked = pd.read_sql_query("SELECT * FROM blocked_signals", conn)
        blocked_filename = f"Blocked_Backup_{date_str}.csv"
        df_blocked.to_csv(blocked_filename, index=False)
        conn.close()

        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
        
        payload_trades = {'chat_id': TELEGRAM_CHAT_ID, 'caption': f"📊 <b>Automated Daily Backup: Trades</b>\nDate: {date_str}", 'parse_mode': 'HTML'}
        with open(trades_filename, 'rb') as f:
            requests.post(url, data=payload_trades, files={'document': f}, timeout=15)
        os.remove(trades_filename)
        
        time.sleep(2)
        
        payload_blocked = {'chat_id': TELEGRAM_CHAT_ID, 'caption': f"🚫 <b>Automated Daily Backup: Blocked Signals</b>\nDate: {date_str}", 'parse_mode': 'HTML'}
        with open(blocked_filename, 'rb') as f:
            requests.post(url, data=payload_blocked, files={'document': f}, timeout=15)
        os.remove(blocked_filename)
    except Exception as e: pass

# ==========================================
# 2. DATABASE SETUP
# ==========================================
def get_db_connection():
    conn = sqlite3.connect('nifty_live_trades.db', check_same_thread=False, timeout=30.0)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal_type TEXT, 
                 entry_time TEXT, entry_price REAL, sl REAL, tp REAL, status TEXT, 
                 exit_time TEXT, exit_price REAL)''')
    try:
        c.execute("ALTER TABLE trades ADD COLUMN htf_trend TEXT")
        c.execute("ALTER TABLE trades ADD COLUMN vol_ratio REAL")
    except sqlite3.OperationalError: pass 
    try: c.execute("ALTER TABLE trades ADD COLUMN atr REAL")
    except sqlite3.OperationalError: pass 
    try: c.execute("ALTER TABLE trades ADD COLUMN adx REAL")
    except sqlite3.OperationalError: pass 
    conn.commit()

    c.execute('''CREATE TABLE IF NOT EXISTS system_status (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS system_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, message TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS live_market_data 
                 (ticker TEXT PRIMARY KEY, last_update TEXT, close_price REAL, ema5 REAL, ema39 REAL, trend TEXT, distance_pct REAL, htf_trend TEXT, vol_ratio REAL)''')
    try:
        c.execute("ALTER TABLE live_market_data ADD COLUMN adx REAL")
        conn.commit()
    except sqlite3.OperationalError: pass

    c.execute('''CREATE TABLE IF NOT EXISTS blocked_signals 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal_type TEXT, 
                 timestamp TEXT, price REAL, adx REAL, htf_trend TEXT, vol_ratio REAL, rejection_reasons TEXT)''')
    conn.commit()
    return conn

# ==========================================
# 3. DUAL-ENGINE LOGIC & ADVANCED MATH
# ==========================================
WATCHLIST = [
    {'name': 'NIFTY 50', 'tv_symbol': 'NIFTY', 'tv_exchange': 'NSE', 'yf_symbol': '^NSEI'},
    {'name': 'BANK NIFTY', 'tv_symbol': 'BANKNIFTY', 'tv_exchange': 'NSE', 'yf_symbol': '^NSEBANK'},
    {'name': 'BITCOIN (24/7)', 'tv_symbol': 'BTCUSDT', 'tv_exchange': 'BINANCE', 'yf_symbol': 'BTC-USD'},
    {'name': 'GOLD', 'tv_symbol': 'XAUUSD', 'tv_exchange': 'OANDA', 'yf_symbol': 'GC=F'},
    {'name': 'SILVER', 'tv_symbol': 'XAGUSD', 'tv_exchange': 'OANDA', 'yf_symbol': 'SI=F'},
    {'name': 'CRUDE OIL', 'tv_symbol': 'USOIL', 'tv_exchange': 'OANDA', 'yf_symbol': 'CL=F'},
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

tv = TvDatafeed()

def fetch_and_analyze(item):
    global tv
    df = None
    try:
        df_tv = tv.get_hist(symbol=item['tv_symbol'], exchange=item['tv_exchange'], interval=Interval.in_15_minute, n_bars=250)
        if df_tv is not None and not df_tv.empty:
            df_tv = df_tv.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
            df = df_tv
    except Exception:
        pass

    if df is None or df.empty:
        try:
            df_yf = yf.Ticker(item['yf_symbol']).history(interval="15m", period="20d")
            if df_yf is not None and not df_yf.empty:
                df_yf.index = df_yf.index.tz_localize(None)
                df = df_yf
        except Exception: pass

    if df is not None and not df.empty:
        try:
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
                
            df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].ffill()
            df.dropna(subset=['Close', 'High', 'Low'], inplace=True) 
            
            df['EMA5'] = ta.ema(df['Close'], length=5)
            df['EMA39'] = ta.ema(df['Close'], length=39)
            df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
            
            df_1h = df.resample('1h').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
            df_1h['EMA39_1H'] = ta.ema(df_1h['Close'], length=39)
            df_1h_aligned = df_1h[['EMA39_1H']].reindex(df.index, method='ffill')
            df['EMA39_1H'] = df_1h_aligned['EMA39_1H']
            
            adx_data = ta.adx(df['High'], df['Low'], df['Close'], length=14)
            if adx_data is not None and not adx_data.empty: df['ADX'] = adx_data.iloc[:, 0].ffill().fillna(0.0)
            else: df['ADX'] = 0.0
            
            if 'Volume' in df.columns:
                df['Volume'] = df['Volume'].fillna(0)
                df['Vol_MA20'] = df['Volume'].rolling(20).mean()
                df['Vol_Ratio'] = np.where(df['Vol_MA20'] > 0, df['Volume'] / df['Vol_MA20'], 1.0)
            else: df['Vol_Ratio'] = 1.0 
                
            df.dropna(subset=['EMA39_1H', 'EMA39', 'EMA5', 'ATR'], inplace=True)
            if len(df) >= 5: return df
        except Exception as e: pass
    return None

def process_market_data():
    conn = get_db_connection()
    c = conn.cursor()
    alerts = []
    ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    current_date_str = ist_now.strftime("%Y-%m-%d")
    scan_time_str = ist_now.strftime("%Y-%m-%d %I:%M %p (IST)")
    
    c.execute("SELECT value FROM system_status WHERE key='last_backup_date'")
    last_backup_row = c.fetchone()
    last_backup_date = last_backup_row[0] if last_backup_row else ""
    
    if current_date_str != last_backup_date and ist_now.hour >= 23 and ist_now.minute >= 30:
        send_telegram_csv_backup()
        c.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES ('last_backup_date', ?)", (current_date_str,))
        conn.commit()

    for item in WATCHLIST:
        name = item['name']
        market_open = True
        if item['tv_exchange'] == 'NSE':
            if ist_now.weekday() >= 5: market_open = False
            else:
                minutes_since_midnight = ist_now.hour * 60 + ist_now.minute
                if minutes_since_midnight < 555 or minutes_since_midnight > 935: market_open = False
                
        df = fetch_and_analyze(item)
        if df is None: continue
            
        c.execute("SELECT id, signal_type, sl, tp, entry_price, entry_time, atr FROM trades WHERE ticker=? AND status='OPEN'", (name,))
        open_trades = c.fetchall()
        
        current_candle = df.iloc[-1]
        last_closed = df.iloc[-2]
        prev_closed = df.iloc[-3]
        
        trend = "🟢 Bullish" if current_candle['EMA5'] > current_candle['EMA39'] else "🔴 Bearish"
        htf_trend = "🟢 Bullish" if current_candle['Close'] > current_candle['EMA39_1H'] else "🔴 Bearish"
        vol_ratio = current_candle['Vol_Ratio']
        adx_val = current_candle['ADX']
        latest_price = current_candle['Close']
        ema5_live, ema39_live = current_candle['EMA5'], current_candle['EMA39']
        dist_pct = abs(ema5_live - ema39_live) / ema39_live * 100
        
        c.execute("INSERT OR REPLACE INTO live_market_data (ticker, last_update, close_price, ema5, ema39, trend, distance_pct, htf_trend, vol_ratio, adx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (name, scan_time_str, round(latest_price, 2), round(ema5_live, 2), round(ema39_live, 2), trend, round(dist_pct, 4), htf_trend, round(vol_ratio, 2), round(adx_val, 2)))
        conn.commit()
        
        if market_open:
            for trade in open_trades:
                trade_id, sig_type, sl, tp, entry_price, entry_time_str, db_atr = trade
                atr_val = db_atr if (db_atr and db_atr > 0) else abs(tp - entry_price) / 3.75
                
                try:
                    clean_time_str = entry_time_str.replace(" (IST)", "")
                    entry_dt_ist = pd.to_datetime(clean_time_str, format="%Y-%m-%d %I:%M %p")
                    temp_idx = df.index.tz_localize(None) if df.index.tz is not None else df.index
                    trade_history = df[temp_idx >= entry_dt_ist]
                    
                    if not trade_history.empty:
                        max_high_reached = trade_history['High'].max()
                        min_low_reached = trade_history['Low'].min()
                    else:
                        max_high_reached = current_candle['High']
                        min_low_reached = current_candle['Low']
                except Exception:
                    max_high_reached = current_candle['High']
                    min_low_reached = current_candle['Low']

                current_open, current_high, current_low = current_candle['Open'], current_candle['High'], current_candle['Low']
                sl_before_update = sl 
                
                if sig_type == 'long':
                    if round(sl, 2) <= round(entry_price, 2):
                        if max_high_reached >= (entry_price + (1.0 * atr_val)):
                            new_sl = entry_price + (0.25 * atr_val)
                            c.execute("UPDATE trades SET sl=? WHERE id=?", (new_sl, trade_id))
                            sl = new_sl
                            send_telegram_alert(f"🛡️ <b>PROFIT LOCKED</b>\n{name} LONG hit 1 ATR. SL moved to lock 0.25 ATR profit ({round(new_sl, 2)}).")
                    
                    if current_open >= tp:
                        c.execute("UPDATE trades SET status='TP HIT (GAP UP)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, current_open, trade_id))
                        send_telegram_alert(f"🎯 <b>GAP UP TARGET HIT</b>\n{name} LONG closed at {round(current_open, 2)}")
                    elif current_open <= sl_before_update:
                        status_text = 'BREAK-EVEN TP HIT (GAP)' if sl_before_update > entry_price else ('BREAK-EVEN (GAP DOWN)' if sl_before_update == entry_price else 'SL HIT (GAP DOWN)')
                        c.execute("UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?", (status_text, scan_time_str, current_open, trade_id))
                        send_telegram_alert(f"🛑 <b>{status_text}</b>\n{name} LONG closed at {round(current_open, 2)}")
                    elif current_high >= tp:
                        c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, tp, trade_id))
                        send_telegram_alert(f"🎯 <b>TARGET HIT</b>\n{name} LONG closed at {round(tp, 2)}")
                    elif current_low <= sl:
                        status_text = 'BREAK-EVEN TP HIT' if sl > entry_price else ('BREAK-EVEN (0 RISK)' if sl == entry_price else 'SL HIT (LOSS)')
                        c.execute("UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?", (status_text, scan_time_str, sl, trade_id))
                        send_telegram_alert(f"🛑 <b>{status_text}</b>\n{name} LONG closed at {round(sl, 2)}")
                        
                elif sig_type == 'short':
                    if round(sl, 2) >= round(entry_price, 2):
                        if min_low_reached <= (entry_price - (1.0 * atr_val)):
                            new_sl = entry_price - (0.25 * atr_val)
                            c.execute("UPDATE trades SET sl=? WHERE id=?", (new_sl, trade_id))
                            sl = new_sl
                            send_telegram_alert(f"🛡️ <b>PROFIT LOCKED</b>\n{name} SHORT hit 1 ATR. SL moved to lock 0.25 ATR profit ({round(new_sl, 2)}).")

                    if current_open <= tp:
                        c.execute("UPDATE trades SET status='TP HIT (GAP DOWN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, current_open, trade_id))
                        send_telegram_alert(f"🎯 <b>GAP DOWN TARGET HIT</b>\n{name} SHORT closed at {round(current_open, 2)}")
                    elif current_open >= sl_before_update:
                        status_text = 'BREAK-EVEN TP HIT (GAP)' if sl_before_update < entry_price else ('BREAK-EVEN (GAP UP)' if sl_before_update == entry_price else 'SL HIT (GAP UP)')
                        c.execute("UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?", (status_text, scan_time_str, current_open, trade_id))
                        send_telegram_alert(f"🛑 <b>{status_text}</b>\n{name} SHORT closed at {round(current_open, 2)}")
                    elif current_low <= tp:
                        c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, tp, trade_id))
                        send_telegram_alert(f"🎯 <b>TARGET HIT</b>\n{name} SHORT closed at {round(tp, 2)}")
                    elif current_high >= sl:
                        status_text = 'BREAK-EVEN TP HIT' if sl < entry_price else ('BREAK-EVEN (0 RISK)' if sl == entry_price else 'SL HIT (LOSS)')
                        c.execute("UPDATE trades SET status=?, exit_time=?, exit_price=? WHERE id=?", (status_text, scan_time_str, sl, trade_id))
                        send_telegram_alert(f"🛑 <b>{status_text}</b>\n{name} SHORT closed at {round(sl, 2)}")
            conn.commit()

            # 2. STATE MACHINE: DETECT FRESH CROSSOVERS & DROP ANCHORS
            long_cross = (prev_closed['EMA5'] <= prev_closed['EMA39']) and (last_closed['EMA5'] > last_closed['EMA39'])
            short_cross = (prev_closed['EMA5'] >= prev_closed['EMA39']) and (last_closed['EMA5'] < last_closed['EMA39'])
            
            try: slot_id = pd.to_datetime(current_candle.name).strftime("%Y-%m-%d %H:%M:%S")
            except: slot_id = scan_time_str 
            
            c.execute("SELECT value FROM system_status WHERE key=?", (f"last_signal_{name}",))
            last_processed = c.fetchone()
            already_entered = last_processed and last_processed[0] == slot_id
            
            if not already_entered:
                if long_cross or short_cross:
                    direction = "LONG" if long_cross else "SHORT"
                    atr_val = last_closed['ATR']
                    
                    # THE FIX: TRUE MARKET ANCHORING
                    # We mathematically lock the start time to the EXACT close of the crossover candle, ignoring server delays.
                    try:
                        last_closed_dt = pd.to_datetime(last_closed.name)
                        if last_closed_dt.tz is not None: 
                            last_closed_dt = last_closed_dt.tz_localize(None)
                        true_anchor_time = last_closed_dt + timedelta(minutes=15)
                        atomic_start = true_anchor_time.strftime("%Y-%m-%d %H:%M:%S")
                    except:
                        atomic_start = ist_now.strftime("%Y-%m-%d %H:%M:%S")

                    anchor_val = f"{atomic_start}|{direction}|{atr_val}|{slot_id}"
                    c.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES (?, ?)", (f"anchor_{name}", anchor_val))
                    conn.commit()

            # 3. STATE MACHINE: EVALUATE ACTIVE ANCHORS
            c.execute("SELECT value FROM system_status WHERE key=?", (f"anchor_{name}",))
            anchor_row = c.fetchone()
            
            if anchor_row:
                anchor_data = anchor_row[0].split('|')
                atomic_start_str = anchor_data[0]
                anchor_direction = anchor_data[1]
                anchor_atr = float(anchor_data[2])
                slot_id = anchor_data[3]
                
                try: anchor_dt = datetime.strptime(atomic_start_str, "%Y-%m-%d %H:%M:%S")
                except: anchor_dt = ist_now.replace(tzinfo=None) - timedelta(minutes=20) 
                
                atomic_now = ist_now.replace(tzinfo=None)
                time_diff = (atomic_now - anchor_dt).total_seconds() / 60.0
                
                if time_diff <= 16.0: 
                    eval_candle = last_closed if time_diff >= 14.0 else current_candle
                    
                    live_adx = eval_candle.get('ADX', 0.0)
                    live_vol = eval_candle.get('Vol_Ratio', 1.0)
                    live_htf_trend = "🟢 Bullish" if eval_candle['Close'] > eval_candle['EMA39_1H'] else "🔴 Bearish"
                    live_distance = abs(eval_candle['Close'] - eval_candle['EMA39'])
                    
                    is_trending = live_adx > 20.0
                    max_extension = 2.5 * anchor_atr
                    is_not_overextended = live_distance <= max_extension
                    
                    required_htf = "🟢 Bullish" if anchor_direction == "LONG" else "🔴 Bearish"
                    rejection_reasons = []
                    
                    if len(open_trades) > 0: rejection_reasons.append("Active trade already open.")
                    if not is_trending: rejection_reasons.append(f"ADX Below 20 ({round(live_adx, 1)}).")
                    if live_htf_trend != required_htf: rejection_reasons.append(f"1H Trend Conflict ({live_htf_trend}).")
                    if not is_not_overextended: rejection_reasons.append(f"Overextended Price Surge.")
                        
                    if len(rejection_reasons) == 0:
                        entry = eval_candle['Close']
                        if anchor_direction == "LONG":
                            sl, tp = entry - (1.5 * anchor_atr), entry + (3.75 * anchor_atr)
                            c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status, htf_trend, vol_ratio, atr, adx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                      (name, 'long', scan_time_str, round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN', live_htf_trend, round(live_vol, 2), round(anchor_atr, 2), round(live_adx, 2)))
                            msg = f"🟢 <b>LONG SIGNAL: {name}</b>\nTime: {scan_time_str}\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}\n\n<i>Context:</i>\n1H Trend: {live_htf_trend}\nVol Surge: {round(live_vol, 1)}x\nADX: {round(live_adx, 1)}\nATR: {round(anchor_atr, 2)}\nR:R Profile: 1:2.5"
                        elif anchor_direction == "SHORT":
                            sl, tp = entry + (1.5 * anchor_atr), entry - (3.75 * anchor_atr)
                            c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status, htf_trend, vol_ratio, atr, adx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                      (name, 'short', scan_time_str, round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN', live_htf_trend, round(live_vol, 2), round(anchor_atr, 2), round(live_adx, 2)))
                            msg = f"🔴 <b>SHORT SIGNAL: {name}</b>\nTime: {scan_time_str}\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}\n\n<i>Context:</i>\n1H Trend: {live_htf_trend}\nVol Surge: {round(live_vol, 1)}x\nADX: {round(live_adx, 1)}\nATR: {round(anchor_atr, 2)}\nR:R Profile: 1:2.5"
                        
                        c.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES (?, ?)", (f"last_signal_{name}", slot_id))
                        c.execute("DELETE FROM system_status WHERE key=?", (f"anchor_{name}",))
                        alerts.append(msg)
                        send_telegram_alert(msg)
                    else:
                        reason_str = " | ".join(rejection_reasons)
                        c.execute("""INSERT INTO blocked_signals (ticker, signal_type, timestamp, price, adx, htf_trend, vol_ratio, rejection_reasons) 
                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                                  (name, anchor_direction, scan_time_str, round(eval_candle['Close'], 2), round(live_adx, 2), live_htf_trend, round(live_vol, 2), reason_str))
                        
                        if time_diff >= 14.0:
                            c.execute("DELETE FROM system_status WHERE key=?", (f"anchor_{name}",))
                            send_telegram_alert(f"💀 <b>SIGNAL EXPIRED: {name}</b>\n{anchor_direction} crossover failed to align conditions within 15 minutes. Signal destroyed.")
                else:
                    c.execute("DELETE FROM system_status WHERE key=?", (f"anchor_{name}",))
            conn.commit()
        time.sleep(1) 
        
    c.execute("DELETE FROM system_logs WHERE id NOT IN (SELECT id FROM system_logs ORDER BY id DESC LIMIT 500)")
    c.execute("DELETE FROM blocked_signals WHERE id NOT IN (SELECT id FROM blocked_signals ORDER BY id DESC LIMIT 300)")
    c.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES ('last_scan', ?)", (scan_time_str,))
    conn.commit()
    conn.close()
    
    gc.collect() 
    return alerts

def get_sleep_time_to_next_5m():
    now = datetime.now()
    next_minute = ((now.minute // 5) + 1) * 5
    if next_minute >= 60:
        next_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        next_time = now.replace(minute=next_minute, second=0, microsecond=0)
    return max(0, (next_time - now).total_seconds())

# ==========================================
# 4. STREAMLIT DASHBOARD UI
# ==========================================
ui_conn = get_db_connection()
ui_c = ui_conn.cursor()

@st.cache_resource
def start_background_scanner():
    def background_loop():
        while True:
            sleep_sec = get_sleep_time_to_next_5m()
            time.sleep(sleep_sec)
            try: 
                process_market_data()
            except Exception: 
                pass
    thread = threading.Thread(target=background_loop, daemon=True)
    thread.start()
    return True

engine_running = start_background_scanner()

st.sidebar.markdown("<h3>⚙️ Control Panel</h3>", unsafe_allow_html=True)
if engine_running: st.sidebar.success("✅ Background Daemon is LIVE")

ui_c.execute("SELECT value FROM system_status WHERE key='last_scan'")
last_scan_row = ui_c.fetchone()
st.sidebar.info(f"⏱️ **Last Database Sync:**\n{last_scan_row[0] if last_scan_row else 'Initializing...'}")

if st.sidebar.button("🔄 Force Manual Data Sync"):
    with st.spinner("Executing Data Sync..."):
        process_market_data()
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("<h3>🛡️ Dual Backup & Restore</h3>", unsafe_allow_html=True)

colA, colB = st.sidebar.columns(2)
backup_trades_df = pd.read_sql_query("SELECT * FROM trades", ui_conn)
csv_trades = backup_trades_df.to_csv(index=False).encode('utf-8')
with colA: st.download_button(label="⬇️ Backup Trades", data=csv_trades, file_name=f"Trades_Backup_{datetime.now().strftime('%Y-%m-%d')}.csv", mime="text/csv")

backup_blocked_df = pd.read_sql_query("SELECT * FROM blocked_signals", ui_conn)
csv_blocked = backup_blocked_df.to_csv(index=False).encode('utf-8')
with colB: st.download_button(label="⬇️ Backup Blocked", data=csv_blocked, file_name=f"Blocked_Backup_{datetime.now().strftime('%Y-%m-%d')}.csv", mime="text/csv")

st.sidebar.markdown("<b>Restore Database (Upload CSV)</b>", unsafe_allow_html=True)
uploaded_file = st.sidebar.file_uploader("Upload CSV", type=None, label_visibility="collapsed")

if uploaded_file is not None:
    if st.sidebar.button("⚙️ Execute Auto-Restore"):
        try:
            restore_df = pd.read_csv(uploaded_file)
            csv_columns = restore_df.columns.tolist()
            
            if 'entry_time' in csv_columns or 'Entry Time' in csv_columns:
                rename_map = {'Asset': 'ticker', 'Signal': 'signal_type', 'Entry Time': 'entry_time', 'Entry': 'entry_price', 'SL': 'sl', 'TP': 'tp', 'ATR': 'atr', 'ADX': 'adx', 'Status': 'status', 'Exit Time': 'exit_time', 'Exit Price': 'exit_price', '1H Trend': 'htf_trend', 'Vol (x)': 'vol_ratio'}
                restore_df = restore_df.rename(columns=rename_map)
                restore_df = restore_df.fillna({'exit_time': '', 'exit_price': 0.0, 'htf_trend': '', 'vol_ratio': 1.0, 'atr': 0.0, 'adx': 0.0})
                for index, row in restore_df.iterrows():
                    ui_c.execute("SELECT id FROM trades WHERE ticker=? AND entry_time=?", (row['ticker'], row['entry_time']))
                    if not ui_c.fetchone():
                        ui_c.execute("""INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status, exit_time, exit_price, htf_trend, vol_ratio, atr, adx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (row['ticker'], row['signal_type'], row['entry_time'], row['entry_price'], row['sl'], row['tp'], row['status'], row['exit_time'], row['exit_price'], row['htf_trend'], row['vol_ratio'], row['atr'], row['adx']))
                ui_conn.commit()
                st.sidebar.success("✅ Trades Restored! Rebooting...")

            elif 'rejection_reasons' in csv_columns or 'Rejection Reasons' in csv_columns:
                rename_map = {'Asset': 'ticker', 'Signal': 'signal_type', 'Time (IST)': 'timestamp', 'Price': 'price', 'ADX': 'adx', '1H Trend': 'htf_trend', 'Vol (x)': 'vol_ratio', 'Rejection Reasons': 'rejection_reasons'}
                restore_df = restore_df.rename(columns=rename_map)
                restore_df = restore_df.fillna({'adx': 0.0, 'htf_trend': '', 'vol_ratio': 1.0, 'rejection_reasons': ''})
                for index, row in restore_df.iterrows():
                    ui_c.execute("SELECT id FROM blocked_signals WHERE ticker=? AND timestamp=?", (row['ticker'], row['timestamp']))
                    if not ui_c.fetchone():
                        ui_c.execute("""INSERT INTO blocked_signals (ticker, signal_type, timestamp, price, adx, htf_trend, vol_ratio, rejection_reasons) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (row['ticker'], row['signal_type'], row['timestamp'], row['price'], row['adx'], row['htf_trend'], row['vol_ratio'], row['rejection_reasons']))
                ui_conn.commit()
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
    send_telegram_alert("🧪 <b>DIAGNOSTIC PING</b>\n<i>Testing HTML Parser:</i>\nAsset: L&amp;T\nReason: ADX Below 20 (< 20)")
    st.sidebar.success("Ping fired! Check your Telegram.")


# ==========================================
# CAPITAL & PNL CALCULATIONS (NATIVE INR)
# ==========================================
INITIAL_CAPITAL = 200000.00
TRADE_ALLOCATION = 10000.00

live_df = pd.read_sql_query("SELECT ticker as Asset, close_price as 'Latest Price', distance_pct as '% Gap', trend as '15m Trend', htf_trend as '1H Trend', vol_ratio as 'Vol (x)', adx as 'ADX', last_update as 'Time (IST)' FROM live_market_data ORDER BY distance_pct ASC", ui_conn)

history_df = pd.read_sql_query("SELECT ticker as Asset, signal_type as Signal, entry_time as 'Entry Time', entry_price as 'Entry', sl as SL, tp as TP, atr as ATR, adx as ADX, status as Status, exit_time as 'Exit Time', exit_price as 'Exit Price', htf_trend as '1H Trend', vol_ratio as 'Vol (x)' FROM trades WHERE status!='OPEN' ORDER BY id DESC", ui_conn)

if not history_df.empty:
    history_df['Yield'] = np.where(history_df['Signal'].str.lower() == 'long', 
                                     (history_df['Exit Price'] - history_df['Entry']) / history_df['Entry'], 
                                     (history_df['Entry'] - history_df['Exit Price']) / history_df['Entry'])
    history_df['PnL (₹)'] = history_df['Yield'] * TRADE_ALLOCATION
    
    is_zero_be = history_df['Status'].str.contains('BREAK-EVEN', regex=True) & ~history_df['Status'].str.contains('TP HIT', regex=True)
    history_df.loc[is_zero_be, 'PnL (₹)'] = 0.0
    history_df['PnL (₹)'] = history_df['PnL (₹)'].round(2)
    realized_pnl = history_df['PnL (₹)'].sum()
else: realized_pnl = 0.0

current_equity = INITIAL_CAPITAL + realized_pnl

open_df_ui = pd.read_sql_query("SELECT ticker as Asset, signal_type as Signal, entry_time as 'Entry Time', entry_price as 'Entry', sl as SL, tp as TP, atr as ATR, adx as ADX, htf_trend as '1H Trend', vol_ratio as 'Vol (x)' FROM trades WHERE status='OPEN' ORDER BY id DESC", ui_conn)

total_unrealized_pnl = 0.0
if not open_df_ui.empty:
    open_df_ui = pd.merge(open_df_ui, live_df[['Asset', 'Latest Price']], on='Asset', how='left')
    open_df_ui['Yield'] = np.where(open_df_ui['Signal'].str.lower() == 'long',
                                   (open_df_ui['Latest Price'] - open_df_ui['Entry']) / open_df_ui['Entry'],
                                   (open_df_ui['Entry'] - open_df_ui['Latest Price']) / open_df_ui['Entry'])
    open_df_ui['Unrealized PnL (₹)'] = (open_df_ui['Yield'] * TRADE_ALLOCATION).round(2)
    total_unrealized_pnl = open_df_ui['Unrealized PnL (₹)'].sum()
    
    open_df_ui['Risk Status'] = np.where( ((open_df_ui['Signal'].str.lower() == 'long') & (open_df_ui['SL'] >= open_df_ui['Entry'])) | 
                                          ((open_df_ui['Signal'].str.lower() == 'short') & (open_df_ui['SL'] <= open_df_ui['Entry'])), 
                                          '🛡️ RISK-FREE', '⚠️ AT RISK')

# ==========================================
# UI: NEW GRID METRICS MATRIX 
# ==========================================
st.markdown("<h1 style='background: -webkit-linear-gradient(45deg, #ffd700, #ffaa00); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>⚡ Algo Engine by Vinayak</h1>", unsafe_allow_html=True)
st.markdown("<p style='color: #8b949e; font-size: 0.95rem; margin-top: -10px;'>Multi Asset Market Tracker</p>", unsafe_allow_html=True)

if not backup_trades_df.empty:
    closed_df = backup_trades_df[backup_trades_df['status'] != 'OPEN']
    open_df = backup_trades_df[backup_trades_df['status'] == 'OPEN']
    
    total_closed = len(closed_df)
    
    win_mask = closed_df['status'].str.contains('TP|WIN', regex=True, na=False)
    be_mask = closed_df['status'].str.contains('BREAK-EVEN', regex=True, na=False) & ~win_mask
    loss_mask = closed_df['status'].str.contains('LOSS|SL HIT', regex=True, na=False)
    
    win_count = len(closed_df[win_mask])
    be_count = len(closed_df[be_mask])
    loss_count = len(closed_df[loss_mask])
    
    win_pct = f"{(win_count / total_closed * 100):.1f}%" if total_closed > 0 else "0.0%"
    be_pct = f"{(be_count / total_closed * 100):.1f}%" if total_closed > 0 else "0.0%"
    loss_pct = f"{(loss_count / total_closed * 100):.1f}%" if total_closed > 0 else "0.0%"
    
    combined_win_be_pct = f"{((win_count + be_count) / total_closed * 100):.1f}%" if total_closed > 0 else "0.0%"
    
    total_open = len(open_df)
    
    risk_free_count = 0
    if not open_df_ui.empty:
        risk_free_count = len(open_df_ui[open_df_ui['Risk Status'] == '🛡️ RISK-FREE'])
else:
    total_closed, win_count, be_count, loss_count = 0, 0, 0, 0
    win_pct, be_pct, loss_pct, combined_win_be_pct = "0.0%", "0.0%", "0.0%", "0.0%"
    total_open, risk_free_count = 0, 0

st.markdown(f"""
<table class="metrics-matrix">
    <tr>
        <th></th>
        <th>Trades</th>
        <th>Win</th>
        <th>Break Even</th>
        <th>Loss</th>
    </tr>
    <tr>
        <td class="row-title">CLOSED TRADES</td>
        <td class="val">{total_closed}</td>
        <td class="val color-win">{win_count}</td>
        <td class="val color-be">{be_count}</td>
        <td class="val color-loss">{loss_count}</td>
    </tr>
    <tr>
        <td class="row-title" style="border-bottom: 2px solid #2b303b;">WIN RATE %</td>
        <td class="pct" style="border-bottom: 2px solid #2b303b; font-weight:bold; color:#f0f6fc;">{combined_win_be_pct} <span style='font-size:0.7rem; color:#8b949e;'>(Win+BE)</span></td>
        <td class="pct color-win" style="border-bottom: 2px solid #2b303b;">{win_pct}</td>
        <td class="pct color-be" style="border-bottom: 2px solid #2b303b;">{be_pct}</td>
        <td class="pct color-loss" style="border-bottom: 2px solid #2b303b;">{loss_pct}</td>
    </tr>
    <tr>
        <td class="row-title" style="border-bottom: 2px solid #2b303b;">CAPITAL & PNL</td>
        <td class="val" style="border-bottom: 2px solid #2b303b; font-size: 1.2rem;">₹{INITIAL_CAPITAL:,.0f}</td>
        <td colspan="3" class="pct" style="border-bottom: 2px solid #2b303b; text-align: left; padding-left: 20px;">
            Realized: <b class="{'color-win' if realized_pnl > 0 else 'color-loss'}">₹{realized_pnl:,.2f}</b> | 
            Equity: <b style="color: #f0f6fc;">₹{current_equity:,.2f}</b> | 
            Unrealized: <b class="{'color-win' if total_unrealized_pnl > 0 else 'color-loss'}">₹{total_unrealized_pnl:,.2f}</b>
        </td>
    </tr>
    <tr>
        <td class="row-title">OPEN TRADES</td>
        <td class="val color-open">{total_open}</td>
        <td colspan="3" class="pct color-be" style="text-align: left; padding-left: 20px;">🛡️ {risk_free_count} Risk-Free | ⚠️ {total_open - risk_free_count} At Risk</td>
    </tr>
</table>
""", unsafe_allow_html=True)

def render_filters(df, tab_name, has_status=False):
    if df.empty: return df
    with st.expander(f"🔍 Filter {tab_name} Data"):
        cols = st.columns(4) if has_status else st.columns(3)
        assets = ['All'] + sorted(df['Asset'].unique().tolist())
        signals = ['All'] + sorted(df['Signal'].unique().tolist())
        
        sel_asset = cols[0].selectbox("Asset", assets, key=f"asset_{tab_name}")
        sel_sig = cols[1].selectbox("Signal", signals, key=f"sig_{tab_name}")
        
        if has_status:
            statuses = ['All'] + sorted(df['Status'].unique().tolist())
            sel_status = cols[2].selectbox("Status", statuses, key=f"status_{tab_name}")
            date_str = cols[3].text_input("Date (YYYY-MM-DD)", key=f"date_{tab_name}")
        else:
            date_str = cols[2].text_input("Date (YYYY-MM-DD)", key=f"date_{tab_name}")

        filtered_df = df.copy()
        if sel_asset != 'All': filtered_df = filtered_df[filtered_df['Asset'] == sel_asset]
        if sel_sig != 'All': filtered_df = filtered_df[filtered_df['Signal'] == sel_sig]
        if has_status and sel_status != 'All': filtered_df = filtered_df[filtered_df['Status'] == sel_status]
        if date_str:
            date_col = 'Entry Time' if 'Entry Time' in filtered_df.columns else 'Time (IST)'
            filtered_df = filtered_df[filtered_df[date_col].str.contains(date_str, na=False)]
        return filtered_df

# ==========================================
# UI: TABBED INTERFACE
# ==========================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🔥 Heatmap", "📈 Chart", "🟢 Open", "📚 Ledger", "🚫 Blocked", "💰 Equity"])

with tab1:
    st.markdown("<p style='font-size:0.9rem; color:gray; margin-bottom:5px; line-height:1.4;'><b>Legend:</b><br>🔴 Red &lt; 0.1% Gap (Imminent) | 🟠 Orange &lt; 0.5% Gap (Watch Closely)</p>", unsafe_allow_html=True)
    def apply_heatmap(val):
        if pd.isna(val): return ''
        try:
            if float(val) < 0.10: return 'background-color: rgba(255, 0, 0, 0.4); color: white;'
            elif float(val) < 0.50: return 'background-color: rgba(255, 165, 0, 0.4); color: white;'
        except: pass
        return ''
    if not live_df.empty: st.dataframe(live_df.style.map(apply_heatmap, subset=['% Gap']), use_container_width=True, height=600, hide_index=True)
    else: st.info("Waiting for first data sync...")

with tab2:
    if not live_df.empty:
        selected_stock = st.selectbox("Select an asset to render:", ["-- Select an Asset --"] + sorted(live_df['Asset'].tolist()), label_visibility="collapsed")
        if selected_stock != "-- Select an Asset --":
            yf_symbol = next(item['yf_symbol'] for item in WATCHLIST if item['name'] == selected_stock)
            with st.spinner(f"Loading order book for {selected_stock}..."):
                try:
                    chart_df = yf.Ticker(yf_symbol).history(interval="15m", period="5d") 
                    if not chart_df.empty:
                        if chart_df.index.tz is not None: chart_df.index = chart_df.index.tz_convert('Asia/Kolkata').tz_localize(None)
                        else: chart_df.index = chart_df.index + timedelta(hours=5, minutes=30)
                        
                        chart_df['EMA5'] = ta.ema(chart_df['Close'], length=5)
                        chart_df['EMA39'] = ta.ema(chart_df['Close'], length=39)
                        chart_df['ATR'] = ta.atr(chart_df['High'], chart_df['Low'], chart_df['Close'], length=14)
                        adx_res = ta.adx(chart_df['High'], chart_df['Low'], chart_df['Close'], length=14)
                        
                        if adx_res is not None and not adx_res.empty: chart_df['ADX'] = adx_res.iloc[:, 0].ffill()
                        else: chart_df['ADX'] = 0.0
                        
                        chart_df.dropna(subset=['EMA39', 'ADX', 'ATR'], inplace=True)
                        time_labels = chart_df.index.strftime('%b %d, %H:%M')
                        
                        live_adx_val = chart_df['ADX'].iloc[-1].round(2)
                        live_atr_val = chart_df['ATR'].iloc[-1].round(2)
                        
                        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05, row_heights=[0.7, 0.3])
                        
                        fig.add_trace(go.Candlestick(
                            x=time_labels, open=chart_df['Open'], high=chart_df['High'], 
                            low=chart_df['Low'], close=chart_df['Close'], name="Price",
                            customdata=chart_df['ATR'].round(2),
                            hovertemplate="Open: %{open}<br>High: %{high}<br>Low: %{low}<br>Close: %{close}<br>ATR: %{customdata}<extra></extra>"
                        ), row=1, col=1)
                        
                        fig.add_trace(go.Scatter(x=time_labels, y=chart_df['EMA5'], line=dict(color='#00ff00', width=1.5), name='EMA 5'), row=1, col=1)
                        fig.add_trace(go.Scatter(x=time_labels, y=chart_df['EMA39'], line=dict(color='#ff0000', width=2), name='EMA 39'), row=1, col=1)
                        
                        fig.add_trace(go.Scatter(x=time_labels, y=chart_df['ADX'], line=dict(color='#ffd700', width=1.5), name='ADX'), row=2, col=1)
                        fig.add_hline(y=20, line_dash="dot", annotation_text="Trend (20)", annotation_position="top right", line_color="#8b949e", row=2, col=1)
                        
                        fig.update_layout(
                            title=f"{selected_stock} | 15m Timeframe &nbsp;&nbsp;&nbsp; <span style='font-size:14px; color:#ffd700;'>Live ADX: {live_adx_val} | Live ATR: {live_atr_val}</span>",
                            template="plotly_dark", 
                            xaxis_rangeslider_visible=False, 
                            margin=dict(l=0, r=0, t=50, b=0), 
                            height=700, 
                            hovermode="x unified"
                        )
                        fig.update_xaxes(type='category', nticks=12, tickangle=-45, row=2, col=1)
                        st.plotly_chart(fig, use_container_width=True)
                except Exception as e: st.error(f"Chart data unavailable right now. Try again shortly. {str(e)}")

with tab3:
    if not open_df_ui.empty: 
        filtered_open = render_filters(open_df_ui, "Open", has_status=False)
        if not filtered_open.empty:
            total_open_pnl = filtered_open['Unrealized PnL (₹)'].sum()
            st.markdown(f"**Total Filtered Trades:** {len(filtered_open)} &nbsp;&nbsp;|&nbsp;&nbsp; **Filtered Unrealized PnL:** ₹{total_open_pnl:,.2f}")
            
            cols = ['Risk Status', 'Unrealized PnL (₹)'] + [col for col in filtered_open.columns if col not in ['Risk Status', 'Unrealized PnL (₹)', 'Latest Price', 'Yield']]
            filtered_open = filtered_open[cols]
            
            def color_open_ui(val):
                val_str = str(val)
                if 'RISK-FREE' in val_str: return 'background-color: rgba(163, 113, 247, 0.2); color: #a371f7; font-weight: bold;'
                if 'AT RISK' in val_str: return 'color: #8b949e;'
                try:
                    if float(val) > 0: return 'color: #3fb950; font-weight: bold;'
                    elif float(val) < 0: return 'color: #f85149; font-weight: bold;'
                except: pass
                return ''
                
            st.dataframe(filtered_open.style.map(color_open_ui, subset=['Risk Status', 'Unrealized PnL (₹)']), use_container_width=True, height=600, hide_index=True)
        else: st.info("No active trades match these filters.")
    else: 
        st.info("No active trades currently open.")

with tab4:
    if not history_df.empty:
        filtered_hist = render_filters(history_df, "Ledger", has_status=True)
        if not filtered_hist.empty:
            total_hist_pnl = filtered_hist['PnL (₹)'].sum()
            st.markdown(f"**Total Filtered Trades:** {len(filtered_hist)} &nbsp;&nbsp;|&nbsp;&nbsp; **Filtered Realized PnL:** ₹{total_hist_pnl:,.2f}")
            
            cols = list(filtered_hist.columns)
            cols.remove('PnL (₹)')
            cols.remove('Yield')
            exit_idx = cols.index('Exit Price') + 1
            cols.insert(exit_idx, 'PnL (₹)')
            filtered_hist = filtered_hist[cols]
            
            def color_status_pnl(val):
                val_str = str(val).upper()
                if 'WIN' in val_str or 'TP' in val_str: return 'background-color: rgba(63, 185, 80, 0.2); color: #3fb950; font-weight: bold;'
                elif 'LOSS' in val_str or 'SL HIT' in val_str: return 'background-color: rgba(248, 81, 73, 0.2); color: #f85149; font-weight: bold;'
                elif 'BREAK' in val_str: return 'background-color: rgba(163, 113, 247, 0.2); color: #a371f7; font-weight: bold;'
                try:
                    if float(val) > 0: return 'color: #3fb950; font-weight: bold;'
                    elif float(val) < 0: return 'color: #f85149; font-weight: bold;'
                    elif float(val) == 0: return 'color: #a371f7;'
                except: pass
                return ''
                
            st.dataframe(filtered_hist.style.map(color_status_pnl, subset=['Status', 'PnL (₹)']), use_container_width=True, height=600, hide_index=True)
        else: st.info("No closed trades match these filters.")
    else: st.info("No closed trades yet.")

with tab5:
    st.markdown("<p style='font-size:0.9rem; color:gray; margin-bottom:5px;'>Signals that were mathematically rejected by institutional filters to protect capital.</p>", unsafe_allow_html=True)
    blocked_df = pd.read_sql_query("SELECT ticker as Asset, signal_type as Signal, timestamp as 'Time (IST)', price as Price, rejection_reasons as 'Rejection Reasons', adx as ADX, htf_trend as '1H Trend', vol_ratio as 'Vol (x)' FROM blocked_signals ORDER BY id DESC", ui_conn)
    if not blocked_df.empty: 
        filtered_blocked = render_filters(blocked_df, "Blocked", has_status=False)
        st.markdown(f"**Total Blocked Signals:** {len(filtered_blocked)}")
        st.dataframe(filtered_blocked, use_container_width=True, height=600, hide_index=True)
    else: st.info("No signals have been blocked yet.")

with tab6:
    if not history_df.empty:
        st.markdown(f"### 📈 Equity Curve (Starting Capital: ₹{INITIAL_CAPITAL:,.0f})")
        equity_df = history_df[['Exit Time', 'PnL (₹)']].copy()
        equity_df['Exit Time'] = pd.to_datetime(equity_df['Exit Time'].str.replace(' (IST)', '', regex=False), errors='coerce')
        equity_df = equity_df.sort_values('Exit Time').dropna()
        equity_df['Cumulative PnL'] = equity_df['PnL (₹)'].cumsum()
        equity_df['Account Equity'] = INITIAL_CAPITAL + equity_df['Cumulative PnL']
        
        start_time = equity_df['Exit Time'].min() - pd.Timedelta(hours=1)
        start_row = pd.DataFrame({'Exit Time': [start_time], 'Account Equity': [INITIAL_CAPITAL]})
        equity_df = pd.concat([start_row, equity_df], ignore_index=True)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=equity_df['Exit Time'], y=equity_df['Account Equity'], mode='lines+markers', line=dict(color='#ffd700', width=3), marker=dict(size=6, color='#ffd700'), fill='tozeroy', fillcolor='rgba(255, 215, 0, 0.1)'))
        
        min_eq = equity_df['Account Equity'].min()
        max_eq = equity_df['Account Equity'].max()
        padding = (max_eq - min_eq) * 0.1 if max_eq != min_eq else 1000
        fig.update_layout(
            template="plotly_dark", 
            margin=dict(l=0, r=0, t=20, b=0), 
            height=500, hovermode="x unified", 
            yaxis_tickformat="₹,.0f", 
            yaxis_title="Capital (₹)",
            yaxis_range=[min_eq - padding, max_eq + padding]
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No closed trades available to plot equity curve yet.")

ui_conn.close()
