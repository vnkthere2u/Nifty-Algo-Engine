import streamlit as st
import pandas as pd
import pandas_ta as ta
import sqlite3
import time
import requests
import threading
import traceback
import yfinance as yf
from datetime import datetime, timedelta, timezone
from tvDatafeed import TvDatafeed, Interval

# ==========================================
# 0. TELEGRAM ALERT SETUP
# ==========================================
try:
    TELEGRAM_TOKEN = st.secrets["TELEGRAM_TOKEN"]
    TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
except:
    TELEGRAM_TOKEN = ""
    TELEGRAM_CHAT_ID = ""

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN: return 
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML'}
    try: requests.post(url, data=payload, timeout=5)
    except: pass

# ==========================================
# 1. DATABASE SETUP
# ==========================================
def get_db_connection():
    conn = sqlite3.connect('nifty_live_trades.db', check_same_thread=False, timeout=30.0)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal_type TEXT, 
                 entry_time TEXT, entry_price REAL, sl REAL, tp REAL, status TEXT, 
                 exit_time TEXT, exit_price REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS system_status 
                 (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS system_logs 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, message TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS live_market_data 
                 (ticker TEXT PRIMARY KEY, last_update TEXT, close_price REAL, ema5 REAL, ema39 REAL, trend TEXT, distance_pct REAL)''')
    conn.commit()
    return conn

get_db_connection().close()

def log_error(message):
    try:
        conn = get_db_connection()
        ist_now = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %I:%M:%S %p")
        conn.cursor().execute("INSERT INTO system_logs (timestamp, message) VALUES (?, ?)", (ist_now, str(message)))
        conn.commit()
        conn.close()
    except:
        pass 

WATCHLIST = [
    {'name': 'NIFTY 50', 'tv_symbol': 'NIFTY', 'tv_exchange': 'NSE', 'yf_symbol': '^NSEI'},
    {'name': 'BANK NIFTY', 'tv_symbol': 'BANKNIFTY', 'tv_exchange': 'NSE', 'yf_symbol': '^NSEBANK'},
    {'name': 'BITCOIN (24/7)', 'tv_symbol': 'BTCUSDT', 'tv_exchange': 'BINANCE', 'yf_symbol': 'BTC-USD'}
]

tv = TvDatafeed()

# ==========================================
# 2. BULLETPROOF DUAL-ENGINE LOGIC
# ==========================================
def fetch_and_analyze(item):
    global tv
    df = None
    
    # ENGINE 1: Attempt TradingView
    try:
        df_tv = tv.get_hist(symbol=item['tv_symbol'], exchange=item['tv_exchange'], interval=Interval.in_15_minute, n_bars=200)
        if df_tv is not None and not df_tv.empty:
            df_tv = df_tv.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
            df = df_tv
    except Exception as e:
        try: tv = TvDatafeed() 
        except: pass

    # ENGINE 2: Fallback to Yahoo Finance
    if df is None or df.empty:
        try:
            df_yf = yf.Ticker(item['yf_symbol']).history(interval="15m", period="5d")
            if df_yf is not None and not df_yf.empty:
                df_yf.index = df_yf.index.tz_localize(None)
                df = df_yf
        except Exception as e:
            pass

    if df is not None and not df.empty:
        try:
            for col in ['Open', 'High', 'Low', 'Close']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df.dropna(subset=['Close', 'High', 'Low'], inplace=True) 
            
            df['EMA5'] = ta.ema(df['Close'], length=5)
            df['EMA39'] = ta.ema(df['Close'], length=39)
            df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
            df.dropna(inplace=True)
            
            if len(df) >= 5: return df
        except Exception as e:
            log_error(f"Math Error on {item['name']}: {e}")
            
    return None

def process_market_data():
    conn = get_db_connection()
    c = conn.cursor()
    alerts = []
    
    for item in WATCHLIST:
        name = item['name']
        df = fetch_and_analyze(item)
        
        if df is None: 
            continue
            
        c.execute("SELECT id, signal_type, sl, tp FROM trades WHERE ticker=? AND status='OPEN'", (name,))
        open_trades = c.fetchall()
        
        ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        scan_time_str = ist_now.strftime("%I:%M:%S %p (IST)")
        
        current_candle = df.iloc[-1]
        last_closed = df.iloc[-2]
        prev_closed = df.iloc[-3]
        
        for trade in open_trades:
            trade_id, sig_type, sl, tp = trade
            high_price, low_price = current_candle['High'], current_candle['Low']
            
            if sig_type == 'long':
                if high_price >= tp:
                    c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, tp, trade_id))
                    send_telegram_alert(f"🎯 <b>TARGET HIT</b>\n{name} LONG closed at {round(tp, 2)}")
                elif low_price <= sl:
                    c.execute("UPDATE trades SET status='SL HIT (LOSS)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, sl, trade_id))
                    send_telegram_alert(f"🛑 <b>STOP LOSS HIT</b>\n{name} LONG closed at {round(sl, 2)}")
            elif sig_type == 'short':
                if low_price <= tp:
                    c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, tp, trade_id))
                    send_telegram_alert(f"🎯 <b>TARGET HIT</b>\n{name} SHORT closed at {round(tp, 2)}")
                elif high_price >= sl:
                    c.execute("UPDATE trades SET status='SL HIT (LOSS)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, sl, trade_id))
                    send_telegram_alert(f"🛑 <b>STOP LOSS HIT</b>\n{name} SHORT closed at {round(sl, 2)}")
        conn.commit()

        # Update Live Data Table for Dashboard
        latest_price = current_candle['Close']
        ema5_live = current_candle['EMA5']
        ema39_live = current_candle['EMA39']
        
        dist_pct = abs(ema5_live - ema39_live) / ema39_live * 100
        trend = "🟢 Bullish" if ema5_live > ema39_live else "🔴 Bearish"
        
        c.execute("INSERT OR REPLACE INTO live_market_data (ticker, last_update, close_price, ema5, ema39, trend, distance_pct) VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (name, scan_time_str, round(latest_price, 2), round(ema5_live, 2), round(ema39_live, 2), trend, round(dist_pct, 4)))
        conn.commit()

        # Execute Trade Signals based on CLOSED candles
        long_cross = (prev_closed['EMA5'] <= prev_closed['EMA39']) and (last_closed['EMA5'] > last_closed['EMA39'])
        short_cross = (prev_closed['EMA5'] >= prev_closed['EMA39']) and (last_closed['EMA5'] < last_closed['EMA39'])
        
        atr_val = last_closed['ATR']
        
        if len(open_trades) == 0:
            if long_cross:
                entry = last_closed['Close']
                sl, tp = entry - atr_val, entry + (3.0 * atr_val)
                c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (name, 'long', scan_time_str, round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN'))
                msg = f"🟢 <b>LONG SIGNAL: {name}</b>\nTime: {scan_time_str}\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}"
                alerts.append(msg)
                send_telegram_alert(msg)
                
            elif short_cross:
                entry = last_closed['Close']
                sl, tp = entry + atr_val, entry - (3.0 * atr_val)
                c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (name, 'short', scan_time_str, round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN'))
                msg = f"🔴 <b>SHORT SIGNAL: {name}</b>\nTime: {scan_time_str}\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}"
                alerts.append(msg)
                send_telegram_alert(msg)
                
        conn.commit()
        time.sleep(1) 
        
    c.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES ('last_scan', ?)", (ist_now.strftime("%I:%M:%S %p (IST)"),))
    conn.commit()
    conn.close()
    return alerts

# ==========================================
# 3. STREAMLIT DASHBOARD UI
# ==========================================
st.set_page_config(page_title="Live Algo Engine", layout="wide")
st.title("⚡ Premium Algo Engine (Background Mode)")

ui_conn = get_db_connection()
ui_c = ui_conn.cursor()

# --- THE BACKGROUND DAEMON ENGINE ---
@st.cache_resource
def start_background_scanner():
    def background_loop():
        while True:
            try: process_market_data()
            except Exception: pass
            time.sleep(300)
    thread = threading.Thread(target=background_loop, daemon=True)
    thread.start()
    return True

engine_running = start_background_scanner()

st.sidebar.header("Control Panel")
if engine_running:
    st.sidebar.success("✅ Engine is LIVE in the background.")

ui_c.execute("SELECT value FROM system_status WHERE key='last_scan'")
last_scan_row = ui_c.fetchone()
st.sidebar.info(f"⏱️ **Last Database Update:**\n{last_scan_row[0] if last_scan_row else 'Initializing...'}")

if st.sidebar.button("Force Manual Scan Now"):
    with st.spinner("Fetching data..."):
        process_market_data()
        st.rerun()

if st.sidebar.button("⚠️ Factory Reset Database"):
    ui_c.execute("DROP TABLE IF EXISTS trades")
    ui_c.execute("DROP TABLE IF EXISTS live_market_data")
    ui_c.execute("DROP TABLE IF EXISTS system_status")
    ui_c.execute("DROP TABLE IF EXISTS system_logs")
    ui_conn.commit()
    st.sidebar.success("Database wiped. Rebooting...")
    time.sleep(1)
    st.rerun()

# --- LIVE MARKET DATA ---
live_df = pd.read_sql_query("SELECT ticker as Asset, close_price as 'Latest Price', distance_pct as '% Gap', ema5 as 'EMA 5', ema39 as 'EMA 39', trend as Trend, last_update as 'Time (IST)' FROM live_market_data ORDER BY distance_pct ASC", ui_conn)

st.markdown("---")
st.subheader("🔥 Assets Nearing Crossover")
if not live_df.empty:
    st.dataframe(live_df, use_container_width=True, hide_index=True)
else:
    st.write("Waiting for the engine to complete its first scan...")

# --- SPECIFIC ASSET LOOKUP ---
st.markdown("---")
st.subheader("🔍 Specific Asset Lookup")
if not live_df.empty:
    selected_stock = st.selectbox("Select an asset to view its latest stored details:", ["-- Select an Asset --"] + sorted(live_df['Asset'].tolist()))
    if selected_stock != "-- Select an Asset --":
        stock_data = live_df[live_df['Asset'] == selected_stock].iloc[0]
        col1, col2, col3 = st.columns(3)
        col1.metric("Latest Updated Price", f"{stock_data['Latest Price']}")
        col2.metric("% Gap to Cross", f"{stock_data['% Gap']}%")
        col3.metric("Current Trend", stock_data['Trend'])
        st.info(f"**EMA 5:** `{stock_data['EMA 5']}` &nbsp;|&nbsp; **EMA 39:** `{stock_data['EMA 39']}` &nbsp;|&nbsp; **Updated At:** `{stock_data['Time (IST)']}`")
else:
    st.write("Data loading...")

# --- LIVE OPEN POSITIONS ---
st.markdown("---")
st.subheader("🟢 Live Open Positions")
open_df = pd.read_sql_query("SELECT ticker, signal_type, entry_time, entry_price, sl, tp FROM trades WHERE status='OPEN' ORDER BY id DESC", ui_conn)
if not open_df.empty: st.dataframe(open_df, use_container_width=True)
else: st.write("No active trades currently open.")

# --- TRADE HISTORY ---
with st.expander("📚 View Closed Trade History"):
    history_df = pd.read_sql_query("SELECT ticker, signal_type, entry_time, entry_price, sl, tp, status, exit_time, exit_price FROM trades WHERE status!='OPEN' ORDER BY id DESC", ui_conn)
    def color_status(val):
        color = '#004d00' if 'WIN' in str(val) else '#660000' if 'LOSS' in str(val) else ''
        return f'background-color: {color}'
    if not history_df.empty: st.dataframe(history_df.style.map(color_status, subset=['status']), use_container_width=True)
    else: st.write("No closed trades yet.")
