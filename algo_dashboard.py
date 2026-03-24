import streamlit as st
import pandas as pd
import pandas_ta as ta
import sqlite3
import time
import requests
import threading
import traceback
from datetime import datetime, timedelta
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
tv = TvDatafeed()

def get_db_connection():
    conn = sqlite3.connect('nifty100_live_trades.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal_type TEXT, 
                 entry_time TEXT, entry_price REAL, sl REAL, tp REAL, status TEXT, 
                 exit_time TEXT, exit_price REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS system_status 
                 (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS system_logs 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, message TEXT)''')
    conn.commit()
    return conn

get_db_connection().close()

def log_error(message):
    conn = get_db_connection()
    ist_now = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %I:%M:%S %p")
    conn.cursor().execute("INSERT INTO system_logs (timestamp, message) VALUES (?, ?)", (ist_now, str(message)))
    conn.commit()
    conn.close()

NIFTY_100 = [
    'NIFTY', 'BANKNIFTY', 'ABB', 'ADANIENT', 'ADANIGREEN', 'ADANIPORTS', 'ADANIENSOL', 
    'AMBUJACEM', 'APOLLOHOSP', 'ASIANPAINT', 'ATGL', 'AXISBANK', 'BAJAJ_AUTO', 'BAJFINANCE', 
    'BAJAJFINSV', 'BAJAJHLDNG', 'BANKBARODA', 'BEL', 'BHARATFORG', 'BHARTIARTL', 'BOSCHLTD', 
    'BPCL', 'BRITANNIA', 'CANBK', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE', 'COLPAL', 
    'DIVISLAB', 'DLF', 'DMART', 'DRREDDY', 'EICHERMOT', 'GAIL', 'GODREJCP', 'GRASIM', 
    'HAL', 'HAVELLS', 'HCLTECH', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 
    'HINDUNILVR', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IDFCFIRSTB', 'INDIGO', 'INDUSINDBK', 
    'INFY', 'IOC', 'IRCTC', 'IRFC', 'ITC', 'JINDALSTEL', 'JIOFIN', 'JSWSTEEL', 'KOTAKBANK', 
    'LICI', 'LODHA', 'LT', 'LTIM', 'LUPIN', 'M_M', 'MARICO', 'MARUTI', 'MUTHOOTFIN', 'NAUKRI', 
    'NESTLEIND', 'NTPC', 'ONGC', 'PIDILITIND', 'PNB', 'POLYCAB', 'POWERGRID', 'RECLTD', 
    'RELIANCE', 'SBILIFE', 'SBIN', 'SCHAEFFLER', 'SHREECEM', 'SIEMENS', 'SRF', 'SUNPHARMA', 
    'TATACONSUM', 'TATAMOTORS', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TRENT', 'TVSMOTOR', 
    'UBL', 'ULTRACEMCO', 'VEDL', 'WIPRO', 'ZOMATO'
]

# ==========================================
# 2. CORE STRATEGY LOGIC
# ==========================================
def fetch_and_analyze(ticker):
    try:
        # INCREASED TO 500 BARS TO ENSURE FLAWLESS EMA MATH
        df = tv.get_hist(symbol=ticker, exchange='NSE', interval=Interval.in_15_minute, n_bars=500)
        if df is None or df.empty: return None
        df.rename(columns={'open': 'High', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
        
        df['EMA5'] = ta.ema(df['Close'], length=5)
        df['EMA39'] = ta.ema(df['Close'], length=39)
        df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
        
        df.dropna(inplace=True)
        return df
    except Exception as e:
        log_error(f"Data fetch failed for {ticker}: {e}")
        return None

def process_market_data():
    conn = get_db_connection()
    c = conn.cursor()
    alerts = []
    
    for ticker in NIFTY_100:
        df = fetch_and_analyze(ticker)
        if df is None or len(df) < 5: continue
            
        c.execute("SELECT id, signal_type, sl, tp FROM trades WHERE ticker=? AND status='OPEN'", (ticker,))
        open_trades = c.fetchall()
        
        current_candle = df.iloc[-1]
        last_closed = df.iloc[-2]
        prev_closed = df.iloc[-3]
        
        for trade in open_trades:
            trade_id, sig_type, sl, tp = trade
            high_price, low_price = current_candle['High'], current_candle['Low']
            
            if sig_type == 'long':
                if high_price >= tp:
                    c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (str(current_candle.name), tp, trade_id))
                    send_telegram_alert(f"🎯 <b>TARGET HIT</b>\n{ticker} LONG closed at {round(tp, 2)}")
                elif low_price <= sl:
                    c.execute("UPDATE trades SET status='SL HIT (LOSS)', exit_time=?, exit_price=? WHERE id=?", (str(current_candle.name), sl, trade_id))
                    send_telegram_alert(f"🛑 <b>STOP LOSS HIT</b>\n{ticker} LONG closed at {round(sl, 2)}")
            elif sig_type == 'short':
                if low_price <= tp:
                    c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (str(current_candle.name), tp, trade_id))
                    send_telegram_alert(f"🎯 <b>TARGET HIT</b>\n{ticker} SHORT closed at {round(tp, 2)}")
                elif high_price >= sl:
                    c.execute("UPDATE trades SET status='SL HIT (LOSS)', exit_time=?, exit_price=? WHERE id=?", (str(current_candle.name), sl, trade_id))
                    send_telegram_alert(f"🛑 <b>STOP LOSS HIT</b>\n{ticker} SHORT closed at {round(sl, 2)}")
        conn.commit()

        long_cross = (prev_closed['EMA5'] <= prev_closed['EMA39']) and (last_closed['EMA5'] > last_closed['EMA39'])
        short_cross = (prev_closed['EMA5'] >= prev_closed['EMA39']) and (last_closed['EMA5'] < last_closed['EMA39'])
        
        atr_val = last_closed['ATR']
        high, low = last_closed['High'], last_closed['Low']
        
        if len(open_trades) == 0:
            if long_cross:
                entry = (high + low) / 2
                sl, tp = entry - atr_val, entry + (3.0 * atr_val)
                c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (ticker, 'long', str(last_closed.name), round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN'))
                msg = f"🟢 <b>LONG SIGNAL: {ticker}</b>\nTime: {str(last_closed.name)}\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}"
                alerts.append(msg)
                send_telegram_alert(msg)
                
            elif short_cross:
                entry = (high + low) / 2
                sl, tp = entry + atr_val, entry - (3.0 * atr_val)
                c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (ticker, 'short', str(last_closed.name), round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN'))
                msg = f"🔴 <b>SHORT SIGNAL: {ticker}</b>\nTime: {str(last_closed.name)}\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}"
                alerts.append(msg)
                send_telegram_alert(msg)
                
        conn.commit()
        time.sleep(0.5) 
        
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    c.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES ('last_scan', ?)", (ist_now.strftime("%I:%M:%S %p (IST)"),))
    conn.commit()
    conn.close()
    return alerts

# ==========================================
# 3. STREAMLIT DASHBOARD UI
# ==========================================
st.set_page_config(page_title="Live Nifty 100 Engine", layout="wide")
st.title("⚡ Live Nifty 100 Algo Engine")

st.subheader("🔔 Recent Signals (Current Scan)")
signal_placeholder = st.empty()

st.sidebar.header("Control Panel")

ui_conn = get_db_connection()
ui_c = ui_conn.cursor()
ui_c.execute("SELECT value FROM system_status WHERE key='last_scan'")
last_scan_row = ui_c.fetchone()
st.sidebar.info(f"⏱️ **Last Background Scan:**\n{last_scan_row[0] if last_scan_row else 'Initializing...'}")

if st.sidebar.button("⚠️ Reset/Clear Database"):
    ui_c.execute("DELETE FROM trades")
    ui_c.execute("DELETE FROM system_status")
    ui_c.execute("DELETE FROM system_logs")
    ui_conn.commit()
    st.sidebar.success("Database wiped clean! Ready for fresh signals.")
    time.sleep(1)
    st.rerun()

# --- THE BACKGROUND DAEMON ENGINE ---
@st.cache_resource
def start_background_scanner():
    def background_loop():
        while True:
            try:
                process_market_data()
            except Exception as e:
                log_error(f"Loop Crash: {traceback.format_exc()}")
            time.sleep(300)
            
    thread = threading.Thread(target=background_loop, daemon=True)
    thread.start()
    return True

engine_running = start_background_scanner()

if engine_running:
    st.sidebar.success("✅ Background Engine is LIVE.")

if st.sidebar.button("Force Manual Scan Now"):
    with st.spinner("Fetching 500 bars of deep historical data for exact EMA matches..."):
        new_alerts = process_market_data()
        if new_alerts:
            for alert in new_alerts: signal_placeholder.success(alert.replace("<b>", "").replace("</b>", ""))
        else:
            signal_placeholder.info("Manual scan complete. No new crossovers found right now.")
        st.rerun()

st.markdown("---")
st.subheader("🟢 Live Open Positions")
open_df = pd.read_sql_query("SELECT ticker, signal_type, entry_time, entry_price, sl, tp FROM trades WHERE status='OPEN' ORDER BY id DESC", ui_conn)
if not open_df.empty: st.dataframe(open_df, use_container_width=True)
else: st.write("No active trades currently open.")

st.markdown("---")
st.subheader("📚 Stock-Wise Trade History")
history_df = pd.read_sql_query("SELECT ticker, signal_type, entry_time, entry_price, sl, tp, status, exit_time, exit_price FROM trades WHERE status!='OPEN' ORDER BY id DESC", ui_conn)

def color_status(val):
    color = '#004d00' if 'WIN' in str(val) else '#660000' if 'LOSS' in str(val) else ''
    return f'background-color: {color}'

if not history_df.empty: st.dataframe(history_df.style.map(color_status, subset=['status']), use_container_width=True)
else: st.write("No closed trades yet.")

st.markdown("---")
with st.expander("🛠️ System Debug Logs (Tap to view)"):
    logs_df = pd.read_sql_query("SELECT timestamp, message FROM system_logs ORDER BY id DESC LIMIT 15", ui_conn)
    if not logs_df.empty: st.dataframe(logs_df, use_container_width=True)
    else: st.write("System operating normally. No errors recorded.")
