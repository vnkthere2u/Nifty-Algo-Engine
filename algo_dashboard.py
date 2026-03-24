import streamlit as st
import pandas as pd
import pandas_ta as ta
import sqlite3
import time
import requests
import threading
from datetime import datetime, timedelta
from tvDatafeed import TvDatafeed, Interval

# ==========================================
# 0. TELEGRAM ALERT SETUP (SECURE VAULT)
# ==========================================
try:
    TELEGRAM_TOKEN = st.secrets["TELEGRAM_TOKEN"]
    TELEGRAM_CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
except:
    TELEGRAM_TOKEN = ""
    TELEGRAM_CHAT_ID = ""

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN:
        return 
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML'}
    try:
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"Telegram alert failed: {e}")

# ==========================================
# 1. TV DATAFEED & DATABASE SETUP
# ==========================================
tv = TvDatafeed()

conn = sqlite3.connect('nifty100_live_trades.db', check_same_thread=False)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS trades 
             (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, signal_type TEXT, 
             entry_time TEXT, entry_price REAL, sl REAL, tp REAL, status TEXT, 
             exit_time TEXT, exit_price REAL)''')
c.execute('''CREATE TABLE IF NOT EXISTS gating_state 
             (ticker TEXT PRIMARY KEY, last_sig TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS system_status 
             (key TEXT PRIMARY KEY, value TEXT)''')
conn.commit()

# Nifty 100 + Major Indices
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
        df = tv.get_hist(symbol=ticker, exchange='NSE', interval=Interval.in_15_minute, n_bars=60)
        if df is None or df.empty: return None
        df.rename(columns={'open': 'High', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
        
        df['EMA5'] = ta.ema(df['Close'], length=5)
        df['EMA39'] = ta.ema(df['Close'], length=39)
        df['RSI'] = ta.rsi(df['Close'], length=14)
        adx_df = ta.adx(df['High'], df['Low'], df['Close'], length=14)
        df['ADX'] = adx_df['ADX_14'] if adx_df is not None else 0
        df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
        
        # Calculate Cumulative Volume for the current trading day
        df['Cum_Volume'] = df.groupby(df.index.date)['Volume'].cumsum()
        
        df.dropna(inplace=True)
        return df
    except:
        return None

def process_market_data():
    alerts = []
    for ticker in NIFTY_100:
        df = fetch_and_analyze(ticker)
        if df is None or df.empty: continue
            
        c.execute("SELECT last_sig FROM gating_state WHERE ticker=?", (ticker,))
        row = c.fetchone()
        last_sig = row[0] if row else "none"
        
        c.execute("SELECT id, signal_type, sl, tp FROM trades WHERE ticker=? AND status='OPEN'", (ticker,))
        open_trades = c.fetchall()
        
        current_candle = df.iloc[-1]
        
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

        # Extract values for the new conditions
        last_closed = df.iloc[-2]
        adx_val, rsi_val = last_closed['ADX'], last_closed['RSI']
        ema5, ema39, atr_val = last_closed['EMA5'], last_closed['EMA39'], last_closed['ATR']
        high, low = last_closed['High'], last_closed['Low']
        
        # Pulling the cumulative volume, not the single candle volume
        close_price, cum_volume_val = last_closed['Close'], last_closed['Cum_Volume']
        
        if last_sig == "long" and ema5 < ema39: last_sig = "none"
        if last_sig == "short" and ema5 > ema39: last_sig = "none"
            
        # NEW BASE CONDITIONS: Liquidity (Cumulative) + Momentum
        liquidity_filter = (close_price > 100) and (cum_volume_val > 350000 or ticker in ['NIFTY', 'BANKNIFTY'])
        
        can_long = liquidity_filter and (adx_val >= 20) and (rsi_val >= 60) and (ema5 > ema39)
        can_short = liquidity_filter and (adx_val >= 20) and (rsi_val <= 40) and (ema39 > ema5)
        
        long_trigger = can_long and (last_sig != "long")
        short_trigger = can_short and (last_sig != "short")
        
        if long_trigger:
            last_sig = "long"
            entry = (high + low) / 2
            sl, tp = entry - atr_val, entry + (3.0 * atr_val)
            c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (ticker, 'long', str(last_closed.name), round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN'))
            msg = f"🟢 <b>LONG SIGNAL: {ticker}</b>\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}"
            alerts.append(msg)
            send_telegram_alert(msg)
            
        elif short_trigger:
            last_sig = "short"
            entry = (high + low) / 2
            sl, tp = entry + atr_val, entry - (3.0 * atr_val)
            c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (ticker, 'short', str(last_closed.name), round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN'))
            msg = f"🔴 <b>SHORT SIGNAL: {ticker}</b>\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}"
            alerts.append(msg)
            send_telegram_alert(msg)

        c.execute("INSERT OR REPLACE INTO gating_state (ticker, last_sig) VALUES (?, ?)", (ticker, last_sig))
        conn.commit()
        time.sleep(0.5) 
        
    # Update the heart-beat timestamp when the scan completes
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    scan_time_str = ist_now.strftime("%I:%M:%S %p (IST)")
    c.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES ('last_scan', ?)", (scan_time_str,))
    conn.commit()
        
    return alerts

# ==========================================
# 3. STREAMLIT DASHBOARD UI
# ==========================================
st.set_page_config(page_title="Live Nifty 100 Engine", layout="wide")
st.title("⚡ Live Nifty 100 Algo Engine")

st.subheader("🔔 Recent Signals (Current Scan)")
signal_placeholder = st.empty()

st.sidebar.header("Control Panel")

# --- UI TIMESTAMPS ---
c.execute("SELECT value FROM system_status WHERE key='last_scan'")
last_scan_row = c.fetchone()
last_scan_time = last_scan_row[0] if last_scan_row else "Initializing..."

st.sidebar.info(f"⏱️ **Last Background Scan:**\n{last_scan_time}")

# --- THE BACKGROUND DAEMON ENGINE ---
@st.cache_resource
def start_background_scanner():
    def background_loop():
        while True:
            try:
                process_market_data()
            except Exception as e:
                print(f"Background scan error: {e}")
            time.sleep(300)

    thread = threading.Thread(target=background_loop, daemon=True)
    thread.start()
    return True

engine_running = start_background_scanner()

if engine_running:
    st.sidebar.success("✅ Background Engine is LIVE.")

if st.sidebar.button("Force Manual Scan Now"):
    with st.spinner("Fetching live data for Nifty 100..."):
        new_alerts = process_market_data()
        if new_alerts:
            for alert in new_alerts:
                signal_placeholder.success(alert.replace("<b>", "").replace("</b>", ""))
        else:
            signal_placeholder.info("Manual scan complete. No new signals right now.")
            st.rerun() # Forces the UI to update the timestamp immediately

st.markdown("---")
st.subheader("🟢 Live Open Positions")
open_df = pd.read_sql_query("SELECT ticker, signal_type, entry_time, entry_price, sl, tp FROM trades WHERE status='OPEN' ORDER BY id DESC", conn)

stock_filter_open = st.selectbox("Filter Open Positions by Stock:", ["All"] + NIFTY_100, key="open_filter")
if stock_filter_open != "All":
    open_df = open_df[open_df['ticker'] == stock_filter_open]

if not open_df.empty:
    st.dataframe(open_df, use_container_width=True)
else:
    st.write("No active trades currently open for this selection.")

st.markdown("---")
st.subheader("📚 Stock-Wise Trade History")
history_df = pd.read_sql_query("SELECT ticker, signal_type, entry_time, entry_price, sl, tp, status, exit_time, exit_price FROM trades WHERE status!='OPEN' ORDER BY id DESC", conn)

stock_filter_history = st.selectbox("Filter History by Stock:", ["All"] + NIFTY_100, key="hist_filter")
if stock_filter_history != "All":
    history_df = history_df[history_df['ticker'] == stock_filter_history]

def color_status(val):
    color = '#004d00' if 'WIN' in str(val) else '#660000' if 'LOSS' in str(val) else ''
    return f'background-color: {color}'

if not history_df.empty:
    try:
        st.dataframe(history_df.style.map(color_status, subset=['status']), use_container_width=True)
    except AttributeError:
        st.dataframe(history_df.style.applymap(color_status, subset=['status']), use_container_width=True)
else:
    st.write("No closed trades yet for this selection.")
