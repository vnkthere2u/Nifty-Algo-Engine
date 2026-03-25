import streamlit as st
import pandas as pd
import numpy as np
import pandas_ta as ta
import sqlite3
import time
import requests
import threading
import traceback
import yfinance as yf
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from tvDatafeed import TvDatafeed, Interval

# ==========================================
# 0. UI INITIALIZATION & CUSTOM CSS
# ==========================================
st.set_page_config(page_title="Alpha Engine Terminal", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
        .block-container { padding-top: 1rem; padding-bottom: 1rem; }
        h1 { font-size: 1.8rem !important; margin-bottom: 0px; padding-bottom: 0px; font-weight: 600; }
        h2 { font-size: 1.2rem !important; font-weight: 500; color: #888; margin-top: 0px; padding-top: 0px; }
        h3 { font-size: 1.1rem !important; font-weight: 600; }
        .stTabs [data-baseweb="tab-list"] { gap: 24px; }
        .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; padding-top: 10px; padding-bottom: 10px; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 1. TELEGRAM ALERT SETUP
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
# 2. DATABASE SETUP & SAFE MIGRATION
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
        conn.commit()
    except sqlite3.OperationalError:
        pass 

    c.execute('''CREATE TABLE IF NOT EXISTS system_status (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS system_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, message TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS live_market_data 
                 (ticker TEXT PRIMARY KEY, last_update TEXT, close_price REAL, ema5 REAL, ema39 REAL, trend TEXT, distance_pct REAL, htf_trend TEXT, vol_ratio REAL)''')
    
    try:
        c.execute("ALTER TABLE live_market_data ADD COLUMN adx REAL")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    conn.commit()
    return conn

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

# ==========================================
# 3. DUAL-ENGINE LOGIC & ADVANCED MATH
# ==========================================
def fetch_and_analyze(item):
    global tv
    df = None
    
    # 1. TradingView Primary Engine
    try:
        df_tv = tv.get_hist(symbol=item['tv_symbol'], exchange=item['tv_exchange'], interval=Interval.in_15_minute, n_bars=250)
        if df_tv is not None and not df_tv.empty:
            df_tv = df_tv.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
            df = df_tv
    except Exception:
        try: tv = TvDatafeed() 
        except: pass

    # 2. Yahoo Finance Fallback Engine
    if df is None or df.empty:
        try:
            # FIX: Pulled 20 days so Indian stocks have > 156 candles for the 1H proxy math
            df_yf = yf.Ticker(item['yf_symbol']).history(interval="15m", period="20d")
            if df_yf is not None and not df_yf.empty:
                df_yf.index = df_yf.index.tz_localize(None)
                df = df_yf
        except Exception:
            pass

    # 3. Mathematical Calculations & Sanitization
    if df is not None and not df.empty:
        try:
            # Force everything to numbers
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
                
            # FIX: Forward-fill micro-gaps before math to prevent EMA/ATR crashes
            df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].ffill()
            df.dropna(subset=['Close', 'High', 'Low'], inplace=True) 
            
            df['EMA5'] = ta.ema(df['Close'], length=5)
            df['EMA39'] = ta.ema(df['Close'], length=39)
            df['ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
            df['EMA20'] = ta.ema(df['Close'], length=20)
            df['EMA156'] = ta.ema(df['Close'], length=156)
            
            # ADX Chop Filter (Sanitized)
            adx_data = ta.adx(df['High'], df['Low'], df['Close'], length=14)
            if adx_data is not None and not adx_data.empty:
                df['ADX'] = adx_data.iloc[:, 0].ffill().fillna(0.0)
            else:
                df['ADX'] = 0.0
            
            # Volume Surge (Vectorized for high speed)
            if 'Volume' in df.columns:
                df['Volume'] = df['Volume'].fillna(0)
                df['Vol_MA20'] = df['Volume'].rolling(20).mean()
                df['Vol_Ratio'] = np.where(df['Vol_MA20'] > 0, df['Volume'] / df['Vol_MA20'], 1.0)
            else:
                df['Vol_Ratio'] = 1.0 
                
            # Safely drop rows only if the EMAs couldn't calculate
            df.dropna(subset=['EMA156', 'EMA39', 'EMA5', 'ATR'], inplace=True)
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
        if df is None: continue
            
        c.execute("SELECT id, signal_type, sl, tp FROM trades WHERE ticker=? AND status='OPEN'", (name,))
        open_trades = c.fetchall()
        
        ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        scan_time_str = ist_now.strftime("%Y-%m-%d %I:%M %p (IST)")
        
        current_candle = df.iloc[-1]
        last_closed = df.iloc[-2]
        prev_closed = df.iloc[-3]
        
        trend = "🟢 Bullish" if current_candle['EMA5'] > current_candle['EMA39'] else "🔴 Bearish"
        htf_trend = "🟢 Bullish" if current_candle['EMA20'] > current_candle['EMA156'] else "🔴 Bearish"
        vol_ratio = current_candle['Vol_Ratio']
        adx_val = current_candle['ADX']
        
        for trade in open_trades:
            trade_id, sig_type, sl, tp = trade
            current_open, current_high, current_low = current_candle['Open'], current_candle['High'], current_candle['Low']
            
            if sig_type == 'long':
                if current_open >= tp:
                    c.execute("UPDATE trades SET status='TP HIT (GAP UP)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, current_open, trade_id))
                    send_telegram_alert(f"🎯 <b>GAP UP TARGET HIT</b>\n{name} LONG closed at {round(current_open, 2)}")
                elif current_open <= sl:
                    c.execute("UPDATE trades SET status='SL HIT (GAP DOWN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, current_open, trade_id))
                    send_telegram_alert(f"🛑 <b>GAP DOWN STOP LOSS</b>\n{name} LONG closed at {round(current_open, 2)}")
                elif current_high >= tp:
                    c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, tp, trade_id))
                    send_telegram_alert(f"🎯 <b>TARGET HIT</b>\n{name} LONG closed at {round(tp, 2)}")
                elif current_low <= sl:
                    c.execute("UPDATE trades SET status='SL HIT (LOSS)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, sl, trade_id))
                    send_telegram_alert(f"🛑 <b>STOP LOSS HIT</b>\n{name} LONG closed at {round(sl, 2)}")
                    
            elif sig_type == 'short':
                if current_open <= tp:
                    c.execute("UPDATE trades SET status='TP HIT (GAP DOWN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, current_open, trade_id))
                    send_telegram_alert(f"🎯 <b>GAP DOWN TARGET HIT</b>\n{name} SHORT closed at {round(current_open, 2)}")
                elif current_open >= sl:
                    c.execute("UPDATE trades SET status='SL HIT (GAP UP)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, current_open, trade_id))
                    send_telegram_alert(f"🛑 <b>GAP UP STOP LOSS</b>\n{name} SHORT closed at {round(current_open, 2)}")
                elif current_low <= tp:
                    c.execute("UPDATE trades SET status='TP HIT (WIN)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, tp, trade_id))
                    send_telegram_alert(f"🎯 <b>TARGET HIT</b>\n{name} SHORT closed at {round(tp, 2)}")
                elif current_high >= sl:
                    c.execute("UPDATE trades SET status='SL HIT (LOSS)', exit_time=?, exit_price=? WHERE id=?", (scan_time_str, sl, trade_id))
                    send_telegram_alert(f"🛑 <b>STOP LOSS HIT</b>\n{name} SHORT closed at {round(sl, 2)}")
        conn.commit()

        latest_price = current_candle['Close']
        ema5_live, ema39_live = current_candle['EMA5'], current_candle['EMA39']
        dist_pct = abs(ema5_live - ema39_live) / ema39_live * 100
        
        c.execute("INSERT OR REPLACE INTO live_market_data (ticker, last_update, close_price, ema5, ema39, trend, distance_pct, htf_trend, vol_ratio, adx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (name, scan_time_str, round(latest_price, 2), round(ema5_live, 2), round(ema39_live, 2), trend, round(dist_pct, 4), htf_trend, round(vol_ratio, 2), round(adx_val, 2)))
        conn.commit()

        long_cross = (prev_closed['EMA5'] <= prev_closed['EMA39']) and (last_closed['EMA5'] > last_closed['EMA39'])
        short_cross = (prev_closed['EMA5'] >= prev_closed['EMA39']) and (last_closed['EMA5'] < last_closed['EMA39'])
        atr_val = last_closed['ATR']
        
        # Chop Filter Enforced
        is_trending = last_closed.get('ADX', 0.0) > 20.0
        
        if len(open_trades) == 0 and is_trending:
            if long_cross and htf_trend == "🟢 Bullish":
                entry = last_closed['Close']
                # 1:3 RR Logic Enforced
                sl, tp = entry - (1.5 * atr_val), entry + (4.5 * atr_val)
                
                c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status, htf_trend, vol_ratio) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (name, 'long', scan_time_str, round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN', htf_trend, round(vol_ratio, 2)))
                msg = f"🟢 <b>LONG SIGNAL: {name}</b>\nTime: {scan_time_str}\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}\n\n<i>Context:</i>\n1H Trend: {htf_trend}\nVol Surge: {round(vol_ratio, 1)}x\nADX Strength: {round(adx_val, 1)}"
                alerts.append(msg)
                send_telegram_alert(msg)
                
            elif short_cross and htf_trend == "🔴 Bearish":
                entry = last_closed['Close']
                # 1:3 RR Logic Enforced
                sl, tp = entry + (1.5 * atr_val), entry - (4.5 * atr_val)
                
                c.execute("INSERT INTO trades (ticker, signal_type, entry_time, entry_price, sl, tp, status, htf_trend, vol_ratio) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (name, 'short', scan_time_str, round(entry, 2), round(sl, 2), round(tp, 2), 'OPEN', htf_trend, round(vol_ratio, 2)))
                msg = f"🔴 <b>SHORT SIGNAL: {name}</b>\nTime: {scan_time_str}\nEntry: {round(entry, 2)}\nSL: {round(sl, 2)}\nTP: {round(tp, 2)}\n\n<i>Context:</i>\n1H Trend: {htf_trend}\nVol Surge: {round(vol_ratio, 1)}x\nADX Strength: {round(adx_val, 1)}"
                alerts.append(msg)
                send_telegram_alert(msg)
                
        conn.commit()
        time.sleep(1) 
        
    c.execute("DELETE FROM system_logs WHERE id NOT IN (SELECT id FROM system_logs ORDER BY id DESC LIMIT 500)")
    
    status_time_str = ist_now.strftime("%Y-%m-%d %I:%M %p (IST)")
    c.execute("INSERT OR REPLACE INTO system_status (key, value) VALUES ('last_scan', ?)", (status_time_str,))
    conn.commit()
    conn.close()
    return alerts

# ==========================================
# 4. STREAMLIT DASHBOARD UI
# ==========================================
st.markdown("<h1>⚡ Quantitative Alpha Engine</h1>", unsafe_allow_html=True)
st.markdown("<h2>Institutional 15m EMA Tracker • Multi-Asset 24/5 Monitoring</h2>", unsafe_allow_html=True)

ui_conn = get_db_connection()
ui_c = ui_conn.cursor()

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

# --- SIDEBAR CONTROL PANEL ---
st.sidebar.markdown("<h3>⚙️ Control Panel</h3>", unsafe_allow_html=True)
if engine_running: st.sidebar.success("✅ Background Daemon is LIVE")

ui_c.execute("SELECT value FROM system_status WHERE key='last_scan'")
last_scan_row = ui_c.fetchone()
st.sidebar.info(f"⏱️ **Last Database Sync:**\n{last_scan_row[0] if last_scan_row else 'Initializing...'}")

if st.sidebar.button("🔔 Send Test Telegram Alert"):
    send_telegram_alert("🟢 <b>TEST ALERT</b>\nSystem is online and tracking 18 assets.")
    st.sidebar.success("Test alert sent!")

if st.sidebar.button("🔄 Force Manual Data Sync"):
    with st.spinner("Executing Data Sync..."):
        process_market_data()
        st.rerun()

st.sidebar.markdown("---")
# LEAVE THIS ALONE - Do not click it so you preserve your logs and positions!
if st.sidebar.button("⚠️ Factory Reset Database"):
    ui_c.execute("DROP TABLE IF EXISTS trades")
    ui_c.execute("DROP TABLE IF EXISTS live_market_data")
    ui_c.execute("DROP TABLE IF EXISTS system_status")
    ui_c.execute("DROP TABLE IF EXISTS system_logs")
    ui_conn.commit()
    st.sidebar.success("Database wiped. Rebooting...")
    time.sleep(1)
    st.rerun()

# --- TOP LEVEL METRICS ---
closed_trades = pd.read_sql_query("SELECT * FROM trades WHERE status!='OPEN'", ui_conn)
col_a, col_b, col_c = st.columns(3)
if not closed_trades.empty:
    wins = len(closed_trades[closed_trades['status'].str.contains('WIN')])
    total = len(closed_trades)
    col_a.metric("Total Executed Trades", total)
    col_b.metric("Historical Win Rate", f"{(wins/total)*100:.1f}%")
else:
    col_a.metric("Total Executed Trades", 0)
    col_b.metric("Historical Win Rate", "0.0%")
col_c.metric("Active Watchlist Size", len(WATCHLIST))

st.markdown("---")

# --- TABBED INTERFACE ---
tab1, tab2, tab3, tab4 = st.tabs(["🔥 Live Heatmap", "📈 Advanced Chart", "🟢 Open Positions", "📚 Trade History"])

with tab1:
    st.markdown("<h3>Imminent Crossover Heatmap</h3>", unsafe_allow_html=True)
    st.markdown("<p style='font-size:0.9rem; color:gray;'><b>Legend:</b> 🔴 Red < 0.1% Gap (Imminent) | 🟠 Orange < 0.5% Gap (Watch Closely)</p>", unsafe_allow_html=True)

    live_df = pd.read_sql_query("SELECT ticker as Asset, close_price as 'Latest Price', distance_pct as '% Gap', trend as '15m Trend', htf_trend as '1H Trend', vol_ratio as 'Vol (x)', adx as 'ADX', last_update as 'Time (IST)' FROM live_market_data ORDER BY distance_pct ASC", ui_conn)

    def apply_heatmap(val):
        if pd.isna(val): return ''
        try:
            if float(val) < 0.10: return 'background-color: rgba(255, 0, 0, 0.4); color: white;'
            elif float(val) < 0.50: return 'background-color: rgba(255, 165, 0, 0.4); color: white;'
        except: pass
        return ''

    if not live_df.empty:
        st.dataframe(live_df.style.map(apply_heatmap, subset=['% Gap']), width='stretch', hide_index=True)
    else:
        st.info("Waiting for first data sync...")

    with st.expander("📝 How to read the Advanced Context Metrics"):
        st.markdown("""
        * **1-Hour Trend:** Must match the 15-minute trend for a trade to execute.
        * **ADX (Trend Strength):** Must be **> 20.0** for a trade to execute. If it's below 20, the market is chopping sideways and will destroy stop losses.
        * **Vol Surge (x):** Compares current 15m volume to the 20-candle average (> 1.5x = Heavy institutional volume).
        * **Risk Management Update:** Trades now execute at a strict **1:3 Risk/Reward** ratio utilizing a 1.5x ATR dynamic Stop Loss for breathing room.
        """)

with tab2:
    st.markdown("<h3>Institutional Chart Terminal</h3>", unsafe_allow_html=True)
    if not live_df.empty:
        selected_stock = st.selectbox("Select an asset to render:", ["-- Select an Asset --"] + sorted(live_df['Asset'].tolist()), label_visibility="collapsed")
        if selected_stock != "-- Select an Asset --":
            yf_symbol = next(item['yf_symbol'] for item in WATCHLIST if item['name'] == selected_stock)
            
            with st.spinner(f"Loading order book for {selected_stock}..."):
                try:
                    chart_df = yf.Ticker(yf_symbol).history(interval="15m", period="3d")
                    if not chart_df.empty:
                        if chart_df.index.tz is not None:
                            chart_df.index = chart_df.index.tz_convert('Asia/Kolkata').tz_localize(None)
                        else:
                            chart_df.index = chart_df.index + timedelta(hours=5, minutes=30)
                            
                        chart_df['EMA5'] = ta.ema(chart_df['Close'], length=5)
                        chart_df['EMA39'] = ta.ema(chart_df['Close'], length=39)
                        
                        time_labels = chart_df.index.strftime('%b %d, %H:%M')
                        
                        fig = go.Figure(data=[go.Candlestick(
                            x=time_labels, open=chart_df['Open'], high=chart_df['High'], 
                            low=chart_df['Low'], close=chart_df['Close'], name="Price"
                        )])
                        fig.add_trace(go.Scatter(x=time_labels, y=chart_df['EMA5'], line=dict(color='#00ff00', width=1.5), name='EMA 5'))
                        fig.add_trace(go.Scatter(x=time_labels, y=chart_df['EMA39'], line=dict(color='#ff0000', width=2), name='EMA 39'))
                        
                        fig.update_layout(
                            title=f"{selected_stock} | 15m Timeframe (IST)",
                            template="plotly_dark",
                            xaxis_rangeslider_visible=False,
                            margin=dict(l=0, r=0, t=40, b=0),
                            height=550,
                            hovermode="x unified"
                        )
                        fig.update_xaxes(type='category', nticks=12, tickangle=-45)
                        st.plotly_chart(fig, width='stretch')
                except Exception:
                    st.error("Chart data unavailable right now. Try again shortly.")

with tab3:
    st.markdown("<h3>Active Open Positions</h3>", unsafe_allow_html=True)
    open_df = pd.read_sql_query("SELECT ticker as Asset, signal_type as Signal, entry_time as 'Entry Time', entry_price as 'Entry', sl as SL, tp as TP, htf_trend as '1H Trend', vol_ratio as 'Vol (x)' FROM trades WHERE status='OPEN' ORDER BY id DESC", ui_conn)
    if not open_df.empty: 
        st.dataframe(open_df, width='stretch', hide_index=True)
    else: 
        st.info("No active trades currently open.")

with tab4:
    st.markdown("<h3>Closed Trade Ledger</h3>", unsafe_allow_html=True)
    history_df = pd.read_sql_query("SELECT ticker as Asset, signal_type as Signal, entry_time as 'Entry Time', entry_price as 'Entry', sl as SL, tp as TP, status as Status, exit_time as 'Exit Time', exit_price as 'Exit Price', htf_trend as '1H Trend', vol_ratio as 'Vol (x)' FROM trades WHERE status!='OPEN' ORDER BY id DESC", ui_conn)
    def color_status(val):
        if 'WIN' in str(val): return 'background-color: rgba(0, 255, 0, 0.2)'
        elif 'LOSS' in str(val): return 'background-color: rgba(255, 0, 0, 0.2)'
        return ''
    if not history_df.empty: 
        st.dataframe(history_df.style.map(color_status, subset=['Status']), width='stretch', hide_index=True)
    else: 
        st.info("No closed trades yet.")
