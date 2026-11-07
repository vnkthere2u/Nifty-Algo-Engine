"""Load NIFTY 1-minute history (technovusin/nifty50-historical-data CSV layout) and
build session-anchored 5m / 15m / 60m bars.  All timestamps IST, bar label = bar start."""
import glob, os
import pandas as pd

DATA_DIR = os.environ.get("NIFTY_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
CACHE = os.path.join(os.path.dirname(DATA_DIR), "nifty_1m.pkl")


def load_1m() -> pd.DataFrame:
    if os.path.exists(CACHE):
        return pd.read_pickle(CACHE)
    frames = []
    for f in sorted(glob.glob(os.path.join(DATA_DIR, "NIFTY50_1min_*.csv"))):
        d = pd.read_csv(f)
        d["ts"] = pd.to_datetime(d["Timestamp"])
        frames.append(d[["ts", "Open", "High", "Low", "Close"]])
    df = pd.concat(frames).drop_duplicates("ts").set_index("ts").sort_index()
    # regular session only (drops muhurat evening sessions)
    t = df.index.hour * 60 + df.index.minute
    df = df[(t >= 555) & (t <= 929)]
    df.to_pickle(CACHE)
    return df


def resample_session(df1: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Bars anchored to 09:15 open (like NSE/yfinance), label = bar start."""
    agg = {"Open": "first", "High": "max", "Low": "min", "Close": "last"}
    out = df1.resample(f"{minutes}min", origin="start_day", offset="15min").agg(agg)
    return out.dropna(subset=["Close"])


if __name__ == "__main__":
    d = load_1m()
    print(d.shape, d.index.min(), d.index.max())
    b15 = resample_session(d, 15)
    print(b15.head(30).tail(5)); print(b15.shape)
