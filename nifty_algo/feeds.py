"""Market data feed with hard timeouts, retry/backoff, circuit breaker, caching and validation.

Primary source: Yahoo Finance v8 chart endpoint (one lightweight HTTP call per request).
Fallback: the yfinance library (if installed).  The engine makes at most ~2 requests per
5-minute cycle for a single symbol, so rate limits are never approached.
"""
from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional

import pandas as pd
import requests

from .config import IST

log = logging.getLogger("nifty_algo.feed")

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
CHART_URL_ALT = "https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
UA = "Mozilla/5.0"  # Yahoo answers 429 to full browser UA strings sent without cookies


class FeedError(RuntimeError):
    pass


@dataclass
class Snapshot:
    """Everything the strategy needs at a decision instant."""
    daily: pd.DataFrame          # completed daily bars BEFORE today (index: date), OHLC
    today: Optional[pd.Series]   # today's partial bar (Open/High/Low/Close) up to `as_of`, None if no session today
    last_price: Optional[float]
    as_of: Optional[datetime]    # IST timestamp of the latest intraday bar used


def parse_chart_json(js: dict) -> pd.DataFrame:
    """Parse Yahoo v8 chart JSON into an OHLCV frame indexed by naive IST timestamps."""
    chart = js.get("chart") or {}
    if chart.get("error"):
        raise FeedError(f"yahoo error: {chart['error']}")
    res = (chart.get("result") or [None])[0]
    if not res or not res.get("timestamp"):
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    q = res["indicators"]["quote"][0]
    idx = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert("Asia/Kolkata").tz_localize(None)
    df = pd.DataFrame({"Open": q.get("open"), "High": q.get("high"), "Low": q.get("low"),
                       "Close": q.get("close"), "Volume": q.get("volume")}, index=idx)
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df.astype(float)


class CircuitBreaker:
    def __init__(self, threshold: int = 4, cooldown_sec: int = 180, clock: Callable[[], float] = time.monotonic):
        self.threshold, self.cooldown, self.clock = threshold, cooldown_sec, clock
        self.failures, self.open_until = 0, 0.0

    def allow(self) -> bool:
        return self.clock() >= self.open_until

    def success(self):
        self.failures = 0

    def failure(self):
        self.failures += 1
        if self.failures >= self.threshold:
            self.open_until = self.clock() + self.cooldown
            self.failures = 0


class YahooFeed:
    def __init__(self, symbol: str, timeout: tuple = (5, 10), retries: int = 3, cache_ttl_sec: int = 20,
                 session: Optional[requests.Session] = None):
        self.symbol = symbol
        self.timeout = timeout
        self.retries = retries
        self.cache_ttl = cache_ttl_sec
        self.s = session or requests.Session()
        self.s.headers.update({"User-Agent": UA, "Accept": "application/json"})
        self.breaker = CircuitBreaker()
        self._cache: dict = {}

    # ------------------------------------------------------------------ transport
    def _chart(self, interval: str, rng: str) -> pd.DataFrame:
        key = (interval, rng)
        hit = self._cache.get(key)
        if hit and time.monotonic() - hit[0] < self.cache_ttl:
            return hit[1]
        if not self.breaker.allow():
            raise FeedError("circuit breaker open (recent repeated failures)")
        params = {"interval": interval, "range": rng, "includePrePost": "false"}
        last_err = None
        for attempt in range(self.retries):
            url = (CHART_URL if attempt % 2 == 0 else CHART_URL_ALT).format(symbol=requests.utils.quote(self.symbol))
            try:
                r = self.s.get(url, params=params, timeout=self.timeout)
                if r.status_code == 429:
                    raise FeedError("rate limited (429)")
                r.raise_for_status()
                df = parse_chart_json(r.json())
                self.breaker.success()
                self._cache[key] = (time.monotonic(), df)
                return df
            except Exception as e:  # network, HTTP, JSON
                last_err = e
                time.sleep(min(8.0, (2 ** attempt)) + random.uniform(0, 0.5))
        self.breaker.failure()
        fb = self._fallback(interval, rng)
        if fb is not None and not fb.empty:
            self._cache[key] = (time.monotonic(), fb)
            return fb
        raise FeedError(f"chart {interval}/{rng} failed: {last_err}")

    def _fallback(self, interval: str, rng: str) -> Optional[pd.DataFrame]:
        try:
            import yfinance as yf  # optional dependency
        except Exception:
            return None
        try:
            df = yf.Ticker(self.symbol).history(period=rng, interval=interval, auto_adjust=False, timeout=10)
            if df is None or df.empty:
                return None
            idx = df.index
            idx = idx.tz_convert("Asia/Kolkata").tz_localize(None) if idx.tz is not None else idx
            df = df.set_axis(idx)[["Open", "High", "Low", "Close", "Volume"]].astype(float)
            return df
        except Exception as e:
            log.warning("yfinance fallback failed: %s", e)
            return None

    # ------------------------------------------------------------------ public
    def intraday(self) -> pd.DataFrame:
        return self._chart("1m", "1d")

    def daily(self, rng: str = "2y") -> pd.DataFrame:
        d = self._chart("1d", rng)
        d.index = d.index.normalize()
        return d[~d.index.duplicated(keep="last")]

    def snapshot(self, now: datetime, session_open_min: int, cutoff_min: Optional[int] = None) -> Snapshot:
        """Build the strategy snapshot.  cutoff_min: only intraday bars that START before this
        minute are used (15:25 -> identical to the backtest)."""
        today = now.astimezone(IST).replace(tzinfo=None).date()
        intr = self.intraday()
        intr = intr[intr.index.date == today]
        mins = intr.index.hour * 60 + intr.index.minute
        intr = intr[mins >= session_open_min]
        if cutoff_min is not None:
            intr = intr[(intr.index.hour * 60 + intr.index.minute) < cutoff_min]
        daily = self.daily()
        daily = daily[daily.index.date < today][["Open", "High", "Low", "Close"]]
        validate_daily(daily)
        if intr.empty:
            return Snapshot(daily=daily, today=None, last_price=None, as_of=None)
        tb = pd.Series({"Open": intr.Open.iloc[0], "High": intr.High.max(), "Low": intr.Low.min(),
                        "Close": intr.Close.iloc[-1]}, name=pd.Timestamp(today))
        validate_today(tb, daily)
        return Snapshot(daily=daily, today=tb, last_price=float(intr.Close.iloc[-1]),
                        as_of=intr.index[-1].to_pydatetime().replace(tzinfo=IST))


def validate_daily(daily: pd.DataFrame):
    if len(daily) < 70:
        raise FeedError(f"insufficient daily history ({len(daily)} rows)")
    if (daily[["Open", "High", "Low", "Close"]] <= 0).any().any():
        raise FeedError("non-positive prices in daily history")
    if (daily.High < daily.Low).any():
        raise FeedError("High < Low in daily history")


def validate_today(tb: pd.Series, daily: pd.DataFrame):
    prev = float(daily.Close.iloc[-1])
    if tb.High < tb.Low or tb.Close <= 0:
        raise FeedError("invalid intraday bar")
    if abs(tb.Close / prev - 1) > 0.15:
        raise FeedError(f"implausible move vs previous close ({tb.Close} vs {prev}); refusing to trade on it")
