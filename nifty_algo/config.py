"""Central configuration. Every tunable lives here; override via environment variables
(prefix NIFTY_ALGO_) or a JSON file passed with --config."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict, fields
from datetime import timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class StrategyParams:
    """NIFTY Trend-Pullback Mean Reversion (TPMR) - validated 2017-2026, see research/."""
    trend_sma: int = 50           # regime: today's 15:25 price must be above SMA(50) of daily closes
    rsi_len: int = 2              # Wilder RSI length
    rsi_entry: float = 10.0       # setup A: RSI(2) < 10
    ibs_entry: float = 0.15       # setup B: internal bar strength < 0.15 (close near day's low)
    atr_len: int = 14
    # exits (evaluated once per day at the decision time on the 15:25 price)
    exit_above_prev_high: bool = True   # primary exit: close > previous day's high
    max_hold_days: int = 10             # time stop (trading days)
    close_stop_atr: float = 0.0         # closing-basis stop in ATRs below entry (0 = off; research: stops hurt MR)
    disaster_stop_atr: float = 8.0      # intraday catastrophic stop (never hit 2017-2026; black-swan insurance only)


@dataclass(frozen=True)
class Config:
    symbol: str = "^NSEI"                 # Yahoo symbol for the signal instrument
    name: str = "NIFTY 50"
    lot_size: int = 65                    # NIFTY futures lot (verify against the current NSE circular)
    lots: int = 1
    cost_pct: float = 0.0004              # round-trip cost+slippage as fraction of notional (used in P&L)
    db_path: str = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "algo_journal.db")
    lock_path: str = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "engine.lock")
    priors_path: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "priors.json")
    # schedule (IST, minutes after midnight)
    session_open_min: int = 9 * 60 + 15
    session_close_min: int = 15 * 60 + 30
    decision_min: int = 15 * 60 + 25      # decision snapshot time
    decision_deadline_min: int = 15 * 60 + 29
    monitor_every_sec: int = 300
    max_data_age_sec: int = 600           # intraday snapshot older than this is considered stale
    # risk governance
    max_consecutive_losses: int = 4       # pause new entries after N consecutive losing trades
    max_drawdown_pts: float = 3000.0      # pause new entries if closed-trade drawdown exceeds this (index points)
    # notifications
    telegram_token: str = ""
    telegram_chat_id: str = ""
    strategy: StrategyParams = field(default_factory=StrategyParams)

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("telegram_token", None)
        return d


def load_config(path: str | None = None) -> Config:
    raw: dict = {}
    if path:
        with open(path) as fh:
            raw = json.load(fh)
    strat_raw = raw.pop("strategy", {}) or {}
    for f in fields(Config):
        env = os.environ.get(f"NIFTY_ALGO_{f.name.upper()}")
        if env is not None and f.name != "strategy":
            raw[f.name] = env
    for f in fields(StrategyParams):
        env = os.environ.get(f"NIFTY_ALGO_STRATEGY_{f.name.upper()}")
        if env is not None:
            strat_raw[f.name] = env
    # secrets fallbacks commonly used by the previous deployment
    raw.setdefault("telegram_token", os.environ.get("TELEGRAM_TOKEN", ""))
    raw.setdefault("telegram_chat_id", os.environ.get("TELEGRAM_CHAT_ID", ""))

    def coerce(cls, data):
        out = {}
        for f in fields(cls):
            if f.name in data and f.name != "strategy":
                v = data[f.name]
                t = type(getattr(cls(), f.name)) if f.default is not None else str
                out[f.name] = t(v) if not isinstance(v, t) else v
                if t is bool and isinstance(v, str):
                    out[f.name] = v.strip().lower() in ("1", "true", "yes", "on")
        return out

    return Config(strategy=StrategyParams(**coerce(StrategyParams, strat_raw)), **coerce(Config, raw))
