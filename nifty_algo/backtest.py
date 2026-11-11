"""Backtester for TPMR using the SAME strategy functions as the live engine.

Usage:
    python -m nifty_algo.backtest --data data/raw [--history full|snapshot] [--write-priors]

Data: NIFTY 1-minute CSVs (Epoch,Timestamp,Open,High,Low,Close[,Volume]); see scripts/fetch_history.py.
Execution model: decision at the 15:25 snapshot; fills at the snapshot price; round-trip cost
`cost_pct` of notional; disaster stop checked against each later day's open (gap) and low.
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os

import numpy as np
import pandas as pd

from .bars import FEATURE_WINDOW, daily_snapshot
from .config import Config, load_config
from .strategy import PositionState, compute_features, disaster_stop_price, evaluate_entry, evaluate_exit


def load_minutes(data_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(data_dir, "*.csv")))
    if not files:
        raise FileNotFoundError(f"no CSV files in {data_dir}; run scripts/fetch_history.py first")
    frames = []
    for f in files:
        d = pd.read_csv(f, usecols=["Timestamp", "Open", "High", "Low", "Close"])
        d["Timestamp"] = pd.to_datetime(d["Timestamp"])
        frames.append(d)
    m = pd.concat(frames).drop_duplicates("Timestamp").set_index("Timestamp").sort_index()
    return m


def run_backtest(m1: pd.DataFrame, cfg: Config, history: str = "snapshot") -> pd.DataFrame:
    p = cfg.strategy
    snap = daily_snapshot(m1, cfg.decision_min)
    full = daily_snapshot(m1, cfg.session_close_min) if history == "full" else snap
    base = full[["Open", "High", "Low", "Close"]]
    # in 'snapshot' mode every row is a 15:25 snapshot, so features can be computed once
    feats_once = compute_features(snap[["Open", "High", "Low", "Close"]], p) if history == "snapshot" else None

    trades, pos, meta = [], None, None
    dates = snap.index
    for i, day in enumerate(dates):
        if history == "snapshot":
            row = feats_once.iloc[i]
        else:
            frame = pd.concat([base.iloc[max(0, i - FEATURE_WINDOW + 1):i], snap.iloc[[i]][["Open", "High", "Low", "Close"]]])
            row = compute_features(frame, p).iloc[-1]
        today = snap.iloc[i]
        hist_len = min(i + 1, FEATURE_WINDOW)

        if pos is not None:
            pos.days_held += 1
            meta["mae"] = min(meta["mae"], today["Low"] - pos.entry_price)
            meta["mfe"] = max(meta["mfe"], today["High"] - pos.entry_price)
            reason, px = None, None
            ds = disaster_stop_price(pos.entry_price, pos.entry_atr, p)
            if ds is not None and today["Open"] <= ds:
                reason, px = "DISASTER STOP (gap)", float(today["Open"])
            elif ds is not None and today["Low"] <= ds:
                reason, px = "DISASTER STOP", float(ds)
            else:
                r = evaluate_exit(row, pos, p)
                if r:
                    reason, px = r, float(today["Close"])
            if reason:
                gross = px - pos.entry_price
                cost = cfg.cost_pct * pos.entry_price
                trades.append(dict(entry_date=pos.entry_date, exit_date=day, entry=pos.entry_price, exit=px,
                                   setup=meta["setup"], reason=reason, days=pos.days_held,
                                   gross_pts=gross, net_pts=gross - cost, mae_pts=meta["mae"], mfe_pts=meta["mfe"],
                                   **{f"f_{k}": v for k, v in meta["features"].items()}))
                pos, meta = None, None
                continue  # no same-day re-entry

        if pos is None:
            dec = evaluate_entry(row, p, hist_len)
            if dec.take:
                pos = PositionState(entry_price=float(today["Close"]), entry_atr=float(row["atr"]),
                                    entry_date=day, days_held=0)
                meta = dict(setup=dec.setup_key, features=dec.features, mae=0.0, mfe=0.0)
    return pd.DataFrame(trades)


def wilson(w: int, n: int, z: float = 1.96):
    if n == 0:
        return (0.0, 0.0)
    ph = w / n
    den = 1 + z * z / n
    c = (ph + z * z / (2 * n)) / den
    h = z * math.sqrt(ph * (1 - ph) / n + z * z / (4 * n * n)) / den
    return (round(100 * (c - h), 1), round(100 * (c + h), 1))


def summarize(tr: pd.DataFrame, cfg: Config) -> dict:
    if tr.empty:
        return {"trades": 0}
    w = tr.net_pts > 0
    eq = tr.net_pts.cumsum()
    yrs = (pd.Timestamp(tr.exit_date.max()) - pd.Timestamp(tr.entry_date.min())).days / 365.25
    out = {
        "trades": int(len(tr)), "trades_per_year": round(len(tr) / yrs, 1),
        "win_rate_pct": round(100 * w.mean(), 1), "win_rate_95ci": wilson(int(w.sum()), len(tr)),
        "profit_factor": round(tr.net_pts[w].sum() / -tr.net_pts[~w].sum(), 2),
        "avg_win_pts": round(tr.net_pts[w].mean(), 1), "avg_loss_pts": round(tr.net_pts[~w].mean(), 1),
        "expectancy_pts": round(tr.net_pts.mean(), 1), "total_pts": round(tr.net_pts.sum(), 1),
        "total_inr_per_lot": round(tr.net_pts.sum() * cfg.lot_size, 0),
        "max_drawdown_pts": round((eq - eq.cummax()).min(), 1),
        "worst_trade_pts": round(tr.net_pts.min(), 1), "avg_days_held": round(tr.days.mean(), 2),
        "exit_reasons": tr.reason.value_counts().to_dict(),
    }
    t = tr.copy()
    t["year"] = pd.to_datetime(t.entry_date).dt.year
    out["by_year"] = {int(y): {"n": int(len(g)), "win_pct": round(100 * (g.net_pts > 0).mean(), 1),
                               "pts": round(g.net_pts.sum(), 1)} for y, g in t.groupby("year")}
    out["by_setup"] = {k: {"n": int(len(g)), "win_pct": round(100 * (g.net_pts > 0).mean(), 1),
                           "pf": round(g.net_pts[g.net_pts > 0].sum() / max(1e-9, -g.net_pts[g.net_pts <= 0].sum()), 2),
                           "pts": round(g.net_pts.sum(), 1)} for k, g in t.groupby("setup")}
    is_ = t[t.year < 2023]; oos = t[t.year >= 2023]
    for lbl, g in (("in_sample_2017_2022", is_), ("out_of_sample_2023_plus", oos)):
        gw = g.net_pts > 0
        out[lbl] = {"n": int(len(g)), "win_pct": round(100 * gw.mean(), 1) if len(g) else None,
                    "pf": round(g.net_pts[gw].sum() / max(1e-9, -g.net_pts[~gw].sum()), 2) if len(g) else None}
    return out


def priors_from_trades(tr: pd.DataFrame) -> dict:
    pri = {"ALL": {"wins": int((tr.net_pts > 0).sum()), "losses": int((tr.net_pts <= 0).sum())}}
    for k, g in tr.groupby("setup"):
        pri[k] = {"wins": int((g.net_pts > 0).sum()), "losses": int((g.net_pts <= 0).sum())}
    return pri


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data/raw")
    ap.add_argument("--config", default=None)
    ap.add_argument("--history", choices=["snapshot", "full"], default="full",
                    help="'full': past days use full-session closes (exactly what the live feed supplies)")
    ap.add_argument("--out", default="data/backtest")
    ap.add_argument("--write-priors", action="store_true")
    a = ap.parse_args()
    cfg = load_config(a.config)
    m1 = load_minutes(a.data)
    tr = run_backtest(m1, cfg, a.history)
    os.makedirs(a.out, exist_ok=True)
    tr.to_csv(os.path.join(a.out, "trades.csv"), index=False)
    s = summarize(tr, cfg)
    with open(os.path.join(a.out, "summary.json"), "w") as fh:
        json.dump(s, fh, indent=2, default=str)
    print(json.dumps(s, indent=2, default=str))
    if a.write_priors:
        with open(cfg.priors_path, "w") as fh:
            json.dump(priors_from_trades(tr), fh, indent=2)
        pkg = os.path.dirname(cfg.priors_path)
        with open(os.path.join(pkg, "backtest_summary.json"), "w") as fh:
            json.dump(s, fh, indent=2, default=str)
        tr[["entry_date", "exit_date", "entry", "exit", "setup", "reason", "days", "net_pts"]].to_csv(
            os.path.join(pkg, "backtest_trades.csv"), index=False)
        print("priors + backtest summary written ->", pkg)


if __name__ == "__main__":
    main()
