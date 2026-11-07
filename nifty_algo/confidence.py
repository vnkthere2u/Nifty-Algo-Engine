"""Calibrated confidence: Beta-binomial posterior win-rate per setup type.

Research finding (research/confidence_wf.py): a feature-based ML score had NO walk-forward
skill over the base rate (Brier 0.195 vs 0.186), so confidence is the honest empirical
probability of the setup type winning, seeded from the backtest (down-weighted) and
updated with every live closed trade.
"""
from __future__ import annotations

import json
import math
import os

PRIOR_WEIGHT = 0.5      # each backtest trade counts as half a live trade
MIN_SETUP_SAMPLES = 15  # below this, fall back to the pooled 'ALL' statistics


def load_priors(path: str) -> dict:
    if not os.path.exists(path):
        return {"ALL": {"wins": 0, "losses": 0}}
    with open(path) as fh:
        return json.load(fh)


def confidence(setup_key: str, priors: dict, live: dict | None = None) -> dict:
    live = live or {}
    src = setup_key
    bt = priors.get(setup_key, {"wins": 0, "losses": 0})
    lv = live.get(setup_key, {"wins": 0, "losses": 0})
    if bt["wins"] + bt["losses"] + lv["wins"] + lv["losses"] < MIN_SETUP_SAMPLES:
        src = "ALL"
        bt = priors.get("ALL", {"wins": 0, "losses": 0})
        lv = live.get("ALL", {"wins": 0, "losses": 0})
    a = 1 + PRIOR_WEIGHT * bt["wins"] + lv["wins"]
    b = 1 + PRIOR_WEIGHT * bt["losses"] + lv["losses"]
    mean = a / (a + b)
    sd = math.sqrt(a * b / ((a + b) ** 2 * (a + b + 1)))
    return {"pct": round(100 * mean, 1), "low90_pct": round(100 * max(0.0, mean - 1.2816 * sd), 1),
            "basis": src, "n_backtest": bt["wins"] + bt["losses"], "n_live": lv["wins"] + lv["losses"]}
