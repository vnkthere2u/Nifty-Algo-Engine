"""Bhadra (Vishti karana) periods from JPL DE421 ephemeris.

Karana = 6 deg step of Moon-Sun elongation (geocentric apparent ecliptic longitudes; the
sidereal ayanamsa cancels in the difference, so tropical longitudes give identical karanas).
Karana number k = floor(elongation / 6) in 0..59:
  k = 0 Kimstughna (fixed); k = 1..56 cycle Bava, Balava, Kaulava, Taitila, Gara, Vanija, VISHTI;
  k = 57-59 Shakuni, Chatushpada, Naga (fixed).  => Vishti for k in {7, 14, ..., 56}.
Times are returned in IST (UTC+05:30), to the second.
"""
import os
import numpy as np
import pandas as pd
import skyfield_data
from skyfield.api import load_file, load
from skyfield.framelib import ecliptic_frame

EPH = load_file(os.path.join(os.path.dirname(skyfield_data.__file__), "data", "de421.bsp"))
TS = load.timescale(builtin=True)
EARTH, SUN, MOON = EPH["earth"], EPH["sun"], EPH["moon"]
VISHTI_K = set(range(7, 57, 7))


def elongation(jd_tt: np.ndarray) -> np.ndarray:
    t = TS.tt_jd(jd_tt)
    e = EARTH.at(t)
    _, ls, _ = e.observe(SUN).apparent().frame_latlon(ecliptic_frame)
    _, lm, _ = e.observe(MOON).apparent().frame_latlon(ecliptic_frame)
    return (lm.degrees - ls.degrees) % 360.0


def _refine(lo, hi, target_k):
    """Vectorised bisection: time where floor(elong/6) first equals target_k (unwrapped)."""
    for _ in range(32):                      # ~ 1 hour / 2^32 -> sub-millisecond
        mid = (lo + hi) / 2
        k = np.floor(elongation(mid) / 6).astype(int)
        after = (k - target_k) % 60 < 30     # reached/passed the target boundary
        hi = np.where(after, mid, hi)
        lo = np.where(after, lo, mid)
    return hi


def vishti_periods(start="2014-12-01", end="2026-12-31") -> pd.DataFrame:
    t0, t1 = TS.utc(*map(int, start.split("-"))), TS.utc(*map(int, end.split("-")))
    grid = np.arange(t0.tt, t1.tt, 1 / 48)   # 30-minute grid (a karana lasts ~ 10-14 h)
    k = np.floor(elongation(grid) / 6).astype(int)
    ch = np.flatnonzero(k[1:] != k[:-1])
    lo, hi, newk = grid[ch], grid[ch + 1], k[ch + 1]
    jd = _refine(lo, hi, newk)
    trans = pd.DataFrame({"jd": jd, "k": newk})
    rows = []
    for i in range(len(trans) - 1):
        if trans.k.iloc[i] in VISHTI_K:
            rows.append((trans.jd.iloc[i], trans.jd.iloc[i + 1]))
    out = pd.DataFrame(rows, columns=["start_jd", "end_jd"])
    for c in ("start", "end"):
        utc = TS.tt_jd(out[f"{c}_jd"].values).utc_datetime()
        out[c] = pd.to_datetime(utc).tz_convert("Asia/Kolkata").tz_localize(None).round("s")
    return out[["start", "end"]]


if __name__ == "__main__":
    v = vishti_periods()
    os.makedirs(os.path.join(os.path.dirname(__file__), "..", "data"), exist_ok=True)
    v.to_csv(os.path.join(os.path.dirname(__file__), "..", "data", "bhadra_periods.csv"), index=False)
    print(len(v), "Bhadra periods;", v.head(3).to_string())
    checks = {"Holika Dahan 2024 (published 09:54-23:13)": "2024-03-24",
              "Raksha Bandhan 2024 (published end 13:30)": "2024-08-19",
              "Raksha Bandhan 2023 (published 10:58-21:01)": "2023-08-30"}
    for lbl, d in checks.items():
        m = v[(v.start.dt.strftime("%Y-%m-%d") == d) | (v.end.dt.strftime("%Y-%m-%d") == d)]
        print(lbl, "->", m.to_string(index=False, header=False))
