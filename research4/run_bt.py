import numpy as np, pandas as pd
from bhadra_bt import *
bh = pd.read_csv(os.path.join(ROOT, "data", "bhadra_periods.csv"), parse_dates=["start", "end"])
inst = {"NIFTY": (load_nifty_1m(), 100), "BANKNIFTY": (load_bank_5m(), 200)}
rows, keep = [], {}
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
for name, (base, tp) in inst.items():
    c15 = candles15(base)
    days = pd.DatetimeIndex(np.unique(base.index.normalize()))
    ev = events(bh[(bh.end >= base.index[0]) & (bh.end <= base.index[-1])], set(days))
    for em in ("close", "next_open"):
        for xm in ("eod", "hold"):
            t = simulate(ev, base, c15, tp, em, xm)
            keep[(name, em, xm)] = t
            rows.append(summarize(t, f"{name} entry={em} exit={xm}"))
    # control: same candle times on random NON-Bhadra trading days (5 draws)
    bh_days = set(ev.day)
    pool = [d for d in days if d not in bh_days]
    rng = np.random.default_rng(7); ctl = []
    for rep in range(5):
        pick = rng.choice(len(pool), size=len(ev), replace=False)
        cev = ev.copy()
        cev["day"] = [pool[i] for i in pick]
        cev["cstart"] = [d + (cs - cs.normalize()) for d, cs in zip(cev.day, ev.cstart)]
        cev["cend"] = cev.cstart + pd.Timedelta(minutes=15)
        ctl.append(simulate(cev, base, c15, tp, "close", "eod"))
    rows.append(summarize(pd.concat(ctl), f"{name} CONTROL random non-Bhadra days (x5)"))
R = pd.DataFrame(rows)
print(R.to_string(index=False))
for name in inst:
    t = keep[(name, "close", "eod")]; tr = t[t.result.isin(["TP", "SL", "EOD"])].copy(); tr["yr"] = tr.day.dt.year
    print(f"\n{name} (close entry, 15:25 exit) by year:")
    print(tr.groupby("yr").agg(n=("net_pts", "size"), tp_pct=("result", lambda x: round(100 * (x == "TP").mean(), 1)),
                               win_pct=("net_pts", lambda x: round(100 * (x > 0).mean(), 1)), net_pts=("net_pts", "sum")).round(1).T.to_string())
    tr.to_csv(os.path.join(ROOT, "data", f"bhadra_trades_{name}.csv"), index=False)
    print(tr[["day", "bhadra_end", "range_candle", "H", "L", "dir", "entry_time", "entry", "sl", "tp", "result", "net_pts"]].tail(5).round(1).to_string(index=False))
