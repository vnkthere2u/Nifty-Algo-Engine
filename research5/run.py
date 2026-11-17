import pandas as pd
from bt import *
pd.set_option("display.width", 250); pd.set_option("display.max_rows", 200)
T = run_all(); T.to_pickle("../data/r5_trades.pkl")
S = table(T); S["pass"] = S.apply(passes, axis=1)
print("=== POOLED (all stocks) per strategy ===")
for st, g in T.groupby("strat"):
    a, b = score(g[g.entry_date < CUT]), score(g[g.entry_date >= CUT])
    print(f"{st:20s} IS n={a['n']:5d} win={a['win']:.1f}% PF={a['pf']:.2f} | OOS n={b['n']:5d} win={b['win']:.1f}% PF={b['pf']:.2f} | avg win {score(g)['avg_win']:.2f}% / loss {score(g)['avg_loss']:.2f}%")
print("\n=== stock x strategy passing (win>=60% & PF>=1.5 in BOTH periods, n>=30, oos>=10) ===")
P = S[S["pass"]].sort_values(["strat", "pf_oos"], ascending=[True, False])
print(P.round(2).to_string(index=False)); print("passes:", len(P), "of", len(S))
# robustness: 2x costs
T2 = run_all(cost=2 * COST); S2 = table(T2); S2["pass2x"] = S2.apply(passes, axis=1)
P = P.merge(S2[["sym", "strat", "pass2x"]], on=["sym", "strat"])
print("\nstill pass at 2x costs:", int(P.pass2x.sum()), "of", len(P))
# null: random entries with identical counts (3 seeds)
nulls = []
for sd in range(3):
    N = table(run_all(null=True, seed=sd)); nulls.append(int(N.apply(passes, axis=1).sum()))
print("random-entry passes per run (same counts):", nulls)
P.to_csv("../data/r5_passes.csv", index=False); S.to_csv("../data/r5_all.csv", index=False)
