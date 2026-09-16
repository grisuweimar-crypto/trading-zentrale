# Historical rank fields rollout

The rank documentation fields are populated from the first productive snapshot
run after this change. No historical values are reconstructed or backfilled.

## Protected input hashes before the change

SHA-256, captured 2026-09-16 before the schema change:

| File | SHA-256 |
| --- | --- |
| `artifacts/snapshots/score_history.csv` | `62c66198d9b1d3bfb84596a890cefa5ee633e22c1eacc0d0a90d4336243af673` |
| `artifacts/snapshots/score_history_recent.csv` | `c46715429760517ce925051e5355ba84d1fb137208b403a7770bc635f92fd6a8` |
| `artifacts/research/history_research.csv` | `b21a1880babe7ad4f45aaf11f7fde1eadb772ad2b04def8e0029b467169f8317` |
| `artifacts/research/history_research_metadata.json` | `cc43ef2ffddc6ced8a58654a89bf5b9f6804bfc9b488fbe567bd39d1005b692a` |

The fields are `rank`, `universe_size`, and `rank_percentile`. The rank is the
existing descending numeric-score rank with `method="min"`; ties share the
best rank. `universe_size` is the number of rows with a numeric score in the
concrete snapshot. `rank_percentile` is `rank / universe_size`.

Recent and research exports preserve these columns and all historical blanks
without inference. Research metadata records the first date and run ID for
rows where all three fields are present.