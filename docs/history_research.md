# Monthly scanner research

The separate `history_research.yml` workflow runs on the fifth day of each
month at **03:17 Europe/Berlin**, using `17 3 5 * *` and native `timezone`.
It can also be dispatched manually on main. The daily scanner is unchanged.

Run `python scripts/generate_history_research.py` from a checkout. Python 3.10+
and IANA timezone data are required (on Windows, install `tzdata`). No scanner
or market-data API is invoked. Tests: `python -m unittest discover -s tests
-p 'test_history_research.py' -v`.

The raw archive is read as UTF-8 CSV strings. All columns, row order, numeric
spellings, blanks, literal NA values, and duplicate observations are retained.
There is no interpolation, aggregation, deduplication, or historical backfill
of symbols. Compact CSV serialization uses LF line endings; substantial size
reduction is deliberately secondary to preserving analytical information.
Forward returns or signals absent from the archive are not computed.

## Completeness policy

The previous calendar month is calculated using the actual Berlin date,
including at UTC month boundaries. The existing daily archive contains weekend
observations, so every calendar day of that month must have observations.
The maximum source date must also be at most three calendar days old.
Missing days and stale input block monthly publication with exit code 1.
Malformed/empty/unreadable input or future dates fail before any export.
This is a conservative date-coverage check, not proof that every symbol or
every run is present: historical universe membership is not inferred.

On a coverage failure, the long-term CSV and metadata can still be rebuilt
from real source observations locally. Metadata explicitly records `blocked`,
the missing dates and the errors; **no monthly CSV is created or overwritten**.
GitHub Actions reports the failure in the step summary and does not commit
any output from that failed run. Existing published files remain unchanged.
Never patch the raw archive to make this check pass.

After a successful check, all fully covered, closed months in the archive
are exported to `artifacts/research/monthly/history_monthly_YYYY-MM.csv`.
Incomplete older months are listed in metadata and remain fully accessible
in the long-term CSV. Existing monthly exports are rebuilt from the source;
files for other months are never deleted. The current partial month is only
included in `history_research.csv`, which always covers the entire source.
`latest_completed_month` means the calendar target, not a completeness claim;
consult `validation.status` and `monthly_exports` before using snapshots.

Metadata includes the source SHA-256 and size, date coverage, missing dates,
row and symbol counts, columns and missing expected columns, version values,
duplicate-key count, and export status. A present but entirely blank version
column yields an empty version list; no version values are invented.

Only `artifacts/research/` is staged by the workflow. SHA-256 checks protect the
full/recent histories, history delta and watchlist. A concurrent update to
main causes a normal push rejection; rerun against the new checkout. There
is no force push or automatic merge of potentially stale research outputs.
