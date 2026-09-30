# QM-B Historical Taxonomy / Classification Integrity

## Purpose

This research-only gate prevents present-day taxonomy or classification metadata from being silently projected into historical scanner observations.

The audited taxonomy family is `category`, `sector`, `industry`, `pillar_primary`, `pillar_tags`, `pillar_confidence`, `pillar_reason`, and `bucket_type`.

## Rules

- Present-day taxonomy must not define historical sample membership.
- Present-day taxonomy must not enrich an old row retroactively.
- Missing historical taxonomy stays missing unless a separate versioned PIT source proves the historical value.
- A taxonomy value stored with the contemporaneous scanner observation is historical evidence for that observation.
- A separately versioned assignment with explicit validity intervals and PIT verification may be historical evidence.
- Current taxonomy used only for current display or current-snapshot classification is not historical evidence.

## Repository audit

The scanner searches source, scripts, configs and workflows for files that contain both taxonomy terms and historical-research terms. Co-occurrence is only a candidate; every candidate requires an explicit disposition in `configs/qm_b_historical_taxonomy_integrity_v1.json`.

On the frozen review based on main commit `b4b104c47abeb4d21bd77423ac31358d9849cfe7`, 380 files were scanned and 15 candidates were found, including four high-priority files that also reference current metadata sources. Manual data-flow review found no confirmed current-taxonomy dependency and no current-metadata retrojection into historical samples.

Key distinctions:

- `research_views.py` carries taxonomy from the observed scanner row into append-only history; it does not use today's universe master to fill old rows.
- `daily_research.py` uses taxonomy from the current published row for current classification/presentation, while its historical matcher consumes separately stored historical state.
- `selection_timing.py` can read the sector stored on a historical row as a conservative crypto marker, but does not join current taxonomy to historical rows.
- Elliott market-context research requires versioned, validity-bounded, PIT-verified context assignments.
- Phase 8C universe coverage is explicitly current-snapshot feasibility, not a reconstructed historical universe.

## Fail-closed CI

The gate fails if:

1. a new candidate appears without a disposition;
2. a disposition remains for a candidate that disappeared, forcing explicit review of code changes;
3. any candidate is still `REVIEW_REQUIRED`;
4. a `CURRENT_TAXONOMY_DEPENDENCY` or `CURRENT_METADATA_RETROJECTION` is confirmed.

The package changes no scanner score, historical value, Decision Layer output, portfolio logic or execution behavior.
