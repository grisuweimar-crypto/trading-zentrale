# Scanner_vNext — Clean Release Package (1n-clean)

This ZIP is a **clean repack** of the previously produced "1n" build, made to be:
- reproducible / auditable
- free of runtime caches (no __pycache__, no *.pyc)
- easy to verify against the original base ZIP

## How to verify
- See `docs/diff_base_to_1n_clean.patch` for a unified diff relative to the base ZIP.
- See `docs/release_manifest.json` for SHA256 hashes of all files in this ZIP.

## Intentional changes (high level)
- Disable Telegram by default (feature flag).
- Disclaimer dismiss is session-only (reappears on a new visit).
- Deterministic briefing generation (briefing.json + briefing.txt) + optional AI enhancement (feature flag).
- UI loads briefing text if present; never crashes if missing.
- Help page expanded and modularized.

## Files changed/added (source)
Changed:
- .github/workflows/run_scanner.yml
- scripts/generate_briefing.py
- scripts/test_pipeline.py
- src/scanner/alerts/telegram.py
- src/scanner/app/build_watchlist.py
- src/scanner/ui/generator.py

Added:
- configs/briefing.yaml
- configs/briefing_schema.json
- src/scanner/reports/__init__.py
- src/scanner/reports/briefing.py

## Notes
- The **scoring engine source code** was not modified.
- Runtime outputs under `artifacts/` are included only as example outputs; pipeline regenerates them.
