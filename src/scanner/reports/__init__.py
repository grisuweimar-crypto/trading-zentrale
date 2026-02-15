"""Reports / explainability artifacts.

This package is intentionally **read-only** with respect to scoring:
- It may read existing outputs (CSVs under artifacts/)
- It must never change scores or rankings

Current modules:
- briefing: deterministic + optional AI-enhanced watchlist briefing
"""
