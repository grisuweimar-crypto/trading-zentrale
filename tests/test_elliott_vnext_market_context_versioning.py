import pandas as pd

from scanner.research.elliott_vnext.market_context import (
    build_context_snapshot,
    build_market_context_history,
    normalize_context_assignments,
    normalize_context_registry,
)


def _registry_versions():
    return pd.DataFrame(
        [
            {
                "context_id": "ctx",
                "context_type": "market",
                "name": "Old proxy",
                "symbol": "OLD",
                "proxy_kind": "official_or_broad_index",
                "source": "source_old",
                "context_quality": "sufficient",
                "price_basis": "raw",
                "valid_from": "2020-01-01",
                "valid_to": "2026-08-31",
            },
            {
                "context_id": "ctx",
                "context_type": "market",
                "name": "New proxy",
                "symbol": "NEW",
                "proxy_kind": "official_or_broad_index",
                "source": "source_new",
                "context_quality": "sufficient",
                "price_basis": "raw",
                "valid_from": "2026-09-01",
                "valid_to": "",
            },
        ]
    )


def _assignment():
    return pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "context_id": "ctx",
                "relationship": "market",
                "source": "classification",
                "assignment_quality": "sufficient",
                "pit_verified": True,
                "valid_from": "2024-01-01",
                "valid_to": "",
            }
        ]
    )


def test_registry_and_assignment_normalization_are_idempotent_with_open_intervals():
    registry = normalize_context_registry(_registry_versions())
    registry_again = normalize_context_registry(registry)
    assignments = normalize_context_assignments(_assignment(), registry_again)
    assignments_again = normalize_context_assignments(assignments, registry_again)
    assert registry_again["valid_to"].isna().sum() == 1
    assert assignments_again["valid_to"].isna().sum() == 1


def test_new_proxy_version_does_not_reuse_old_proxy_history():
    raw = pd.DataFrame(
        [
            {
                "date": "2026-08-31",
                "context_id": "ctx",
                "open": 99.0,
                "high": 101.0,
                "low": 98.0,
                "close": 100.0,
                "volume": 1000,
                "currency": "USD",
            }
        ]
    )
    history = build_market_context_history(raw, _registry_versions())
    snapshot = build_context_snapshot(
        history,
        _assignment(),
        _registry_versions(),
        symbol="AAA",
        as_of="2026-09-02",
    )
    assert len(snapshot) == 1
    assert snapshot.iloc[0]["status"] == "missing_context_history"
    assert pd.isna(snapshot.iloc[0]["context_date"])
