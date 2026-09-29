from __future__ import annotations

from pathlib import Path

import scanner.research.governance.qm_b_alias_listing_boundaries as mod


def fake_base() -> dict:
    return {
        "reconciled_identifiers": [
            {
                "observed_identifier": "AAPL",
                "candidate_instrument_id": "urn:scanner:isin:US0378331005",
                "canonical_isin": "US0378331005",
                "identity_status": "VERIFIED",
                "pit_alias_available_from": "2026-02-16",
            },
            {
                "observed_identifier": "SHOP.TO",
                "candidate_instrument_id": "urn:scanner:isin:CA82509L1076",
                "canonical_isin": "CA82509L1076",
                "identity_status": "PARTIAL",
                "pit_alias_available_from": None,
            },
            {
                "observed_identifier": "BTC-USD",
                "candidate_instrument_id": "candidate:crypto-family:BTC",
                "canonical_isin": None,
                "identity_status": "PARTIAL",
                "pit_alias_available_from": None,
            },
        ]
    }


def fake_supplemental() -> dict:
    return {
        "audited_identifiers": [
            {
                "observed_identifier": "SHOP.TO",
                "upgraded": True,
                "pit_alias_available_from": "2026-09-05",
            }
        ]
    }


def fake_schema_audit() -> dict:
    return {
        "history_cutoff_commit": "cutoff",
        "source_path": "data/inputs/universe_master.csv",
        "commit_count": 3,
        "all_fields": ["active", "symbol", "isin", "asset_type"],
        "explicit_fields": {
            "listing_venue": [],
            "listing_date": [],
            "delisting_date": [],
            "investability": [],
        },
        "explicit_listing_venue_field_present": False,
        "explicit_listing_date_field_present": False,
        "explicit_delisting_date_field_present": False,
        "explicit_investability_field_present": False,
        "headers_by_commit": [],
    }


def fake_observations() -> list[dict]:
    return [
        {"as_of_date": "2026-02-10", "observed_symbol": "AAPL", "source_row_number": 2},
        {"as_of_date": "2026-02-20", "observed_symbol": "AAPL", "source_row_number": 3},
        {"as_of_date": "2026-09-01", "observed_symbol": "SHOP.TO", "source_row_number": 4},
        {"as_of_date": "2026-09-06", "observed_symbol": "SHOP.TO", "source_row_number": 5},
        {"as_of_date": "2026-03-01", "observed_symbol": "BTC-USD", "source_row_number": 6},
        {"as_of_date": "2026-03-01", "observed_symbol": "UNKNOWN", "source_row_number": 7},
    ]


def patch_dependencies(monkeypatch):
    monkeypatch.setattr(mod, "build_identity_reconciliation", lambda *a, **k: fake_base())
    monkeypatch.setattr(mod, "audit_repo_history_upgrades", lambda *a, **k: fake_supplemental())
    monkeypatch.setattr(mod, "audit_universe_schema_history", lambda *a, **k: fake_schema_audit())
    monkeypatch.setattr(mod, "_observation_rows", lambda *a, **k: fake_observations())


def test_observation_classes_separate_pre_boundary_crypto_and_unresolved(monkeypatch, tmp_path: Path):
    patch_dependencies(monkeypatch)
    payload = mod.build_alias_listing_boundary_audit(
        tmp_path / "history.csv",
        tmp_path / "universe.csv",
        repo_root=tmp_path,
    )
    assert payload["scanner_observation_count"] == 6
    assert payload["pit_supported_observation_count"] == 2
    assert payload["pit_unverified_observation_count"] == 4
    assert payload["pre_alias_boundary_non_crypto_observation_count"] == 2
    assert payload["crypto_stable_object_unresolved_observation_count"] == 1
    assert payload["identity_or_boundary_unresolved_observation_count"] == 1
    assert payload["observation_class_counts"] == {
        "CRYPTO_STABLE_OBJECT_UNRESOLVED": 1,
        "IDENTITY_OR_BOUNDARY_UNRESOLVED": 1,
        "PIT_IDENTITY_ALIAS_SUPPORTED": 2,
        "PRE_ALIAS_PIT_BOUNDARY_NON_CRYPTO": 2,
    }


def test_repo_history_upgrade_supplies_boundary_without_back_projection(monkeypatch, tmp_path: Path):
    patch_dependencies(monkeypatch)
    payload = mod.build_alias_listing_boundary_audit(
        tmp_path / "history.csv",
        tmp_path / "universe.csv",
        repo_root=tmp_path,
    )
    rows = {row["observed_identifier"]: row for row in payload["boundary_rows"]}
    shop = rows["SHOP.TO"]
    assert shop["identity_status"] == "VERIFIED"
    assert shop["pit_alias_available_from"] == "2026-09-05"
    assert shop["pit_supported_observation_count"] == 1
    assert shop["pit_unverified_observation_count"] == 1


def test_symbol_suffix_is_hint_only_and_never_listing_venue(monkeypatch, tmp_path: Path):
    patch_dependencies(monkeypatch)
    payload = mod.build_alias_listing_boundary_audit(
        tmp_path / "history.csv",
        tmp_path / "universe.csv",
        repo_root=tmp_path,
    )
    rows = {row["observed_identifier"]: row for row in payload["boundary_rows"]}
    shop = rows["SHOP.TO"]
    assert shop["symbol_namespace_hint"] == "TO"
    assert shop["symbol_namespace_hint_status"] == "HINT_ONLY"
    assert shop["listing_venue"] is None
    assert shop["listing_venue_status"] == "UNKNOWN"
    assert payload["symbol_suffix_interpreted_as_listing_venue"] is False


def test_scanner_presence_never_becomes_listing_or_investability(monkeypatch, tmp_path: Path):
    patch_dependencies(monkeypatch)
    payload = mod.build_alias_listing_boundary_audit(
        tmp_path / "history.csv",
        tmp_path / "universe.csv",
        repo_root=tmp_path,
    )
    for row in payload["boundary_rows"]:
        assert row["listing_date"] is None
        assert row["listing_date_status"] == "UNKNOWN"
        assert row["delisting_date"] is None
        assert row["delisting_status"] == "UNKNOWN"
        assert row["investability_status"] == "UNKNOWN"
        assert row["strict_alias_promoted"] is False
        assert row["strict_membership_promoted"] is False
    assert payload["absence_interpreted_as_delisting"] is False
    assert payload["scanner_presence_interpreted_as_investability"] is False


def test_repo_schema_without_explicit_fields_stays_unknown(monkeypatch, tmp_path: Path):
    patch_dependencies(monkeypatch)
    payload = mod.build_alias_listing_boundary_audit(
        tmp_path / "history.csv",
        tmp_path / "universe.csv",
        repo_root=tmp_path,
    )
    assert payload["listing_venue_evidence_available"] is False
    assert payload["listing_date_evidence_available"] is False
    assert payload["delisting_date_evidence_available"] is False
    assert payload["investability_evidence_available"] is False
    assert payload["strict_instrument_master"] is False
    assert payload["strict_alias_ledger"] is False
    assert payload["strict_membership_ledger"] is False
