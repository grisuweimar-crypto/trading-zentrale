from __future__ import annotations

import csv
from pathlib import Path

import pytest

from scanner.research.governance.qm_b import (
    QMBIntegrityError,
    UniverseIntegrityBundle,
    build_bundle,
    inspect_current_universe_master,
    load_qm_b_contract,
)


def base_parts():
    instruments = [
        {
            "instrument_id": "qmb:sec:alpha",
            "asset_type": "stock",
            "identity_status": "VERIFIED",
            "source_id": "internal_master_review",
        },
        {
            "instrument_id": "qmb:sec:beta",
            "asset_type": "stock",
            "identity_status": "VERIFIED",
            "source_id": "internal_master_review",
        },
    ]
    aliases = [
        {
            "instrument_id": "qmb:sec:alpha",
            "identifier_type": "SYMBOL",
            "identifier_value": "OLD",
            "listing_venue": "XNYS",
            "valid_from": "2020-01-01",
            "valid_to": "2024-01-01",
            "source_id": "exchange_history",
            "pit_verified": True,
            "status": "KNOWN",
        },
        {
            "instrument_id": "qmb:sec:alpha",
            "identifier_type": "SYMBOL",
            "identifier_value": "NEW",
            "listing_venue": "XNYS",
            "valid_from": "2024-01-01",
            "valid_to": None,
            "source_id": "exchange_history",
            "pit_verified": True,
            "status": "KNOWN",
        },
        {
            "instrument_id": "qmb:sec:alpha",
            "identifier_type": "ISIN",
            "identifier_value": "US0000000001",
            "listing_venue": "",
            "valid_from": "2020-01-01",
            "valid_to": None,
            "source_id": "instrument_master",
            "pit_verified": True,
            "status": "KNOWN",
        },
        {
            "instrument_id": "qmb:sec:beta",
            "identifier_type": "SYMBOL",
            "identifier_value": "BETA",
            "listing_venue": "XNAS",
            "valid_from": "2021-01-01",
            "valid_to": None,
            "source_id": "exchange_history",
            "pit_verified": True,
            "status": "KNOWN",
        },
    ]
    memberships = [
        {
            "as_of_date": "2023-06-01",
            "instrument_id": "qmb:sec:alpha",
            "symbol": "OLD",
            "listing_venue": "XNYS",
            "membership_status": "IN_SCOPE",
            "membership_reason": "observed_scanner_history",
            "listing_date": "2020-01-01",
            "delisting_date": None,
            "listing_status": "LISTED",
            "investability_status": "INVESTABLE",
            "scanner_observable": True,
            "source_id": "internal_scanner_history",
            "pit_verified": True,
            "reason_codes": [],
        },
        {
            "as_of_date": "2024-06-01",
            "instrument_id": "qmb:sec:alpha",
            "symbol": "NEW",
            "listing_venue": "XNYS",
            "membership_status": "IN_SCOPE",
            "membership_reason": "observed_scanner_history",
            "listing_date": "2020-01-01",
            "delisting_date": None,
            "listing_status": "LISTED",
            "investability_status": "INVESTABLE",
            "scanner_observable": True,
            "source_id": "internal_scanner_history",
            "pit_verified": True,
            "reason_codes": [],
        },
        {
            "as_of_date": "2020-06-01",
            "instrument_id": "qmb:sec:beta",
            "symbol": "BETA",
            "listing_venue": "XNAS",
            "membership_status": "NOT_YET_LISTED",
            "membership_reason": "listing_history",
            "listing_date": "2021-01-01",
            "delisting_date": None,
            "listing_status": "NOT_YET_LISTED",
            "investability_status": "NOT_INVESTABLE",
            "scanner_observable": False,
            "source_id": "listing_history",
            "pit_verified": True,
            "reason_codes": ["NOT_YET_LISTED"],
        },
    ]
    provider = [
        {
            "as_of_date": "2024-06-01",
            "instrument_id": "qmb:sec:alpha",
            "source_id": "sec_edgar",
            "external_family": "fundamentals",
            "coverage_status": "KNOWN",
            "pit_verified": True,
            "reason_codes": [],
        }
    ]
    outcomes = [
        {
            "as_of_date": "2024-06-10",
            "instrument_id": "qmb:sec:alpha",
            "outcome_id": "fwd_5d_alpha",
            "label_definition_hash": "sha256:label",
            "availability_status": "AVAILABLE",
            "available_from": "2024-06-08",
            "source_id": "price_outcomes",
            "pit_verified": True,
            "reason_codes": [],
        }
    ]
    taxonomy = [
        {
            "instrument_id": "qmb:sec:alpha",
            "taxonomy_type": "SECTOR",
            "taxonomy_value": "Technology",
            "valid_from": "2020-01-01",
            "valid_to": "2024-01-01",
            "source_id": "historical_taxonomy",
            "pit_verified": True,
            "status": "KNOWN",
        },
        {
            "instrument_id": "qmb:sec:alpha",
            "taxonomy_type": "SECTOR",
            "taxonomy_value": "Industrials",
            "valid_from": "2024-01-01",
            "valid_to": None,
            "source_id": "historical_taxonomy",
            "pit_verified": True,
            "status": "KNOWN",
        },
    ]
    return instruments, aliases, memberships, provider, outcomes, taxonomy


def bundle_payload(**overrides):
    instruments, aliases, memberships, provider, outcomes, taxonomy = base_parts()
    params = {
        "bundle_as_of": "2026-09-29",
        "created_at": "2026-09-29T17:00:00Z",
        "instruments": instruments,
        "identifier_aliases": aliases,
        "universe_membership": memberships,
        "provider_coverage": provider,
        "outcome_availability": outcomes,
        "taxonomy_assignments": taxonomy,
        "provenance": {"test": True},
    }
    params.update(overrides)
    return build_bundle(**params)


def test_contract_is_research_only_and_non_productive():
    contract = load_qm_b_contract()
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["sample_audit"]["historical_sample_may_be_filtered_to_current_symbols"] is False


def test_bundle_versions_are_deterministic_and_qm_a_compatible():
    payload = bundle_payload()
    first = UniverseIntegrityBundle(payload).versions()
    second = UniverseIntegrityBundle(payload).versions()
    assert first == second
    assert first["instrument_master_version"].startswith("sha256:")
    assert first["universe_ledger_version"].startswith("sha256:")
    assert first["bundle_hash"].startswith("sha256:")


def test_declared_hash_tampering_is_rejected():
    payload = bundle_payload()
    payload["bundle_hash"] = "sha256:not-the-real-hash"
    with pytest.raises(QMBIntegrityError, match="declared_hash_mismatch:bundle_hash"):
        UniverseIntegrityBundle(payload)


def test_symbol_change_resolves_by_as_of_date():
    bundle = UniverseIntegrityBundle(bundle_payload())
    old = bundle.resolve_identifier(
        identifier_type="SYMBOL", identifier_value="OLD", as_of_date="2023-06-01", listing_venue="XNYS"
    )
    new = bundle.resolve_identifier(
        identifier_type="SYMBOL", identifier_value="NEW", as_of_date="2024-06-01", listing_venue="XNYS"
    )
    wrong = bundle.resolve_identifier(
        identifier_type="SYMBOL", identifier_value="NEW", as_of_date="2023-06-01", listing_venue="XNYS"
    )
    assert old["instrument_id"] == "qmb:sec:alpha"
    assert new["instrument_id"] == "qmb:sec:alpha"
    assert wrong["status"] == "UNKNOWN"


def test_overlapping_identifier_for_different_instruments_fails_closed():
    instruments, aliases, memberships, provider, outcomes, taxonomy = base_parts()
    aliases.append(
        {
            "instrument_id": "qmb:sec:beta",
            "identifier_type": "SYMBOL",
            "identifier_value": "NEW",
            "listing_venue": "XNYS",
            "valid_from": "2025-01-01",
            "valid_to": None,
            "source_id": "bad_mapping",
            "pit_verified": True,
            "status": "KNOWN",
        }
    )
    with pytest.raises(QMBIntegrityError, match="identifier_overlap_across_instruments"):
        bundle_payload(identifier_aliases=aliases)


def test_pit_membership_requires_verified_historical_symbol_alias():
    instruments, aliases, memberships, provider, outcomes, taxonomy = base_parts()
    memberships[0]["symbol"] = "CURRENT_ONLY"
    with pytest.raises(QMBIntegrityError, match="pit_membership_requires_historical_symbol_alias"):
        bundle_payload(universe_membership=memberships)


def test_pit_membership_requires_verified_identity():
    instruments, aliases, memberships, provider, outcomes, taxonomy = base_parts()
    instruments[0]["identity_status"] = "PARTIAL"
    with pytest.raises(QMBIntegrityError, match="pit_membership_requires_verified_identity"):
        bundle_payload(instruments=instruments)


def test_missing_as_of_membership_is_unknown_not_neutral():
    bundle = UniverseIntegrityBundle(bundle_payload())
    result = bundle.membership_as_of(instrument_id="qmb:sec:alpha", as_of_date="2022-01-01")
    assert result["status"] == "UNKNOWN"
    assert result["membership_status"] == "UNKNOWN"
    assert result["reason_codes"] == ["NO_EXPLICIT_AS_OF_MEMBERSHIP_ROW"]


def test_sample_audit_keeps_out_of_scope_and_unknown_rows():
    bundle = UniverseIntegrityBundle(bundle_payload())
    result = bundle.audit_sample(
        [
            {"as_of_date": "2024-06-01", "instrument_id": "qmb:sec:alpha"},
            {"as_of_date": "2020-06-01", "instrument_id": "qmb:sec:beta"},
            {"as_of_date": "2022-01-01", "instrument_id": "qmb:sec:alpha"},
        ]
    )
    assert result["row_count"] == 3
    assert result["counts"] == {"ELIGIBLE": 1, "NOT_ELIGIBLE": 1, "FAIL_CLOSED": 1}
    assert result["fail_closed"] is True


def test_provider_coverage_missing_is_explicit_unknown():
    bundle = UniverseIntegrityBundle(bundle_payload())
    result = bundle.provider_coverage_as_of(
        instrument_id="qmb:sec:alpha",
        as_of_date="2024-06-01",
        source_id="other_vendor",
        external_family="fundamentals",
    )
    assert result["coverage_status"] == "UNKNOWN"
    assert result["reason_codes"] == ["NO_EXPLICIT_PROVIDER_COVERAGE_ROW"]


def test_available_outcome_cannot_be_claimed_before_available_from():
    instruments, aliases, memberships, provider, outcomes, taxonomy = base_parts()
    outcomes[0]["available_from"] = "2024-06-11"
    with pytest.raises(QMBIntegrityError, match="outcome_not_available_as_of_claim"):
        bundle_payload(outcome_availability=outcomes)


def test_taxonomy_is_effective_dated():
    bundle = UniverseIntegrityBundle(bundle_payload())
    old = bundle.taxonomy_as_of(
        instrument_id="qmb:sec:alpha", taxonomy_type="SECTOR", as_of_date="2023-06-01"
    )
    new = bundle.taxonomy_as_of(
        instrument_id="qmb:sec:alpha", taxonomy_type="SECTOR", as_of_date="2024-06-01"
    )
    assert old["taxonomy_value"] == "Technology"
    assert new["taxonomy_value"] == "Industrials"


def test_current_universe_inventory_never_infers_history(tmp_path: Path):
    path = tmp_path / "universe.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["active", "symbol", "isin", "name"])
        writer.writeheader()
        writer.writerow({"active": "1", "symbol": "AAA", "isin": "US0000000001", "name": "A"})
        writer.writerow({"active": "1", "symbol": "AAA", "isin": "US0000000001", "name": "A duplicate"})
        writer.writerow({"active": "1", "symbol": "BBB", "isin": "", "name": "B"})
    result = inspect_current_universe_master(path)
    assert result["scope"] == "CURRENT_STATE_ONLY_NOT_HISTORICAL_MEMBERSHIP"
    assert result["historical_membership_inferred"] is False
    assert result["historical_identity_inferred"] is False
    assert result["duplicate_symbols"]["AAA"] == [2, 3]
    assert result["duplicate_isins"]["US0000000001"] == [2, 3]
    assert result["missing_isin_rows"] == [4]


def test_bundle_rejects_historical_backfill_claim():
    payload = bundle_payload()
    payload["historical_backfill_claim"] = True
    payload.pop("bundle_hash", None)
    payload.pop("instrument_master_version", None)
    payload.pop("universe_ledger_version", None)
    with pytest.raises(QMBIntegrityError, match="historical_backfill_claim_must_be_false"):
        UniverseIntegrityBundle(payload)


def test_in_scope_membership_cannot_claim_delisted_listing_status():
    instruments, aliases, memberships, provider, outcomes, taxonomy = base_parts()
    memberships[0]["listing_status"] = "DELISTED"
    with pytest.raises(QMBIntegrityError, match="in_scope_listing_impossible"):
        bundle_payload(universe_membership=memberships)


def test_known_listing_date_blocks_prelisting_in_scope_claim():
    instruments, aliases, memberships, provider, outcomes, taxonomy = base_parts()
    memberships[0]["listing_date"] = "2023-07-01"
    with pytest.raises(QMBIntegrityError, match="membership_before_listing_date"):
        bundle_payload(universe_membership=memberships)


def test_known_delisting_date_blocks_in_scope_claim():
    instruments, aliases, memberships, provider, outcomes, taxonomy = base_parts()
    memberships[1]["delisting_date"] = "2024-05-01"
    with pytest.raises(QMBIntegrityError, match="in_scope_on_or_after_delisting_date"):
        bundle_payload(universe_membership=memberships)


def test_bundle_hash_binds_created_at():
    payload = bundle_payload()
    payload["created_at"] = "2026-09-29T18:00:00Z"
    with pytest.raises(QMBIntegrityError, match="declared_hash_mismatch:bundle_hash"):
        UniverseIntegrityBundle(payload)
