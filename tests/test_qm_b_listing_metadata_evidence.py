from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from scanner.research.governance.qm_b_listing_metadata_evidence import (
    ListingMetadataEvidenceError,
    assess_listing_metadata_sources,
    load_listing_metadata_contract,
    validate_listing_metadata_contract,
)


ROOT = Path(__file__).resolve().parents[1]


def contract() -> dict:
    return json.loads((ROOT / "configs" / "qm_b_listing_metadata_sources_v1.json").read_text(encoding="utf-8"))


def test_real_contract_loads_and_keeps_production_disabled():
    payload = load_listing_metadata_contract()
    assert payload["schema_version"] == "qm_b_listing_metadata_sources_v1"
    assert payload["research_only"] is True
    assert payload["productive_integration_enabled"] is False
    assert payload["assessment"]["strict_listing_ledger_status"] == "BLOCKED_SOURCE_GAP"


def test_source_assessment_exact_matrix():
    result = assess_listing_metadata_sources(contract())
    assert result["source_count"] == 8
    assert result["source_class_counts"] == {
        "COMMERCIAL_VENDOR": 2,
        "IDENTIFIER_SERVICE": 1,
        "MARKET_REFERENCE_AUTHORITY": 1,
        "OFFICIAL_EXCHANGE": 2,
        "REGULATOR": 2,
    }
    assert result["pit_status_counts"] == {"PARTIAL": 2, "SAFE": 4, "UNSAFE": 2}
    assert result["access_status_counts"] == {"LIMITED": 2, "OPEN": 5, "PAID_RESTRICTED": 1}
    assert result["license_status_counts"] == {"RESTRICTED": 1, "REVIEW_REQUIRED": 5, "USABLE": 2}
    assert result["blocked_required_fields"] == ["venue_assignment", "listing_start", "listing_end"]
    assert result["strict_listing_ledger_ready"] is False
    assert result["global_strict_listing_source_available_and_cleared"] is False


def test_esma_firds_is_regionally_strict_ready_without_closing_global_gap():
    result = assess_listing_metadata_sources(contract())
    rows = {row["source_id"]: row for row in result["source_rows"]}
    esma = rows["esma_firds"]
    assert esma["pit_status"] == "SAFE"
    assert esma["license_status"] == "USABLE"
    assert esma["promotion_eligible"] is True
    assert esma["global_scope"] is False
    assert esma["strict_ready_fields"] == ["listing_end", "listing_start", "venue_assignment"]
    assert result["strict_ready_sources_by_field"]["venue_assignment"] == ["esma_firds"]
    assert result["strict_ready_sources_by_field"]["listing_start"] == ["esma_firds"]
    assert result["strict_ready_sources_by_field"]["listing_end"] == ["esma_firds"]
    assert all(
        not result["global_strict_ready_sources_by_field"][field]
        for field in ("venue_assignment", "listing_start", "listing_end")
    )
    assert result["regional_strict_ready_source_ids"] == ["esma_firds"]
    assert result["blocked_required_fields"] == ["venue_assignment", "listing_start", "listing_end"]
    assert result["strict_listing_ledger_ready"] is False


def test_prospective_and_event_date_challengers_are_separate():
    result = assess_listing_metadata_sources(contract())
    assert result["prospective_collection_sources"] == ["nasdaq_symbol_directory"]
    assert result["event_date_challenger_sources"] == [
        "eodhd_exchange_symbol_delisted",
        "fmp_delisted_companies",
    ]


def test_market_venue_reference_does_not_become_security_assignment():
    result = assess_listing_metadata_sources(contract())
    iso = next(row for row in result["source_rows"] if row["source_id"] == "iso10383_mic")
    assert iso["capabilities"]["venue_master"] == "PIT_SAFE"
    assert iso["capabilities"]["venue_assignment"] == "NOT_SUPPORTED"
    assert iso["strict_ready_fields"] == []


def test_current_mapping_sources_cannot_back_project_listing_state():
    result = assess_listing_metadata_sources(contract())
    rows = {row["source_id"]: row for row in result["source_rows"]}
    assert rows["sec_company_tickers_exchange"]["capabilities"]["venue_assignment"] == "CURRENT_ONLY"
    assert rows["openfigi_mapping"]["capabilities"]["venue_assignment"] == "CURRENT_ONLY"
    assert rows["sec_company_tickers_exchange"]["strict_ready_fields"] == []
    assert rows["openfigi_mapping"]["strict_ready_fields"] == []


def test_delisted_vendor_event_dates_do_not_satisfy_strict_pit_by_themselves():
    result = assess_listing_metadata_sources(contract())
    rows = {row["source_id"]: row for row in result["source_rows"]}
    for source_id in ("eodhd_exchange_symbol_delisted", "fmp_delisted_companies"):
        assert "EVENT_DATE_NO_PUBLICATION_VINTAGE" in set(rows[source_id]["capabilities"].values())
        assert rows[source_id]["strict_ready_fields"] == []


def test_nasdaq_daily_list_semantics_are_not_enough_without_access_license_clearance():
    result = assess_listing_metadata_sources(contract())
    daily = next(row for row in result["source_rows"] if row["source_id"] == "nasdaq_daily_list")
    assert daily["pit_status"] == "SAFE"
    assert daily["capabilities"]["listing_start"] == "PIT_SAFE"
    assert daily["capabilities"]["listing_end"] == "PIT_SAFE"
    assert daily["license_status"] == "RESTRICTED"
    assert daily["promotion_eligible"] is False
    assert daily["strict_ready_fields"] == []


def test_project_investability_is_not_an_external_listing_field():
    result = assess_listing_metadata_sources(contract())
    assert result["project_investability_requires_separate_project_rule"] is True
    assert all(
        row["capabilities"]["project_investability"] == "NOT_SUPPORTED"
        for row in result["source_rows"]
    )


def test_current_only_may_never_be_declared_sufficient():
    payload = contract()
    payload["strict_promotion_rule"]["current_only_mapping_is_sufficient"] = True
    with pytest.raises(ListingMetadataEvidenceError, match="current_only_must_not_be_sufficient"):
        validate_listing_metadata_contract(payload)


def test_event_date_without_publication_vintage_may_never_be_declared_sufficient():
    payload = contract()
    payload["strict_promotion_rule"]["event_date_without_publication_vintage_is_sufficient"] = True
    with pytest.raises(ListingMetadataEvidenceError, match="event_date_without_vintage_must_not_be_sufficient"):
        validate_listing_metadata_contract(payload)


def test_unsafe_source_cannot_claim_pit_safe_field():
    payload = contract()
    bad = deepcopy(payload)
    sec = next(source for source in bad["sources"] if source["source_id"] == "sec_company_tickers_exchange")
    sec["capabilities"]["venue_assignment"] = "PIT_SAFE"
    with pytest.raises(ListingMetadataEvidenceError, match="pit_safe_capability_on_unsafe_source"):
        validate_listing_metadata_contract(bad)


def test_promotion_eligible_source_cannot_keep_blockers():
    payload = contract()
    bad = deepcopy(payload)
    source = bad["sources"][0]
    source["promotion_eligible"] = True
    assert source["promotion_blockers"]
    with pytest.raises(ListingMetadataEvidenceError, match="promotion_eligible_with_blockers"):
        validate_listing_metadata_contract(bad)
