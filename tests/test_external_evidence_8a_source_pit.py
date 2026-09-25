import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_registry():
    return json.loads((ROOT / "configs" / "external_source_registry_v1.json").read_text(encoding="utf-8"))


def load_universe_contract():
    return json.loads((ROOT / "configs" / "external_universe_coverage_contract_v1.json").read_text(encoding="utf-8"))


def sources_by_id():
    r = load_registry()
    return {s["source_id"]: s for s in r["sources"]}


def test_registry_has_expected_schema_and_no_promoted_source_yet():
    r = load_registry()
    assert r["schema_version"] == "external_source_registry_v1"
    assert r["8A_decision"]["no_source_is_promoted_yet"] is True
    assert all(s["promotion_eligible"] is False for s in r["sources"])


def test_registry_contains_all_initial_evidence_families():
    r = load_registry()
    families = {s["family"] for s in r["sources"]}
    assert {
        "fundamentals",
        "revisions",
        "positioning",
        "structured_events",
        "macro_exposure",
    }.issubset(families)


def test_revisions_candidates_do_not_bypass_pit_or_license_gate():
    src = sources_by_id()
    assert src["alphavantage_earnings_estimates"]["pit_status"] == "PARTIAL"
    assert src["eodhd_calendar_trends"]["pit_status"] == "PARTIAL"
    assert src["intrinio_zacks_estimates"]["pit_status"] == "SAFE"
    assert src["intrinio_zacks_estimates"]["license_status"] == "RESTRICTED"
    assert src["fmp_analyst_estimates"]["pit_status"] == "UNSAFE"
    assert src["fmp_analyst_estimates"]["promotion_eligible"] is False


def test_current_estimate_retrojection_is_not_allowed_via_8a_decision():
    r = load_registry()
    decision = r["8A_decision"]
    assert decision["revisions_pilot_status"] == "CANDIDATE_VALIDATION_REQUIRED"
    assert decision["no_source_is_promoted_yet"] is True


def test_sec_companyfacts_requires_versioned_accession_reconstruction():
    s = sources_by_id()["sec_edgar_companyfacts"]
    assert s["pit_status"] == "PARTIAL"
    assert s["restatement_policy"] == "both_versioned"
    notes = " ".join(s["source_quality_notes"]).lower()
    assert "accession" in notes
    assert "restatement" in notes


def test_finra_short_interest_uses_publication_not_settlement_as_valid_from():
    s = sources_by_id()["finra_equity_short_interest"]
    assert s["pit_status"] == "PARTIAL"
    semantics = s["publication_semantics"].lower()
    assert "publication date" in semantics
    assert "never the settlement date" in semantics


def test_alfred_is_vintage_safe_but_not_yet_promoted_macro_evidence():
    s = sources_by_id()["fred_alfred_realtime"]
    assert s["pit_status"] == "SAFE"
    assert "vintage_dates" in s["validated_domain"]
    assert s["promotion_eligible"] is False


def test_asof_universe_contract_blocks_survivorship_shortcuts():
    c = load_universe_contract()
    rules = c["rules"]
    assert rules["current_universe_may_define_historical_universe"] is False
    assert rules["delisted_symbols_may_be_dropped_from_historical_sample"] is False
    assert rules["source_coverage_must_be_evaluated_as_of_date"] is True
    assert rules["missing_external_data_may_be_imputed_neutral"] is False
    assert rules["unknown_membership_may_be_silently_excluded"] is False


def test_asof_ledger_builds_coverage_before_outcomes():
    c = load_universe_contract()
    order = c["dataset_build_order"]
    assert order[-1] == "only_then_join_outcomes"
    assert order.index("attach_family_specific_source_coverage_as_of_date") < order.index("only_then_join_outcomes")
    assert order.index("attach_publication_valid_from") < order.index("only_then_join_outcomes")
