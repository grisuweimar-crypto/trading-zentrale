import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_registry():
    return json.loads((ROOT / "configs" / "external_source_registry_v1.json").read_text(encoding="utf-8"))


def load_universe_contract():
    return json.loads((ROOT / "configs" / "external_universe_coverage_contract_v1.json").read_text(encoding="utf-8"))


def load_revision_probe():
    return json.loads((ROOT / "configs" / "external_revision_source_probe_v1.json").read_text(encoding="utf-8"))


def sources_by_id():
    r = load_registry()
    return {s["source_id"]: s for s in r["sources"]}


def test_registry_is_closed_and_no_source_is_promoted_yet():
    r = load_registry()
    assert r["schema_version"] == "external_source_registry_v1"
    assert r["registry_status"] == "8A_CLOSED_NEXT_PHASE_8C"
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
    assert src["alphavantage_earnings_estimates"]["pit_status"] == "UNSAFE"
    assert src["eodhd_calendar_trends"]["pit_status"] == "UNSAFE"
    assert src["intrinio_zacks_estimates"]["pit_status"] == "SAFE"
    assert src["intrinio_zacks_estimates"]["license_status"] == "RESTRICTED"
    assert src["fmp_analyst_estimates"]["pit_status"] == "UNSAFE"
    assert all(src[k]["promotion_eligible"] is False for k in [
        "alphavantage_earnings_estimates",
        "eodhd_calendar_trends",
        "intrinio_zacks_estimates",
        "fmp_analyst_estimates",
    ])


def test_phase8b_is_deferred_and_8c_is_next():
    decision = load_registry()["8A_decision"]
    assert decision["revisions_pilot_status"] == "DEFERRED_NO_PIT_SAFE_USABLE_SOURCE"
    assert decision["phase8b_status"] == "DEFERRED_NOT_CANCELLED"
    assert decision["next_phase"] == "8C_FUNDAMENTALS_AND_STRUCTURED_CORPORATE_EVENTS"
    assert decision["no_source_is_promoted_yet"] is True


def test_revision_probe_has_no_retrospective_8b_eligible_source():
    p = load_revision_probe()
    assert p["decision"] == "PROCEED_8C"
    assert p["phase8b_status"] == "DEFERRED_NOT_CANCELLED"
    assert not any(c["retrospective_8B_eligible"] for c in p["candidates"])


def test_revision_probe_requires_full_pit_and_access_clearance():
    req = load_revision_probe()["required_for_retrospective_8B"]
    assert req["historical_consensus_vintages"] is True
    assert req["observation_or_publication_timestamp"] is True
    assert req["historical_asof_reconstruction"] is True
    assert req["access_license_cleared"] is True
    assert req["economic_access_cleared"] is True


def test_alpha_vantage_and_eodhd_are_rejected_for_specific_pit_reasons():
    p = {c["source_id"]: c for c in load_revision_probe()["candidates"]}
    assert "NO_ASOF_PARAMETER" in p["alphavantage_earnings_estimates"]["reason_codes"]
    assert "NO_PROVEN_OBSERVATION_TIMESTAMP" in p["alphavantage_earnings_estimates"]["reason_codes"]
    assert "NO_ASOF_PARAMETER" in p["eodhd_calendar_trends"]["reason_codes"]
    assert "DATE_IS_FISCAL_PERIOD_NOT_VINTAGE" in p["eodhd_calendar_trends"]["reason_codes"]


def test_intrinio_is_not_promoted_despite_pit_capability():
    p = {c["source_id"]: c for c in load_revision_probe()["candidates"]}
    intrinio = p["intrinio_zacks_estimates"]
    assert intrinio["historical_asof_reconstruction"] is True
    assert intrinio["access_license_cleared"] is False
    assert intrinio["economic_access_cleared"] is False
    assert intrinio["retrospective_8B_eligible"] is False


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
