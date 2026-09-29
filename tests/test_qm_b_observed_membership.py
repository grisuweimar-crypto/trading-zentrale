from __future__ import annotations

import csv
from pathlib import Path

import pytest

from scanner.reports.research_views import observed as research_views_observed
from scanner.research.governance.qm_b_observed_membership import (
    ObservedMembershipError,
    analyze_history,
    build_candidates,
    candidate_from_row,
    classify_history_row,
    load_observed_membership_contract,
    scanner_membership_evidence,
    summarize_candidates,
)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_contract_is_research_only_and_forbids_absence_as_out_of_scope():
    contract = load_observed_membership_contract()
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["promotion_rules"]["absence_may_create_out_of_scope"] is False
    assert contract["promotion_rules"]["current_master_match_may_verify_historical_identity"] is False


@pytest.mark.parametrize(
    "row",
    [
        {"observation_type": "", "data_source": ""},
        {"observation_type": "observed_scanner", "data_source": "scanner_run"},
        {"observation_type": "", "data_source": "scanner_run"},
        {"observation_type": "observed_scanner", "data_source": ""},
    ],
)
def test_scanner_predicate_matches_productive_research_views(row):
    evidence, claim, _ = classify_history_row(row)
    assert research_views_observed(row) is True
    assert evidence == "SCANNER_OBSERVED"
    assert claim == "OBSERVED_IN_SCANNER"


@pytest.mark.parametrize(
    "row",
    [
        {"observation_type": "backfill", "data_source": "yfinance"},
        {"observation_type": "market_data", "data_source": ""},
        {"observation_type": "", "data_source": "yfinance"},
        {"observation_type": "price_backfill", "data_source": "price_backfill"},
    ],
)
def test_price_or_market_backfill_is_never_membership_evidence(row):
    evidence, claim, reasons = classify_history_row(row)
    assert research_views_observed(row) is False
    assert evidence == "BACKFILL_DERIVED"
    assert claim == "NOT_MEMBERSHIP_EVIDENCE"
    assert "PRICE_OR_MARKET_BACKFILL" in reasons


def test_unknown_provenance_fails_closed():
    row = {"observation_type": "mystery", "data_source": "vendor_x"}
    evidence, claim, reasons = classify_history_row(row)
    assert evidence == "UNKNOWN_PROVENANCE"
    assert claim == "UNKNOWN"
    assert reasons == ["PROVENANCE_NOT_RECOGNIZED"]


def test_candidate_never_claims_stable_identity():
    candidate = candidate_from_row(
        {
            "date": "2026-01-05",
            "symbol": "AAA",
            "name": "Alpha",
            "observation_type": "observed_scanner",
            "data_source": "scanner_run",
        },
        source_path="history.csv",
        source_row_number=2,
        source_file_sha256="0" * 64,
    )
    assert candidate["membership_claim"] == "OBSERVED_IN_SCANNER"
    assert candidate["identity_status"] == "UNRESOLVED"
    assert candidate["source_file_sha256"] == "sha256:" + "0" * 64
    assert candidate["source_row_sha256"].startswith("sha256:")


def test_missing_date_or_symbol_is_rejected():
    with pytest.raises(ObservedMembershipError, match="missing_as_of"):
        candidate_from_row(
            {"symbol": "AAA"}, source_path="history.csv", source_row_number=2, source_file_sha256="0" * 64
        )
    with pytest.raises(ObservedMembershipError, match="missing_symbol"):
        candidate_from_row(
            {"date": "2026-01-05"}, source_path="history.csv", source_row_number=2, source_file_sha256="0" * 64
        )


def test_history_analysis_retains_backfill_but_scanner_evidence_filters_it(tmp_path: Path):
    history = tmp_path / "history_analysis.csv"
    write_csv(
        history,
        [
            {
                "date": "2026-01-05",
                "symbol": "AAA",
                "observation_type": "observed_scanner",
                "data_source": "scanner_run",
                "run_id": "run-1",
            },
            {
                "date": "2026-01-05",
                "symbol": "BBB",
                "observation_type": "backfill",
                "data_source": "yfinance",
                "run_id": "",
            },
            {
                "date": "2026-01-05",
                "symbol": "CCC",
                "observation_type": "mystery",
                "data_source": "vendor_x",
                "run_id": "",
            },
        ],
    )
    ledger = build_candidates(history, source_path_label="artifacts/research/history_analysis.csv")
    rows = scanner_membership_evidence(ledger)
    assert [row["observed_symbol"] for row in rows] == ["AAA"]
    summary = summarize_candidates(ledger)
    assert summary["evidence_class_counts"] == {
        "BACKFILL_DERIVED": 1,
        "SCANNER_OBSERVED": 1,
        "UNKNOWN_PROVENANCE": 1,
    }
    assert summary["historical_identity_verified"] is False
    assert summary["absence_interpreted_as_out_of_scope"] is False


def test_legacy_blank_provenance_follows_existing_project_contract(tmp_path: Path):
    history = tmp_path / "history_analysis.csv"
    write_csv(history, [{"date": "2026-01-05", "symbol": "LEGACY", "observation_type": "", "data_source": ""}])
    ledger = build_candidates(history)
    row = ledger["candidates"][0]
    assert row["evidence_class"] == "SCANNER_OBSERVED"
    assert "LEGACY_BLANK_OBSERVATION_TYPE_ACCEPTED_BY_RESEARCH_VIEWS" in row["reason_codes"]
    assert "LEGACY_BLANK_DATA_SOURCE_ACCEPTED_BY_RESEARCH_VIEWS" in row["reason_codes"]


def test_repeated_real_observations_are_retained_not_collapsed(tmp_path: Path):
    history = tmp_path / "history_analysis.csv"
    row = {
        "date": "2026-01-05",
        "symbol": "AAA",
        "observation_type": "observed_scanner",
        "data_source": "scanner_run",
        "run_id": "run-1",
    }
    write_csv(history, [row, row])
    ledger = build_candidates(history)
    assert len(ledger["candidates"]) == 2
    summary = summarize_candidates(ledger)
    assert summary["repeated_scanner_observation_key_count"] == 1
    assert summary["repeated_scanner_observation_keys_sample"][0]["count"] == 2


def test_current_universe_comparison_is_diagnostic_only(tmp_path: Path):
    history = tmp_path / "history_analysis.csv"
    current = tmp_path / "universe_master.csv"
    write_csv(
        history,
        [
            {"date": "2025-01-05", "symbol": "OLD", "observation_type": "observed_scanner", "data_source": "scanner_run"},
            {"date": "2025-01-05", "symbol": "KEEP", "observation_type": "observed_scanner", "data_source": "scanner_run"},
        ],
    )
    with current.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["active", "symbol", "isin"])
        writer.writeheader()
        writer.writerow({"active": "1", "symbol": "KEEP", "isin": "US0000000001"})
        writer.writerow({"active": "1", "symbol": "NEW", "isin": "US0000000002"})
    _, summary = analyze_history(history, current_universe_path=current)
    assert summary["historical_observed_missing_from_current_master"] == ["OLD"]
    assert summary["current_master_never_scanner_observed"] == ["NEW"]
    assert "review triggers only" in summary["diagnostic_interpretation"]
    assert summary["historical_identity_verified"] is False


def test_no_history_row_can_create_negative_membership_claim(tmp_path: Path):
    history = tmp_path / "history_analysis.csv"
    write_csv(
        history,
        [{"date": "2026-01-05", "symbol": "AAA", "observation_type": "observed_scanner", "data_source": "scanner_run"}],
    )
    ledger = build_candidates(history)
    claims = {row["membership_claim"] for row in ledger["candidates"]}
    assert "OUT_OF_SCOPE" not in claims
    assert "DELISTED" not in claims
    assert ledger["absence_interpreted_as_out_of_scope"] is False
