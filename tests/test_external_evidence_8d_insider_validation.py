from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.sec_insider_validation import (
    SecInsiderValidationError,
    evaluate_insider_annotations,
    prepare_insider_validation_package,
    write_validation_package,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs" / "external_evidence_8d_insider_validation_v1.json"


def _row(index: int, code: str, *, status: str = "P_S_HIGH_PRECISION_CANDIDATE") -> dict:
    return {
        "source_authority": "U.S. SEC EDGAR",
        "source_dataset": "Insider Transactions Data Sets",
        "source_quarter": "2026Q2",
        "accession_number": f"0000000001-26-{index:06d}",
        "document_type": "4",
        "issuer_cik": f"{(index % 20) + 1:010d}",
        "issuer_name": f"Issuer {index % 20}",
        "scanner_symbol": f"T{index % 20}",
        "transaction_key": str(index),
        "transaction_date": "2026-04-01",
        "event_time": "2026-04-01",
        "transaction_code": code,
        "acquired_disposed_code": "A" if code == "P" else "D",
        "transaction_shares": 100.0,
        "transaction_price_per_share": 10.0,
        "transaction_value_when_price_known": 1000.0,
        "aff10b5one": False,
        "equity_swap_involved": False,
        "reporting_owners": [{"reporting_owner_cik": "0000009999"}],
        "published_at": "2026-04-01T16:00:00-04:00",
        "valid_from": "2026-04-01T16:00:00-04:00",
        "pit_status": "SAFE",
        "strict_pit_eligible": True,
        "amendment": False,
        "candidate_status": status,
        "reason_codes": [],
        "market_direction": "UNASSIGNED",
    }


def _evidence(path: Path, rows: list[dict], *, outcome_read: bool = False) -> Path:
    payload = {
        "schema_version": "external_evidence_8d_sec_insider_bulk_v1",
        "phase": "8D_B1_B2_SEC_insider_bulk",
        "source_quarter": "2026Q2",
        "source_url": "https://www.sec.gov/files/example.zip",
        "source_zip_sha256": "a" * 64,
        "rows": rows,
        "guards": {
            "exact_accession_join_required": True,
            "exact_issuer_cik_join_required": True,
            "current_ticker_used_as_historical_identity": False,
            "market_outcomes_read": outcome_read,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
            "historical_selection_research_enabled": False,
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_b3_package_is_deterministic_and_stratified(tmp_path: Path) -> None:
    rows = [_row(i, "P") for i in range(1, 31)] + [_row(i + 100, "S") for i in range(1, 31)]
    rows.append(_row(999, "S", status="P_S_10B5_1_DECLARED"))
    evidence = _evidence(tmp_path / "evidence.json", rows)

    first = prepare_insider_validation_package(evidence_path=evidence, config_path=CONFIG)
    second = prepare_insider_validation_package(evidence_path=evidence, config_path=CONFIG)

    assert first["rows"] == second["rows"]
    assert first["guards"]["market_outcomes_read"] is False
    assert first["population"]["candidate_count_by_code"] == {"P": 30, "S": 30}
    assert first["sample"]["candidate_sample_by_code"] == {"P": 30, "S": 30}
    assert first["population"]["quarantine_population_by_status"]["P_S_10B5_1_DECLARED"] == 1


def test_b3_refuses_outcome_contaminated_input(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path / "evidence.json", [_row(1, "P")], outcome_read=True)
    with pytest.raises(SecInsiderValidationError, match="market_outcomes_read"):
        prepare_insider_validation_package(evidence_path=evidence, config_path=CONFIG)


def test_b3_small_real_sample_cannot_pass_promotion(tmp_path: Path) -> None:
    evidence = _evidence(
        tmp_path / "evidence.json",
        [_row(i, "P") for i in range(1, 6)] + [_row(i + 100, "S") for i in range(1, 6)],
    )
    package = prepare_insider_validation_package(evidence_path=evidence, config_path=CONFIG)
    package_path = tmp_path / "package.json"
    annotations_path = tmp_path / "annotations.csv"
    write_validation_package(package, json_path=package_path, annotation_csv_path=annotations_path)

    with annotations_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
        fields = list(rows[0])
    for row in rows:
        row["review_status"] = "REVIEWED"
        row["market_outcomes_seen"] = "FALSE"
        row["truth_label"] = (
            "VALID_P_TRANSACTION" if row["transaction_code"] == "P" else "VALID_S_TRANSACTION"
        )
        for field in fields:
            if field.endswith("_correct"):
                row[field] = "TRUE"
    with annotations_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    result = evaluate_insider_annotations(
        package_path=package_path,
        annotation_csv_path=annotations_path,
        config_path=CONFIG,
    )
    assert result["decision"] == "LOW_COVERAGE_NOT_PROMOTABLE"
    assert result["requirements"]["minimum_labeled_high_precision_candidates"] is False
    assert result["guards"]["market_outcomes_read_by_pipeline"] is False


def test_b3_market_outcome_seen_annotation_blocks_pass(tmp_path: Path) -> None:
    rows = [_row(i, "P") for i in range(1, 31)] + [_row(i + 100, "S") for i in range(1, 31)]
    evidence = _evidence(tmp_path / "evidence.json", rows)
    package = prepare_insider_validation_package(evidence_path=evidence, config_path=CONFIG)
    package_path = tmp_path / "package.json"
    annotations_path = tmp_path / "annotations.csv"
    write_validation_package(package, json_path=package_path, annotation_csv_path=annotations_path)

    with annotations_path.open("r", encoding="utf-8", newline="") as handle:
        audit = list(csv.DictReader(handle))
        fields = list(audit[0])
    for row in audit:
        row["review_status"] = "REVIEWED"
        row["market_outcomes_seen"] = "FALSE"
        row["truth_label"] = (
            "VALID_P_TRANSACTION" if row["transaction_code"] == "P" else "VALID_S_TRANSACTION"
        )
        for field in fields:
            if field.endswith("_correct"):
                row[field] = "TRUE"
    audit[0]["market_outcomes_seen"] = "TRUE"
    with annotations_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(audit)

    result = evaluate_insider_annotations(
        package_path=package_path,
        annotation_csv_path=annotations_path,
        config_path=CONFIG,
    )
    assert result["market_outcome_violation_count"] == 1
    assert result["requirements"]["market_outcome_violation_count"] is False
    assert result["decision"] == "REMAIN_CHALLENGER"
