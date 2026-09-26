from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.bulk_fundamental_evaluation import (
    BulkFundamentalEvaluationError,
    evaluate_sec_bulk_fundamentals,
)


def _write_json(path: Path, payload: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _config(path: Path) -> Path:
    payload = {
        "schema_version": "external_evidence_8c_fundamental_change_v1",
        "default_research_basis": "FIRST_RELEASE",
        "outcome_research_enabled": False,
        "decision_integration_enabled": False,
        "metric_families": {
            "revenue": {
                "kind": "duration",
                "concepts": ["RevenueFromContractWithCustomerExcludingAssessedTax"],
                "allowed_units": ["USD"],
            },
            "operating_income": {"kind": "duration", "concepts": ["OperatingIncomeLoss"]},
            "net_income": {"kind": "duration", "concepts": ["NetIncomeLoss"]},
            "diluted_eps": {"kind": "duration", "concepts": ["EarningsPerShareDiluted"]},
            "operating_cash_flow": {
                "kind": "duration",
                "concepts": ["NetCashProvidedByUsedInOperatingActivities"],
            },
            "capex": {
                "kind": "duration",
                "concepts": ["PaymentsToAcquirePropertyPlantAndEquipment"],
            },
            "cash": {"kind": "instant", "concepts": ["CashAndCashEquivalentsAtCarryingValue"]},
            "assets": {"kind": "instant", "concepts": ["Assets"]},
            "equity": {"kind": "instant", "concepts": ["StockholdersEquity"]},
            "debt": {
                "kind": "instant",
                "concepts": ["LongTermDebt", "LongTermDebtCurrent", "LongTermDebtNoncurrent"],
            },
        },
    }
    _write_json(path, payload)
    return path


def _bundle(tmp_path: Path, *, direct: bool = True) -> Path:
    bundle = tmp_path / "bundle"
    cik = "0000320193"
    accn_2024 = "0000320193-24-000001"
    accn_2025 = "0000320193-25-000001"
    submissions = {
        "cik": 320193,
        "name": "Example Corp",
        "tickers": ["EXM"],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": [accn_2025, accn_2024],
                "form": ["10-K", "10-K"],
                "filingDate": ["2025-02-10", "2024-02-10"],
                "acceptanceDateTime": [
                    "2025-02-10T16:30:00-05:00",
                    "2024-02-10T16:30:00-05:00",
                ],
                "reportDate": ["2024-12-31", "2023-12-31"],
            },
            "files": [],
        },
    }
    companyfacts = {
        "cik": 320193,
        "entityName": "Example Corp",
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "label": "Revenue",
                    "description": "Revenue",
                    "units": {
                        "USD": [
                            {
                                "start": "2023-01-01",
                                "end": "2023-12-31",
                                "val": 100.0,
                                "accn": accn_2024,
                                "filed": "2024-02-10",
                                "form": "10-K",
                                "fy": 2023,
                                "fp": "FY",
                            },
                            {
                                "start": "2024-01-01",
                                "end": "2024-12-31",
                                "val": 120.0,
                                "accn": accn_2025,
                                "filed": "2025-02-10",
                                "form": "10-K",
                                "fy": 2024,
                                "fp": "FY",
                            },
                        ]
                    },
                }
            }
        },
    }
    sub_rel = f"raw/companies/{cik}/submissions.json"
    facts_rel = f"raw/companies/{cik}/companyfacts.json"
    sub_sha = _write_json(bundle / sub_rel, submissions)
    facts_sha = _write_json(bundle / facts_rel, companyfacts)
    manifest = {
        "schema_version": "external_evidence_8c_sec_bulk_bundle_v1",
        "source_mode": (
            "DIRECT_SEC_BULK_OPERATOR_ATTESTED"
            if direct
            else "NON_AUTHORITATIVE_TRANSPORT_FOR_CHALLENGER_ONLY"
        ),
        "source_authority": "U.S. SEC EDGAR" if direct else "NON_AUTHORITATIVE_TRANSPORT",
        "operator_attested_direct_sec_download": direct,
        "transport_provenance": {
            "submissions_archive": {"sha256": "a" * 64},
            "companyfacts_archive": {"sha256": "b" * 64},
        },
        "coverage": {
            "scanner_symbol_count": 1,
            "sec_bulk_identity_verified_count": 1,
            "companyfacts_verified_count": 1,
        },
        "companies": [
            {
                "symbol": "EXM",
                "cik": cik,
                "sec_title": "Example Corp",
                "identity_status": "VERIFIED_BY_SEC_BULK_SUBMISSIONS",
                "reason_codes": [],
                "submissions_file": {"path": sub_rel, "sha256": sub_sha},
                "history_files": [],
                "companyfacts_file": {"path": facts_rel, "sha256": facts_sha},
            }
        ],
    }
    _write_json(bundle / "bulk_manifest.json", manifest)
    return bundle


def test_real_bulk_adapter_builds_pit_first_release_features(tmp_path: Path):
    bundle = _bundle(tmp_path)
    config = _config(tmp_path / "config.json")
    output = tmp_path / "out"

    summary = evaluate_sec_bulk_fundamentals(
        bundle_dir=bundle,
        config_path=config,
        output_dir=output,
    )

    assert summary["counts"]["research_ready_company_count"] == 1
    assert summary["feature_counts"]["revenue_yoy_change"]["observation_count"] == 1
    assert summary["feature_counts"]["revenue_yoy_change"]["issuer_count"] == 1
    assert summary["research_guards"]["market_outcomes_read"] is False
    assert summary["research_guards"]["direction_assigned"] is False
    assert (output / "summary.json").is_file()
    assert (output / "company_coverage.csv").is_file()
    assert (output / "feature_observations.csv").is_file()
    with (output / "feature_observations.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    revenue = next(row for row in rows if row["feature"] == "revenue_yoy_change")
    assert float(revenue["pct_change"]) == pytest.approx(0.2)
    assert float(revenue["absolute_change"]) == pytest.approx(20.0)


def test_real_bulk_adapter_rejects_non_authoritative_transport(tmp_path: Path):
    bundle = _bundle(tmp_path, direct=False)
    config = _config(tmp_path / "config.json")
    with pytest.raises(BulkFundamentalEvaluationError, match="direct SEC bulk mode"):
        evaluate_sec_bulk_fundamentals(
            bundle_dir=bundle,
            config_path=config,
            output_dir=tmp_path / "out",
        )


def test_real_bulk_adapter_rejects_tampered_raw_file(tmp_path: Path):
    bundle = _bundle(tmp_path)
    config = _config(tmp_path / "config.json")
    raw = bundle / "raw" / "companies" / "0000320193" / "companyfacts.json"
    raw.write_text("{}\n", encoding="utf-8")
    with pytest.raises(BulkFundamentalEvaluationError, match="SHA-256 mismatch"):
        evaluate_sec_bulk_fundamentals(
            bundle_dir=bundle,
            config_path=config,
            output_dir=tmp_path / "out",
        )
