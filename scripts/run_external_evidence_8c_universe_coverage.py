#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from scanner.research.external_evidence.fundamental_change import (
    build_debt_to_equity,
    build_derived_yoy_changes,
    build_free_cash_flow,
    build_growth_acceleration,
    build_operating_margin,
    build_yoy_changes,
    first_release_rows,
    resolve_debt_values,
)
from scanner.research.external_evidence.fundamental_coverage import measure_concept_coverage
from scanner.research.external_evidence.sec_edgar import build_acceptance_index, companyfacts_rows
from scanner.research.external_evidence.sec_history import (
    assemble_full_submission_history,
    companyfacts_accession_coverage,
    historical_submission_file_specs,
)
from scanner.research.external_evidence.sec_snapshot import (
    load_snapshot_manifest,
    read_json_verified,
    validate_snapshot_bundle,
)
from scanner.research.external_evidence.universe_coverage import aggregate_company_coverage

ROOT = Path(__file__).resolve().parents[1]


def load_contract() -> dict[str, Any]:
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_fundamental_change_v1.json").read_text(
            encoding="utf-8"
        )
    )


def load_scanner_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows or "symbol" not in rows[0]:
        raise ValueError("scanner CSV must contain symbol rows")
    unique: dict[str, dict[str, str]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if symbol and symbol not in unique:
            unique[symbol] = row
    return list(unique.values())


def feature_counts(first_release_facts: list[dict[str, Any]], *, metric_families: dict[str, Any]) -> dict[str, int]:
    revenue_yoy = build_yoy_changes(first_release_facts, metric="revenue", metric_families=metric_families)
    revenue_acceleration = build_growth_acceleration(revenue_yoy)
    margins = build_operating_margin(first_release_facts, metric_families=metric_families)
    margin_changes = build_derived_yoy_changes(
        margins,
        source_feature="operating_margin",
        output_feature="operating_margin_change",
        kind="duration",
    )
    fcf = build_free_cash_flow(first_release_facts, metric_families=metric_families)
    fcf_changes = build_derived_yoy_changes(
        fcf,
        source_feature="free_cash_flow",
        output_feature="free_cash_flow_yoy_change",
        kind="duration",
    )
    debt = resolve_debt_values(first_release_facts, metric_families=metric_families)
    debt_known = [row for row in debt if row.get("status") == "KNOWN"]
    debt_conflicting = [row for row in debt if row.get("status") == "CONFLICTING_SOURCES"]
    debt_to_equity = build_debt_to_equity(debt, first_release_facts, metric_families=metric_families)
    debt_to_equity_known = [row for row in debt_to_equity if row.get("status") == "KNOWN"]
    debt_to_equity_changes = build_derived_yoy_changes(
        debt_to_equity,
        source_feature="debt_to_equity",
        output_feature="debt_to_equity_change",
        kind="instant",
    )
    diluted_eps_yoy = build_yoy_changes(first_release_facts, metric="diluted_eps", metric_families=metric_families)
    return {
        "revenue_yoy_change": len(revenue_yoy),
        "revenue_growth_acceleration": len(revenue_acceleration),
        "operating_margin": len(margins),
        "operating_margin_change": len(margin_changes),
        "free_cash_flow": len(fcf),
        "free_cash_flow_yoy_change": len(fcf_changes),
        "resolved_debt_known": len(debt_known),
        "resolved_debt_conflicting": len(debt_conflicting),
        "debt_to_equity": len(debt_to_equity_known),
        "debt_to_equity_change": len(debt_to_equity_changes),
        "diluted_eps_yoy_change": len(diluted_eps_yoy),
    }


def write_result(
    output_path: Path,
    *,
    scanner_as_of: str | None,
    snapshot_summary: dict[str, Any],
    companies: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "schema_version": "external_evidence_8c_current_universe_coverage_v1",
        "scope": "CURRENT_SCANNER_SNAPSHOT_FEASIBILITY_ONLY_NOT_HISTORICAL_UNIVERSE",
        "scanner_as_of": scanner_as_of,
        "completed": True,
        "transport_mode": "OFFLINE_VERIFIED_SEC_SNAPSHOT",
        "snapshot_summary": snapshot_summary,
        "identity_policy": {
            "current_exact_sec_ticker_match_only": True,
            "fuzzy_name_matching": False,
            "strip_exchange_suffixes": False,
            "historical_identity_claim": False,
            "identity_must_be_verified_by_sec_submissions": True,
        },
        "outcome_research": "NOT_RUN",
        "decision_integration": "NOT_RUN",
        "companies": companies,
        "aggregate": aggregate_company_coverage(companies),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def run(*, scanner_path: Path, snapshot_dir: Path, output_path: Path) -> dict[str, Any]:
    metric_families = load_contract()["metric_families"]
    scanner = load_scanner_rows(scanner_path)
    scanner_as_of = scanner[0].get("as_of") if scanner else None

    snapshot_summary = validate_snapshot_bundle(snapshot_dir)
    manifest = load_snapshot_manifest(snapshot_dir)
    if manifest.get("scanner_as_of") and scanner_as_of and manifest.get("scanner_as_of") != scanner_as_of:
        raise ValueError(
            f"Snapshot/scanner as_of mismatch: snapshot={manifest.get('scanner_as_of')} scanner={scanner_as_of}"
        )

    snapshot_by_symbol = {
        str(row.get("symbol") or "").strip().upper(): row
        for row in manifest["companies"]
        if isinstance(row, dict) and str(row.get("symbol") or "").strip()
    }
    companies: list[dict[str, Any]] = []

    for scanner_row in scanner:
        symbol = str(scanner_row.get("symbol") or "").strip().upper()
        snapshot = snapshot_by_symbol.get(symbol)
        base = {
            "symbol": symbol,
            "scanner_name": scanner_row.get("name"),
            "scanner_currency": scanner_row.get("currency"),
            "scanner_sector": scanner_row.get("sector"),
            "scanner_as_of": scanner_row.get("as_of"),
        }
        if snapshot is None:
            companies.append({
                **base,
                "sec_match_status": "UNKNOWN",
                "processing_status": "NOT_IN_SEC_SNAPSHOT",
                "research_ready": False,
                "reason_codes": ["SYMBOL_NOT_IN_SEC_SNAPSHOT"],
            })
            continue

        identity_status = str(snapshot.get("identity_status") or "UNKNOWN")
        if identity_status != "VERIFIED_BY_SEC_SUBMISSIONS":
            companies.append({
                **base,
                "sec_match_status": "UNKNOWN",
                "cik": snapshot.get("cik"),
                "sec_title": snapshot.get("sec_title"),
                "processing_status": identity_status,
                "research_ready": False,
                "reason_codes": list(snapshot.get("reason_codes") or []),
            })
            continue

        primary = read_json_verified(snapshot_dir, snapshot["submissions_file"])
        historical_payloads: dict[str, dict[str, Any]] = {}
        history_specs_by_name = {
            Path(str(spec.get("path") or "")).name: spec
            for spec in (snapshot.get("history_files") or [])
            if isinstance(spec, dict)
        }
        for required in historical_submission_file_specs(primary):
            spec = history_specs_by_name.get(required["name"])
            if spec is None:
                raise ValueError(f"Snapshot missing referenced history file for {symbol}: {required['name']}")
            historical_payloads[required["name"]] = read_json_verified(snapshot_dir, spec)

        history = assemble_full_submission_history(
            primary,
            historical_payloads=historical_payloads,
            forms=None,
            require_complete=True,
        )
        acceptance_index = build_acceptance_index(history["rows"])
        facts_payload = read_json_verified(snapshot_dir, snapshot["companyfacts_file"])
        fact_rows = companyfacts_rows(facts_payload, acceptance_index=acceptance_index)
        accession_coverage = companyfacts_accession_coverage(fact_rows)
        first_release_facts = first_release_rows(fact_rows)
        concept_coverage = measure_concept_coverage(first_release_facts, metric_families=metric_families)
        features = feature_counts(first_release_facts, metric_families=metric_families)
        mapped = int(concept_coverage.get("mapped_rows") or 0)
        research_ready = bool(
            history["coverage"].get("research_ready")
            and accession_coverage.get("research_ready")
            and mapped > 0
        )
        reasons: list[str] = []
        if not history["coverage"].get("research_ready"):
            reasons.extend(history["coverage"].get("reason_codes") or [])
        if not accession_coverage.get("research_ready"):
            reasons.extend(accession_coverage.get("reason_codes") or [])
        if mapped == 0:
            reasons.append("NO_MAPPED_FUNDAMENTAL_FACTS")

        companies.append({
            **base,
            "sec_match_status": "KNOWN",
            "cik": snapshot.get("cik"),
            "sec_title": snapshot.get("sec_title"),
            "processing_status": "SUCCESS",
            "sec_authority_validation": "SNAPSHOT_COLLECTED_AND_VERIFIED_BY_SEC_SUBMISSIONS",
            "history_coverage": history["coverage"],
            "companyfacts_accession_coverage": accession_coverage,
            "concept_coverage": concept_coverage,
            "feature_counts": features,
            "research_ready": research_ready,
            "reason_codes": sorted(set(reasons)),
        })

    return write_result(
        output_path,
        scanner_as_of=scanner_as_of,
        snapshot_summary=snapshot_summary,
        companies=companies,
    )


def self_test() -> dict[str, Any]:
    return {
        "transport_mode": "OFFLINE_VERIFIED_SEC_SNAPSHOT",
        "live_sec_network_calls": False,
        "outcome_research": "NOT_RUN",
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Phase 8C-C coverage from a verified offline SEC snapshot. No live SEC calls are made."
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--scanner", default=str(ROOT / "artifacts" / "research" / "latest_scanner.csv"))
    parser.add_argument("--snapshot-dir")
    parser.add_argument(
        "--output",
        default=str(ROOT / "artifacts" / "research" / "external_evidence_8c_current_universe_coverage.json"),
    )
    args = parser.parse_args()

    if args.self_test:
        print(json.dumps(self_test(), indent=2, sort_keys=True))
        return 0
    if not args.snapshot_dir:
        parser.error("--snapshot-dir is required; live SEC collection is intentionally separated from coverage analysis")

    result = run(
        scanner_path=Path(args.scanner),
        snapshot_dir=Path(args.snapshot_dir),
        output_path=Path(args.output),
    )
    print(json.dumps(result["aggregate"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
