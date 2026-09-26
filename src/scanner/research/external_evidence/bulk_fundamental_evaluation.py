from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .fundamental_change import (
    build_debt_to_equity,
    build_derived_yoy_changes,
    build_free_cash_flow,
    build_growth_acceleration,
    build_operating_margin,
    build_yoy_changes,
    first_release_rows,
    resolve_debt_values,
)
from .fundamental_coverage import measure_concept_coverage
from .sec_edgar import (
    SecEdgarContractError,
    build_acceptance_index,
    companyfacts_rows,
)
from .sec_history import assemble_full_submission_history, companyfacts_accession_coverage

EVALUATION_SCHEMA = "external_evidence_8c_real_fundamental_evaluation_v1"
BULK_SCHEMA = "external_evidence_8c_sec_bulk_bundle_v1"
DIRECT_BULK_MODE = "DIRECT_SEC_BULK_OPERATOR_ATTESTED"


class BulkFundamentalEvaluationError(ValueError):
    """Raised when a local bulk bundle cannot satisfy the real-data evaluation contract."""


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - exact filesystem/JSON error varies
        raise BulkFundamentalEvaluationError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise BulkFundamentalEvaluationError(f"JSON root must be an object: {path}")
    return payload


def _resolve_bundle_path(bundle_dir: Path, relative_path: str) -> Path:
    root = bundle_dir.resolve()
    candidate = (bundle_dir / str(relative_path or "")).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise BulkFundamentalEvaluationError(
            f"bundle path escapes root: {relative_path!r}"
        ) from exc
    return candidate


def _load_verified_json(bundle_dir: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    relative_path = str(spec.get("path") or "").strip()
    expected_sha = str(spec.get("sha256") or "").strip().lower()
    if not relative_path or not expected_sha:
        raise BulkFundamentalEvaluationError("bundle file spec requires path and sha256")
    path = _resolve_bundle_path(bundle_dir, relative_path)
    if not path.is_file():
        raise BulkFundamentalEvaluationError(f"bundle file missing: {relative_path}")
    raw = path.read_bytes()
    actual_sha = _sha256_bytes(raw)
    if actual_sha != expected_sha:
        raise BulkFundamentalEvaluationError(
            f"SHA-256 mismatch for {relative_path}: {actual_sha} != {expected_sha}"
        )
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise BulkFundamentalEvaluationError(
            f"cannot decode bundle JSON: {relative_path}"
        ) from exc
    if not isinstance(payload, dict):
        raise BulkFundamentalEvaluationError(
            f"bundle JSON root must be an object: {relative_path}"
        )
    return payload


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], *, base_fields: Iterable[str] = ()) -> None:
    materialized = [dict(row) for row in rows]
    fields: list[str] = []
    seen: set[str] = set()
    for field in base_fields:
        if field not in seen:
            fields.append(field)
            seen.add(field)
    for row in materialized:
        for field in row:
            if field not in seen:
                fields.append(field)
                seen.add(field)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in materialized:
            serializable = {
                key: (
                    json.dumps(value, sort_keys=True, ensure_ascii=False)
                    if isinstance(value, (list, dict))
                    else value
                )
                for key, value in row.items()
            }
            writer.writerow(serializable)


def _parse_timestamp(value: Any) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if text:
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


def _latest_feature_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        symbol = str(row.get("symbol") or "")
        feature = str(row.get("feature") or "")
        if not symbol or not feature:
            continue
        key = (symbol, feature)
        candidate_key = (
            _parse_timestamp(row.get("valid_from")),
            str(row.get("current_end") or row.get("end") or ""),
            str(row.get("current_accession") or row.get("accession_number") or ""),
        )
        existing = selected.get(key)
        if existing is None:
            selected[key] = row
            continue
        existing_key = (
            _parse_timestamp(existing.get("valid_from")),
            str(existing.get("current_end") or existing.get("end") or ""),
            str(existing.get("current_accession") or existing.get("accession_number") or ""),
        )
        if candidate_key > existing_key:
            selected[key] = row
    return [selected[key] for key in sorted(selected)]


def _enrich_rows(
    rows: Iterable[Mapping[str, Any]], *, symbol: str, sec_title: str | None
) -> list[dict[str, Any]]:
    return [
        {"symbol": symbol, "sec_title": sec_title, **dict(row)}
        for row in rows
    ]


def _feature_bundle(
    fact_rows: list[dict[str, Any]],
    *,
    metric_families: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    first_release = first_release_rows(fact_rows)

    revenue_yoy = build_yoy_changes(
        first_release, metric="revenue", metric_families=metric_families
    )
    revenue_acceleration = build_growth_acceleration(revenue_yoy)

    operating_margin = build_operating_margin(
        first_release, metric_families=metric_families
    )
    operating_margin_change = build_derived_yoy_changes(
        operating_margin,
        source_feature="operating_margin",
        output_feature="operating_margin_change",
        kind="duration",
    )

    free_cash_flow = build_free_cash_flow(
        first_release, metric_families=metric_families
    )
    free_cash_flow_change = build_derived_yoy_changes(
        free_cash_flow,
        source_feature="free_cash_flow",
        output_feature="free_cash_flow_yoy_change",
        kind="duration",
    )

    resolved_debt = resolve_debt_values(
        first_release, metric_families=metric_families
    )
    debt_to_equity = build_debt_to_equity(
        resolved_debt, first_release, metric_families=metric_families
    )
    debt_to_equity_change = build_derived_yoy_changes(
        debt_to_equity,
        source_feature="debt_to_equity",
        output_feature="debt_to_equity_change",
        kind="instant",
    )

    diluted_eps_yoy = build_yoy_changes(
        first_release, metric="diluted_eps", metric_families=metric_families
    )

    features = (
        revenue_yoy
        + revenue_acceleration
        + operating_margin
        + operating_margin_change
        + free_cash_flow
        + free_cash_flow_change
        + debt_to_equity
        + debt_to_equity_change
        + diluted_eps_yoy
    )
    return features, resolved_debt


def evaluate_sec_bulk_fundamentals(
    *,
    bundle_dir: Path,
    config_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Evaluate authoritative local SEC bulk data through the frozen 8C-C engine.

    This function is intentionally outcome-blind. It validates every referenced raw
    JSON file by SHA-256, reconstructs complete submission histories, builds a PIT
    acceptance index, normalizes CompanyFacts, applies FIRST_RELEASE, and only then
    computes the already-frozen non-directional fundamental features.
    """
    bundle_dir = bundle_dir.resolve()
    manifest_path = bundle_dir / "bulk_manifest.json"
    if not manifest_path.is_file():
        raise BulkFundamentalEvaluationError(
            f"bulk manifest missing: {manifest_path}"
        )
    manifest = _load_json_file(manifest_path)
    if manifest.get("schema_version") != BULK_SCHEMA:
        raise BulkFundamentalEvaluationError("unsupported SEC bulk manifest schema")
    if manifest.get("source_mode") != DIRECT_BULK_MODE:
        raise BulkFundamentalEvaluationError(
            "real fundamental evaluation requires direct SEC bulk mode"
        )
    if manifest.get("source_authority") != "U.S. SEC EDGAR":
        raise BulkFundamentalEvaluationError("source authority must be U.S. SEC EDGAR")
    if manifest.get("operator_attested_direct_sec_download") is not True:
        raise BulkFundamentalEvaluationError(
            "direct SEC bulk operator attestation is required"
        )

    config = _load_json_file(config_path)
    if config.get("schema_version") != "external_evidence_8c_fundamental_change_v1":
        raise BulkFundamentalEvaluationError("unexpected fundamental-change config schema")
    if config.get("default_research_basis") != "FIRST_RELEASE":
        raise BulkFundamentalEvaluationError("8C-C real evaluation requires FIRST_RELEASE")
    if config.get("outcome_research_enabled") is not False:
        raise BulkFundamentalEvaluationError("outcome research must remain disabled")
    if config.get("decision_integration_enabled") is not False:
        raise BulkFundamentalEvaluationError("decision integration must remain disabled")
    metric_families = config.get("metric_families") or {}
    if not isinstance(metric_families, Mapping) or not metric_families:
        raise BulkFundamentalEvaluationError("metric_families config is required")

    output_dir.mkdir(parents=True, exist_ok=True)
    company_coverage: list[dict[str, Any]] = []
    feature_rows: list[dict[str, Any]] = []
    debt_diagnostics: list[dict[str, Any]] = []
    metric_coverage_rows: list[dict[str, Any]] = []
    unmapped_concept_rows = Counter()
    unmapped_concept_issuers: dict[str, set[str]] = defaultdict(set)
    custom_taxonomy_rows = Counter()
    custom_taxonomy_issuers: dict[str, set[str]] = defaultdict(set)

    companies = manifest.get("companies") or []
    if not isinstance(companies, list):
        raise BulkFundamentalEvaluationError("manifest companies must be a list")

    for company in companies:
        if not isinstance(company, Mapping):
            raise BulkFundamentalEvaluationError("manifest company entry must be an object")
        symbol = str(company.get("symbol") or "").strip()
        cik = str(company.get("cik") or "").strip()
        sec_title = company.get("sec_title")
        identity_status = str(company.get("identity_status") or "UNKNOWN")
        base_coverage: dict[str, Any] = {
            "symbol": symbol,
            "cik": cik or None,
            "sec_title": sec_title,
            "identity_status": identity_status,
            "history_complete": False,
            "filing_count": 0,
            "fact_row_count": 0,
            "resolved_accession_count": 0,
            "unresolved_accession_count": 0,
            "first_release_fact_row_count": 0,
            "research_ready": False,
            "processing_status": "NOT_ELIGIBLE",
            "reason_codes": list(company.get("reason_codes") or []),
        }

        if identity_status != "VERIFIED_BY_SEC_BULK_SUBMISSIONS":
            base_coverage["reason_codes"].append("IDENTITY_NOT_SEC_BULK_VERIFIED")
            company_coverage.append(base_coverage)
            continue
        if not company.get("submissions_file") or not company.get("companyfacts_file"):
            base_coverage["reason_codes"].append("MISSING_SUBMISSIONS_OR_COMPANYFACTS")
            company_coverage.append(base_coverage)
            continue

        primary = _load_verified_json(bundle_dir, company["submissions_file"])
        historical_payloads: dict[str, dict[str, Any]] = {}
        for spec in company.get("history_files") or []:
            payload = _load_verified_json(bundle_dir, spec)
            historical_payloads[Path(str(spec.get("path") or "")).name] = payload
        facts_payload = _load_verified_json(bundle_dir, company["companyfacts_file"])

        try:
            assembled = assemble_full_submission_history(
                primary,
                historical_payloads=historical_payloads,
                forms=None,
                require_complete=False,
            )
            history_coverage = assembled["coverage"]
            acceptance_index = build_acceptance_index(assembled["rows"])
            normalized_facts = companyfacts_rows(
                facts_payload, acceptance_index=acceptance_index
            )
            fact_coverage = companyfacts_accession_coverage(normalized_facts)
            concept_coverage = measure_concept_coverage(
                normalized_facts, metric_families=metric_families
            )
        except SecEdgarContractError as exc:
            base_coverage["processing_status"] = "CONTRACT_ERROR"
            base_coverage["reason_codes"].append(f"SEC_CONTRACT_ERROR:{exc}")
            company_coverage.append(base_coverage)
            continue

        first_release = first_release_rows(normalized_facts)
        research_ready = bool(
            history_coverage.get("history_complete")
            and fact_coverage.get("research_ready")
        )
        reason_codes = list(base_coverage["reason_codes"])
        reason_codes.extend(history_coverage.get("reason_codes") or [])
        reason_codes.extend(fact_coverage.get("reason_codes") or [])

        coverage_row = {
            **base_coverage,
            "history_complete": bool(history_coverage.get("history_complete")),
            "history_files_expected": history_coverage.get("history_files_expected"),
            "history_files_loaded": history_coverage.get("history_files_loaded"),
            "filing_count": history_coverage.get("filing_count"),
            "unique_accession_count": history_coverage.get("unique_accession_count"),
            "earliest_filing_date": history_coverage.get("earliest_filing_date"),
            "latest_filing_date": history_coverage.get("latest_filing_date"),
            "exact_publication_timestamp_count": history_coverage.get(
                "exact_publication_timestamp_count"
            ),
            "date_only_delayed_count": history_coverage.get("date_only_delayed_count"),
            "unknown_publication_time_count": history_coverage.get(
                "unknown_publication_time_count"
            ),
            "fact_row_count": fact_coverage.get("fact_row_count"),
            "resolved_accession_count": fact_coverage.get("resolved_accession_count"),
            "unresolved_accession_count": fact_coverage.get("unresolved_accession_count"),
            "missing_accession_count": fact_coverage.get("missing_accession_count"),
            "accession_resolution_rate": fact_coverage.get("accession_resolution_rate"),
            "first_release_fact_row_count": len(first_release),
            "pit_usable_fact_rows": concept_coverage.get("pit_usable_rows"),
            "mapped_fact_rows": concept_coverage.get("mapped_rows"),
            "mapped_share_of_usable": concept_coverage.get("mapped_share_of_usable"),
            "research_ready": research_ready,
            "processing_status": "READY" if research_ready else "COVERAGE_BLOCKED",
            "reason_codes": sorted(set(reason_codes)),
        }
        company_coverage.append(coverage_row)

        for metric in sorted(metric_families):
            concept_counts = (concept_coverage.get("metric_concept_counts") or {}).get(metric) or {}
            metric_coverage_rows.append(
                {
                    "symbol": symbol,
                    "cik": cik,
                    "metric": metric,
                    "available": bool(
                        (concept_coverage.get("metric_entity_counts") or {}).get(metric)
                    ),
                    "mapped_row_count": (concept_coverage.get("metric_row_counts") or {}).get(metric, 0),
                    "concept_counts": concept_counts,
                }
            )

        for concept, count in (concept_coverage.get("unmapped_us_gaap_concepts") or {}).items():
            unmapped_concept_rows[str(concept)] += int(count)
            unmapped_concept_issuers[str(concept)].add(cik)
        for taxonomy, count in (concept_coverage.get("custom_taxonomies") or {}).items():
            custom_taxonomy_rows[str(taxonomy)] += int(count)
            custom_taxonomy_issuers[str(taxonomy)].add(cik)

        if not research_ready:
            continue

        company_features, company_debt = _feature_bundle(
            normalized_facts, metric_families=metric_families
        )
        feature_rows.extend(
            _enrich_rows(company_features, symbol=symbol, sec_title=sec_title)
        )
        debt_diagnostics.extend(
            _enrich_rows(company_debt, symbol=symbol, sec_title=sec_title)
        )

    latest_features = _latest_feature_rows(feature_rows)

    feature_counts: dict[str, Any] = {}
    by_feature: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in feature_rows:
        by_feature[str(row.get("feature") or "UNKNOWN")].append(row)
    for feature, rows in sorted(by_feature.items()):
        status_counts = Counter(str(row.get("status") or "UNKNOWN") for row in rows)
        feature_counts[feature] = {
            "observation_count": len(rows),
            "issuer_count": len({str(row.get("cik")) for row in rows if row.get("cik")}),
            "status_counts": dict(sorted(status_counts.items())),
        }

    metric_summary: dict[str, Any] = {}
    for metric in sorted(metric_families):
        rows = [row for row in metric_coverage_rows if row["metric"] == metric]
        metric_summary[metric] = {
            "issuer_count": sum(1 for row in rows if row.get("available")),
            "mapped_row_count": sum(int(row.get("mapped_row_count") or 0) for row in rows),
        }

    direct_verified = sum(
        1
        for company in companies
        if isinstance(company, Mapping)
        and company.get("identity_status") == "VERIFIED_BY_SEC_BULK_SUBMISSIONS"
    )
    with_companyfacts = sum(
        1
        for company in companies
        if isinstance(company, Mapping) and company.get("companyfacts_file")
    )
    ready_count = sum(1 for row in company_coverage if row.get("research_ready"))

    summary = {
        "schema_version": EVALUATION_SCHEMA,
        "phase": "8C_C_real_bulk_fundamental_evaluation",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_bundle": {
            "manifest": str(manifest_path),
            "schema_version": manifest.get("schema_version"),
            "source_mode": manifest.get("source_mode"),
            "source_authority": manifest.get("source_authority"),
            "scanner_symbol_count": (manifest.get("coverage") or {}).get("scanner_symbol_count"),
            "bulk_identity_verified_count": (manifest.get("coverage") or {}).get("sec_bulk_identity_verified_count"),
            "companyfacts_verified_count": (manifest.get("coverage") or {}).get("companyfacts_verified_count"),
            "submissions_archive_sha256": ((manifest.get("transport_provenance") or {}).get("submissions_archive") or {}).get("sha256"),
            "companyfacts_archive_sha256": ((manifest.get("transport_provenance") or {}).get("companyfacts_archive") or {}).get("sha256"),
        },
        "fundamental_config": {
            "path": str(config_path),
            "schema_version": config.get("schema_version"),
            "sha256": _sha256_file(config_path),
            "research_basis": "FIRST_RELEASE",
        },
        "counts": {
            "manifest_company_count": len(companies),
            "direct_verified_company_count": direct_verified,
            "companies_with_companyfacts": with_companyfacts,
            "research_ready_company_count": ready_count,
            "feature_observation_count": len(feature_rows),
            "latest_feature_row_count": len(latest_features),
            "debt_resolution_observation_count": len(debt_diagnostics),
        },
        "metric_coverage": metric_summary,
        "feature_counts": feature_counts,
        "unmapped_us_gaap": {
            "concept_count": len(unmapped_concept_rows),
            "row_count": sum(unmapped_concept_rows.values()),
        },
        "custom_taxonomy": {
            "taxonomy_count": len(custom_taxonomy_rows),
            "row_count": sum(custom_taxonomy_rows.values()),
        },
        "research_guards": {
            "market_outcomes_read": False,
            "direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
        },
        "artifacts": {
            "company_coverage": "company_coverage.csv",
            "metric_coverage": "metric_coverage.csv",
            "feature_observations": "feature_observations.csv",
            "latest_features": "latest_features.csv",
            "debt_resolution": "debt_resolution.csv",
            "unmapped_us_gaap_concepts": "unmapped_us_gaap_concepts.csv",
            "custom_taxonomies": "custom_taxonomies.csv",
        },
    }

    _write_csv(
        output_dir / "company_coverage.csv",
        company_coverage,
        base_fields=("symbol", "cik", "sec_title", "identity_status", "research_ready", "processing_status"),
    )
    _write_csv(
        output_dir / "metric_coverage.csv",
        metric_coverage_rows,
        base_fields=("symbol", "cik", "metric", "available", "mapped_row_count", "concept_counts"),
    )
    _write_csv(
        output_dir / "feature_observations.csv",
        feature_rows,
        base_fields=("symbol", "sec_title", "feature", "cik", "status", "value", "valid_from", "direction"),
    )
    _write_csv(
        output_dir / "latest_features.csv",
        latest_features,
        base_fields=("symbol", "sec_title", "feature", "cik", "status", "value", "valid_from", "direction"),
    )
    _write_csv(
        output_dir / "debt_resolution.csv",
        debt_diagnostics,
        base_fields=("symbol", "sec_title", "feature", "cik", "status", "value", "resolution_mode", "valid_from"),
    )
    _write_csv(
        output_dir / "unmapped_us_gaap_concepts.csv",
        [
            {
                "concept": concept,
                "row_count": count,
                "issuer_count": len(unmapped_concept_issuers.get(concept, set())),
            }
            for concept, count in sorted(
                unmapped_concept_rows.items(), key=lambda item: (-item[1], item[0])
            )
        ],
        base_fields=("concept", "row_count", "issuer_count"),
    )
    _write_csv(
        output_dir / "custom_taxonomies.csv",
        [
            {
                "taxonomy": taxonomy,
                "row_count": count,
                "issuer_count": len(custom_taxonomy_issuers.get(taxonomy, set())),
            }
            for taxonomy, count in sorted(
                custom_taxonomy_rows.items(), key=lambda item: (-item[1], item[0])
            )
        ],
        base_fields=("taxonomy", "row_count", "issuer_count"),
    )
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return summary
