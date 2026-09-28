from __future__ import annotations

import csv
import hashlib
import io
from collections import Counter
from typing import Any, Mapping


SCHEMA_VERSION = "external_evidence_8f_research_domain_v1"


class ExposureDomain8FError(ValueError):
    pass


def _git_blob_sha(text: str) -> str:
    raw = text.encode("utf-8")
    header = f"blob {len(raw)}\0".encode("ascii")
    return hashlib.sha1(header + raw).hexdigest()  # noqa: S324 - Git object identity, not security


def _derive_subjects_from_pinned_universe(
    source: Mapping[str, Any],
    universe_csv_text: str | None,
) -> tuple[list[str], dict[str, Any]]:
    if universe_csv_text is None:
        raise ExposureDomain8FError("pinned subject_source requires universe_csv_text")

    expected_sha = str(source.get("git_blob_sha") or "").strip().lower()
    actual_sha = _git_blob_sha(universe_csv_text)
    if not expected_sha or actual_sha != expected_sha:
        raise ExposureDomain8FError(
            f"research-domain source blob mismatch expected={expected_sha} actual={actual_sha}"
        )
    if str(source.get("filter") or "") != "active == 1 AND asset_type == stock":
        raise ExposureDomain8FError("unsupported research-domain source filter")
    if str(source.get("deduplicate_by") or "") != "symbol":
        raise ExposureDomain8FError("research-domain source must deduplicate by symbol")
    symbol_column = str(source.get("symbol_column") or "")
    if symbol_column != "symbol":
        raise ExposureDomain8FError("research-domain source symbol_column must be symbol")

    reader = csv.DictReader(io.StringIO(universe_csv_text))
    required = {"active", "symbol", "asset_type"}
    if not required.issubset(set(reader.fieldnames or [])):
        raise ExposureDomain8FError("universe source requires active,symbol,asset_type columns")

    selected: list[str] = []
    duplicate_count = 0
    seen: set[str] = set()
    source_row_count = 0
    eligible_row_count = 0
    for row in reader:
        source_row_count += 1
        active = str(row.get("active") or "").strip().lower()
        asset_type = str(row.get("asset_type") or "").strip().lower()
        if active not in {"1", "true", "yes"} or asset_type != "stock":
            continue
        eligible_row_count += 1
        symbol = str(row.get("symbol") or "").strip()
        if not symbol:
            raise ExposureDomain8FError("eligible universe row has empty symbol")
        if symbol in seen:
            duplicate_count += 1
            continue
        seen.add(symbol)
        selected.append(symbol)

    if not selected:
        raise ExposureDomain8FError("pinned universe filter produced empty subject set")

    return sorted(selected), {
        "source_path": str(source.get("path") or ""),
        "source_ref": str(source.get("source_ref") or ""),
        "git_blob_sha": actual_sha,
        "source_row_count": source_row_count,
        "eligible_row_count_before_dedup": eligible_row_count,
        "duplicate_symbol_rows_removed": duplicate_count,
    }


def build_exposure_domain_audit(
    *,
    domain_config: Mapping[str, Any],
    exposure_map: Mapping[str, Any],
    universe_csv_text: str | None = None,
) -> dict[str, Any]:
    if domain_config.get("schema_version") != SCHEMA_VERSION:
        raise ExposureDomain8FError("unexpected Phase 8F research-domain schema_version")

    rules = domain_config.get("rules") or {}
    required_true = {
        "domain_must_be_defined_before_8f_freeze",
        "subject_may_not_enter_domain_because_mapping_exists",
        "mapped_and_unmapped_subjects_both_remain_in_denominator",
        "explicit_unmapped_requires_reason",
    }
    required_false = {
        "market_outcomes_may_be_used_to_define_domain",
        "current_mapping_coverage_may_be_used_to_select_domain",
    }
    wrong_true = sorted(key for key in required_true if rules.get(key) is not True)
    wrong_false = sorted(key for key in required_false if rules.get(key) is not False)
    if wrong_true or wrong_false:
        raise ExposureDomain8FError(
            f"invalid research-domain rules true={wrong_true} false={wrong_false}"
        )

    explicit_subjects = [str(value).strip() for value in domain_config.get("subject_ids") or []]
    source = domain_config.get("subject_source")
    source_meta: dict[str, Any] | None = None
    if source:
        if explicit_subjects:
            raise ExposureDomain8FError("research domain cannot mix subject_source with explicit subject_ids")
        if not isinstance(source, Mapping):
            raise ExposureDomain8FError("subject_source must be an object")
        subjects, source_meta = _derive_subjects_from_pinned_universe(source, universe_csv_text)
    else:
        subjects = explicit_subjects

    if any(not value for value in subjects):
        raise ExposureDomain8FError("research-domain subject_ids must be non-empty strings")
    if len(subjects) != len(set(subjects)):
        raise ExposureDomain8FError("research-domain subject_ids must be unique")
    subject_set = set(subjects)

    explicit_unmapped_rows = domain_config.get("explicit_unmapped") or []
    explicit_unmapped: dict[str, str] = {}
    for row in explicit_unmapped_rows:
        subject_id = str(row.get("subject_id") or "").strip()
        reason = str(row.get("reason") or "").strip()
        if not subject_id or not reason:
            raise ExposureDomain8FError("explicit_unmapped rows require subject_id and reason")
        if subject_id not in subject_set:
            raise ExposureDomain8FError(
                f"explicit_unmapped subject is outside research domain: {subject_id}"
            )
        if subject_id in explicit_unmapped:
            raise ExposureDomain8FError(f"duplicate explicit_unmapped subject: {subject_id}")
        explicit_unmapped[subject_id] = reason

    active_mappings = [
        row
        for row in exposure_map.get("mappings") or []
        if str(row.get("review_status") or "").upper() == "ACTIVE"
    ]
    mapped_subjects = {
        str(row.get("subject_id") or "").strip()
        for row in active_mappings
        if str(row.get("subject_id") or "").strip() in subject_set
    }

    overlap = mapped_subjects & set(explicit_unmapped)
    if overlap:
        raise ExposureDomain8FError(
            f"subject cannot be both mapped and explicit_unmapped: {sorted(overlap)}"
        )

    unaccounted = subject_set - mapped_subjects - set(explicit_unmapped)
    factor_counts = Counter(
        str(row.get("factor_id") or "")
        for row in active_mappings
        if str(row.get("subject_id") or "").strip() in mapped_subjects
    )

    return {
        "schema_version": "external_evidence_8f_exposure_domain_audit_v1",
        "phase": "8F_macro_exposure_context",
        "domain_version": str(domain_config.get("domain_version") or ""),
        "domain_status": str(domain_config.get("status") or ""),
        "domain_subject_count": len(subjects),
        "mapped_subject_count": len(mapped_subjects),
        "explicit_unmapped_subject_count": len(explicit_unmapped),
        "unaccounted_subject_count": len(unaccounted),
        "mapped_subject_ids": sorted(mapped_subjects),
        "explicit_unmapped_subjects": [
            {"subject_id": subject_id, "reason": explicit_unmapped[subject_id]}
            for subject_id in sorted(explicit_unmapped)
        ],
        "unaccounted_subject_ids": sorted(unaccounted),
        "active_mapping_factor_counts": dict(sorted(factor_counts.items())),
        "source_snapshot": source_meta,
        "market_outcomes_read": False,
        "guards": {
            "domain_selected_from_mapping_coverage": False,
            "domain_pinned_to_pre8f_source_blob": source_meta is not None,
            "unmapped_subjects_silently_dropped": False,
            "market_outcomes_read": False,
            "phase7_integration_enabled": False,
        },
    }
