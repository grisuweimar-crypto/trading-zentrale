from __future__ import annotations

import csv
import hashlib
import io
from typing import Any, Mapping


SCHEMA_VERSION = "external_evidence_8f_exposure_review_queue_v1"


class ExposureReviewQueue8FError(ValueError):
    pass


def git_blob_sha(text: str) -> str:
    raw = text.encode("utf-8")
    payload = f"blob {len(raw)}\0".encode("ascii") + raw
    return hashlib.sha1(payload).hexdigest()  # noqa: S324 - Git object identity, not security


def build_exposure_review_queue(
    *,
    domain_config: Mapping[str, Any],
    universe_csv: str,
) -> dict[str, Any]:
    source = domain_config.get("subject_source") or {}
    expected_blob = str(source.get("git_blob_sha") or "").strip()
    if not expected_blob:
        raise ExposureReviewQueue8FError("research domain does not pin a source git_blob_sha")
    actual_blob = git_blob_sha(universe_csv)
    if actual_blob != expected_blob:
        raise ExposureReviewQueue8FError(
            f"universe source blob changed: {actual_blob} != {expected_blob}"
        )

    reader = csv.DictReader(io.StringIO(universe_csv))
    required = {"active", "symbol", "name", "isin", "asset_type", "sector", "industry", "country", "currency"}
    missing = required - set(reader.fieldnames or [])
    if missing:
        raise ExposureReviewQueue8FError(f"universe source missing columns: {sorted(missing)}")

    by_symbol: dict[str, dict[str, str]] = {}
    active_stock_rows = 0
    duplicate_rows = 0
    for raw in reader:
        if str(raw.get("active") or "").strip() != "1":
            continue
        if str(raw.get("asset_type") or "").strip().lower() != "stock":
            continue
        active_stock_rows += 1
        symbol = str(raw.get("symbol") or "").strip()
        if not symbol:
            raise ExposureReviewQueue8FError("active stock row has empty symbol")
        if symbol in by_symbol:
            duplicate_rows += 1
            continue
        by_symbol[symbol] = {key: str(raw.get(key) or "").strip() for key in required}

    if not by_symbol:
        raise ExposureReviewQueue8FError("frozen research domain resolved to zero active stocks")

    queue = []
    for symbol in sorted(by_symbol):
        row = by_symbol[symbol]
        queue.append(
            {
                "subject_id": symbol,
                "name": row["name"],
                "isin": row["isin"],
                "sector": row["sector"],
                "industry": row["industry"],
                "country": row["country"],
                "currency": row["currency"],
                "review_status": "PENDING_DOCUMENTARY_REVIEW",
                "documentary_evidence_required": True,
                "factor_mapping_proposed": False,
                "relationship_class_proposed": False,
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8F_macro_exposure_context",
        "domain_version": str(domain_config.get("domain_version") or ""),
        "source_ref": str(source.get("source_ref") or ""),
        "source_git_blob_sha": expected_blob,
        "status": "OUTCOME_BLIND_DOCUMENTARY_REVIEW_QUEUE",
        "subject_count": len(queue),
        "active_stock_source_rows": active_stock_rows,
        "deduplicated_duplicate_rows": duplicate_rows,
        "subjects": queue,
        "guards": {
            "market_outcomes_read": False,
            "factor_mapping_inferred_from_sector": False,
            "factor_mapping_inferred_from_name": False,
            "factor_mapping_inferred_from_pillar_tags": False,
            "automatic_llm_mapping_used": False,
            "signed_exposure_assigned": False,
            "weight_assigned": False,
        },
    }
