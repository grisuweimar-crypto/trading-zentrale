from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Iterable, Mapping


class Phase8CReuseAdapterError(ValueError):
    pass


SAFE_MAPPING = {
    "DIRECTOR_OR_OFFICER_CHANGE": "MANAGEMENT_CHANGE",
}

EXPLICITLY_UNMAPPED = {
    "MATERIAL_DEFINITIVE_AGREEMENT",
    "ACQUISITION_OR_DISPOSITION_COMPLETED",
    "UNREGISTERED_EQUITY_SALE",
    "REGULATION_FD_DISCLOSURE",
    "OTHER_MATERIAL_EVENT",
    "RESULTS_RELEASE",
}


def _aware_datetime(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise Phase8CReuseAdapterError(f"invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Phase8CReuseAdapterError(f"{field} must be timezone-aware")
    return parsed


def _canonical_hash(row: Mapping[str, Any]) -> str:
    raw = json.dumps(dict(row), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def adapt_phase8c_structured_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    adapter_ingested_at: datetime,
) -> dict[str, Any]:
    if adapter_ingested_at.tzinfo is None or adapter_ingested_at.utcoffset() is None:
        raise Phase8CReuseAdapterError("adapter_ingested_at must be timezone-aware")

    evidence: list[dict[str, Any]] = []
    unmapped_counts: dict[str, int] = {}
    skipped_counts: dict[str, int] = {}

    for raw in rows:
        candidate = str(raw.get("candidate_event_type") or "").strip().upper()
        if candidate in EXPLICITLY_UNMAPPED:
            unmapped_counts[candidate] = unmapped_counts.get(candidate, 0) + 1
            continue
        if candidate not in SAFE_MAPPING:
            skipped_counts[candidate or "UNKNOWN"] = skipped_counts.get(candidate or "UNKNOWN", 0) + 1
            continue

        if str(raw.get("direction") or "").upper() != "UNKNOWN":
            raise Phase8CReuseAdapterError("8C source row assigned forbidden market direction")
        if str(raw.get("semantic_state") or "").upper() != "FILING_ITEM_ONLY":
            raise Phase8CReuseAdapterError(
                "B1 accepts only the validated 8C filing-item semantic layer"
            )

        cik = str(raw.get("cik") or "").strip()
        accession = str(raw.get("accession_number") or "").strip()
        if not cik or not accession:
            skipped_counts["MISSING_IDENTITY"] = skipped_counts.get("MISSING_IDENTITY", 0) + 1
            continue

        published_at = _aware_datetime(raw.get("published_at"), field="published_at")
        valid_from = _aware_datetime(raw.get("valid_from"), field="valid_from")
        if valid_from < published_at:
            raise Phase8CReuseAdapterError("8C valid_from cannot precede SEC published_at")

        item_code = str(raw.get("item_code") or "").strip() or "NO_ITEM"
        source_event_id = f"{accession}:{item_code}"
        canonical_event_key = f"sec:{cik}:management_change:{accession}:{item_code}"

        evidence.append(
            {
                "canonical_event_key": canonical_event_key,
                "event_type": SAFE_MAPPING[candidate],
                "subject_id": f"SEC_CIK:{cik}",
                "source_id": "phase8c_sec_structured_events",
                "source_class": "MANDATORY_ISSUER_FILING",
                "source_event_id": source_event_id,
                "event_state": "DISCLOSED",
                "authority_scope": "issuer_mandatory_filing_disclosure",
                "published_at": published_at.isoformat(),
                "valid_from": valid_from.isoformat(),
                "ingested_at": adapter_ingested_at.isoformat(),
                "historical_publication_time_independently_proven": True,
                "public_release_proof_status": "PROVEN",
                "strict_pit_eligible": True,
                "discovery_only": False,
                "content_sha256": _canonical_hash(raw),
                "upstream_phase": "8C_D",
                "upstream_accession_number": accession,
                "upstream_item_code": item_code,
                "event_identity_scope": "ACCESSION_SCOPED_DETERMINISTIC",
            }
        )

    evidence.sort(key=lambda row: (row["valid_from"], row["canonical_event_key"]))
    return {
        "schema_version": "external_evidence_8e_phase8c_reuse_v1",
        "phase": "8E_B1_phase8c_reuse_adapter",
        "status": "OUTCOME_BLIND_CONSERVATIVE_REUSE",
        "row_count": len(evidence),
        "rows": evidence,
        "unmapped_candidate_event_type_counts": dict(sorted(unmapped_counts.items())),
        "skipped_candidate_event_type_counts": dict(sorted(skipped_counts.items())),
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "duplicate_sec_reparse_enabled": False,
            "rich_semantics_inferred_from_8c_metadata": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }
