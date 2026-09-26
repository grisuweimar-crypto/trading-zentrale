from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

SCHEMA_VERSION = "external_evidence_8e_issuer_primary_release_v1"


class IssuerPrimaryRelease8EError(ValueError):
    pass


def _aware(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IssuerPrimaryRelease8EError(f"invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IssuerPrimaryRelease8EError(f"{field} must be timezone-aware")
    return parsed


def _hash(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_issuer_primary_release_evidence(
    *, records: Iterable[Mapping[str, Any]], config: Mapping[str, Any]
) -> dict[str, Any]:
    if config.get("schema_version") != SCHEMA_VERSION:
        raise IssuerPrimaryRelease8EError("unsupported issuer-release config")
    hard = config.get("hard_boundaries") or {}
    if hard.get("market_outcomes_may_be_read") is not False:
        raise IssuerPrimaryRelease8EError("issuer-release adapter must remain outcome-blind")
    allowed = {str(v).upper() for v in config.get("supported_event_types") or []}
    denied = {str(v).upper() for v in config.get("explicitly_not_supported") or []}
    rows: list[dict[str, Any]] = []

    for raw in records:
        event_type = str(raw.get("event_type") or "").strip().upper()
        if event_type in denied:
            raise IssuerPrimaryRelease8EError(f"event type remains blocked by Phase 8C boundary: {event_type}")
        if event_type not in allowed:
            raise IssuerPrimaryRelease8EError(f"unsupported issuer event type: {event_type}")
        if raw.get("operator_verified_explicit_event_type") is not True:
            raise IssuerPrimaryRelease8EError("explicit operator-verified event type is required")

        issuer_id = str(raw.get("issuer_stable_id") or "").strip()
        source_event_id = str(raw.get("source_event_id") or "").strip()
        source_url = str(raw.get("source_url") or "").strip()
        event_state = str(raw.get("event_state") or "").strip().upper()
        authority_scope = str(raw.get("authority_scope") or "ISSUER_STATEMENT").strip()
        if not all((issuer_id, source_event_id, source_url, event_state)):
            raise IssuerPrimaryRelease8EError("issuer_stable_id, source_event_id, source_url and event_state are required")
        parsed = urlparse(source_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise IssuerPrimaryRelease8EError("issuer source_url must be an absolute HTTPS URL")

        ingested_at = _aware(raw.get("ingested_at"), field="ingested_at")
        proof = str(raw.get("public_release_proof_status") or "INGESTION_ONLY").strip().upper()
        historical_proven = raw.get("historical_publication_time_independently_proven") is True
        published_at = None
        if proof == "PROVEN":
            published_at = _aware(raw.get("published_at"), field="published_at")
            if not historical_proven:
                raise IssuerPrimaryRelease8EError("PROVEN publication timestamp requires independent proof")
            valid_from = published_at
        elif proof == "INGESTION_ONLY":
            if historical_proven:
                raise IssuerPrimaryRelease8EError("historical proof flag requires PROVEN publication timestamp")
            valid_from = ingested_at
        else:
            raise IssuerPrimaryRelease8EError(f"unsupported public release proof status: {proof}")

        source_record = raw.get("source_record")
        if not isinstance(source_record, Mapping) or not source_record:
            raise IssuerPrimaryRelease8EError("source_record mapping is required")
        rows.append({
            "canonical_event_key": f"ISSUER_IR:{event_type}:{issuer_id}:{source_event_id}",
            "event_type": event_type,
            "subject_id": issuer_id,
            "source_id": "ISSUER_IR",
            "source_class": "ISSUER_PRIMARY_RELEASE",
            "source_event_id": source_event_id,
            "event_state": event_state,
            "authority_scope": authority_scope,
            "source_url": source_url,
            "published_at": published_at.isoformat() if published_at else None,
            "valid_from": valid_from.isoformat(),
            "ingested_at": ingested_at.isoformat(),
            "content_sha256": _hash(source_record),
            "strict_pit_eligible": True,
            "discovery_only": False,
            "public_release_proof_status": proof,
            "historical_publication_time_independently_proven": historical_proven,
            "source_record": dict(source_record),
            "semantic_status": "OPERATOR_VERIFIED_EXPLICIT_EVENT_CHALLENGER",
            "market_direction": "UNASSIGNED"
        })

    rows.sort(key=lambda row: (row["subject_id"], row["event_type"], row["source_event_id"]))
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8E_B4_issuer_primary_release",
        "status": "PROSPECTIVE_ONLY_ISSUER_PRIMARY_RELEASE_CHALLENGER",
        "row_count": len(rows),
        "rows": rows,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "generic_sentiment_enabled": False,
            "guidance_or_capital_raise_reparsed": False,
            "fuzzy_ticker_mapping_enabled": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False
        }
    }
