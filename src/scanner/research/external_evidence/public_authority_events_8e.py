from __future__ import annotations

import hashlib
import json
from datetime import datetime
from urllib.parse import urlparse
from typing import Any, Iterable, Mapping

SCHEMA_VERSION = "external_evidence_8e_public_authority_events_v1"


class PublicAuthority8EError(ValueError):
    pass


def _aware(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise PublicAuthority8EError(f"invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PublicAuthority8EError(f"{field} must be timezone-aware")
    return parsed


def _canonical_hash(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_public_authority_evidence(
    *, records: Iterable[Mapping[str, Any]], config: Mapping[str, Any]
) -> dict[str, Any]:
    if config.get("schema_version") != SCHEMA_VERSION:
        raise PublicAuthority8EError("unsupported public-authority config")
    hard = config.get("hard_boundaries") or {}
    if hard.get("market_outcomes_may_be_read") is not False:
        raise PublicAuthority8EError("adapter must remain outcome-blind")
    providers = config.get("providers") or {}
    rows: list[dict[str, Any]] = []

    for raw in records:
        provider = str(raw.get("provider") or "").strip().upper()
        provider_cfg = providers.get(provider)
        if not isinstance(provider_cfg, Mapping):
            raise PublicAuthority8EError(f"unsupported provider: {provider}")
        event_type = str(raw.get("event_type") or "").strip().upper()
        if event_type not in set(provider_cfg.get("supported_event_types") or []):
            raise PublicAuthority8EError(f"unsupported event type for {provider}: {event_type}")
        source_url = str(raw.get("source_url") or "").strip()
        host = (urlparse(source_url).hostname or "").lower()
        if host not in set(provider_cfg.get("allowed_hosts") or []):
            raise PublicAuthority8EError(f"non-authoritative source host for {provider}: {host}")

        source_event_id = str(raw.get("source_event_id") or "").strip()
        subject_id = str(raw.get("subject_id") or "").strip()
        event_state = str(raw.get("event_state") or "").strip().upper()
        authority_scope = str(raw.get("authority_scope") or "").strip()
        if not all((source_event_id, subject_id, event_state, authority_scope)):
            raise PublicAuthority8EError("source_event_id, subject_id, event_state and authority_scope are required")

        ingested_at = _aware(raw.get("ingested_at"), field="ingested_at")
        proof = str(raw.get("public_release_proof_status") or "INGESTION_ONLY").strip().upper()
        if proof not in {"PROVEN", "INGESTION_ONLY"}:
            raise PublicAuthority8EError(f"unsupported public release proof status: {proof}")
        published_at = None
        historical_proven = raw.get("historical_publication_time_independently_proven") is True
        if proof == "PROVEN":
            published_at = _aware(raw.get("published_at"), field="published_at")
            if not historical_proven:
                raise PublicAuthority8EError("PROVEN historical publication time requires independent proof flag")
            valid_from = published_at
        else:
            if historical_proven:
                raise PublicAuthority8EError("independent publication proof cannot be true without PROVEN timestamp")
            valid_from = ingested_at

        source_record = raw.get("source_record")
        if not isinstance(source_record, Mapping) or not source_record:
            raise PublicAuthority8EError("source_record mapping is required for provenance")
        rows.append({
            "canonical_event_key": f"{provider}:{event_type}:{source_event_id}",
            "event_type": event_type,
            "subject_id": subject_id,
            "source_id": provider,
            "source_class": "PUBLIC_AUTHORITY_PRIMARY",
            "source_event_id": source_event_id,
            "event_state": event_state,
            "authority_scope": authority_scope,
            "source_url": source_url,
            "published_at": published_at.isoformat() if published_at else None,
            "valid_from": valid_from.isoformat(),
            "ingested_at": ingested_at.isoformat(),
            "content_sha256": _canonical_hash(source_record),
            "strict_pit_eligible": True,
            "discovery_only": False,
            "public_release_proof_status": proof,
            "historical_publication_time_independently_proven": historical_proven,
            "source_record": dict(source_record),
            "security_identity_status": "DEFERRED_NOT_INFERRED_FROM_AUTHORITY_NAME"
        })

    rows.sort(key=lambda row: (row["source_id"], row["canonical_event_key"], row["valid_from"]))
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8E_B3_public_authority_events",
        "status": "PROSPECTIVE_PIT_PUBLIC_AUTHORITY_EVIDENCE",
        "row_count": len(rows),
        "rows": rows,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "generic_sentiment_enabled": False,
            "ticker_mapping_enabled": False,
            "historical_backdating_without_proof": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False
        }
    }
