from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse


SCHEMA_VERSION = "external_evidence_8e_primary_release_v1"


class PrimaryRelease8EError(ValueError):
    """Raised when a structured primary release violates the frozen 8E contract."""


def _aware_datetime(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise PrimaryRelease8EError(f"missing {field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise PrimaryRelease8EError(f"invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PrimaryRelease8EError(f"{field} must be timezone-aware")
    return parsed


def _optional_aware_datetime(value: Any, *, field: str) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    return _aware_datetime(value, field=field)


def _required_text(row: Mapping[str, Any], field: str) -> str:
    value = str(row.get(field) or "").strip()
    if not value:
        raise PrimaryRelease8EError(f"missing {field}")
    return value


def _sha256(value: Any, *, field: str) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text):
        raise PrimaryRelease8EError(f"{field} must be a 64-character SHA-256 hex digest")
    return text


def _validate_source_url(
    *, provider_id: str, row: Mapping[str, Any], provider: Mapping[str, Any]
) -> str:
    source_url = _required_text(row, "source_url")
    parsed = urlparse(source_url)
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise PrimaryRelease8EError("source_url must be an absolute HTTPS URL")
    host = parsed.hostname.lower()
    allowed_hosts = {str(value).strip().lower() for value in provider.get("allowed_hosts") or []}
    if allowed_hosts and host not in allowed_hosts:
        raise PrimaryRelease8EError(
            f"source_url host {host!r} is not allowed for provider {provider_id}"
        )
    if provider.get("requires_issuer_domain_attestation") is True and row.get("issuer_domain_attested") is not True:
        raise PrimaryRelease8EError("ISSUER_IR records require issuer_domain_attested=true")
    return source_url


def build_primary_release_evidence(
    *,
    records: Iterable[Mapping[str, Any]],
    ingested_at: str | datetime,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize prospectively observed authoritative/issuer releases outcome-blind.

    Free text is never auto-classified. Event type/state must be supplied by source-native
    structure or a human-reviewed challenger workflow. A source/page date is preserved as a
    claim, but `published_at` is populated only when historical publication timing has independent
    archival proof. Otherwise strict PIT begins at actual ingestion and First Public Release stays
    unresolved rather than being fabricated from the ingestion timestamp.
    """
    if config.get("schema_version") != SCHEMA_VERSION:
        raise PrimaryRelease8EError("unsupported primary release config")
    hard = config.get("hard_boundaries") or {}
    for field in (
        "market_outcomes_may_be_read",
        "market_direction_may_be_assigned",
        "threshold_selection_enabled",
        "phase7_integration_enabled",
        "production_external_evidence_enabled",
    ):
        if hard.get(field) is not False:
            raise PrimaryRelease8EError(f"hard boundary {field} must be false")

    observed_at = _aware_datetime(ingested_at, field="ingested_at")
    providers = config.get("providers") or {}
    if not isinstance(providers, Mapping) or not providers:
        raise PrimaryRelease8EError("primary release config has no providers")

    rows: list[dict[str, Any]] = []
    seen_identity: set[tuple[str, str]] = set()
    for raw in records:
        provider_id = _required_text(raw, "provider_id").upper()
        provider = providers.get(provider_id)
        if not isinstance(provider, Mapping):
            raise PrimaryRelease8EError(f"unsupported provider_id: {provider_id}")

        event_type = _required_text(raw, "event_type").upper()
        allowed_event_types = {str(value).upper() for value in provider.get("allowed_event_types") or []}
        if event_type not in allowed_event_types:
            raise PrimaryRelease8EError(f"event_type {event_type} is not allowed for provider {provider_id}")

        source_event_id = _required_text(raw, "source_event_id")
        identity = (provider_id, source_event_id)
        if identity in seen_identity:
            raise PrimaryRelease8EError(f"duplicate provider/source_event_id: {identity}")
        seen_identity.add(identity)

        source_url = _validate_source_url(provider_id=provider_id, row=raw, provider=provider)
        content_sha256 = _sha256(raw.get("content_sha256"), field="content_sha256")
        source_claimed_published_at = _optional_aware_datetime(
            raw.get("source_claimed_published_at"), field="source_claimed_published_at"
        )
        historical_proven = raw.get("historical_publication_time_independently_proven") is True
        archival_proof_sha256: str | None = None

        if historical_proven:
            if source_claimed_published_at is None:
                raise PrimaryRelease8EError("historical publication proof requires source_claimed_published_at")
            archival_proof_sha256 = _sha256(raw.get("archival_proof_sha256"), field="archival_proof_sha256")
            if source_claimed_published_at > observed_at:
                raise PrimaryRelease8EError("proven publication time cannot be after actual ingestion")
            published_at = source_claimed_published_at
            valid_from = source_claimed_published_at
            proof_status = "PROVEN"
        else:
            published_at = None
            valid_from = observed_at
            proof_status = "INGESTION_ONLY"

        semantic_origin = _required_text(raw, "semantic_label_origin").upper()
        if semantic_origin not in {"SOURCE_NATIVE_STRUCTURED", "HUMAN_REVIEWED_STRUCTURED"}:
            raise PrimaryRelease8EError(
                "semantic_label_origin must be SOURCE_NATIVE_STRUCTURED or HUMAN_REVIEWED_STRUCTURED"
            )

        rows.append({
            "canonical_event_key": _required_text(raw, "canonical_event_key"),
            "event_type": event_type,
            "subject_id": _required_text(raw, "subject_id"),
            "source_id": provider_id.lower(),
            "source_class": _required_text(provider, "source_class"),
            "source_event_id": source_event_id,
            "event_state": _required_text(raw, "event_state").upper(),
            "authority_scope": _required_text(raw, "authority_scope"),
            "source_url": source_url,
            "published_at": published_at.isoformat() if published_at else None,
            "source_claimed_published_at": source_claimed_published_at.isoformat() if source_claimed_published_at else None,
            "valid_from": valid_from.isoformat(),
            "ingested_at": observed_at.isoformat(),
            "content_sha256": content_sha256,
            "archival_proof_sha256": archival_proof_sha256,
            "strict_pit_eligible": True,
            "discovery_only": False,
            "public_release_proof_status": proof_status,
            "historical_publication_time_independently_proven": historical_proven,
            "semantic_label_origin": semantic_origin,
            "semantic_status": (
                "SOURCE_NATIVE_STRUCTURED" if semantic_origin == "SOURCE_NATIVE_STRUCTURED"
                else "HUMAN_REVIEWED_STRUCTURED_CHALLENGER"
            ),
            "market_direction": "UNASSIGNED",
        })

    rows.sort(key=lambda row: (str(row["valid_from"]), str(row["source_id"]), str(row["source_event_id"])))
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "8E_B3_primary_release_adapter",
        "status": "PROSPECTIVE_STRUCTURED_RELEASE_EVIDENCE",
        "ingested_at": observed_at.isoformat(),
        "row_count": len(rows),
        "rows": rows,
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "generic_sentiment_enabled": False,
            "automatic_free_text_event_classification_enabled": False,
            "historical_backdating_without_independent_proof": False,
            "ingestion_time_fabricated_as_published_at": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def write_primary_release_evidence(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
