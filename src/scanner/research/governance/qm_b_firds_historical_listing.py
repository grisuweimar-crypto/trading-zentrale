"""QM-B historical ESMA FIRDS listing evidence.

This module converts an official ESMA FIRDS publication descriptor plus a parsed
FIRDS file into source-scoped PIT listing evidence. It deliberately uses a
conservative availability boundary later than the documented publication time
and never infers tradability, execution-channel availability or investability.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_b_identity_reconciliation import is_valid_isin


SCHEMA_VERSION = "qm_b_firds_historical_listing_v1"
RESULT_SCHEMA_VERSION = "qm_b_firds_historical_listing_result_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_firds_historical_listing_v1.json"


class FirdsHistoricalListingError(ValueError):
    """Raised when historical FIRDS evidence cannot be bound safely."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _canonical_hash(value: Mapping[str, Any]) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return sha256(raw.encode("utf-8")).hexdigest()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise FirdsHistoricalListingError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise FirdsHistoricalListingError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise FirdsHistoricalListingError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def load_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FirdsHistoricalListingError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise FirdsHistoricalListingError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise FirdsHistoricalListingError("contract_scope_invalid")
    if payload.get("source_id") != "esma_firds":
        raise FirdsHistoricalListingError("source_id_invalid")
    if payload.get("global_scope") is not False:
        raise FirdsHistoricalListingError("global_scope_must_remain_false")
    publication = payload.get("publication_rules")
    guards = payload.get("hard_guards")
    promotion = payload.get("promotion")
    if not isinstance(publication, Mapping) or publication.get("conservative_available_from") != "NEXT_CALENDAR_DAY_00_00_UTC":
        raise FirdsHistoricalListingError("publication_boundary_rule_invalid")
    if not isinstance(guards, Mapping) or guards.get("regional_evidence_may_close_global_listing_blocker") is not False:
        raise FirdsHistoricalListingError("regional_global_boundary_invalid")
    if not isinstance(promotion, Mapping) or promotion.get("global_listing_promotion_performed") is not False:
        raise FirdsHistoricalListingError("global_promotion_guard_invalid")
    return payload


def conservative_available_from(publication_date: str) -> str:
    try:
        day = date.fromisoformat(_clean(publication_date))
    except ValueError as exc:
        raise FirdsHistoricalListingError(f"publication_date_invalid:{publication_date}") from exc
    boundary = datetime.combine(day + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return boundary.isoformat()


def validate_publication_descriptor(
    descriptor: Mapping[str, Any],
    *,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_contract(contract_path)
    if descriptor.get("schema_version") != "qm_b_firds_publication_descriptor_v1":
        raise FirdsHistoricalListingError("publication_descriptor_schema_invalid")
    if descriptor.get("source_id") != contract["source_id"]:
        raise FirdsHistoricalListingError("publication_descriptor_source_invalid")
    file_type = _clean(descriptor.get("file_type")).upper()
    if file_type not in set(contract["supported_file_types"]):
        raise FirdsHistoricalListingError(f"publication_file_type_invalid:{file_type}")
    publication_date = _clean(descriptor.get("publication_date"))
    available_from = conservative_available_from(publication_date)
    index_url = _clean(descriptor.get("official_index_url"))
    if not index_url.startswith(contract["official_file_index"]["base_url"]):
        raise FirdsHistoricalListingError("publication_index_not_official_esma")
    download_link = _clean(descriptor.get("download_link"))
    if not download_link.startswith("https://"):
        raise FirdsHistoricalListingError("publication_download_link_invalid")
    checksum = _clean(descriptor.get("checksum"))
    if not checksum:
        raise FirdsHistoricalListingError("publication_checksum_required")
    raw_sha256 = _clean(descriptor.get("raw_payload_sha256")).lower()
    if len(raw_sha256) != 64 or any(ch not in "0123456789abcdef" for ch in raw_sha256):
        raise FirdsHistoricalListingError("raw_payload_sha256_invalid")
    retrieved_at = _utc(descriptor.get("retrieved_at"), field="retrieved_at")
    if retrieved_at < _utc(available_from, field="available_from"):
        raise FirdsHistoricalListingError("retrieval_precedes_conservative_publication_boundary")
    normalized = {
        "schema_version": "qm_b_firds_publication_descriptor_v1",
        "source_id": "esma_firds",
        "publication_date": publication_date,
        "available_from": available_from,
        "file_type": file_type,
        "file_name": _clean(descriptor.get("file_name")),
        "official_index_url": index_url,
        "download_link": download_link,
        "checksum": checksum,
        "raw_payload_sha256": raw_sha256,
        "retrieved_at": retrieved_at.isoformat(),
    }
    if not normalized["file_name"]:
        raise FirdsHistoricalListingError("publication_file_name_required")
    normalized["publication_descriptor_hash"] = _canonical_hash(normalized)
    return normalized


def _listing_status(record: Mapping[str, Any], file_type: str) -> str:
    event = _clean(record.get("source_event_type"))
    if file_type == "FULINS" and event in {"FULL_RECORD", "REFERENCE_RECORD"}:
        return "LISTED"
    if file_type == "DLTINS":
        if event in {"NewRcrd", "ModfdRcrd"}:
            return "LISTED"
        if event == "TermntdRcrd":
            return "DELISTED"
        if event == "CancRcrd":
            return "UNKNOWN"
    return "UNKNOWN"


def build_historical_listing_evidence(
    *,
    publication_descriptor: Mapping[str, Any],
    parser_result: Mapping[str, Any],
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_contract(contract_path)
    descriptor = validate_publication_descriptor(
        publication_descriptor,
        contract_path=contract_path,
    )
    if parser_result.get("parser_id") != contract["provenance_rules"]["parser_id_required"]:
        raise FirdsHistoricalListingError("parser_id_invalid")
    if _clean(parser_result.get("parser_version")) != contract["provenance_rules"]["parser_version_required"]:
        raise FirdsHistoricalListingError("parser_version_invalid")
    parser_file_type = _clean(parser_result.get("file_type")).upper()
    if parser_file_type != descriptor["file_type"]:
        raise FirdsHistoricalListingError("parser_publication_file_type_mismatch")
    records = parser_result.get("records")
    if not isinstance(records, list) or not records:
        raise FirdsHistoricalListingError("parser_records_required")

    evidence: list[dict[str, Any]] = []
    for index, raw in enumerate(records, start=1):
        if not isinstance(raw, Mapping):
            raise FirdsHistoricalListingError(f"parser_record_not_object:{index}")
        isin = _clean(raw.get("source_isin") or raw.get("instrument_id")).upper()
        mic = _clean(raw.get("venue_code")).upper()
        if not is_valid_isin(isin):
            raise FirdsHistoricalListingError(f"parser_record_isin_invalid:{index}:{isin}")
        if len(mic) != 4:
            raise FirdsHistoricalListingError(f"parser_record_mic_invalid:{index}:{mic}")
        status = _listing_status(raw, descriptor["file_type"])
        evidence.append({
            "schema_version": "qm_b_historical_listing_evidence_v1",
            "instrument_id": f"urn:scanner:isin:{isin}",
            "dimension": "listing_state",
            "status": status,
            "venue_id": mic,
            "valid_from": descriptor["available_from"],
            "pit_verified": True,
            "source_id": "esma_firds",
            "source_scope": contract["source_scope"],
            "publication_descriptor_hash": descriptor["publication_descriptor_hash"],
            "source_file_type": descriptor["file_type"],
            "source_event_type": raw.get("source_event_type"),
            "source_effective_listing_start": raw.get("listing_start"),
            "source_effective_listing_end": raw.get("listing_end"),
            "reason_codes": [
                "OFFICIAL_ESMA_FIRDS_PUBLICATION",
                "CONSERVATIVE_POST_PUBLICATION_PIT_BOUNDARY",
                "REGIONAL_EEA_MIFIR_SCOPE_ONLY",
            ],
        })

    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "source_id": "esma_firds",
        "source_scope": contract["source_scope"],
        "global_scope": False,
        "publication": descriptor,
        "record_count": len(evidence),
        "listing_evidence": evidence,
        "market_tradability_inferred": False,
        "execution_channel_inferred": False,
        "project_investability_inferred": False,
        "global_listing_promotion_performed": False,
        "ba_qm2_global_blocker_released": False,
    }
