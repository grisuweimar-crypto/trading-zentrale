"""QM-B prospective listing-snapshot to stable-instrument candidate matcher.

Research-only.  This matcher joins already archived prospective listing-source
records to a separately time-stamped universe snapshot.  It is intentionally
strict: no symbol normalization, no name matching and no back-projection.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_b_identity_reconciliation import (
    is_valid_isin,
    stable_isin_instrument_id,
)


SCHEMA_VERSION = "qm_b_prospective_identity_match_result_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_prospective_identity_match_v1.json"


class ProspectiveIdentityMatchError(ValueError):
    """Raised when prospective identity matching cannot be performed safely."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _clean(value).upper()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise ProspectiveIdentityMatchError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProspectiveIdentityMatchError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise ProspectiveIdentityMatchError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load_match_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProspectiveIdentityMatchError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_prospective_identity_match_v1":
        raise ProspectiveIdentityMatchError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ProspectiveIdentityMatchError("contract_scope_invalid")
    auto = payload.get("automatic_match")
    if not isinstance(auto, Mapping):
        raise ProspectiveIdentityMatchError("automatic_match_contract_missing")
    if auto.get("symbol_normalization_allowed") is not False or auto.get("name_matching_allowed") is not False:
        raise ProspectiveIdentityMatchError("unsafe_match_rule_enabled")
    return payload


def load_universe_snapshot(path: str | Path) -> tuple[list[dict[str, str]], str]:
    target = Path(path)
    try:
        raw = target.read_bytes()
    except OSError as exc:
        raise ProspectiveIdentityMatchError(f"universe_snapshot_unreadable:{target}") from exc
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ProspectiveIdentityMatchError("universe_snapshot_not_utf8") from exc
    try:
        rows = list(csv.DictReader(text.splitlines()))
    except csv.Error as exc:
        raise ProspectiveIdentityMatchError("universe_snapshot_csv_invalid") from exc
    if not rows and not text.strip():
        raise ProspectiveIdentityMatchError("universe_snapshot_empty")
    normalized = [
        {str(k): "" if v is None else str(v) for k, v in row.items()}
        for row in rows
    ]
    return normalized, _sha256_bytes(raw)


def _active_master_rows(rows: list[Mapping[str, Any]], allowed_asset_types: set[str]) -> dict[str, list[dict[str, Any]]]:
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row_number, raw in enumerate(rows, start=2):
        active = _clean(raw.get("active")).lower()
        if active not in {"1", "true", "yes", "y"}:
            continue
        asset_type = _clean(raw.get("asset_type")).lower()
        if asset_type not in allowed_asset_types:
            continue
        symbol = _upper(raw.get("symbol"))
        if not symbol:
            continue
        row = {
            "source_row_number": row_number,
            "symbol": symbol,
            "isin": _upper(raw.get("isin")),
            "asset_type": asset_type,
            "name": _clean(raw.get("name")),
            "country": _clean(raw.get("country")),
            "currency": _clean(raw.get("currency")),
        }
        by_symbol.setdefault(symbol, []).append(row)
    return by_symbol


def _record_aliases(record: Mapping[str, Any]) -> list[str]:
    # Exact source-provided aliases only.  No punctuation/suffix rewriting.
    values = []
    for field in ("source_symbol", "cqs_symbol", "nasdaq_symbol"):
        value = _upper(record.get(field))
        if value and value not in values:
            values.append(value)
    return values


def match_listing_snapshot(
    snapshot: Mapping[str, Any],
    *,
    universe_rows: list[Mapping[str, Any]],
    universe_snapshot_id: str,
    universe_observed_at: str,
    universe_sha256: str,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_match_contract(contract_path)
    missing_snapshot = [field for field in contract["required_snapshot_fields"] if field not in snapshot]
    if missing_snapshot:
        raise ProspectiveIdentityMatchError("snapshot_fields_missing:" + ",".join(missing_snapshot))
    if snapshot.get("historical_retrojection_permitted") is not False:
        raise ProspectiveIdentityMatchError("snapshot_retrojection_guard_missing")
    records = snapshot.get("records")
    if not isinstance(records, list):
        raise ProspectiveIdentityMatchError("snapshot_records_must_be_list")

    snapshot_valid = _utc(snapshot.get("valid_from"), field="snapshot.valid_from")
    universe_time = _utc(universe_observed_at, field="universe_observed_at")
    match_valid_from = max(snapshot_valid, universe_time).isoformat()
    universe_snapshot_id = _clean(universe_snapshot_id)
    universe_sha256 = _clean(universe_sha256).lower()
    if not universe_snapshot_id:
        raise ProspectiveIdentityMatchError("universe_snapshot_id_required")
    if len(universe_sha256) != 64 or any(ch not in "0123456789abcdef" for ch in universe_sha256):
        raise ProspectiveIdentityMatchError("universe_sha256_invalid")

    allowed = {str(v).lower() for v in contract["automatic_match"]["allowed_asset_types"]}
    by_symbol = _active_master_rows(universe_rows, allowed)
    output_rows: list[dict[str, Any]] = []

    for source_index, source in enumerate(records, start=1):
        if not isinstance(source, Mapping):
            raise ProspectiveIdentityMatchError(f"snapshot_record_not_object:{source_index}")
        aliases = _record_aliases(source)
        candidate_rows: list[dict[str, Any]] = []
        seen_master_rows: set[int] = set()
        for alias in aliases:
            for row in by_symbol.get(alias, []):
                row_no = int(row["source_row_number"])
                if row_no not in seen_master_rows:
                    seen_master_rows.add(row_no)
                    candidate_rows.append(dict(row))

        valid_isin_rows = [row for row in candidate_rows if is_valid_isin(row.get("isin"))]
        unique_isins = sorted({str(row["isin"]) for row in valid_isin_rows})

        if not aliases:
            status = "UNSUPPORTED"
            reason = "NO_SUPPORTED_EXACT_SOURCE_IDENTIFIER"
            stable_id = None
            canonical_isin = None
        elif not candidate_rows:
            status = "UNMATCHED"
            reason = "NO_EXACT_ACTIVE_MASTER_SYMBOL_MATCH"
            stable_id = None
            canonical_isin = None
        elif len(unique_isins) > 1:
            status = "AMBIGUOUS"
            reason = "EXACT_SOURCE_IDENTIFIER_MAPS_TO_MULTIPLE_ISINS"
            stable_id = None
            canonical_isin = None
        elif len(candidate_rows) > 1:
            status = "REVIEW_REQUIRED_DUPLICATE_MASTER_ROWS"
            reason = "MULTIPLE_ACTIVE_MASTER_ROWS_FOR_EXACT_IDENTIFIER"
            stable_id = None
            canonical_isin = unique_isins[0] if len(unique_isins) == 1 else None
        elif len(valid_isin_rows) != 1:
            status = "UNMATCHED"
            reason = "EXACT_MASTER_MATCH_HAS_NO_VALID_ISIN"
            stable_id = None
            canonical_isin = None
        else:
            canonical_isin = unique_isins[0]
            stable_id = stable_isin_instrument_id(canonical_isin)
            status = "MATCHED"
            reason = "UNIQUE_EXACT_ACTIVE_MASTER_SYMBOL_WITH_VALID_ISIN"

        output_rows.append(
            {
                "source_record_index": source_index,
                "source_symbol": _clean(source.get("source_symbol")) or None,
                "exact_source_aliases": aliases,
                "match_status": status,
                "reason_code": reason,
                "candidate_instrument_id": stable_id,
                "canonical_isin": canonical_isin,
                "candidate_master_rows": candidate_rows,
                "identity_valid_from": match_valid_from if status == "MATCHED" else None,
                "historical_membership_verified": False,
                "tradability_verified": False,
                "project_investability_verified": False,
            }
        )

    counts: dict[str, int] = {}
    for row in output_rows:
        counts[row["match_status"]] = counts.get(row["match_status"], 0) + 1

    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "source_snapshot_id": _clean(snapshot.get("snapshot_id")),
        "source_id": _clean(snapshot.get("source_id")),
        "source_snapshot_valid_from": snapshot_valid.isoformat(),
        "universe_snapshot_id": universe_snapshot_id,
        "universe_observed_at": universe_time.isoformat(),
        "universe_sha256": universe_sha256,
        "identity_match_valid_from": match_valid_from,
        "match_counts": dict(sorted(counts.items())),
        "record_count": len(output_rows),
        "historical_retrojection_permitted": False,
        "historical_membership_promotion_performed": False,
        "tradability_promotion_performed": False,
        "project_investability_promotion_performed": False,
        "matches": output_rows,
    }
