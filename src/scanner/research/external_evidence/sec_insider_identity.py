from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

IDENTITY_SCHEMA = "external_evidence_8d_insider_asof_identity_v1"
EVIDENCE_SCHEMA = "external_evidence_8d_sec_insider_bulk_v1"


class SecInsiderIdentityError(ValueError):
    pass


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SecInsiderIdentityError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise SecInsiderIdentityError(f"JSON root must be an object: {path}")
    return payload


def _aware_datetime(value: Any) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise SecInsiderIdentityError(f"invalid datetime: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SecInsiderIdentityError(
            f"as-of/valid-from datetime must be timezone-aware: {value!r}"
        )
    return parsed


def _norm_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def build_identity_evidence(
    evidence_payloads: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Build dated family-specific symbol/CIK evidence from PIT-safe insider rows.

    This deliberately does not use current SEC ticker arrays or current scanner mappings.
    A symbol/CIK relationship first becomes usable at the row's historical ``valid_from``.
    """
    seen: set[tuple[str, str, str, str]] = set()
    points: list[dict[str, Any]] = []
    for payload in evidence_payloads:
        if payload.get("schema_version") != EVIDENCE_SCHEMA:
            raise SecInsiderIdentityError("unsupported SEC insider evidence schema")
        guards = payload.get("guards") or {}
        if guards.get("market_outcomes_read") is not False:
            raise SecInsiderIdentityError("identity evidence must remain outcome-blind")
        quarter = str(payload.get("source_quarter") or "").strip().upper()
        for row in payload.get("rows") or []:
            if not isinstance(row, Mapping):
                continue
            if row.get("strict_pit_eligible") is not True:
                continue
            cik = str(row.get("issuer_cik") or "").strip()
            symbol = _norm_symbol(row.get("issuer_trading_symbol_reported"))
            accession = str(row.get("accession_number") or "").strip()
            valid_from_text = str(row.get("valid_from") or "").strip()
            if not cik or not symbol or not accession or not valid_from_text:
                continue
            valid_from = _aware_datetime(valid_from_text)
            key = (cik, symbol, accession, valid_from.isoformat())
            if key in seen:
                continue
            seen.add(key)
            points.append(
                {
                    "issuer_cik": cik,
                    "symbol": symbol,
                    "accession_number": accession,
                    "valid_from": valid_from.isoformat(),
                    "pit_status": row.get("pit_status"),
                    "source_quarter": quarter,
                }
            )
    points.sort(
        key=lambda point: (
            point["issuer_cik"],
            point["valid_from"],
            point["symbol"],
            point["accession_number"],
        )
    )
    return points


def _latest_known_identity_by_cik(
    evidence_points: Iterable[Mapping[str, Any]], as_of: datetime
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for point in evidence_points:
        valid_from = _aware_datetime(point.get("valid_from"))
        if valid_from <= as_of:
            cik = str(point.get("issuer_cik") or "").strip()
            if cik:
                grouped[cik].append(point)

    latest: dict[str, dict[str, Any]] = {}
    for cik, rows in grouped.items():
        newest = max(_aware_datetime(row.get("valid_from")) for row in rows)
        at_newest = [row for row in rows if _aware_datetime(row.get("valid_from")) == newest]
        symbols = sorted({_norm_symbol(row.get("symbol")) for row in at_newest if _norm_symbol(row.get("symbol"))})
        accessions = sorted(
            {str(row.get("accession_number") or "").strip() for row in at_newest if str(row.get("accession_number") or "").strip()}
        )
        pit_statuses = sorted(
            {str(row.get("pit_status") or "UNKNOWN") for row in at_newest}
        )
        latest[cik] = {
            "issuer_cik": cik,
            "symbols": symbols,
            "ambiguous_within_cik": len(symbols) != 1,
            "valid_from": newest.isoformat(),
            "accessions": accessions,
            "pit_statuses": pit_statuses,
        }
    return latest


def resolve_insider_asof_identity(
    *,
    scanner_observations: Iterable[Mapping[str, Any]],
    evidence_payloads: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    points = build_identity_evidence(evidence_payloads)
    output_rows: list[dict[str, Any]] = []

    for observation in scanner_observations:
        symbol = _norm_symbol(observation.get("symbol"))
        if not symbol:
            raise SecInsiderIdentityError("scanner observation missing symbol")
        as_of = _aware_datetime(observation.get("as_of"))
        latest_by_cik = _latest_known_identity_by_cik(points, as_of)

        candidates: list[dict[str, Any]] = []
        internal_ambiguity = False
        for state in latest_by_cik.values():
            if state["ambiguous_within_cik"]:
                if symbol in state["symbols"]:
                    internal_ambiguity = True
                continue
            if state["symbols"] == [symbol]:
                candidates.append(state)

        reason_codes: list[str] = []
        issuer_cik: str | None = None
        instrument_id: str | None = None
        evidence_valid_from: str | None = None
        evidence_accessions: list[str] = []
        pit_status: str = "UNKNOWN"

        if internal_ambiguity or len(candidates) > 1:
            identity_status = "AMBIGUOUS_PIT_IDENTITY"
            source_coverage_status = "CONFLICTING_SOURCES"
            reason_codes.append("MULTIPLE_OR_CONFLICTING_PIT_IDENTITY_CANDIDATES")
        elif len(candidates) == 1:
            candidate = candidates[0]
            identity_status = "VERIFIED_FAMILY_PIT_IDENTITY"
            source_coverage_status = "KNOWN"
            issuer_cik = candidate["issuer_cik"]
            instrument_id = f"SEC_CIK:{issuer_cik}"
            evidence_valid_from = candidate["valid_from"]
            evidence_accessions = list(candidate["accessions"])
            pit_statuses = candidate["pit_statuses"]
            pit_status = pit_statuses[0] if len(pit_statuses) == 1 else "CONFLICTING_SOURCES"
        else:
            identity_status = "UNKNOWN_NO_PIT_IDENTITY"
            source_coverage_status = "UNKNOWN"
            reason_codes.append("NO_PRIOR_PIT_SYMBOL_CIK_EVIDENCE")

        output_rows.append(
            {
                "as_of": as_of.isoformat(),
                "as_of_date": as_of.date().isoformat(),
                "symbol": symbol,
                "issuer_cik": issuer_cik,
                "instrument_id": instrument_id,
                "listing_venue": observation.get("listing_venue"),
                "membership_status": "IN_SCOPE",
                "membership_reason": "OBSERVED_IN_INTERNAL_SCANNER_HISTORY",
                "listing_date": None,
                "delisting_date": None,
                "scanner_observable": True,
                "external_family": "INSIDER_ACTIVITY",
                "source_id": "sec_insider_bulk_pit_identity",
                "source_coverage_status": source_coverage_status,
                "pit_status": pit_status,
                "identity_status": identity_status,
                "identity_evidence_valid_from": evidence_valid_from,
                "identity_evidence_accessions": evidence_accessions,
                "reason_codes": reason_codes,
            }
        )

    output_rows.sort(key=lambda row: (row["as_of"], row["symbol"]))
    counts: dict[str, int] = defaultdict(int)
    for row in output_rows:
        counts[row["identity_status"]] += 1

    return {
        "schema_version": IDENTITY_SCHEMA,
        "phase": "8D_B5_insider_asof_identity",
        "status": "OUTCOME_BLIND_FAMILY_SPECIFIC_IDENTITY",
        "identity_evidence_point_count": len(points),
        "row_count": len(output_rows),
        "identity_status_counts": dict(sorted(counts.items())),
        "rows": output_rows,
        "guards": {
            "scanner_history_primary_observability_ledger": True,
            "exact_symbol_only": True,
            "current_ticker_retrojection_enabled": False,
            "future_identity_evidence_used": False,
            "missing_identity_imputed_neutral": False,
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def load_identity_evidence(paths: Iterable[Path]) -> list[dict[str, Any]]:
    return [_load_json(path) for path in paths]


def write_identity_result(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
