"""QM-B observed historical membership evidence.

This module converts preserved research-history rows into *evidence candidates* only.
It deliberately does not create strict QM-B universe membership or stable instrument
identity. A historical scanner observation can prove that a symbol was observed by
the scanner on a date, but it cannot prove universe completeness, absence, listing
identity, investability or stable identity across symbol changes.
"""
from __future__ import annotations

from collections import Counter
import csv
from datetime import date
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "qm_b_observed_membership_v1"
CANDIDATE_SCHEMA_VERSION = "qm_b_observed_membership_candidates_v1"
SUMMARY_SCHEMA_VERSION = "qm_b_observed_membership_summary_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_observed_membership_v1.json"


class ObservedMembershipError(ValueError):
    """Raised when historical evidence cannot be parsed safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _sha256_bytes(raw: bytes) -> str:
    return sha256(raw).hexdigest()


def _sha256_json(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _as_of(row: Mapping[str, Any]) -> str:
    return _clean(row.get("as_of")) or _clean(row.get("date"))


def load_observed_membership_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ObservedMembershipError(f"contract_unreadable:{contract_path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise ObservedMembershipError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ObservedMembershipError("contract_scope_invalid")
    return payload


def classify_history_row(
    row: Mapping[str, Any], *, contract: Mapping[str, Any] | None = None
) -> tuple[str, str, list[str]]:
    """Classify one preserved history row without inferring missing provenance.

    The scanner predicate mirrors `scanner.reports.research_views.observed`:
    observation_type must be blank/observed_scanner and data_source blank/scanner_run.
    Known backfill indicators are deliberately checked separately. Every other
    combination is UNKNOWN_PROVENANCE and therefore not membership evidence.
    """
    contract = contract or load_observed_membership_contract()
    source = contract["source_contract"]
    observation_type = _clean(row.get("observation_type"))
    data_source = _clean(row.get("data_source"))
    observed_spec = source["scanner_observation"]

    if (
        observation_type in set(observed_spec["observation_type_allowed"])
        and data_source in set(observed_spec["data_source_allowed"])
    ):
        reasons = ["PRESERVED_SCANNER_OBSERVATION"]
        if not observation_type:
            reasons.append("LEGACY_BLANK_OBSERVATION_TYPE_ACCEPTED_BY_RESEARCH_VIEWS")
        if not data_source:
            reasons.append("LEGACY_BLANK_DATA_SOURCE_ACCEPTED_BY_RESEARCH_VIEWS")
        return "SCANNER_OBSERVED", "OBSERVED_IN_SCANNER", reasons

    backfill = source["backfill_indicators"]
    if (
        observation_type in set(backfill["observation_type"])
        or data_source in set(backfill["data_source"])
    ):
        return "BACKFILL_DERIVED", "NOT_MEMBERSHIP_EVIDENCE", ["PRICE_OR_MARKET_BACKFILL"]

    return "UNKNOWN_PROVENANCE", "UNKNOWN", ["PROVENANCE_NOT_RECOGNIZED"]


def candidate_from_row(
    row: Mapping[str, Any],
    *,
    source_path: str,
    source_row_number: int,
    source_file_sha256: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    contract = contract or load_observed_membership_contract()
    when = _as_of(row)
    symbol = _clean(row.get("symbol"))
    if not when:
        raise ObservedMembershipError(f"missing_as_of:row:{source_row_number}")
    try:
        date.fromisoformat(when)
    except ValueError as exc:
        raise ObservedMembershipError(f"invalid_as_of:{when}:row:{source_row_number}") from exc
    if not symbol:
        raise ObservedMembershipError(f"missing_symbol:row:{source_row_number}")

    evidence_class, membership_claim, reasons = classify_history_row(row, contract=contract)
    row_snapshot = {str(k): "" if v is None else str(v) for k, v in row.items()}
    candidate = {
        "as_of_date": when,
        "observed_symbol": symbol,
        "name": _clean(row.get("name")),
        "currency": _clean(row.get("currency")),
        "sector": _clean(row.get("sector")),
        "run_id": _clean(row.get("run_id")),
        "universe_version": _clean(row.get("universe_version")),
        "config_version": _clean(row.get("config_version")),
        "source_observation_type": _clean(row.get("observation_type")),
        "source_data_source": _clean(row.get("data_source")),
        "source_path": source_path,
        "source_row_number": source_row_number,
        "source_file_sha256": f"sha256:{source_file_sha256}",
        "source_row_sha256": f"sha256:{_sha256_json(row_snapshot)}",
        "evidence_class": evidence_class,
        "membership_claim": membership_claim,
        "identity_status": "UNRESOLVED",
        "reason_codes": reasons,
    }
    required = contract["candidate_required_fields"]
    missing = [field for field in required if field not in candidate]
    if missing:
        raise ObservedMembershipError("candidate_required_fields_missing:" + ",".join(sorted(missing)))
    return candidate


def _read_history(path: Path) -> tuple[bytes, list[dict[str, str]]]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ObservedMembershipError(f"history_unreadable:{path}") from exc
    if not raw:
        raise ObservedMembershipError(f"history_empty:{path}")
    try:
        text = raw.decode("utf-8-sig")
        reader = csv.DictReader(text.splitlines())
        if not reader.fieldnames or "symbol" not in reader.fieldnames or not ({"date", "as_of"} & set(reader.fieldnames)):
            raise ObservedMembershipError("history_required_columns_missing")
        rows = list(reader)
    except UnicodeDecodeError as exc:
        raise ObservedMembershipError("history_not_utf8") from exc
    return raw, rows


def _active_current_symbols(path: Path) -> set[str]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError as exc:
        raise ObservedMembershipError(f"current_universe_unreadable:{path}") from exc
    result: set[str] = set()
    for row in rows:
        active = _clean(row.get("active")).lower()
        if active not in {"1", "true", "yes", "y"}:
            continue
        symbol = _clean(row.get("symbol")).upper()
        if symbol:
            result.add(symbol)
    return result


def build_candidates(
    history_path: str | Path,
    *,
    source_path_label: str | None = None,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build a deterministic candidate ledger from one preserved history CSV."""
    path = Path(history_path)
    raw, rows = _read_history(path)
    contract = load_observed_membership_contract(contract_path)
    file_hash = _sha256_bytes(raw)
    label = source_path_label or path.as_posix()
    candidates = [
        candidate_from_row(
            row,
            source_path=label,
            source_row_number=index,
            source_file_sha256=file_hash,
            contract=contract,
        )
        for index, row in enumerate(rows, start=2)
    ]
    return {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "source_path": label,
        "source_file_sha256": f"sha256:{file_hash}",
        "historical_identity_verified": False,
        "absence_interpreted_as_out_of_scope": False,
        "candidates": candidates,
    }


def summarize_candidates(
    candidate_ledger: Mapping[str, Any],
    *,
    current_universe_path: str | Path | None = None,
) -> dict[str, Any]:
    if candidate_ledger.get("schema_version") != CANDIDATE_SCHEMA_VERSION:
        raise ObservedMembershipError("candidate_ledger_schema_invalid")
    candidates = candidate_ledger.get("candidates")
    if not isinstance(candidates, list):
        raise ObservedMembershipError("candidates_must_be_list")

    evidence_counts = Counter(str(row.get("evidence_class")) for row in candidates)
    claim_counts = Counter(str(row.get("membership_claim")) for row in candidates)
    observation_type_counts = Counter(str(row.get("source_observation_type") or "<blank>") for row in candidates)
    data_source_counts = Counter(str(row.get("source_data_source") or "<blank>") for row in candidates)

    scanner_rows = [row for row in candidates if row.get("evidence_class") == "SCANNER_OBSERVED"]
    observed_symbols = {_clean(row.get("observed_symbol")).upper() for row in scanner_rows if _clean(row.get("observed_symbol"))}
    all_dates = [_clean(row.get("as_of_date")) for row in candidates if _clean(row.get("as_of_date"))]
    observed_dates = [_clean(row.get("as_of_date")) for row in scanner_rows if _clean(row.get("as_of_date"))]

    key_counts = Counter(
        (
            _clean(row.get("as_of_date")),
            _clean(row.get("observed_symbol")).upper(),
            _clean(row.get("run_id")),
        )
        for row in scanner_rows
    )
    repeated_keys = [
        {"as_of_date": key[0], "symbol": key[1], "run_id": key[2], "count": count}
        for key, count in sorted(key_counts.items())
        if count > 1
    ]

    summary: dict[str, Any] = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "source_path": candidate_ledger.get("source_path"),
        "source_file_sha256": candidate_ledger.get("source_file_sha256"),
        "row_count": len(candidates),
        "date_min": min(all_dates) if all_dates else None,
        "date_max": max(all_dates) if all_dates else None,
        "scanner_observed_date_min": min(observed_dates) if observed_dates else None,
        "scanner_observed_date_max": max(observed_dates) if observed_dates else None,
        "evidence_class_counts": dict(sorted(evidence_counts.items())),
        "membership_claim_counts": dict(sorted(claim_counts.items())),
        "observation_type_counts": dict(sorted(observation_type_counts.items())),
        "data_source_counts": dict(sorted(data_source_counts.items())),
        "unique_scanner_observed_symbols": len(observed_symbols),
        "unresolved_identity_count": sum(row.get("identity_status") == "UNRESOLVED" for row in candidates),
        "repeated_scanner_observation_key_count": len(repeated_keys),
        "repeated_scanner_observation_keys_sample": repeated_keys[:50],
        "absence_interpreted_as_out_of_scope": False,
        "historical_identity_verified": False,
        "diagnostic_interpretation": "symbol-set differences are review triggers only, not proof of survivorship bias or historical exclusion",
    }

    if current_universe_path is not None:
        current_symbols = _active_current_symbols(Path(current_universe_path))
        summary.update(
            {
                "current_active_symbol_count": len(current_symbols),
                "historical_observed_missing_from_current_master_count": len(observed_symbols - current_symbols),
                "historical_observed_missing_from_current_master": sorted(observed_symbols - current_symbols),
                "current_master_never_scanner_observed_count": len(current_symbols - observed_symbols),
                "current_master_never_scanner_observed": sorted(current_symbols - observed_symbols),
            }
        )
    return summary


def analyze_history(
    history_path: str | Path,
    *,
    current_universe_path: str | Path | None = None,
    source_path_label: str | None = None,
    contract_path: str | Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    ledger = build_candidates(
        history_path,
        source_path_label=source_path_label,
        contract_path=contract_path,
    )
    summary = summarize_candidates(ledger, current_universe_path=current_universe_path)
    return ledger, summary


def scanner_membership_evidence(candidate_ledger: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return only positive observed-presence evidence, still with unresolved identity.

    The result is *not* a strict QM-B membership ledger. Promotion to that ledger
    requires separate stable identity reconciliation and PIT evidence.
    """
    if candidate_ledger.get("schema_version") != CANDIDATE_SCHEMA_VERSION:
        raise ObservedMembershipError("candidate_ledger_schema_invalid")
    return [
        dict(row)
        for row in candidate_ledger.get("candidates", [])
        if row.get("evidence_class") == "SCANNER_OBSERVED"
        and row.get("membership_claim") == "OBSERVED_IN_SCANNER"
    ]
