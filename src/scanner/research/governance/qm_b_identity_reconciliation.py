"""QM-B historical identity reconciliation candidate builder.

Research-only. It reconciles scanner-observed identifiers against immutable repository
snapshots and the current universe while preserving the distinction between stable
instrument identity, identifier aliases and point-in-time availability.

The output is deliberately a candidate layer. It is not the strict QM-B alias ledger
and it never turns current-only matches into historical truth.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
from datetime import date
import io
import json
from pathlib import Path
import subprocess
from typing import Any, Iterable, Mapping

from scanner.research.governance.qm_b_crypto_identifier import parse_crypto_identifier
from scanner.research.governance.qm_b_observed_membership import build_candidates


SCHEMA_VERSION = "qm_b_identity_reconciliation_candidates_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_identity_reconciliation_v1.json"


class IdentityReconciliationError(ValueError):
    """Raised when identity evidence cannot be reconciled safely."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _clean(value).upper()


def _parse_date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(_clean(value))
    except ValueError as exc:
        raise IdentityReconciliationError(f"invalid_date:{field}:{value}") from exc


def load_identity_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IdentityReconciliationError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_identity_reconciliation_v1":
        raise IdentityReconciliationError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise IdentityReconciliationError("contract_scope_invalid")
    return payload


def is_valid_isin(value: Any) -> bool:
    """Validate an ISIN using syntax plus the ISO 6166 Luhn check digit."""
    isin = _upper(value)
    if len(isin) != 12 or not isin[:2].isalpha() or not isin[-1].isdigit() or not isin.isalnum():
        return False
    digits = "".join(str(ord(ch) - 55) if ch.isalpha() else ch for ch in isin)
    total = 0
    double = False
    for ch in reversed(digits):
        n = int(ch)
        if double:
            n *= 2
            total += n // 10 + n % 10
        else:
            total += n
        double = not double
    return total % 10 == 0


def stable_isin_instrument_id(isin: str) -> str:
    if not is_valid_isin(isin):
        raise IdentityReconciliationError(f"invalid_isin_for_instrument_id:{isin}")
    return f"urn:scanner:isin:{isin.upper()}"


def crypto_family_candidate_id(base: str) -> str:
    return f"candidate:crypto-family:{base.upper()}"


def _read_csv_text(text: str, *, source_id: str) -> list[dict[str, str]]:
    try:
        reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
        if not reader.fieldnames:
            raise IdentityReconciliationError(f"snapshot_header_missing:{source_id}")
        return [{str(k): "" if v is None else str(v) for k, v in row.items()} for row in reader]
    except csv.Error as exc:
        raise IdentityReconciliationError(f"snapshot_csv_invalid:{source_id}") from exc


def _git_show(repo_root: Path, commit: str, path: str) -> str:
    try:
        proc = subprocess.run(
            ["git", "show", f"{commit}:{path}"],
            cwd=repo_root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise IdentityReconciliationError(f"historical_snapshot_unreadable:{commit}:{path}") from exc
    try:
        return proc.stdout.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise IdentityReconciliationError(f"historical_snapshot_not_utf8:{commit}:{path}") from exc


def _snapshot_records(spec: Mapping[str, Any], text: str) -> list[dict[str, Any]]:
    source_id = _clean(spec.get("source_id"))
    schema = _clean(spec.get("schema"))
    snapshot_date = _clean(spec.get("snapshot_date"))
    available = _clean(spec.get("pit_available_from"))
    _parse_date(snapshot_date, field=f"{source_id}.snapshot_date")
    if _parse_date(available, field=f"{source_id}.pit_available_from") <= _parse_date(snapshot_date, field=f"{source_id}.snapshot_date"):
        raise IdentityReconciliationError(f"pit_available_from_not_conservative:{source_id}")

    result: list[dict[str, Any]] = []
    for row_number, row in enumerate(_read_csv_text(text, source_id=source_id), start=2):
        aliases: list[dict[str, str]] = []
        if schema == "universe_master":
            symbol = _upper(row.get("symbol"))
            isin = _upper(row.get("isin"))
            if symbol:
                aliases.append({"identifier_type": "SYMBOL", "identifier_value": symbol})
        elif schema == "legacy_watchlist":
            isin = _upper(row.get("ISIN"))
            for field, identifier_type in (
                ("Ticker", "SYMBOL"),
                ("Symbol", "SYMBOL"),
                ("YahooSymbol", "YAHOO_SYMBOL"),
                ("Yahoo", "YAHOO_SYMBOL"),
            ):
                value = _upper(row.get(field))
                if value and not is_valid_isin(value):
                    aliases.append({"identifier_type": identifier_type, "identifier_value": value})
        else:
            raise IdentityReconciliationError(f"snapshot_schema_unknown:{schema}")

        explicit_isin = isin if is_valid_isin(isin) else ""
        dedup = {(a["identifier_type"], a["identifier_value"]): a for a in aliases}
        if explicit_isin:
            dedup[("ISIN", explicit_isin)] = {"identifier_type": "ISIN", "identifier_value": explicit_isin}
        if not dedup:
            continue
        result.append(
            {
                "source_id": source_id,
                "snapshot_date": snapshot_date,
                "pit_available_from": available,
                "schema": schema,
                "source_row_number": row_number,
                "name": _clean(row.get("name") or row.get("Name")),
                "asset_type": _clean(row.get("asset_type")) or ("crypto" if any(parse_crypto_identifier(a["identifier_value"]) for a in dedup.values()) and not explicit_isin else ""),
                "isin": explicit_isin,
                "aliases": sorted(dedup.values(), key=lambda a: (a["identifier_type"], a["identifier_value"])),
            }
        )
    return result


def load_historical_snapshot_records(
    repo_root: str | Path,
    *,
    contract: Mapping[str, Any] | None = None,
    snapshot_overrides: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    contract = contract or load_identity_contract()
    root = Path(repo_root)
    result: list[dict[str, Any]] = []
    for spec in contract["sources"]["historical_snapshots"]:
        source_id = str(spec["source_id"])
        if snapshot_overrides is not None and source_id in snapshot_overrides:
            text = snapshot_overrides[source_id]
        else:
            text = _git_show(root, str(spec["commit"]), str(spec["path"]))
        result.extend(_snapshot_records(spec, text))
    return result


def _current_records(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError as exc:
        raise IdentityReconciliationError(f"current_universe_unreadable:{path}") from exc
    result: list[dict[str, Any]] = []
    for row_number, row in enumerate(rows, start=2):
        if _clean(row.get("active")).lower() not in {"1", "true", "yes", "y"}:
            continue
        result.append(
            {
                "source_id": "current_universe_master",
                "source_row_number": row_number,
                "symbol": _upper(row.get("symbol")),
                "isin": _upper(row.get("isin")) if is_valid_isin(row.get("isin")) else "",
                "name": _clean(row.get("name")),
                "asset_type": _clean(row.get("asset_type")),
            }
        )
    return result


def _snapshot_indexes(records: Iterable[Mapping[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    by_isin: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_identifier: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        rec = dict(record)
        if rec.get("isin"):
            by_isin[str(rec["isin"])].append(rec)
        for alias in rec.get("aliases", []):
            by_identifier[_upper(alias.get("identifier_value"))].append(rec)
    return by_isin, by_identifier


def _current_indexes(records: Iterable[Mapping[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    by_isin: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        rec = dict(record)
        if rec.get("isin"):
            by_isin[str(rec["isin"])].append(rec)
        if rec.get("symbol"):
            by_symbol[str(rec["symbol"])].append(rec)
    return by_isin, by_symbol


def _unique_isins(records: Iterable[Mapping[str, Any]]) -> set[str]:
    return {str(row.get("isin")) for row in records if row.get("isin")}


def _historical_support_for_isin(records: Iterable[Mapping[str, Any]], isin: str) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in records if row.get("isin") == isin],
        key=lambda row: (row["pit_available_from"], row["source_id"], row["source_row_number"]),
    )


def _alias_pit_boundary(records: Iterable[Mapping[str, Any]], identifier: str, isin: str) -> str | None:
    boundaries: list[str] = []
    for record in records:
        if record.get("isin") != isin:
            continue
        if any(_upper(alias.get("identifier_value")) == identifier for alias in record.get("aliases", [])):
            boundaries.append(str(record["pit_available_from"]))
    return min(boundaries) if boundaries else None


def _classify_observed_identifier(
    identifier: str,
    *,
    snapshot_by_isin: Mapping[str, list[dict[str, Any]]],
    snapshot_by_identifier: Mapping[str, list[dict[str, Any]]],
    current_by_isin: Mapping[str, list[dict[str, Any]]],
    current_by_symbol: Mapping[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if is_valid_isin(identifier):
        historical_targets = _unique_isins(snapshot_by_isin.get(identifier, []))
        if len(historical_targets) == 1:
            return {
                "candidate_class": "HISTORICAL_ISIN_SNAPSHOT_MATCH",
                "candidate_instrument_id": stable_isin_instrument_id(identifier),
                "canonical_isin": identifier,
                "identity_status": "VERIFIED",
                "reason_codes": ["OBSERVED_IDENTIFIER_VALID_ISIN", "EXPLICIT_HISTORICAL_SNAPSHOT_ISIN_MATCH"],
            }
        current_targets = _unique_isins(current_by_isin.get(identifier, []))
        if len(current_targets) == 1:
            return {
                "candidate_class": "CURRENT_ISIN_ONLY_MATCH",
                "candidate_instrument_id": stable_isin_instrument_id(identifier),
                "canonical_isin": identifier,
                "identity_status": "PARTIAL",
                "reason_codes": ["OBSERVED_IDENTIFIER_VALID_ISIN", "CURRENT_ONLY_ISIN_MATCH_NOT_HISTORICAL_PROOF"],
            }

    historical = snapshot_by_identifier.get(identifier, [])
    historical_isins = _unique_isins(historical)
    if len(historical_isins) == 1:
        isin = next(iter(historical_isins))
        return {
            "candidate_class": "HISTORICAL_SYMBOL_SNAPSHOT_MATCH",
            "candidate_instrument_id": stable_isin_instrument_id(isin),
            "canonical_isin": isin,
            "identity_status": "VERIFIED",
            "reason_codes": ["EXACT_HISTORICAL_IDENTIFIER_MATCH", "HISTORICAL_SNAPSHOT_EXPLICIT_ISIN"],
        }
    if len(historical_isins) > 1:
        return {
            "candidate_class": "AMBIGUOUS",
            "candidate_instrument_id": None,
            "canonical_isin": None,
            "identity_status": "CONFLICTING",
            "reason_codes": ["HISTORICAL_IDENTIFIER_MAPS_TO_MULTIPLE_ISINS"],
        }

    current = current_by_symbol.get(identifier, [])
    current_isins = _unique_isins(current)
    if len(current_isins) == 1:
        isin = next(iter(current_isins))
        return {
            "candidate_class": "CURRENT_SYMBOL_ONLY_MATCH",
            "candidate_instrument_id": stable_isin_instrument_id(isin),
            "canonical_isin": isin,
            "identity_status": "PARTIAL",
            "reason_codes": ["EXACT_CURRENT_SYMBOL_MATCH", "CURRENT_ONLY_MATCH_NOT_HISTORICAL_PROOF"],
        }
    if len(current_isins) > 1:
        return {
            "candidate_class": "AMBIGUOUS",
            "candidate_instrument_id": None,
            "canonical_isin": None,
            "identity_status": "CONFLICTING",
            "reason_codes": ["CURRENT_SYMBOL_MAPS_TO_MULTIPLE_ISINS"],
        }

    parsed = parse_crypto_identifier(identifier)
    if parsed:
        base = parsed["base"]
        crypto_current = []
        for rows in current_by_symbol.values():
            for row in rows:
                if _clean(row.get("asset_type")).lower() != "crypto":
                    continue
                current_parsed = parse_crypto_identifier(row.get("symbol"))
                if current_parsed and current_parsed["base"] == base:
                    crypto_current.append(row)
        if crypto_current:
            return {
                "candidate_class": "CRYPTO_BASE_LINEAGE",
                "candidate_instrument_id": crypto_family_candidate_id(base),
                "canonical_isin": None,
                "identity_status": "PARTIAL",
                "crypto_base": base,
                "reason_codes": ["SHARED_CRYPTO_BASE", "CRYPTO_QUOTE_PAIR_EQUIVALENCE_NOT_VERIFIED"],
            }

    return {
        "candidate_class": "UNRESOLVED",
        "candidate_instrument_id": None,
        "canonical_isin": None,
        "identity_status": "UNKNOWN",
        "reason_codes": ["NO_UNAMBIGUOUS_IDENTITY_EVIDENCE"],
    }


def build_identity_reconciliation(
    history_path: str | Path,
    current_universe_path: str | Path,
    *,
    repo_root: str | Path,
    contract_path: str | Path | None = None,
    snapshot_overrides: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    contract = load_identity_contract(contract_path)
    snapshot_records = load_historical_snapshot_records(
        repo_root,
        contract=contract,
        snapshot_overrides=snapshot_overrides,
    )
    current_records = _current_records(Path(current_universe_path))
    snapshot_by_isin, snapshot_by_identifier = _snapshot_indexes(snapshot_records)
    current_by_isin, current_by_symbol = _current_indexes(current_records)

    history = build_candidates(Path(history_path), source_path_label=Path(history_path).as_posix())
    scanner_rows = [
        row
        for row in history["candidates"]
        if row.get("evidence_class") == "SCANNER_OBSERVED"
        and row.get("membership_claim") == "OBSERVED_IN_SCANNER"
    ]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in scanner_rows:
        grouped[_upper(row.get("observed_symbol"))].append(row)

    reconciled: list[dict[str, Any]] = []
    for identifier, rows in sorted(grouped.items()):
        classification = _classify_observed_identifier(
            identifier,
            snapshot_by_isin=snapshot_by_isin,
            snapshot_by_identifier=snapshot_by_identifier,
            current_by_isin=current_by_isin,
            current_by_symbol=current_by_symbol,
        )
        dates = sorted(_clean(row.get("as_of_date")) for row in rows)
        isin = classification.get("canonical_isin")
        pit_boundary = None
        if isin:
            pit_boundary = _alias_pit_boundary(snapshot_records, identifier, str(isin))
            if pit_boundary is None and is_valid_isin(identifier):
                historical_support = _historical_support_for_isin(snapshot_records, identifier)
                pit_boundary = historical_support[0]["pit_available_from"] if historical_support else None
        pit_verified_count = 0
        if pit_boundary:
            boundary_date = _parse_date(pit_boundary, field="pit_boundary")
            pit_verified_count = sum(_parse_date(d, field="observation_date") >= boundary_date for d in dates)
        source_ids: list[str] = []
        if isin:
            source_ids = sorted({row["source_id"] for row in snapshot_records if row.get("isin") == isin})
        elif classification.get("candidate_class") == "CRYPTO_BASE_LINEAGE":
            source_ids = sorted({row["source_id"] for row in snapshot_records if any((parse_crypto_identifier(alias.get("identifier_value")) or {}).get("base") == classification.get("crypto_base") for alias in row.get("aliases", []))})

        reconciled.append(
            {
                "observed_identifier": identifier,
                "first_seen": dates[0],
                "last_seen": dates[-1],
                "scanner_observation_count": len(rows),
                "names": sorted({_clean(row.get("name")) for row in rows if _clean(row.get("name"))}),
                "currencies": sorted({_clean(row.get("currency")) for row in rows if _clean(row.get("currency"))}),
                **classification,
                "historical_evidence_source_ids": source_ids,
                "pit_alias_available_from": pit_boundary,
                "pit_verified_observation_count": pit_verified_count,
                "pit_unverified_observation_count": len(rows) - pit_verified_count,
                "strict_alias_promoted": False,
                "strict_membership_promoted": False,
            }
        )

    instruments: dict[str, dict[str, Any]] = {}
    for row in reconciled:
        instrument_id = row.get("candidate_instrument_id")
        if not instrument_id:
            continue
        status = str(row["identity_status"])
        existing = instruments.get(str(instrument_id))
        if existing is None:
            instruments[str(instrument_id)] = {
                "instrument_id": instrument_id,
                "canonical_isin": row.get("canonical_isin"),
                "crypto_base": row.get("crypto_base"),
                "identity_status": status,
                "source_ids": list(row["historical_evidence_source_ids"]),
                "observed_identifiers": [row["observed_identifier"]],
                "strict_historical_usable": False,
            }
        else:
            existing["observed_identifiers"] = sorted(set(existing["observed_identifiers"]) | {row["observed_identifier"]})
            existing["source_ids"] = sorted(set(existing["source_ids"]) | set(row["historical_evidence_source_ids"]))
            if "CONFLICTING" in {existing["identity_status"], status}:
                existing["identity_status"] = "CONFLICTING"
            elif "PARTIAL" in {existing["identity_status"], status}:
                existing["identity_status"] = "PARTIAL"

    alias_seeds: list[dict[str, Any]] = []
    seen_alias_seed: set[tuple[str, str, str, str]] = set()
    alias_targets: dict[tuple[str, str], set[str]] = defaultdict(set)
    raw_alias_candidates: list[tuple[str, str, str, str, str]] = []
    for record in snapshot_records:
        isin = str(record.get("isin") or "")
        if not isin:
            continue
        instrument_id = stable_isin_instrument_id(isin)
        for alias in record.get("aliases", []):
            identifier_type = str(alias["identifier_type"])
            value = _upper(alias["identifier_value"])
            alias_targets[(identifier_type, value)].add(instrument_id)
            raw_alias_candidates.append(
                (instrument_id, identifier_type, value, str(record["pit_available_from"]), str(record["source_id"]))
            )
    conflicting_aliases = {key for key, targets in alias_targets.items() if len(targets) > 1}
    for instrument_id, identifier_type, value, valid_from, source_id in sorted(raw_alias_candidates):
        key = (identifier_type, value)
        if key in conflicting_aliases:
            continue
        dedup_key = (instrument_id, identifier_type, value, valid_from)
        if dedup_key in seen_alias_seed:
            continue
        seen_alias_seed.add(dedup_key)
        alias_seeds.append(
            {
                "instrument_id": instrument_id,
                "identifier_type": identifier_type,
                "identifier_value": value,
                "valid_from": valid_from,
                "valid_to": None,
                "source_id": source_id,
                "pit_verified": True,
                "status": "KNOWN",
                "candidate_only": True,
                "promotion_requires_interval_review": True,
            }
        )

    counts = Counter(str(row["candidate_class"]) for row in reconciled)
    identity_counts = Counter(str(row["identity_status"]) for row in reconciled)
    unresolved = [row["observed_identifier"] for row in reconciled if row["candidate_class"] in {"UNRESOLVED", "AMBIGUOUS"}]
    total_obs = sum(row["scanner_observation_count"] for row in reconciled)
    pit_obs = sum(row["pit_verified_observation_count"] for row in reconciled)

    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "strict_alias_ledger": False,
        "strict_membership_ledger": False,
        "absence_interpreted_as_out_of_scope": False,
        "current_only_match_verifies_historical_identity": False,
        "source_history_sha256": history.get("source_file_sha256"),
        "historical_snapshot_count": len(contract["sources"]["historical_snapshots"]),
        "historical_snapshot_record_count": len(snapshot_records),
        "scanner_observation_count": total_obs,
        "unique_observed_identifier_count": len(reconciled),
        "candidate_class_counts": dict(sorted(counts.items())),
        "identity_status_counts": dict(sorted(identity_counts.items())),
        "pit_verified_observation_count": pit_obs,
        "pit_unverified_observation_count": total_obs - pit_obs,
        "candidate_instrument_count": len(instruments),
        "pit_alias_seed_count": len(alias_seeds),
        "conflicting_snapshot_alias_count": len(conflicting_aliases),
        "conflicting_snapshot_aliases": [
            {"identifier_type": key[0], "identifier_value": key[1], "instrument_ids": sorted(alias_targets[key])}
            for key in sorted(conflicting_aliases)
        ],
        "unresolved_or_ambiguous_identifiers": unresolved,
        "instruments": sorted(instruments.values(), key=lambda row: str(row["instrument_id"])),
        "pit_alias_seeds": alias_seeds,
        "reconciled_identifiers": reconciled,
    }
