from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping


COVERAGE_SCHEMA = "external_evidence_8d_finra_short_interest_coverage_v1"
SNAPSHOT_SCHEMA = "external_evidence_8d_finra_short_interest_snapshot_v1"


class FinraShortInterestCoverageError(ValueError):
    """Raised when the Phase 8D-A2 coverage audit cannot be performed safely."""


def _load_snapshot(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise FinraShortInterestCoverageError(f"cannot read FINRA snapshot: {path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SNAPSHOT_SCHEMA:
        raise FinraShortInterestCoverageError("unsupported FINRA snapshot schema")
    guards = payload.get("guards") or {}
    if guards.get("market_outcomes_read") is not False:
        raise FinraShortInterestCoverageError("coverage audit refuses outcome-contaminated FINRA evidence")
    if guards.get("market_direction_assigned") is not False:
        raise FinraShortInterestCoverageError("coverage audit refuses direction-assigned FINRA evidence")
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        raise FinraShortInterestCoverageError("FINRA snapshot rows must be a non-empty array")
    settlement_dates = {
        str(row.get("settlement_date") or "").strip()
        for row in rows
        if isinstance(row, Mapping)
    }
    if "" in settlement_dates or len(settlement_dates) != 1:
        raise FinraShortInterestCoverageError(
            "8D-A2 requires exactly one FINRA settlement date; no implicit latest-period selection is allowed"
        )
    return payload


def _load_scanner(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"symbol", "date", "name", "as_of", "snapshot_id"}
        fields = set(reader.fieldnames or [])
        missing = sorted(required - fields)
        if missing:
            raise FinraShortInterestCoverageError(
                "scanner CSV missing required columns: " + ", ".join(missing)
            )
        rows = [dict(row) for row in reader]
    if not rows:
        raise FinraShortInterestCoverageError("scanner CSV contains no rows")

    symbols = [str(row.get("symbol") or "").strip().upper() for row in rows]
    if any(not symbol for symbol in symbols):
        raise FinraShortInterestCoverageError("scanner CSV contains blank symbols")
    duplicates = sorted(symbol for symbol, count in Counter(symbols).items() if count > 1)
    if duplicates:
        raise FinraShortInterestCoverageError(
            f"scanner snapshot contains duplicate symbols: {duplicates[:10]}"
        )

    snapshot_ids = {str(row.get("snapshot_id") or "").strip() for row in rows}
    as_of_values = {str(row.get("as_of") or "").strip() for row in rows}
    if len(snapshot_ids) != 1 or "" in snapshot_ids:
        raise FinraShortInterestCoverageError("scanner rows must belong to exactly one snapshot_id")
    if len(as_of_values) != 1 or "" in as_of_values:
        raise FinraShortInterestCoverageError("scanner rows must share exactly one as_of value")
    return rows


def audit_finra_current_universe(
    *,
    scanner_path: Path,
    finra_snapshot_path: Path,
) -> dict[str, Any]:
    scanner_rows = _load_scanner(scanner_path)
    snapshot = _load_snapshot(finra_snapshot_path)

    settlement_date = next(
        str(row.get("settlement_date"))
        for row in snapshot["rows"]
        if isinstance(row, Mapping)
    )
    finra_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in snapshot["rows"]:
        if not isinstance(raw, Mapping):
            continue
        symbol = str(raw.get("symbol") or "").strip().upper()
        if symbol:
            finra_by_symbol[symbol].append(dict(raw))

    coverage_rows: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    known_feature_counts: Counter[str] = Counter()

    for scanner in scanner_rows:
        symbol = str(scanner.get("symbol") or "").strip().upper()
        matches = finra_by_symbol.get(symbol, [])
        base = {
            "scanner_date": scanner.get("date"),
            "scanner_as_of": scanner.get("as_of"),
            "scanner_snapshot_id": scanner.get("snapshot_id"),
            "symbol": symbol,
            "name": scanner.get("name"),
            "currency": scanner.get("currency"),
            "match_method": "EXACT_SYMBOL_ONLY",
            "market_direction": "UNASSIGNED",
        }

        if not matches:
            row = {
                **base,
                "coverage_status": "UNKNOWN_NOT_IN_FINRA_SNAPSHOT",
                "match_count": 0,
                "reason_codes": ["NO_EXACT_SYMBOL_MATCH"],
                "settlement_date": None,
                "valid_from": None,
                "strict_pit_eligible": None,
                "vintage_status": None,
                "short_position_quantity": None,
                "short_position_change_quantity": None,
                "short_position_change_percent": None,
                "days_to_cover": None,
            }
        elif len(matches) > 1:
            row = {
                **base,
                "coverage_status": "AMBIGUOUS_MULTIPLE_FINRA_ROWS",
                "match_count": len(matches),
                "reason_codes": ["MULTIPLE_EXACT_SYMBOL_ROWS", "NO_AUTOMATIC_MARKET_CLASS_SELECTION"],
                "settlement_date": None,
                "valid_from": None,
                "strict_pit_eligible": None,
                "vintage_status": None,
                "short_position_quantity": None,
                "short_position_change_quantity": None,
                "short_position_change_percent": None,
                "days_to_cover": None,
                "candidate_market_class_codes": sorted(
                    {str(match.get("market_class_code") or "") for match in matches}
                ),
            }
        else:
            match = matches[0]
            row = {
                **base,
                "coverage_status": "KNOWN_EXACT_SYMBOL",
                "match_count": 1,
                "reason_codes": [],
                "market_class_code": match.get("market_class_code"),
                "settlement_date": match.get("settlement_date"),
                "published_at": match.get("published_at"),
                "valid_from": match.get("valid_from"),
                "strict_pit_eligible": match.get("strict_pit_eligible"),
                "vintage_status": match.get("vintage_status"),
                "revision_flag": match.get("revision_flag"),
                "stock_split_flag": match.get("stock_split_flag"),
                "feature_status": match.get("feature_status"),
                "short_position_quantity": match.get("short_position_quantity"),
                "short_position_change_quantity": match.get("short_position_change_quantity"),
                "short_position_change_percent": match.get("short_position_change_percent"),
                "days_to_cover": match.get("days_to_cover"),
            }
            for feature in (
                "short_position_quantity",
                "short_position_change_quantity",
                "short_position_change_percent",
                "days_to_cover",
            ):
                if match.get(feature) is not None:
                    known_feature_counts[feature] += 1

        status_counts[row["coverage_status"]] += 1
        coverage_rows.append(row)

    scanner_as_of = str(scanner_rows[0]["as_of"])
    scanner_snapshot_id = str(scanner_rows[0]["snapshot_id"])
    known = status_counts["KNOWN_EXACT_SYMBOL"]
    total = len(scanner_rows)
    return {
        "schema_version": COVERAGE_SCHEMA,
        "phase": "8D_A2_FINRA_current_universe_coverage",
        "scanner_source": str(scanner_path),
        "scanner_as_of": scanner_as_of,
        "scanner_snapshot_id": scanner_snapshot_id,
        "finra_snapshot_source": str(finra_snapshot_path),
        "finra_settlement_date": settlement_date,
        "finra_published_at": snapshot.get("published_at"),
        "finra_source_mode": snapshot.get("source_mode"),
        "finra_strict_pit_eligible": snapshot.get("strict_pit_eligible"),
        "scanner_symbol_count": total,
        "exact_known_symbol_count": known,
        "exact_known_symbol_fraction": (known / total) if total else 0.0,
        "status_counts": dict(sorted(status_counts.items())),
        "known_feature_counts": dict(sorted(known_feature_counts.items())),
        "rows": coverage_rows,
        "guards": {
            "single_finra_settlement_date_required": True,
            "exact_symbol_only": True,
            "ticker_suffix_stripping_enabled": False,
            "adr_substitution_enabled": False,
            "fuzzy_name_matching_enabled": False,
            "ambiguous_market_class_auto_selection_enabled": False,
            "missing_evidence_defaults_to_neutral": False,
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def write_coverage(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
