"""CY-02: fail-closed audit of the current (not historical) cycle publication."""
from __future__ import annotations

import argparse
import csv
import gzip
import pandas as pd
from scanner.data.enrich.cycle_oscillator import calculate_cycle
from scanner.data.enrich.yahoo_prices import _looks_like_crypto_pair
from datetime import datetime, date
import math
from pathlib import Path
import re
from collections import Counter

FORMULA = "cycle_detrended_sma20_range40_v1"
SOURCE = "YAHOO_PIT_CYCLE_V1"
QUALITY = {"VALID", "MISSING_SOURCE", "INVALID_VALUE", "STALE", "INSUFFICIENT_HISTORY"}
FIELDS = ("asset_id", "cycle", "cycle_quality", "cycle_source",
          "cycle_formula_version", "cycle_currency", "cycle_currency_lineage",
          "cycle_session_time_quality", "cycle_last_bar", "cycle_as_of",
          "cycle_price_sha256", "cycle_price_symbol", "cycle_quality_reason")
HEX = re.compile(r"^[a-f0-9]{64}$")
NULL = {"", "nan", "null", "none", "na", "n/a"}


def _missing(value: object) -> bool:
    return str(value or "").strip().lower() in NULL


def audit_rows(rows: list[dict[str, str]], *, columns: list[str]) -> tuple[list[str], dict]:
    errors: list[str] = []
    lacking = set(FIELDS) - set(columns)
    if lacking:
        return ["missing_cycle_columns:" + ",".join(sorted(lacking))], {}
    counts = Counter()
    ids = set()
    if not rows:
        return ["empty_current_scanner"], {}
    for index, row in enumerate(rows, start=2):
        ident = row.get("asset_id", "").strip()
        if not ident or ident in ids:
            errors.append(f"row_{index}:missing_or_duplicate_asset_id:{ident}")
        ids.add(ident)
        q = row["cycle_quality"].strip().upper()
        counts[q] += 1
        if q not in QUALITY:
            errors.append(f"{ident}:unexpected_cycle_quality:{q}")
            continue
        raw = row["cycle"].strip()
        valid = q == "VALID"
        if valid:
            try:
                value = float(raw)
            except ValueError:
                value = float("nan")
            if not math.isfinite(value) or not 0 <= value <= 100:
                errors.append(f"{ident}:valid_cycle_not_finite_or_in_range")
            if row["cycle_source"] != SOURCE or row["cycle_formula_version"] != FORMULA:
                errors.append(f"{ident}:wrong_cycle_source_or_formula")
            if not row["cycle_currency"].strip() or row["cycle_currency_lineage"] != "WATCHLIST_DECLARED_ONLY":
                errors.append(f"{ident}:quote_currency_without_declared_lineage")
            if row["cycle_session_time_quality"] != "SESSION_DATE_CUTOFF_ONLY":
                errors.append(f"{ident}:session_date_not_caveated")
            if not row["cycle_price_symbol"].strip():
                errors.append(f"{ident}:missing_price_symbol")
            if not HEX.fullmatch(row["cycle_price_sha256"].strip()):
                errors.append(f"{ident}:invalid_cycle_price_sha256")
            try:
                last = date.fromisoformat(row["cycle_last_bar"])
                as_of = datetime.fromisoformat(row["cycle_as_of"])
                if as_of.tzinfo is None or not last < as_of.date():
                    raise ValueError("not a completed prior UTC session")
            except (TypeError, ValueError):
                errors.append(f"{ident}:missing_or_future_cycle_bar")
        else:
            if not _missing(raw):
                errors.append(f"{ident}:nonvalid_cycle_resurrected:{raw}")
            if not _missing(row["cycle_price_sha256"]):
                errors.append(f"{ident}:nonvalid_cycle_has_price_hash")
    return errors, {"asset_count": len(rows), "quality_counts": dict(sorted(counts.items()))}



def audit_price_windows(rows: list[dict[str, str]], bars_path: Path) -> list[str]:
    """Recompute every VALID cycle from the precise 60 stored provider closes.

    Current-run only; no claim that a retrospective Yahoo adjustment equals
    the actual historical provider payload of an old snapshot.
    """
    if not bars_path.is_file():
        return ["missing_cycle_bar_evidence"]
    try:
        with gzip.open(bars_path, "rt", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            expected_cols = {"symbol", "currency", "formula_version", "price_sha256",
                             "as_of", "session_date", "close"}
            if not expected_cols.issubset(reader.fieldnames or []):
                return ["cycle_bar_evidence_missing_columns"]
            evidence = list(reader)
    except (OSError, UnicodeError, csv.Error):
        return ["cycle_bar_evidence_unreadable"]

    groups: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for bar in evidence:
        key = (bar["symbol"], bar["currency"], bar["price_sha256"])
        groups.setdefault(key, []).append(bar)

    errors = []
    used = set()
    for row in rows:
        if row.get("cycle_quality", "").strip().upper() != "VALID":
            continue
        symbol = row["cycle_price_symbol"].strip()
        currency = row["cycle_currency"].strip()
        sha = row["cycle_price_sha256"].strip()
        key = (symbol, currency, sha)
        used.add(key)
        sample = groups.get(key)
        ident = row.get("asset_id") or symbol
        if sample is None or len(sample) != 60:
            errors.append(f"{ident}:missing_or_incomplete_60_bar_window")
            continue
        if any(bar["formula_version"] != row["cycle_formula_version"] or
               bar["as_of"] != row["cycle_as_of"] for bar in sample):
            errors.append(f"{ident}:bar_window_formula_or_as_of_mismatch")
            continue
        try:
            dates = pd.to_datetime([bar["session_date"] for bar in sample], errors="raise")
            closes = pd.Series([float(bar["close"]) for bar in sample], index=dates)
            actual = calculate_cycle(
                closes, symbol=symbol, currency=currency,
                is_crypto=_looks_like_crypto_pair(symbol),
                as_of=datetime.fromisoformat(row["cycle_as_of"]),
            )
        except (ValueError, TypeError, OverflowError) as exc:
            errors.append(f"{ident}:bar_window_unreplayable:{type(exc).__name__}")
            continue
        if (actual.get("cycle_quality") != "VALID"
                or actual.get("cycle_price_sha256") != sha
                or actual.get("cycle_last_bar") != row.get("cycle_last_bar")):
            errors.append(f"{ident}:bar_window_fingerprint_or_date_mismatch")
            continue
        try:
            if float(row["cycle"]) != float(actual["Zyklus %"]):
                errors.append(f"{ident}:bar_window_cycle_replay_mismatch")
        except ValueError:
            errors.append(f"{ident}:bar_window_cycle_not_numeric")
    for key in set(groups) - used:
        errors.append(f"orphan_cycle_bars:{key[0]}")
    return errors


def audit_csv(path: Path, *, report_path: Path | None = None,
              bars_path: Path | None = None) -> tuple[list[str], dict]:
    if not path.is_file():
        return [f"missing_current_scanner:{path}"], {}
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        errors, summary = audit_rows(rows, columns=reader.fieldnames or [])
    if report_path is not None:
        if not report_path.is_file():
            errors.append("missing_cycle_quality_report")
        else:
            with report_path.open(encoding="utf-8", newline="") as f:
                report = list(csv.DictReader(f))
            if len(rows) != len(report):
                errors.append(f"cycle_quality_report_count_mismatch:{len(rows)}:{len(report)}")
            else:
                for i, (row, audit) in enumerate(zip(rows, report), start=2):
                    for k in ("asset_id", "cycle_quality", "cycle_source", "cycle_price_sha256"):
                        if str(row.get(k, "")).strip() != str(audit.get(k, "")).strip():
                            errors.append(f"row_{i}:cycle_quality_report_mismatch:{k}")
                            break
    if bars_path is not None:
        errors.extend(audit_price_windows(rows, bars_path))
    return errors, summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("artifacts/watchlist/watchlist_full.csv"))
    parser.add_argument("--report", type=Path, default=Path("artifacts/reports/cycle_quality.csv"))
    parser.add_argument("--bars", type=Path, default=Path("artifacts/reports/cycle_input_bars.csv.gz"))
    opts = parser.parse_args()
    errors, summary = audit_csv(opts.source, report_path=opts.report, bars_path=opts.bars)
    print(f"CY-02 audit: {summary}")
    for msg in errors[:30]:
        print("CY-02 BLOCKER: " + msg)
    if len(errors) > 30:
        print(f"CY-02 BLOCKER: {len(errors)-30} more errors")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
