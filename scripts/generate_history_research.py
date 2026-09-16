"""Read-only research export; no scanner imports, inference, or deduplication."""

from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from zoneinfo import ZoneInfo


EXPECTED_COLUMNS = (
    "date symbol name close currency score rank universe_size rank_percentile r_code rs3m trend200 cycle "
    "confidence opportunity risk sector pillar_primary cluster_official "
    "bucket_type scoring_version run_id universe_version config_version"
).split()
SOURCE = Path("artifacts/snapshots/score_history.csv")
OUTPUT = Path("artifacts/research")


def previous_month(today: date) -> tuple[date, date]:
    end = today.replace(day=1) - timedelta(days=1)
    return end.replace(day=1), end


def missing_days(start: date, end: date, observed: set[date]) -> list[str]:
    return [(start + timedelta(days=i)).isoformat()
            for i in range((end - start).days + 1)
            if start + timedelta(days=i) not in observed]


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temp = Path(handle.name)
        try:
            handle.write(content)
        except BaseException:
            handle.close()
            temp.unlink(missing_ok=True)
            raise
    try:
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def csv_bytes(columns: list[str], rows: list[list[str]]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def build_research(root: Path, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    today = now.astimezone(ZoneInfo("Europe/Berlin")).date()
    start, end = previous_month(today)
    source = root / SOURCE
    raw = source.read_bytes()
    records = list(csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""), strict=True))
    if not records or len(records) < 2:
        raise ValueError("History is empty or contains only a header")
    columns, rows = records[0], records[1:]
    if (len(set(columns)) != len(columns) or any(not c.strip() for c in columns)
            or not {"date", "symbol"}.issubset(columns)):
        raise ValueError("History requires unique column names including date and symbol")
    di, si = columns.index("date"), columns.index("symbol")
    dates = []
    for number, row in enumerate(rows, start=2):
        if len(row) != len(columns):
            raise ValueError(f"History row {number}: incorrect number of fields")
        parsed = date.fromisoformat(row[di])
        if parsed.isoformat() != row[di] or not row[si].strip():
            raise ValueError(f"History row {number}: invalid date or empty symbol")
        if parsed > today:
            raise ValueError(f"History row {number}: future date {parsed}")
        dates.append(parsed)
    observed = set(dates)
    oldest, newest = min(dates), max(dates)
    missing = missing_days(start, end, observed)
    problems = []
    if missing:
        problems.append(f"Previous month {start:%Y-%m} is incomplete; missing daily observations: "
                        + ", ".join(missing))
    if (today - newest).days > 3:
        problems.append(f"History is stale: newest date {newest}, Berlin date {today} (limit: 3 days)")

    # Keep every field and row, including conflicting observations of the same key.
    research = csv_bytes(columns, rows)
    monthly = {}
    skipped = {}
    if not problems:
        # Backfill only fully covered calendar months. Partial early months stay
        # accessible in history_research.csv and are explicitly listed below.
        month = oldest.replace(day=1)
        while month <= start:
            next_month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
            gaps = missing_days(month, next_month - timedelta(days=1), observed)
            key = month.strftime("%Y-%m")
            if gaps:
                skipped[key] = gaps
            else:
                selected = [r for r, d in zip(rows, dates) if month <= d < next_month]
                monthly[key] = csv_bytes(columns, selected)
            month = next_month

    metadata = {
        "generated_at": now.astimezone(timezone.utc).isoformat(),
        "source_file": SOURCE.as_posix(),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "source_size_bytes": len(raw),
        "source_oldest_date": oldest.isoformat(),
        "source_newest_date": newest.isoformat(),
        "source_row_count": len(rows),
        "research_row_count": len(rows),
        "unique_symbols": len({r[si] for r in rows}),
        "covered_period": {"start": oldest.isoformat(), "end": newest.isoformat()},
        "latest_completed_month": start.strftime("%Y-%m"),
        "columns": columns,
        "missing_expected_columns": [c for c in EXPECTED_COLUMNS if c not in columns],
        "research_size_bytes": len(research),
        "deduplication": "none; all source observations and their order are retained",
        "duplicate_date_symbol_rows": len(rows) - len({(r[di], r[si]) for r in rows}),
        "validation": {
            "status": "blocked" if problems else "passed",
            "errors": problems,
            "policy": "Every calendar day in previous month; newest date at most 3 days old. "
                      "Date coverage is not a guarantee of per-symbol completeness.",
            "previous_month_missing_dates": missing,
            "source_missing_dates": missing_days(oldest, newest, observed),
        },
        "monthly_exports": sorted(monthly),
        "skipped_incomplete_months": skipped,
    }
    rank_fields = ("rank", "universe_size", "rank_percentile")
    rank_indexes = [
        i for i, row in enumerate(rows)
        if all(field in columns and row[columns.index(field)].strip() for field in rank_fields)
    ]
    metadata["rank_data"] = {
        "fields": list(rank_fields),
        "first_observed_date": min((dates[i] for i in rank_indexes), default=None).isoformat()
        if rank_indexes else None,
        "first_observed_run_id": (
            rows[min(rank_indexes)][columns.index("run_id")].strip()
            if rank_indexes and "run_id" in columns else None
        ),
        "observed_row_count": len(rank_indexes),
    }
    for version in ("scoring_version", "universe_version", "config_version"):
        metadata[version + "s"] = (sorted({r[columns.index(version)] for r in rows
                                           if r[columns.index(version)].strip()})
                                    if version in columns else [])
    # Validate and serialize everything before writing any output. The source
    # hash also detects a concurrently updated archive before publication.
    if hashlib.sha256(source.read_bytes()).hexdigest() != metadata["source_sha256"]:
        raise ValueError("History changed during export; retry from a stable checkout")
    output = root / OUTPUT
    targets = [output / "history_research.csv", output / "history_research_metadata.json"]
    targets += [output / "monthly" / f"history_monthly_{key}.csv" for key in monthly]
    # Do not follow a pre-existing output symlink outside the research directory.
    for target in targets:
        if target.is_symlink() or not target.resolve().is_relative_to((root / "artifacts" / "research").absolute()):
            raise ValueError(f"Unsafe research output path: {target}")
    atomic_write(targets[0], research)
    for key, content in monthly.items():
        atomic_write(output / "monthly" / f"history_monthly_{key}.csv", content)
    atomic_write(targets[1], (json.dumps(metadata, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
    return metadata


def main() -> int:
    try:
        metadata = build_research(Path(__file__).resolve().parents[1])
        report = json.dumps(metadata, indent=2, ensure_ascii=False)
        failed = metadata["validation"]["status"] == "blocked"
    except (OSError, ValueError, csv.Error) as exc:
        report, failed = f"Research export failed: {exc}", True
    print(report)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write("## Monthly history research\n\n```text\n" + report + "\n```\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
