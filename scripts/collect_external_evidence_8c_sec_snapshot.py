#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scanner.research.external_evidence.sec_edgar import (
    companyfacts_url,
    fetch_sec_json,
    normalize_cik,
    submissions_url,
)
from scanner.research.external_evidence.sec_history import historical_submission_file_specs
from scanner.research.external_evidence.sec_snapshot import (
    SNAPSHOT_SCHEMA,
    write_json_with_digest,
)
from scanner.research.external_evidence.universe_coverage import build_sec_ticker_map, exact_sec_match

ROOT = Path(__file__).resolve().parents[1]
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


class RequestPacer:
    def __init__(self, interval: float) -> None:
        self.interval = max(float(interval), 0.0)
        self.last: float | None = None

    def wait(self) -> None:
        now = time.monotonic()
        if self.last is not None:
            remaining = self.interval - (now - self.last)
            if remaining > 0:
                time.sleep(remaining)
        self.last = time.monotonic()


def fetch(url: str, *, user_agent: str, pacer: RequestPacer, retries: int = 3) -> dict[str, Any]:
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            pacer.wait()
            return fetch_sec_json(url, user_agent=user_agent, timeout=60.0)
        except Exception as exc:
            last = exc
            if attempt < retries:
                time.sleep(2 ** attempt)
    assert last is not None
    raise last


def scanner_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows or "symbol" not in rows[0]:
        raise ValueError("scanner CSV must contain symbol")
    unique: dict[str, dict[str, str]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if symbol and symbol not in unique:
            unique[symbol] = row
    return list(unique.values())


def file_spec(path: str, digest: str, source_url: str) -> dict[str, str]:
    return {"path": path, "sha256": digest, "source_url": source_url}


def collect(
    *,
    scanner_path: Path,
    output_dir: Path,
    user_agent: str,
    minimum_interval_seconds: float,
    max_symbols: int | None,
) -> dict[str, Any]:
    rows = scanner_rows(scanner_path)
    if max_symbols is not None:
        rows = rows[:max_symbols]
    output_dir.mkdir(parents=True, exist_ok=True)
    pacer = RequestPacer(minimum_interval_seconds)

    tickers_payload = fetch(SEC_TICKERS_URL, user_agent=user_agent, pacer=pacer)
    tickers_rel = "raw/company_tickers.json"
    tickers_digest = write_json_with_digest(output_dir / tickers_rel, tickers_payload)
    ticker_map = build_sec_ticker_map(tickers_payload)

    created_at = datetime.now(timezone.utc).isoformat()
    scanner_as_of = rows[0].get("as_of") if rows else None
    companies: list[dict[str, Any]] = []

    for index, row in enumerate(rows, start=1):
        symbol = str(row.get("symbol") or "").strip().upper()
        identity = exact_sec_match(symbol, ticker_map)
        entry: dict[str, Any] = {
            "symbol": symbol,
            "scanner_name": row.get("name"),
            "scanner_as_of": row.get("as_of"),
            "identity_status": "UNKNOWN",
            "reason_codes": list(identity.get("reason_codes") or []),
        }
        if identity.get("sec_match_status") != "KNOWN":
            companies.append(entry)
            print(f"[{index}/{len(rows)}] {symbol}: NO_EXACT_SEC_MATCH", flush=True)
            continue

        cik = normalize_cik(identity["cik"])
        entry.update({"cik": cik, "sec_title": identity.get("sec_title")})
        try:
            primary_url = submissions_url(cik)
            primary = fetch(primary_url, user_agent=user_agent, pacer=pacer)
            current_tickers = {str(x).strip().upper() for x in (primary.get("tickers") or [])}
            if symbol not in current_tickers:
                entry["identity_status"] = "REJECTED_STALE_OR_MISMATCHED_TICKER"
                entry["reason_codes"].append("SEC_SUBMISSIONS_TICKER_MISMATCH")
                companies.append(entry)
                print(f"[{index}/{len(rows)}] {symbol}: TICKER_MISMATCH", flush=True)
                continue

            company_dir = f"raw/companies/{cik}"
            submissions_rel = f"{company_dir}/submissions.json"
            submissions_digest = write_json_with_digest(output_dir / submissions_rel, primary)
            entry["submissions_file"] = file_spec(submissions_rel, submissions_digest, primary_url)

            history_specs: list[dict[str, str]] = []
            for spec in historical_submission_file_specs(primary):
                history_payload = fetch(spec["url"], user_agent=user_agent, pacer=pacer)
                history_rel = f"{company_dir}/history/{spec['name']}"
                digest = write_json_with_digest(output_dir / history_rel, history_payload)
                history_specs.append(file_spec(history_rel, digest, spec["url"]))
            entry["history_files"] = history_specs

            facts_url = companyfacts_url(cik)
            facts = fetch(facts_url, user_agent=user_agent, pacer=pacer)
            facts_rel = f"{company_dir}/companyfacts.json"
            facts_digest = write_json_with_digest(output_dir / facts_rel, facts)
            entry["companyfacts_file"] = file_spec(facts_rel, facts_digest, facts_url)
            entry["identity_status"] = "VERIFIED_BY_SEC_SUBMISSIONS"
            entry["reason_codes"] = []
            print(f"[{index}/{len(rows)}] {symbol}: VERIFIED", flush=True)
        except Exception as exc:
            entry["identity_status"] = "FETCH_ERROR"
            entry["reason_codes"].append("SEC_FETCH_ERROR")
            entry["error_type"] = type(exc).__name__
            entry["error_message"] = str(exc)[:1000]
            print(f"[{index}/{len(rows)}] {symbol}: FETCH_ERROR", flush=True)
        companies.append(entry)

        manifest = {
            "schema_version": SNAPSHOT_SCHEMA,
            "source_authority": "U.S. SEC EDGAR",
            "created_at": created_at,
            "scanner_as_of": scanner_as_of,
            "scanner_source": str(scanner_path),
            "collector_environment": "OUTSIDE_GITHUB_ACTIONS_REQUIRED_IF_SEC_BLOCKS_HOSTED_RUNNERS",
            "company_tickers_file": file_spec(tickers_rel, tickers_digest, SEC_TICKERS_URL),
            "companies": companies,
            "market_outcomes_read": False,
            "direction_assigned": False,
        }
        write_json_with_digest(output_dir / "manifest.json", manifest)

    manifest = {
        "schema_version": SNAPSHOT_SCHEMA,
        "source_authority": "U.S. SEC EDGAR",
        "created_at": created_at,
        "scanner_as_of": scanner_as_of,
        "scanner_source": str(scanner_path),
        "collector_environment": "OUTSIDE_GITHUB_ACTIONS_REQUIRED_IF_SEC_BLOCKS_HOSTED_RUNNERS",
        "company_tickers_file": file_spec(tickers_rel, tickers_digest, SEC_TICKERS_URL),
        "companies": companies,
        "market_outcomes_read": False,
        "direction_assigned": False,
    }
    write_json_with_digest(output_dir / "manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect official SEC files into a deterministic Phase 8C snapshot bundle.")
    parser.add_argument("--scanner", default=str(ROOT / "artifacts" / "research" / "latest_scanner.csv"))
    parser.add_argument("--output-dir", default=str(ROOT / "artifacts" / "external_evidence" / "sec_snapshot"))
    parser.add_argument(
        "--user-agent",
        required=True,
        help="Descriptive SEC User-Agent including project/contact identity.",
    )
    parser.add_argument("--minimum-interval-seconds", type=float, default=0.22)
    parser.add_argument("--max-symbols", type=int)
    args = parser.parse_args()

    result = collect(
        scanner_path=Path(args.scanner),
        output_dir=Path(args.output_dir),
        user_agent=args.user_agent,
        minimum_interval_seconds=args.minimum_interval_seconds,
        max_symbols=args.max_symbols,
    )
    counts: dict[str, int] = {}
    for company in result["companies"]:
        status = str(company.get("identity_status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    print(json.dumps({"scanner_as_of": result.get("scanner_as_of"), "identity_status_counts": counts}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
