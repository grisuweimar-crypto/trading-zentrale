#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any

from scanner.research.external_evidence.fundamental_change import (
    build_debt_to_equity,
    build_derived_yoy_changes,
    build_free_cash_flow,
    build_growth_acceleration,
    build_operating_margin,
    build_yoy_changes,
    first_release_rows,
    resolve_debt_values,
)
from scanner.research.external_evidence.fundamental_coverage import measure_concept_coverage
from scanner.research.external_evidence.sec_edgar import (
    build_acceptance_index,
    companyfacts_rows,
    companyfacts_url,
    fetch_sec_json,
    normalize_cik,
    submissions_url,
)
from scanner.research.external_evidence.sec_history import (
    assemble_full_submission_history,
    companyfacts_accession_coverage,
    historical_submission_file_specs,
)
from scanner.research.external_evidence.universe_coverage import (
    aggregate_company_coverage,
    build_sec_ticker_map,
    exact_sec_match,
)

ROOT = Path(__file__).resolve().parents[1]
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
# Transport fallback only. Every candidate CIK from this mirror must be revalidated
# against the current official data.sec.gov submissions payload before it is used.
TICKER_BOOTSTRAP_MIRROR_URL = (
    "https://gist.githubusercontent.com/Philfal1234/"
    "02a1b7649736e5b8b80d3aece43d8656/raw/"
    "e8c96213400618831229f5a9c4559026cbf22359/company_tickers.json"
)


class RequestPacer:
    def __init__(self, minimum_interval_seconds: float) -> None:
        self.minimum_interval_seconds = max(float(minimum_interval_seconds), 0.0)
        self.last_request_monotonic: float | None = None

    def wait(self) -> None:
        now = time.monotonic()
        if self.last_request_monotonic is not None:
            remaining = self.minimum_interval_seconds - (now - self.last_request_monotonic)
            if remaining > 0:
                time.sleep(remaining)
        self.last_request_monotonic = time.monotonic()


def load_contract() -> dict[str, Any]:
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_fundamental_change_v1.json").read_text(
            encoding="utf-8"
        )
    )


def load_scanner_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows or "symbol" not in rows[0]:
        raise ValueError("scanner CSV must contain symbol rows")
    unique: dict[str, dict[str, str]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if symbol and symbol not in unique:
            unique[symbol] = row
    return list(unique.values())


def retry_fetch(
    url: str,
    *,
    user_agent: str,
    pacer: RequestPacer,
    retries: int = 2,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            pacer.wait()
            return fetch_sec_json(url, user_agent=user_agent, timeout=45.0)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.0 * (attempt + 1))
    assert last_error is not None
    raise last_error


def load_ticker_bootstrap(*, user_agent: str, pacer: RequestPacer) -> tuple[dict[str, Any], dict[str, Any]]:
    """Prefer the official SEC file; fail over only for transport.

    The mirror is never authoritative. A mirror-derived candidate becomes usable only
    after the corresponding current data.sec.gov submissions payload confirms both
    the same CIK and the exact ticker.
    """
    try:
        payload = retry_fetch(
            SEC_TICKERS_URL,
            user_agent=user_agent,
            pacer=pacer,
            retries=0,
        )
        return payload, {
            "mode": "SEC_OFFICIAL_LIVE",
            "source": SEC_TICKERS_URL,
            "requires_current_submissions_validation": True,
            "coverage_interpretation": "CURRENT_EXACT_MATCH",
        }
    except Exception as official_error:
        payload = retry_fetch(
            TICKER_BOOTSTRAP_MIRROR_URL,
            user_agent=user_agent,
            pacer=pacer,
            retries=2,
        )
        return payload, {
            "mode": "STATIC_MIRROR_BOOTSTRAP_ONLY",
            "source": TICKER_BOOTSTRAP_MIRROR_URL,
            "official_source_blocked_error_type": type(official_error).__name__,
            "requires_current_submissions_validation": True,
            "coverage_interpretation": "VALIDATED_LOWER_BOUND_CURRENT_EXACT_MATCH",
        }


def validate_candidate_identity(
    *, symbol: str, candidate_cik: str, submissions_payload: dict[str, Any]
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    try:
        payload_cik = normalize_cik(submissions_payload.get("cik", ""))
    except Exception:
        return False, ["SUBMISSIONS_MISSING_VALID_CIK"]
    if payload_cik != normalize_cik(candidate_cik):
        reasons.append("SUBMISSIONS_CIK_MISMATCH")
    current_tickers = {
        str(value).strip().upper()
        for value in (submissions_payload.get("tickers") or [])
        if str(value).strip()
    }
    if str(symbol).strip().upper() not in current_tickers:
        reasons.append("TICKER_NOT_CONFIRMED_BY_CURRENT_SEC_SUBMISSIONS")
    return not reasons, reasons


def feature_counts(first_release_facts: list[dict[str, Any]], *, metric_families: dict[str, Any]) -> dict[str, int]:
    revenue_yoy = build_yoy_changes(
        first_release_facts, metric="revenue", metric_families=metric_families
    )
    revenue_acceleration = build_growth_acceleration(revenue_yoy)
    margins = build_operating_margin(first_release_facts, metric_families=metric_families)
    margin_changes = build_derived_yoy_changes(
        margins,
        source_feature="operating_margin",
        output_feature="operating_margin_change",
        kind="duration",
    )
    fcf = build_free_cash_flow(first_release_facts, metric_families=metric_families)
    fcf_changes = build_derived_yoy_changes(
        fcf,
        source_feature="free_cash_flow",
        output_feature="free_cash_flow_yoy_change",
        kind="duration",
    )
    debt = resolve_debt_values(first_release_facts, metric_families=metric_families)
    debt_known = [row for row in debt if row.get("status") == "KNOWN"]
    debt_conflicting = [row for row in debt if row.get("status") == "CONFLICTING_SOURCES"]
    debt_to_equity = build_debt_to_equity(
        debt, first_release_facts, metric_families=metric_families
    )
    debt_to_equity_known = [row for row in debt_to_equity if row.get("status") == "KNOWN"]
    debt_to_equity_changes = build_derived_yoy_changes(
        debt_to_equity,
        source_feature="debt_to_equity",
        output_feature="debt_to_equity_change",
        kind="instant",
    )
    diluted_eps_yoy = build_yoy_changes(
        first_release_facts, metric="diluted_eps", metric_families=metric_families
    )
    return {
        "revenue_yoy_change": len(revenue_yoy),
        "revenue_growth_acceleration": len(revenue_acceleration),
        "operating_margin": len(margins),
        "operating_margin_change": len(margin_changes),
        "free_cash_flow": len(fcf),
        "free_cash_flow_yoy_change": len(fcf_changes),
        "resolved_debt_known": len(debt_known),
        "resolved_debt_conflicting": len(debt_conflicting),
        "debt_to_equity": len(debt_to_equity_known),
        "debt_to_equity_change": len(debt_to_equity_changes),
        "diluted_eps_yoy_change": len(diluted_eps_yoy),
    }


def write_checkpoint(
    output_path: Path,
    *,
    scanner_as_of: str | None,
    companies: list[dict[str, Any]],
    completed: bool,
    ticker_bootstrap: dict[str, Any],
) -> None:
    payload = {
        "schema_version": "external_evidence_8c_current_universe_coverage_v1",
        "scope": "CURRENT_SCANNER_SNAPSHOT_FEASIBILITY_ONLY_NOT_HISTORICAL_UNIVERSE",
        "scanner_as_of": scanner_as_of,
        "completed": completed,
        "ticker_bootstrap": ticker_bootstrap,
        "identity_policy": {
            "current_exact_sec_ticker_match_only": True,
            "fuzzy_name_matching": False,
            "strip_exchange_suffixes": False,
            "historical_identity_claim": False,
            "candidate_must_be_validated_by_current_sec_submissions": True,
        },
        "outcome_research": "NOT_RUN",
        "decision_integration": "NOT_RUN",
        "companies": companies,
        "aggregate": aggregate_company_coverage(companies),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def run(
    *, scanner_path: Path, output_path: Path, user_agent: str,
    minimum_interval_seconds: float, max_symbols: int | None,
    max_sec_matches: int | None,
) -> dict[str, Any]:
    metric_families = load_contract()["metric_families"]
    scanner_rows = load_scanner_rows(scanner_path)
    if max_symbols is not None:
        scanner_rows = scanner_rows[:max_symbols]

    pacer = RequestPacer(minimum_interval_seconds)
    ticker_payload, ticker_bootstrap = load_ticker_bootstrap(
        user_agent=user_agent, pacer=pacer
    )
    ticker_map = build_sec_ticker_map(ticker_payload)
    scanner_as_of = scanner_rows[0].get("as_of") if scanner_rows else None
    companies: list[dict[str, Any]] = []
    matched_processed = 0

    for index, scanner in enumerate(scanner_rows, start=1):
        symbol = str(scanner.get("symbol") or "").strip()
        identity = exact_sec_match(symbol, ticker_map)
        base = {
            "symbol": symbol,
            "scanner_name": scanner.get("name"),
            "scanner_currency": scanner.get("currency"),
            "scanner_sector": scanner.get("sector"),
            "scanner_as_of": scanner.get("as_of"),
            "identity_bootstrap_mode": ticker_bootstrap["mode"],
            **identity,
        }
        if identity["sec_match_status"] != "KNOWN":
            companies.append({
                **base,
                "processing_status": "NOT_RUN_NO_EXACT_BOOTSTRAP_MATCH",
                "research_ready": False,
            })
            write_checkpoint(
                output_path, scanner_as_of=scanner_as_of, companies=companies,
                completed=False, ticker_bootstrap=ticker_bootstrap
            )
            continue
        if max_sec_matches is not None and matched_processed >= max_sec_matches:
            companies.append({
                **base,
                "processing_status": "NOT_RUN_MATCH_LIMIT",
                "research_ready": False,
                "reason_codes": ["LIVE_COVERAGE_MATCH_LIMIT"],
            })
            continue

        matched_processed += 1
        cik = identity["cik"]
        try:
            primary = retry_fetch(
                submissions_url(cik), user_agent=user_agent, pacer=pacer
            )
            identity_valid, identity_reasons = validate_candidate_identity(
                symbol=symbol, candidate_cik=cik, submissions_payload=primary
            )
            if not identity_valid:
                companies.append({
                    **base,
                    "processing_status": "REJECTED_STALE_OR_UNVALIDATED_BOOTSTRAP_IDENTITY",
                    "sec_authority_validation": "FAILED",
                    "research_ready": False,
                    "reason_codes": identity_reasons,
                })
                write_checkpoint(
                    output_path, scanner_as_of=scanner_as_of, companies=companies,
                    completed=False, ticker_bootstrap=ticker_bootstrap
                )
                print(f"[{index}/{len(scanner_rows)}] {symbol}: identity rejected", flush=True)
                continue

            historical_payloads: dict[str, dict[str, Any]] = {}
            for spec in historical_submission_file_specs(primary):
                historical_payloads[spec["name"]] = retry_fetch(
                    spec["url"], user_agent=user_agent, pacer=pacer
                )
            history = assemble_full_submission_history(
                primary, historical_payloads=historical_payloads,
                forms=None, require_complete=True
            )
            acceptance_index = build_acceptance_index(history["rows"])
            facts_payload = retry_fetch(
                companyfacts_url(cik), user_agent=user_agent, pacer=pacer
            )
            fact_rows = companyfacts_rows(
                facts_payload, acceptance_index=acceptance_index
            )
            accession_coverage = companyfacts_accession_coverage(fact_rows)
            first_release_facts = first_release_rows(fact_rows)
            concept_coverage = measure_concept_coverage(
                first_release_facts, metric_families=metric_families
            )
            features = feature_counts(
                first_release_facts, metric_families=metric_families
            )
            mapped = int(concept_coverage.get("mapped_rows") or 0)
            research_ready = bool(
                history["coverage"].get("research_ready")
                and accession_coverage.get("research_ready")
                and mapped > 0
            )
            reasons: list[str] = []
            if not history["coverage"].get("research_ready"):
                reasons.extend(history["coverage"].get("reason_codes") or [])
            if not accession_coverage.get("research_ready"):
                reasons.extend(accession_coverage.get("reason_codes") or [])
            if mapped == 0:
                reasons.append("NO_MAPPED_FUNDAMENTAL_FACTS")
            companies.append({
                **base,
                "processing_status": "SUCCESS",
                "sec_authority_validation": "CURRENT_SUBMISSIONS_CONFIRMED",
                "history_coverage": history["coverage"],
                "companyfacts_accession_coverage": accession_coverage,
                "concept_coverage": concept_coverage,
                "feature_counts": features,
                "research_ready": research_ready,
                "reason_codes": sorted(set(reasons)),
            })
        except Exception as exc:
            companies.append({
                **base,
                "processing_status": "FETCH_OR_CONTRACT_ERROR",
                "research_ready": False,
                "reason_codes": ["LIVE_SEC_FETCH_OR_CONTRACT_ERROR"],
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:1000],
            })

        write_checkpoint(
            output_path, scanner_as_of=scanner_as_of, companies=companies,
            completed=False, ticker_bootstrap=ticker_bootstrap
        )
        print(f"[{index}/{len(scanner_rows)}] {symbol}: {companies[-1]['processing_status']}", flush=True)

    write_checkpoint(
        output_path, scanner_as_of=scanner_as_of, companies=companies,
        completed=True, ticker_bootstrap=ticker_bootstrap
    )
    return json.loads(output_path.read_text(encoding="utf-8"))


def self_test() -> dict[str, Any]:
    ticker_map = build_sec_ticker_map({
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 1234, "ticker": "TEST", "title": "Test Corp"},
    })
    valid, reasons = validate_candidate_identity(
        symbol="AAPL",
        candidate_cik="0000320193",
        submissions_payload={"cik": "320193", "tickers": ["AAPL"]},
    )
    stale, stale_reasons = validate_candidate_identity(
        symbol="XOM",
        candidate_cik="0000034088",
        submissions_payload={"cik": "34088", "tickers": []},
    )
    return {
        "aapl": exact_sec_match("aapl", ticker_map),
        "foreign_native_unmatched": exact_sec_match("1810.HK", ticker_map),
        "authority_validation_valid": valid and not reasons,
        "authority_validation_rejects_stale": (not stale) and bool(stale_reasons),
        "outcome_research": "NOT_RUN",
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Phase 8C-C current scanner universe SEC coverage runner. No market outcomes are read."
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument(
        "--scanner", default=str(ROOT / "artifacts" / "research" / "latest_scanner.csv")
    )
    parser.add_argument(
        "--output",
        default=str(ROOT / "artifacts" / "research" / "external_evidence_8c_current_universe_coverage.json"),
    )
    parser.add_argument(
        "--user-agent",
        default="trading-zentrale Phase8C research github.com/grisuweimar-crypto/trading-zentrale",
    )
    parser.add_argument(
        "--minimum-interval-seconds", type=float, default=0.20,
        help="Default 0.20s = at most ~5 SEC requests/sec."
    )
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--max-sec-matches", type=int)
    args = parser.parse_args()
    if args.self_test:
        print(json.dumps(self_test(), indent=2, sort_keys=True))
        return 0
    result = run(
        scanner_path=Path(args.scanner), output_path=Path(args.output),
        user_agent=args.user_agent,
        minimum_interval_seconds=args.minimum_interval_seconds,
        max_symbols=args.max_symbols, max_sec_matches=args.max_sec_matches,
    )
    print(json.dumps(result["aggregate"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
