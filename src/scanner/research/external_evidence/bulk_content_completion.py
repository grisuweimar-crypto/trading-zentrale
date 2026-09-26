from __future__ import annotations

import hashlib
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping

from .content_parser import (
    anchor_coverage,
    extract_evidence_anchors,
    split_sec_documents,
    validate_anchor_contract,
)
from .filing_documents import (
    SEC_ARCHIVES_BASE,
    fetch_sec_document_bytes,
    research_window_from_history,
    select_content_filings,
)
from .sec_history import assemble_full_submission_history

BULK_SCHEMA = "external_evidence_8c_sec_bulk_bundle_v1"
DIRECT_BULK_MODE = "DIRECT_SEC_BULK_OPERATOR_ATTESTED"
CONTENT_MANIFEST_SCHEMA = "external_evidence_8c_real_content_manifest_v1"
ANCHOR_SCHEMA = "external_evidence_8c_content_anchors_v1"


class BulkContentCompletionError(ValueError):
    """Raised when real filing-content acquisition violates the frozen 8C contract."""


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise BulkContentCompletionError(f"cannot read JSON object: {path}") from exc
    if not isinstance(payload, dict):
        raise BulkContentCompletionError(f"JSON root must be an object: {path}")
    return payload


def _resolve(root: Path, relative: str) -> Path:
    base = root.resolve()
    candidate = (root / str(relative or "")).resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise BulkContentCompletionError(f"bundle path escapes root: {relative!r}") from exc
    return candidate


def _read_verified_json(bundle_dir: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    relative = str(spec.get("path") or "").strip()
    expected = str(spec.get("sha256") or "").strip().lower()
    if not relative or len(expected) != 64:
        raise BulkContentCompletionError("bundle JSON spec requires path and SHA-256")
    path = _resolve(bundle_dir, relative)
    if not path.is_file():
        raise BulkContentCompletionError(f"bundle file missing: {relative}")
    raw = path.read_bytes()
    if _sha256_bytes(raw) != expected:
        raise BulkContentCompletionError(f"SHA-256 mismatch for {relative}")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise BulkContentCompletionError(f"cannot decode bundle JSON: {relative}") from exc
    if not isinstance(payload, dict):
        raise BulkContentCompletionError(f"bundle JSON must contain an object: {relative}")
    return payload


def _load_direct_bulk_manifest(bundle_dir: Path) -> tuple[dict[str, Any], str]:
    path = bundle_dir / "bulk_manifest.json"
    if not path.is_file():
        raise BulkContentCompletionError(f"bulk manifest missing: {path}")
    manifest = _load_json(path)
    if manifest.get("schema_version") != BULK_SCHEMA:
        raise BulkContentCompletionError("unsupported SEC bulk manifest schema")
    if manifest.get("source_mode") != DIRECT_BULK_MODE:
        raise BulkContentCompletionError("real content validation requires direct SEC bulk mode")
    if manifest.get("source_authority") != "U.S. SEC EDGAR":
        raise BulkContentCompletionError("source authority must be U.S. SEC EDGAR")
    if manifest.get("operator_attested_direct_sec_download") is not True:
        raise BulkContentCompletionError("direct SEC bulk operator attestation is required")
    if manifest.get("market_outcomes_read") is not False:
        raise BulkContentCompletionError("bulk bundle must remain outcome-blind")
    return manifest, _sha256_file(path)


def _verified_company_payloads(
    bundle_dir: Path,
    company: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if company.get("identity_status") != "VERIFIED_BY_SEC_BULK_SUBMISSIONS":
        raise BulkContentCompletionError("company is not verified by direct SEC bulk submissions")
    primary = _read_verified_json(bundle_dir, company.get("submissions_file") or {})
    symbol = str(company.get("symbol") or "").strip().upper()
    cik = str(company.get("cik") or "").strip()
    current_tickers = {str(value or "").strip().upper() for value in (primary.get("tickers") or [])}
    payload_cik = str(primary.get("cik") or "").strip().lstrip("0") or "0"
    expected_cik = cik.lstrip("0") or "0"
    if symbol not in current_tickers or payload_cik != expected_cik:
        raise BulkContentCompletionError(f"hashed submissions identity mismatch for {symbol}")

    history: dict[str, dict[str, Any]] = {}
    for spec in company.get("history_files") or []:
        payload = _read_verified_json(bundle_dir, spec)
        history[Path(str(spec.get("path") or "")).name] = payload
    return primary, history


def _load_resume_state(
    manifest_path: Path,
    *,
    base_manifest_sha256: str,
) -> dict[str, dict[str, Any]]:
    if not manifest_path.is_file():
        return {}
    payload = _load_json(manifest_path)
    if payload.get("schema_version") != CONTENT_MANIFEST_SCHEMA:
        return {}
    if payload.get("base_bulk_manifest_sha256") != base_manifest_sha256:
        return {}
    rows = payload.get("content_filings") or []
    if not isinstance(rows, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        key = f"{row.get('cik')}|{row.get('accession_number')}"
        result[key] = dict(row)
    return result


def _resume_bytes(output_dir: Path, row: Mapping[str, Any], source_url: str) -> bytes | None:
    if str(row.get("source_url") or "") != source_url:
        return None
    relative = str(row.get("path") or "").strip()
    expected = str(row.get("sha256") or "").strip().lower()
    if not relative or len(expected) != 64:
        return None
    path = _resolve(output_dir, relative)
    if not path.is_file():
        return None
    raw = path.read_bytes()
    return raw if _sha256_bytes(raw) == expected else None


def _write_manifest(
    path: Path,
    *,
    base_bundle: Path,
    base_manifest_sha256: str,
    content_window: Mapping[str, Any],
    selected_count: int,
    content_rows: list[dict[str, Any]],
    fetch_errors: list[dict[str, Any]],
    parser_errors: list[dict[str, Any]],
) -> None:
    payload = {
        "schema_version": CONTENT_MANIFEST_SCHEMA,
        "phase": "8C_F_to_8C_G_real_content_completion",
        "source_authority": "U.S. SEC EDGAR",
        "base_bulk_bundle": str(base_bundle),
        "base_bulk_manifest_sha256": base_manifest_sha256,
        "content_source": "OFFICIAL_SEC_ARCHIVES_FULL_SUBMISSION_TEXT",
        "content_window": dict(content_window),
        "selected_filing_count": selected_count,
        "content_document_count": len(content_rows),
        "fetch_error_count": len(fetch_errors),
        "parser_error_count": len(parser_errors),
        "content_filings": content_rows,
        "fetch_errors": fetch_errors,
        "parser_errors": parser_errors,
        "market_outcomes_read": False,
        "market_direction_assigned": False,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def acquire_and_extract_bulk_content(
    *,
    bundle_dir: Path,
    research_history_path: Path,
    parser_contract_path: Path,
    output_dir: Path,
    anchors_output_path: Path,
    user_agent: str,
    minimum_interval_seconds: float = 0.22,
    content_lookback_days: int = 365,
    fetcher: Callable[..., bytes] = fetch_sec_document_bytes,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Acquire official SEC filing text for a verified bulk bundle and run frozen 8C-G.

    The bulk bundle remains unchanged. Downloaded content is resumable and every
    document is stored with a SHA-256. Market outcomes are never read.
    """
    if not str(user_agent or "").strip():
        raise BulkContentCompletionError("descriptive SEC User-Agent is required")
    manifest, base_sha = _load_direct_bulk_manifest(bundle_dir)
    contract = _load_json(parser_contract_path)
    if contract.get("schema_version") != "external_evidence_8c_content_parser_v1":
        raise BulkContentCompletionError("unexpected 8C-G parser contract schema")
    if contract.get("market_outcomes_read") not in {None, False}:
        raise BulkContentCompletionError("8C-G parser contract may not enable market outcomes")

    content_window = research_window_from_history(
        research_history_path,
        lookback_days=content_lookback_days,
    )
    content_window["history_source"] = str(research_history_path)
    content_window["note"] = "Acquisition buffer only; not a feature lookback definition."

    selected_rows: list[dict[str, Any]] = []
    identity_errors: list[dict[str, Any]] = []
    for company in manifest.get("companies") or []:
        if not isinstance(company, Mapping):
            continue
        if company.get("identity_status") != "VERIFIED_BY_SEC_BULK_SUBMISSIONS":
            continue
        symbol = str(company.get("symbol") or "").strip().upper()
        cik = str(company.get("cik") or "").strip()
        try:
            primary, historical = _verified_company_payloads(bundle_dir, company)
            assembled = assemble_full_submission_history(
                primary,
                historical_payloads=historical,
                forms=None,
                require_complete=True,
            )
            rows = select_content_filings(
                assembled["rows"],
                acquisition_start=str(content_window["acquisition_start"]),
                acquisition_end=str(content_window["acquisition_end"]),
            )
            for row in rows:
                enriched = dict(row)
                enriched["symbol"] = symbol
                enriched["cik"] = cik
                selected_rows.append(enriched)
        except Exception as exc:
            identity_errors.append(
                {
                    "symbol": symbol,
                    "cik": cik,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:1000],
                }
            )

    if identity_errors:
        raise BulkContentCompletionError(
            f"{len(identity_errors)} direct-verified companies failed local history validation; first={identity_errors[0]}"
        )

    # Accession is globally unique, but retain CIK in the key as an additional guard.
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in selected_rows:
        key = (str(row.get("cik") or ""), str(row.get("accession_number") or ""))
        deduped[key] = row
    selected_rows = [deduped[key] for key in sorted(deduped)]

    output_dir.mkdir(parents=True, exist_ok=True)
    content_manifest_path = output_dir / "content_manifest.json"
    resume = _load_resume_state(content_manifest_path, base_manifest_sha256=base_sha)
    content_rows: list[dict[str, Any]] = []
    fetch_errors: list[dict[str, Any]] = []
    parser_errors: list[dict[str, Any]] = []
    anchors: list[dict[str, Any]] = []
    reused_count = 0
    fetched_count = 0
    last_request_at: float | None = None

    policy = contract["document_policy"]
    for index, filing in enumerate(selected_rows, start=1):
        symbol = str(filing.get("symbol") or "")
        cik = str(filing.get("cik") or "")
        accession = str(filing.get("accession_number") or "")
        source_url = str(filing.get("content_source_url") or "")
        if not source_url.startswith(f"{SEC_ARCHIVES_BASE}/"):
            raise BulkContentCompletionError(f"non-official filing source URL rejected: {source_url}")
        key = f"{cik}|{accession}"
        prior = resume.get(key)
        raw = _resume_bytes(output_dir, prior or {}, source_url) if prior else None

        if raw is not None:
            reused_count += 1
        else:
            error: Exception | None = None
            for attempt in range(4):
                try:
                    if last_request_at is not None:
                        remaining = float(minimum_interval_seconds) - (time.monotonic() - last_request_at)
                        if remaining > 0:
                            sleep_fn(remaining)
                    raw = fetcher(source_url, user_agent=user_agent, timeout=90.0)
                    last_request_at = time.monotonic()
                    error = None
                    break
                except Exception as exc:
                    error = exc
                    last_request_at = time.monotonic()
                    if attempt < 3:
                        sleep_fn(float(2**attempt))
            if raw is None:
                fetch_errors.append(
                    {
                        "symbol": symbol,
                        "cik": cik,
                        "accession_number": accession,
                        "form": filing.get("form"),
                        "source_url": source_url,
                        "error_type": type(error).__name__ if error else "UNKNOWN",
                        "error_message": str(error)[:1000] if error else "unknown fetch failure",
                    }
                )
                _write_manifest(
                    content_manifest_path,
                    base_bundle=bundle_dir,
                    base_manifest_sha256=base_sha,
                    content_window=content_window,
                    selected_count=len(selected_rows),
                    content_rows=content_rows,
                    fetch_errors=fetch_errors,
                    parser_errors=parser_errors,
                )
                continue
            relative = f"documents/{cik}/{accession.replace('-', '')}/{accession}.txt"
            destination = _resolve(output_dir, relative)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
            fetched_count += 1
            prior = {
                "symbol": symbol,
                "cik": cik,
                "accession_number": accession,
                "form": filing.get("form"),
                "source_valid_from": filing.get("valid_from"),
                "source_published_at": filing.get("published_at"),
                "publication_stage": filing.get("publication_stage"),
                "source_url": source_url,
                "path": relative,
                "sha256": _sha256_bytes(raw),
            }

        assert raw is not None
        row_meta = dict(prior or {})
        # Recompute SHA even for reused files; the manifest never trusts a stale digest.
        row_meta["sha256"] = _sha256_bytes(raw)
        content_rows.append(row_meta)
        try:
            documents = split_sec_documents(raw)
            filing_anchors = extract_evidence_anchors(
                documents,
                anchor_families=contract["evidence_anchor_families"],
                parser_version=contract["parser_version"],
                excluded_type_prefixes=policy["exclude_document_type_prefixes"],
                minimum_visible_characters=policy["minimum_visible_characters"],
            )
            for anchor in filing_anchors:
                anchor.update(
                    {
                        "symbol": symbol,
                        "cik": cik,
                        "accession_number": accession,
                        "form": filing.get("form"),
                        "source_valid_from": filing.get("valid_from"),
                        "source_published_at": filing.get("published_at"),
                        "publication_stage": filing.get("publication_stage"),
                        "source_document_file": row_meta.get("path"),
                    }
                )
            validate_anchor_contract(
                filing_anchors,
                required_fields=contract["anchor_output_fields"],
            )
            anchors.extend(filing_anchors)
        except Exception as exc:
            parser_errors.append(
                {
                    "symbol": symbol,
                    "cik": cik,
                    "accession_number": accession,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:1000],
                }
            )

        _write_manifest(
            content_manifest_path,
            base_bundle=bundle_dir,
            base_manifest_sha256=base_sha,
            content_window=content_window,
            selected_count=len(selected_rows),
            content_rows=content_rows,
            fetch_errors=fetch_errors,
            parser_errors=parser_errors,
        )
        print(
            f"[{index}/{len(selected_rows)}] {symbol} {accession}: "
            f"{'REUSED' if prior and key in resume else 'FETCHED'} anchors={len(filing_anchors) if 'filing_anchors' in locals() else 0}",
            flush=True,
        )

    coverage = anchor_coverage(anchors)
    coverage.update(
        {
            "selected_filing_count": len(selected_rows),
            "content_document_count": len(content_rows),
            "fetched_document_count": fetched_count,
            "reused_document_count": reused_count,
            "fetch_error_count": len(fetch_errors),
            "parser_error_count": len(parser_errors),
            "direct_verified_company_count": sum(
                1
                for company in (manifest.get("companies") or [])
                if isinstance(company, Mapping)
                and company.get("identity_status") == "VERIFIED_BY_SEC_BULK_SUBMISSIONS"
            ),
        }
    )
    anchor_payload = {
        "schema_version": ANCHOR_SCHEMA,
        "phase": "8C_G_content_parser_foundation",
        "source_bulk_schema": BULK_SCHEMA,
        "source_bulk_manifest_sha256": base_sha,
        "scanner_as_of": (manifest.get("companies") or [{}])[0].get("scanner_as_of") if manifest.get("companies") else None,
        "source_authority": "U.S. SEC EDGAR",
        "transport_mode": "DIRECT_SEC_BULK_PLUS_OFFICIAL_SEC_ARCHIVES",
        "parser_version": contract["parser_version"],
        "semantic_classification": "NOT_RUN",
        "market_outcomes_read": False,
        "market_direction_assigned": False,
        "anchors": anchors,
        "filing_errors": fetch_errors + parser_errors,
        "coverage": coverage,
    }
    anchors_output_path.parent.mkdir(parents=True, exist_ok=True)
    anchors_output_path.write_text(json.dumps(anchor_payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_manifest(
        content_manifest_path,
        base_bundle=bundle_dir,
        base_manifest_sha256=base_sha,
        content_window=content_window,
        selected_count=len(selected_rows),
        content_rows=content_rows,
        fetch_errors=fetch_errors,
        parser_errors=parser_errors,
    )
    return {
        "schema_version": CONTENT_MANIFEST_SCHEMA,
        "coverage": coverage,
        "anchors_output": str(anchors_output_path),
        "content_manifest": str(content_manifest_path),
        "market_outcomes_read": False,
    }
