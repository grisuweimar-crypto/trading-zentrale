from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


SNAPSHOT_SCHEMA = "external_evidence_8c_sec_snapshot_v1"


class SecSnapshotError(ValueError):
    """Raised when an offline SEC snapshot bundle is incomplete or corrupted."""


def canonical_json_bytes(payload: Any) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_json_with_digest(path: Path, payload: Any) -> str:
    data = canonical_json_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return sha256_bytes(data)


def read_json_verified(bundle_dir: Path, file_spec: Mapping[str, Any]) -> dict[str, Any]:
    relative = str(file_spec.get("path") or "").strip()
    expected = str(file_spec.get("sha256") or "").strip().lower()
    if not relative or not expected:
        raise SecSnapshotError("Snapshot file spec requires path and sha256")

    root = bundle_dir.resolve()
    path = (bundle_dir / relative).resolve()
    if root != path and root not in path.parents:
        raise SecSnapshotError(f"Snapshot path escapes bundle: {relative}")
    if not path.is_file():
        raise SecSnapshotError(f"Snapshot file missing: {relative}")

    data = path.read_bytes()
    actual = sha256_bytes(data)
    if actual != expected:
        raise SecSnapshotError(
            f"Snapshot digest mismatch for {relative}: expected {expected}, got {actual}"
        )
    payload = json.loads(data.decode("utf-8"))
    if not isinstance(payload, dict):
        raise SecSnapshotError(f"Snapshot JSON must be an object: {relative}")
    return payload


def load_snapshot_manifest(bundle_dir: Path) -> dict[str, Any]:
    path = bundle_dir / "manifest.json"
    if not path.is_file():
        raise SecSnapshotError("Snapshot manifest.json is missing")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != SNAPSHOT_SCHEMA:
        raise SecSnapshotError("Unsupported SEC snapshot schema")
    if payload.get("source_authority") != "U.S. SEC EDGAR":
        raise SecSnapshotError("SEC snapshot source_authority must be U.S. SEC EDGAR")
    if payload.get("market_outcomes_read") is not False:
        raise SecSnapshotError("SEC snapshot must remain outcome-blind")
    companies = payload.get("companies")
    if not isinstance(companies, list):
        raise SecSnapshotError("SEC snapshot companies must be an array")
    return payload


def validate_snapshot_bundle(bundle_dir: Path) -> dict[str, Any]:
    manifest = load_snapshot_manifest(bundle_dir)
    verified_files = 0
    verified_companies = 0

    ticker_spec = manifest.get("company_tickers_file")
    if not isinstance(ticker_spec, Mapping):
        raise SecSnapshotError("company_tickers_file is required")
    read_json_verified(bundle_dir, ticker_spec)
    verified_files += 1

    for company in manifest["companies"]:
        if not isinstance(company, Mapping):
            raise SecSnapshotError("Each company snapshot entry must be an object")
        status = str(company.get("identity_status") or "UNKNOWN")
        if status != "VERIFIED_BY_SEC_SUBMISSIONS":
            continue
        submissions = company.get("submissions_file")
        companyfacts = company.get("companyfacts_file")
        history = company.get("history_files") or []
        if not isinstance(submissions, Mapping) or not isinstance(companyfacts, Mapping):
            raise SecSnapshotError("Verified company requires submissions and companyfacts files")
        if not isinstance(history, list):
            raise SecSnapshotError("history_files must be an array")
        read_json_verified(bundle_dir, submissions)
        read_json_verified(bundle_dir, companyfacts)
        verified_files += 2
        for spec in history:
            if not isinstance(spec, Mapping):
                raise SecSnapshotError("history file spec must be an object")
            read_json_verified(bundle_dir, spec)
            verified_files += 1
        verified_companies += 1

    return {
        "schema_version": SNAPSHOT_SCHEMA,
        "company_count": len(manifest["companies"]),
        "verified_company_count": verified_companies,
        "verified_file_count": verified_files,
        "snapshot_created_at": manifest.get("created_at"),
        "scanner_as_of": manifest.get("scanner_as_of"),
        "market_outcomes_read": False,
    }
