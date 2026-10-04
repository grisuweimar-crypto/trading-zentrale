"""Prospective Scanner input provenance for BA-QM8 Data -> Scanner lineage.

The module records only provenance. It never changes scanner values, scores,
weights, decisions or execution. The prospective contract has three steps:

1. capture the persistent/local input state before the scanner mutates anything;
2. capture the exact table consumed by scoring after master/Yahoo enrichment;
3. bind both to the newly created scanner snapshot.

Historical snapshots are never backfilled by inference.
"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Mapping

from scanner._version import __build__, __version__


PRE_RUN_SCHEMA = "scanner_pre_run_provenance_v1"
RUNTIME_SCHEMA = "scanner_runtime_provenance_v1"
FINAL_SCHEMA = "scanner_input_provenance_v1"

RUNTIME_RELATIVE_PATH = Path("artifacts/reports/scanner_runtime_provenance.json")
FINAL_RELATIVE_PATH = Path("artifacts/research/scanner_input_provenance.json")

WATCHLIST_DB = Path("artifacts/watchlist/watchlist.csv")
WATCHLIST_TEMPLATE = Path("data/inputs/watchlist.csv")
MASTER_UNIVERSE = Path("data/inputs/universe_master.csv")
YAHOO_TAXONOMY = Path("artifacts/mapping/yahoo_taxonomy.csv")
PILLARS_MAPPING = Path("artifacts/mapping/pillars.csv")


class ScannerProvenanceError(ValueError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash_bytes(raw: bytes) -> str:
    return sha256(raw).hexdigest()


def _hash_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    return _hash_bytes(path.read_bytes())


def _atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temp = Path(handle.name)
        handle.write(raw)
    try:
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def current_run_id() -> str | None:
    run = str(os.environ.get("GITHUB_RUN_ID") or "").strip()
    if not run:
        return None
    attempt = str(os.environ.get("GITHUB_RUN_ATTEMPT") or "1").strip() or "1"
    return f"github-{run}-{attempt}"


def _code_revision(root: Path) -> str | None:
    env = str(os.environ.get("GITHUB_SHA") or "").strip()
    if env:
        return env
    try:
        value = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return value or None


def _record(root: Path, relative: Path, *, role: str, required: bool) -> dict[str, Any]:
    path = root / relative
    exists = path.exists() and path.is_file()
    return {
        "role": role,
        "path": relative.as_posix(),
        "required": required,
        "exists": exists,
        "sha256": _hash_file(path) if exists else None,
        "size_bytes": path.stat().st_size if exists else None,
    }


def _selected_watchlist_input(root: Path) -> Path:
    if (root / WATCHLIST_DB).exists():
        return WATCHLIST_DB
    return WATCHLIST_TEMPLATE


def capture_pre_run_provenance(root: str | Path) -> dict[str, Any]:
    """Hash material local inputs before build_watchlist can mutate them."""
    root = Path(root).resolve()
    selected = _selected_watchlist_input(root)
    inputs = [
        _record(root, selected, role="persistent_watchlist_state", required=True),
        _record(root, MASTER_UNIVERSE, role="active_universe_master", required=False),
        _record(root, YAHOO_TAXONOMY, role="taxonomy_mapping", required=False),
        _record(root, PILLARS_MAPPING, role="pillar_mapping", required=False),
    ]
    if not inputs[0]["exists"] or not inputs[0]["sha256"]:
        raise ScannerProvenanceError("pre_run_selected_watchlist_missing")

    return {
        "schema_version": PRE_RUN_SCHEMA,
        "captured_at": _now(),
        "run_id": current_run_id(),
        "selected_watchlist_input": selected.as_posix(),
        "inputs": inputs,
        "runtime_intent": {
            "scanner_fetch_yahoo": str(
                os.environ.get("SCANNER_FETCH_YAHOO") or ""
            ).strip(),
        },
        "code_revision": _code_revision(root),
        "scanner_version": __version__,
        "scanner_build": __build__,
        "historical_backfill": False,
    }


def write_runtime_provenance(
    root: str | Path,
    *,
    selected_source: str | Path,
    scoring_rows_path: str | Path,
    yahoo_report: object | None,
) -> dict[str, Any]:
    """Record exact post-enrichment scanner inputs before scoring begins."""
    root = Path(root).resolve()
    selected_path = Path(selected_source)
    if not selected_path.is_absolute():
        selected_path = root / selected_path
    scoring_path = Path(scoring_rows_path)
    if not scoring_path.is_absolute():
        scoring_path = root / scoring_path

    try:
        selected_relative = selected_path.resolve().relative_to(root).as_posix()
        scoring_relative = scoring_path.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise ScannerProvenanceError("runtime_provenance_path_outside_repository") from exc

    selected_hash = _hash_file(selected_path)
    scoring_hash = _hash_file(scoring_path)
    if not selected_hash:
        raise ScannerProvenanceError("runtime_scoring_universe_missing")
    if not scoring_hash:
        raise ScannerProvenanceError("runtime_scoring_rows_missing")

    if yahoo_report is None:
        yahoo = None
    elif is_dataclass(yahoo_report):
        yahoo = asdict(yahoo_report)
    elif isinstance(yahoo_report, Mapping):
        yahoo = dict(yahoo_report)
    else:
        raise ScannerProvenanceError("runtime_yahoo_report_invalid")

    payload = {
        "schema_version": RUNTIME_SCHEMA,
        "captured_at": _now(),
        "run_id": current_run_id(),
        "selected_scoring_universe": {
            "path": selected_relative,
            "sha256": selected_hash,
            "size_bytes": selected_path.stat().st_size,
        },
        "scoring_rows_input": {
            "path": scoring_relative,
            "sha256": scoring_hash,
            "size_bytes": scoring_path.stat().st_size,
        },
        "yahoo_enrichment": yahoo,
        "code_revision": _code_revision(root),
        "scanner_version": __version__,
        "scanner_build": __build__,
        "historical_backfill": False,
    }
    target = root / RUNTIME_RELATIVE_PATH
    raw = (json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    _atomic_write(target, raw)
    return payload


def finalize_scanner_input_provenance(
    root: str | Path,
    *,
    receipt: Mapping[str, Any],
    research_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind pre-run + runtime provenance to the newly published scanner snapshot."""
    root = Path(root).resolve()
    pre = receipt.get("scanner_pre_run_provenance")
    if not isinstance(pre, Mapping) or pre.get("schema_version") != PRE_RUN_SCHEMA:
        raise ScannerProvenanceError("scanner_pre_run_provenance_required")

    runtime_path = root / RUNTIME_RELATIVE_PATH
    try:
        runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScannerProvenanceError("scanner_runtime_provenance_unreadable") from exc
    if not isinstance(runtime, dict) or runtime.get("schema_version") != RUNTIME_SCHEMA:
        raise ScannerProvenanceError("scanner_runtime_provenance_invalid")

    receipt_run = str(receipt.get("run_id") or "").strip()
    pre_run = str(pre.get("run_id") or "").strip()
    runtime_run = str(runtime.get("run_id") or "").strip()
    if receipt_run and pre_run and receipt_run != pre_run:
        raise ScannerProvenanceError("pre_run_receipt_identity_mismatch")
    if receipt_run and runtime_run and receipt_run != runtime_run:
        raise ScannerProvenanceError("runtime_receipt_identity_mismatch")

    snapshot_id = str(research_metadata.get("snapshot_id") or "").strip()
    as_of = str(research_metadata.get("as_of") or "").strip()
    source = research_metadata.get("source")
    latest = research_metadata.get("latest_scanner")
    if not snapshot_id or not as_of:
        raise ScannerProvenanceError("scanner_snapshot_identity_required")
    if not isinstance(source, Mapping) or not str(source.get("sha256") or ""):
        raise ScannerProvenanceError("scanner_output_source_hash_required")
    if not isinstance(latest, Mapping) or not str(latest.get("sha256") or ""):
        raise ScannerProvenanceError("latest_scanner_hash_required")

    pre_inputs = pre.get("inputs")
    if not isinstance(pre_inputs, list) or not pre_inputs:
        raise ScannerProvenanceError("pre_run_inputs_required")
    selected = pre_inputs[0]
    if not isinstance(selected, Mapping) or not selected.get("sha256"):
        raise ScannerProvenanceError("pre_run_selected_input_hash_required")

    direct = runtime.get("scoring_rows_input")
    universe = runtime.get("selected_scoring_universe")
    yahoo = runtime.get("yahoo_enrichment")
    if not isinstance(direct, Mapping) or not direct.get("sha256"):
        raise ScannerProvenanceError("direct_scoring_input_hash_required")
    if not isinstance(universe, Mapping) or not universe.get("sha256"):
        raise ScannerProvenanceError("scoring_universe_hash_required")
    yahoo_digest_bound = True
    if isinstance(yahoo, Mapping) and yahoo.get("enabled") is True:
        digest = str(yahoo.get("provider_frame_sha256") or "").strip()
        if len(digest) != 64:
            raise ScannerProvenanceError("yahoo_provider_frame_hash_required")
        if int(yahoo.get("provider_frame_rows") or 0) <= 0:
            raise ScannerProvenanceError("yahoo_provider_frame_rows_required")
    elif yahoo is not None and not isinstance(yahoo, Mapping):
        raise ScannerProvenanceError("runtime_yahoo_report_invalid")

    pre_code = str(pre.get("code_revision") or "")
    runtime_code = str(runtime.get("code_revision") or "")
    if pre_code and runtime_code and pre_code != runtime_code:
        raise ScannerProvenanceError("scanner_code_revision_changed_during_run")
    code_revision = runtime_code or pre_code or None

    payload = {
        "schema_version": FINAL_SCHEMA,
        "snapshot_id": snapshot_id,
        "as_of": as_of,
        "run_id": receipt_run or runtime_run or pre_run or None,
        "bound_at": _now(),
        "pre_run": dict(pre),
        "runtime": runtime,
        "scanner_output": {
            "source_file": source.get("file"),
            "source_sha256": source.get("sha256"),
            "latest_scanner_path": latest.get("path"),
            "latest_scanner_sha256": latest.get("sha256"),
        },
        "transform_identity": {
            "code_revision": code_revision,
            "scanner_version": runtime.get("scanner_version") or pre.get("scanner_version"),
            "scanner_build": runtime.get("scanner_build") or pre.get("scanner_build"),
        },
        "coverage": {
            "pre_run_state_bound": True,
            "direct_scoring_input_bound": True,
            "scoring_universe_bound": True,
            "scanner_output_bound": True,
            "snapshot_identity_bound": True,
            "provider_frame_digest_bound": yahoo_digest_bound,
            "provider_payload_retained": False,
            "historical_backfill": False,
        },
        "boundaries": {
            "scanner_values_changed": False,
            "scanner_weights_changed": False,
            "decision_semantics_changed": False,
            "historical_provenance_inferred": False,
        },
    }

    body = dict(payload)
    payload["content_hash"] = _hash_bytes(_json(body).encode("utf-8"))
    target = root / FINAL_RELATIVE_PATH
    raw = (json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    _atomic_write(target, raw)
    return {
        "path": FINAL_RELATIVE_PATH.as_posix(),
        "sha256": _hash_bytes(raw),
        "content_hash": payload["content_hash"],
        "snapshot_id": snapshot_id,
        "as_of": as_of,
        "run_id": payload["run_id"],
        "complete": True,
        "historical_backfill": False,
        "direct_scoring_input_sha256": direct["sha256"],
        "scoring_universe_sha256": universe["sha256"],
        "yahoo_provider_frame_sha256": (
            yahoo.get("provider_frame_sha256")
            if isinstance(yahoo, Mapping)
            else None
        ),
        "source_watchlist_full_sha256": source["sha256"],
        "latest_scanner_sha256": latest["sha256"],
        "code_revision": code_revision,
    }


def validate_bound_provenance(
    root: str | Path,
    reference: Mapping[str, Any],
    *,
    expected_snapshot_id: str,
) -> dict[str, Any]:
    """Validate a final provenance manifest against its published reference."""
    root = Path(root).resolve()
    path_text = str(reference.get("path") or "").strip()
    expected_sha = str(reference.get("sha256") or "").strip()
    if not path_text or not expected_sha:
        raise ScannerProvenanceError("scanner_provenance_reference_incomplete")
    path = root / path_text
    actual_sha = _hash_file(path)
    if actual_sha != expected_sha:
        raise ScannerProvenanceError("scanner_provenance_file_hash_mismatch")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema_version") != FINAL_SCHEMA:
        raise ScannerProvenanceError("scanner_provenance_schema_invalid")
    if str(value.get("snapshot_id") or "") != str(expected_snapshot_id):
        raise ScannerProvenanceError("scanner_provenance_snapshot_mismatch")
    if reference.get("complete") is not True:
        raise ScannerProvenanceError("scanner_provenance_reference_not_complete")
    if value.get("content_hash") != str(reference.get("content_hash") or ""):
        raise ScannerProvenanceError("scanner_provenance_content_hash_mismatch")
    coverage = value.get("coverage")
    boundaries = value.get("boundaries")
    if not isinstance(coverage, Mapping) or not isinstance(boundaries, Mapping):
        raise ScannerProvenanceError("scanner_provenance_contract_sections_missing")
    for key in (
        "pre_run_state_bound",
        "direct_scoring_input_bound",
        "scoring_universe_bound",
        "scanner_output_bound",
        "snapshot_identity_bound",
        "provider_frame_digest_bound",
    ):
        if coverage.get(key) is not True:
            raise ScannerProvenanceError(f"scanner_provenance_coverage_missing:{key}")
    if coverage.get("historical_backfill") is not False:
        raise ScannerProvenanceError("scanner_provenance_historical_backfill_forbidden")
    for key in (
        "scanner_values_changed",
        "scanner_weights_changed",
        "decision_semantics_changed",
        "historical_provenance_inferred",
    ):
        if boundaries.get(key) is not False:
            raise ScannerProvenanceError(f"scanner_provenance_boundary_violation:{key}")
    return value
