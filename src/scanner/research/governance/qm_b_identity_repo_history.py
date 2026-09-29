"""Supplemental QM-B identity evidence from frozen universe-master git history.

This module upgrades only CURRENT_* identity candidates when an exact historical
symbol/ISIN relationship exists in repository history at or before a frozen cutoff.
It never uses commits after the cutoff and never promotes crypto base-token lineage.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
from datetime import date, timedelta
import io
from pathlib import Path
import subprocess
from typing import Any, Mapping

from scanner.research.governance.qm_b_identity_reconciliation import (
    IdentityReconciliationError,
    build_identity_reconciliation,
    is_valid_isin,
    load_identity_contract,
)
from scanner.research.governance.qm_b_observed_membership import build_candidates


SCHEMA_VERSION = "qm_b_identity_repo_history_audit_v1"


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _clean(value).upper()


def _git(repo_root: Path, args: list[str], *, error_code: str) -> bytes:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return proc.stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise IdentityReconciliationError(error_code) from exc


def _list_history(repo_root: Path, *, cutoff: str, path: str) -> list[tuple[str, str]]:
    raw = _git(
        repo_root,
        ["log", "--format=%H%x09%cs", cutoff, "--", path],
        error_code=f"git_history_unreadable:{cutoff}:{path}",
    )
    result: list[tuple[str, str]] = []
    for line in raw.decode("utf-8").splitlines():
        if not line.strip():
            continue
        try:
            commit, commit_date = line.split("\t", 1)
            date.fromisoformat(commit_date)
        except ValueError as exc:
            raise IdentityReconciliationError(f"git_history_line_invalid:{line}") from exc
        result.append((commit, commit_date))
    return result


def _show_csv(repo_root: Path, commit: str, path: str) -> list[dict[str, str]]:
    raw = _git(
        repo_root,
        ["show", f"{commit}:{path}"],
        error_code=f"git_snapshot_unreadable:{commit}:{path}",
    )
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise IdentityReconciliationError(f"git_snapshot_not_utf8:{commit}:{path}") from exc
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "symbol" not in reader.fieldnames or "isin" not in reader.fieldnames:
        raise IdentityReconciliationError(f"git_snapshot_schema_invalid:{commit}:{path}")
    return [{str(k): "" if v is None else str(v) for k, v in row.items()} for row in reader]


def collect_universe_history_events(
    repo_root: str | Path,
    *,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_identity_contract(contract_path)
    spec = contract["sources"]["historical_git_history"]
    root = Path(repo_root)
    path = str(spec["path"])
    cutoff = str(spec["history_until_commit"])
    commits = _list_history(root, cutoff=cutoff, path=path)

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_isin: dict[str, list[dict[str, Any]]] = defaultdict(list)
    total_rows = 0
    for commit, commit_date in commits:
        available_from = (date.fromisoformat(commit_date) + timedelta(days=1)).isoformat()
        for row_number, row in enumerate(_show_csv(root, commit, path), start=2):
            if _clean(row.get("active")).lower() not in {"1", "true", "yes", "y"}:
                continue
            symbol = _upper(row.get("symbol"))
            isin = _upper(row.get("isin")) if is_valid_isin(row.get("isin")) else ""
            if not symbol and not isin:
                continue
            event = {
                "commit": commit,
                "commit_date": commit_date,
                "pit_available_from": available_from,
                "source_path": path,
                "source_row_number": row_number,
                "symbol": symbol,
                "isin": isin,
                "name": _clean(row.get("name")),
                "asset_type": _clean(row.get("asset_type")),
            }
            total_rows += 1
            if symbol:
                by_symbol[symbol].append(event)
            if isin:
                by_isin[isin].append(event)

    return {
        "cutoff_commit": cutoff,
        "source_path": path,
        "commit_count": len(commits),
        "snapshot_active_row_count": total_rows,
        "by_symbol": dict(by_symbol),
        "by_isin": dict(by_isin),
    }


def _observation_dates(history_path: Path) -> dict[str, list[str]]:
    ledger = build_candidates(history_path, source_path_label=history_path.as_posix())
    result: dict[str, list[str]] = defaultdict(list)
    for row in ledger["candidates"]:
        if row.get("evidence_class") != "SCANNER_OBSERVED" or row.get("membership_claim") != "OBSERVED_IN_SCANNER":
            continue
        result[_upper(row.get("observed_symbol"))].append(_clean(row.get("as_of_date")))
    return {key: sorted(values) for key, values in result.items()}


def audit_repo_history_upgrades(
    history_path: str | Path,
    current_universe_path: str | Path,
    *,
    repo_root: str | Path,
    contract_path: str | Path | None = None,
    base_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    base = dict(base_payload) if base_payload is not None else build_identity_reconciliation(
        history_path,
        current_universe_path,
        repo_root=repo_root,
        contract_path=contract_path,
    )
    history = collect_universe_history_events(repo_root, contract_path=contract_path)
    dates_by_identifier = _observation_dates(Path(history_path))

    audited: list[dict[str, Any]] = []
    for row in base["reconciled_identifiers"]:
        candidate_class = str(row.get("candidate_class"))
        if candidate_class not in {"CURRENT_SYMBOL_ONLY_MATCH", "CURRENT_ISIN_ONLY_MATCH"}:
            continue
        identifier = _upper(row.get("observed_identifier"))
        canonical_isin = _upper(row.get("canonical_isin"))
        matches: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []

        if candidate_class == "CURRENT_SYMBOL_ONLY_MATCH":
            for event in history["by_symbol"].get(identifier, []):
                if event.get("isin") == canonical_isin:
                    matches.append(event)
                elif event.get("isin"):
                    conflicts.append(event)
        else:
            for event in history["by_isin"].get(canonical_isin, []):
                matches.append(event)

        status = "NO_HISTORICAL_REPO_MATCH"
        upgraded = False
        pit_available_from = None
        pit_verified_count = 0
        observation_dates = dates_by_identifier.get(identifier, [])
        if conflicts:
            status = "CONFLICTING_HISTORICAL_REPO_MAPPING"
        elif matches:
            status = "HISTORICAL_REPO_MATCH"
            upgraded = True
            pit_available_from = min(str(event["pit_available_from"]) for event in matches)
            boundary = date.fromisoformat(pit_available_from)
            pit_verified_count = sum(date.fromisoformat(value) >= boundary for value in observation_dates)

        audited.append(
            {
                "observed_identifier": identifier,
                "base_candidate_class": candidate_class,
                "canonical_isin": canonical_isin or None,
                "base_identity_status": row.get("identity_status"),
                "repo_history_status": status,
                "identity_status_after_repo_history": "VERIFIED" if upgraded else ("CONFLICTING" if conflicts else row.get("identity_status")),
                "upgraded": upgraded,
                "pit_alias_available_from": pit_available_from,
                "scanner_observation_count": len(observation_dates),
                "pit_verified_observation_count": pit_verified_count,
                "pit_unverified_observation_count": len(observation_dates) - pit_verified_count,
                "matching_commit_count": len({event["commit"] for event in matches}),
                "first_matching_commit": min((event["commit_date"] for event in matches), default=None),
                "conflicting_commit_count": len({event["commit"] for event in conflicts}),
                "strict_alias_promoted": False,
                "strict_membership_promoted": False,
            }
        )

    counts = Counter(row["repo_history_status"] for row in audited)
    upgraded_rows = [row for row in audited if row["upgraded"]]
    still_partial = [row["observed_identifier"] for row in audited if not row["upgraded"] and row["repo_history_status"] != "CONFLICTING_HISTORICAL_REPO_MAPPING"]
    conflicting = [row["observed_identifier"] for row in audited if row["repo_history_status"] == "CONFLICTING_HISTORICAL_REPO_MAPPING"]

    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "strict_alias_ledger": False,
        "strict_membership_ledger": False,
        "history_cutoff_commit": history["cutoff_commit"],
        "history_source_path": history["source_path"],
        "history_commit_count": history["commit_count"],
        "history_snapshot_active_row_count": history["snapshot_active_row_count"],
        "base_current_only_identifier_count": len(audited),
        "repo_history_status_counts": dict(sorted(counts.items())),
        "upgraded_identifier_count": len(upgraded_rows),
        "still_partial_identifier_count": len(still_partial),
        "still_partial_identifiers": sorted(still_partial),
        "conflicting_identifier_count": len(conflicting),
        "conflicting_identifiers": sorted(conflicting),
        "upgraded_scanner_observation_count": sum(row["scanner_observation_count"] for row in upgraded_rows),
        "upgraded_pit_verified_observation_count": sum(row["pit_verified_observation_count"] for row in upgraded_rows),
        "upgraded_pit_unverified_observation_count": sum(row["pit_unverified_observation_count"] for row in upgraded_rows),
        "audited_identifiers": audited,
    }
