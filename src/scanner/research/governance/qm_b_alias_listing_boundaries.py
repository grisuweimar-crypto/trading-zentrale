"""QM-B audit of PIT alias boundaries and listing-evidence availability.

Research-only. This package explains which scanner observations remain outside a
verified identity/alias PIT boundary after identity reconciliation. It also audits
whether the frozen repository actually contains explicit listing venue/date,
delisting or investability fields. It never infers those facts from ticker suffixes,
scanner presence, active flags or absence.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
from datetime import date
import io
import json
from pathlib import Path
import subprocess
from typing import Any, Mapping

from scanner.research.governance.qm_b_crypto_identifier import parse_crypto_identifier
from scanner.research.governance.qm_b_identity_reconciliation import (
    IdentityReconciliationError,
    build_identity_reconciliation,
    load_identity_contract,
)
from scanner.research.governance.qm_b_identity_repo_history import audit_repo_history_upgrades
from scanner.research.governance.qm_b_observed_membership import build_candidates


SCHEMA_VERSION = "qm_b_alias_listing_boundaries_audit_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_alias_listing_boundaries_v1.json"


class AliasListingBoundaryError(ValueError):
    """Raised when alias/listing boundary evidence cannot be audited safely."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _clean(value).upper()


def _parse_date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(_clean(value))
    except ValueError as exc:
        raise AliasListingBoundaryError(f"invalid_date:{field}:{value}") from exc


def load_boundary_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AliasListingBoundaryError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_alias_listing_boundaries_v1":
        raise AliasListingBoundaryError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise AliasListingBoundaryError("contract_scope_invalid")
    return payload


def _git_text(repo_root: Path, args: list[str], *, error_code: str) -> str:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise AliasListingBoundaryError(error_code) from exc
    try:
        return proc.stdout.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise AliasListingBoundaryError(f"{error_code}:not_utf8") from exc


def audit_universe_schema_history(repo_root: str | Path) -> dict[str, Any]:
    """Inspect frozen universe-master headers only; do not infer business semantics."""
    identity_contract = load_identity_contract()
    spec = identity_contract["sources"]["historical_git_history"]
    path = str(spec["path"])
    cutoff = str(spec["history_until_commit"])
    root = Path(repo_root)
    raw_log = _git_text(
        root,
        ["log", "--format=%H", cutoff, "--", path],
        error_code=f"git_history_unreadable:{cutoff}:{path}",
    )
    commits = [line.strip() for line in raw_log.splitlines() if line.strip()]
    headers_by_commit: list[dict[str, Any]] = []
    all_fields: set[str] = set()
    for commit in commits:
        text = _git_text(root, ["show", f"{commit}:{path}"], error_code=f"git_snapshot_unreadable:{commit}:{path}")
        reader = csv.reader(io.StringIO(text))
        try:
            header = [str(value).strip() for value in next(reader)]
        except StopIteration as exc:
            raise AliasListingBoundaryError(f"git_snapshot_empty:{commit}:{path}") from exc
        if not header:
            raise AliasListingBoundaryError(f"git_snapshot_header_missing:{commit}:{path}")
        all_fields.update(header)
        headers_by_commit.append({"commit": commit, "fields": header})

    field_groups = {
        "listing_venue": ["listing_venue", "exchange", "venue", "mic", "market"],
        "listing_date": ["listing_date", "ipo_date", "listed_from"],
        "delisting_date": ["delisting_date", "delisted_at", "listed_to"],
        "investability": ["investability_status", "investable", "tradable", "restricted"],
    }
    explicit_fields = {
        group: sorted(set(candidates) & all_fields)
        for group, candidates in field_groups.items()
    }
    return {
        "history_cutoff_commit": cutoff,
        "source_path": path,
        "commit_count": len(commits),
        "all_fields": sorted(all_fields),
        "explicit_fields": explicit_fields,
        "explicit_listing_venue_field_present": bool(explicit_fields["listing_venue"]),
        "explicit_listing_date_field_present": bool(explicit_fields["listing_date"]),
        "explicit_delisting_date_field_present": bool(explicit_fields["delisting_date"]),
        "explicit_investability_field_present": bool(explicit_fields["investability"]),
        "headers_by_commit": headers_by_commit,
    }


def _namespace_hint(identifier: str) -> str | None:
    """Return only the literal suffix token, never a venue interpretation."""
    if parse_crypto_identifier(identifier) is not None or "." not in identifier:
        return None
    suffix = identifier.rsplit(".", 1)[1].strip().upper()
    if not suffix or len(suffix) > 6 or not suffix.replace("-", "").isalnum():
        return None
    return suffix


def _observation_rows(history_path: Path) -> list[dict[str, Any]]:
    ledger = build_candidates(history_path, source_path_label=history_path.as_posix())
    return [
        dict(row)
        for row in ledger["candidates"]
        if row.get("evidence_class") == "SCANNER_OBSERVED"
        and row.get("membership_claim") == "OBSERVED_IN_SCANNER"
    ]


def build_alias_listing_boundary_audit(
    history_path: str | Path,
    current_universe_path: str | Path,
    *,
    repo_root: str | Path,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    contract = load_boundary_contract(contract_path)
    history = Path(history_path)
    current = Path(current_universe_path)

    base = build_identity_reconciliation(history, current, repo_root=repo_root)
    supplemental = audit_repo_history_upgrades(
        history,
        current,
        repo_root=repo_root,
        base_payload=base,
    )
    schema_audit = audit_universe_schema_history(repo_root)

    supplemental_by_identifier = {
        _upper(row.get("observed_identifier")): row
        for row in supplemental["audited_identifiers"]
    }
    identity_by_identifier: dict[str, dict[str, Any]] = {}
    for row in base["reconciled_identifiers"]:
        identifier = _upper(row.get("observed_identifier"))
        resolved = dict(row)
        upgrade = supplemental_by_identifier.get(identifier)
        if upgrade and upgrade.get("upgraded") is True:
            resolved["identity_status"] = "VERIFIED"
            resolved["pit_alias_available_from"] = upgrade.get("pit_alias_available_from")
            resolved["repo_history_upgrade"] = True
        else:
            resolved["repo_history_upgrade"] = False
        identity_by_identifier[identifier] = resolved

    observations = _observation_rows(history)
    observation_audit: list[dict[str, Any]] = []
    identifier_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in observations:
        identifier = _upper(row.get("observed_symbol"))
        when = _parse_date(row.get("as_of_date"), field=f"observation:{identifier}")
        identity = identity_by_identifier.get(identifier)
        if identity is None:
            observation_class = "IDENTITY_OR_BOUNDARY_UNRESOLVED"
            boundary = None
            identity_status = "UNKNOWN"
        elif parse_crypto_identifier(identifier) is not None:
            observation_class = "CRYPTO_STABLE_OBJECT_UNRESOLVED"
            boundary = None
            identity_status = str(identity.get("identity_status") or "PARTIAL")
        else:
            boundary_raw = identity.get("pit_alias_available_from")
            identity_status = str(identity.get("identity_status") or "UNKNOWN")
            if identity_status == "VERIFIED" and boundary_raw:
                boundary_date = _parse_date(boundary_raw, field=f"boundary:{identifier}")
                boundary = boundary_raw
                observation_class = (
                    "PIT_IDENTITY_ALIAS_SUPPORTED"
                    if when >= boundary_date
                    else "PRE_ALIAS_PIT_BOUNDARY_NON_CRYPTO"
                )
            else:
                boundary = boundary_raw or None
                observation_class = "IDENTITY_OR_BOUNDARY_UNRESOLVED"

        audited = {
            "as_of_date": row.get("as_of_date"),
            "observed_identifier": identifier,
            "identity_status": identity_status,
            "pit_alias_available_from": boundary,
            "observation_class": observation_class,
            "source_row_number": row.get("source_row_number"),
        }
        observation_audit.append(audited)
        identifier_rows[identifier].append(audited)

    boundary_rows: list[dict[str, Any]] = []
    required = contract["required_boundary_fields"]
    for identifier, rows in sorted(identifier_rows.items()):
        identity = identity_by_identifier.get(identifier, {})
        dates = sorted(str(row["as_of_date"]) for row in rows)
        class_counts = Counter(str(row["observation_class"]) for row in rows)
        suffix_hint = _namespace_hint(identifier)
        boundary = {
            "observed_identifier": identifier,
            "candidate_instrument_id": identity.get("candidate_instrument_id"),
            "canonical_isin": identity.get("canonical_isin"),
            "identity_status": identity.get("identity_status", "UNKNOWN"),
            "first_scanner_observed_date": dates[0],
            "last_scanner_observed_date": dates[-1],
            "pit_alias_available_from": identity.get("pit_alias_available_from"),
            "scanner_observation_count": len(rows),
            "pit_supported_observation_count": class_counts.get("PIT_IDENTITY_ALIAS_SUPPORTED", 0),
            "pit_unverified_observation_count": len(rows) - class_counts.get("PIT_IDENTITY_ALIAS_SUPPORTED", 0),
            "observation_class_counts": dict(sorted(class_counts.items())),
            "symbol_namespace_hint": suffix_hint,
            "symbol_namespace_hint_status": "HINT_ONLY" if suffix_hint else "UNKNOWN",
            "listing_venue": None,
            "listing_venue_status": "UNKNOWN",
            "listing_date": None,
            "listing_date_status": "UNKNOWN",
            "delisting_date": None,
            "delisting_status": "UNKNOWN",
            "investability_status": "UNKNOWN",
            "strict_alias_promoted": False,
            "strict_membership_promoted": False,
        }
        missing = [field for field in required if field not in boundary]
        if missing:
            raise AliasListingBoundaryError("boundary_required_fields_missing:" + ",".join(sorted(missing)))
        boundary_rows.append(boundary)

    class_counts = Counter(str(row["observation_class"]) for row in observation_audit)
    unresolved = [
        row for row in observation_audit
        if row["observation_class"] != "PIT_IDENTITY_ALIAS_SUPPORTED"
    ]
    non_crypto_pre_boundary = [
        row for row in unresolved
        if row["observation_class"] == "PRE_ALIAS_PIT_BOUNDARY_NON_CRYPTO"
    ]
    crypto_unresolved = [
        row for row in unresolved
        if row["observation_class"] == "CRYPTO_STABLE_OBJECT_UNRESOLVED"
    ]

    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "formal_audit_input_commit": contract["formal_audit_input_commit"],
        "strict_instrument_master": False,
        "strict_alias_ledger": False,
        "strict_membership_ledger": False,
        "scanner_observation_count": len(observation_audit),
        "unique_observed_identifier_count": len(boundary_rows),
        "observation_class_counts": dict(sorted(class_counts.items())),
        "pit_supported_observation_count": class_counts.get("PIT_IDENTITY_ALIAS_SUPPORTED", 0),
        "pit_unverified_observation_count": len(unresolved),
        "pre_alias_boundary_non_crypto_observation_count": len(non_crypto_pre_boundary),
        "pre_alias_boundary_non_crypto_identifier_count": len({row["observed_identifier"] for row in non_crypto_pre_boundary}),
        "crypto_stable_object_unresolved_observation_count": len(crypto_unresolved),
        "crypto_stable_object_unresolved_identifier_count": len({row["observed_identifier"] for row in crypto_unresolved}),
        "identity_or_boundary_unresolved_observation_count": class_counts.get("IDENTITY_OR_BOUNDARY_UNRESOLVED", 0),
        "universe_schema_history": schema_audit,
        "listing_venue_evidence_available": schema_audit["explicit_listing_venue_field_present"],
        "listing_date_evidence_available": schema_audit["explicit_listing_date_field_present"],
        "delisting_date_evidence_available": schema_audit["explicit_delisting_date_field_present"],
        "investability_evidence_available": schema_audit["explicit_investability_field_present"],
        "absence_interpreted_as_delisting": False,
        "scanner_presence_interpreted_as_investability": False,
        "symbol_suffix_interpreted_as_listing_venue": False,
        "boundary_rows": boundary_rows,
        "unverified_observations": unresolved,
    }
