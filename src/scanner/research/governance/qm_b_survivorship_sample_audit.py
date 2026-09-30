"""Static candidate discovery for QM-B survivorship/current-universe leakage.

This scanner deliberately produces REVIEW_REQUIRED candidates, not defect claims.
A concrete call-path review is required before CURRENT_UNIVERSE_DEPENDENCY or other
methodology-risk classification.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_CONTRACT = Path(__file__).resolve().parents[4] / "configs" / "qm_b_survivorship_sample_audit_v1.json"
TEXT_SUFFIXES = {".py", ".yml", ".yaml", ".json", ".md"}


class SurvivorshipAuditError(ValueError):
    pass


def load_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path else DEFAULT_CONTRACT
    payload = json.loads(target.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "qm_b_survivorship_sample_audit_v1":
        raise SurvivorshipAuditError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise SurvivorshipAuditError("contract_scope_invalid")
    return payload


def _hits(text: str, markers: list[str]) -> list[str]:
    low = text.lower()
    return sorted({marker for marker in markers if marker.lower() in low})


def scan_repository(root: str | Path, *, contract_path: str | Path | None = None) -> dict[str, Any]:
    root = Path(root).resolve()
    contract = load_contract(contract_path)
    known_safe = contract.get("known_safe_paths", {})
    candidates: list[dict[str, Any]] = []
    scanned = 0
    for scan_root in contract["scan_roots"]:
        base = root / scan_root
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            rel = path.relative_to(root).as_posix()
            if rel.startswith("src/scanner/research/governance/qm_b_survivorship_sample_audit"):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            scanned += 1
            historical = _hits(text, contract["historical_markers"])
            current_universe = _hits(text, contract["current_universe_markers"])
            current_snapshot = _hits(text, contract["current_snapshot_markers"])
            if historical and (current_universe or current_snapshot):
                candidates.append({
                    "path": rel,
                    "classification": "SAFE" if rel in known_safe else "REVIEW_REQUIRED",
                    "historical_markers": historical,
                    "current_universe_markers": current_universe,
                    "current_snapshot_markers": current_snapshot,
                    "known_safe_reason": known_safe.get(rel),
                })
    counts: dict[str, int] = {}
    for row in candidates:
        counts[row["classification"]] = counts.get(row["classification"], 0) + 1
    return {
        "schema_version": "qm_b_survivorship_sample_audit_result_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "files_scanned": scanned,
        "candidate_count": len(candidates),
        "classification_counts": dict(sorted(counts.items())),
        "candidates": candidates,
        "defect_claims_performed": False,
        "historical_retrojection_permitted": False,
    }
