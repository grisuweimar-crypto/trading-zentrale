from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

SCHEMA_VERSION = "qm_b_evidence_impact_v1"
RESULT_SCHEMA_VERSION = "qm_b_evidence_impact_result_v1"
DEFAULT_CONTRACT = Path(__file__).resolve().parents[4] / "configs" / "qm_b_evidence_impact_v1.json"


class EvidenceImpactError(ValueError):
    pass


def _git_blob_sha(raw: bytes) -> str:
    header = f"blob {len(raw)}\0".encode("ascii")
    return hashlib.sha1(header + raw).hexdigest()


def load_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path else DEFAULT_CONTRACT
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvidenceImpactError(f"contract_unreadable:{target}") from exc
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise EvidenceImpactError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise EvidenceImpactError("contract_scope_invalid")
    return payload


def verify_source_documents(root: str | Path, contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    root = Path(root)
    rows: list[dict[str, Any]] = []
    for source_id, spec in contract["source_documents"].items():
        path = root / str(spec["path"])
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise EvidenceImpactError(f"source_document_missing:{source_id}:{path}") from exc
        actual = _git_blob_sha(raw)
        expected = str(spec["git_blob_sha"])
        if actual != expected:
            raise EvidenceImpactError(f"source_document_changed_without_impact_review:{source_id}:{actual}")
        rows.append({
            "source_id": source_id,
            "path": str(spec["path"]),
            "git_blob_sha": actual,
            "verified": True,
        })
    return rows


def evaluate(root: str | Path = ".", *, contract_path: str | Path | None = None) -> dict[str, Any]:
    contract = load_contract(contract_path)
    sources = verify_source_documents(root, contract)
    impacts = [dict(row) for row in contract["impact_register"]]
    allowed = set(contract["impact_classes"])
    ids: set[str] = set()
    for row in impacts:
        finding_id = str(row.get("finding_id") or "")
        if not finding_id or finding_id in ids:
            raise EvidenceImpactError("impact_finding_id_invalid_or_duplicate")
        ids.add(finding_id)
        if row.get("impact_class") not in allowed:
            raise EvidenceImpactError(f"impact_class_invalid:{finding_id}")
        if row.get("automatic_global_invalidation") is not False:
            raise EvidenceImpactError(f"automatic_global_invalidation_not_allowed:{finding_id}")

    counts = Counter(str(row["impact_class"]) for row in impacts)
    conclusions = dict(contract["expected_conclusions"])
    if conclusions.get("global_research_invalidation_required") is not False:
        raise EvidenceImpactError("global_invalidation_must_not_be_inferred")
    if conclusions.get("phase1_selection_timing_invalidation_required") is not False:
        raise EvidenceImpactError("phase1_invalidation_conflicts_with_provider_outcome_audit")
    if conclusions.get("daily_research_historical_matcher_revalidation_required") is not True:
        raise EvidenceImpactError("historical_matcher_revalidation_must_be_explicit")
    if conclusions.get("strict_asof_backtest_claim_allowed") is not False:
        raise EvidenceImpactError("strict_backtest_claim_must_remain_blocked")
    if conclusions.get("crypto_cross_namespace_merge_allowed") is not False:
        raise EvidenceImpactError("crypto_cross_namespace_merge_must_remain_blocked")
    if conclusions.get("historical_observations_deleted_or_neutralized") is not False:
        raise EvidenceImpactError("historical_rows_must_not_be_deleted_or_neutralized")

    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "audit_gate_status": "PASS_WITH_SCOPED_RESTRICTIONS",
        "source_document_count": len(sources),
        "source_documents": sources,
        "impact_count": len(impacts),
        "impact_class_counts": dict(sorted(counts.items())),
        "impacts": impacts,
        **conclusions,
        "strict_qm_b_promotion_ready": False,
        "reason_strict_qm_b_promotion_blocked": [
            "historical listing/tradability/execution/investability evidence incomplete",
            "crypto stable asset identity remains PARTIAL_BY_DESIGN",
            "daily_research HistoricalMatcher requires scoped methodology revalidation",
        ],
        "interpretation": (
            "QM-B findings do not justify global invalidation of prior research. "
            "They impose explicit scope, denominator, identity and investability restrictions, "
            "plus local revalidation of HistoricalMatcher-derived historical matches."
        ),
    }


def require_pass(result: Mapping[str, Any]) -> None:
    if result.get("audit_gate_status") != "PASS_WITH_SCOPED_RESTRICTIONS":
        raise EvidenceImpactError("evidence_impact_gate_failed")
    if result.get("global_research_invalidation_required") is not False:
        raise EvidenceImpactError("unexpected_global_research_invalidation")
    if result.get("strict_qm_b_promotion_ready") is not False:
        raise EvidenceImpactError("unexpected_strict_qm_b_promotion")
