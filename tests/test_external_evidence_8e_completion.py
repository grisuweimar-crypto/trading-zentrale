from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.structured_events_8e_completion import (
    StructuredEvents8ECompletionError,
    validate_8e_source_layer_completion,
)


ROOT = Path(__file__).resolve().parents[1]


def _load(name: str) -> dict:
    return json.loads((ROOT / "configs" / name).read_text(encoding="utf-8"))


def test_phase8e_source_layer_completion_gate_passes() -> None:
    result = validate_8e_source_layer_completion(
        structured_config=_load("external_evidence_8e_structured_events_v1.json"),
        fda_config=_load("external_evidence_8e_fda_approval_v1.json"),
        doj_config=_load("external_evidence_8e_doj_antitrust_rss_v1.json"),
        news_config=_load("external_evidence_8e_news_discovery_v1.json"),
    )
    assert result["status"] == "PHASE_8E_SOURCE_LAYER_COMPLETE_OUTCOME_RESEARCH_PENDING_8G"
    assert result["event_type_count"] == result["event_type_classified_count"]
    assert "REGULATORY_APPROVAL" in result["implemented_event_types"]
    assert "LITIGATION_FILED" in result["implemented_event_types"]
    assert "MANAGEMENT_CHANGE" in result["implemented_event_types"]
    assert result["guards"]["market_outcomes_read"] is False
    assert result["guards"]["phase7_integration_enabled"] is False


def test_completion_gate_rejects_unclassified_event_type() -> None:
    structured = _load("external_evidence_8e_structured_events_v1.json")
    structured["event_coverage_matrix"].pop("PRODUCT_LAUNCH")
    with pytest.raises(StructuredEvents8ECompletionError, match="event coverage matrix mismatch"):
        validate_8e_source_layer_completion(
            structured_config=structured,
            fda_config=_load("external_evidence_8e_fda_approval_v1.json"),
            doj_config=_load("external_evidence_8e_doj_antitrust_rss_v1.json"),
            news_config=_load("external_evidence_8e_news_discovery_v1.json"),
        )


def test_completion_gate_rejects_unresolved_source_candidate() -> None:
    structured = _load("external_evidence_8e_structured_events_v1.json")
    structured["source_adapters"]["ISSUER_IR"]["status"] = "SOURCE_CANDIDATE"
    with pytest.raises(StructuredEvents8ECompletionError, match="source candidates remain unresolved"):
        validate_8e_source_layer_completion(
            structured_config=structured,
            fda_config=_load("external_evidence_8e_fda_approval_v1.json"),
            doj_config=_load("external_evidence_8e_doj_antitrust_rss_v1.json"),
            news_config=_load("external_evidence_8e_news_discovery_v1.json"),
        )


def test_completion_gate_rejects_outcome_enablement() -> None:
    structured = _load("external_evidence_8e_structured_events_v1.json")
    structured["research_gate"]["outcome_research_enabled"] = True
    with pytest.raises(StructuredEvents8ECompletionError, match="outcome_research_enabled must be false"):
        validate_8e_source_layer_completion(
            structured_config=structured,
            fda_config=_load("external_evidence_8e_fda_approval_v1.json"),
            doj_config=_load("external_evidence_8e_doj_antitrust_rss_v1.json"),
            news_config=_load("external_evidence_8e_news_discovery_v1.json"),
        )
