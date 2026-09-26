from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


RESULT_SCHEMA = "external_evidence_8e_completion_v1"


class StructuredEvents8ECompletionError(ValueError):
    pass


def _require_false(mapping: Mapping[str, Any], fields: tuple[str, ...], *, label: str) -> None:
    for field in fields:
        if mapping.get(field) is not False:
            raise StructuredEvents8ECompletionError(f"{label}.{field} must be false")


def validate_8e_source_layer_completion(
    *,
    structured_config: Mapping[str, Any],
    fda_config: Mapping[str, Any],
    doj_config: Mapping[str, Any],
    news_config: Mapping[str, Any],
    primary_release_config: Mapping[str, Any],
) -> dict[str, Any]:
    expected_schemas = {
        "structured": "external_evidence_8e_structured_events_v1",
        "fda": "external_evidence_8e_fda_approval_v1",
        "doj": "external_evidence_8e_doj_antitrust_rss_v1",
        "news": "external_evidence_8e_news_discovery_v1",
        "primary": "external_evidence_8e_primary_release_v1",
    }
    payloads = {
        "structured": structured_config,
        "fda": fda_config,
        "doj": doj_config,
        "news": news_config,
        "primary": primary_release_config,
    }
    for label, expected in expected_schemas.items():
        if payloads[label].get("schema_version") != expected:
            raise StructuredEvents8ECompletionError(f"unsupported {label} config schema")

    principles = structured_config.get("principles") or {}
    _require_false(
        principles,
        (
            "market_outcomes_may_be_read",
            "market_direction_may_be_assigned",
            "phase7_integration_enabled",
            "production_external_evidence_enabled",
            "generic_sentiment_enabled",
            "missing_evidence_may_default_to_neutral",
            "current_news_may_be_retrojected",
        ),
        label="principles",
    )
    gate = structured_config.get("research_gate") or {}
    _require_false(
        gate,
        (
            "outcome_research_enabled",
            "threshold_selection_enabled",
            "interaction_research_enabled",
            "phase7_integration_enabled",
            "production_external_evidence_enabled",
        ),
        label="research_gate",
    )

    taxonomy = {str(value) for value in structured_config.get("initial_event_taxonomy") or []}
    coverage = structured_config.get("event_coverage_matrix") or {}
    if not taxonomy:
        raise StructuredEvents8ECompletionError("initial event taxonomy is empty")
    if set(coverage) != taxonomy:
        missing = sorted(taxonomy - set(coverage))
        extra = sorted(set(coverage) - taxonomy)
        raise StructuredEvents8ECompletionError(
            f"event coverage matrix mismatch; missing={missing}, extra={extra}"
        )
    if any(not str(value or "").strip() for value in coverage.values()):
        raise StructuredEvents8ECompletionError("every event type requires an explicit coverage state")

    adapters = structured_config.get("source_adapters") or {}
    unresolved_candidates = sorted(
        key for key, value in adapters.items()
        if str((value or {}).get("status") or "").strip() == "SOURCE_CANDIDATE"
    )
    if unresolved_candidates:
        raise StructuredEvents8ECompletionError(
            "source candidates remain unresolved: " + ", ".join(unresolved_candidates)
        )

    required_adapter_statuses = {
        "FDA_DRUGS_AT_FDA": "IMPLEMENTED_AUTHORITATIVE_APPROVAL_STATE",
        "DOJ_ANTITRUST_CASE_FILINGS": "IMPLEMENTED_AUTHORITATIVE_CASE_FILING_FEEDS",
        "PHASE8C_CANONICAL_SEC_EVENTS": "IMPLEMENTED_CONSERVATIVE_REUSE",
        "PRIMARY_RELEASE_CHALLENGER": "IMPLEMENTED_PROSPECTIVE_STRUCTURED_CHALLENGER",
    }
    for key, expected in required_adapter_statuses.items():
        if adapters.get(key, {}).get("status") != expected:
            raise StructuredEvents8ECompletionError(
                f"{key} adapter is not frozen as {expected}"
            )

    _require_false(
        fda_config.get("hard_boundaries") or {},
        (
            "market_outcomes_may_be_read",
            "market_direction_may_be_assigned",
            "approval_is_predefined_bullish",
            "phase7_integration_enabled",
            "production_external_evidence_enabled",
        ),
        label="fda.hard_boundaries",
    )
    _require_false(
        doj_config.get("hard_boundaries") or {},
        (
            "market_outcomes_may_be_read",
            "market_direction_may_be_assigned",
            "litigation_is_predefined_bullish_or_bearish",
            "rss_title_semantic_reclassification_enabled",
            "phase7_integration_enabled",
            "production_external_evidence_enabled",
        ),
        label="doj.hard_boundaries",
    )
    _require_false(
        news_config.get("hard_boundaries") or {},
        (
            "generic_sentiment_enabled",
            "market_outcomes_may_be_read",
            "phase7_integration_enabled",
            "production_external_evidence_enabled",
        ),
        label="news.hard_boundaries",
    )
    _require_false(
        primary_release_config.get("hard_boundaries") or {},
        (
            "market_outcomes_may_be_read",
            "market_direction_may_be_assigned",
            "threshold_selection_enabled",
            "phase7_integration_enabled",
            "production_external_evidence_enabled",
        ),
        label="primary.hard_boundaries",
    )
    pit = primary_release_config.get("pit_contract") or {}
    if pit.get("prospective_observation_is_strict_pit_from_ingested_at") is not True:
        raise StructuredEvents8ECompletionError("primary release prospective PIT gate is not enabled")
    if pit.get("page_date_alone_is_historical_proof") is not False:
        raise StructuredEvents8ECompletionError("primary release page date may not prove historical PIT")
    if pit.get("later_snapshot_may_backdate_knowledge") is not False:
        raise StructuredEvents8ECompletionError("primary release later snapshots may not backdate knowledge")

    completion = structured_config.get("completion_contract") or {}
    if completion.get("architecture_and_source_layer_complete") is not True:
        raise StructuredEvents8ECompletionError("source-layer completion flag is not true")
    if completion.get("predictive_outcome_validation_belongs_to_phase8g") is not True:
        raise StructuredEvents8ECompletionError("predictive validation must remain assigned to Phase 8G")

    implemented = sorted(
        event for event, state in coverage.items() if str(state).startswith("IMPLEMENTED_")
    )
    challenger = sorted(
        event for event, state in coverage.items() if "CHALLENGER" in str(state)
    )
    deferred = sorted(
        event for event in taxonomy if event not in implemented and event not in challenger
    )
    discovery_only = sorted(
        key for key, value in adapters.items()
        if "DISCOVERY_ONLY" in str((value or {}).get("status") or "")
    )
    return {
        "schema_version": RESULT_SCHEMA,
        "phase": "8E_structured_events_news",
        "status": "PHASE_8E_SOURCE_LAYER_COMPLETE_OUTCOME_RESEARCH_PENDING_8G",
        "implemented_event_types": implemented,
        "prospective_challenger_event_types": challenger,
        "explicitly_deferred_or_source_gap_event_types": deferred,
        "discovery_only_sources": discovery_only,
        "event_type_count": len(taxonomy),
        "event_type_classified_count": len(coverage),
        "source_adapter_count": len(adapters),
        "guards": {
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "generic_sentiment_enabled": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
            "predictive_validation_deferred_to_phase8g": True,
        },
    }


def load_and_validate_8e_completion(
    *,
    structured_config_path: Path,
    fda_config_path: Path,
    doj_config_path: Path,
    news_config_path: Path,
    primary_release_config_path: Path,
) -> dict[str, Any]:
    paths = (
        structured_config_path,
        fda_config_path,
        doj_config_path,
        news_config_path,
        primary_release_config_path,
    )
    payloads: list[dict[str, Any]] = []
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise StructuredEvents8ECompletionError(f"cannot load config: {path}") from exc
        if not isinstance(payload, dict):
            raise StructuredEvents8ECompletionError(f"config root must be an object: {path}")
        payloads.append(payload)
    return validate_8e_source_layer_completion(
        structured_config=payloads[0],
        fda_config=payloads[1],
        doj_config=payloads[2],
        news_config=payloads[3],
        primary_release_config=payloads[4],
    )


def write_completion_result(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
