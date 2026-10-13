from copy import deepcopy
import pytest
from scanner.research.decision_layer.external_watch_context import (
    ExternalWatchContextError, attach_external_context,
)

WATCH = {"rows": [{"symbol": "AEM", "decision": {"action": "HOLD"}, "attention_required": False}]}
SOURCE = {
    "schema_version": "argus_watch_external_context_v1",
    "research_only": True,
    "observations": [{
        "id": "news-001", "symbol": "AEM", "source": "test source",
        "source_url": "https://example.com/report",
        "published_at": "2026-10-12T12:00:00Z",
        "ingested_at": "2026-10-12T12:01:00Z",
        "status": "KNOWN", "summary": "Test observation, not a trading signal",
    }],
}

def test_attach_is_research_only_and_does_not_change_decision():
    before = deepcopy(WATCH)
    result = attach_external_context(WATCH, SOURCE, decision_as_of="2026-10-13T02:19:01Z")
    assert WATCH == before
    assert result["rows"][0]["decision"] == before["rows"][0]["decision"]
    assert result["rows"][0]["attention_required"] is False
    assert result["rows"][0]["external_research_context"]["observations"][0]["decision_effect"] == "none"

def test_empty_records_explicitly_not_supplied():
    source = {**SOURCE, "observations": []}
    result = attach_external_context(WATCH, source, decision_as_of="2026-10-13T02:19:01Z")
    assert result["rows"][0]["external_research_context"]["status"] == "NOT_SUPPLIED"

@pytest.mark.parametrize("field,value", [
    ("published_at", "2026-10-14T00:00:00Z"),
    ("ingested_at", "2026-10-14T00:00:00Z"),
])
def test_future_evidence_rejected(field, value):
    source = deepcopy(SOURCE)
    source["observations"][0][field] = value
    with pytest.raises(ExternalWatchContextError):
        attach_external_context(WATCH, source, decision_as_of="2026-10-13T02:19:01Z")

def test_unknown_status_does_not_invent_evidence():
    source = deepcopy(SOURCE)
    source["observations"][0]["status"] = "UNKNOWN"
    source["observations"][0].pop("source_url")
    result = attach_external_context(WATCH, source, decision_as_of="2026-10-13T02:19:01Z")
    assert result["rows"][0]["external_research_context"]["observations"][0]["status"] == "UNKNOWN"

def test_timezone_and_duplicates_rejected():
    source = deepcopy(SOURCE)
    source["observations"][0]["published_at"] = "2026-10-12T12:00:00"
    with pytest.raises(ExternalWatchContextError):
        attach_external_context(WATCH, source, decision_as_of="2026-10-13T02:19:01Z")
    source = deepcopy(SOURCE)
    source["observations"].append(deepcopy(source["observations"][0]))
    with pytest.raises(ExternalWatchContextError):
        attach_external_context(WATCH, source, decision_as_of="2026-10-13T02:19:01Z")

def test_global_macro_context_stays_out_of_actions():
    source = deepcopy(SOURCE)
    source["observations"][0]["id"] = "macro-001"
    source["observations"][0]["symbol"] = "__GLOBAL__"
    source["observations"][0]["summary"] = "Interest rate scenario, unvalidated"
    result = attach_external_context(WATCH, source, decision_as_of="2026-10-13T02:19:01Z")
    assert result["rows"][0]["decision"] == WATCH["rows"][0]["decision"]
    assert result["rows"][0]["external_research_context"]["status"] == "NOT_SUPPLIED"
    assert result["global_external_research_context"]["observations"][0]["id"] == "macro-001"
    assert result["external_research_context_diagnostics"]["global_observation_count"] == 1
    assert result["external_research_context_diagnostics"]["unmatched_observation_count"] == 0
