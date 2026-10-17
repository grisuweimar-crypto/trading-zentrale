from scanner.research.decision_layer.watch_explainability import explain_watch, render_markdown
import pytest


def test_missing_elliott_visible_without_fabrication():
    watch = {
        "source_snapshot_id": "snapshot-test",
        "watch_status": "complete",
        "rows": [{"symbol": "UEC", "availability": "decision_available",
                  "decision": {"portfolio_action_state": "HOLD",
                               "universal_stance_state": "HOLD",
                               "reliability_assessment": "limited"}}],
    }
    manifest = {"snapshot_id": "snapshot-test",
                "stages": {"phase6_elliott": {"status": "not_supplied"}}}
    report = explain_watch(watch, manifest)
    assert report["rows"][0]["elliott_source"] == "not_supplied_at_w10"
    assert report["changes_portfolio_action"] is False
    assert "keine Neuberechnung" in render_markdown(report)


def test_snapshot_mismatch_fails_closed():
    with pytest.raises(ValueError, match="snapshot"):
        explain_watch({"source_snapshot_id": "old"}, {"snapshot_id": "new"})


def test_late_elliott_injection_fails_closed():
    watch = {"source_snapshot_id": "x",
             "rows": [{"symbol": "UUUU", "elliott_swing_context": {}}]}
    manifest = {"snapshot_id": "x", "stages": {"phase6_elliott": {"status": "not_supplied"}}}
    with pytest.raises(ValueError, match="elliott"):
        explain_watch(watch, manifest)
