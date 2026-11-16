"""UEC exemplar: real archived closed bars -> scanner -> ARGUS W10 -> Watch.

No private portfolio fixtures, no mock observations, no invented Elliott vote.
The daily artifacts are real main snapshots; historical returns are research only.
"""
from __future__ import annotations

import csv
from datetime import date
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
RESEARCH = ROOT / "artifacts/research"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_uec_real_snapshot_consistent_across_layers():
    daily = _json(RESEARCH / "daily_research.json")
    w10 = _json(RESEARCH / "decision_snapshot_w10.json")
    runtime = _json(RESEARCH / "watch_runtime/manifest.json")
    meta = _json(RESEARCH / "history_metadata.json")
    symbol_views = _json(RESEARCH / "watch_runtime/symbols/index.json")
    symbols = daily["symbols"]
    assert "UEC" in symbols, "UEC unexpectedly missing from scanner universe"
    uec = symbols["UEC"]
    snap = daily["snapshot_id"]
    assert meta["latest_run_complete"] is True
    assert meta["latest_run_error"] is None
    assert snap == meta["snapshot_id"] == w10["snapshot_id"] == runtime["snapshot_id"] == symbol_views["snapshot_id"]
    assert w10["status"] == "sealed"
    assert runtime["w10_status"] == "sealed"
    assert runtime["private_position_data_included"] is False
    assert runtime["decision_logic_changed"] is False
    assert symbol_views["private_position_data_included"] is False
    assert symbol_views["decision_logic_changed"] is False
    assert symbol_views["source_runtime_projection_sha256"] == runtime["runtime_projection_sha256"]
    assert runtime["symbol_shards"]["UEC"].startswith("shard_")
    assert "UEC" in symbol_views["summary_files"]
    assert "UEC" in symbol_views["symbol_files"]

    with (RESEARCH / "latest_scanner.csv").open(newline="", encoding="utf-8") as stream:
        rows = [row for row in csv.DictReader(stream) if row["symbol"] == "UEC"]
    assert len(rows) == 1
    row = rows[0]
    assert row["snapshot_id"] == snap
    # A completed, fully traceable scan is not automatically a BUY candidate.
    # UEC can legitimately be marked AVOID (e.g. score 0 and weak trend).
    # Check data integrity below and verify the decision stays non-ordering;
    # never convert a negative scanner disposition into OK to pass CI.
    assert row["score_status"] in {"OK", "AVOID"}, (
        f"Unexpected scanner disposition for UEC: {row['score_status']}"
    )
    assert row["cycle_quality"] == "VALID"
    assert row["cycle_quality_reason"] == "COMPLETED_DAILY_BARS"
    assert date.fromisoformat(row["cycle_last_bar"]) <= date.fromisoformat(daily["as_of"])
    assert float(row["cycle_eligible_bars"]) >= 200
    assert float(row["close"]) == pytest.approx(uec["current"]["close"], rel=1e-9)
    assert float(row["score"]) == pytest.approx(uec["current"]["score"], abs=1e-9)
    assert float(row["confidence"]) == pytest.approx(uec["current"]["confidence"], abs=1e-9)
    assert float(row["rank"]) == uec["current"]["rank"]
    assert row["r_code"] == uec["current"]["r_code"]
    assert 0 <= float(row["score"]) <= 100

    for scenario,expected_position in (("public_long_reference.json", "long"), ("public_flat_reference.json", "flat")):
        reference = _json(RESEARCH / "watch_runtime" / scenario)
        assert reference["snapshot_id"] == snap
        assert reference["position_assumption"]["position_state"] == expected_position
        assert reference["position_assumption"]["private_position_data_included"] is False
        entries = [x for x in reference["rows"] if x["symbol"] == "UEC"]
        assert len(entries) == 1
        decision = entries[0]["decision"]
        assert entries[0]["availability"] == "decision_available"
        assert entries[0]["current"]["close"] == pytest.approx(float(row["close"]), rel=1e-9)
        assert decision["phase5_shadow_production_change_performed"] is False
        assert decision["phase5_shadow_changes_portfolio_action"] is False
        assert decision["state_history_changed_universal_stance"] is False
        assert decision["path_review_is_trade_decision"] is False
        assert decision["portfolio_action_state"] not in ("BUY", "SELL", "ORDER")
        if decision["universal_stance_state"] == "insufficient_evidence":
            assert decision["portfolio_action_state"] == ("HOLD" if expected_position == "long" else "NO_ACTION")



def test_avoid_is_a_trade_disposition_not_an_analysis_access_gate():
    """Every AVOID symbol stays available in research and public Watch views.

    A negative market assessment must not remove the analysis. Elliott output
    can be absent for independent evidence reasons, never because AVOID
    suppressed the symbol entirely.
    """
    daily = _json(RESEARCH / "daily_research.json")
    symbol_index = _json(RESEARCH / "watch_runtime/symbols/index.json")
    flat = _json(RESEARCH / "watch_runtime/public_flat_reference.json")
    long = _json(RESEARCH / "watch_runtime/public_long_reference.json")
    flat_by_symbol = {entry["symbol"]: entry for entry in flat["rows"]}
    long_by_symbol = {entry["symbol"]: entry for entry in long["rows"]}

    with (RESEARCH / "latest_scanner.csv").open(newline="", encoding="utf-8") as stream:
        avoid_symbols = {
            row["symbol"] for row in csv.DictReader(stream)
            if row["score_status"] == "AVOID"
        }
    assert avoid_symbols, "No AVOID case available for real-snapshot acceptance"

    for symbol in sorted(avoid_symbols):
        assert symbol in daily["symbols"], f"AVOID hid research for {symbol}"
        assert symbol in flat_by_symbol, f"AVOID hid public flat scenario for {symbol}"
        assert symbol in long_by_symbol, f"AVOID hid public long scenario for {symbol}"
        assert flat_by_symbol[symbol]["availability"] == "decision_available"
        assert long_by_symbol[symbol]["availability"] == "decision_available"
        assert symbol in symbol_index["symbol_files"], (
            f"AVOID hid per-symbol packets for {symbol}"
        )
        assert symbol in symbol_index["summary_files"], (
            f"AVOID hid per-symbol summary for {symbol}"
        )
        summary_path = (
            RESEARCH / "watch_runtime/symbols"
            / symbol_index["summary_files"][symbol]
        )
        summary = _json(summary_path)
        assert summary["symbol"] == symbol
        assert summary["snapshot_id"] == daily["snapshot_id"]
        elliott = summary["elliott_vnext_research"]
        assert elliott["research_only"] is True
        assert elliott["direct_ordering_allowed"] is False
        # Zero computed Elliott counts are an evidence/coverage gap,
        # not permission to suppress the research or invent a forecast.


def test_capture_readers_handle_both_lossless_formats_without_stale_fallback():
    paths = (
        ".github/workflows/decision_watch_pipeline.yml",
        ".github/workflows/decision_watch_runtime.yml",
        ".github/workflows/decision_watch_symbol_views.yml",
    )
    for path in paths:
        workflow = (ROOT / path).read_text(encoding="utf-8")
        assert "elliott_vnext_prospective_current_6h.json" in workflow
        assert "gzip -t" in workflow
        assert "gzip -cd" in workflow
        assert "CAPTURE" in workflow or "capture=" in workflow
        assert "elliott-vnext-shadow-data" in workflow
    w10 = (ROOT / paths[0]).read_text(encoding="utf-8")
    assert 'git log -1 --format=%H origin/elliott-vnext-shadow-data -- "$CAPTURE_STORAGE_PATH"' in w10
    runtime = (ROOT / paths[1]).read_text(encoding="utf-8")
    assert 'git show "$SOURCE_COMMIT:$CAPTURE_PATH.gz"' in runtime
    assert 'git show "$SOURCE_COMMIT:$CAPTURE_PATH"' in runtime
