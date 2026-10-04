from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

import scanner.research.elliott_vnext.prospective_capture as capture_module
from scanner.research.elliott_vnext.prospective_capture import (
    ProspectiveCaptureError,
    archive_capture,
    build_prospective_capture,
)


def _daily(*symbols: str) -> dict[str, object]:
    return {
        "snapshot_id": "snapshot-2026-10-01",
        "as_of": "2026-10-01",
        "universe_size": len(symbols),
        "symbols": {symbol: {} for symbol in symbols},
    }


def _routed(symbol: str, timeframe: str, degree: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "as_of": "2026-10-01",
        "timeframe": timeframe,
        "degree": degree,
        "research_only": True,
    }


def _output(routed: dict[str, object]) -> dict[str, object]:
    symbol = str(routed["symbol"])
    timeframe = str(routed["timeframe"])
    degree = str(routed["degree"])
    return {
        "schema_version": "elliott_vnext_output_v2",
        "module": "6H_module_output",
        "symbol": symbol,
        "as_of": "2026-10-01",
        "timeframe": timeframe,
        "degree": degree,
        "output_id": f"{symbol}-{timeframe}-{degree}",
        "research_only": True,
        "integration": {
            "productive_integration_enabled": False,
            "direct_ordering_allowed": False,
        },
    }


def _prices(*symbols: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-10-01",
                "symbol": symbol,
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "adj_close": 100,
                "volume": 1000,
            }
            for symbol in symbols
        ]
    )


def _build(monkeypatch, daily, prices):
    calls = []

    def fake_replay(frame, symbol, *, config, as_of_dates, keep_unchanged):
        calls.append((symbol, list(as_of_dates), keep_unchanged, config.replay_price_basis))
        if symbol == "AAA":
            states = [
                _routed("AAA", "daily", "fine"),
                _routed("AAA", "weekly", "coarse"),
            ]
        else:
            states = []
        return states, {
            "symbol": symbol,
            "status": "ok" if states else "no_elliott_state_emitted",
            "evaluated_dates": 1,
            "snapshots": len(states),
            "future_rows_used": False,
        }

    monkeypatch.setattr(capture_module, "replay_symbol_states", fake_replay)
    monkeypatch.setattr(capture_module, "build_module_output", _output)
    monkeypatch.setattr(capture_module, "validate_module_output", lambda value: value)
    result = build_prospective_capture(
        prices,
        daily,
        source_publication_commit="a" * 40,
        scanner_published_at="2026-10-01T20:00:00+00:00",
        run_id="github-123-1",
        price_source_sha256="price-hash",
        daily_source_sha256="daily-hash",
        captured_at="2026-10-01T20:05:00+00:00",
    )
    return result, calls


def test_capture_runs_exact_asof_and_keeps_all_degrees(monkeypatch):
    result, calls = _build(monkeypatch, _daily("AAA", "BBB"), _prices("AAA", "BBB"))

    assert calls == [
        ("AAA", ["2026-10-01"], True, "adjusted"),
        ("BBB", ["2026-10-01"], True, "adjusted"),
    ]
    assert result["capture_engine_version"] == "prospective_capture_engine_v2_iso_date_replay"
    assert result["validation_partition"] == "prospective_unspent"
    assert result["output_count"] == 2
    assert result["symbols_with_outputs"] == 1
    assert {(row["timeframe"], row["degree"]) for row in result["outputs"]} == {
        ("daily", "fine"),
        ("weekly", "coarse"),
    }
    assert result["coverage"]["symbols_without_outputs"] == 1
    assert result["guards"] == {
        "research_only": True,
        "productive_integration_enabled": False,
        "w10_source_emitted": False,
        "changes_universal_stance": False,
        "changes_portfolio_action": False,
        "direct_ordering_allowed": False,
        "future_rows_used": False,
        "missing_evidence_not_imputed": True,
        "frozen_elliott_core_modified": False,
        "multi_degree_outputs_retained_without_reducer": True,
    }


def test_missing_symbol_state_stays_missing_not_neutral(monkeypatch):
    result, _calls = _build(monkeypatch, _daily("BBB"), _prices("BBB"))

    assert result["outputs"] == []
    assert result["output_count"] == 0
    assert result["symbols_with_outputs"] == 0
    assert result["coverage"]["symbols_without_outputs"] == 1
    assert result["coverage"]["missing_evidence_not_imputed"] is True


def test_duplicate_symbol_timeframe_degree_fails_closed(monkeypatch):
    def fake_replay(frame, symbol, *, config, as_of_dates, keep_unchanged):
        return [
            _routed(symbol, "daily", "fine"),
            _routed(symbol, "daily", "fine"),
        ], {"symbol": symbol, "snapshots": 2}

    monkeypatch.setattr(capture_module, "replay_symbol_states", fake_replay)
    monkeypatch.setattr(capture_module, "build_module_output", _output)
    monkeypatch.setattr(capture_module, "validate_module_output", lambda value: value)

    with pytest.raises(ProspectiveCaptureError, match="duplicate_symbol_timeframe_degree"):
        build_prospective_capture(
            _prices("AAA"),
            _daily("AAA"),
            source_publication_commit="b" * 40,
            scanner_published_at="2026-10-01T20:00:00+00:00",
            run_id="github-123-2",
            price_source_sha256="price-hash",
            daily_source_sha256="daily-hash",
            captured_at="2026-10-01T20:05:00+00:00",
        )


def test_archive_is_idempotent_and_rejects_conflicting_same_snapshot(tmp_path, monkeypatch):
    capture, _calls = _build(monkeypatch, _daily("AAA"), _prices("AAA"))
    path = tmp_path / "history.jsonl"

    appended, stored = archive_capture(path, capture)
    assert appended is True
    assert stored == capture

    appended_again, stored_again = archive_capture(path, capture)
    assert appended_again is False
    assert stored_again == capture
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1

    conflict = deepcopy(capture)
    conflict["capture_id"] = "different"
    with pytest.raises(ProspectiveCaptureError, match="prospective_capture_identity_conflict"):
        archive_capture(path, conflict)


def test_capture_cannot_predate_scanner_publication(monkeypatch):
    with pytest.raises(ProspectiveCaptureError, match="capture_before_scanner_publication"):
        build_prospective_capture(
            _prices("AAA"),
            _daily("AAA"),
            source_publication_commit="c" * 40,
            scanner_published_at="2026-10-01T20:05:00+00:00",
            run_id="github-123-3",
            price_source_sha256="price-hash",
            daily_source_sha256="daily-hash",
            captured_at="2026-10-01T20:00:00+00:00",
        )


def test_legacy_empty_capture_can_be_superseded_once_after_replay_fix(tmp_path, monkeypatch):
    repaired, _calls = _build(monkeypatch, _daily("AAA"), _prices("AAA"))
    path = tmp_path / "history.jsonl"

    legacy = deepcopy(repaired)
    legacy.pop("capture_engine_version", None)
    legacy["capture_id"] = "legacy-empty-capture"
    legacy["outputs"] = []
    legacy["output_count"] = 0
    legacy["symbols_with_outputs"] = 0
    legacy["coverage"] = {
        "symbols_requested": 1,
        "symbols_with_outputs": 0,
        "symbols_without_outputs": 1,
        "output_count": 0,
        "details": [],
        "missing_evidence_not_imputed": True,
    }
    path.write_text(json.dumps(legacy) + "\n", encoding="utf-8")

    appended, stored = archive_capture(path, repaired)

    assert appended is True
    assert stored["capture_engine_version"] == "prospective_capture_engine_v2_iso_date_replay"
    assert stored["output_count"] == 2
    assert stored["repair"]["legacy_records_preserved"] is True
    assert stored["repair"]["supersedes_capture_ids"] == ["legacy-empty-capture"]
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert rows[0]["capture_id"] == "legacy-empty-capture"
    assert rows[1]["capture_id"] == repaired["capture_id"]


def test_repaired_capture_does_not_supersede_nonempty_legacy_record(tmp_path, monkeypatch):
    repaired, _calls = _build(monkeypatch, _daily("AAA"), _prices("AAA"))
    path = tmp_path / "history.jsonl"

    legacy = deepcopy(repaired)
    legacy.pop("capture_engine_version", None)
    legacy["capture_id"] = "legacy-nonempty-capture"
    path.write_text(json.dumps(legacy) + "\n", encoding="utf-8")

    with pytest.raises(ProspectiveCaptureError, match="prospective_capture_identity_conflict"):
        archive_capture(path, repaired)
