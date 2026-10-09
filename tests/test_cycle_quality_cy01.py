"""CY-01 regression: nullable cycle source, independent of price-provider access."""
import math
import pandas as pd

from scanner.data.schema.cycle_quality import normalize_cycle_source
from scanner.reports.daily_research import ALIASES
from scanner.reports.history_delta import build_snapshot_from_watchlist
from scanner.reports.research_views import KNOWN_COLUMNS
from scanner.ui.generator import _render_fallback_tbody, _render_html


def test_blank_legacy_never_falls_back_to_fake_canonical_zero():
    frame = pd.DataFrame({
        "Zyklus %": ["", "  ", None, float("nan")],
        "cycle": [0.0, 0.0, 0.0, 0.0],
    })
    out = normalize_cycle_source(frame)
    assert out["cycle"].isna().all()
    assert set(out["cycle_quality"]) == {"MISSING_SOURCE"}
    assert set(out["cycle_source"]) == {"NONE"}


def test_true_zero_hundred_and_fifty_are_not_deleted():
    frame = pd.DataFrame({
        "Zyklus %": [0, 100, 50, 37.4],
        "cycle": [0, 0, 0, 0],
    })
    out = normalize_cycle_source(frame)
    assert out["cycle"].tolist() == [0.0, 100.0, 50.0, 37.4]
    assert set(out["cycle_quality"]) == {"VALID"}
    assert set(out["cycle_source"]) == {"LEGACY_ZYKLUS_PCT"}


def test_invalid_out_of_range_nonfinite_never_becomes_valid():
    frame = pd.DataFrame({"Zyklus %": ["bad", "-0.1", "101", "inf", "-inf"]})
    out = normalize_cycle_source(frame)
    assert out["cycle"].isna().all()
    assert set(out["cycle_quality"]) == {"INVALID_VALUE"}


def test_explicit_stale_or_insufficient_history_vetoes_numeric_source():
    frame = pd.DataFrame({
        "Zyklus %": ["22", "50", "30"],
        "cycle_quality": ["STALE", "INSUFFICIENT_HISTORY", "VALID"],
    })
    out = normalize_cycle_source(frame)
    assert out["cycle"].isna().tolist() == [True, True, False]
    assert out["cycle_quality"].tolist() == ["STALE", "INSUFFICIENT_HISTORY", "VALID"]


def test_canonical_only_input_can_still_preserve_numeric_zero():
    frame = pd.DataFrame({"cycle": [0, 100, None]})
    out = normalize_cycle_source(frame)
    assert out["cycle"].iloc[0] == 0
    assert out["cycle"].iloc[1] == 100
    assert math.isnan(out["cycle"].iloc[2])
    assert out["cycle_quality"].tolist() == ["VALID", "VALID", "MISSING_SOURCE"]


def test_no_source_fails_closed():
    out = normalize_cycle_source(pd.DataFrame({"score": [42, 17]}))
    assert out["cycle"].isna().all()
    assert out["cycle_quality"].tolist() == ["MISSING_SOURCE", "MISSING_SOURCE"]


def test_research_bridge_preserves_quality_columns_not_score_semantics():
    for field in ("cycle", "cycle_quality", "cycle_source"):
        assert field in ALIASES
        assert field in KNOWN_COLUMNS


def test_snapshot_keeps_nullable_cycle_and_quality_without_mutating_score():
    frame = pd.DataFrame({
        "asset_id": ["AAA", "BBB"],
        "name": ["A", "B"],
        "score": [42.0, 42.0],
        "cycle": [float("nan"), 0.0],
        "cycle_quality": ["MISSING_SOURCE", "VALID"],
        "cycle_source": ["NONE", "LEGACY_ZYKLUS_PCT"],
    })
    snap = build_snapshot_from_watchlist(frame, date="2026-10-09")
    assert snap.loc[0, "cycle_quality"] == "MISSING_SOURCE"
    assert math.isnan(snap.loc[0, "cycle"])
    assert snap.loc[1, "cycle"] == 0
    assert snap.loc[1, "cycle_source"] == "LEGACY_ZYKLUS_PCT"
    assert snap["score"].tolist() == [42.0, 42.0]


def test_server_fallback_ui_never_invents_cycle_zero():
    html = _render_fallback_tbody(pd.DataFrame([
        {"ticker": "AAA", "name": "No cycle", "score": 55, "cycle": float("nan")},
        {"ticker": "BBB", "name": "True zero", "score": 50, "cycle": 0.0},
    ]))
    assert html.count("0%") == 1


def test_client_ui_missing_cycle_uses_dash_not_zero():
    html = _render_html(
        data_records=[],
        presets={"ALL": {"filters": [], "sort": [], "limit": 0}},
        source_csv="fixture.csv", version="test", build="test",
        briefing_text="", briefing_source="", history_delta={},
        segment_monitor={}, reality_check={}, macro_chain_signal={},
        briefing_realities_text="", briefing_realities_source="",
        run_at="", run_src="", run_universe="", fallback_tbody_html="",
    )
    assert "function formatCycle(r)" in html
    assert "r.cycle_quality && r.cycle_quality !== 'VALID'" in html
    assert "cyclePct(r) ?? 0" not in html


def test_daily_research_does_not_resurrect_canonical_null_from_legacy():
    from datetime import datetime, timezone
    from pathlib import Path
    from tempfile import TemporaryDirectory
    from scanner.reports.daily_research import WATCHLIST, begin_daily, generate_daily
    from scanner.reports.research_views import ValidationPolicy, encode_csv, parse_csv

    with TemporaryDirectory() as directory:
        root = Path(directory)
        receipt = root / "receipt.json"
        cols = [
            "market_date", "symbol", "asset_id", "name", "score",
            "OpportunityScore", "RiskScore", "confidence", "rs3m",
            "trend200", "Zyklus %", "cycle", "cycle_quality",
            "cycle_source", "score_status", "trend_ok", "liquidity_ok",
        ]
        def row(symbol, legacy, canonical, quality):
            return dict(zip(cols, [
                "2026-10-08", symbol, symbol, symbol, "42",
                "62", "20", "60", "0.1", "0.2", legacy, canonical,
                quality, "LEGACY_ZYKLUS_PCT", "OK", "True", "True",
            ]))

        begin_daily(root, receipt, run_id="cycle-test")
        path = root / WATCHLIST
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encode_csv(cols, [
            row("STALE", "19", "", "STALE"),
            row("ZERO", "0", "0", "VALID"),
        ]))
        meta = generate_daily(
            root, receipt, policy=ValidationPolicy(expected_symbol_count=2),
            now=datetime(2026, 10, 9, tzinfo=timezone.utc),
        )
        assert meta["latest_run_complete"] is True
        _, latest = parse_csv((root / "artifacts/research/latest_scanner.csv").read_bytes())
        by_symbol = {entry["symbol"]: entry for entry in latest}
        assert by_symbol["STALE"]["cycle"] == ""
        assert by_symbol["STALE"]["cycle_quality"] == "STALE"
        assert by_symbol["ZERO"]["cycle"] == "0"
        assert by_symbol["ZERO"]["cycle_quality"] == "VALID"
        assert all(item["score"] == "42" for item in latest)
