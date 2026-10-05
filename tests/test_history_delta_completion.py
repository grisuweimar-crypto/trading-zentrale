from __future__ import annotations

import pandas as pd

from scanner.reports.daily_research import ALIASES, DailyInput
from scanner.reports.history_delta import (
    HISTORY_SCHEMA_VERSION,
    build_snapshot_from_watchlist,
    compute_history_delta,
)
from scanner.reports.reality_check import build_reality_check
from scanner.reports.segment_monitor import compute_segment_monitor
from scanner.ui.generator import _render_fallback_tbody, _render_html


def test_fallback_table_keeps_dscore_column_alignment() -> None:
    empty_html = _render_fallback_tbody(pd.DataFrame())
    assert 'colspan="11"' in empty_html

    html = _render_fallback_tbody(
        pd.DataFrame(
            [
                {
                    "ticker": "AAA",
                    "name": "Alpha",
                    "price": 222.27000042724609,
                    "score": 42.0,
                    "confidence": 70.0,
                    "cycle": 50.0,
                    "trend_ok": True,
                    "liquidity_ok": True,
                    "score_status": "OK",
                    "is_crypto": False,
                }
            ]
        )
    )
    assert html.count("<td") == 11
    assert "222.27" in html
    assert "222.27000042724609" not in html
    assert "42.00" in html
    assert "70.0" in html
    assert "50%" in html


def test_ui_polish_exposes_history_basis_filters_and_explainability() -> None:
    html = _render_html(
        data_records=[],
        presets={"ALL": {"filters": [], "sort": [], "limit": 0}},
        source_csv="fixture.csv",
        version="test",
        build="test",
        briefing_text="",
        briefing_source="",
        history_delta={},
        segment_monitor={},
        reality_check={},
        macro_chain_signal={},
        briefing_realities_text="",
        briefing_realities_source="",
        run_at="",
        run_src="",
        run_universe="",
        fallback_tbody_html="",
    )
    assert 'id="activeFilters"' in html
    assert 'data-key="onlyHistoryBasis"' in html
    assert "Nur Werte mit belastbarer dScore-1D-Vergleichsbasis" in html
    assert "kein Handelssignal" in html
    assert "sampleThin" in html
    assert "sampleUnclear" in html
    assert "Datenbasis: History" in html


def test_productive_daily_bridge_preserves_history_status_contract() -> None:
    assert "trend_ok" in ALIASES
    assert "liquidity_ok" in ALIASES
    assert "score_status" in ALIASES

    raw = (
        "score,trend_ok,liquidity_ok,score_status\n"
        "42,true,false,OK\n"
    ).encode("utf-8")
    daily = DailyInput(
        source="fixture.csv",
        source_sha256=None,
        columns=[],
        rows=[],
        errors=[],
        context={},
        watchlist_raw=raw,
    )
    enriched = daily.enrich([{"r_code": ""}])
    assert enriched[0]["history_schema_version"] == HISTORY_SCHEMA_VERSION


def test_history_snapshot_carries_status_and_schema_fields() -> None:
    watchlist = pd.DataFrame(
        [
            {
                "asset_id": "AAA",
                "name": "Alpha",
                "score": 42.0,
                "trend_ok": True,
                "liquidity_ok": False,
                "score_status": "OK",
                "pillar_primary": "S1",
                "cluster_official": "Cluster A",
                "sector": "Technology",
            }
        ]
    )

    snapshot = build_snapshot_from_watchlist(watchlist, date="2026-10-05")
    row = snapshot.iloc[0]

    assert row["history_schema_version"] == HISTORY_SCHEMA_VERSION
    assert bool(row["trend_ok"]) is True
    assert bool(row["liquidity_ok"]) is False
    assert row["score_status"] == "OK"
    assert int(row["universe_size"]) == 1


def test_history_events_require_real_previous_basis() -> None:
    rows: list[dict[str, object]] = []
    for idx in range(1, 31):
        symbol = f"S{idx:02d}"
        base = {
            "symbol": symbol,
            "name": symbol,
            "trend_ok": True,
            "liquidity_ok": True,
            "score_status": "OK",
        }
        rows.append({"date": "2026-10-04", "score": 101 - idx, **base})

        score = 101 - idx
        trend = True
        status = "OK"
        if symbol == "S11":
            trend = False
            status = "AVOID"
        elif symbol == "S26":
            score = 200
        elif symbol == "S05":
            score = -100
        rows.append(
            {
                "date": "2026-10-05",
                "score": score,
                **base,
                "trend_ok": trend,
                "score_status": status,
            }
        )

    rows.append(
        {
            "date": "2026-10-05",
            "symbol": "NEW",
            "name": "New",
            "score": 50,
            "trend_ok": True,
            "liquidity_ok": True,
            "score_status": "OK",
        }
    )

    _, payload = compute_history_delta(pd.DataFrame(rows))
    by_symbol = payload["by_symbol"]

    s11_events = {event["type"] for event in by_symbol["S11"]["events"]}
    assert "trend_ok_changed" in s11_events
    assert "score_status_changed" in s11_events

    s26_events = {event["type"] for event in by_symbol["S26"]["events"]}
    assert "entered_top_10" in s26_events
    assert "entered_top_25" in s26_events

    s05_events = {event["type"] for event in by_symbol["S05"]["events"]}
    assert "left_top_10" in s05_events
    assert "left_top_25" in s05_events

    assert by_symbol["NEW"]["status"] == "new"
    assert by_symbol["NEW"]["events"] == []


def test_universe_addition_does_not_create_fake_rank_threshold_event() -> None:
    rows = []
    for rank in range(1, 11):
        score = 100 - rank
        for date in ("2026-10-04", "2026-10-05"):
            rows.append(
                {
                    "date": date,
                    "symbol": f"S{rank:02d}",
                    "name": f"S{rank:02d}",
                    "score": score,
                }
            )

    # A new top scorer changes full-snapshot ranks, but must not change the
    # stable common-universe rank of the existing titles.
    rows.append(
        {
            "date": "2026-10-05",
            "symbol": "NEW",
            "name": "New",
            "score": 1000,
        }
    )

    _, payload = compute_history_delta(pd.DataFrame(rows))
    s10 = payload["by_symbol"]["S10"]
    assert s10["rank_prev"] == 10
    assert s10["rank_now"] == 11
    assert s10["rank_prev_common"] == 10
    assert s10["rank_now_common"] == 10
    assert s10["rank_delta"] == 0
    assert s10["events"] == []


def test_missing_score_has_no_fake_rank_delta_or_event() -> None:
    history = pd.DataFrame(
        [
            {
                "date": "2026-10-04",
                "symbol": "AAA",
                "name": "Alpha",
                "score": "10",
                "trend_ok": "True",
                "liquidity_ok": "True",
                "score_status": "OK",
            },
            {
                "date": "2026-10-05",
                "symbol": "AAA",
                "name": "Alpha",
                "score": "",
                "trend_ok": "False",
                "liquidity_ok": "True",
                "score_status": "AVOID",
            },
        ]
    )
    _, payload = compute_history_delta(history)
    row = payload["by_symbol"]["AAA"]
    assert row["score_delta"] is None
    assert row["rank_delta"] is None
    assert payload["stats"]["comparison_basis_count"] == 0
    event_types = {event["type"] for event in row["events"]}
    assert "entered_top_10" not in event_types
    assert "left_top_10" not in event_types
    assert "entered_top_25" not in event_types
    assert "left_top_25" not in event_types
    # Independent status comparisons remain valid even when the score is missing.
    assert event_types == {"trend_ok_changed", "score_status_changed"}


def test_segment_monitor_reports_dscore_breadth_and_coverage() -> None:
    current = pd.DataFrame(
        [
            {"date": "2026-10-05", "symbol": "A", "pillar_primary": "P1", "cluster_official": "C1", "sector": "Tech", "bucket_type": "x"},
            {"date": "2026-10-05", "symbol": "B", "pillar_primary": "P1", "cluster_official": "C1", "sector": "Tech", "bucket_type": "x"},
            {"date": "2026-10-05", "symbol": "C", "pillar_primary": "P1", "cluster_official": "C1", "sector": "Tech", "bucket_type": "x"},
            {"date": "2026-10-05", "symbol": "D", "pillar_primary": "P1", "cluster_official": "C1", "sector": "Tech", "bucket_type": "x"},
            {"date": "2026-10-05", "symbol": "E", "pillar_primary": "P1", "cluster_official": "C1", "sector": "Tech", "bucket_type": "x"},
            {"date": "2026-10-05", "symbol": "F", "pillar_primary": "P2", "cluster_official": "", "sector": "Health", "bucket_type": "y"},
        ]
    )
    history_delta = {
        "by_symbol": {
            "A": {"status": "ok", "score_delta": 1.0},
            "B": {"status": "ok", "score_delta": 0.5},
            "C": {"status": "ok", "score_delta": -0.5},
            "D": {"status": "ok", "score_delta": 0.0},
            "E": {"status": "ok", "score_delta": 1.0},
            "F": {"status": "new", "score_delta": None},
        }
    }

    _, payload = compute_segment_monitor(
        pd.DataFrame(),
        current,
        history_delta=history_delta,
    )

    p1 = next(row for row in payload["internal_segments"] if row["segment"] == "P1")
    assert p1["n_total"] == 5
    assert p1["n_valid"] == 5
    assert p1["stable_sample"] is True
    assert p1["coverage"] == 1.0
    assert round(float(p1["average_dscore_1d"]), 2) == 0.40
    assert round(float(p1["positive_share"]), 2) == 0.60

    health = next(row for row in payload["official_segments"] if row["segment"] == "Health")
    assert health["sample_state"] == "unavailable"
    assert health["coverage"] == 0.0


def test_reality_check_uses_transparent_segment_categories() -> None:
    rows = []
    history = {"by_symbol": {}}
    definitions = [
        ("P1", "C1"),
        ("P2", "C2"),
        ("P3", "C3"),
        ("P4", "C4"),
    ]
    for pair_index, (pillar, cluster) in enumerate(definitions, start=1):
        for member in range(5):
            symbol = f"X{pair_index}{member}"
            rows.append(
                {
                    "asset_id": symbol,
                    "pillar_primary": pillar,
                    "cluster_official": cluster,
                    "sector": cluster,
                }
            )
            history["by_symbol"][symbol] = {"status": "ok", "score_delta": 0.1}

    segment_monitor = {
        "internal_segments": [
            {"segment": "P1", "average_dscore_1d": 0.50, "n_valid": 5, "sample_state": "stable"},
            {"segment": "P2", "average_dscore_1d": 0.80, "n_valid": 5, "sample_state": "stable"},
            {"segment": "P3", "average_dscore_1d": 0.10, "n_valid": 5, "sample_state": "stable"},
            {"segment": "P4", "average_dscore_1d": -0.40, "n_valid": 5, "sample_state": "stable"},
        ],
        "official_segments": [
            {"segment": "C1", "average_dscore_1d": 0.45, "n_valid": 5, "sample_state": "stable"},
            {"segment": "C2", "average_dscore_1d": 0.20, "n_valid": 5, "sample_state": "stable"},
            {"segment": "C3", "average_dscore_1d": 0.40, "n_valid": 5, "sample_state": "stable"},
            {"segment": "C4", "average_dscore_1d": 0.30, "n_valid": 5, "sample_state": "stable"},
        ],
    }

    _, payload = build_reality_check(
        pd.DataFrame(rows),
        history_delta=history,
        segment_monitor=segment_monitor,
    )
    verdicts = {(row["intern"], row["offiziell"]): row["verdict"] for row in payload["comparisons"]}

    assert verdicts[("P1", "C1")] == "aligned"
    assert verdicts[("P2", "C2")] == "scanner_stronger"
    assert verdicts[("P3", "C3")] == "scanner_weaker"
    assert verdicts[("P4", "C4")] == "contra_market"
    assert payload["rules"] == {
        "alignment_tolerance_dscore": 0.10,
        "min_pair_valid": 5,
        "min_pair_coverage": 0.50,
    }
    assert payload["semantics"]["absolute_truth_claimed"] is False
    assert payload["semantics"]["mixed_super_metric_created"] is False


def test_reality_check_thin_sample_is_unclear() -> None:
    rows = [
        {
            "asset_id": f"A{i}",
            "pillar_primary": "P1",
            "cluster_official": "C1",
            "sector": "C1",
        }
        for i in range(2)
    ]
    history = {
        "by_symbol": {
            f"A{i}": {"status": "ok", "score_delta": 0.5}
            for i in range(2)
        }
    }
    segment_monitor = {
        "internal_segments": [
            {
                "segment": "P1",
                "average_dscore_1d": 0.8,
                "n_valid": 2,
                "sample_state": "thin",
            }
        ],
        "official_segments": [
            {
                "segment": "C1",
                "average_dscore_1d": 0.1,
                "n_valid": 2,
                "sample_state": "thin",
            }
        ],
    }

    _, payload = build_reality_check(
        pd.DataFrame(rows),
        history_delta=history,
        segment_monitor=segment_monitor,
    )
    assert payload["comparisons"][0]["verdict"] == "unclear"
