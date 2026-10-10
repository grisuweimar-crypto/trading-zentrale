"""CY-04: released descriptive lag direction, missingness and UI attachment."""
from datetime import date, timedelta
from pathlib import Path

from scanner.ui.cycle_direction import (
    attach_verified_directions, load_direction_for_ui, project_eligible_directions,
)


def row(day, cycle, asset="AAA", suffix=None):
    d = date.fromisoformat(day)
    return {
        "snapshot_id": suffix or day,
        "asset_id": asset, "run_id": "run-" + day,
        "as_of": day, "generated_at": day + "T18:00:00+00:00",
        "cycle": str(cycle), "quality": "VALID",
        "availability": "PROVISIONAL_REPLAYED",
        "currency": "USD", "listing_symbol": asset, "price_symbol": asset,
        "source": "YAHOO_PIT_CYCLE_V1",
        "formula": "cycle_detrended_sma20_range40_v1",
        "price_source": "yfinance.download",
        "price_basis": "1d_auto_adjust_true_close",
        "currency_lineage": "WATCHLIST_DECLARED_ONLY",
        "session_time_quality": "SESSION_DATE_CUTOFF_ONLY",
        "price_sha256": "a" * 64,
        "last_bar": (d - timedelta(days=1)).isoformat(),
    }


def eligible_mask(r, *, research_status="ELIGIBLE", lags=(1, 5, 10)):
    return {
        "snapshot_id": r["snapshot_id"], "asset_id": r["asset_id"],
        "as_of": r["as_of"], "research_status": research_status,
        **{f"lag_{i}obs": "PROVISIONAL_CHAIN" if i in lags else "NO_PRIOR"
           for i in (1, 5, 10)},
    }


def test_real_zero_unchanged_up_down_and_1_5_10_observations():
    start = date(2026, 10, 1)
    values = [20, 22, 30, 30, 35, 40, 40, 35, 60, 75, 0]
    rows = [row((start + timedelta(days=i)).isoformat(), v)
            for i, v in enumerate(values)]
    end = rows[-1]
    result = project_eligible_directions(
        rows, [eligible_mask(end)], latest_snapshot_id=end["snapshot_id"]
    )
    actual = result["AAA"]
    assert actual["cycle"] == 0
    assert actual["delta_1obs"] == -75
    assert actual["direction_1obs"] == "DOWN"
    assert actual["delta_5obs"] == -40
    assert actual["direction_5obs"] == "DOWN"
    assert actual["delta_10obs"] == -20
    assert actual["direction_10obs"] == "DOWN"
    # Exactly unchanged must be descriptive UNCHANGED, not a missing zero.
    same = row(end["as_of"], 75)
    same["snapshot_id"] = "2026-10-11-extra"
    prior = row("2026-10-10", 75)
    project = project_eligible_directions(
        [prior, same], [eligible_mask(same, lags=(1,))],
        latest_snapshot_id=same["snapshot_id"],
    )
    assert project["AAA"]["delta_1obs"] == 0
    assert project["AAA"]["direction_1obs"] == "UNCHANGED"
    up = row("2026-10-12", 100)
    result = project_eligible_directions(
        [end, up], [eligible_mask(up, lags=(1,))],
        latest_snapshot_id=up["snapshot_id"],
    )
    assert result["AAA"]["direction_1obs"] == "UP"
    assert result["AAA"]["delta_1obs"] == 100


def test_missing_lag_blocked_status_and_changed_listing_never_make_arrows():
    old = row("2026-10-10", 23)
    new = row("2026-10-11", 26)
    for status in ("BLOCKED_EXTERNAL_VERIFICATION_269", "UNVERIFIED", "REVIEW"):
        assert project_eligible_directions(
            [old, new], [eligible_mask(new, research_status=status, lags=(1,))],
            latest_snapshot_id=new["snapshot_id"],
        ) == {}
    missing = project_eligible_directions(
        [new], [eligible_mask(new, lags=())],
        latest_snapshot_id=new["snapshot_id"],
    )
    assert missing == {}
    changed = dict(new, price_symbol="AAA.NEW")
    # Even if a malformed mask says the chain is valid, identity changes block it.
    assert project_eligible_directions(
        [old, changed], [eligible_mask(changed, lags=(1,))],
        latest_snapshot_id=changed["snapshot_id"],
    ) == {}
    stale = dict(new, quality="STALE", availability="EXCLUDED_STALE", cycle="")
    assert project_eligible_directions(
        [old, stale], [eligible_mask(stale, lags=(1,))],
        latest_snapshot_id=stale["snapshot_id"],
    ) == {}


def test_only_latest_snapshot_and_never_same_day_superseded():
    first = row("2026-10-10", 20)
    same_day = row("2026-10-10", 40, suffix="2026-10-10-new")
    same_day["generated_at"] = "2026-10-10T20:00:00+00:00"
    latest = row("2026-10-11", 80)
    result = project_eligible_directions(
        [first, same_day, latest], [eligible_mask(latest, lags=(1,))],
        latest_snapshot_id=latest["snapshot_id"],
    )
    assert result["AAA"]["delta_1obs"] == 40
    assert project_eligible_directions(
        [first, same_day, latest], [eligible_mask(first, lags=(1,))],
        latest_snapshot_id=first["snapshot_id"],
    ) == {}


def test_ui_attachment_rejects_legacy_alias_currency_and_bar_hash():
    old = row("2026-10-10", 20)
    now = row("2026-10-11", 28)
    item = project_eligible_directions(
        [old, now], [eligible_mask(now, lags=(1,))],
        latest_snapshot_id=now["snapshot_id"],
    )["AAA"]
    projection = {"state": "RELEASED", "latest_snapshot_id": now["snapshot_id"],
                  "by_asset": {"AAA": item}}
    canonical = {
        "asset_id": "AAA", "cycle": 28, "cycle_quality": "VALID",
        "cycle_formula_version": now["formula"], "cycle_currency": "USD",
        "cycle_price_symbol": "AAA", "cycle_price_sha256": "a" * 64,
    }
    ok = attach_verified_directions([dict(canonical)], projection)[0]
    assert ok["cy04_delta_1obs"] == 8
    assert ok["cy04_delta_5obs"] is None
    assert ok["cy04_quality"] == "ELIGIBLE_DESCRIPTIVE_ONLY"
    variants = [
        {"asset_id": "AAA-ADR"}, {"cycle_quality": "STALE"},
        {"cycle_formula_version": "legacy"}, {"cycle_currency": "EUR"},
        {"cycle_price_symbol": "AAA.ALT"}, {"cycle_price_sha256": "b" * 64},
        {"cycle": 0}, {"cycle": None},
    ]
    for variation in variants:
        bad = attach_verified_directions([dict(canonical, **variation)], projection)[0]
        assert bad["cy04_delta_1obs"] is None
        assert bad["cy04_quality"] == "UNAVAILABLE"
    quarantined = attach_verified_directions(
        [dict(canonical)], {**projection, "state": "UNAVAILABLE"}
    )[0]
    assert quarantined["cy04_delta_1obs"] is None
    assert quarantined["cy04_quality"] == "UNAVAILABLE"


def test_current_cy03_v1_fails_closed_in_absence_of_archive(tmp_path: Path):
    payload = load_direction_for_ui(tmp_path)
    assert payload["state"] == "UNAVAILABLE"
    assert payload["by_asset"] == {}


def test_dashboard_markup_is_descriptive_and_no_other_score_data_changed():
    from scanner.ui.generator import _render_html
    html = _render_html(
        data_records=[{"asset_id": "AAA", "cycle": 50, "cy04_delta_5obs": None}],
        presets={}, source_csv="fixture.csv", version="test", build="test",
        briefing_text="", briefing_source="", history_delta={},
        segment_monitor={}, reality_check={}, macro_chain_signal={},
        briefing_realities_text="", briefing_realities_source="",
        run_at="", run_src="", run_universe="", fallback_tbody_html="",
    )
    assert "Zyklus / Δ5" in html
    assert "cycleDeltaLabel(r, 10)" in html
    assert "cy04_quality !== 'ELIGIBLE_DESCRIPTIVE_ONLY'" in html
    assert "Scannerbeobachtungen ≠ Handelstage" in html
    assert "<th data-k=\"score\"" in html
    assert "<th data-k=\"dscore_1d\"" in html
    assert "<th data-k=\"cycle\" class=\"right\"" in html
    assert "— (keine zulässige Vergleichsbasis)" in html
    assert "NO_DIRECTIONAL_CHANGE" in html  # only in explanatory JS comment
