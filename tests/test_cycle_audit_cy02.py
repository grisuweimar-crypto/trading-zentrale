"""Production-side CY-02 gate must fail closed, without market API access."""
from pathlib import Path

import pandas as pd
from scripts.audit_cycle_current import audit_rows, audit_csv, FIELDS


def valid_row():
    return dict.fromkeys(FIELDS, "") | {
        "asset_id": "AAA", "cycle": "0", "cycle_quality": "VALID",
        "cycle_source": "YAHOO_PIT_CYCLE_V1",
        "cycle_formula_version": "cycle_detrended_sma20_range40_v1",
        "cycle_currency": "USD",
        "cycle_currency_lineage": "WATCHLIST_DECLARED_ONLY",
        "cycle_session_time_quality": "SESSION_DATE_CUTOFF_ONLY",
        "cycle_last_bar": "2026-10-09",
        "cycle_as_of": "2026-10-10T06:30:00+00:00",
        "cycle_price_sha256": "a" * 64, "cycle_price_symbol": "AAA",
        "cycle_quality_reason": "COMPLETED_DAILY_BARS",
    }


def test_valid_and_missing_cycle_accepted():
    live = valid_row()
    missing = dict(live, asset_id="BBB", cycle="", cycle_quality="MISSING_SOURCE",
                   cycle_price_sha256="", cycle_quality_reason="QUOTE_CURRENCY_UNKNOWN")
    errors, info = audit_rows([live, missing], columns=list(live))
    assert not errors, errors
    assert info["quality_counts"] == {"MISSING_SOURCE": 1, "VALID": 1}


def test_fails_on_invalid_missing_and_invented_cycle_zero():
    good = valid_row()
    broken = (
        dict(good, cycle="", cycle_quality="VALID"),
        dict(good, cycle="0", cycle_quality="STALE"),
        dict(good, cycle="103"),
        dict(good, cycle_price_sha256=""),
        dict(good, cycle_last_bar="2026-10-10"),
        dict(good, cycle_currency=""),
        dict(good, cycle_session_time_quality="PROVIDER_VERIFIED"),
    )
    for row in broken:
        errors, _ = audit_rows([row], columns=list(row))
        assert errors, row


def test_fails_when_cycle_column_missing_or_source_rows_duplicate():
    good = valid_row()
    assert audit_rows([good], columns=[x for x in good if x != "cycle"])[0]
    assert audit_rows([good, good], columns=list(good))[0]


def test_audit_file_checks_run_report_coherence(tmp_path):
    rows = [valid_row(), dict(valid_row(), asset_id="BBB", cycle="", cycle_quality="STALE",
                             cycle_price_sha256="")]
    source = tmp_path / "watchlist_full.csv"
    report = tmp_path / "cycle_quality.csv"
    pd.DataFrame(rows).to_csv(source, index=False)
    pd.DataFrame(rows).to_csv(report, index=False)
    errors, stats = audit_csv(source, report_path=report)
    assert not errors, errors
    assert stats["asset_count"] == 2

    altered = pd.DataFrame(rows)
    altered.loc[1, "cycle_quality"] = "VALID"
    altered.to_csv(report, index=False)
    errors, _ = audit_csv(source, report_path=report)
    assert any("cycle_quality_report_mismatch" in err for err in errors)
