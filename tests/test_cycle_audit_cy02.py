"""Production-side CY-02 gate must fail closed, without market API access."""
from pathlib import Path
from datetime import datetime, timezone
import csv
import gzip
import io
import math

from scanner.data.enrich.cycle_oscillator import calculate_cycle

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


def test_actual_60_price_bars_can_be_replayed_and_tampering_is_detected(tmp_path):
    dates = pd.date_range(end="2026-10-09", periods=90, freq="B")
    closes = pd.Series([100 + 0.1 * i + 3 * math.sin(i * 0.27) for i in range(90)], index=dates)
    calc = calculate_cycle(closes, symbol="AAA", currency="USD", is_crypto=False,
                           as_of=datetime(2026, 10, 10, 6, 30, tzinfo=timezone.utc))
    assert calc["cycle_quality"] == "VALID"
    assert len(calc["_cycle_input_bars"]) == 60
    row = valid_row()
    row.update({
        "cycle": str(calc["Zyklus %"]),
        "cycle_price_sha256": calc["cycle_price_sha256"],
        "cycle_last_bar": calc["cycle_last_bar"],
        "cycle_as_of": calc["cycle_as_of"],
    })
    source = tmp_path / "watchlist_full.csv"
    report = tmp_path / "cycle_quality.csv"
    bars_file = tmp_path / "cycle_input_bars.csv.gz"
    pd.DataFrame([row]).to_csv(source, index=False)
    pd.DataFrame([row]).to_csv(report, index=False)

    bar_rows = [
        {"symbol": "AAA", "currency": "USD",
         "formula_version": calc["cycle_formula_version"],
         "price_sha256": calc["cycle_price_sha256"], "as_of": calc["cycle_as_of"],
         "session_date": date, "close": price}
        for date, price in calc["_cycle_input_bars"]
    ]

    def write_bars(observations):
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=list(bar_rows[0]),
                                lineterminator="\n")
        writer.writeheader()
        writer.writerows(observations)
        bars_file.write_bytes(gzip.compress(stream.getvalue().encode("utf-8"), mtime=0))

    write_bars(bar_rows)
    errors, _ = audit_csv(source, report_path=report, bars_path=bars_file)
    assert errors == []
    modified = [dict(r) for r in bar_rows]
    modified[40]["close"] = "999.999"
    write_bars(modified)
    errors, _ = audit_csv(source, report_path=report, bars_path=bars_file)
    assert any("bar_window_fingerprint_or_date_mismatch" in error for error in errors)
