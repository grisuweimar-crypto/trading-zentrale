from datetime import datetime, timezone
import base64
import gzip
import json

from scripts.audit_cycle_client_vintage import capture_events, classify_window


def test_exact_partial_and_other_vintages():
    available = {("A", "2026-10-01"): {10., 11.},
                 ("A", "2026-10-02"): {12.},
                 ("B", "2026-10-03"): {13.}}
    assert classify_window([("2026-10-01", "10"),
                            ("2026-10-02", "12.0001"),
                            ("2026-10-03", "13")], "A", available) == {
        "exact_match": 1, "not_captured": 1, "different_only": 1,
        "other_vintage_also_seen": 1}


def test_capture_only_adjusted_positive_and_requested_symbols():
    rows = [{"symbol":"A","date":"2026-10-01","adj_close":"10","close":"12"},
            {"symbol":"B","date":"2026-10-01","adj_close":"5"},
            {"symbol":"A","date":"2026-10-02","adj_close":"nan"}]
    packed = base64.b64encode(gzip.compress(json.dumps(rows).encode())).decode()
    attempt = {"finished_at_utc":"2026-10-02T08:00:00+00:00",
               "requested_provider_symbols":["A"],"frame_rows_gzip_base64":packed,
               "exception_type":None}
    doc = {"generated_at":"2026-10-02T09:00:00+00:00",
           "witness":{"attempts":[attempt, dict(attempt, exception_type="Timeout")]}}
    assert list(capture_events(doc)) == [
        (datetime(2026,10,2,8,tzinfo=timezone.utc),"A","2026-10-01",10.)]
