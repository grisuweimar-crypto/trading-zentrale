from scanner.research.decision_layer.depot_watch import build_depot_watch


AS_OF = "2026-10-01T18:00:00+02:00"
SNAPSHOT = "daily-snap-w1"


def test_ferrari_daily_research_context_reaches_phase_7h_unchanged():
    upstream_context = {
        "current": {
            "name": "Ferrari N.V.",
            "score": 26.34,
            "rank": 74,
            "rank_percentile": 0.49,
            "r_code": "R4",
            "rs3m": -0.08,
            "trend200": 0.04,
            "cycle": 0.0,
            "confidence": 71.0,
            "confidence_label": "HIGH",
            "close": 413.78,
            "currency": "USD",
            "sector": "Consumer Cyclical",
            "cluster": "Automobiles",
            "cluster_official": "Automobiles",
        },
        "dynamics": {
            "score_delta_1d": -1.25,
            "score_delta_5d": -4.5,
            "rank_delta_1d": 3.0,
        },
        "persistence": {
            "days_r4": 11,
            "consecutive_days_current_r_code": 7,
            "consecutive_days_rs3m_negative": 5,
            "consecutive_days_trend200_negative": 0,
        },
        # The sentinel proves 7H transports the upstream classification instead
        # of deriving a replacement classification from current scanner values.
        "classification": {
            "top_10_percent": False,
            "overextension_warning": False,
            "upstream_sentinel": "preserve-me",
        },
    }
    daily_research = {
        "schema_version": "daily_research_v1",
        "snapshot_id": SNAPSHOT,
        "as_of": AS_OF,
        "source_snapshot_id": SNAPSHOT,
        "universe_size": 1,
        "symbols": {"RACE": upstream_context},
    }
    position_book = {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "broker-snap-w1",
        "as_of": "2026-10-01T17:59:00+02:00",
        "positions": [
            {
                "schema_version": "decision_position_snapshot_v1",
                "symbol": "RACE",
                "as_of": "2026-10-01T17:58:00+02:00",
                "source_snapshot_id": "broker-position-w1",
                "position_state": "long",
                "quantity": 1,
            }
        ],
    }
    bundle_set = {
        "schema_version": "decision_chain_bundle_set_v1",
        "bundles": [],
    }

    result = build_depot_watch(daily_research, position_book, bundle_set)
    row = result["rows"][0]
    context = row["daily_scanner_context"]

    assert row["symbol"] == "RACE"
    assert row["availability"] == "decision_bundle_missing"
    assert context["current"] == upstream_context["current"]
    assert context["dynamics"] == upstream_context["dynamics"]
    assert context["persistence"] == upstream_context["persistence"]
    assert context["classification"] == upstream_context["classification"]
    assert context["classification"]["upstream_sentinel"] == "preserve-me"
    # Existing flattened 7H fields stay backwards-compatible.
    assert context["score"] == 26.34
    assert context["r_code"] == "R4"
