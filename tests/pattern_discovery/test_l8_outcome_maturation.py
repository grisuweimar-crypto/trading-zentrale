from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path

import pytest

from scanner.research.pattern_discovery.outcome_maturation import (
    MaturedOutcomeRegistry,
    OutcomeMaturationError,
    build_outcome_maturation_check,
    load_outcome_maturation_contract,
    persist_outcome_maturation,
    verify_maturation_check,
    verify_matured_outcome,
)


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def digest(value):
    return sha256(canonical(value).encode("utf-8")).hexdigest()


def make_claim(
    *,
    claim_id="PCL-TEST",
    event_id="PEV-TEST",
    symbol="AAA",
    target_id="return_5t_gt_0",
    horizon=5,
    direction="POSITIVE",
    captured_at="2026-10-07T18:10:00Z",
    start_at="2026-10-08T08:00:00Z",
    snapshot_id="snap-l7",
    snapshot_binding_hash="1" * 64,
):
    claim = {
        "schema_version": "pattern_discovery_l7_prospective_claim_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "capture_state": "CAPTURED_UNMATURED",
        "claim_id": claim_id,
        "event_id": event_id,
        "captured_at": captured_at,
        "pattern": {
            "pattern_id": "PAT-TEST",
            "pattern_version": "v1",
            "pattern_spec_hash": "2" * 64,
            "frozen_record_hash": "3" * 64,
            "freeze_timestamp": "2026-10-06T15:05:00Z",
            "discovery_run_id": "DISC-TEST",
        },
        "governance": {
            "hypothesis_id": "H-PAT-TEST",
            "hypothesis_version": "v1",
            "hypothesis_version_hash": "4" * 64,
            "analysis_plan_id": "AP-PAT-TEST",
            "analysis_plan_version": "v1",
            "analysis_plan_hash": "5" * 64,
            "hypothesis_state": "FROZEN_FOR_CONFIRMATION",
            "analysis_plan_state": "FROZEN_FOR_CONFIRMATION",
            "qm_a_state_at_capture": "NOT_VALIDATED_BY_L5",
        },
        "match": {
            "symbol": symbol,
            "observation_as_of": "2026-10-07T12:00:00Z",
            "snapshot_id": snapshot_id,
            "snapshot_generated_at": "2026-10-07T18:00:00Z",
            "snapshot_binding_hash": snapshot_binding_hash,
            "history_binding_hash": "6" * 64,
            "session_map_hash": "7" * 64,
            "current_row_hash": "8" * 64,
            "condition_evidence": [],
            "condition_evidence_hash": digest([]),
        },
        "forecast": {
            "target_id": target_id,
            "expected_direction": direction,
            "horizon_sessions": horizon,
            "baseline": "same_horizon_unconditional_return_baseline",
            "reference_definition": "same_horizon_unconditional_return_baseline",
        },
        "start_market_session": {
            "session_id": f"{symbol}-2026-10-08",
            "calendar_id": f"{symbol}-CAL",
            "start_at": start_at,
            "source": "fixture-session-source-v1",
        },
        "outcome_available_at_capture": False,
        "outcome_maturation_performed": False,
        "confirmation_evaluation_performed": False,
        "rating_assigned": False,
        "promotion_performed": False,
    }
    claim["claim_hash"] = digest(claim)
    return claim


def session_map_hash(rows):
    projection = [
        {
            "symbol": row["symbol"],
            "session_id": row["session_id"],
            "calendar_id": row["calendar_id"],
            "start_at": row["start_at"],
            "source": row["source"],
        }
        for row in sorted(rows, key=lambda item: item["symbol"])
    ]
    return digest(projection)


def make_capture_report(claims, sessions):
    snapshot_file_sha = "a" * 64
    exact_session_map_hash = session_map_hash(sessions)
    bound_claims = []
    for original in claims:
        claim = deepcopy(original)
        claim["match"]["session_map_hash"] = exact_session_map_hash
        claim["claim_hash"] = digest(
            {key: value for key, value in claim.items() if key != "claim_hash"}
        )
        bound_claims.append(claim)
    claims = bound_claims
    report = {
        "schema_version": "pattern_discovery_l7_capture_report_v1",
        "module": "pattern_discovery_lab",
        "phase": "L7",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "capture_id": "PCAP-TEST",
        "captured_at": claims[0]["captured_at"] if claims else "2026-10-07T18:10:00Z",
        "l7_contract_hash": "b" * 64,
        "l5_snapshot_hashes": ["c" * 64],
        "snapshot_binding": {
            "snapshot_id": "snap-l7",
            "snapshot_as_of": "2026-10-07T12:00:00Z",
            "snapshot_generated_at": "2026-10-07T18:00:00Z",
            "snapshot_metadata_schema_version": "research_views_v1",
            "snapshot_source_file": "artifacts/watchlist/watchlist_full.csv",
            "snapshot_source_sha256": "d" * 64,
            "snapshot_file_sha256": snapshot_file_sha,
            "row_count": 4,
            "symbol_count": 4,
            "projected_rows_hash": "e" * 64,
            "snapshot_binding_hash": "1" * 64,
        },
        "history_binding": {
            "history_file_sha256": "f" * 64,
            "row_count": 100,
            "symbol_count": 4,
            "projected_rows_hash": "0" * 64,
            "max_generated_at": "2026-10-07T13:00:00Z",
            "history_binding_hash": "6" * 64,
        },
        "session_map_hash": exact_session_map_hash,
        "counts": {
            "frozen_pattern_count": 1,
            "post_freeze_pattern_count": 1,
            "current_symbol_count": 4,
            "prospective_claim_count": len(claims),
            "excluded_item_count": 0,
        },
        "pattern_summaries": [],
        "exclusions": [],
        "claims": claims,
        "boundaries": {
            "outcome_information_used": False,
            "outcome_maturation_performed": False,
            "confirmation_evaluation_performed": False,
            "sequential_monitoring_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
        },
    }
    report["capture_hash"] = digest(report)
    return report


def peer_snapshot():
    return [
        {"symbol": "AAA", "currency": "USD", "snapshot_id": "snap-l7"},
        {"symbol": "BBB", "currency": "USD", "snapshot_id": "snap-l7"},
        {"symbol": "CCC", "currency": "USD", "snapshot_id": "snap-l7"},
        {"symbol": "DDD", "currency": "EUR", "snapshot_id": "snap-l7"},
    ]


def session_bindings(claim=None, *, subject_session_date="2026-10-08"):
    subject = claim or make_claim()
    subject_symbol = subject["match"]["symbol"]
    subject_start = subject["start_market_session"]
    rows = []
    for symbol in ("AAA", "BBB", "CCC", "DDD"):
        if symbol == subject_symbol:
            rows.append({
                "symbol": symbol,
                "session_id": subject_start["session_id"],
                "calendar_id": subject_start["calendar_id"],
                "session_date": subject_session_date,
                "start_at": subject_start["start_at"],
                "source": subject_start["source"],
            })
        else:
            rows.append({
                "symbol": symbol,
                "session_id": f"{symbol}-2026-10-08",
                "calendar_id": f"{symbol}-CAL",
                "session_date": "2026-10-08",
                "start_at": "2026-10-08T08:00:00Z",
                "source": "fixture-session-source-v1",
            })
    return rows


def price_row(symbol, day, adj, currency="USD", close=None):
    return {
        "date": day,
        "symbol": symbol,
        "currency": currency,
        "open": str(close if close is not None else adj),
        "high": str((close if close is not None else adj) + 1.0),
        "low": str((close if close is not None else adj) - 1.0),
        "close": str(close if close is not None else adj),
        "adj_close": "" if adj is None else str(adj),
        "volume": "100",
        "source": "yahoo",
        "retrieved_at": "2026-10-20T20:00:00Z",
        "observation_type": "price_backfill",
    }


def complete_prices(count=21):
    dates = [
        f"2026-10-{day:02d}"
        for day in range(8, 8 + count)
    ]
    rows = []
    aaa = [100, 98, 102, 101, 108, 110] + [110 + i for i in range(max(0, count - 6))]
    bbb = [100 + i for i in range(count)]
    ccc = [100 + 0.6 * i for i in range(count)]
    ddd = [100 + 0.8 * i for i in range(count)]
    for symbol, values, currency in (
        ("AAA", aaa[:count], "USD"),
        ("BBB", bbb, "USD"),
        ("CCC", ccc, "USD"),
        ("DDD", ddd, "EUR"),
    ):
        for day, value in zip(dates, values):
            rows.append(price_row(symbol, day, value, currency=currency))
    return rows


def build_check(
    *,
    claim=None,
    claims=None,
    prices=None,
    peers=None,
    sessions=None,
    checked_at="2026-10-31T20:00:00Z",
    price_as_of="2026-10-31",
    peer_hash="a" * 64,
    session_hash="8" * 64,
    price_hash="9" * 64,
):
    if claims is None:
        claims = [claim or make_claim()]
    primary_claim = claims[0] if claims else make_claim()
    session_rows = (
        sessions if sessions is not None else session_bindings(primary_claim)
    )
    capture = make_capture_report(claims, session_rows)
    return build_outcome_maturation_check(
        capture,
        peers if peers is not None else peer_snapshot(),
        session_rows,
        prices if prices is not None else complete_prices(),
        checked_at=checked_at,
        price_as_of=price_as_of,
        peer_snapshot_file_sha256=peer_hash,
        start_session_binding_file_sha256=session_hash,
        price_file_sha256=price_hash,
    )


def test_l8_contract_keeps_maturation_separate_from_confirmation():
    contract = load_outcome_maturation_contract()
    assert contract["phase"] == "L8"
    assert contract["research_only"] is True
    assert contract["principles"]["adjusted_close_is_required_for_returns"] is True
    assert contract["principles"]["calendar_days_are_never_substituted_for_market_sessions"] is True
    assert contract["principles"]["l9_confirmation_is_not_performed_here"] is True
    assert contract["boundaries"]["confirmation_evaluation_performed"] is False


def test_full_5t_horizon_matures_directional_claim_with_exact_provenance():
    report = build_check()
    assert verify_maturation_check(report)["valid"] is True
    assert report["counts"]["matured_count"] == 1
    record = report["matured_outcomes"][0]
    assert verify_matured_outcome(record)["valid"] is True
    assert record["target"]["horizon_sessions"] == 5
    assert record["horizon_provenance"]["session_dates"] == [
        "2026-10-08",
        "2026-10-09",
        "2026-10-10",
        "2026-10-11",
        "2026-10-12",
        "2026-10-13",
    ]
    assert record["horizon_provenance"]["target_session_date"] == "2026-10-13"
    assert record["price_provenance"]["price_kind"] == "ADJUSTED_CLOSE"
    assert record["price_provenance"]["currency"] == "USD"
    assert record["price_provenance"]["currency_conversion_performed"] is False
    assert abs(record["outcome"]["return"] - 0.10) < 1e-12
    assert record["outcome"]["target_value_kind"] == "RETURN"
    assert abs(record["outcome"]["adverse_excursion"] - (-0.02)) < 1e-12
    assert record["outcome"]["path_max_drawdown"] <= -0.019999999


def test_relative_alpha_uses_leave_one_out_same_currency_peer_median():
    claim = make_claim(
        target_id="peer_excess_5t_gt_0",
        direction="POSITIVE",
    )
    report = build_check(claim=claim)
    record = report["matured_outcomes"][0]

    bbb_return = 105 / 100 - 1
    ccc_return = 103 / 100 - 1
    expected_peer = (bbb_return + ccc_return) / 2
    expected_excess = 0.10 - expected_peer

    assert record["target"]["pattern_type"] == "RELATIVE_ALPHA"
    assert record["reference"]["scope"] == "SAME_CURRENCY"
    assert record["reference"]["peer_count"] == 2
    assert abs(record["reference"]["peer_return"] - expected_peer) < 1e-12
    assert abs(record["outcome"]["peer_excess"] - expected_excess) < 1e-12
    assert record["outcome"]["target_value_kind"] == "PEER_EXCESS"
    assert abs(record["outcome"]["target_value"] - expected_excess) < 1e-12


def test_peer_baseline_excludes_subject_and_falls_back_global():
    claim = make_claim(
        target_id="peer_excess_5t_gt_0",
        direction="POSITIVE",
    )
    peers = [
        {"symbol": "AAA", "currency": "BRL", "snapshot_id": "snap-l7"},
        {"symbol": "BBB", "currency": "USD", "snapshot_id": "snap-l7"},
        {"symbol": "CCC", "currency": "USD", "snapshot_id": "snap-l7"},
        {"symbol": "DDD", "currency": "EUR", "snapshot_id": "snap-l7"},
    ]
    prices = complete_prices()
    for row in prices:
        if row["symbol"] == "AAA":
            row["currency"] = "BRL"
    report = build_check(claim=claim, peers=peers, prices=prices)
    record = report["matured_outcomes"][0]
    assert record["reference"]["scope"] == "GLOBAL_FALLBACK"
    assert record["reference"]["peer_count"] == 3
    assert record["reference"]["same_currency_peer_count"] == 0


def test_missing_exact_l7_start_session_remains_missing():
    prices = [
        row
        for row in complete_prices()
        if not (row["symbol"] == "AAA" and row["date"] == "2026-10-08")
    ]
    report = build_check(prices=prices)
    evaluation = report["evaluations"][0]
    assert evaluation["status"] == "MISSING_START_SESSION"
    assert report["matured_outcomes"] == []


def test_incomplete_horizon_is_not_matured_early():
    prices = [
        row
        for row in complete_prices()
        if row["date"] <= "2026-10-12"
    ]
    report = build_check(
        prices=prices,
        checked_at="2026-10-12T20:00:00Z",
        price_as_of="2026-10-12",
    )
    evaluation = report["evaluations"][0]
    assert evaluation["status"] == "IMMATURE_HORIZON"
    assert report["counts"]["matured_count"] == 0


def test_missing_intermediate_adjusted_price_does_not_collapse_session_count():
    prices = complete_prices()
    for row in prices:
        if row["symbol"] == "AAA" and row["date"] == "2026-10-10":
            row["adj_close"] = ""
    report = build_check(prices=prices)
    evaluation = report["evaluations"][0]
    assert evaluation["status"] == "MISSING_ADJUSTED_PRICE"
    assert report["matured_outcomes"] == []


def test_invalid_adjusted_close_preserves_raw_session_and_fails_closed():
    prices = complete_prices()
    for row in prices:
        if row["symbol"] == "AAA" and row["date"] == "2026-10-10":
            row["adj_close"] = "0"
    report = build_check(prices=prices)
    evaluation = report["evaluations"][0]
    assert evaluation["status"] == "MISSING_ADJUSTED_PRICE"
    assert report["matured_outcomes"] == []
    issues = report["price_binding"]["validation_issues"]["AAA"]
    assert issues["invalid_adj_close_preserved_as_missing"] == 1


def test_reverse_split_raw_close_jump_never_changes_adjusted_return():
    prices = complete_prices()
    for row in prices:
        if row["symbol"] == "AAA":
            if row["date"] == "2026-10-08":
                row["close"] = "2.54"
                row["open"] = "2.54"
                row["high"] = "2.60"
                row["low"] = "2.50"
            else:
                raw = float(row["adj_close"]) * 13.0
                row["close"] = str(raw)
                row["open"] = str(raw)
                row["high"] = str(raw + 1)
                row["low"] = str(raw - 1)
    report = build_check(prices=prices)
    record = report["matured_outcomes"][0]
    assert abs(record["outcome"]["return"] - 0.10) < 1e-12


def test_price_rows_after_price_as_of_cannot_mature_claim():
    prices = complete_prices()
    report = build_check(
        prices=prices,
        checked_at="2026-10-12T20:00:00Z",
        price_as_of="2026-10-12",
    )
    assert report["evaluations"][0]["status"] == "IMMATURE_HORIZON"
    assert report["matured_outcomes"] == []


def test_price_as_of_after_check_time_fails_closed():
    with pytest.raises(
        OutcomeMaturationError,
        match="price_as_of_after_checked_at",
    ):
        build_check(
            checked_at="2026-10-12T20:00:00Z",
            price_as_of="2026-10-13",
        )


def test_peer_snapshot_must_be_exact_l7_snapshot_file():
    with pytest.raises(
        OutcomeMaturationError,
        match="peer_snapshot_file_hash_mismatch_l7",
    ):
        build_check(peer_hash="f" * 64)


def test_peer_snapshot_rows_must_share_l7_snapshot_id():
    peers = peer_snapshot()
    peers[1]["snapshot_id"] = "different-snapshot"
    with pytest.raises(
        OutcomeMaturationError,
        match="peer_snapshot_id_mismatch",
    ):
        build_check(peers=peers)


def test_subject_start_session_binding_must_match_l7_claim_exactly():
    claim = make_claim()
    sessions = session_bindings(claim)
    sessions[0]["calendar_id"] = "WRONG-CALENDAR"
    with pytest.raises(
        OutcomeMaturationError,
        match="subject_start_session_binding_mismatch:AAA:calendar_id",
    ):
        build_check(claim=claim, sessions=sessions)


def test_explicit_session_date_can_differ_from_utc_start_date():
    claim = make_claim(
        captured_at="2026-10-07T18:10:00Z",
        start_at="2026-10-07T23:00:00Z",
    )
    sessions = session_bindings(
        claim,
        subject_session_date="2026-10-08",
    )
    report = build_check(claim=claim, sessions=sessions)
    assert report["counts"]["matured_count"] == 1
    record = report["matured_outcomes"][0]
    assert record["horizon_provenance"]["start_at"] == "2026-10-07T23:00:00Z"
    assert record["horizon_provenance"]["start_session_date"] == "2026-10-08"
    assert record["horizon_provenance"]["explicit_session_date_source"] == "START_SESSION_BINDING"


def test_peer_without_explicit_start_session_is_excluded_not_inferred():
    claim = make_claim(
        target_id="peer_excess_5t_gt_0",
        direction="POSITIVE",
    )
    sessions = [
        row for row in session_bindings(claim)
        if row["symbol"] != "BBB"
    ]
    report = build_check(claim=claim, sessions=sessions)
    record = report["matured_outcomes"][0]
    assert record["reference"]["excluded_peer_status_counts"][
        "START_SESSION_BINDING_UNAVAILABLE"
    ] == 1
    assert record["reference"]["peer_count"] == 1


def test_relative_alpha_does_not_mature_without_reference():
    claim = make_claim(
        target_id="peer_excess_5t_gt_0",
        direction="POSITIVE",
    )
    prices = [
        row
        for row in complete_prices()
        if row["symbol"] == "AAA"
    ]
    report = build_check(claim=claim, prices=prices)
    evaluation = report["evaluations"][0]
    assert evaluation["status"] == "REFERENCE_UNAVAILABLE"
    assert report["matured_outcomes"] == []


def test_directional_claim_can_mature_when_peer_reference_is_unavailable():
    prices = [
        row
        for row in complete_prices()
        if row["symbol"] == "AAA"
    ]
    report = build_check(prices=prices)
    assert report["evaluations"][0]["status"] == "MATURED"
    record = report["matured_outcomes"][0]
    assert record["reference"]["status"] == "UNAVAILABLE"
    assert record["outcome"]["peer_excess"] is None


def test_5t_and_20t_are_distinct_horizons_and_target_sessions():
    claim5 = make_claim(
        claim_id="PCL-5T",
        event_id="PEV-5T",
        target_id="return_5t_gt_0",
        horizon=5,
    )
    claim20 = make_claim(
        claim_id="PCL-20T",
        event_id="PEV-20T",
        target_id="return_20t_gt_0",
        horizon=20,
    )
    report = build_check(claims=[claim5, claim20])
    assert report["counts"]["matured_count"] == 2
    records = {
        row["target"]["horizon_sessions"]: row
        for row in report["matured_outcomes"]
    }
    assert records[5]["horizon_provenance"]["target_session_date"] == "2026-10-13"
    assert records[20]["horizon_provenance"]["target_session_date"] == "2026-10-28"
    assert records[5]["horizon_provenance"]["session_dates"] != records[20]["horizon_provenance"]["session_dates"]


def test_negative_direction_adverse_excursion_is_aligned_against_expected_move():
    claim = make_claim(
        target_id="return_5t_lt_0",
        direction="NEGATIVE",
    )
    report = build_check(claim=claim)
    record = report["matured_outcomes"][0]
    # AAA rises 10%; that is adverse to a predeclared negative direction.
    assert record["outcome"]["adverse_excursion"] <= -0.099999999


def test_build_does_not_mutate_l7_capture_or_claim():
    claim = make_claim()
    sessions = session_bindings(claim)
    capture = make_capture_report([claim], sessions)
    before = deepcopy(capture)
    build_outcome_maturation_check(
        capture,
        peer_snapshot(),
        sessions,
        complete_prices(),
        checked_at="2026-10-31T20:00:00Z",
        price_as_of="2026-10-31",
        peer_snapshot_file_sha256="a" * 64,
        start_session_binding_file_sha256="8" * 64,
        price_file_sha256="9" * 64,
    )
    assert capture == before


def test_same_inputs_are_deterministic():
    first = build_check()
    second = build_check()
    assert first == second
    assert first["check_id"] == second["check_id"]
    assert first["check_hash"] == second["check_hash"]
    assert first["matured_outcomes"][0]["outcome_hash"] == second["matured_outcomes"][0]["outcome_hash"]


def test_matured_outcome_registry_is_append_only_idempotent_and_claim_unique(tmp_path):
    report = build_check()
    first = persist_outcome_maturation(
        tmp_path,
        report,
        actor_id="tester",
        actor_role="researcher",
    )
    assert first["registry"]["appended_outcome_count"] == 1

    second = persist_outcome_maturation(
        tmp_path,
        report,
        actor_id="tester",
        actor_role="researcher",
    )
    assert second["registry"]["appended_outcome_count"] == 0
    assert second["registry"]["idempotent_outcome_count"] == 1

    registry = MaturedOutcomeRegistry(first["registry_path"])
    stored = registry.get_outcome("PCL-TEST")
    assert stored == report["matured_outcomes"][0]

    changed = deepcopy(report["matured_outcomes"][0])
    changed["outcome"]["return"] += 0.001
    changed["outcome_hash"] = digest(
        {k: v for k, v in changed.items() if k != "outcome_hash"}
    )
    with pytest.raises(
        OutcomeMaturationError,
        match="matured_outcome_claim_collision",
    ):
        registry.register_outcomes(
            [changed],
            recorded_at="2026-11-01T20:00:00Z",
            actor_id="tester",
            actor_role="researcher",
        )


def test_maturation_registry_hash_chain_detects_tampering(tmp_path):
    report = build_check()
    status = persist_outcome_maturation(
        tmp_path,
        report,
        actor_id="tester",
        actor_role="researcher",
    )
    path = Path(status["registry_path"])
    lines = path.read_text(encoding="utf-8").splitlines()
    event = json.loads(lines[0])
    event["record"]["outcome"]["return"] = 99.0
    lines[0] = canonical(event)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    registry = MaturedOutcomeRegistry(path)
    with pytest.raises(
        OutcomeMaturationError,
        match="maturation_registry_entry_hash_invalid",
    ):
        registry.verify_integrity()


def test_check_report_and_outcome_are_hash_protected():
    report = build_check()
    tampered = deepcopy(report)
    tampered["counts"]["matured_count"] = 0
    with pytest.raises(
        OutcomeMaturationError,
        match="maturation_check_hash_mismatch",
    ):
        verify_maturation_check(tampered)

    record = deepcopy(report["matured_outcomes"][0])
    record["outcome"]["return"] = 99.0
    with pytest.raises(
        OutcomeMaturationError,
        match="matured_outcome_hash_mismatch",
    ):
        verify_matured_outcome(record)


def test_l8_creates_no_confirmation_rating_promotion_or_productive_authority():
    report = build_check()
    record = report["matured_outcomes"][0]
    serialized = json.dumps(record, sort_keys=True)
    for forbidden in (
        '"universal_stance"',
        '"portfolio_action"',
        '"trade_decision"',
        '"order_instruction"',
        '"buy_signal"',
        '"sell_signal"',
        '"position_size"',
        '"target_weight"',
        '"direction_probability"',
        '"probability_advantage_lift"',
    ):
        assert forbidden not in serialized
    assert record["boundaries"]["direction_hit_computed"] is False
    assert record["boundaries"]["probability_computed"] is False
    assert record["boundaries"]["confirmation_evaluation_performed"] is False
    assert record["boundaries"]["rating_assigned"] is False
    assert record["boundaries"]["promotion_performed"] is False
