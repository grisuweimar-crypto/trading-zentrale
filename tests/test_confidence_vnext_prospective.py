from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scanner.reports.confidence_vnext_prospective import (
    CLAIM_COLUMNS,
    OUTCOME_COLUMNS,
    append_claims,
    append_outcomes,
    build_claim_rows,
    compute_mature_outcomes,
    derive_peer_labels,
    validation_summary,
)


def _metadata():
    return {
        "snapshot_id": "snap-1",
        "as_of": "2026-09-23",
        "generated_at": "2026-09-23T16:05:34+00:00",
        "latest_run_complete": True,
        "daily_run": {"run_id": "run-1", "scanner_status": "success"},
    }


def _latest():
    return pd.DataFrame(
        [
            {
                "date": "2026-09-23",
                "as_of": "2026-09-23",
                "symbol": "AAA",
                "name": "AAA Corp",
                "score": 80.0,
                "currency": "USD",
                "observation_type": "observed_scanner",
            },
            {
                "date": "2026-09-23",
                "as_of": "2026-09-23",
                "symbol": "BBB",
                "name": "BBB Corp",
                "score": 20.0,
                "currency": "USD",
                "observation_type": "observed_scanner",
            },
            {
                "date": "2026-09-23",
                "as_of": "2026-09-23",
                "symbol": "BTC-USD",
                "name": "Bitcoin",
                "score": 50.0,
                "currency": "USD",
                "sector": "Cryptocurrency",
                "observation_type": "observed_scanner",
            },
        ]
    )


def _phase4_report():
    def row(symbol, selection_direction, timing_direction, agreement_state):
        return {
            "as_of": "2026-09-23",
            "symbol": symbol,
            "name": symbol,
            "horizon_sessions": 5,
            "selection_band": "B5" if symbol == "AAA" else "B1",
            "selection": {"state": "robust", "direction": selection_direction},
            "timing": {
                "state": "robust_claim",
                "direction": timing_direction,
                "matched_patterns": ["pattern_a"],
            },
            "risk": {"state": "low"},
            "data_quality": {
                "selection": {"state": "proxy_complete"},
                "timing": {"state": "proxy_complete"},
                "risk": {"state": "proxy_complete"},
            },
            "model_agreement": {"state": agreement_state, "conflicts": []},
        }

    return {
        "phase": "4_confidence_vnext_empirical_research",
        "config": {"evidence_version": "phase4_confidence_research_v1"},
        "current": {
            "as_of": "2026-09-23",
            "asset_scope": "stocks_only",
            "rows": [
                row("AAA", "positive", "positive", "compatible"),
                row("BBB", "negative", "negative", "compatible"),
            ],
            "pit_source_checks": {
                "phase2": {
                    "source_as_of": "2026-09-23T00:00:00+00:00",
                    "not_after_current_scan": True,
                },
                "phase3": {
                    "source_as_of": "2026-09-23T00:00:00+00:00",
                    "not_after_current_scan": True,
                },
            },
            "risk_metric_applicability": {
                "volatility": {"status": "scale_incompatible"},
            },
        },
    }


def _fingerprints():
    return {
        "evidence_fingerprint": "e" * 64,
        "phase4_report_sha256": "a" * 64,
        "phase2_sha256": "b" * 64,
        "phase3_sha256": "c" * 64,
        "risk_scale_sha256": "missing",
    }


def _prices():
    dates = pd.date_range("2026-09-23", periods=6, freq="D")
    aaa = [100.0, 102.0, 101.0, 104.0, 105.0, 110.0]
    bbb = [100.0, 99.0, 98.0, 97.0, 96.0, 95.0]
    rows = []
    for symbol, values in (("AAA", aaa), ("BBB", bbb)):
        for day, value in zip(dates, values):
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "symbol": symbol,
                    "open": value,
                    "high": value,
                    "low": value,
                    "close": value,
                    "adj_close": value,
                    "volume": 1,
                    "observation_type": "price_backfill",
                }
            )
    return pd.DataFrame(rows)


def _claims(claim_prices: pd.DataFrame | None = None):
    return build_claim_rows(
        _phase4_report(),
        _latest(),
        _metadata(),
        _fingerprints(),
        _prices() if claim_prices is None else claim_prices,
    )


def test_claim_snapshot_is_stock_only_compact_and_score_free():
    claims = _claims()
    assert list(claims.columns) == list(CLAIM_COLUMNS)
    assert set(claims["symbol"]) == {"AAA", "BBB"}
    assert "score" not in claims.columns
    assert set(claims["return_claim_direction"]) == {"positive", "negative"}
    assert set(claims["volatility_application_status"]) == {"scale_incompatible"}
    assert set(claims["outcome_eligibility"]) == {"eligible"}
    assert set(claims["outcome_unavailable_reason"]) == {""}
    assert claims["claim_id"].nunique() == 2
    assert set(claims["start_market_date"]) == {"2026-09-23"}


def test_claim_append_is_idempotent_and_rejects_reinterpretation():
    claims = _claims()
    first = append_claims(pd.DataFrame(columns=CLAIM_COLUMNS), claims)
    second = append_claims(first.astype(str), claims)
    assert len(second) == len(first)

    changed = claims.copy()
    changed.loc[changed["symbol"].eq("AAA"), "agreement_state"] = "single_model"
    changed.loc[changed["symbol"].eq("AAA"), "claim_id"] = "different"
    with pytest.raises(ValueError, match="immutable shadow claim conflict"):
        append_claims(first.astype(str), changed)


def test_claim_start_session_is_frozen_before_later_same_day_close_exists():
    claim_prices = pd.DataFrame(
        [
            {"date": "2026-09-22", "symbol": "AAA", "open": 90, "high": 90, "low": 90, "close": 90, "adj_close": 90, "volume": 1, "observation_type": "price_backfill"},
            {"date": "2026-09-22", "symbol": "BBB", "open": 110, "high": 110, "low": 110, "close": 110, "adj_close": 110, "volume": 1, "observation_type": "price_backfill"},
        ]
    )
    claims = _claims(claim_prices)
    aaa_claim = claims.loc[claims["symbol"].eq("AAA")].iloc[0]
    assert aaa_claim["start_market_date"] == "2026-09-22"
    assert float(aaa_claim["start_adjusted_close"]) == pytest.approx(90.0)

    evaluation = claim_prices.copy()
    evaluation.loc[evaluation["symbol"].eq("AAA"), ["open", "high", "low", "close", "adj_close"]] = 95
    future = pd.concat([evaluation, _prices()], ignore_index=True)
    outcomes = compute_mature_outcomes(
        claims,
        future,
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-28T00:00:00+00:00",
    )
    aaa = outcomes.loc[outcomes["symbol"].eq("AAA")].iloc[0]
    # The immutable session remains 2026-09-22; the raw claim audit price remains 90.
    # Outcome prices may be rebased together later, so the evaluation-basis start is 95.
    assert aaa["start_market_date"] == "2026-09-22"
    assert float(aaa_claim["start_adjusted_close"]) == pytest.approx(90.0)
    assert aaa["start_adjusted_close"] == pytest.approx(95.0)
    assert aaa["end_market_date"] == "2026-09-27"
    assert aaa["end_adjusted_close"] == pytest.approx(105.0)
    assert aaa["return"] == pytest.approx(105.0 / 95.0 - 1.0)


def test_corporate_action_rebases_entire_outcome_path_on_one_adjustment_basis():
    claims = _claims()
    aaa_claim = claims.loc[claims["symbol"].eq("AAA")].iloc[0]
    assert float(aaa_claim["start_adjusted_close"]) == pytest.approx(100.0)

    evaluation = _prices().copy()
    # Simulate a later provider adjustment after a split/dividend: the historical
    # start and every path point are now expressed on a new common basis.
    mask = evaluation["symbol"].eq("AAA")
    evaluation.loc[mask, "adj_close"] = [50.0, 51.0, 50.5, 52.0, 52.5, 55.0]

    outcomes = compute_mature_outcomes(
        claims,
        evaluation,
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-29T00:00:00+00:00",
    )
    aaa = outcomes.loc[outcomes["symbol"].eq("AAA")].iloc[0]
    assert aaa["start_market_date"] == "2026-09-23"
    assert aaa["start_adjusted_close"] == pytest.approx(50.0)
    assert aaa["end_adjusted_close"] == pytest.approx(55.0)
    assert aaa["return"] == pytest.approx(0.10)
    assert aaa["adverse_excursion"] == pytest.approx(0.0)
    assert aaa["path_max_drawdown"] == pytest.approx(0.5 / 51.0)


def test_missing_claim_time_price_is_permanently_unevaluable_not_backfilled():
    claim_prices = _prices().loc[lambda x: x["symbol"].eq("AAA")].copy()
    claims = _claims(claim_prices)
    bbb = claims.loc[claims["symbol"].eq("BBB")].iloc[0]
    assert bbb["outcome_eligibility"] == "unevaluable"
    assert bbb["outcome_unavailable_reason"] == "claim_time_price_history_missing"
    assert bbb["start_market_date"] == ""
    assert pd.isna(bbb["start_adjusted_close"])

    outcomes = compute_mature_outcomes(
        claims,
        _prices(),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-29T00:00:00+00:00",
    )
    assert set(outcomes["symbol"]) == {"AAA"}

    later_reinterpreted = _claims(_prices())
    with pytest.raises(ValueError, match="immutable shadow claim conflict"):
        append_claims(claims.astype(str), later_reinterpreted)

    report = validation_summary(claims, outcomes)
    assert report["horizons"]["5"]["outcome_eligible_claims"] == 1
    assert report["horizons"]["5"]["outcome_unevaluable_claims"] == 1
    assert report["semantics"]["missing_claim_time_start_remains_permanently_unevaluable"] is True


def test_raw_outcomes_mature_only_when_full_horizon_exists():
    claims = _claims()
    immature_prices = _prices().loc[lambda x: x["date"] < "2026-09-28"].copy()
    none = compute_mature_outcomes(
        claims,
        immature_prices,
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-27T00:00:00+00:00",
    )
    assert none.empty

    outcomes = compute_mature_outcomes(
        claims,
        _prices(),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-29T00:00:00+00:00",
    )
    assert list(outcomes.columns) == list(OUTCOME_COLUMNS)
    assert len(outcomes) == 2
    assert "peer_excess" not in outcomes.columns
    aaa = outcomes.loc[outcomes["symbol"].eq("AAA")].iloc[0]
    bbb = outcomes.loc[outcomes["symbol"].eq("BBB")].iloc[0]
    assert aaa["return"] == pytest.approx(0.10)
    assert bbb["return"] == pytest.approx(-0.05)
    assert bbb["adverse_excursion"] == pytest.approx(0.05)
    assert bbb["path_max_drawdown"] == pytest.approx(0.05)


def test_target_session_must_be_completed_before_evaluation_time():
    claims = _claims()
    same_day = compute_mature_outcomes(
        claims,
        _prices(),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-28T23:59:59+00:00",
    )
    assert same_day.empty

    next_day = compute_mature_outcomes(
        claims,
        _prices(),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-29T00:00:00+00:00",
    )
    assert len(next_day) == 2
    assert set(next_day["end_market_date"]) == {"2026-09-28"}

    with pytest.raises(ValueError, match="evaluated_at must be a parseable timestamp"):
        compute_mature_outcomes(
            claims,
            _prices(),
            pd.DataFrame(columns=OUTCOME_COLUMNS),
            "not-a-timestamp",
        )


def test_peer_labels_are_derived_from_all_currently_matured_snapshot_rows():
    claims = _claims()
    outcomes = compute_mature_outcomes(
        claims,
        _prices(),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-29T00:00:00+00:00",
    )
    labels = derive_peer_labels(claims, outcomes)
    assert set(labels["snapshot_id"]) == {"snap-1"}
    aaa = labels.loc[labels["symbol"].eq("AAA")].iloc[0]
    bbb = labels.loc[labels["symbol"].eq("BBB")].iloc[0]
    assert aaa["peer_median_return"] == pytest.approx(-0.05)
    assert bbb["peer_median_return"] == pytest.approx(0.10)
    assert aaa["peer_excess"] == pytest.approx(0.15)
    assert bbb["peer_excess"] == pytest.approx(-0.15)
    assert aaa["signed_peer_excess"] == pytest.approx(0.15)
    assert bbb["signed_peer_excess"] == pytest.approx(0.15)
    assert int(aaa["direction_hit"]) == 1
    assert int(bbb["direction_hit"]) == 1


def test_same_day_rerun_snapshots_do_not_mix_peer_cross_sections():
    claims = _claims()
    outcomes = compute_mature_outcomes(
        claims,
        _prices(),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-29T00:00:00+00:00",
    )

    claims2 = claims.copy()
    claims2["snapshot_id"] = "snap-2"
    claims2["claim_id"] = [f"snap2-{symbol}" for symbol in claims2["symbol"]]
    outcomes2 = outcomes.copy()
    outcomes2["claim_id"] = [f"snap2-{symbol}" for symbol in outcomes2["symbol"]]
    outcomes2.loc[outcomes2["symbol"].eq("AAA"), "return"] = 0.50
    outcomes2.loc[outcomes2["symbol"].eq("BBB"), "return"] = 0.40

    labels = derive_peer_labels(
        pd.concat([claims, claims2], ignore_index=True),
        pd.concat([outcomes, outcomes2], ignore_index=True),
    )
    snap1_aaa = labels.loc[
        labels["snapshot_id"].eq("snap-1") & labels["symbol"].eq("AAA")
    ].iloc[0]
    snap2_aaa = labels.loc[
        labels["snapshot_id"].eq("snap-2") & labels["symbol"].eq("AAA")
    ].iloc[0]
    assert snap1_aaa["peer_median_return"] == pytest.approx(-0.05)
    assert snap1_aaa["peer_excess"] == pytest.approx(0.15)
    assert snap2_aaa["peer_median_return"] == pytest.approx(0.40)
    assert snap2_aaa["peer_excess"] == pytest.approx(0.10)


def test_late_peer_maturity_improves_derived_label_without_rewriting_raw_outcome():
    claims = _claims()
    outcomes = compute_mature_outcomes(
        claims,
        _prices(),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-29T00:00:00+00:00",
    )
    aaa_raw = outcomes.loc[outcomes["symbol"].eq("AAA")].copy()
    bbb_raw = outcomes.loc[outcomes["symbol"].eq("BBB")].copy()

    early = derive_peer_labels(claims, aaa_raw)
    assert pd.isna(early.iloc[0]["peer_excess"])

    combined_raw = append_outcomes(aaa_raw, bbb_raw)
    later = derive_peer_labels(claims, combined_raw)
    aaa_later = later.loc[later["symbol"].eq("AAA")].iloc[0]
    assert aaa_later["peer_excess"] == pytest.approx(0.15)
    pd.testing.assert_frame_equal(
        aaa_raw.reset_index(drop=True),
        combined_raw.loc[combined_raw["symbol"].eq("AAA")].reset_index(drop=True),
        check_dtype=False,
    )


def test_outcomes_are_append_only():
    claims = _claims()
    new = compute_mature_outcomes(
        claims,
        _prices(),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-29T00:00:00+00:00",
    )
    existing = new.iloc[[0]].copy()
    remaining = new.iloc[[1]].copy()
    combined = append_outcomes(existing, remaining)
    assert len(combined) == 2
    with pytest.raises(ValueError, match="outcome rewrite attempted"):
        append_outcomes(existing, new.iloc[[0]].copy())


def test_validation_summary_cannot_create_scalar_confidence_or_tune_thresholds():
    claims = _claims()
    outcomes = compute_mature_outcomes(
        claims,
        _prices(),
        pd.DataFrame(columns=OUTCOME_COLUMNS),
        "2026-09-29T00:00:00+00:00",
    )
    report = validation_summary(claims, outcomes)
    assert report["status"] == "collecting_prospective_evidence"
    assert report["semantics"]["scalar_confidence_mapping_created"] is False
    assert report["semantics"]["confidence_thresholds_created"] is False
    assert report["semantics"]["claim_time_start_session_is_frozen"] is True
    assert report["semantics"]["missing_claim_time_start_remains_permanently_unevaluable"] is True
    assert report["semantics"]["peer_labels_are_derived_not_frozen_early"] is True
    assert report["semantics"]["peer_cross_sections_use_exact_snapshot_cohorts"] is True
    assert report["semantics"]["outcomes_require_completed_session_day"] is True
    assert report["semantics"]["outcome_adjusted_prices_share_one_evaluation_basis"] is True
    assert report["validation_contract"]["weights_or_thresholds_may_be_tuned_on_this_stream"] is False
    assert report["validation_contract"]["fixed_cooldown_sessions"] == 5
    assert report["horizons"]["5"]["block_length_sessions_for_future_inference"] == 10
    assert report["horizons"]["5"]["derived_peer_labels"] == 2
    assert report["horizons"]["5"]["snapshot_cohorts"] == 1
    assert report["horizons"]["5"]["outcome_eligible_claims"] == 2
    assert report["horizons"]["5"]["outcome_unevaluable_claims"] == 0


def test_workflow_contract_drains_only_proven_scanner_publications_and_uses_one_basis_prices():
    workflow = Path(".github/workflows/confidence_vnext_4e.yml").read_text(encoding="utf-8")
    assert "Bind to oldest unclaimed scanner publication" in workflow
    assert "rev-list', '--reverse', 'origin/main'" in workflow
    assert workflow.count("'rev-list', '--reverse', 'origin/main', '--first-parent', '--'") == 2
    assert "No unclaimed scanner publication exists; Phase 4E is a clean no-op." in workflow
    assert "backlog_count" in workflow
    assert "gh workflow run confidence_vnext_4e.yml --ref main" in workflow
    assert "github.event.workflow_run.head_branch == 'main'" in workflow
    assert "github.event.workflow_run.conclusion == 'success'" not in workflow
    assert "github.event.workflow_run.head_branch || github.ref_name" in workflow
    assert "📊 Autopilot: artifacts update [skip ci]" in workflow
    assert "diff-tree" in workflow
    assert "hashlib.sha256(latest_raw).hexdigest()" in workflow
    assert "claim_prices.csv" in workflow
    assert "evaluation_symbols.txt" in workflow
    assert "phase4e_evaluation_prices.csv" in workflow
    assert "prefetch_history" in workflow
    assert "phase4e-shadow-data" in workflow
    assert "group: phase4e-shadow-publication" in workflow
    assert "group: scanner-daily-publication" not in workflow
    assert "git push origin HEAD:refs/heads/phase4e-shadow-data" in workflow
    assert "git push origin HEAD:main" not in workflow
    assert "origin/phase4e-shadow-data:artifacts/research/confidence_vnext_shadow_claims_4e.csv" in workflow
