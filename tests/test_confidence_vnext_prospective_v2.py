from __future__ import annotations

import json

import pandas as pd
import pytest

from scanner.reports.confidence_vnext_prospective_v2 import (
    CLAIM_COLUMNS_V2,
    OUTCOME_COLUMNS_V2,
    SCHEMA_VERSION,
    _mature_one_v2,
    _risk_context,
    _sha_payload,
    _timing_context,
    append_claims_v2,
    append_outcomes_v2,
    validate_v2_archives,
)


def _claim(claim_id: str | None = None, *, symbol: str = "AAA", horizon: int = 5, start: str = "2026-10-01", as_of: str = "2026-10-01"):
    payload = {column: "" for column in CLAIM_COLUMNS_V2 if column != "claim_id"}
    payload.update(
        {
            "schema_version": SCHEMA_VERSION,
            "source_commit": "a" * 40,
            "as_of": as_of,
            "generated_at": f"{as_of}T16:00:00+00:00",
            "run_id": "github-1-1",
            "snapshot_id": f"snap-{as_of}",
            "symbol": symbol,
            "currency": "USD",
            "horizon_sessions": horizon,
            "start_market_date": start,
            "start_adjusted_close": 100.0,
            "outcome_eligibility": "eligible",
            "outcome_unavailable_reason": "",
            "cooldown_forbidden_start_dates": json.dumps([start], separators=(",", ":")),
            "cooldown_context_complete": True,
            "evidence_version": "phase4_confidence_research_v1",
            "evidence_fingerprint": "e" * 64,
            "phase4_report_sha256": "1" * 64,
            "phase2_sha256": "2" * 64,
            "phase3_sha256": "3" * 64,
            "risk_scale_sha256": "missing",
            "phase2_source_as_of": f"{as_of}T00:00:00+00:00",
            "phase3_source_as_of": f"{as_of}T00:00:00+00:00",
            "selection_band": "B5",
            "selection_statistical_state": "robust",
            "selection_statistical_direction": "positive",
            "selection_statistical_context": "{}",
            "timing_statistical_context": "[]",
            "timing_matched_states": "",
            "timing_unevaluable_state_counts": "{}",
            "risk_statistical_context": "[]",
            "risk_evidence_states": "",
            "statistical_context_sha256": "4" * 64,
            "timing_model_state": "unknown",
            "timing_model_direction": "",
            "risk_model_state": "middle",
            "agreement_state": "single_model",
            "agreement_conflicts": "",
            "return_claim_direction": "positive",
            "dq_selection_state": "proxy_complete",
            "dq_timing_state": "proxy_complete_no_claim",
            "dq_risk_state": "proxy_complete",
            "volatility_application_status": "compatible",
        }
    )
    payload["claim_id"] = claim_id or _sha_payload(payload)
    return payload


def _prices(symbol: str = "AAA", periods: int = 10):
    dates = pd.bdate_range("2026-10-01", periods=periods)
    return pd.DataFrame(
        {
            "symbol": symbol,
            "date": dates,
            "adj_close": [100.0 + i for i in range(periods)],
        }
    )


def test_timing_context_preserves_matched_ordinal_states_without_ranking():
    feature = pd.Series({"a": True, "b": True})
    registry = {
        "a": {"state": "robust", "direction": "positive", "conditions": ["a"], "pattern": "a"},
        "b": {"state": "directional_only", "direction": "negative", "conditions": ["b"], "pattern": "b"},
        "missing": {"state": "immature", "direction": None, "conditions": ["c"], "pattern": "c"},
    }
    matched, unevaluable = _timing_context(feature, registry)
    assert {row["state"] for row in matched} == {"robust", "directional_only"}
    assert unevaluable == {"immature": 1}


def test_risk_context_preserves_feature_state_and_claim_level():
    feature = pd.Series({"volatility": 0.9, "drawdown": 0.1})
    registry = {
        "volatility": {"state": "robust", "direction": "higher_is_riskier", "low_cutoff": 0.2, "high_cutoff": 0.8},
        "drawdown": {"state": "directional_only", "direction": "higher_is_riskier", "low_cutoff": 0.2, "high_cutoff": 0.8},
    }
    context = _risk_context(feature, registry)
    by_feature = {row["feature"]: row for row in context}
    assert by_feature["volatility"]["state"] == "robust"
    assert by_feature["volatility"]["claim_level"] == "high"
    assert by_feature["drawdown"]["state"] == "directional_only"
    assert by_feature["drawdown"]["claim_level"] == "low"


def test_v2_outcome_integrity_binds_exact_market_session_path():
    claim = _claim()
    outcome = _mature_one_v2(claim, _prices(), "2026-10-12T12:00:00Z")
    assert outcome is not None
    assert outcome["elapsed_market_sessions"] == 5
    assert outcome["path_session_count"] == 6
    assert outcome["start_market_date"] == "2026-10-01"
    assert outcome["end_market_date"] == "2026-10-08"
    assert len(outcome["session_dates_sha256"]) == 64
    assert len(outcome["path_sha256"]) == 64
    assert len(outcome["outcome_id"]) == 64


def test_v2_outcome_requires_completed_target_session_day():
    claim = _claim()
    assert _mature_one_v2(claim, _prices(), "2026-10-08T18:00:00Z") is None


def test_v2_archives_survive_csv_round_trip_without_hash_drift(tmp_path):
    claim = _claim()
    outcome = _mature_one_v2(claim, _prices(), "2026-10-12T12:00:00Z")
    claims = pd.DataFrame([claim], columns=CLAIM_COLUMNS_V2)
    outcomes = pd.DataFrame([outcome], columns=OUTCOME_COLUMNS_V2)
    validate_v2_archives(claims, outcomes)

    claims_path = tmp_path / "claims.csv"
    outcomes_path = tmp_path / "outcomes.csv"
    claims.to_csv(claims_path, index=False)
    outcomes.to_csv(outcomes_path, index=False)
    reloaded_claims = pd.read_csv(claims_path, dtype=str, keep_default_na=False)
    reloaded_outcomes = pd.read_csv(outcomes_path, dtype=str, keep_default_na=False)
    validate_v2_archives(reloaded_claims, reloaded_outcomes)


def test_append_only_claims_and_outcomes_reject_conflicts():
    claim = _claim()
    claims = pd.DataFrame([claim], columns=CLAIM_COLUMNS_V2)
    changed = dict(claim)
    changed["selection_statistical_state"] = "mixed"
    changed["claim_id"] = _sha_payload({key: value for key, value in changed.items() if key != "claim_id"})
    fresh = pd.DataFrame([changed], columns=CLAIM_COLUMNS_V2)
    with pytest.raises(ValueError, match="immutable v2 claim conflict"):
        append_claims_v2(claims, fresh)

    outcome = _mature_one_v2(claim, _prices(), "2026-10-12T12:00:00Z")
    outcomes = pd.DataFrame([outcome], columns=OUTCOME_COLUMNS_V2)
    changed_outcome = dict(outcome)
    changed_outcome["return"] = float(changed_outcome["return"]) + 0.01
    changed_outcome["outcome_id"] = _sha_payload({key: value for key, value in changed_outcome.items() if key != "outcome_id"})
    with pytest.raises(ValueError, match="immutable v2 outcome conflict"):
        append_outcomes_v2(outcomes, pd.DataFrame([changed_outcome], columns=OUTCOME_COLUMNS_V2))



def test_phase5b_transport_is_deterministic_lossless_and_cross_stage(tmp_path):
    """The large v2 archive must roundtrip without truncation or new predictions."""
    import gzip
    import hashlib
    import subprocess
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    original = (b'run_id,claim_id,outcome_eligibility\r\n'
                + b'github-1-1,unchanged,eligible\r\n' * 14000)
    path = tmp_path / 'claims.csv'
    zipped = tmp_path / 'claims.csv.gz'
    path.write_bytes(original)
    with zipped.open('wb') as dst:
        subprocess.run(['gzip', '-n', '-6', '-c', str(path)], stdout=dst, check=True)
    subprocess.run(['gzip', '-t', str(zipped)], check=True)
    assert gzip.decompress(zipped.read_bytes()) == original
    first = hashlib.sha256(zipped.read_bytes()).hexdigest()
    with zipped.open('wb') as dst:
        subprocess.run(['gzip', '-n', '-6', '-c', str(path)], stdout=dst, check=True)
    assert hashlib.sha256(zipped.read_bytes()).hexdigest() == first

    b = (root / '.github/workflows/confidence_vnext_5b.yml').read_text()
    assert 'gzip -n -6 -c "$path" > "$path.gz"' in b
    assert 'cmp -s "$path" <(gzip -cd "$path.gz")' in b
    assert 'phase5b_v2_compressed_archive_corrupt' in b
    assert 'phase5b_queue_compressed_archive_corrupt' in b
    assert '80000000' in b
    assert 'V2_PATHS=(' in b
    assert 'git rm -f --ignore-unmatch -- "$path"' in b

    for suffix in ('5c', '5d', '5e'):
        flow = (root / '.github/workflows' / f'confidence_vnext_{suffix}.yml').read_text()
        assert 'origin/phase4e-shadow-data:$path.gz' in flow
        assert 'gzip -t "$path.gz"' in flow
        assert 'gzip -cd "$path.gz" > "$path"' in flow
        assert 'SHADOW_AVAILABLE=true' in flow
