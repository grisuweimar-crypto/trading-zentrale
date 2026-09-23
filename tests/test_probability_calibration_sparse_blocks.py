import pandas as pd

from scanner.reports.probability_calibration import Phase2Config, _block_bootstrap_uncertainty


def test_block_uncertainty_requires_two_time_separated_occurrence_regions():
    dates = pd.bdate_range("2026-05-01", periods=25)
    baseline = pd.DataFrame(
        {
            "obs_date": dates.repeat(2),
            "peer_excess_5t": [-0.2, 0.1] * 25,
        }
    )
    first_block_dates = set(dates[:5])
    occurrences = baseline.loc[baseline["obs_date"].isin(first_block_dates)].copy()

    out = _block_bootstrap_uncertainty(
        occurrences,
        baseline,
        "peer_excess_5t",
        5,
        Phase2Config(cluster_bootstrap_reps=100),
        123,
    )

    assert out["block_count"] == 25
    assert out["occurrence_block_count"] == 1
    assert out["mean_peer_excess_95"] is None
    assert out["probability_advantage_95"] is None
