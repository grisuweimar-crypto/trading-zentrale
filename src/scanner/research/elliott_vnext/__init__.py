"""Research-only Elliott vNext building blocks.

Module 6 remains outside productive scanner/trading logic until its separate
validation and integration gates are completed.
"""

from .pivots import (
    DEFAULT_PIVOT_SPECS,
    PivotSpec,
    aggregate_weekly,
    atr_series,
    detect_confirmed_pivots,
    detect_multidegree_pivots,
    prepare_daily_ohlcv,
)
from .scenarios import (
    CLASSIC_IMPULSE_RULES,
    CONSERVATIVE_SUPPORT,
    FULL_SUPPORT,
    ScenarioInputError,
    evaluate_correction_window,
    evaluate_motive_window,
    generate_scenario_sets,
)

__all__ = [
    "DEFAULT_PIVOT_SPECS",
    "PivotSpec",
    "aggregate_weekly",
    "atr_series",
    "detect_confirmed_pivots",
    "detect_multidegree_pivots",
    "prepare_daily_ohlcv",
    "CLASSIC_IMPULSE_RULES",
    "CONSERVATIVE_SUPPORT",
    "FULL_SUPPORT",
    "ScenarioInputError",
    "evaluate_correction_window",
    "evaluate_motive_window",
    "generate_scenario_sets",
]
