"""
Quality Module für Universe Robustness und Confidence Scoring

Enthält:
- Winsorizing für Ausreißer-Kontrolle
- Confidence Score für Datenqualität
- Calibration Light Snapshots

Author: Trading-Zentrale v6
"""

from .winsorize import winsorize_series, winsorize_df, apply_winsorizing_if_enabled, print_winsorize_report
from .confidence import compute_confidence, print_confidence_report
from .snapshots import save_daily_snapshot, load_snapshot_history, compute_forward_returns, analyze_calibration, print_calibration_report

__all__ = [
    'winsorize_series',
    'winsorize_df', 
    'apply_winsorizing_if_enabled',
    'print_winsorize_report',
    'compute_confidence',
    'print_confidence_report',
    'save_daily_snapshot',
    'load_snapshot_history',
    'compute_forward_returns',
    'analyze_calibration',
    'print_calibration_report'
]
