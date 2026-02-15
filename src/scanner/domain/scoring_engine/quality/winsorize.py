"""
Winsorizing Module für Universe Robustness

Clipped Ausreißer auf 1%/99% Quantile um stabilere Scores zu gewährleisten.
Config-driven und modular integrierbar.

Author: Trading-Zentrale v6
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
import logging

logger = logging.getLogger(__name__)

def winsorize_series(series: pd.Series, lower_q: float = 0.01, upper_q: float = 0.99) -> pd.Series:
    """
    Winsorized eine Series auf angegebene Quantile.
    
    Args:
        series: Input Series
        lower_q: Unteres Quantil (default 0.01)
        upper_q: Oberes Quantil (default 0.99)
    
    Returns:
        Winsorized Series
    """
    if series.empty:
        return series
    
    # Quantile berechnen
    lower_bound = series.quantile(lower_q)
    upper_bound = series.quantile(upper_q)
    
    # Clippen
    clipped = series.clip(lower=lower_bound, upper=upper_bound)
    
    # Logging für Debug
    clipped_low = (series < lower_bound).sum()
    clipped_high = (series > upper_bound).sum()
    
    if clipped_low > 0 or clipped_high > 0:
        logger.debug(f"Winsorized {series.name}: {clipped_low} low, {clipped_high} high outliers")
    
    return clipped

def winsorize_df(df: pd.DataFrame, cols: List[str], lower_q: float = 0.01, upper_q: float = 0.99) -> Tuple[pd.DataFrame, Dict[str, Dict[str, Any]]]:
    """
    Winsorized mehrere Spalten in einem DataFrame.
    
    Args:
        df: Input DataFrame
        cols: Liste der zu winsorizierenden Spalten
        lower_q: Unteres Quantil
        upper_q: Oberes Quantil
    
    Returns:
        Tuple: (winsorized_df, report_dict)
    """
    df_wins = df.copy()
    report = {}
    
    for col in cols:
        if col not in df.columns:
            logger.warning(f"Column {col} not found for winsorizing")
            continue
        
        series = df[col]
        
        # Original stats
        before_min = series.min()
        before_max = series.max()
        
        # NaNs zählen und temporär entfernen
        nan_count = series.isna().sum()
        series_clean = series.dropna()
        
        if series_clean.empty:
            logger.warning(f"Column {col} has only NaN values")
            report[col] = {
                "before_min": None,
                "before_max": None,
                "after_min": None,
                "after_max": None,
                "clipped_low": 0,
                "clipped_high": 0,
                "nan_count": nan_count
            }
            continue
        
        # Winsorizen
        lower_bound = series_clean.quantile(lower_q)
        upper_bound = series_clean.quantile(upper_q)
        
        clipped_series = series_clean.clip(lower=lower_bound, upper=upper_bound)
        
        # Stats nach Winsorizing
        after_min = clipped_series.min()
        after_max = clipped_series.max()
        
        # Outlier counts
        clipped_low = (series_clean < lower_bound).sum()
        clipped_high = (series_clean > upper_bound).sum()
        
        # Zurück in DataFrame mit NaNs
        df_wins.loc[series.index, col] = clipped_series.reindex(series.index)
        
        # Report
        report[col] = {
            "before_min": float(before_min) if pd.notna(before_min) else None,
            "before_max": float(before_max) if pd.notna(before_max) else None,
            "after_min": float(after_min) if pd.notna(after_min) else None,
            "after_max": float(after_max) if pd.notna(after_max) else None,
            "clipped_low": int(clipped_low),
            "clipped_high": int(clipped_high),
            "nan_count": int(nan_count),
            "lower_bound": float(lower_bound),
            "upper_bound": float(upper_bound)
        }
    
    return df_wins, report

def print_winsorize_report(report: Dict[str, Dict[str, Any]]) -> None:
    """
    Gibt Winsorizing-Report aus.
    
    Args:
        report: Report von winsorize_df()
    """
    print("\nWINSORIZING REPORT")
    print("=" * 60)
    
    for col, stats in report.items():
        print(f"\n{col}:")
        print(f"  Before: {stats['before_min']:.3f} / {stats['before_max']:.3f}")
        print(f"  After:  {stats['after_min']:.3f} / {stats['after_max']:.3f}")
        print(f"  Outliers: {stats['clipped_low']} low, {stats['clipped_high']} high")
        print(f"  NaNs: {stats['nan_count']}")
        print(f"  Bounds: [{stats['lower_bound']:.3f}, {stats['upper_bound']:.3f}]")
    
    print("=" * 60)

def apply_winsorizing_if_enabled(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Dict[str, Any]]]:
    """
    Wendet Winsorizing an falls konfiguriert.
    
    Args:
        df: Input DataFrame
        config: Config Dictionary
    
    Returns:
        Tuple: (processed_df, report)
    """
    if not config.get('WINSORIZE_ENABLED', False):
        logger.info("Winsorizing disabled")
        return df, {}
    
    cols = config.get('WINSORIZE_COLS', [])
    lower_q = config.get('WINSORIZE_Q_LOW', 0.01)
    upper_q = config.get('WINSORIZE_Q_HIGH', 0.99)
    
    if not cols:
        logger.warning("No columns configured for winsorizing")
        return df, {}
    
    logger.info(f"Applying winsorizing to {len(cols)} columns (q={lower_q:.3f}-{upper_q:.3f})")
    
    df_wins, report = winsorize_df(df, cols, lower_q, upper_q)
    
    # Summary
    total_clipped = sum(stats['clipped_low'] + stats['clipped_high'] for stats in report.values())
    logger.info(f"Winsorizing complete: {total_clipped} outliers clipped across {len(report)} columns")
    
    return df_wins, report
