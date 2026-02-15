"""
Calibration Light Snapshots Module

Speichert tägliche Snapshots für späte Outcome-Analyse.
Kein voller Backtest, sondern einfache Korrelationsanalyse.

Author: Trading-Zentrale v6
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List
from pathlib import Path
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def save_daily_snapshot(df_scored: pd.DataFrame, path: str = "data/snapshots/score_history.csv") -> None:
    """
    Speichert täglichen Snapshot der gescouteten Daten.
    
    Args:
        df_scored: DataFrame mit Scores und Faktoren
        path: Pfad zur Snapshot-Datei
    """
    try:
        # Sicherstellen dass Verzeichnis existiert
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        
        # Snapshot-Metadaten generieren
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        universe_version = generate_universe_hash(df_scored)
        config_version = get_config_version()
        
        # Snapshot-Daten vorbereiten
        snapshot_data = []
        
        for _, row in df_scored.iterrows():
            snapshot_row = {
                'run_id': run_id,
                'universe_version': universe_version,
                'config_version': config_version,
                'date': datetime.now().strftime('%Y-%m-%d'),
                'symbol': row.get('Ticker', ''),
                'name': row.get('Name', ''),
                'score': row.get('Score', np.nan),
                'opportunity': row.get('OpportunityScore', np.nan),
                'risk': row.get('RiskScore', np.nan),
                'confidence': row.get('ConfidenceScore', np.nan),
                'confidence_label': row.get('ConfidenceLabel', ''),
                'rs3m': row.get('RS3M', np.nan),
                'trend200': row.get('Trend200', np.nan),
                'liquidity_risk': row.get('LiquidityRisk', np.nan),
                'volatility': row.get('Volatility', np.nan),
                'drawdown': row.get('MaxDrawdown', np.nan),
                'roe': row.get('ROE %', np.nan),
                'growth': row.get('Growth %', np.nan),
                'margin': row.get('Margin %', np.nan),
                'debt_ratio': row.get('Debt/Equity', np.nan),
                'close': row.get('Akt. Kurs', np.nan),
                'currency': row.get('Währung', ''),
                'sector': row.get('Sektor', ''),
                'market_regime_stock': row.get('MarketRegimeStock', ''),
                'market_regime_crypto': row.get('MarketRegimeCrypto', ''),
                'market_trend200_stock': row.get('MarketTrend200Stock', np.nan),
                'market_trend200_crypto': row.get('MarketTrend200Crypto', np.nan)
            }
            snapshot_data.append(snapshot_row)
        
        # DataFrame erstellen
        snapshot_df = pd.DataFrame(snapshot_data)
        
        # Append oder neu erstellen
        if Path(path).exists():
            # Bestehende Datei laden und appenden
            existing_df = pd.read_csv(path)
            combined_df = pd.concat([existing_df, snapshot_df], ignore_index=True)
        else:
            combined_df = snapshot_df
        
        # Speichern
        combined_df.to_csv(path, index=False)
        
        # Cleanup: Nur letzte 90 Tage behalten (optional)
        cleanup_old_snapshots(path, days_to_keep=90)
        
        logger.info(f"Daily snapshot saved: {len(snapshot_df)} symbols to {path}")
        logger.info(f"Run ID: {run_id}, Universe: {universe_version[:8]}..., Config: {config_version}")
        
    except Exception as e:
        logger.error(f"Failed to save daily snapshot: {e}")

def generate_universe_hash(df: pd.DataFrame) -> str:
    """Generiert Hash der Watchlist für Universe Version"""
    try:
        import hashlib
        
        # Relevante Spalten für Hash
        key_data = df[['Ticker', 'Name', 'Yahoo']].sort_values('Ticker').to_string()
        
        # Hash generieren
        hash_obj = hashlib.md5(key_data.encode('utf-8'))
        return hash_obj.hexdigest()
        
    except Exception as e:
        logger.warning(f"Failed to generate universe hash: {e}")
        return "unknown"

def get_config_version() -> str:
    """Gibt Config Version zurück (git commit oder manuell)"""
    try:
        # Versuche git commit zu bekommen
        import subprocess
        result = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            return f"git-{result.stdout.strip()}"
    except Exception:
        pass
    
    # Fallback: Timestamp
    return datetime.now().strftime('%Y%m%d')

def cleanup_old_snapshots(path: str, days_to_keep: int = 90) -> None:
    """
    Entfernt alte Snapshots um Dateigröße zu kontrollieren.
    
    Args:
        path: Pfad zur Snapshot-Datei
        days_to_keep: Anzahl Tage die behalten werden
    """
    try:
        if not Path(path).exists():
            return
        
        df = pd.read_csv(path)
        if 'date' not in df.columns:
            return
        
        # Datum konvertieren
        df['date'] = pd.to_datetime(df['date'])
        
        # Kürzeste Daten
        cutoff_date = datetime.now() - pd.Timedelta(days=days_to_keep)
        
        # Filtern
        filtered_df = df[df['date'] >= cutoff_date]
        
        # Zurückschreiben
        filtered_df.to_csv(path, index=False)
        
        removed = len(df) - len(filtered_df)
        if removed > 0:
            logger.info(f"Cleaned up {removed} old snapshot records")
            
    except Exception as e:
        logger.warning(f"Failed to cleanup old snapshots: {e}")

def load_snapshot_history(path: str = "data/snapshots/score_history.csv") -> pd.DataFrame:
    """
    Lädt Snapshot-Historie.
    
    Args:
        path: Pfad zur Snapshot-Datei
    
    Returns:
        DataFrame mit Historie
    """
    try:
        if not Path(path).exists():
            logger.warning(f"Snapshot file not found: {path}")
            return pd.DataFrame()
        
        df = pd.read_csv(path)
        
        # Datum konvertieren
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        logger.info(f"Loaded {len(df)} snapshot records from {path}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load snapshot history: {e}")
        return pd.DataFrame()

def compute_forward_returns(df_history: pd.DataFrame, forward_days: int = 20) -> pd.DataFrame:
    """
    Berechnet Forward Returns für Kalibrationsanalyse.
    
    Args:
        df_history: Snapshot-Historie
        forward_days: Anzahl Tage für Forward Return
    
    Returns:
        DataFrame mit Forward Returns
    """
    try:
        if df_history.empty or 'close' not in df_history:
            logger.warning("No price data for forward return calculation")
            return df_history
        
        # Pro Symbol Forward Returns berechnen
        df_with_fwd = df_history.copy()
        df_with_fwd['forward_return'] = np.nan
        
        for symbol in df_with_fwd['symbol'].unique():
            symbol_data = df_with_fwd[df_with_fwd['symbol'] == symbol].sort_values('date')
            
            for i, current_row in symbol_data.iterrows():
                current_date = current_row['date']
                current_close = current_row['close']
                
                if pd.isna(current_close):
                    continue
                
                # Finde Preis nach forward_days
                future_date = current_date + pd.Timedelta(days=forward_days)
                future_data = symbol_data[symbol_data['date'] >= future_date]
                
                if not future_data.empty:
                    future_close = future_data.iloc[0]['close']
                    if pd.notna(future_close):
                        forward_return = (future_close - current_close) / current_close
                        df_with_fwd.loc[i, 'forward_return'] = forward_return
        
        # Hit Rate (positive return)
        df_with_fwd['hit_rate'] = (df_with_fwd['forward_return'] > 0).astype(int)
        
        logger.info(f"Computed forward returns for {forward_days} days")
        return df_with_fwd
        
    except Exception as e:
        logger.error(f"Failed to compute forward returns: {e}")
        return df_history

def analyze_calibration(df_with_fwd: pd.DataFrame) -> Dict[str, Any]:
    """
    Analysiert Kalibration durch Korrelationen.
    
    Args:
        df_with_fwd: DataFrame mit Forward Returns
    
    Returns:
        Dict mit Kalibrations-Resultaten
    """
    try:
        # Nur gültige Daten
        valid_data = df_with_fwd.dropna(subset=['score', 'forward_return', 'opportunity', 'risk'])
        
        if valid_data.empty:
            return {'error': 'No valid data for calibration analysis'}
        
        # Korrelationen berechnen
        correlations = {}
        
        # Score vs Forward Return
        score_corr = valid_data['score'].corr(valid_data['forward_return'])
        correlations['score_vs_return'] = score_corr
        
        # Opportunity vs Forward Return
        opp_corr = valid_data['opportunity'].corr(valid_data['forward_return'])
        correlations['opportunity_vs_return'] = opp_corr
        
        # Risk vs Negative Drawdown (inverse correlation expected)
        risk_corr = valid_data['risk'].corr(-valid_data['forward_return'])
        correlations['risk_vs_negative_return'] = risk_corr
        
        # Confidence vs Forward Return
        if 'confidence' in valid_data.columns:
            conf_corr = valid_data['confidence'].corr(valid_data['forward_return'])
            correlations['confidence_vs_return'] = conf_corr
        
        # Hit Rate Analysis
        hit_rate = valid_data['hit_rate'].mean()
        correlations['overall_hit_rate'] = hit_rate
        
        # Score Quintile Performance
        valid_data['score_quintile'] = pd.qcut(valid_data['score'], 5, labels=['Q1', 'Q2', 'Q3', 'Q4', 'Q5'])
        quintile_performance = valid_data.groupby('score_quintile')['forward_return'].mean()
        correlations['quintile_performance'] = quintile_performance.to_dict()
        
        # Summary
        analysis = {
            'sample_size': len(valid_data),
            'correlations': correlations,
            'recommendations': generate_recommendations(correlations)
        }
        
        logger.info(f"Calibration analysis complete: {len(valid_data)} samples")
        return analysis
        
    except Exception as e:
        logger.error(f"Failed to analyze calibration: {e}")
        return {'error': str(e)}

def generate_recommendations(correlations: Dict[str, Any]) -> List[str]:
    """
    Generiert einfache Empfehlungen basierend auf Korrelationen.
    """
    recommendations = []
    
    score_corr = correlations.get('score_vs_return', 0)
    opp_corr = correlations.get('opportunity_vs_return', 0)
    risk_corr = correlations.get('risk_vs_negative_return', 0)
    
    # Score Performance
    if abs(score_corr) < 0.1:
        recommendations.append("Score shows low correlation with returns - consider factor reweighting")
    elif score_corr < 0:
        recommendations.append("Score negatively correlated - check factor directions")
    
    # Opportunity Factors
    if opp_corr > 0.3:
        recommendations.append("Opportunity factors show strong predictive power")
    elif opp_corr < 0.1:
        recommendations.append("Opportunity factors need improvement")
    
    # Risk Factors
    if risk_corr > 0.2:
        recommendations.append("Risk factors effectively predict negative returns")
    elif risk_corr < 0:
        recommendations.append("Risk factors may be mis-specified")
    
    # Hit Rate
    hit_rate = correlations.get('overall_hit_rate', 0.5)
    if hit_rate < 0.5:
        recommendations.append(f"Low hit rate ({hit_rate:.1%}) - review entry conditions")
    
    return recommendations

def print_calibration_report(analysis: Dict[str, Any]) -> None:
    """
    Gibt Kalibrations-Report aus.
    """
    if 'error' in analysis:
        print(f"Calibration Error: {analysis['error']}")
        return
    
    print("\nCALIBRATION ANALYSIS")
    print("=" * 50)
    print(f"Sample Size: {analysis['sample_size']}")
    
    print("\nCorrelations:")
    corr = analysis['correlations']
    for metric, value in corr.items():
        if metric != 'quintile_performance' and isinstance(value, (int, float)):
            print(f"  {metric}: {value:.3f}")
    
    print("\nQuintile Performance:")
    if 'quintile_performance' in corr:
        for quintile, perf in corr['quintile_performance'].items():
            print(f"  {quintile}: {perf:.2%}")
    
    print("\nRecommendations:")
    for rec in analysis['recommendations']:
        print(f"  • {rec}")
    
    print("=" * 50)
