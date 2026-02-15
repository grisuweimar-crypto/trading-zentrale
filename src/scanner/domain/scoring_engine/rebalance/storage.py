"""
Rebalance Storage Module

Speichert und lädt Rebalance-Snapshots für Historie.
Optionales Feature für Tracking und Analyse.

Author: Trading-Zentrale v6
"""

import json
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

def save_rebalance_snapshot(
    result: Dict[str, Any], 
    path: str = "data/rebalance_last.json"
) -> bool:
    """
    Speichert Rebalance-Ergebnis als Snapshot.
    
    Args:
        result: Rebalance-Ergebnis
        path: Pfad zur Snapshot-Datei
        
    Returns:
        True bei Erfolg
    """
    try:
        # Stelle sicher dass Ordner existiert
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # Snapshot erstellen
        snapshot = {
            'timestamp': datetime.now().isoformat(),
            'meta': result.get('meta', {}),
            'holdings_summary': {
                'total_value': result.get('holdings', {}).get('total_value', 0),
                'positions_count': result.get('holdings', {}).get('positions_count', 0),
                'cash_value': result.get('holdings', {}).get('cash_value', 0)
            },
            'target_summary': {
                'positions_count': len(result.get('target_portfolio', {}).get('positions', [])),
                'cash_pct': result.get('target_portfolio', {}).get('meta', {}).get('cash_pct', 0)
            },
            'plan_summary': {
                'actions_count': result.get('plan', {}).get('meta', {}).get('actions_count', 0),
                'turnover': result.get('plan', {}).get('meta', {}).get('turnover', 0),
                'turnover_limit': result.get('plan', {}).get('meta', {}).get('turnover_limit', 0)
            },
            'actions': result.get('plan', {}).get('actions', [])[:10],  # Nur Top 10 Actions
            'version': 'v1.0'
        }
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Rebalance snapshot saved: {path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Snapshot save error: {e}")
        return False

def load_last_rebalance_snapshot(path: str = "data/rebalance_last.json") -> Optional[Dict[str, Any]]:
    """
    Lädt letzten Rebalance-Snapshot.
    
    Args:
        path: Pfad zur Snapshot-Datei
        
    Returns:
        Snapshot-Dict oder None
    """
    try:
        if not os.path.exists(path):
            logger.info(f"📂 No snapshot found: {path}")
            return None
        
        with open(path, 'r', encoding='utf-8') as f:
            snapshot = json.load(f)
        
        logger.info(f"✅ Snapshot loaded: {snapshot.get('timestamp', 'unknown')}")
        return snapshot
        
    except Exception as e:
        logger.error(f"❌ Snapshot load error: {e}")
        return None

def get_rebalance_history(path: str = "data/rebalance_last.json") -> Optional[Dict[str, Any]]:
    """
    Holt Rebalance-Historie für Vergleich.
    
    Args:
        path: Pfad zur Snapshot-Datei
        
    Returns:
        Historie-Dict oder None
    """
    snapshot = load_last_rebalance_snapshot(path)
    if not snapshot:
        return None
    
    return {
        'last_run': snapshot.get('timestamp'),
        'last_actions': snapshot.get('plan_summary', {}).get('actions_count', 0),
        'last_turnover': snapshot.get('plan_summary', {}).get('turnover', 0),
        'last_total_value': snapshot.get('holdings_summary', {}).get('total_value', 0),
        'last_market_regime': snapshot.get('meta', {}).get('market_regime', 'unknown')
    }

def cleanup_old_snapshots(
    snapshot_dir: str = "data/",
    keep_count: int = 5
) -> int:
    """
    Räumt alte Snapshots auf.
    
    Args:
        snapshot_dir: Verzeichnis mit Snapshots
        keep_count: Anzahl der Snapshots die behalten werden
        
    Returns:
        Anzahl der gelöschten Dateien
    """
    try:
        snapshot_files = []
        
        # Snapshot-Dateien finden
        for filename in os.listdir(snapshot_dir):
            if filename.startswith('rebalance_') and filename.endswith('.json'):
                filepath = os.path.join(snapshot_dir, filename)
                mtime = os.path.getmtime(filepath)
                snapshot_files.append((filepath, mtime, filename))
        
        # Nach Zeit sortieren (neueste zuerst)
        snapshot_files.sort(key=lambda x: x[1], reverse=True)
        
        # Alte Dateien löschen
        deleted_count = 0
        for filepath, _, filename in snapshot_files[keep_count:]:
            try:
                os.remove(filepath)
                logger.info(f"🗑️ Deleted old snapshot: {filename}")
                deleted_count += 1
            except Exception as e:
                logger.warning(f"⚠️ Could not delete {filename}: {e}")
        
        if deleted_count > 0:
            logger.info(f"✅ Cleaned up {deleted_count} old snapshots")
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"❌ Snapshot cleanup error: {e}")
        return 0

def compare_snapshots(
    current: Dict[str, Any], 
    previous: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Vergleicht aktuellen mit vorherigem Snapshot.
    
    Args:
        current: Aktuelles Ergebnis
        previous: Vorheriger Snapshot
        
    Returns:
        Vergleichs-Dict
    """
    if not previous:
        return {'change': 'first_run', 'differences': {}}
    
    try:
        current_meta = current.get('meta', {})
        prev_meta = previous.get('meta', {})
        
        current_plan = current.get('plan', {}).get('meta', {})
        prev_plan = previous.get('plan_summary', {})
        
        differences = {
            'total_value_change': current_meta.get('total_value', 0) - previous.get('holdings_summary', {}).get('total_value', 0),
            'actions_change': current_plan.get('actions_count', 0) - prev_plan.get('actions_count', 0),
            'turnover_change': current_plan.get('turnover', 0) - prev_plan.get('turnover', 0),
            'regime_change': current_meta.get('market_regime') != prev_meta.get('market_regime'),
            'positions_change': current_meta.get('positions_count', 0) - previous.get('target_summary', {}).get('positions_count', 0)
        }
        
        # Signifikante Änderungen erkennen
        significant_changes = []
        
        if abs(differences['total_value_change']) > 1000:  # > 1000€
            significant_changes.append(f"Portfolio value: {differences['total_value_change']:+,.0f}€")
        
        if differences['actions_change'] != 0:
            significant_changes.append(f"Actions: {differences['actions_change']:+d}")
        
        if differences['turnover_change'] > 0.1:  # > 10% Turnover-Änderung
            significant_changes.append(f"Turnover: {differences['turnover_change']:+.1%}")
        
        if differences['regime_change']:
            old_regime = prev_meta.get('market_regime', 'unknown')
            new_regime = current_meta.get('market_regime', 'unknown')
            significant_changes.append(f"Regime: {old_regime} → {new_regime}")
        
        return {
            'change': 'comparison' if significant_changes else 'no_significant_change',
            'differences': differences,
            'significant_changes': significant_changes
        }
        
    except Exception as e:
        logger.error(f"❌ Snapshot comparison error: {e}")
        return {'change': 'error', 'error': str(e)}
