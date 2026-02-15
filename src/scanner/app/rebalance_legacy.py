#!/usr/bin/env python3
"""
Rebalance Run Script mit echten Broker-Exports

CLI-Interface für die Rebalance Engine mit ISIN-Matching.
Vergleicht echte Depot-Exports mit Ziel-Portfolio aus Portfolio Builder.

Usage: python rebalance_run.py [--top_n 10] [--min_score 30] [--turnover_limit 0.35]

Author: Trading-Zentrale v6
"""

import argparse
import logging
import sys
from pathlib import Path

# Projekt-Root zum Python-Pfad hinzufügen
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.logging_setup import setup_logging
from scoring_engine.rebalance import (
    run_rebalance,
    create_sample_broker_files,
    print_rebalance_summary,
    validate_environment
)

# Logging setup
logger = setup_logging()
logger.info("RUN REBALANCE")

def parse_arguments():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Rebalance Engine mit echten Broker-Exports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python rebalance_run.py
  python rebalance_run.py --top_n 15 --min_score 40
  python rebalance_run.py --dry-run --turnover_limit 0.25
  python rebalance_run.py --create-sample-broker-files
        """
    )
    
    parser.add_argument(
        '--top_n',
        type=int,
        default=10,
        help='Anzahl der Top-Assets (default: 10)'
    )
    
    parser.add_argument(
        '--min_score',
        type=float,
        default=30.0,
        help='Minimaler Score für Selektion (default: 30.0)'
    )
    
    parser.add_argument(
        '--max_positions',
        type=int,
        default=20,
        help='Maximale Anzahl Positionen (default: 20)'
    )
    
    parser.add_argument(
        '--turnover_limit',
        type=float,
        default=0.35,
        help='Maximaler Turnover (default: 0.35 = 35%%)'
    )
    
    parser.add_argument(
        '--min_trade_pct',
        type=float,
        default=1.0,
        help='Minimale Trade-Größe in %% (default: 1.0%%)'
    )
    
    parser.add_argument(
        '--min_trade_value',
        type=float,
        default=25.0,
        help='Minimaler Trade-Wert in EUR (default: 25.0)'
    )
    
    parser.add_argument(
        '--allow_crypto',
        action='store_true',
        default=True,
        help='Krypto-Assets erlauben (default: True)'
    )
    
    parser.add_argument(
        '--stocks_path',
        type=str,
        default='data/holdings/stocks.csv',
        help='Pfad zur Aktien-CSV (default: data/holdings/stocks.csv)'
    )
    
    parser.add_argument(
        '--crypto_path',
        type=str,
        default='data/holdings/crypto.csv',
        help='Pfad zur Krypto-CSV (default: data/holdings/crypto.csv)'
    )
    
    parser.add_argument(
        '--watchlist_path',
        type=str,
        default='watchlist.csv',
        help='Pfad zur watchlist.csv (default: watchlist.csv)'
    )
    
    parser.add_argument(
        '--symbol_map_path',
        type=str,
        default='data/holdings/symbol_map.csv',
        help='Pfad zur Symbol-Mapping (default: data/holdings/symbol_map.csv)'
    )
    
    parser.add_argument(
        '--snapshot_path',
        type=str,
        default='data/snapshots/rebalance_last.json',
        help='Pfad zum Snapshot (default: data/snapshots/rebalance_last.json)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Nur Plan anzeigen, nicht speichern/senden'
    )
    
    parser.add_argument(
        '--no-alert',
        action='store_true',
        help='Keine Telegram-Nachricht senden'
    )
    
    parser.add_argument(
        '--create-sample-broker-files',
        action='store_true',
        help='Erstelle Beispiel-Broker-CSVs'
    )
    
    parser.add_argument(
        '--validate-env',
        action='store_true',
        help='Validiere Umgebung (Dateien, Module etc.)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Verbose Logging'
    )
    
    return parser.parse_args()

def main():
    """Hauptfunktion."""
    try:
        args = parse_arguments()
        
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        logger.info("🚀 Starting Rebalance Engine CLI with Broker Exports")
        
        # Environment Validation
        if args.validate_env:
            print("🔍 Environment Validation:")
            print("=" * 50)
            
            validation = validate_environment()
            for component, status in validation.items():
                status_icon = "✅" if status else "❌"
                print(f"{status_icon} {component}: {'OK' if status else 'MISSING'}")
            
            print("=" * 50)
            return 0
        
        # Sample Broker Files erstellen
        if args.create_sample_broker_files:
            print("📝 Creating sample broker files...")
            success = create_sample_broker_files()
            if success:
                print("✅ Sample broker files created:")
                print("   📈 data/holdings/stocks.csv")
                print("   🪙 data/holdings/crypto.csv")
                print("   🔗 data/holdings/symbol_map.csv")
                print("📊 Edit files with your actual holdings and run again.")
            else:
                print("❌ Failed to create sample broker files")
            return 0 if success else 1
        
        # Environment prüfen
        validation = validate_environment()
        critical_failures = [k for k, v in validation.items() if not v and k != 'telegram_configured']
        
        if critical_failures:
            print("❌ Environment validation failed:")
            for component in critical_failures:
                print(f"   ❌ {component}: Missing or invalid")
            print("\n💡 Tips:")
            print("   • Run --create-sample-broker-files to create sample CSVs")
            print("   • Ensure watchlist.csv exists and is up-to-date")
            print("   • Check portfolio builder module is available")
            print("   • Telegram is optional - use --no-alert if not configured")
            return 1
        
        # Rebalance durchführen
        logger.info(f"📊 Config: top_n={args.top_n}, min_score={args.min_score}, "
                   f"turnover_limit={args.turnover_limit}")
        
        result = run_rebalance(
            stocks_path=args.stocks_path,
            crypto_path=args.crypto_path,
            watchlist_path=args.watchlist_path,
            symbol_map_path=args.symbol_map_path,
            top_n=args.top_n,
            min_score=args.min_score,
            max_positions=args.max_positions,
            allow_crypto=args.allow_crypto,
            turnover_limit=args.turnover_limit,
            min_trade_pct=args.min_trade_pct,
            min_trade_value=args.min_trade_value,
            send_alert=not args.no_alert and not args.dry_run
        )
        
        # Ergebnis verarbeiten
        if "error" in result:
            print(f"❌ Rebalance failed: {result['error']}")
            print(f"📍 Stage: {result.get('stage', 'unknown')}")
            return 1
        
        # Zusammenfassung anzeigen
        print_rebalance_summary(result)
        
        # Snapshot speichern (optional)
        if not args.dry_run:
            try:
                import json
                from pathlib import Path
                
                # Snapshot-Verzeichnis erstellen
                Path(args.snapshot_path).parent.mkdir(parents=True, exist_ok=True)
                
                # Snapshot erstellen
                snapshot = {
                    'timestamp': result.get('meta', {}).get('stage', 'unknown'),
                    'holdings': result.get('holdings', {}),
                    'match_result': result.get('match_result', {}),
                    'plan': result.get('plan', {}),
                    'meta': result.get('meta', {})
                }
                
                with open(args.snapshot_path, 'w', encoding='utf-8') as f:
                    json.dump(snapshot, f, indent=2, ensure_ascii=False, default=str)
                
                print(f"Snapshot saved: {args.snapshot_path}")
                
            except Exception as e:
                logger.warning(f"Failed to save snapshot: {e}")
        
        # Dry-Run Hinweis
        if args.dry_run:
            print("\nDRY RUN - No changes saved or alerts sent")
        
        print("\nRebalance completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Rebalance interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Rebalance CLI failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
