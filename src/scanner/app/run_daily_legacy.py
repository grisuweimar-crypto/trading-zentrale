"""
Daily Runner Script für Scanner Projekt
Führt main.py und rebalance_run.py aus mit Logging und optionalen Telegram Alerts

Usage:
- python scripts/run_daily.py
- python scripts/run_daily.py --skip_rebalance
- python scripts/run_daily.py --skip_scan

Environment Variables (optional):
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

Windows Task Scheduler Setup:
1. Task erstellen: "Scanner Daily Run"
2. Trigger: Täglich um 07:15
3. Aktion: 
   - Programm: C:\\Users\\CW\\OneDrive\\Desktop\\Scanner\\.venv\\Scripts\\python.exe
   - Argumente: scripts\\run_daily.py
   - Starten in: C:\\Users\\CW\\OneDrive\\Desktop\\Scanner
4. Bedingungen: "Unabhängig von der Benutzeranmeldung ausführen"
5. Einstellungen: "Aufgabe so schnell wie möglich nach einem verpassten Start ausführen"
"""
import subprocess
import sys
import os
import argparse
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from utils.logging_setup import setup_logging
    logger = setup_logging()
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    logger = logging.getLogger("scanner")

def run_script(script_name, description):
    """
    Führt ein Python Script aus und loggt das Ergebnis
    
    Args:
        script_name (str): Name des Scripts (z.B. "main.py")
        description (str): Beschreibung für Logs
    
    Returns:
        tuple: (success: bool, exit_code: int)
    """
    logger.info(f"=== START {description.upper()} ===")
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=False
        )
        
        # Log stdout
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    logger.info(f"{script_name}: {line}")
        
        # Log stderr
        if result.stderr:
            for line in result.stderr.strip().split('\n'):
                if line.strip():
                    logger.error(f"{script_name}: {line}")
        
        success = result.returncode == 0
        logger.info(f"=== END {description.upper()} - Exit Code: {result.returncode} ===")
        
        return success, result.returncode
        
    except Exception as e:
        logger.error(f"Failed to run {script_name}: {e}")
        return False, -1

def send_telegram_message(message):
    """
    Sende Nachricht an Telegram wenn konfiguriert
    
    Args:
        message (str): Nachricht zum Senden
    """
    try:
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not bot_token or not chat_id:
            logger.info("Telegram disabled: Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
            return False
        
        # Import telegram module if available
        try:
            sys.path.insert(0, str(project_root / "alerts"))
            import telegram
            return telegram.send_message(message)
        except ImportError:
            logger.warning("Telegram module not found in alerts/telegram.py")
            return False
            
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")
        return False

def main():
    """Main runner function"""
    parser = argparse.ArgumentParser(description="Daily Scanner Runner")
    parser.add_argument('--skip_rebalance', action='store_true', help='Skip rebalance run')
    parser.add_argument('--skip_scan', action='store_true', help='Skip main scan')
    args = parser.parse_args()
    
    logger.info("=== DAILY RUNNER START ===")
    
    # Track overall success
    scan_success = True
    rebalance_success = True
    
    # Run main scan
    if not args.skip_scan:
        scan_success, scan_exit_code = run_script("main.py", "Main Scan")
        
        if not scan_success:
            logger.warning(f"Main scan failed with exit code {scan_exit_code}")
            # Continue with rebalance even if scan fails
        else:
            logger.info("Main scan completed successfully")
    
    # Run rebalance
    if not args.skip_rebalance:
        rebalance_success, rebalance_exit_code = run_script("rebalance_run.py", "Rebalance")
        
        if not rebalance_success:
            logger.error(f"Rebalance failed with exit code {rebalance_exit_code}")
        else:
            logger.info("Rebalance completed successfully")
    
    # Send summary to Telegram if configured
    overall_success = scan_success and rebalance_success
    status = "✅ SUCCESS" if overall_success else "❌ PARTIAL FAILURE"
    
    # Get current time without subprocess
    from datetime import datetime
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    summary = f"""
📊 Scanner Daily Run {status}

Main Scan: {"✅ OK" if args.skip_scan else ("✅ OK" if scan_success else "❌ FAILED")}
Rebalance: {"✅ OK" if args.skip_rebalance else ("✅ OK" if rebalance_success else "❌ FAILED")}

Time: {current_time}
"""
    
    send_telegram_message(summary)
    
    logger.info(f"=== DAILY RUNNER END - Overall: {status} ===")
    
    # Exit with appropriate code
    sys.exit(0 if overall_success else 1)

if __name__ == "__main__":
    main()
