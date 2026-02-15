import sys
from pathlib import Path

# src/ zum Importpfad hinzufügen
sys.path.insert(0, str(Path(__file__).parent / "src"))

from scanner.app.run_daily import main

if __name__ == "__main__":
    main()
