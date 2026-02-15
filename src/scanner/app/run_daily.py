from scanner.app.build_watchlist import build_watchlist_outputs
from scanner._version import __version__, __build__

import scanner
import scanner.app.build_watchlist as bw

def main():
    print(f"Scanner_vNext {__version__} (build {__build__})")
    print(f"scanner package -> {scanner.__file__}")
    print(f"build_watchlist  -> {bw.__file__}")
    build_watchlist_outputs()

if __name__ == "__main__":
    main()
