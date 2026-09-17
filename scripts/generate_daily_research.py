"""Generate and validate the compact daily research artifact."""
import argparse
import json
from pathlib import Path

from scanner.reports.daily_research import generate_daily_research, validate_daily_research


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    payload = generate_daily_research(args.root)
    validate_daily_research(args.root, payload)
    print(json.dumps({
        "as_of": payload["as_of"],
        "snapshot_id": payload["snapshot_id"],
        "symbol_count": payload["universe_size"],
    }, indent=2))


if __name__ == "__main__":
    main()