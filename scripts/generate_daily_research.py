"""Generate and validate the compact daily research artifact."""
import argparse
import json
from pathlib import Path

from scanner.reports.daily_research import generate_daily_research, validate_daily_research
from scanner.reports.historical_matches import MatchPolicy, MIN_MATCHES, COOLDOWN_TRADING_DAYS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--min-matches", type=int, default=MIN_MATCHES)
    parser.add_argument("--cooldown-trading-days", type=int, default=COOLDOWN_TRADING_DAYS)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    payload = validate_daily_research(args.root) if args.validate_only else generate_daily_research(
        args.root, match_policy=MatchPolicy(args.min_matches, args.cooldown_trading_days))
    validate_daily_research(args.root, payload)
    print(json.dumps({
        "as_of": payload["as_of"],
        "snapshot_id": payload["snapshot_id"],
        "symbol_count": payload["universe_size"],
    }, indent=2))


if __name__ == "__main__":
    main()
