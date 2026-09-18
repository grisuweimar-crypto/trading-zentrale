"""Skip scheduled retries only after a published run from today's evening window."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
STATE = Path("artifacts/autorun_state.json")
BERLIN = ZoneInfo("Europe/Berlin")


def should_run(root: Path, event: str, now: datetime) -> bool:
    if event != "schedule":
        return True
    local_now = now.astimezone(BERLIN)
    window_start = local_now.replace(hour=17, minute=7, second=0, microsecond=0)
    try:
        state = json.loads((root / STATE).read_text(encoding="utf-8"))
        published = datetime.fromisoformat(state["published_at"])
        if published.tzinfo is None:
            return True
        return not (window_start <= published <= now)
    except (OSError, ValueError, KeyError, TypeError):
        return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--record", action="store_true")
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    if args.record:
        path = args.root / STATE
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "published_at": now.isoformat(),
            "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        }, indent=2) + "\n", encoding="utf-8")
        return
    run = should_run(args.root, os.environ.get("GITHUB_EVENT_NAME", ""), now)
    print("Abendscan erforderlich." if run else "Abendscan bereits publiziert; Nachholtermin uebersprungen.")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"run={'true' if run else 'false'}\n")


if __name__ == "__main__":
    main()
