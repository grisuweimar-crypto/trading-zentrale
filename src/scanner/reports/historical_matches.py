"""Cross-universe matches from stored scanner fields and separate observed prices.

See docs/historical_matches.md for interval boundaries and missing-data rules.
No scanner field is reconstructed, carried forward, or inferred from prices.
"""
from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from datetime import date
import math
from statistics import median

from scanner.reports.research_views import observed
from scanner.data.price_history import validated_rows


MIN_MATCHES = 20
COOLDOWN_TRADING_DAYS = 5
HORIZONS = (5, 10, 20, 40)
# Rank intervals include their upper bound; metric intervals include their lower
# bound. Values are fractions (0.15 = 15%), never inferred percentage units.
BUCKETS = {
    "rank_percentile": {
        "boundaries": [0.10, 0.20, 0.40, 0.60, 0.80, 0.90],
        "labels": ["top10", "10..20", "20..40", "40..60", "60..80", "80..90", "bottom10"],
        "boundary_side": "upper_inclusive", "valid_range": [0, 1],
    },
    "rank_percentile_coarse": {
        "boundaries": [0.20, 0.60],
        "labels": ["top20", "20..60", "bottom40"],
        "boundary_side": "upper_inclusive", "valid_range": [0, 1],
    },
    "rs3m": {
        "boundaries": [-0.20, 0.0, 0.15, 0.30],
        "labels": ["<-20%", "[-20%,0%)", "[0%,15%)", "[15%,30%)", ">=30%"],
        "boundary_side": "lower_inclusive",
    },
    "trend200": {
        "boundaries": [-0.20, 0.0, 0.20, 0.40],
        "labels": ["<-20%", "[-20%,0%)", "[0%,20%)", "[20%,40%)", ">=40%"],
        "boundary_side": "lower_inclusive",
    },
}


@dataclass(frozen=True)
class MatchPolicy:
    min_matches: int = MIN_MATCHES
    cooldown_trading_days: int = COOLDOWN_TRADING_DAYS

    def __post_init__(self):
        if (type(self.min_matches) is not int or self.min_matches < 1
                or type(self.cooldown_trading_days) is not int or self.cooldown_trading_days < 0):
            raise ValueError("min_matches must be a positive integer; cooldown must be a nonnegative integer")


def finite_number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def parse_date(value):
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def bucket(field, value):
    number = finite_number(value)
    if number is None:
        return None
    definition = BUCKETS[field]
    if "valid_range" in definition and not 0 <= number <= 1:
        return None
    locate = bisect_left if definition["boundary_side"] == "upper_inclusive" else bisect_right
    return definition["labels"][locate(definition["boundaries"], number)]


def features(row):
    # In particular, do NOT calculate percentile from score, rank/universe_size,
    # today's ordering, or any other row. The stored percentile is mandatory.
    code = str(row.get("r_code") or "").strip()
    return {
        "rank_percentile_bucket": bucket("rank_percentile", row.get("rank_percentile")),
        "rank_percentile_coarse_bucket": bucket("rank_percentile_coarse", row.get("rank_percentile")),
        "r_code": code if code in {"R0", "R1", "R2", "R3", "R4", "R5"} else None,
        "rs3m_bucket": bucket("rs3m", row.get("rs3m")),
        "trend200_bucket": bucket("trend200", row.get("trend200")),
    }


def _historical_rank_percentiles(rows):
    """Derive missing historical ranks from stored scanner scores only."""
    groups = {}
    for index, row in enumerate(rows):
        group = (str(row.get("date") or row.get("as_of") or ""), row.get("run_id", ""))
        score = finite_number(row.get("score"))
        if score is not None:
            groups.setdefault(group, []).append((index, score))
    result = {}
    for values in groups.values():
        ordered = sorted((score for _, score in values), reverse=True)
        for index, score in values:
            result[index] = (ordered.index(score) + 1) / len(ordered)
    return result


class PriceSessions:
    """One finite positive close per actual symbol/date, never scanner run dates."""

    def __init__(self, rows):
        valid, _ = validated_rows(rows)
        self.closes = {}
        for row in valid:
            self.closes.setdefault(row["symbol"], {})[parse_date(row["date"])] = float(row["close"])
        self.dates = {symbol: sorted(values) for symbol, values in self.closes.items()}

    def elapsed(self, symbol, start, end):
        days = self.dates.get(symbol, [])
        return bisect_right(days, end) - bisect_right(days, start)

    def forward_return(self, symbol, start, horizon, as_of):
        days = self.dates.get(symbol, [])
        index = bisect_left(days, start)
        target = index + horizon
        if index >= len(days) or days[index] != start or target >= len(days) or days[target] > as_of:
            return None
        closes = self.closes[symbol]
        result = closes[days[target]] / closes[start] - 1
        return result if math.isfinite(result) else None


def statistics(values):
    positive = sum(value > 0 for value in values)
    return {"N": len(values), "median_return": median(values) if values else None,
            "positive_count": positive, "positive_rate": positive / len(values) if values else None}


class HistoricalMatcher:
    def __init__(self, history_rows, price_rows=(), policy=None):
        self.policy = policy or MatchPolicy()
        self.prices = PriceSessions(price_rows)
        # The first stored scanner observation of each symbol/date wins intact.
        # Never merge fields from separate intraday runs or fill missing ranks.
        seen, self.events = set(), []
        derived_ranks = _historical_rank_percentiles(history_rows)
        for index, row in enumerate(history_rows):
            symbol = str(row.get("symbol") or "").strip()
            day = parse_date(row.get("date"))
            if not symbol or day is None or not observed(row) or (symbol, day) in seen:
                continue
            seen.add((symbol, day))
            enriched = row
            if not str(row.get("rank_percentile") or "").strip() and index in derived_ranks:
                enriched = dict(row, rank_percentile=str(derived_ranks[index]))
            self.events.append((day, symbol, features(enriched)))
        self.events.sort(key=lambda event: (event[0], event[1]))
        self.cache = {}

    def summary(self, current_row):
        current = features(current_row)
        as_of = parse_date(current_row.get("date") or current_row.get("as_of"))
        key = (as_of, *current.values())
        if key in self.cache:
            return self.cache[key]
        kept, selected, conditions = [], "none", {}
        attempts = {}
        for level in (1, 2, 3):
            fields = ["rank_percentile_coarse_bucket" if level == 3 else "rank_percentile_bucket",
                      "rs3m_bucket", "trend200_bucket"]
            if level == 1:
                fields.insert(1, "r_code")
            wanted = {field: current[field] for field in fields}
            events, last = [], {}
            if as_of is not None and all(value is not None for value in wanted.values()):
                for day, symbol, state in self.events:
                    if day >= as_of or any(state[field] != value for field, value in wanted.items()):
                        continue
                    if symbol in last and self.prices.elapsed(symbol, last[symbol], day) < self.policy.cooldown_trading_days:
                        continue
                    events.append((day, symbol))
                    last[symbol] = day
            attempts[f"level_{level}"] = len(events)
            if events:
                kept, selected, conditions = events, f"level_{level}", wanted
            if len(events) >= self.policy.min_matches:
                break
        # With too few events even at L3, report the actual L3 sample explicitly.
        values = {horizon: [] for horizon in HORIZONS}
        for day, symbol in kept:
            for horizon in HORIZONS:
                value = self.prices.forward_return(symbol, day, horizon, as_of)
                if value is not None:
                    values[horizon].append(value)
        result = {
            "filter_id": selected,
            "filter_description": "; ".join(f"{field}={value}" for field, value in conditions.items())
                if kept else "No eligible stored historical scanner events for L1/L2/L3",
            "conditions": conditions,
            "cooldown_trading_days": self.policy.cooldown_trading_days,
            "min_matches": self.policy.min_matches,
            "sufficient_matches": len(kept) >= self.policy.min_matches,
            "N": len(kept),
            "evaluated_levels": attempts,
            **{f"forward_{horizon}t": statistics(values[horizon]) for horizon in HORIZONS},
        }
        self.cache[key] = result
        return result


def method_metadata(policy):
    return {
        "version": "cross_universe_v1", **asdict(policy), "buckets": BUCKETS,
        "event_source": "artifacts/research/history_recent.csv",
        "session_source": "artifacts/research/price_backfill.csv",
        "universe": "all stored scanner symbols",
        "level_selection": "independent event N before outcome availability",
        "duplicate_events": "first stored observation per symbol/date, no field merging",
        "missing_sessions": "no inferred sessions; first event retained, subsequent events require observed cooldown",
        "outcomes": "exact event-day close to h-th later valid price session, at or before snapshot as_of",
    }
