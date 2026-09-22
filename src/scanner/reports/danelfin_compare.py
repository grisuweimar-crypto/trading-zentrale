"""Independent Danelfin-vs-scanner research harness.

This module is intentionally isolated from production scoring. It never changes
scanner scores or history. It reads historical scanner/price observations,
fetches Danelfin score history with an API key supplied only through the
environment, aligns data point-in-time, and measures forward returns/alpha.

No API key is ever written to disk or included in output.
"""
from __future__ import annotations

from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
import csv
import json
import math
import os
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

import requests


BASE_URL = "https://apirest.danelfin.com"
SCORE_FIELDS = ("aiscore", "fundamental", "technical", "sentiment", "low_risk")
HORIZONS = (5, 20, 40, 60)
PRICE_OBSERVATION_TYPES = {"market_data", "price_backfill"}
EU_SUFFIXES = (
    ".AS", ".BR", ".CO", ".DE", ".HE", ".L", ".LS", ".MC",
    ".MI", ".OL", ".PA", ".ST", ".SW", ".VI",
)


def finite_number(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def parse_day(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def infer_danelfin_market(symbol: str, currency: str | None = None) -> str | None:
    """Return Danelfin market for an obviously supported scanner symbol.

    We deliberately avoid guessing mappings for Canada, Hong Kong, Japan,
    Australia, Korea, crypto, etc. Those stay excluded until an explicit
    mapping is added.
    """
    ticker = str(symbol or "").strip().upper()
    if not ticker or "-" in ticker:
        return None
    if ticker.endswith(EU_SUFFIXES):
        return "europe"
    if "." not in ticker and (not currency or str(currency).upper() in {"", "USD"}):
        return "us"
    return None


def parse_ranking_history(payload: Mapping[str, Any], *, ticker: str, market: str) -> list[dict[str, Any]]:
    """Normalize /ranking?ticker=... response into ordered rows."""
    rows: list[dict[str, Any]] = []
    for day_text, scores in payload.items():
        day = parse_day(day_text)
        if day is None or not isinstance(scores, Mapping):
            continue
        row: dict[str, Any] = {
            "date": day.isoformat(),
            "ticker": ticker,
            "market": market,
        }
        useful = False
        for field in SCORE_FIELDS:
            value = finite_number(scores.get(field))
            if value is not None and 1 <= value <= 10:
                row[field] = value
                useful = True
            else:
                row[field] = None
        for field in ("proven_buy_signal", "proven_sell_signal", "buy_track_record", "sell_track_record"):
            if field in scores:
                row[field] = scores.get(field)
        if useful:
            rows.append(row)
    rows.sort(key=lambda item: item["date"])
    return rows


class DanelfinClient:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = BASE_URL,
        session: requests.Session | None = None,
        timeout: float = 30.0,
    ):
        if not str(api_key or "").strip():
            raise ValueError("Danelfin API key is required")
        self._api_key = str(api_key).strip()
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout

    @classmethod
    def from_env(cls, env_var: str = "DANELFIN_API_KEY", **kwargs):
        api_key = os.environ.get(env_var, "")
        if not api_key:
            raise RuntimeError(
                f"{env_var} is not set. Supply the key via the environment; "
                "do not put it in source code, CLI arguments, URLs, or output files."
            )
        return cls(api_key, **kwargs)

    def ranking_history(self, ticker: str, *, market: str = "us") -> list[dict[str, Any]]:
        params: dict[str, str] = {"ticker": ticker}
        if market == "europe":
            params["market"] = "europe"
        elif market != "us":
            raise ValueError(f"Unsupported Danelfin market: {market}")
        response = self.session.get(
            f"{self.base_url}/ranking",
            params=params,
            headers={"x-api-key": self._api_key},
            timeout=self.timeout,
        )
        if response.status_code == 429:
            raise RuntimeError("Danelfin API rate limit reached")
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"Danelfin API returned HTTP {response.status_code} for {ticker}"
            ) from exc
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise RuntimeError(f"Unexpected Danelfin response for {ticker}: expected object")
        return parse_ranking_history(payload, ticker=ticker, market=market)


def read_csv_rows(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _is_scanner_row(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("observation_type") or "") == "observed_scanner"
        and finite_number(row.get("score")) is not None
        and parse_day(row.get("date")) is not None
        and str(row.get("symbol") or "").strip() != ""
    )


def _is_price_row(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("observation_type") or "") in PRICE_OBSERVATION_TYPES
        and finite_number(row.get("close")) not in (None, 0)
        and parse_day(row.get("date")) is not None
        and str(row.get("symbol") or "").strip() != ""
    )


@dataclass
class PriceSeries:
    dates: list[date]
    closes: list[float]

    def index(self, day: date) -> int | None:
        idx = bisect_left(self.dates, day)
        return idx if idx < len(self.dates) and self.dates[idx] == day else None

    def target_day(self, day: date, horizon: int) -> date | None:
        idx = self.index(day)
        if idx is None:
            return None
        target = idx + horizon
        if target >= len(self.dates):
            return None
        return self.dates[target]

    def return_between(self, start: date, end: date) -> float | None:
        start_idx = self.index(start)
        end_idx = self.index(end)
        if start_idx is None or end_idx is None or end_idx <= start_idx:
            return None
        return self.closes[end_idx] / self.closes[start_idx] - 1.0

    def forward_return(self, day: date, horizon: int) -> float | None:
        target_day = self.target_day(day, horizon)
        return self.return_between(day, target_day) if target_day is not None else None


def build_price_series(rows: Iterable[Mapping[str, Any]]) -> dict[str, PriceSeries]:
    grouped: dict[str, dict[date, float]] = defaultdict(dict)
    conflicts: dict[str, set[date]] = defaultdict(set)
    for row in rows:
        if not _is_price_row(row):
            continue
        symbol = str(row["symbol"]).strip()
        day = parse_day(row["date"])
        close = finite_number(row["close"])
        assert day is not None and close is not None
        previous = grouped[symbol].get(day)
        if previous is not None and not math.isclose(previous, close, rel_tol=1e-12, abs_tol=1e-12):
            conflicts[symbol].add(day)
        else:
            grouped[symbol][day] = close
    result: dict[str, PriceSeries] = {}
    for symbol, values in grouped.items():
        for day in conflicts[symbol]:
            values.pop(day, None)
        days = sorted(values)
        result[symbol] = PriceSeries(days, [values[d] for d in days])
    return result


def build_scanner_series(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, dict[date, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        if not _is_scanner_row(row):
            continue
        symbol = str(row["symbol"]).strip()
        day = parse_day(row["date"])
        assert day is not None
        grouped[symbol].setdefault(day, dict(row))
    result: dict[str, list[dict[str, Any]]] = {}
    for symbol, values in grouped.items():
        result[symbol] = [
            {"_day": day, **values[day]}
            for day in sorted(values)
        ]
    return result


def scanner_asof(
    series: Sequence[Mapping[str, Any]],
    target: date,
    *,
    max_staleness_days: int = 3,
) -> Mapping[str, Any] | None:
    days = [row["_day"] for row in series]
    idx = bisect_left(days, target)
    if idx < len(days) and days[idx] == target:
        row = series[idx]
    elif idx > 0:
        row = series[idx - 1]
    else:
        return None
    age = (target - row["_day"]).days
    return row if 0 <= age <= max_staleness_days else None


def select_symbols(
    rows: Iterable[Mapping[str, Any]],
    *,
    limit: int = 30,
    markets: Sequence[str] = ("us",),
    min_scanner_observations: int = 5,
    min_price_sessions: int = 65,
) -> list[dict[str, str]]:
    scanner = build_scanner_series(rows)
    prices = build_price_series(rows)
    candidates: list[tuple[int, int, str, str]] = []
    for symbol, observations in scanner.items():
        latest = observations[-1]
        market = infer_danelfin_market(symbol, latest.get("currency"))
        if market not in markets:
            continue
        sessions = len(prices.get(symbol, PriceSeries([], [])).dates)
        if len(observations) < min_scanner_observations or sessions < min_price_sessions:
            continue
        candidates.append((len(observations), sessions, symbol, market))
    candidates.sort(key=lambda item: (-item[0], -item[1], item[2]))
    return [
        {"symbol": symbol, "ticker": symbol, "market": market}
        for _, _, symbol, market in candidates[:limit]
    ]


def _average_ranks(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    pos = 0
    while pos < len(order):
        end = pos + 1
        while end < len(order) and values[order[end]] == values[order[pos]]:
            end += 1
        avg = (pos + 1 + end) / 2.0
        for k in range(pos, end):
            ranks[order[k]] = avg
        pos = end
    return ranks


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    den = math.sqrt(sum(x * x for x in dx) * sum(y * y for y in dy))
    if den == 0:
        return None
    return sum(x * y for x, y in zip(dx, dy)) / den


def spearman(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    return _pearson(_average_ranks(xs), _average_ranks(ys))


def _stats(values: Sequence[float]) -> dict[str, Any]:
    vals = [v for v in values if v is not None and math.isfinite(v)]
    return {
        "N": len(vals),
        "positive_rate": sum(v > 0 for v in vals) / len(vals) if vals else None,
        "median": median(vals) if vals else None,
        "mean": sum(vals) / len(vals) if vals else None,
    }


def build_comparison_events(
    rows: Iterable[Mapping[str, Any]],
    danelfin_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    benchmark_symbol: str | None = "SPY",
    cooldown_sessions: int = 5,
    max_scanner_staleness_days: int = 3,
) -> list[dict[str, Any]]:
    rows = list(rows)
    scanner = build_scanner_series(rows)
    prices = build_price_series(rows)
    benchmark = prices.get(benchmark_symbol) if benchmark_symbol else None
    events: list[dict[str, Any]] = []

    for symbol, drows in sorted(danelfin_by_symbol.items()):
        sseries = scanner.get(symbol)
        pseries = prices.get(symbol)
        if not sseries or not pseries:
            continue
        last_kept_index: int | None = None
        for drow in sorted(drows, key=lambda row: str(row.get("date") or "")):
            day = parse_day(drow.get("date"))
            if day is None:
                continue
            price_index = pseries.index(day)
            if price_index is None:
                continue
            if last_kept_index is not None and price_index - last_kept_index < cooldown_sessions:
                continue
            snapshot = scanner_asof(
                sseries, day, max_staleness_days=max_scanner_staleness_days
            )
            if snapshot is None:
                continue
            scanner_score = finite_number(snapshot.get("score"))
            aiscore = finite_number(drow.get("aiscore"))
            if scanner_score is None or aiscore is None:
                continue

            event: dict[str, Any] = {
                "symbol": symbol,
                "date": day.isoformat(),
                "scanner_date": snapshot["_day"].isoformat(),
                "scanner_staleness_days": (day - snapshot["_day"]).days,
                "scanner_score": scanner_score,
                "rank_percentile": finite_number(snapshot.get("rank_percentile")),
                **{field: finite_number(drow.get(field)) for field in SCORE_FIELDS},
            }
            for horizon in HORIZONS:
                target_day = pseries.target_day(day, horizon)
                ret = (
                    pseries.return_between(day, target_day)
                    if target_day is not None
                    else None
                )
                benchmark_return = (
                    benchmark.return_between(day, target_day)
                    if benchmark is not None and target_day is not None
                    else None
                )
                event[f"target_date_{horizon}t"] = (
                    target_day.isoformat() if target_day is not None else None
                )
                event[f"return_{horizon}t"] = ret
                event[f"benchmark_return_{horizon}t"] = benchmark_return
                event[f"alpha_{horizon}t"] = (
                    ret - benchmark_return
                    if ret is not None and benchmark_return is not None
                    else None
                )
            events.append(event)
            last_kept_index = price_index
    return events


def summarize_events(
    events: Sequence[Mapping[str, Any]],
    *,
    danelfin_positive_min: float = 8.0,
    scanner_top_percentile: float = 0.20,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "event_count": len(events),
        "symbol_count": len({str(row["symbol"]) for row in events}),
        "signal_thresholds": {
            "danelfin_ai_score_min": danelfin_positive_min,
            "scanner_rank_percentile_max": scanner_top_percentile,
        },
        "horizons": {},
    }
    for horizon in HORIZONS:
        target = f"alpha_{horizon}t"
        if not any(finite_number(row.get(target)) is not None for row in events):
            target = f"return_{horizon}t"
        usable = [
            row for row in events
            if finite_number(row.get(target)) is not None
            and finite_number(row.get("scanner_score")) is not None
            and finite_number(row.get("aiscore")) is not None
        ]
        outcome = [float(row[target]) for row in usable]
        horizon_summary: dict[str, Any] = {
            "outcome": target,
            "N": len(usable),
            "scanner_spearman": spearman(
                [float(row["scanner_score"]) for row in usable], outcome
            ),
            "danelfin_ai_spearman": spearman(
                [float(row["aiscore"]) for row in usable], outcome
            ),
            "subscore_spearman": {},
        }
        for field in SCORE_FIELDS[1:]:
            filtered = [
                row for row in usable if finite_number(row.get(field)) is not None
            ]
            horizon_summary["subscore_spearman"][field] = spearman(
                [float(row[field]) for row in filtered],
                [float(row[target]) for row in filtered],
            )

        groups: dict[str, list[float]] = defaultdict(list)
        for row in usable:
            danelfin_positive = float(row["aiscore"]) >= danelfin_positive_min
            percentile = finite_number(row.get("rank_percentile"))
            if percentile is None:
                key = "scanner_rank_unknown"
            else:
                scanner_positive = percentile <= scanner_top_percentile
                if danelfin_positive and scanner_positive:
                    key = "both_positive"
                elif danelfin_positive:
                    key = "danelfin_only"
                elif scanner_positive:
                    key = "scanner_only"
                else:
                    key = "neither"
            groups[key].append(float(row[target]))
        group_order = (
            "both_positive",
            "danelfin_only",
            "scanner_only",
            "neither",
            "scanner_rank_unknown",
        )
        horizon_summary["agreement_groups"] = {
            key: _stats(groups.get(key, [])) for key in group_order
        }
        horizon_summary["agreement_rank_known_N"] = sum(
            len(groups.get(key, [])) for key in group_order[:-1]
        )
        horizon_summary["agreement_rank_unknown_N"] = len(
            groups.get("scanner_rank_unknown", [])
        )
        summary["horizons"][str(horizon)] = horizon_summary
    return summary


def fetch_histories(
    client: DanelfinClient,
    mappings: Sequence[Mapping[str, str]],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for mapping in mappings:
        symbol = mapping["symbol"]
        result[symbol] = client.ranking_history(
            mapping.get("ticker", symbol),
            market=mapping.get("market", "us"),
        )
    return result


def write_events_csv(events: Sequence[Mapping[str, Any]], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol", "date", "scanner_date", "scanner_staleness_days",
        "scanner_score", "rank_percentile", *SCORE_FIELDS,
    ]
    for horizon in HORIZONS:
        fieldnames.extend([
            f"target_date_{horizon}t",
            f"return_{horizon}t",
            f"benchmark_return_{horizon}t",
            f"alpha_{horizon}t",
        ])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(events)


def write_summary(summary: Mapping[str, Any], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
