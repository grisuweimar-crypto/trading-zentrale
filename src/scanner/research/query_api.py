from __future__ import annotations

import csv
import hashlib
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

MAX_SYMBOLS = 25
MAX_DAYS = 365
MAX_FIELDS = 25
WHITELIST_FIELDS = {
    "date",
    "symbol",
    "score",
    "rank",
    "rank_percentile",
    "r_code",
    "rs3m",
    "trend200",
    "cycle",
    "confidence",
    "confidence_label",
    "close",
    "currency",
    "sector",
    "cluster",
    "cluster_official",
    "name",
    "run_id",
    "snapshot_id",
    "as_of",
    "generated_at",
    "schema_version",
}


class HistoryQueryService:
    """Lean query façade over the canonical history_recent.csv source."""

    def __init__(self, source: str | Path):
        self.source = Path(source)

    @property
    def source_hash(self) -> str:
        if not self.source.exists():
            return ""
        digest = hashlib.sha256()
        digest.update(self.source.read_bytes())
        return digest.hexdigest()

    @property
    def etag(self) -> str:
        return self.source_hash

    def _read_rows(self) -> list[dict[str, str]]:
        if not self.source.exists():
            raise FileNotFoundError(f"History source missing: {self.source}")
        with self.source.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return list(reader)

    def _parse_date_value(self, raw: str | None):
        if not raw:
            return None
        try:
            return date.fromisoformat(str(raw))
        except ValueError:
            return None

    def query(
        self,
        *,
        symbol: str | None = None,
        symbols: str | Iterable[str] | None = None,
        from_: str | None = None,
        to: str | None = None,
        days: int | str | None = None,
        fields: str | Iterable[str] | None = None,
    ) -> dict:
        if symbol is None and symbols is None and from_ is None and to is None and days is None and fields is None:
            return {"error": "empty_query", "schema_version": "history_query_v1"}

        try:
            rows = self._read_rows()
        except FileNotFoundError:
            return {"error": "source_missing", "schema_version": "history_query_v1"}

        if fields is not None:
            field_list = [item.strip() for item in (fields.split(",") if isinstance(fields, str) else list(fields)) if item and item.strip()]
            invalid = [name for name in field_list if name not in WHITELIST_FIELDS]
            if invalid:
                return {"error": "invalid_fields", "schema_version": "history_query_v1", "invalid": invalid[:10]}
            if len(field_list) > MAX_FIELDS:
                return {"error": "request_too_large", "schema_version": "history_query_v1"}
        else:
            field_list = list(rows[0].keys()) if rows else []

        requested_symbols = []
        raw_symbols = symbols if symbols is not None else []
        if isinstance(raw_symbols, str):
            raw_symbols = [part.strip() for part in raw_symbols.split(",") if part and part.strip()]
        elif raw_symbols is not None:
            raw_symbols = [str(item).strip() for item in raw_symbols if str(item).strip()]
        if symbol:
            requested_symbols.append(str(symbol).strip())
        requested_symbols.extend(raw_symbols)
        requested_symbols = [value for value in dict.fromkeys(requested_symbols) if value]
        if len(requested_symbols) > MAX_SYMBOLS:
            return {"error": "request_too_large", "schema_version": "history_query_v1", "limit": MAX_SYMBOLS}

        if days is not None:
            try:
                days_value = int(days)
            except (TypeError, ValueError):
                return {"error": "invalid_days", "schema_version": "history_query_v1"}
            if days_value <= 0 or days_value > MAX_DAYS:
                return {"error": "request_too_large", "schema_version": "history_query_v1", "limit": MAX_DAYS}
        else:
            days_value = None

        if from_ is not None:
            try:
                date.fromisoformat(str(from_))
            except ValueError:
                return {"error": "invalid_date_range", "schema_version": "history_query_v1"}
        if to is not None:
            try:
                date.fromisoformat(str(to))
            except ValueError:
                return {"error": "invalid_date_range", "schema_version": "history_query_v1"}

        all_dates = []
        for row in rows:
            parsed = self._parse_date_value(row.get("date") or row.get("as_of") or "")
            if parsed is not None:
                all_dates.append(parsed)
        anchor_day = date.fromisoformat(str(to)) if to is not None else (max(all_dates) if all_dates else datetime.now().date())

        filtered = []
        for row in rows:
            symbol_name = row.get("symbol", "")
            if requested_symbols and symbol_name not in requested_symbols:
                continue
            row_date = self._parse_date_value(row.get("date") or row.get("as_of") or "")
            if row_date is None:
                continue
            if from_ is not None and row_date < date.fromisoformat(str(from_)):
                continue
            if to is not None and row_date > date.fromisoformat(str(to)):
                continue
            if days_value is not None and (anchor_day - row_date).days > days_value:
                continue
            filtered.append(row)

        if not filtered:
            return {
                "schema_version": "history_query_v1",
                "source": str(self.source),
                "as_of": anchor_day.isoformat(),
                "count": 0,
                "data": [],
                "etag": self.source_hash,
            }

        filtered.sort(key=lambda row: row.get("date") or row.get("as_of") or "")
        selected = []
        for row in filtered:
            picked = {key: row.get(key, "") for key in field_list if key in row}
            if picked:
                selected.append(picked)

        return {
            "schema_version": "history_query_v1",
            "source": str(self.source),
            "as_of": max((row.get("date") or row.get("as_of") or "") for row in filtered),
            "count": len(selected),
            "data": selected,
            "etag": self.source_hash,
        }
