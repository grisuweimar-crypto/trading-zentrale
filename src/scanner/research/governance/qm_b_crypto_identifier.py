"""QM-B diagnostic for historical crypto identifier namespace migration.

Research-only. This module groups preserved scanner-observed crypto identifiers by
base token (for example CRYPTO:BTC, BTC-USD and BTC-EUR -> BTC) to expose identifier
lineage candidates. Grouping is deliberately weaker than stable instrument identity:
quote-pair equivalence, listing identity and historical membership are not promoted.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
from pathlib import Path
import re
from typing import Any, Mapping

from scanner.research.governance.qm_b_observed_membership import (
    build_candidates,
)


SCHEMA_VERSION = "qm_b_crypto_identifier_lineage_v1"
_ALLOWED_QUOTES = {"USD", "EUR", "USDT", "USDC"}
_BASE_RE = re.compile(r"^[A-Z0-9]{2,12}$")


class CryptoIdentifierError(ValueError):
    """Raised when the crypto identifier diagnostic cannot be evaluated safely."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def parse_crypto_identifier(value: Any) -> dict[str, str] | None:
    """Parse only conservative project crypto identifier forms.

    Accepted forms:
    - CRYPTO:<BASE>
    - <BASE>-USD/EUR/USDT/USDC

    A parse result is a lineage *candidate key*, not proof of instrument identity.
    """
    symbol = _clean(value).upper()
    if not symbol:
        return None
    if symbol.startswith("CRYPTO:"):
        base = symbol.split(":", 1)[1]
        if _BASE_RE.fullmatch(base):
            return {"base": base, "style": "INTERNAL_BASE", "quote": ""}
        return None
    if "-" not in symbol:
        return None
    base, quote = symbol.rsplit("-", 1)
    if _BASE_RE.fullmatch(base) and quote in _ALLOWED_QUOTES:
        return {"base": base, "style": "QUOTE_PAIR", "quote": quote}
    return None


def _current_crypto_symbols(path: Path) -> dict[str, list[str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError as exc:
        raise CryptoIdentifierError(f"current_universe_unreadable:{path}") from exc

    result: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        active = _clean(row.get("active")).lower()
        if active not in {"1", "true", "yes", "y"}:
            continue
        if _clean(row.get("asset_type")).lower() != "crypto":
            continue
        symbol = _clean(row.get("symbol")).upper()
        parsed = parse_crypto_identifier(symbol)
        if parsed:
            result[parsed["base"]].add(symbol)
    return {base: sorted(symbols) for base, symbols in sorted(result.items())}


def build_crypto_identifier_timeline(
    history_path: str | Path,
    *,
    current_universe_path: str | Path | None = None,
    source_path_label: str | None = None,
) -> dict[str, Any]:
    """Build a scanner-observation-only timeline of crypto identifier forms."""
    history = Path(history_path)
    ledger = build_candidates(history, source_path_label=source_path_label or history.as_posix())
    scanner_rows = [
        row
        for row in ledger["candidates"]
        if row.get("evidence_class") == "SCANNER_OBSERVED"
        and row.get("membership_claim") == "OBSERVED_IN_SCANNER"
    ]

    by_base: dict[str, dict[str, list[Mapping[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in scanner_rows:
        symbol = _clean(row.get("observed_symbol")).upper()
        parsed = parse_crypto_identifier(symbol)
        if parsed:
            by_base[parsed["base"]][symbol].append(row)

    current = _current_crypto_symbols(Path(current_universe_path)) if current_universe_path else {}
    bases = sorted(set(by_base) | set(current))
    base_rows: list[dict[str, Any]] = []

    for base in bases:
        identifier_rows: list[dict[str, Any]] = []
        internal_dates: set[str] = set()
        quote_dates: set[str] = set()
        for identifier, rows in sorted(by_base.get(base, {}).items()):
            parsed = parse_crypto_identifier(identifier)
            assert parsed is not None
            dates = sorted(_clean(row.get("as_of_date")) for row in rows)
            names = sorted({_clean(row.get("name")) for row in rows if _clean(row.get("name"))})
            currencies = sorted({_clean(row.get("currency")) for row in rows if _clean(row.get("currency"))})
            run_ids = sorted({_clean(row.get("run_id")) for row in rows if _clean(row.get("run_id"))})
            identifier_rows.append(
                {
                    "identifier": identifier,
                    "style": parsed["style"],
                    "quote": parsed["quote"] or None,
                    "first_seen": dates[0],
                    "last_seen": dates[-1],
                    "row_count": len(rows),
                    "names": names,
                    "currencies": currencies,
                    "run_id_count": len(run_ids),
                }
            )
            target = internal_dates if parsed["style"] == "INTERNAL_BASE" else quote_dates
            target.update(dates)

        all_dates = sorted(internal_dates | quote_dates)
        internal_first = min(internal_dates) if internal_dates else None
        internal_last = max(internal_dates) if internal_dates else None
        quote_first = min(quote_dates) if quote_dates else None
        quote_last = max(quote_dates) if quote_dates else None
        overlap = sorted(internal_dates & quote_dates)
        historical_identifiers = [row["identifier"] for row in identifier_rows]
        current_symbols = current.get(base, [])
        candidate = bool(historical_identifiers and current_symbols)

        base_rows.append(
            {
                "base": base,
                "lineage_status": "CANDIDATE" if candidate else "UNRESOLVED",
                "historical_first_seen": all_dates[0] if all_dates else None,
                "historical_last_seen": all_dates[-1] if all_dates else None,
                "internal_identifier_first_seen": internal_first,
                "internal_identifier_last_seen": internal_last,
                "quote_pair_first_seen": quote_first,
                "quote_pair_last_seen": quote_last,
                "overlap_date_count": len(overlap),
                "overlap_dates": overlap,
                "historical_identifiers": historical_identifiers,
                "current_crypto_symbols": current_symbols,
                "identifier_details": identifier_rows,
                "stable_identity_verified": False,
                "quote_pair_equivalence_verified": False,
            }
        )

    style_counts = Counter()
    for row in base_rows:
        for detail in row["identifier_details"]:
            style_counts[detail["style"]] += detail["row_count"]

    return {
        "schema_version": SCHEMA_VERSION,
        "source_path": ledger.get("source_path"),
        "source_file_sha256": ledger.get("source_file_sha256"),
        "research_only": True,
        "productive_integration_enabled": False,
        "stable_identity_verified": False,
        "quote_pair_equivalence_verified": False,
        "absence_interpreted_as_out_of_scope": False,
        "base_token_is_identity": False,
        "interpretation": (
            "Shared base tokens and temporal transitions are identifier-lineage candidates only; "
            "they do not prove stable instrument identity, quote-pair equivalence or historical universe membership."
        ),
        "scanner_crypto_row_count": sum(style_counts.values()),
        "style_row_counts": dict(sorted(style_counts.items())),
        "base_count": len(base_rows),
        "bases": base_rows,
    }
