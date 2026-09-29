"""QM-B point-in-time universe, identity, coverage and survivorship controls.

Research-only. This module validates and queries explicit historical identity and
universe records. It does not modify the productive scanner universe, scoring,
Decision Layer, portfolio state or orders.

The design is intentionally fail-closed: a current symbol, current universe row or
provider record is never back-projected into history merely because it exists now.
"""
from __future__ import annotations

import csv
from datetime import date, datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


BUNDLE_SCHEMA_VERSION = "qm_b_universe_integrity_bundle_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_universe_integrity_v1.json"


class QMBIntegrityError(ValueError):
    """Raised when a QM-B point-in-time integrity invariant is violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _parse_date(value: Any, *, field: str, allow_none: bool = False) -> date | None:
    if value is None or str(value).strip() == "":
        if allow_none:
            return None
        raise QMBIntegrityError(f"date_required:{field}")
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise QMBIntegrityError(f"date_invalid:{field}:{value}") from exc


def _validate_interval(valid_from: Any, valid_to: Any, *, prefix: str) -> tuple[date | None, date | None]:
    start = _parse_date(valid_from, field=f"{prefix}.valid_from", allow_none=True)
    end = _parse_date(valid_to, field=f"{prefix}.valid_to", allow_none=True)
    if start is not None and end is not None and end <= start:
        raise QMBIntegrityError(f"interval_invalid:{prefix}")
    return start, end


def _contains(start: date | None, end: date | None, when: date) -> bool:
    """Closed-open interval membership: [valid_from, valid_to)."""
    return (start is None or when >= start) and (end is None or when < end)


def _intervals_overlap(
    a_start: date | None,
    a_end: date | None,
    b_start: date | None,
    b_end: date | None,
) -> bool:
    left = a_start is None or b_end is None or a_start < b_end
    right = b_start is None or a_end is None or b_start < a_end
    return left and right


def _require_fields(row: Mapping[str, Any], fields: Sequence[str], *, collection: str, index: int) -> None:
    missing = [field for field in fields if field not in row]
    if missing:
        raise QMBIntegrityError(
            f"required_fields_missing:{collection}:{index}:" + ",".join(sorted(missing))
        )


def _require_nonblank(value: Any, *, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise QMBIntegrityError(f"value_required:{field}")
    return result


def _require_bool(value: Any, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise QMBIntegrityError(f"boolean_required:{field}")
    return value


def _normalized_identifier(identifier_type: str, identifier_value: Any) -> str:
    value = _require_nonblank(identifier_value, field="identifier_value")
    if identifier_type in {"SYMBOL", "YAHOO_SYMBOL", "ISIN", "CIK", "FIGI", "CUSIP", "SEDOL", "EXCHANGE_SYMBOL"}:
        return value.upper()
    return value


def load_qm_b_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QMBIntegrityError(f"qm_b_contract_unreadable:{contract_path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "qm_b_universe_integrity_v1":
        raise QMBIntegrityError("qm_b_contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise QMBIntegrityError("qm_b_contract_scope_invalid")
    return payload


class UniverseIntegrityBundle:
    """Validated immutable in-memory view of one QM-B research bundle."""

    def __init__(self, payload: Mapping[str, Any], *, contract_path: str | Path | None = None) -> None:
        self.contract = load_qm_b_contract(contract_path)
        if not isinstance(payload, Mapping):
            raise QMBIntegrityError("bundle_must_be_object")
        self.payload = json.loads(_canonical_json(dict(payload)))
        self._validate()
        self._build_indexes()

    @classmethod
    def from_path(cls, path: str | Path, *, contract_path: str | Path | None = None) -> "UniverseIntegrityBundle":
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise QMBIntegrityError(f"bundle_unreadable:{path}") from exc
        return cls(payload, contract_path=contract_path)

    def _collection(self, name: str) -> list[dict[str, Any]]:
        value = self.payload.get(name)
        if not isinstance(value, list):
            raise QMBIntegrityError(f"bundle_collection_must_be_list:{name}")
        rows: list[dict[str, Any]] = []
        for index, row in enumerate(value):
            if not isinstance(row, Mapping):
                raise QMBIntegrityError(f"bundle_row_must_be_object:{name}:{index}")
            rows.append(dict(row))
        return rows

    def _validate(self) -> None:
        if self.payload.get("schema_version") != BUNDLE_SCHEMA_VERSION:
            raise QMBIntegrityError("bundle_schema_invalid")
        _require_nonblank(self.payload.get("bundle_as_of"), field="bundle_as_of")
        _parse_date(self.payload.get("bundle_as_of"), field="bundle_as_of")
        created_at = _require_nonblank(self.payload.get("created_at"), field="created_at")
        try:
            datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise QMBIntegrityError("created_at_invalid") from exc
        if self.payload.get("historical_backfill_claim") is not False:
            raise QMBIntegrityError("historical_backfill_claim_must_be_false_unless_separately_validated")

        required_collections = self.contract["bundle"]["required_collections"]
        for name in required_collections:
            if name not in self.payload:
                raise QMBIntegrityError(f"bundle_collection_missing:{name}")

        self._validate_instruments(self._collection("instruments"))
        self._validate_aliases(self._collection("identifier_aliases"))
        self._validate_membership(self._collection("universe_membership"))
        self._validate_provider_coverage(self._collection("provider_coverage"))
        self._validate_outcomes(self._collection("outcome_availability"))
        self._validate_taxonomy(self._collection("taxonomy_assignments"))
        self._validate_declared_hashes()

    def _validate_instruments(self, rows: list[dict[str, Any]]) -> None:
        spec = self.contract["instrument_master"]
        allowed = set(spec["identity_status_values"])
        seen: set[str] = set()
        for i, row in enumerate(rows):
            _require_fields(row, spec["required_fields"], collection="instruments", index=i)
            instrument_id = _require_nonblank(row.get("instrument_id"), field=f"instruments[{i}].instrument_id")
            if instrument_id in seen:
                raise QMBIntegrityError(f"duplicate_instrument_id:{instrument_id}")
            seen.add(instrument_id)
            _require_nonblank(row.get("asset_type"), field=f"instruments[{i}].asset_type")
            _require_nonblank(row.get("source_id"), field=f"instruments[{i}].source_id")
            if str(row.get("identity_status")) not in allowed:
                raise QMBIntegrityError(f"identity_status_invalid:{instrument_id}")

    def _validate_aliases(self, rows: list[dict[str, Any]]) -> None:
        spec = self.contract["identifier_aliases"]
        required = spec["required_fields"]
        types = set(spec["identifier_type_values"])
        statuses = set(spec["status_values"])
        instruments = {row["instrument_id"]: row for row in self._collection("instruments")}
        parsed: list[tuple[dict[str, Any], date | None, date | None]] = []
        unique_exact: set[tuple[Any, ...]] = set()

        for i, row in enumerate(rows):
            _require_fields(row, required, collection="identifier_aliases", index=i)
            instrument_id = _require_nonblank(row.get("instrument_id"), field=f"identifier_aliases[{i}].instrument_id")
            if instrument_id not in instruments:
                raise QMBIntegrityError(f"alias_unknown_instrument:{instrument_id}")
            identifier_type = str(row.get("identifier_type") or "")
            if identifier_type not in types:
                raise QMBIntegrityError(f"identifier_type_invalid:{identifier_type}")
            normalized = _normalized_identifier(identifier_type, row.get("identifier_value"))
            status = str(row.get("status") or "")
            if status not in statuses:
                raise QMBIntegrityError(f"identifier_status_invalid:{status}")
            _require_nonblank(row.get("source_id"), field=f"identifier_aliases[{i}].source_id")
            _require_bool(row.get("pit_verified"), field=f"identifier_aliases[{i}].pit_verified")
            start, end = _validate_interval(row.get("valid_from"), row.get("valid_to"), prefix=f"identifier_aliases[{i}]")
            venue = str(row.get("listing_venue") or "").strip().upper()
            exact = (instrument_id, identifier_type, normalized, venue, start, end, status)
            if exact in unique_exact:
                raise QMBIntegrityError(f"duplicate_identifier_alias:{identifier_type}:{normalized}:{venue}")
            unique_exact.add(exact)
            parsed.append((row, start, end))

        for i, (left, left_start, left_end) in enumerate(parsed):
            left_type = str(left["identifier_type"])
            left_value = _normalized_identifier(left_type, left["identifier_value"])
            left_venue = str(left.get("listing_venue") or "").strip().upper()
            for right, right_start, right_end in parsed[i + 1 :]:
                right_type = str(right["identifier_type"])
                right_value = _normalized_identifier(right_type, right["identifier_value"])
                right_venue = str(right.get("listing_venue") or "").strip().upper()
                if (left_type, left_value, left_venue) != (right_type, right_value, right_venue):
                    continue
                if left["instrument_id"] == right["instrument_id"]:
                    continue
                if not _intervals_overlap(left_start, left_end, right_start, right_end):
                    continue
                if "CONFLICTING" not in {str(left["status"]), str(right["status"])}:
                    raise QMBIntegrityError(
                        f"identifier_overlap_across_instruments:{left_type}:{left_value}:{left_venue}"
                    )

        aliases_by_instrument: dict[str, set[str]] = {}
        for row in rows:
            if row["identifier_type"] in {"SYMBOL", "YAHOO_SYMBOL", "EXCHANGE_SYMBOL"}:
                aliases_by_instrument.setdefault(str(row["instrument_id"]), set()).add(
                    _normalized_identifier(str(row["identifier_type"]), row["identifier_value"])
                )
        for instrument_id, aliases in aliases_by_instrument.items():
            if instrument_id.upper() in aliases:
                raise QMBIntegrityError(f"instrument_id_must_not_be_symbol:{instrument_id}")

    @staticmethod
    def _matching_symbol_aliases(
        rows: list[dict[str, Any]], instrument_id: str, symbol: str, venue: str, when: date
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for row in rows:
            if str(row.get("instrument_id")) != instrument_id:
                continue
            if row.get("identifier_type") not in {"SYMBOL", "YAHOO_SYMBOL", "EXCHANGE_SYMBOL"}:
                continue
            if _normalized_identifier(str(row["identifier_type"]), row.get("identifier_value")) != symbol.upper():
                continue
            alias_venue = str(row.get("listing_venue") or "").strip().upper()
            if alias_venue and alias_venue != venue.upper():
                continue
            start, end = _validate_interval(row.get("valid_from"), row.get("valid_to"), prefix="identifier_alias")
            if _contains(start, end, when) and row.get("pit_verified") is True and row.get("status") == "KNOWN":
                result.append(row)
        return result

    def _validate_membership(self, rows: list[dict[str, Any]]) -> None:
        spec = self.contract["universe_membership"]
        required = spec["required_fields"]
        memberships = set(spec["membership_status_values"])
        listings = set(spec["listing_status_values"])
        investability = set(spec["investability_status_values"])
        instruments = {row["instrument_id"]: row for row in self._collection("instruments")}
        aliases = self._collection("identifier_aliases")
        seen: set[tuple[str, str, str, str]] = set()

        for i, row in enumerate(rows):
            _require_fields(row, required, collection="universe_membership", index=i)
            as_of = str(row.get("as_of_date") or "")
            when = _parse_date(as_of, field=f"universe_membership[{i}].as_of_date")
            instrument_id = _require_nonblank(row.get("instrument_id"), field=f"universe_membership[{i}].instrument_id")
            if instrument_id not in instruments:
                raise QMBIntegrityError(f"membership_unknown_instrument:{instrument_id}")
            symbol = _require_nonblank(row.get("symbol"), field=f"universe_membership[{i}].symbol").upper()
            venue = _require_nonblank(row.get("listing_venue"), field=f"universe_membership[{i}].listing_venue").upper()
            membership = str(row.get("membership_status") or "")
            listing = str(row.get("listing_status") or "")
            inv = str(row.get("investability_status") or "")
            if membership not in memberships:
                raise QMBIntegrityError(f"membership_status_invalid:{membership}")
            if listing not in listings:
                raise QMBIntegrityError(f"listing_status_invalid:{listing}")
            if inv not in investability:
                raise QMBIntegrityError(f"investability_status_invalid:{inv}")
            _require_bool(row.get("scanner_observable"), field=f"universe_membership[{i}].scanner_observable")
            pit_verified = _require_bool(row.get("pit_verified"), field=f"universe_membership[{i}].pit_verified")
            if not isinstance(row.get("reason_codes"), list):
                raise QMBIntegrityError(f"reason_codes_must_be_list:universe_membership:{i}")
            _require_nonblank(row.get("source_id"), field=f"universe_membership[{i}].source_id")
            key = (as_of, instrument_id, symbol, venue)
            if key in seen:
                raise QMBIntegrityError(f"duplicate_membership_row:{'|'.join(key)}")
            seen.add(key)

            listing_date = _parse_date(
                row.get("listing_date"), field=f"universe_membership[{i}].listing_date", allow_none=True
            )
            delisting_date = _parse_date(
                row.get("delisting_date"), field=f"universe_membership[{i}].delisting_date", allow_none=True
            )
            if listing_date is not None and delisting_date is not None and delisting_date < listing_date:
                raise QMBIntegrityError(f"listing_lifecycle_invalid:{instrument_id}")
            if membership == "NOT_YET_LISTED" and listing != "NOT_YET_LISTED":
                raise QMBIntegrityError(f"membership_listing_inconsistent:{instrument_id}:{as_of}")
            if membership == "DELISTED" and listing != "DELISTED":
                raise QMBIntegrityError(f"membership_listing_inconsistent:{instrument_id}:{as_of}")
            if membership == "IN_SCOPE" and listing in {"NOT_YET_LISTED", "DELISTED"}:
                raise QMBIntegrityError(f"in_scope_listing_impossible:{instrument_id}:{as_of}")
            if listing_date is not None and when < listing_date and membership not in {"NOT_YET_LISTED", "UNKNOWN"}:
                raise QMBIntegrityError(f"membership_before_listing_date:{instrument_id}:{as_of}")
            if delisting_date is not None and when >= delisting_date and membership == "IN_SCOPE":
                raise QMBIntegrityError(f"in_scope_on_or_after_delisting_date:{instrument_id}:{as_of}")
            if membership == "DELISTED" and delisting_date is not None and when < delisting_date:
                raise QMBIntegrityError(f"delisted_before_delisting_date:{instrument_id}:{as_of}")

            if membership == "IN_SCOPE" and pit_verified:
                identity_status = str(instruments[instrument_id].get("identity_status"))
                if identity_status != "VERIFIED":
                    raise QMBIntegrityError(f"pit_membership_requires_verified_identity:{instrument_id}:{as_of}")
                matches = self._matching_symbol_aliases(aliases, instrument_id, symbol, venue, when)
                if not matches:
                    raise QMBIntegrityError(f"pit_membership_requires_historical_symbol_alias:{instrument_id}:{as_of}")

    def _validate_provider_coverage(self, rows: list[dict[str, Any]]) -> None:
        spec = self.contract["provider_coverage"]
        statuses = set(spec["coverage_status_values"])
        instrument_ids = {str(row["instrument_id"]) for row in self._collection("instruments")}
        seen: set[tuple[str, str, str, str]] = set()
        for i, row in enumerate(rows):
            _require_fields(row, spec["required_fields"], collection="provider_coverage", index=i)
            as_of = str(row.get("as_of_date") or "")
            _parse_date(as_of, field=f"provider_coverage[{i}].as_of_date")
            instrument_id = _require_nonblank(row.get("instrument_id"), field=f"provider_coverage[{i}].instrument_id")
            if instrument_id not in instrument_ids:
                raise QMBIntegrityError(f"coverage_unknown_instrument:{instrument_id}")
            source = _require_nonblank(row.get("source_id"), field=f"provider_coverage[{i}].source_id")
            family = _require_nonblank(row.get("external_family"), field=f"provider_coverage[{i}].external_family")
            status = str(row.get("coverage_status") or "")
            if status not in statuses:
                raise QMBIntegrityError(f"coverage_status_invalid:{status}")
            _require_bool(row.get("pit_verified"), field=f"provider_coverage[{i}].pit_verified")
            if not isinstance(row.get("reason_codes"), list):
                raise QMBIntegrityError(f"reason_codes_must_be_list:provider_coverage:{i}")
            key = (as_of, instrument_id, source, family)
            if key in seen:
                raise QMBIntegrityError(f"duplicate_provider_coverage:{'|'.join(key)}")
            seen.add(key)

    def _validate_outcomes(self, rows: list[dict[str, Any]]) -> None:
        spec = self.contract["outcome_availability"]
        statuses = set(spec["availability_status_values"])
        instrument_ids = {str(row["instrument_id"]) for row in self._collection("instruments")}
        seen: set[tuple[str, str, str]] = set()
        for i, row in enumerate(rows):
            _require_fields(row, spec["required_fields"], collection="outcome_availability", index=i)
            as_of = str(row.get("as_of_date") or "")
            as_of_date = _parse_date(as_of, field=f"outcome_availability[{i}].as_of_date")
            instrument_id = _require_nonblank(row.get("instrument_id"), field=f"outcome_availability[{i}].instrument_id")
            if instrument_id not in instrument_ids:
                raise QMBIntegrityError(f"outcome_unknown_instrument:{instrument_id}")
            outcome_id = _require_nonblank(row.get("outcome_id"), field=f"outcome_availability[{i}].outcome_id")
            _require_nonblank(row.get("label_definition_hash"), field=f"outcome_availability[{i}].label_definition_hash")
            status = str(row.get("availability_status") or "")
            if status not in statuses:
                raise QMBIntegrityError(f"outcome_status_invalid:{status}")
            available_from = _parse_date(
                row.get("available_from"), field=f"outcome_availability[{i}].available_from", allow_none=True
            )
            _require_nonblank(row.get("source_id"), field=f"outcome_availability[{i}].source_id")
            pit_verified = _require_bool(row.get("pit_verified"), field=f"outcome_availability[{i}].pit_verified")
            if not isinstance(row.get("reason_codes"), list):
                raise QMBIntegrityError(f"reason_codes_must_be_list:outcome_availability:{i}")
            if status == "AVAILABLE":
                if available_from is None:
                    raise QMBIntegrityError(f"available_outcome_requires_available_from:{instrument_id}:{outcome_id}")
                if available_from > as_of_date:
                    raise QMBIntegrityError(f"outcome_not_available_as_of_claim:{instrument_id}:{outcome_id}")
                if not pit_verified:
                    raise QMBIntegrityError(f"available_outcome_requires_pit_verification:{instrument_id}:{outcome_id}")
            key = (as_of, instrument_id, outcome_id)
            if key in seen:
                raise QMBIntegrityError(f"duplicate_outcome_availability:{'|'.join(key)}")
            seen.add(key)

    def _validate_taxonomy(self, rows: list[dict[str, Any]]) -> None:
        spec = self.contract["taxonomy_assignments"]
        types = set(spec["taxonomy_type_values"])
        statuses = set(spec["status_values"])
        instrument_ids = {str(row["instrument_id"]) for row in self._collection("instruments")}
        parsed: list[tuple[dict[str, Any], date | None, date | None]] = []
        for i, row in enumerate(rows):
            _require_fields(row, spec["required_fields"], collection="taxonomy_assignments", index=i)
            instrument_id = _require_nonblank(row.get("instrument_id"), field=f"taxonomy_assignments[{i}].instrument_id")
            if instrument_id not in instrument_ids:
                raise QMBIntegrityError(f"taxonomy_unknown_instrument:{instrument_id}")
            taxonomy_type = str(row.get("taxonomy_type") or "")
            if taxonomy_type not in types:
                raise QMBIntegrityError(f"taxonomy_type_invalid:{taxonomy_type}")
            _require_nonblank(row.get("taxonomy_value"), field=f"taxonomy_assignments[{i}].taxonomy_value")
            _require_nonblank(row.get("source_id"), field=f"taxonomy_assignments[{i}].source_id")
            _require_bool(row.get("pit_verified"), field=f"taxonomy_assignments[{i}].pit_verified")
            status = str(row.get("status") or "")
            if status not in statuses:
                raise QMBIntegrityError(f"taxonomy_status_invalid:{status}")
            start, end = _validate_interval(row.get("valid_from"), row.get("valid_to"), prefix=f"taxonomy_assignments[{i}]")
            parsed.append((row, start, end))

        for i, (left, left_start, left_end) in enumerate(parsed):
            for right, right_start, right_end in parsed[i + 1 :]:
                if left["instrument_id"] != right["instrument_id"] or left["taxonomy_type"] != right["taxonomy_type"]:
                    continue
                if left["taxonomy_value"] == right["taxonomy_value"]:
                    continue
                if not _intervals_overlap(left_start, left_end, right_start, right_end):
                    continue
                if "CONFLICTING" not in {str(left["status"]), str(right["status"])}:
                    raise QMBIntegrityError(
                        f"taxonomy_overlap_conflict:{left['instrument_id']}:{left['taxonomy_type']}"
                    )

    def _expected_component_hashes(self) -> dict[str, str]:
        instruments = self._collection("instruments")
        aliases = self._collection("identifier_aliases")
        memberships = self._collection("universe_membership")
        provider = self._collection("provider_coverage")
        outcomes = self._collection("outcome_availability")
        taxonomy = self._collection("taxonomy_assignments")
        instrument_hash = _hash({"instruments": instruments, "identifier_aliases": aliases, "taxonomy_assignments": taxonomy})
        universe_hash = _hash({"universe_membership": memberships, "provider_coverage": provider, "outcome_availability": outcomes})
        content_hash = _hash({
            "schema_version": self.payload["schema_version"],
            "bundle_as_of": self.payload["bundle_as_of"],
            "created_at": self.payload["created_at"],
            "historical_backfill_claim": self.payload["historical_backfill_claim"],
            "instruments": instruments,
            "identifier_aliases": aliases,
            "universe_membership": memberships,
            "provider_coverage": provider,
            "outcome_availability": outcomes,
            "taxonomy_assignments": taxonomy,
            "provenance": self.payload.get("provenance", {}),
        })
        return {
            "instrument_master_version": f"sha256:{instrument_hash}",
            "universe_ledger_version": f"sha256:{universe_hash}",
            "bundle_hash": f"sha256:{content_hash}",
        }

    def _validate_declared_hashes(self) -> None:
        expected = self._expected_component_hashes()
        for field, value in expected.items():
            declared = self.payload.get(field)
            if declared is not None and declared != value:
                raise QMBIntegrityError(f"declared_hash_mismatch:{field}")

    def _build_indexes(self) -> None:
        self.instruments = {str(row["instrument_id"]): row for row in self._collection("instruments")}
        self.aliases = self._collection("identifier_aliases")
        self.memberships = self._collection("universe_membership")
        self.provider_rows = self._collection("provider_coverage")
        self.outcome_rows = self._collection("outcome_availability")
        self.taxonomy_rows = self._collection("taxonomy_assignments")

    def versions(self) -> dict[str, str]:
        """Return immutable QM-B identity values suitable for QM-A analysis identity."""
        return self._expected_component_hashes()

    def normalized_payload(self) -> dict[str, Any]:
        """Return the bundle with deterministic component/version hashes attached."""
        result = json.loads(_canonical_json(self.payload))
        result.update(self.versions())
        return result

    def resolve_identifier(
        self,
        *,
        identifier_type: str,
        identifier_value: str,
        as_of_date: str,
        listing_venue: str | None = None,
    ) -> dict[str, Any]:
        when = _parse_date(as_of_date, field="as_of_date")
        if identifier_type not in set(self.contract["identifier_aliases"]["identifier_type_values"]):
            raise QMBIntegrityError(f"identifier_type_invalid:{identifier_type}")
        normalized = _normalized_identifier(identifier_type, identifier_value)
        venue = str(listing_venue or "").strip().upper()
        matches: list[dict[str, Any]] = []
        for row in self.aliases:
            if row["identifier_type"] != identifier_type:
                continue
            if _normalized_identifier(identifier_type, row["identifier_value"]) != normalized:
                continue
            alias_venue = str(row.get("listing_venue") or "").strip().upper()
            if venue and alias_venue and venue != alias_venue:
                continue
            start, end = _validate_interval(row.get("valid_from"), row.get("valid_to"), prefix="identifier_alias")
            if not _contains(start, end, when):
                continue
            if row.get("pit_verified") is not True or row.get("status") != "KNOWN":
                continue
            matches.append(row)

        instrument_ids = sorted({str(row["instrument_id"]) for row in matches})
        if not instrument_ids:
            return {
                "status": "UNKNOWN",
                "instrument_id": None,
                "reason_codes": ["NO_PIT_VERIFIED_IDENTIFIER_MATCH"],
            }
        if len(instrument_ids) > 1:
            return {
                "status": "CONFLICTING",
                "instrument_id": None,
                "candidate_instrument_ids": instrument_ids,
                "reason_codes": ["MULTIPLE_PIT_VERIFIED_IDENTIFIER_MATCHES"],
            }
        return {"status": "KNOWN", "instrument_id": instrument_ids[0], "reason_codes": []}

    def membership_as_of(
        self, *, instrument_id: str, as_of_date: str, listing_venue: str | None = None
    ) -> dict[str, Any]:
        _parse_date(as_of_date, field="as_of_date")
        if instrument_id not in self.instruments:
            return {
                "status": "UNKNOWN",
                "membership_status": "UNKNOWN",
                "reason_codes": ["INSTRUMENT_NOT_IN_MASTER"],
            }
        venue = str(listing_venue or "").strip().upper()
        matches = [
            row
            for row in self.memberships
            if str(row["instrument_id"]) == instrument_id
            and str(row["as_of_date"]) == as_of_date
            and (not venue or str(row.get("listing_venue") or "").upper() == venue)
        ]
        if not matches:
            return {
                "status": "UNKNOWN",
                "membership_status": "UNKNOWN",
                "reason_codes": ["NO_EXPLICIT_AS_OF_MEMBERSHIP_ROW"],
            }
        if len(matches) > 1:
            statuses = {str(row["membership_status"]) for row in matches}
            if len(statuses) > 1 or not venue:
                return {
                    "status": "CONFLICTING",
                    "membership_status": "UNKNOWN",
                    "reason_codes": ["MULTIPLE_AS_OF_MEMBERSHIP_ROWS"],
                }
        row = matches[0]
        return {"status": "KNOWN", **row}

    def provider_coverage_as_of(
        self,
        *,
        instrument_id: str,
        as_of_date: str,
        source_id: str,
        external_family: str,
    ) -> dict[str, Any]:
        _parse_date(as_of_date, field="as_of_date")
        matches = [
            row for row in self.provider_rows
            if str(row["instrument_id"]) == instrument_id
            and str(row["as_of_date"]) == as_of_date
            and str(row["source_id"]) == source_id
            and str(row["external_family"]) == external_family
        ]
        if not matches:
            return {
                "status": "UNKNOWN",
                "coverage_status": "UNKNOWN",
                "reason_codes": ["NO_EXPLICIT_PROVIDER_COVERAGE_ROW"],
            }
        return {"status": "KNOWN", **matches[0]}

    def taxonomy_as_of(self, *, instrument_id: str, taxonomy_type: str, as_of_date: str) -> dict[str, Any]:
        when = _parse_date(as_of_date, field="as_of_date")
        matches: list[dict[str, Any]] = []
        for row in self.taxonomy_rows:
            if str(row["instrument_id"]) != instrument_id or str(row["taxonomy_type"]) != taxonomy_type:
                continue
            start, end = _validate_interval(row.get("valid_from"), row.get("valid_to"), prefix="taxonomy")
            if _contains(start, end, when) and row.get("pit_verified") is True and row.get("status") == "KNOWN":
                matches.append(row)
        values = sorted({str(row["taxonomy_value"]) for row in matches})
        if not values:
            return {"status": "UNKNOWN", "taxonomy_value": None, "reason_codes": ["NO_PIT_TAXONOMY_ASSIGNMENT"]}
        if len(values) > 1:
            return {"status": "CONFLICTING", "taxonomy_value": None, "candidate_values": values, "reason_codes": ["MULTIPLE_PIT_TAXONOMY_ASSIGNMENTS"]}
        return {"status": "KNOWN", "taxonomy_value": values[0], "reason_codes": []}

    def audit_sample(self, rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        required = self.contract["sample_audit"]["required_sample_fields"]
        audited: list[dict[str, Any]] = []
        counts = {"ELIGIBLE": 0, "NOT_ELIGIBLE": 0, "FAIL_CLOSED": 0}
        for index, raw in enumerate(rows):
            if not isinstance(raw, Mapping):
                raise QMBIntegrityError(f"sample_row_must_be_object:{index}")
            row = dict(raw)
            _require_fields(row, required, collection="sample", index=index)
            instrument_id = _require_nonblank(row.get("instrument_id"), field=f"sample[{index}].instrument_id")
            as_of_date = _require_nonblank(row.get("as_of_date"), field=f"sample[{index}].as_of_date")
            _parse_date(as_of_date, field=f"sample[{index}].as_of_date")

            membership = self.membership_as_of(instrument_id=instrument_id, as_of_date=as_of_date)
            instrument = self.instruments.get(instrument_id)
            reasons: list[str] = []
            if membership.get("status") != "KNOWN" or membership.get("membership_status") == "UNKNOWN":
                audit_status = "FAIL_CLOSED"
                reasons.extend(membership.get("reason_codes") or ["MEMBERSHIP_UNKNOWN"])
            elif instrument is None or instrument.get("identity_status") != "VERIFIED":
                audit_status = "FAIL_CLOSED"
                reasons.append("IDENTITY_NOT_VERIFIED")
            elif membership.get("membership_status") != "IN_SCOPE":
                audit_status = "NOT_ELIGIBLE"
                reasons.append(f"MEMBERSHIP_{membership.get('membership_status')}")
            elif membership.get("pit_verified") is not True:
                audit_status = "FAIL_CLOSED"
                reasons.append("MEMBERSHIP_NOT_PIT_VERIFIED")
            elif membership.get("scanner_observable") is not True:
                audit_status = "NOT_ELIGIBLE"
                reasons.append("SCANNER_NOT_OBSERVABLE")
            else:
                audit_status = "ELIGIBLE"
            counts[audit_status] += 1
            audited.append({
                "row_index": index,
                "instrument_id": instrument_id,
                "as_of_date": as_of_date,
                "audit_status": audit_status,
                "membership_status": membership.get("membership_status", "UNKNOWN"),
                "investability_status": membership.get("investability_status", "UNKNOWN"),
                "reason_codes": sorted(set(reasons)),
            })

        return {
            "schema_version": "qm_b_sample_audit_v1",
            "row_count": len(audited),
            "counts": counts,
            "fail_closed": counts["FAIL_CLOSED"] > 0,
            "rows": audited,
        }

    def outcome_as_of(self, *, instrument_id: str, as_of_date: str, outcome_id: str) -> dict[str, Any]:
        _parse_date(as_of_date, field="as_of_date")
        matches = [
            row for row in self.outcome_rows
            if str(row["instrument_id"]) == instrument_id
            and str(row["as_of_date"]) == as_of_date
            and str(row["outcome_id"]) == outcome_id
        ]
        if not matches:
            return {
                "status": "UNKNOWN",
                "availability_status": "UNKNOWN",
                "reason_codes": ["NO_EXPLICIT_OUTCOME_AVAILABILITY_ROW"],
            }
        return {"status": "KNOWN", **matches[0]}


def build_bundle(
    *,
    bundle_as_of: str,
    created_at: str,
    instruments: Sequence[Mapping[str, Any]],
    identifier_aliases: Sequence[Mapping[str, Any]],
    universe_membership: Sequence[Mapping[str, Any]],
    provider_coverage: Sequence[Mapping[str, Any]] = (),
    outcome_availability: Sequence[Mapping[str, Any]] = (),
    taxonomy_assignments: Sequence[Mapping[str, Any]] = (),
    provenance: Mapping[str, Any] | None = None,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build and validate a deterministic QM-B bundle.

    `historical_backfill_claim` is deliberately false. Historical rows may be present,
    but their PIT truth must be established row-by-row through source/pit_verified
    fields rather than inferred from the bundle creation time.
    """
    payload: dict[str, Any] = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "bundle_as_of": bundle_as_of,
        "created_at": created_at,
        "historical_backfill_claim": False,
        "instruments": [dict(row) for row in instruments],
        "identifier_aliases": [dict(row) for row in identifier_aliases],
        "universe_membership": [dict(row) for row in universe_membership],
        "provider_coverage": [dict(row) for row in provider_coverage],
        "outcome_availability": [dict(row) for row in outcome_availability],
        "taxonomy_assignments": [dict(row) for row in taxonomy_assignments],
        "provenance": dict(provenance or {}),
    }
    bundle = UniverseIntegrityBundle(payload, contract_path=contract_path)
    return bundle.normalized_payload()


def inspect_current_universe_master(path: str | Path) -> dict[str, Any]:
    """Inventory current universe identity quality without creating historical truth.

    This intentionally emits findings only. It never assigns historical valid_from,
    membership, listing or investability dates from a current CSV.
    """
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise QMBIntegrityError("current_universe_empty")

    active_rows = [row for row in rows if str(row.get("active") or "").strip().lower() in {"1", "true", "yes", "y"}]
    symbol_map: dict[str, list[int]] = {}
    isin_map: dict[str, list[int]] = {}
    missing_symbol: list[int] = []
    missing_isin: list[int] = []
    for index, row in enumerate(active_rows, start=2):
        symbol = str(row.get("symbol") or "").strip().upper()
        isin = str(row.get("isin") or "").strip().upper()
        if symbol:
            symbol_map.setdefault(symbol, []).append(index)
        else:
            missing_symbol.append(index)
        if isin:
            isin_map.setdefault(isin, []).append(index)
        else:
            missing_isin.append(index)

    duplicate_symbols = {key: value for key, value in sorted(symbol_map.items()) if len(value) > 1}
    duplicate_isins = {key: value for key, value in sorted(isin_map.items()) if len(value) > 1}
    return {
        "schema_version": "qm_b_current_universe_inventory_v1",
        "scope": "CURRENT_STATE_ONLY_NOT_HISTORICAL_MEMBERSHIP",
        "row_count": len(rows),
        "active_row_count": len(active_rows),
        "unique_active_symbol_count": len(symbol_map),
        "unique_active_isin_count": len(isin_map),
        "missing_symbol_rows": missing_symbol,
        "missing_isin_rows": missing_isin,
        "duplicate_symbols": duplicate_symbols,
        "duplicate_isins": duplicate_isins,
        "historical_membership_inferred": False,
        "historical_identity_inferred": False,
    }
