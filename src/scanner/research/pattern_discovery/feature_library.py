"""Phase L2 versioned point-in-time Feature Library.

The library defines what Pattern Discovery is allowed to use. It validates
feature/transform references and point-in-time availability, but it deliberately
does not calculate discovery outcomes, search patterns, rank candidates, promote
evidence or alter productive scanner/Decision-Layer semantics.
"""
from __future__ import annotations

from datetime import date, datetime, time, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from .run_contract import verify_run_manifest


SCHEMA_VERSION = "pattern_discovery_feature_library_v1"
DEFAULT_LIBRARY_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "feature_library_v1.json"
)


class FeatureLibraryError(ValueError):
    """Raised when L2 feature-library or PIT invariants are violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise FeatureLibraryError(f"value_required:{field}")
    return result


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _asof(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        text_value = _text(value, field)
        if len(text_value) == 10:
            try:
                parsed = datetime.combine(date.fromisoformat(text_value), time.min)
            except ValueError as exc:
                raise FeatureLibraryError(f"invalid_as_of:{field}") from exc
        else:
            normalized = text_value[:-1] + "+00:00" if text_value.endswith("Z") else text_value
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError as exc:
                raise FeatureLibraryError(f"invalid_as_of:{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_feature_library(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_LIBRARY_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FeatureLibraryError(f"feature_library_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise FeatureLibraryError("feature_library_schema_invalid")
    if payload.get("research_only") is not True:
        raise FeatureLibraryError("feature_library_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise FeatureLibraryError("feature_library_productive_integration_forbidden")
    if payload.get("execution_allowed") is not False:
        raise FeatureLibraryError("feature_library_execution_forbidden")
    return payload


def feature_library_hash(payload: Mapping[str, Any] | None = None) -> str:
    value = dict(payload) if payload is not None else load_feature_library()
    return _hash(value)


def feature_version_hash(feature: Mapping[str, Any]) -> str:
    return _hash(dict(feature))


class FeatureLibrary:
    """Validated registry of exact feature versions and transformations."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else DEFAULT_LIBRARY_PATH
        self.payload = load_feature_library(self.path)
        self.version = _text(
            self.payload.get("feature_library_version"), "feature_library_version"
        )
        self.library_hash = feature_library_hash(self.payload)
        self._validate_contract()

    @staticmethod
    def _feature_key(feature_id: str, feature_version: str) -> str:
        return f"{feature_id}::{feature_version}"

    @staticmethod
    def _transformation_key(transformation_id: str, transformation_version: str) -> str:
        return f"{transformation_id}::{transformation_version}"

    def _validate_contract(self) -> None:
        observation = self.payload.get("observation_contract")
        if not isinstance(observation, Mapping):
            raise FeatureLibraryError("observation_contract_missing")
        self.as_of_field = _text(
            observation.get("as_of_field"), "observation_contract.as_of_field"
        )
        self.entity_field = _text(
            observation.get("entity_field"), "observation_contract.entity_field"
        )
        if observation.get("historical_reconstruction_allowed") is not False:
            raise FeatureLibraryError("historical_reconstruction_must_be_forbidden")

        raw_transformations = self.payload.get("transformations")
        if not isinstance(raw_transformations, list) or not raw_transformations:
            raise FeatureLibraryError("transformations_nonempty_list_required")
        self.transformations: dict[str, dict[str, Any]] = {}
        for index, raw in enumerate(raw_transformations):
            if not isinstance(raw, Mapping):
                raise FeatureLibraryError(f"transformation_must_be_object:{index}")
            tid = _text(
                raw.get("transformation_id"),
                f"transformations[{index}].transformation_id",
            )
            version = _text(
                raw.get("transformation_version"),
                f"transformations[{index}].transformation_version",
            )
            key = self._transformation_key(tid, version)
            if key in self.transformations:
                raise FeatureLibraryError(f"duplicate_transformation_version:{key}")
            semantic_types = raw.get("allowed_semantic_types")
            required_parameters = raw.get("required_parameters")
            if not isinstance(semantic_types, list) or not semantic_types:
                raise FeatureLibraryError(f"transformation_semantic_types_invalid:{key}")
            if not isinstance(required_parameters, list):
                raise FeatureLibraryError(f"transformation_required_parameters_invalid:{key}")
            self.transformations[key] = dict(raw)

        raw_features = self.payload.get("features")
        if not isinstance(raw_features, list) or not raw_features:
            raise FeatureLibraryError("features_nonempty_list_required")
        self.features: dict[str, dict[str, Any]] = {}
        for index, raw in enumerate(raw_features):
            if not isinstance(raw, Mapping):
                raise FeatureLibraryError(f"feature_must_be_object:{index}")
            feature_id = _text(raw.get("feature_id"), f"features[{index}].feature_id")
            version = _text(raw.get("feature_version"), f"features[{index}].feature_version")
            source_field = _text(raw.get("source_field"), f"features[{index}].source_field")
            semantic_type = _text(
                raw.get("semantic_type"), f"features[{index}].semantic_type"
            ).upper()
            semantics = _text(raw.get("semantics"), f"features[{index}].semantics")
            key = self._feature_key(feature_id, version)
            if key in self.features:
                raise FeatureLibraryError(f"duplicate_feature_version:{key}")
            availability = raw.get("availability")
            if not isinstance(availability, Mapping):
                raise FeatureLibraryError(f"feature_availability_missing:{key}")
            if availability.get("mode") != "OBSERVED_ROW_FIELD":
                raise FeatureLibraryError(f"feature_availability_mode_invalid:{key}")
            if availability.get("available_from_semantics") != "CURRENT_OBSERVATION_AS_OF":
                raise FeatureLibraryError(
                    f"feature_available_from_semantics_invalid:{key}"
                )
            if availability.get("historical_reconstruction_allowed") is not False:
                raise FeatureLibraryError(
                    f"feature_historical_reconstruction_forbidden:{key}"
                )
            allowed = raw.get("allowed_transformations")
            if not isinstance(allowed, list) or not allowed:
                raise FeatureLibraryError(f"feature_transformations_invalid:{key}")
            for transformation_id in allowed:
                tkey = self._transformation_key(str(transformation_id), "v1")
                if tkey not in self.transformations:
                    raise FeatureLibraryError(
                        f"feature_references_unknown_transformation:{key}:{transformation_id}"
                    )
                if semantic_type not in set(
                    self.transformations[tkey]["allowed_semantic_types"]
                ):
                    raise FeatureLibraryError(
                        f"feature_transformation_type_mismatch:{key}:{transformation_id}"
                    )
            self.features[key] = {
                **dict(raw),
                "source_field": source_field,
                "semantic_type": semantic_type,
                "semantics": semantics,
                "feature_version_hash": feature_version_hash(raw),
            }

    def verify_integrity(self) -> dict[str, Any]:
        return {
            "valid": True,
            "schema_version": self.payload["schema_version"],
            "feature_library_version": self.version,
            "feature_library_hash": self.library_hash,
            "feature_version_count": len(self.features),
            "transformation_version_count": len(self.transformations),
        }

    def validate_source_columns(self, columns: Sequence[str]) -> dict[str, Any]:
        observed = {str(value) for value in columns}
        required = {feature["source_field"] for feature in self.features.values()}
        required.update({self.as_of_field, self.entity_field})
        missing = sorted(required - observed)
        if missing:
            raise FeatureLibraryError(
                "registered_source_columns_missing:" + ",".join(missing)
            )
        return {"valid": True, "required_column_count": len(required)}

    def get_feature(
        self,
        feature_id: str,
        feature_version: str = "v1",
    ) -> dict[str, Any]:
        key = self._feature_key(
            _text(feature_id, "feature_id"),
            _text(feature_version, "feature_version"),
        )
        try:
            return dict(self.features[key])
        except KeyError as exc:
            raise FeatureLibraryError(
                f"feature_version_not_registered:{key}"
            ) from exc

    def get_transformation(
        self,
        transformation_id: str,
        transformation_version: str = "v1",
    ) -> dict[str, Any]:
        key = self._transformation_key(
            _text(transformation_id, "transformation_id"),
            _text(transformation_version, "transformation_version"),
        )
        try:
            return dict(self.transformations[key])
        except KeyError as exc:
            raise FeatureLibraryError(
                f"transformation_version_not_registered:{key}"
            ) from exc

    def validate_feature_use(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, Mapping):
            raise FeatureLibraryError("feature_use_must_be_object")
        required = {
            "feature_id",
            "feature_version",
            "transformation_id",
            "transformation_version",
            "parameters",
        }
        missing = sorted(required - set(raw))
        if missing:
            raise FeatureLibraryError(
                "feature_use_fields_missing:" + ",".join(missing)
            )
        extra = sorted(set(raw) - required)
        if extra:
            raise FeatureLibraryError(
                "feature_use_unknown_fields:" + ",".join(extra)
            )

        feature = self.get_feature(
            str(raw["feature_id"]), str(raw["feature_version"])
        )
        transformation = self.get_transformation(
            str(raw["transformation_id"]),
            str(raw["transformation_version"]),
        )
        tid = transformation["transformation_id"]
        if tid not in set(feature["allowed_transformations"]):
            raise FeatureLibraryError(
                f"transformation_not_allowed_for_feature:{feature['feature_id']}:{tid}"
            )
        if feature["semantic_type"] not in set(
            transformation["allowed_semantic_types"]
        ):
            raise FeatureLibraryError(
                f"transformation_semantic_type_invalid:{feature['feature_id']}:{tid}"
            )

        params = raw["parameters"]
        if not isinstance(params, Mapping):
            raise FeatureLibraryError("feature_use_parameters_must_be_object")
        required_params = set(transformation["required_parameters"])
        if set(params) != required_params:
            raise FeatureLibraryError(
                f"feature_use_parameters_mismatch:{tid}"
            )

        normalized_params: dict[str, Any] = {}
        if tid in {"delta_observations", "change_direction"}:
            lag = params.get("lag_observations")
            if isinstance(lag, bool) or not isinstance(lag, int):
                raise FeatureLibraryError("lag_observations_integer_required")
            if lag not in set(transformation["allowed_lag_observations"]):
                raise FeatureLibraryError("lag_observations_not_registered")
            normalized_params["lag_observations"] = lag
        elif tid == "threshold_crossing":
            threshold = params.get("threshold")
            if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
                raise FeatureLibraryError("threshold_numeric_required")
            threshold = float(threshold)
            allowed_thresholds = feature.get("allowed_thresholds")
            if not isinstance(allowed_thresholds, list) or threshold not in {
                float(value) for value in allowed_thresholds
            }:
                raise FeatureLibraryError(
                    f"threshold_not_registered_for_feature:{feature['feature_id']}:{threshold}"
                )
            normalized_params["threshold"] = threshold
        elif tid == "level_band":
            # Versioned CY-05 v2 opt-in: exact bands, never post-hoc choices.
            if (
                feature["feature_id"] != "scanner.cycle"
                or feature.get("level_band_boundaries") != [25, 50, 75]
            ):
                raise FeatureLibraryError("cycle_level_bands_not_preregistered")

        return {
            "feature_id": feature["feature_id"],
            "feature_version": feature["feature_version"],
            "feature_version_hash": feature["feature_version_hash"],
            "source_field": feature["source_field"],
            "semantic_type": feature["semantic_type"],
            "transformation_id": transformation["transformation_id"],
            "transformation_version": transformation["transformation_version"],
            "parameters": normalized_params,
        }

    def _history_requirement(self, validated_use: Mapping[str, Any]) -> int:
        tid = str(validated_use["transformation_id"])
        if tid in {"raw", "regime_context", "level_band"}:
            return 0
        if tid in {"state_transition", "threshold_crossing"}:
            return 1
        if tid in {"delta_observations", "change_direction"}:
            return int(validated_use["parameters"]["lag_observations"])
        raise FeatureLibraryError(f"unsupported_transformation_runtime:{tid}")

    @staticmethod
    def _cy05_research_row_status(row: Mapping[str, Any]) -> str | None:
        """A CY-05 row must come from an independently released CY-03 ledger.

        This is an additional fail-closed INPUT contract, not a verifier of
        upstream archive hashes. A future read-only adapter must establish those
        hashes/eligibility and may then attach these exact provenance fields.
        """
        if row.get("cycle_research_status") != "ELIGIBLE":
            return "CYCLE_RESEARCH_NOT_RELEASED"
        if row.get("cycle_history_source") != "CY03_VERIFIED_LEDGER":
            return "CYCLE_VERIFIED_LEDGER_REQUIRED"
        if row.get("cycle_quality") != "VALID":
            return "CYCLE_QUALITY_NOT_VALID"
        if not all(
            isinstance(row.get(key), str) and row[key].strip()
            for key in (
                "cycle_snapshot_id", "cycle_asset_id", "cycle_formula",
                "cycle_currency", "cycle_listing_symbol", "cycle_price_symbol",
            )
        ):
            return "CYCLE_LINEAGE_MISSING"
        if row.get("cycle_asset_id") != row.get("symbol"):
            return "CYCLE_ASSET_ID_MISMATCH"
        value = row.get("cycle")
        if isinstance(value, bool):
            return "CYCLE_VALUE_OUT_OF_RANGE"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "CYCLE_VALUE_OUT_OF_RANGE"
        if not math.isfinite(numeric) or not 0.0 <= numeric <= 100.0:
            return "CYCLE_VALUE_OUT_OF_RANGE"
        return None

    def pit_availability(
        self,
        feature_use: Mapping[str, Any],
        observation: Mapping[str, Any],
        *,
        data_cutoff: str | date | datetime,
        history: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        """Report whether one feature use genuinely existed at the requested PIT."""
        validated = self.validate_feature_use(feature_use)
        if not isinstance(observation, Mapping):
            raise FeatureLibraryError("observation_must_be_object")

        cutoff = _asof(data_cutoff, "data_cutoff")
        if self.as_of_field not in observation:
            return self._availability_result(
                validated, False, "AS_OF_NOT_RECORDED", None
            )
        current_as_of = _asof(
            observation[self.as_of_field],
            f"observation.{self.as_of_field}",
        )
        if current_as_of > cutoff:
            return self._availability_result(
                validated, False, "OBSERVATION_AFTER_CUTOFF", None
            )

        field = str(validated["source_field"])
        if field not in observation:
            return self._availability_result(
                validated, False, "SOURCE_FIELD_NOT_RECORDED", None
            )
        if _is_missing(observation[field]):
            return self._availability_result(
                validated, False, "SOURCE_VALUE_MISSING", None
            )

        # The opt-in CY-05 library cannot turn a legacy/imputed/current-only
        # Cycle into research evidence. V1 remains byte-for-byte unchanged.
        cy05_cycle = (
            self.version == "PDL-FEATURE-LIBRARY-CYCLE-v2"
            and validated["feature_id"] == "scanner.cycle"
        )
        if cy05_cycle:
            refusal = self._cy05_research_row_status(observation)
            if refusal is not None:
                return self._availability_result(validated, False, refusal, None)

        required_prior = self._history_requirement(validated)
        if required_prior:
            entity = observation.get(self.entity_field)
            prior_rows: list[tuple[datetime, Mapping[str, Any]]] = []
            for row in history:
                if not isinstance(row, Mapping) or self.as_of_field not in row:
                    continue
                if entity is not None and row.get(self.entity_field) != entity:
                    continue
                row_as_of = _asof(
                    row[self.as_of_field],
                    f"history.{self.as_of_field}",
                )
                if row_as_of < current_as_of and row_as_of <= cutoff:
                    prior_rows.append((row_as_of, row))
            prior_rows.sort(key=lambda item: item[0], reverse=True)

            if len(prior_rows) < required_prior:
                return self._availability_result(
                    validated,
                    False,
                    "INSUFFICIENT_PRIOR_OBSERVATIONS",
                    None,
                )
            if cy05_cycle:
                # Eligibility is supplied by the separately verified, hash-
                # checked CY-03 archive adapter; mere 1/5/10 historical rows
                # must never be promoted implicitly to a valid comparison.
                lag_key = f"cycle_lag_{required_prior}obs"
                if observation.get(lag_key) != "RESEARCH_ELIGIBLE":
                    return self._availability_result(
                        validated, False, "CYCLE_LAG_NOT_RESEARCH_ELIGIBLE", None
                    )
                same_lineage = (
                    "cycle_asset_id", "cycle_formula", "cycle_currency",
                    "cycle_listing_symbol", "cycle_price_symbol",
                )
                for _, prior in prior_rows[:required_prior]:
                    refusal = self._cy05_research_row_status(prior)
                    if refusal is not None:
                        return self._availability_result(
                            validated, False, f"PRIOR_{refusal}", None
                        )
                    if any(observation[key] != prior[key] for key in same_lineage):
                        return self._availability_result(
                            validated, False, "CYCLE_LAG_LINEAGE_MISMATCH", None
                        )
            required_row = prior_rows[required_prior - 1][1]
            if field not in required_row:
                return self._availability_result(
                    validated,
                    False,
                    "PRIOR_SOURCE_FIELD_NOT_RECORDED",
                    None,
                )
            if _is_missing(required_row[field]):
                return self._availability_result(
                    validated,
                    False,
                    "PRIOR_SOURCE_VALUE_MISSING",
                    None,
                )

        return self._availability_result(
            validated,
            True,
            "AVAILABLE",
            current_as_of.isoformat(),
        )

    @staticmethod
    def _availability_result(
        validated_use: Mapping[str, Any],
        available: bool,
        status: str,
        available_from: str | None,
    ) -> dict[str, Any]:
        return {
            "feature_id": validated_use["feature_id"],
            "feature_version": validated_use["feature_version"],
            "transformation_id": validated_use["transformation_id"],
            "transformation_version": validated_use["transformation_version"],
            "available": available,
            "status": status,
            "available_from": available_from,
        }

    def validate_run_binding(
        self,
        manifest: Mapping[str, Any],
        *,
        repo_root: str | Path,
    ) -> dict[str, Any]:
        """Bind an L1 run to the exact immutable Feature Library bytes."""
        verify_run_manifest(manifest)
        prereg = manifest.get("preregistration")
        fingerprints = manifest.get("input_fingerprints")
        if not isinstance(prereg, Mapping) or not isinstance(fingerprints, list):
            raise FeatureLibraryError(
                "run_manifest_feature_binding_components_missing"
            )
        if prereg.get("feature_library_version") != self.version:
            raise FeatureLibraryError(
                "run_feature_library_version_mismatch"
            )

        source_repo_path = str(self.payload["identity"]["source_repo_path"])
        if source_repo_path not in prereg.get("data_sources", []):
            raise FeatureLibraryError(
                "run_must_fingerprint_feature_library_source"
            )

        matching = [
            item
            for item in fingerprints
            if isinstance(item, Mapping)
            and item.get("path") == source_repo_path
        ]
        if len(matching) != 1:
            raise FeatureLibraryError(
                "run_feature_library_fingerprint_missing_or_duplicate"
            )

        expected_bytes_hash = sha256(self.path.read_bytes()).hexdigest()
        if matching[0].get("sha256") != expected_bytes_hash:
            raise FeatureLibraryError(
                "run_feature_library_fingerprint_mismatch"
            )

        root = Path(repo_root).resolve()
        candidate = (root / source_repo_path).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise FeatureLibraryError(
                "run_feature_library_source_outside_repo"
            ) from exc
        if not candidate.is_file():
            raise FeatureLibraryError(
                "run_feature_library_source_missing"
            )
        if sha256(candidate.read_bytes()).hexdigest() != expected_bytes_hash:
            raise FeatureLibraryError(
                "run_repo_feature_library_bytes_mismatch"
            )

        return {
            "valid": True,
            "feature_library_version": self.version,
            "feature_library_hash": self.library_hash,
            "feature_library_file_sha256": expected_bytes_hash,
        }
