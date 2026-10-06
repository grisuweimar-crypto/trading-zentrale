"""Phase L0 boundary guard for Pattern Discovery Lab v2.

This module is intentionally small. It establishes the research-only namespace,
read/write boundaries, deterministic contract identity and reuse bindings to
existing QM-C governance. It does not discover patterns, evaluate outcomes,
change scanner scores, create Decision-Layer evidence, alter portfolio actions
or execute orders.
"""
from __future__ import annotations

from hashlib import sha256
from importlib import import_module
import json
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "pattern_discovery_l0_boundary_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l0_boundary_v1.json"
)


class BoundaryViolation(ValueError):
    """Raised when Pattern Discovery Lab crosses its frozen L0 boundary."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _normalized_repo_path(value: str | Path) -> str:
    raw = str(value).strip().replace("\\", "/")
    if not raw:
        raise BoundaryViolation("path_required")
    path = PurePosixPath(raw)
    if path.is_absolute():
        raise BoundaryViolation("absolute_path_forbidden")
    if ".." in path.parts:
        raise BoundaryViolation("path_traversal_forbidden")
    parts = [part for part in path.parts if part not in ("", ".")]
    if not parts:
        raise BoundaryViolation("path_required")
    return "/".join(parts)


def _under_prefix(path: str, prefix: str) -> bool:
    root = prefix.rstrip("/")
    return path == root or path.startswith(root + "/")


def load_boundary_contract(path: str | Path | None = None) -> dict[str, Any]:
    """Load and validate the immutable L0 boundary contract."""
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BoundaryViolation(f"boundary_contract_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise BoundaryViolation("boundary_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise BoundaryViolation("boundary_contract_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise BoundaryViolation("productive_integration_must_be_disabled")
    if payload.get("execution_allowed") is not False:
        raise BoundaryViolation("execution_must_be_disabled")

    read_policy = payload.get("read_policy")
    write_policy = payload.get("write_policy")
    forbidden = payload.get("forbidden_semantics")
    bindings = payload.get("governance_bindings")
    if not isinstance(read_policy, Mapping) or not isinstance(write_policy, Mapping):
        raise BoundaryViolation("boundary_path_policy_missing")
    if not isinstance(forbidden, Mapping) or not isinstance(bindings, Mapping):
        raise BoundaryViolation("boundary_semantics_or_governance_missing")
    if not read_policy.get("allowed_prefixes"):
        raise BoundaryViolation("read_prefixes_required")
    if write_policy.get("allowed_prefixes") != ["artifacts/research/pattern_discovery/"]:
        raise BoundaryViolation("write_namespace_must_be_lab_only")
    if write_policy.get("productive_artifact_write_allowed") is not False:
        raise BoundaryViolation("productive_artifact_write_must_be_forbidden")
    return payload


def boundary_contract_hash(contract: Mapping[str, Any] | None = None) -> str:
    """Return deterministic SHA-256 identity for the complete L0 contract."""
    value = dict(contract) if contract is not None else load_boundary_contract()
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _forbidden_payload_paths(
    value: object,
    forbidden_keys: frozenset[str],
    path: str = "$",
) -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            child = f"{path}.{key_text}"
            if key_text.lower() in forbidden_keys:
                found.append(child)
            found.extend(_forbidden_payload_paths(item, forbidden_keys, child))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            found.extend(_forbidden_payload_paths(item, forbidden_keys, f"{path}[{index}]"))
    return found


class PatternDiscoveryBoundary:
    """Fail-closed runtime boundary for Pattern Discovery Lab v2."""

    def __init__(self, contract: Mapping[str, Any] | None = None) -> None:
        self.contract = dict(contract) if contract is not None else load_boundary_contract()
        if self.contract.get("schema_version") != SCHEMA_VERSION:
            raise BoundaryViolation("boundary_contract_schema_invalid")
        self.contract_hash = boundary_contract_hash(self.contract)
        self.read_prefixes = tuple(self.contract["read_policy"]["allowed_prefixes"])
        self.write_prefixes = tuple(self.contract["write_policy"]["allowed_prefixes"])
        self.forbidden_payload_keys = frozenset(
            str(key).lower()
            for key in self.contract["forbidden_semantics"]["payload_keys"]
        )

    def assert_read_path_allowed(self, value: str | Path) -> str:
        """Return normalized path when it is inside an L0-approved read namespace."""
        path = _normalized_repo_path(value)
        if not any(_under_prefix(path, prefix) for prefix in self.read_prefixes):
            raise BoundaryViolation(f"read_path_outside_lab_contract:{path}")
        return path

    def assert_write_path_allowed(self, value: str | Path) -> str:
        """Return normalized path when runtime writing stays inside lab artifacts."""
        path = _normalized_repo_path(value)
        if not any(_under_prefix(path, prefix) for prefix in self.write_prefixes):
            raise BoundaryViolation(f"write_path_outside_lab_namespace:{path}")
        return path

    def assert_research_payload(self, payload: object) -> None:
        """Reject productive decision/execution semantics anywhere in a lab payload."""
        found = _forbidden_payload_paths(payload, self.forbidden_payload_keys)
        if found:
            raise BoundaryViolation(
                "productive_semantics_forbidden:" + ",".join(sorted(found))
            )

    def validate_qm_c_bindings(self) -> dict[str, str]:
        """Verify declared QM-C authorities exist; do not create parallel governance."""
        result: dict[str, str] = {}
        bindings = self.contract.get("governance_bindings")
        if not isinstance(bindings, Mapping) or not bindings:
            raise BoundaryViolation("governance_bindings_missing")
        for binding_name, raw in bindings.items():
            if not isinstance(raw, Mapping) or raw.get("reuse_required") is not True:
                raise BoundaryViolation(f"governance_binding_invalid:{binding_name}")
            module_name = str(raw.get("module") or "").strip()
            symbol_name = str(raw.get("symbol") or "").strip()
            authority = str(raw.get("authority") or "").strip()
            if not module_name or not symbol_name or not authority:
                raise BoundaryViolation(f"governance_binding_incomplete:{binding_name}")
            try:
                module = import_module(module_name)
            except ImportError as exc:
                raise BoundaryViolation(
                    f"governance_module_unavailable:{binding_name}:{module_name}"
                ) from exc
            if not hasattr(module, symbol_name):
                raise BoundaryViolation(
                    f"governance_symbol_unavailable:{binding_name}:{symbol_name}"
                )
            result[str(binding_name)] = authority
        return result
