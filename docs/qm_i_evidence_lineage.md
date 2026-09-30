# QM-I / BA-QM3 — Evidence Lineage & Double Counting

## Scope

QM-I makes material evidence ancestry machine-readable without changing scanner or Decision-Layer semantics. It consumes the stable identities produced by QM-B and QM-C and the existing Phase-7 claim IDs rather than creating a competing namespace.

The canonical lineage vocabulary covers the masterplan path:

`RAW_SOURCE -> FEATURE -> INDICATOR -> SCORE -> CLAIM -> CALIBRATION -> DECISION -> WATCH`

The graph also supports `DERIVED_METRIC`, `HYPOTHESIS`, `ANALYSIS_PLAN`, `CONTROL_PLAN`, `RESULT`, `EXTERNAL_EVIDENCE`, `PORTFOLIO_ACTION` and `POSITION_SNAPSHOT` so existing governance and decision artifacts can be connected without flattening their semantics.

## Typed provenance graph

Every node has:

- stable `node_id`
- explicit `version_id`
- typed `node_type`
- immutable SHA-256 `content_hash`
- `lineage_complete` flag
- optional `as_of`
- metadata that is descriptive, not an identity substitute

Node versions are immutable. A changed artifact requires an explicit successor version.

Edges are also immutable and typed. Material ancestry relations include `DERIVED_FROM`, `REFERENCES`, `ANNOTATES`, `CALIBRATES`, `INFORMS`, `PRODUCES`, `USES_POSITION` and `PRESENTS`. Material provenance is required to remain acyclic.

## Common ancestry

QM-I classifies a pair as one of:

- `SAME_EVIDENCE`
- `DIRECT_DEPENDENCY`
- `COMMON_ANCESTRY`
- `UNKNOWN_INCOMPLETE_LINEAGE`
- `NO_COMMON_ANCESTRY_DETECTED`

For common ancestry it returns the actual shared node IDs and paths from the ancestor to both descendants.

Common ancestry is **not automatically a defect**. It is a review trigger because two items can legitimately reuse a common source while still serving different roles. The review must decide whether the downstream combination double-counts the shared information.

## Missing lineage is not independence

`NO_COMMON_ANCESTRY_DETECTED` is only possible when both relevant ancestry trees are declared complete. A non-root node that has no material parent cannot support an independence claim merely because no parent was registered.

If lineage is incomplete, the state is `UNKNOWN_INCOMPLETE_LINEAGE` and a combination that depends on independence requires review.

## Independence Claim Registry

Independence claims are themselves immutable, versioned governance objects. Supported statuses are:

- `INDEPENDENT_SUPPORTED`
- `DEPENDENT_COMMON_ANCESTRY`
- `DEPENDENT_DIRECT_REFERENCE`
- `UNKNOWN_REVIEW_REQUIRED`
- `NOT_APPLICABLE`

`INDEPENDENT_SUPPORTED` requires complete lineage, no detected common ancestry or direct dependency, and a non-empty review reference.

If new provenance is later added and contradicts a previously supported independence claim, the old claim is not erased. Integrity verification reports it as stale and Double-Counting Review emits `INDEPENDENCE_CLAIM_CONTRADICTION_REVIEW_REQUIRED`.

## Double-Counting Review

`double_counting_review(...)` evaluates every pair in one proposed evidence combination. It can emit:

- `DIRECT_DEPENDENCY_REVIEW_REQUIRED`
- `COMMON_ANCESTRY_REVIEW_REQUIRED`
- `INCOMPLETE_LINEAGE_REVIEW_REQUIRED`
- `INDEPENDENCE_CLAIM_MISSING_REVIEW_REQUIRED`
- `INDEPENDENCE_CLAIM_CONTRADICTION_REVIEW_REQUIRED`

The report never changes a scanner weight, stance, portfolio action or order. It only makes the review obligation explicit.

## Phase-7 integration

QM-I reuses the existing Phase-7 semantics instead of replacing `relation_graph.py`.

A validated 7A packet contributes:

- its existing `source_snapshot_id` as the known packet snapshot source;
- each existing `claim_id` as the Claim ID;
- each `source_version` as the claim version;
- every `claim_ref` as a material `REFERENCES` edge.

This makes relationships such as Probability -> referenced Selection/Timing claim or Confidence -> referenced claim directly visible as ancestry. QM-I does not reinterpret these annotations as independent votes.

The packet-level snapshot is known provenance, but the imported claim remains `lineage_complete=False` because a 7A packet alone does not prove the full historical Raw Source -> Feature -> Metric chain.

Phase 7D and 7F currently do not expose native stable Decision/Portfolio-Action IDs. QM-I therefore requires the caller to provide explicit stable IDs for new registrations. It does **not** backfill historical IDs by hashing or guessing. Phase 7H already exposes a native content-addressed `watch_id`; QM-I reuses it unchanged.

## QM-C integration

`register_qm_c_result_chain(...)` imports the exact IDs and hashes from the completed QM-C governance chain:

- hypothesis ID/version/hash
- analysis-plan ID/version/hash when present
- control-plan ID/version/hash when present
- result ID/version/hash

No re-keying is allowed.

## Regression coverage

The QM-I test suite includes:

- a complete typed Raw Source -> Feature -> Indicator -> Score -> Claim -> Calibration -> Decision -> Watch path;
- common-ancestry detection with explicit paths;
- incomplete-lineage fail-closed behavior;
- supported independence only with complete separate ancestry and review reference;
- later provenance invalidating a former independence claim without deleting history;
- material cycle rejection;
- real Phase-7A Claim/claim_ref ingestion;
- real 7D Decision lineage;
- real 7F Portfolio Action lineage;
- native 7H `watch_id` reuse;
- exact QM-C identity reuse for a retained rejected result;
- tamper detection through the hash chain.

## Evidence Impact

QM-I is infrastructure for traceability and review. It does not promote any research rule, change Selection/Timing/Probability/Risk/Confidence/Elliott, alter Decision-Layer outputs, change portfolio actions or generate broker instructions.

QM-A remains authoritative for evidence-consumption state. QM-B remains authoritative for PIT universe and investability constraints. QM-C remains authoritative for hypothesis, analysis-plan, control-plan and result identities.

## Remaining limitations

Historical artifacts that do not already contain stable lineage IDs are not reconstructed by guesswork. This is intentional.

A complete registered graph can rule out *known* shared ancestry inside that graph, but it cannot prove that an undocumented external dependency never existed. `INDEPENDENT_SUPPORTED` therefore also requires an explicit review reference.

QM-B strict historical promotion remains blocked by external listing, market-tradability and execution-channel evidence gaps. QM-I keeps those constraints visible and does not reopen completed QM-B engineering.

## Closure

Final status target:

`QM-I COMPLETE — TYPED EVIDENCE LINEAGE AND DOUBLE-COUNTING REVIEW ACTIVE`

BA-QM3 status target:

`BA-QM3 COMPLETE — EVIDENCE LINEAGE READY FOR DEPENDENCE AND CALIBRATION QM`

Next mandatory work package:

**BA-QM4 / QM-D + QM-E — Dependence, Effective N & Calibration**.
