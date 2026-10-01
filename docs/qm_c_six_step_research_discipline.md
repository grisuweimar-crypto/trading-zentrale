# QM-C — Six-step Research Discipline Chain

QM-C is implemented as exactly six auditable work packages. The split is architectural, not cosmetic: each package owns one governance responsibility and exposes stable IDs/hashes to the next package.

1. **QM-C1 — Hypothesis Registry Core**
   - stable hypothesis IDs and immutable version hashes
   - Discovery vs Confirmation
   - lifecycle binding to QM-A

2. **QM-C2 — Analysis Plan Contract & Freeze**
   - immutable analysis-plan identity
   - primary estimand/metrics, population, universe, windows, exclusions and sensitivities
   - confirmation readiness only after exact QM-A/QM-B binding

3. **QM-C3 — Hypothesis Families & Multiplicity**
   - exact family membership
   - predeclared multiplicity treatment
   - no sequential look schedule and no outcome inspection

4. **QM-C4 — Sequential Monitoring**
   - separate monitoring-plan ID/version/hash
   - exact binding to a frozen QM-C3 control-plan ID/version/hash
   - only predeclared, ordered looks
   - repeated looks require consumed/evaluated evidence state in QM-A

5. **QM-C5 — Negative / Rejected Results Registry**
   - symmetric retention of positive, negative and inconclusive results
   - rejected/retired hypotheses are retained without invented outcome evidence
   - confirmatory results bind exactly to C1+C2+C3+C4+QM-A

6. **QM-C6 — Closure Gate & Übergabe**
   - validates all six manifests and the full identity chain
   - deterministic duplicate-hypothesis review without auto-merge
   - creates the BA-QM2 handoff while preserving QM-B external blockers
   - does not implement BA-QM3 or any later QM axis

## Canonical identity path

`hypothesis_id/version/hash`
→ `analysis_plan_id/version/hash`
→ `control_plan_id/version/hash`
→ `monitoring_plan_id/version/hash`
→ `result_id/version/hash`

QM-A analysis identity and QM-B universe/instrument versions remain authoritative dependencies throughout.

## Scope guards

- No scanner, Selection, Timing, Probability, Risk, Confidence, Elliott or Decision-Layer semantics are changed by QM-C.
- Missing/UNKNOWN evidence is never converted to neutral evidence.
- QM-B strict historical promotion remains blocked by its documented external evidence gaps.
- Engineering completion does not claim empirical promotion.
- Work beyond QM-C requires a separate user authorization; QM-C6 does not silently start BA-QM3.

## Final status

`QM-C COMPLETE — SIX-STEP RESEARCH DISCIPLINE CHAIN ACTIVE`
