# QM-F / BA-QM5 — Stateful Decision-Layer Incremental Ablation

QM-F evaluates the incremental research value of the frozen Decision-Layer ladder without changing Phase-7I conclusions or productive behavior.

## Candidate ladder

- `B0_FLAT`: remain flat.
- `B0_LONG`: remain fully long.
- `B1`: Selection-only research reference.
- `B2`: Selection plus eligible Timing without Decision-Layer hysteresis.
- `B3`: raw Universal Stance.
- `B4`: Universal Stance plus hysteresis.
- `B5`: Portfolio Action core without Elliott swing adjustment.
- `B6`: same Portfolio Action core with the registered Elliott swing adjustment.

## Stateful evaluation

Portfolio actions are path-dependent. QM-F therefore evaluates an explicit exposure path rather than treating later claims as independent paired observations after actions diverge. Every step carries the point-in-time snapshot identity, exposure before/after, realized asset return, transaction-cost assumption, eligibility, tradeability and action availability. State discontinuities and impossible state changes fail closed.

Turnover costs are applied to absolute exposure changes. `REALIZED_POLICY_VALUE` additionally requires an explicit benchmark; no benchmark is silently invented.

## B5 vs B6

B5/B6 interpretation is blocked unless both arms share:

- common starting state,
- common observation grid,
- identical eligibility,
- identical tradeability,
- identical action availability,
- identical transaction-cost model,
- identical realized asset-return input,
- QM-I core lineage equality after removing only explicitly registered B6 Elliott-adjustment nodes.

Different labels are not sufficient evidence of equivalence. Missing or incomplete QM-I lineage fails closed.

## Boundaries

QM-F is research-only. It does not retrain Selection, Timing, Probability, Confidence, Risk or Elliott; does not alter scanner weights, Universal Stance, hysteresis, Portfolio Action semantics or orders; and does not relabel Phase-7I validation. No empirical promotion is claimed.
