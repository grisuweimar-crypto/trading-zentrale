# Phase 7B — Prospective typed-evidence archive

## Why the archive exists

The historical Decision Research Dataset can replay Selection, frozen Timing
patterns and raw stored Risk state. It cannot honestly recreate model evidence
that did not yet exist at the old observation date.

In particular, Phase 7B must not attach today's:

- calibrated Probability evidence,
- Confidence-vNext evidence,
- Elliott-vNext 6H output,
- later validation maturity,

to older scanner rows merely because those values are available now.

At the same time, these evidence families must become available for genuine
future Phase-7 research. Phase 7B therefore adds a separate prospective JSONL
archive for validated Phase-7A packets:

`artifacts/research/decision_evidence_7a.jsonl`

The archive may legitimately be absent until the first future packet exists.
That state is reported as explicit zero coverage, not treated as an error and
not replaced by synthetic evidence.

## Freeze and spend boundary

The canonical prospective-unspent start is **2026-09-26**.

By default, `archive_decision_input_7a.py` rejects packets earlier than that
date. A pre-start packet can be stored only with the explicit `--allow-spent`
flag, and it remains labelled `legacy_replay_spent`; that flag can never convert
spent evidence into unspent evidence.

## Validation

Every archive entry is passed through the complete Phase-7A validator before it
is accepted. Therefore the archive inherits the 7A guards:

- no future evidence relative to packet `as_of`,
- Probability cannot become an independent directional vote,
- Risk cannot encode a stance/vote,
- Confidence cannot encode attractiveness/direction,
- Elliott 6H must retain its research-only and no-order integration guards,
- portfolio/action/order fields remain forbidden.

Duplicate `(source_snapshot_id, symbol, as_of)` identities fail closed.

## Why the archive is a sidecar rather than a date join

A scanner research row currently has a market/session date, while the Phase-7A
packet has an exact `as_of` plus `source_snapshot_id`.

Date-only joining is unsafe. On a day with multiple scanner runs or a later
intraday packet, a naive date join could attach evidence generated after the
specific scanner observation and create look-ahead leakage.

For that reason Phase 7B does **not** flatten prospective packets into historical
rows merely on matching calendar date. The packet archive remains a sidecar
until `source_snapshot_id` can be matched to a sufficiently strong scanner
snapshot identity.

This is a deliberate fail-closed design, not missing implementation.

## Archive metadata

The archive reader reports:

- packet count,
- distinct symbols,
- distinct source snapshots,
- first/last `as_of`,
- spent/unspent partition counts,
- evidence-family counts,
- 7A admission-state counts,
- explicit flags that retrojection, evidence fusion, Universal Stance and
  Portfolio Action were not performed.

The standard 7B dataset CLI includes this archive summary in
`decision_research_7b_metadata.json`.

## Append one future packet

```bash
python scripts/archive_decision_input_7a.py \
  --input packet.json \
  --archive artifacts/research/decision_evidence_7a.jsonl
```

The file is rewritten deterministically after each validated append. This keeps
archive ordering reproducible and catches duplicate identities before they can
enter the research record.

## Relationship to Phase 7C

Phase 7C can use the historical spent table for discovery and diagnostics, but
full six-family evidence combinations cannot be claimed historically where the
underlying evidence did not exist.

As prospective packets accumulate, later Phase-7 evaluation may align them to
scanner snapshots by `source_snapshot_id` and evaluate genuinely out-of-sample
interactions. The archive therefore supplies the missing bridge from the 7A
contract to future empirical Decision-Layer validation without weakening the PIT
rules.
