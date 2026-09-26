# Phase 8C-G – Content Parser Foundation

## Objective

8C-G turns accession-bound SEC full-submission text into **verifiable evidence anchors**. It still does not classify issuer actions such as `GUIDANCE_RAISE`, `DIVIDEND_CUT` or `BUYBACK_AUTHORIZATION`.

The purpose of this layer is to make later semantic extraction auditable instead of allowing an opaque parser or language model to emit an unsupported label.

## Stage 1 – SEC document decomposition

Each full-submission file is split by SEC `<DOCUMENT>` blocks. For every block the parser retains:
- document type;
- sequence;
- filename;
- description;
- normalized visible text.

Script/style content and markup are removed from the searchable text. Non-text document types such as graphics, ZIP/Excel and XBRL/XML are excluded from evidence-anchor extraction.

No exhibit is declared authoritative merely because of its filename or sequence.

## Stage 2 – literal evidence anchors

The first parser version searches explicit phrase families for:
- guidance/outlook;
- dividends;
- share repurchases;
- financing/capital raises.

A hit records:
- family;
- exact matched phrase;
- document metadata;
- surrounding text excerpt;
- exact character location in normalized document text;
- SHA-256 of the excerpt;
- parser version.

Every hit has:

`semantic_status = ANCHOR_ONLY`

and

`market_direction = UNKNOWN`

## What an anchor does NOT mean

Examples:
- the word `guidance` does not prove guidance was raised or cut;
- `quarterly dividend` does not prove the dividend changed;
- `share repurchase program` does not prove a new authorization or expansion;
- `public offering` does not prove a financing was completed;
- keyword frequency is not a trading signal.

The later semantic layer must use the 8C-E comparability/evidence contract to turn anchors into specific issuer-side events.

## Reproducibility

The runner consumes only filing bytes already present in the verified offline SEC snapshot. It therefore makes no network requests and can reproduce the same anchors from the same snapshot and parser version.

Run:

```bash
python scripts/run_external_evidence_8c_content_anchors.py \
  --snapshot-dir artifacts/external_evidence/sec_snapshot \
  --output artifacts/research/external_evidence_8c_content_anchors.json
```

## Research boundary

8C-G does not:
- inspect future returns;
- classify positive/negative market impact;
- perform free-form LLM inference;
- calculate analyst-consensus beat/miss;
- modify Phase 7;
- create portfolio actions.

The next semantic-parser slice may use these evidence anchors, but every promoted semantic record must remain accession-bound and satisfy the 8C-E provenance/comparability rules.
