# QM-B – As-of Candidate Evidence Fusion

Status: **research validation**  
Productive integration: **disabled**

## Purpose

This package fuses three already separated evidence classes:

1. stable instrument identity;
2. observed project-universe membership;
3. prospective listing evidence.

The output is a **pre-strict As-of Universe candidate**, not a productive universe row.

## Required alignment

A positive fused candidate requires all of the following:

- membership status = `OBSERVED_IN_PROJECT_UNIVERSE`;
- identity match status = `MATCHED`;
- the identity-match universe snapshot ID equals the membership universe snapshot ID;
- the identity-match universe SHA-256 equals the membership universe SHA-256;
- the identity-match universe observation time equals the membership observation time;
- listing snapshot ID/source ID match the identity-match source;
- listing state = `LISTED_IN_SNAPSHOT`;
- a non-empty source-provided venue code exists.

No ticker suffix, name, punctuation rewrite or current metadata is allowed to fill a gap.

## PIT boundary

For a fused candidate:

`candidate_valid_from = max(membership_valid_from, listing_valid_from, identity_valid_from)`

No evidence may be moved backwards.

## Fail-closed states

The package can return:

- `FUSED_LISTED_MEMBER_CANDIDATE`
- `BLOCKED_NO_LISTING_EVIDENCE`
- `BLOCKED_NO_IDENTITY_MATCH`
- `BLOCKED_MEMBERSHIP_NOT_POSITIVE`
- `BLOCKED_IDENTITY_MISMATCH`
- `BLOCKED_LISTING_NOT_POSITIVE`
- `BLOCKED_VENUE_MISSING`
- `REVIEW_REQUIRED_DUPLICATE_LISTING_VENUE`

Missing listing evidence is an expected audit result and is not replaced by inference.

## Strict-bundle boundary

Even `FUSED_LISTED_MEMBER_CANDIDATE` remains:

- `market_tradability_status = UNKNOWN`
- `project_investability_status = UNKNOWN`
- `strict_bundle_promotion_ready = false`

This package therefore never writes `UniverseIntegrityBundle.universe_membership` and never promotes `UNKNOWN` investability to `INVESTABLE`.

## Current repository interpretation

The repository currently contains a durable prospective membership archive but no real archived listing snapshot under `artifacts/research/qm/`. Therefore the current real audit must return zero fused listing+membership candidates and classify positive membership claims as `BLOCKED_NO_LISTING_EVIDENCE` until a real PIT-safe listing snapshot exists.

## Definition of Done

1. evidence-source IDs and hashes align before fusion;
2. PIT times align and candidate validity starts at the latest required evidence time;
3. positive membership and positive listing are both explicit;
4. missing listing evidence fails closed;
5. missing venue fails closed;
6. duplicate same-instrument/same-venue listing records require review;
7. no negative membership is inferred from absence;
8. no tradability or investability is inferred;
9. no strict bundle or productive scanner promotion occurs;
10. real current-repository audit confirms the present listing-evidence gap rather than hiding it.
