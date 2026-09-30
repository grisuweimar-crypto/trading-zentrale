# QM-B – Listing Data Persistence Gate

Status: **research governance**  
Productive integration: **disabled**

## Purpose

This gate separates three questions that must not be conflated:

1. Is a source publicly reachable?
2. Is a source useful for prospective QM research?
3. Is its data explicitly cleared for durable persistence or redistribution in this public GitHub repository?

Only the third question controls public-repository persistence.

## Fail-closed rule

Public repository persistence is allowed only when **both** are true:

- the source assessment says `license_status = USABLE`;
- the source ID is explicitly listed in `explicitly_cleared_source_ids` in `configs/qm_b_listing_persistence_policy_v1.json`.

No source is currently explicitly cleared.

Therefore `nasdaq_symbol_directory`, whose current source assessment is `license_status = REVIEW_REQUIRED`, is blocked from durable public-repository persistence.

## Local ephemeral tests

`local_ephemeral` is available only so parser, hash, PIT and archive mechanics can be tested with controlled inputs. It explicitly does **not** imply license clearance and the resulting data may not be treated by this gate as approved for commit or redistribution.

The archive CLI now requires callers to state the persistence scope explicitly. There is no default.

## Relationship to listing evidence

This gate does not change the semantic value of a source. It only controls persistence.

A source can therefore be:

- methodologically useful but persistence-blocked;
- persistence-cleared but still insufficient for strict historical listing proof;
- both cleared and semantically sufficient;
- neither.

The existing listing-source contract remains authoritative for PIT, coverage and field capabilities.

## Current impact

The As-of Candidate Fusion package remains correctly blocked on real listing evidence. This gate prevents resolving that blocker by committing third-party raw listing files before rights are explicitly cleared.

## Definition of Done

1. public persistence defaults to denied;
2. public availability never implies redistribution clearance;
3. `REVIEW_REQUIRED`, `RESTRICTED` or `UNKNOWN` license status cannot pass the public gate;
4. `USABLE` alone is insufficient without explicit redistribution clearance;
5. local ephemeral tests never imply broader rights;
6. the archive CLI requires an explicit persistence scope;
7. existing local snapshot smoke tests remain possible;
8. no raw third-party listing data is newly committed by this package.
