# QM-B baseline audit – existing controls vs required controls

This is a structural inventory, not a claim that historical ledgers have already
been populated.

| QM-B requirement | Existing protection before QM-B | Baseline status | QM-B treatment |
| --- | --- | --- | --- |
| Current universe must not define history | Phase-8 external coverage contract forbids it | PARTIAL | Promoted to system-wide invariant |
| Stable instrument identity | Current ISIN/symbol fields, SEC CIK bootstrap for US current coverage | PARTIAL | Stable internal ID + effective-dated aliases |
| Historical ticker changes | Current SEC submissions validation only | MISSING generally | Effective-dated identifier aliases |
| As-of universe membership | Preserved scanner observations exist; Phase-8 contract defines fields | PARTIAL | Explicit per-date membership ledger |
| Listing/delisting | Phase-8 contract has fields; no general project ledger | PARTIAL | Required membership lifecycle fields and consistency checks |
| Investability | No common as-of status | MISSING | Explicit INVESTABLE/NOT_INVESTABLE/RESTRICTED/UNKNOWN |
| Provider coverage | Phase 8 has family-specific current/PIT controls | PARTIAL | Common source+family+date ledger |
| Missing outcomes | Research pipelines retain some missingness | PARTIAL | Separate label-versioned availability ledger |
| Historical sector/domain | Elliott market-context assignments already use valid_from/valid_to | PARTIAL | General effective-dated taxonomy ledger |
| Survivorship audit | Phase-8 contract prohibits dropping delisted/current-missing symbols | PARTIAL | Sample audit retains all rows and fails closed on unknown |
| QM-A binding | QM-A requires universe/instrument versions | READY INTERFACE | Deterministic SHA-256 versions supplied by QM-B |

## Baseline findings

### QMB-F01 – current universe master is not a historical ledger

`data/inputs/universe_master.csv` represents current master state. It has no general
historical validity intervals or per-date membership semantics. It must therefore
remain an input/current inventory, not be used to define historical inclusion.

Classification: `METHODOLOGY_RISK`  
Containment: enforced by QM-B contract and current-inventory scope marker.

### QMB-F02 – current SEC identity is intentionally current-only

Phase-8 exact SEC ticker mapping and submissions validation are correctly conservative,
but they validate current SEC identity. They do not prove that the same ticker mapped
to the same security at arbitrary historical dates.

Classification: `OBSERVATION` / historical gap  
Containment: historical aliases require their own PIT evidence.

### QMB-F03 – research-history completeness is not membership completeness

The existing research-data architecture validates complete scanner runs using symbol
counts, schema and snapshot consistency. Its own documentation notes that this is a
conservative plausibility check, not proof that a changed universe is complete.

Classification: `METHODOLOGY_RISK`  
Containment: QM-B separates observed historical scanner rows from intended universe
membership and represents unresolved membership explicitly.

### QMB-F04 – historical taxonomy mechanisms exist but are not general

Market-context assignments already implement `valid_from`, `valid_to` and PIT flags.
That is a reusable pattern, but it is specific to market context rather than a common
instrument taxonomy registry.

Classification: `PASS` for reusable pattern / `PARTIAL` for system-wide coverage.

### QMB-F05 – current universe has duplicate and incomplete identifiers

The dedicated QM-B CI inventory of the current `data/inputs/universe_master.csv`
reported:

- 222 active rows;
- 213 unique active symbols;
- 9 duplicate-symbol keys;
- 9 duplicate-ISIN keys;
- 6 active rows with no ISIN.

These counts are a current-state identity/data-quality finding only. They do not by
themselves prove that the duplicates are erroneous securities, nor that historical
research is biased. They do prove that current row count cannot safely be treated as
stable-instrument count and that identity resolution must occur before historical
membership reconstruction.

Classification: `DATA_QUALITY_EVENT` / `METHODOLOGY_RISK`  
Containment: do not deduplicate by ticker alone; resolve the affected rows through
stable instrument identity during historical ledger population. No productive row is
automatically removed or merged by this finding.

## Evidence impact

No historical evidence is invalidated by creating QM-B infrastructure alone. No
outcomes were inspected to redesign a trading rule. The work is preventive QA and
adds a validation layer around future historical dataset construction.

Actual historical reconciliation may later identify rows whose identity, membership,
coverage or outcome availability is wrong. Those findings require separate QM-A/QM-H
evidence-impact handling; they are not pre-judged here.
