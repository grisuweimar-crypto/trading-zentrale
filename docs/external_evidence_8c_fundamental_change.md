# Phase 8C-C – Fundamental Change Engine

Status: **ACTIVE – coverage + feature construction, outcomes disabled**

## Objective

8C-C converts PIT-safe, accession-versioned SEC facts into deterministic fundamental change observations. It does not assign bullish/bearish meaning and it does not inspect future returns.

## Comparison bases

Two views remain separate:

### FIRST_RELEASE
For a given fact context, retain only the earliest PIT-safe accession/publication. This is the default research basis because it best represents what was first publicly knowable.

### ASOF_LATEST_VERSION
For an explicit historical `as_of` timestamp, retain the latest version whose `valid_from` was already reached. A later amendment may therefore alter a later snapshot, but it can never rewrite an earlier one.

These views may not be mixed silently.

## Explicit metric mapping

The implementation maps only explicitly configured `us-gaap` concepts into metric families. Company-specific custom tags remain unmapped rather than being guessed from label/name similarity.

Current families include revenue, operating income, net income, diluted EPS, operating cash flow, capex, cash, assets, equity and debt.

Where a metric family declares allowed units, rows outside those units are not mapped.

## Concept coverage measurement

Coverage is now a first-class 8C-C artifact rather than an implicit side effect.

`measure_concept_coverage()` reports:
- total and PIT-usable rows;
- mapped share of usable and US-GAAP facts;
- metric-level row counts;
- metric-level entity counts;
- mapped concepts by metric;
- unmapped US-GAAP concepts;
- custom-taxonomy rows and taxonomies.

Coverage is descriptive only. It may not select concepts, domains or thresholds from market outcomes. Unmapped rows remain visible instead of disappearing from the denominator.

This means a future decision to expand concept mappings must be justified by accounting semantics and coverage needs before any return inspection.

## Comparable YoY changes

The YoY engine requires:
- same entity;
- same taxonomy;
- same exact concept;
- same unit/currency;
- end dates approximately one year apart;
- similar period duration;
- matching fiscal period when both rows provide one;
- PIT-safe accession provenance.

No cross-concept continuity is assumed merely because two concepts belong to the same broad metric family.

## Revenue growth acceleration

Revenue growth acceleration is now implemented as:

`current YoY growth - prior YoY growth`

The two growth observations must form a true consecutive chain: the current observation's `previous_end` must equal the prior growth observation's `current_end`. The engine does not interpolate missing years or splice unrelated periods.

The arithmetic sign remains `direction=UNASSIGNED`.

## Derived values and changes

### Operating margin
`operating_income / revenue`

Both components must come from the same accession, same unit, same start and same end.

### Operating margin YoY change
The engine now calculates an absolute YoY margin change only between comparable duration contexts. A positive change is not automatically positive evidence.

### Free cash flow
`operating_cash_flow - capex`

Both components must share accession, unit and period context.

### Free cash flow YoY change
The engine now calculates absolute YoY FCF change between comparable periods. It intentionally does not force percentage growth across zero/negative FCF regimes.

## Deterministic debt resolution

Debt-to-equity is now calculable, but only after a conservative debt-resolution step.

Within one CIK/accession/end/unit context, only two representations are accepted:

1. **TOTAL_ONLY** – exactly one `LongTermDebt` fact and no current/noncurrent component facts;
2. **CURRENT_PLUS_NONCURRENT** – no `LongTermDebt` total, exactly one `LongTermDebtCurrent` and exactly one `LongTermDebtNoncurrent` fact.

The engine deliberately refuses to decide which representation is authoritative when total and components coexist. Such a context becomes `CONFLICTING_SOURCES`. Duplicate concepts are also conflicts; incomplete component pairs remain `UNKNOWN`.

This prevents the common silent error of summing a debt total together with its current/noncurrent components.

### Debt to equity
`resolved_debt / stockholders_equity`

Equity must match the same accession, end and unit. Missing/ambiguous equity remains explicit. Zero equity does not generate an infinite ratio.

### Debt-to-equity YoY change
The same absolute-change engine can compare instant debt/equity ratios approximately one year apart. Direction remains unassigned.

## No semantic direction yet

Every numeric feature emitted by 8C-C has `direction=UNASSIGNED`.

Examples:
- +20% revenue growth is not automatically positive evidence;
- accelerating growth is not automatically bullish;
- improving operating margin is not automatically bullish;
- positive or improving FCF is not automatically a BUY input;
- falling debt/equity is not automatically positive evidence.

Those relationships are hypotheses for later Phase-8 outcome research, not assumptions in the feature builder.

## Missing and edge cases

Zero prior values do not create infinite growth; percentage change becomes unavailable with an explicit `ZERO_PREVIOUS_VALUE` reason.

Facts without valid accession/publication provenance do not enter the change engine. Different units/currencies are not converted automatically. Custom XBRL tags are not inferred into standard families.

Debt ambiguity is never silently resolved. Missing mappings remain part of coverage reporting.

## Remaining 8C-C work before feature/domain freeze

1. run concept/entity coverage on the actual SEC-covered project universe;
2. decide mapping expansion only from semantic/coverage evidence, without outcomes;
3. determine whether IFRS filers can be supported with a separately validated `ifrs-full` mapping;
4. verify whether the selected US-GAAP feature domain has adequate cross-sector and time coverage;
5. freeze feature definitions, accepted concepts and validated domain before any outcome join.

Only after those steps does 8C-D begin structured filing-content extraction for guidance, dividends, buybacks, capital raises and similar events.
