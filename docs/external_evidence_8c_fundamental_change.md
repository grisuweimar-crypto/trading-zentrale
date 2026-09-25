# Phase 8C-C – Fundamental Change Engine

Status: **ACTIVE – feature construction only, outcomes disabled**

## Objective

8C-C converts PIT-safe, accession-versioned SEC facts into deterministic fundamental change observations. It does not assign bullish/bearish meaning and it does not inspect future returns.

## Comparison bases

Two views are kept separate:

### FIRST_RELEASE
For a given fact context, retain only the earliest PIT-safe accession/publication. This is the default research basis because it best represents what was first publicly knowable.

### ASOF_LATEST_VERSION
For an explicit historical `as_of` timestamp, retain the latest version whose `valid_from` was already reached. A later amendment may therefore alter a later snapshot, but it can never rewrite an earlier one.

These views may not be mixed silently.

## Explicit metric mapping

The first implementation maps only explicitly configured `us-gaap` concepts into metric families. Company-specific custom tags are left unmapped rather than guessed.

Current families include revenue, operating income, net income, diluted EPS, operating cash flow, capex, cash, assets, equity and debt.

This deliberately sacrifices coverage before it sacrifices semantic validity.

## Comparable YoY changes

The initial YoY engine requires:
- same entity;
- same taxonomy;
- same exact concept;
- same unit/currency;
- end dates approximately one year apart;
- similar period duration;
- matching fiscal period when both rows provide one;
- PIT-safe accession provenance.

No cross-concept continuity is assumed merely because two concepts both belong to the same broad family. Broader concept harmonisation is a later coverage problem and must be frozen before outcome inspection.

## Derived values

### Operating margin
`operating_income / revenue`

Both components must come from the same accession, same unit, same start and same end. The engine refuses to combine components from different filing vintages.

### Free cash flow
`operating_cash_flow - capex`

Again, both components must share accession, unit and period context.

### Debt / equity
The contract registers debt-to-equity as a future candidate, but the engine does not yet calculate it. SEC debt concepts can represent total long-term debt or current/noncurrent components, and naïve summation could double count. A deterministic debt-concept resolution rule must be designed and coverage-tested first.

## No semantic direction yet

Every numeric feature emitted by 8C-C has `direction=UNASSIGNED`.

Examples:
- +20% revenue growth is not automatically positive evidence;
- improving operating margin is not automatically bullish;
- positive FCF is not automatically a BUY input;
- negative EPS change is not automatically bearish.

Those relationships are hypotheses for later Phase 8G research, not assumptions in the feature builder.

## Missing and edge cases

Zero prior values do not create infinite growth; percentage change becomes unavailable with an explicit `ZERO_PREVIOUS_VALUE` reason.

Facts without valid accession/publication provenance do not enter the change engine. Different units/currencies are not converted automatically. Custom XBRL tags are not inferred into standard families.

## Next 8C-C slices

1. quantify concept-family coverage before expanding mappings;
2. add revenue-growth acceleration from two consecutive YoY observations;
3. add YoY margin and FCF changes;
4. design non-double-counting debt resolution before leverage features;
5. determine whether IFRS filers can be supported with a separately validated `ifrs-full` mapping;
6. freeze feature definitions and coverage domain before any outcome join.

Only after those steps does 8C-D begin structured filing-content extraction for guidance, dividends, buybacks, capital raises and similar events.
