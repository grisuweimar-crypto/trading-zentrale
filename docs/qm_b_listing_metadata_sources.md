# QM-B – Historical Listing Metadata Source Assessment

Status: **research validation / source gap analysis**  
Productive integration: **disabled**

## Purpose

The previous QM-B boundary audit established that the repository itself contains no explicit historical fields for listing venue, listing date, delisting date or investability. This package therefore evaluates external evidence sources before any strict Instrument Master / Alias Ledger / As-of Membership promotion.

The central rule is that **listing state, venue assignment, market tradability and project investability are different facts**. None may be substituted for another.

## Reused governance

This package reuses the Phase-8 source-governance dimensions:

- coverage;
- PIT status;
- access;
- license;
- publication semantics;
- promotion blockers.

A source is not promotion-ready merely because it contains an IPO date, delisting date or exchange code.

## Field model

The source contract distinguishes:

- `stable_instrument_identity` – security identity independent of ticker;
- `venue_master` – identity/lifecycle of the market venue itself;
- `venue_assignment` – security/listing assigned to a venue;
- `listing_start` – effective start of that represented listing;
- `listing_end` – effective end/delisting;
- `symbol_change` – effective symbol transition;
- `market_tradability` – explicit active/suspended/ceased state;
- `project_investability` – scanner/project-specific eligibility after identity, venue/listing, tradability and project restrictions.

## Source assessment

Seven sources were assessed.

### Nasdaq Daily List

Official Nasdaq source. It documents notifications for new listings, delistings, symbol/name changes and related corporate actions, with historical corporate-action data dating back to 1999. It is the strongest semantic fit among reviewed sources, but access is subscription-controlled and coverage is Nasdaq-specific. It is therefore **not promotion-cleared** for this project.

Documentation: `https://classic.nasdaqtrader.com/Trader.aspx?id=DailyListPD`

### Nasdaq Symbol Directory

Official current symbol-directory files are updated during the trading day and contain file-creation timestamps. This makes them suitable for a **prospective archived snapshot ledger** from collection time onward. The reviewed documentation does not establish a public historical as-of archive for retrospective reconstruction.

Documentation: `https://www.nasdaqtrader.com/Trader.aspx?id=SymbolDirDefs`

### SEC company ticker / exchange associations

SEC provides current CIK/ticker/exchange association files and explicitly states that accuracy or scope is not guaranteed. The documented files are useful as current reference mappings but are not a historical listing-event ledger.

Documentation: `https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data`

### OpenFIGI

OpenFIGI maps identifiers to current FIGI/ticker/exchange/MIC information. The documented mapping request has no historical as-of/vintage parameter. It is useful for current identity/reference work, but not for retrospective strict listing-state reconstruction.

Documentation: `https://www.openfigi.com/api/documentation`

### ISO 10383 MIC

The ISO/SWIFT MIC dataset is authoritative for market-venue identity. It has a documented monthly publication/implementation cycle. It can support a venue master but **does not map individual securities to venues**.

Documentation: `https://www.iso20022.org/market-identifier-codes`

### EODHD exchange / delisted symbol lists

EODHD exposes active and delisted inventories by exchange; its US symbol-change endpoint includes effective dates. The reviewed documentation does not establish immutable historical publication vintages for the exchange-list snapshots, so current delisted inventories cannot by themselves prove what was publicly knowable on an arbitrary earlier scanner date.

Documentation: `https://eodhd.com/financial-apis/exchanges-api-list-of-tickers-and-trading-hours`

### Financial Modeling Prep delisted companies

FMP returns symbol, exchange, IPO date and delisted date in the current delisted-company endpoint. The reviewed documentation does not establish historical publication vintages or an as-of query for when each row became knowable.

Documentation: `https://site.financialmodelingprep.com/developer/docs/stable/delisted-companies`

## Strict promotion result

No reviewed source is simultaneously:

1. field-level `PIT_SAFE` for all required strict listing fields;
2. source-level `SAFE`;
3. access/license-cleared for this project;
4. global enough for the current scanner universe;
5. promotion-eligible without blockers.

Therefore:

- `venue_assignment`: **BLOCKED**;
- `listing_start`: **BLOCKED**;
- `listing_end`: **BLOCKED**;
- strict historical listing ledger: **BLOCKED_SOURCE_GAP**;
- globally promotion-ready source count: **0**.

This is a source limitation, not a license to infer missing facts from suffixes, prices, scanner presence or current mappings.

## Prospective path

The package identifies a safe forward path without retrojection:

- archive official/current source snapshots at ingestion time;
- preserve raw payload/hash/source timestamp/ingestion timestamp;
- never rewrite an old snapshot with newer state;
- keep venue reference, security-to-venue assignment, listing events and tradability separate;
- derive project investability only in a later project-specific rule layer.

Nasdaq Symbol Directory is the strongest open prospective US candidate from this assessment. It does **not** solve the global retrospective problem.

## Safety invariants

- current mappings never back-project;
- event effective date alone is not a publication vintage;
- `delisted=1` current inventories do not become historical as-of truth without archived vintages;
- MIC identity does not become security venue assignment;
- listing does not imply tradability;
- tradability does not imply project investability;
- missing source coverage remains `UNKNOWN`;
- no source is promoted by agreement with ticker suffixes/current universe alone;
- source licensing/access remains part of promotion eligibility.

## Evidence impact

No historical scanner row is altered by this package. The existing QM-B uncertainty classes remain unchanged.

The practical result is architectural: strict historical venue/listing/investability fields must remain unresolved until a source with acceptable PIT, coverage and access/license properties exists. Prospective evidence may be collected from now on without pretending it reconstructs the past.

## Next package

Build source-adapter and immutable prospective-snapshot contracts. That package may begin collecting future evidence, but retrospective strict ledger promotion remains blocked until a suitable historical source is obtained or separately validated.
