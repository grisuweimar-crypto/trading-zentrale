# CY-02 / CY-03: provider vintage feasibility — 2026-11-25

## Verified documentation and decision

- yfinance download: https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html
  start/end restrict market dates; no documented historic knowledge-cutoff parameter.
  No claim that Yahoo can serve old response vintages.
- EODHD methodology: https://eodhd.com/help/methodology
  Explicitly current data only; corrections overwrite records without change timestamps.
  Not an independent historical vintage solution.
- Databento trades: https://databento.com/docs/schemas/trades
  Event and capture timestamps, venue publisher ID. Daily aggregates:
  https://databento.com/docs/schemas/ohlcv — UTC daily bins not necessarily exchange sessions.
  Corporate actions: https://databento.com/docs/api-reference-data/corporate-actions
  retain all event records for PIT via API. Candidate for independent reconstruction of
  US pilot using session-corrected trade data and time-appropriate corporate actions;
  not proof of Yahoo's original adjusted responses.
- OHLC: https://ohlc.io/data/historical-stock-prices
  Documents as_of knowledge cutoff at UTC end of day, but actual retained vintage
  coverage, provenance and ability to resolve exact scanner cutoff must be tested.
  Provider documentation search result only; full page unavailable in this review.

## Immediate evidence recovery

Existing archive contains exact 60-day input vectors and actual normalized prefetch
responses. Audit exact adjusted-close equality using only captures that finished
at or before the individual observation's cycle_as_of; preserve distinctions:
never captured, matching at least one earlier capture, differing captures only,
other vintages also observed. Same ticker only; no tolerance, cross-provider aliases,
raw close fallback or reconstructed quote currencies. Multiple matching receipts do
not constitute independent market evidence. This audit does not qualify a row.

Execute: python scripts/audit_cycle_client_vintage.py --output /tmp/client-vintage.json
CI executes on actual archive and keeps a report; no credentials or paid requests.

## Pilot specification for independent source

Preselect UEC and TTD, 60 sessions before the first usable snapshot through
2026-11-25; do not inspect forward outcomes or select by observed returns.
Request source's original trade/session close with publisher/receive timestamps,
original quote units, point-in-time corporate-action versions, corrections and
coverage. First obtain sample and licensing terms. Compare full input vectors,
record revisions and discrepancy, do not conflate providers, do not substitute
a new series into a historical Yahoo snapshot. Any alternate independently
reconstructed oscillator must have its own source and version, reviewed separately.
No credentials, subscription, external request or independent signed review
has been obtained here. No change to #269/#290 or CY-05 gates.

## Observed cross-source checks and missing session diagnosis

The actual normalized witness for snapshot a40e915d-71d6-4ba3-89f8-f2c8ae53cf21,
request 2026-11-18 to exclusive 2026-11-25, finished 2026-11-25T01:36:05.507387Z,
was decoded and checked against its frame row count and gzip CRC. For UEC and
TTD the response supplied 18, 19, 20 and 23 November only. All eight closes
agree to the displayed cent with the current S&P Global Market Intelligence
historical tables at https://stockanalysis.com/stocks/uec/history/ and
https://stockanalysis.com/stocks/ttd/history/ (last updated 24 Nov).
The external source also reports 24 Nov close UEC USD 5.57, TTD USD 14.07;
that date is absent from the actual prefetch response and both cycle_quality
rows name 23 Nov as last_bar. The archived prices for 23 Nov are correct to
displayed precision, not expired or retroactively overwritten. This is
partial retrospective cross-source agreement only, not complete 60-bar
coverage or proof of the historical provider publication time.

The pilot now detects a missing expected completed US exchange session and
uses a bounded targeted request through the existing validated acquisition
path. A full-year capture stays evidence-only. Existing archive entries
remain unchanged; failures remain explicitly missing. This repair requires
the new calendar dependency and production merge.
