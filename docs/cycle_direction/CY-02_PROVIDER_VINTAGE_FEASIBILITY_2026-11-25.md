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
