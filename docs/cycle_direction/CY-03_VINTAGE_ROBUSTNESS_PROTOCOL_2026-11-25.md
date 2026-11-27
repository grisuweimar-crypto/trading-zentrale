# CY-03 historical input robustness diagnostic

25 November 2026 — no outcome inspection, no release.

Baseline vintage audit run 45571588196: 13,957 valid windows, 837,420 inputs;
160,701 exact previously seen, 648,527 unseen, 28,192 different-only,
46/65 snapshots with receipts. Eight full exact windows. These count
any earlier matching value, not necessarily the latest prior value.

## New comparison
Always use the same archived 60 session dates and exact production cycle formula.
Replay the baseline, then distinguish:
1. SINGLE_RESPONSE: latest actual earlier successful response covering all 60.
2. MULTI_RESPONSE: latest earlier captured value per date, all 60 available.
3. PARTIAL_HYBRID: replace only captured dates, keep archived inputs elsewhere.
Modes 2 and 3 are sensitivity scenarios, not original source vintages.
Never mix across symbols or use a response after the cycle cutoff.
Use no forward returns, no adaptive tolerance to improve results.

Record absolute relative price deviations in basis points (0.1/1/10/100
thresholds), scale-ratio dispersion (1bp exploratory criterion), absolute cycle
point differences, 25/50/75 band changes, below-25 changes and valid 1/5/10obs
canonical chain direction/primary below-25+UP changes for full modes only.
Zero chains means unavailable, not zero risk. Comparisons overlap and do not
provide independent statistical samples. Uniform scaling is mathematically
invariant, but observed ratios do not identify split/dividend causes.
Do not generalize partial-hybrid results to missing prices.

Run: python -m scripts.audit_cycle_vintage_robustness --output /tmp/cycle-vintage-robustness.json
CI publishes actual results. Existing research gate stays closed.

## Full-window capture pilot (implemented, disabled by default)
Set SCANNER_CYCLE_FULL_WINDOW_PILOT=UEC,TTD on the pre-scoring input step
only after review. The extra one-year normalized Yahoo adjusted-close response
is appended to the existing timestamped witness attempts, NOT merged into the
market cache, and is bound by the existing snapshot witness mechanism.
A failed pilot does not replace normal inputs. No external quote unit, venue
or raw HTTP proof is claimed. Independent publisher PIT and quote metadata
remain separate work. The pilot has not been enabled or executed in production.
