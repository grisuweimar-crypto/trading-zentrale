# QM-B Crypto Stable-Object Semantics

## Purpose

Historical crypto identifiers in the scanner changed namespace over time. Examples include provider-style quote pairs such as `BTC-USD` and project-internal references such as `CRYPTO:BTC`.

This research-only layer prevents those forms from being treated as the same stable instrument merely because they share the token `BTC`.

## Object model

### INTERNAL_BASE_REFERENCE

`CRYPTO:<BASE>` is a project namespace reference to a base crypto concept. It is not, by itself, a provider instrument, venue listing or executable trading pair.

### QUOTE_PAIR_REFERENCE

`<BASE>-<QUOTE>` preserves both components. `BTC-USD` and `BTC-EUR` therefore remain distinct semantic references. The provider and venue are unknown unless separately evidenced.

Both object classes may point to the same `base_component_id`. That relationship means only that they reference the same textual base component; it is not stable-identity proof.

## Prohibited inferences

The contract forbids:

- `CRYPTO:BTC == BTC-USD`;
- `BTC-USD == BTC-EUR`;
- dropping quote currency;
- inferring provider or venue from a pair string;
- rewriting historical identifiers to the current namespace;
- using pair presence to prove membership, listing, tradability or investability;
- promoting a stable crypto asset identity without separate authoritative evidence.

## Audit

The frozen audit consumes `history_analysis.csv` and `universe_master.csv` at their pinned hashes and reuses the existing conservative crypto identifier parser/timeline. It reports every observed identifier form, object class, base/quote components, time range and row count.

The output is a semantic inventory. It deliberately leaves stable asset identity and provider-instrument identity `PARTIAL_BY_DESIGN` until a later source can prove those objects.

No scanner score, historical observation, Decision Layer output, portfolio action or execution behavior is changed.
