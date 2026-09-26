# 8C-I annotation template

For each sampled anchor, record:

- `anchor_id`
- `family`
- `truth_label`
- `review_status`
- `market_outcomes_seen` (must be `false`)
- `notes`

DIVIDEND additionally records truth values for amount per share, currency, security class and frequency. BUYBACK additionally records truth values for action, authorization amount and currency.

`UNCERTAIN` is permitted but never counted as correct. Market outcomes must remain hidden during review.
