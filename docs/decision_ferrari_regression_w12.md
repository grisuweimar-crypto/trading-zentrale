# W12 – Ferrari Regressionstest

W12 ist ein reiner Regression- und Schutzschritt. Er führt keine neue Investmentlogik, keinen neuen Schwellenwert, keinen Ersatz-Score und keine neue Watch-Architektur ein.

## Verbindliche Fälle

Die vier W12-Fälle sind:

1. **F1 – überdehnt + übergeordnet positiv**  
   Erwartung: kein unnötiger Verkauf. Die bestehende 7F/W8-Kette bleibt bei `HOLD`, solange Überdehnung allein vorliegt.

2. **F2 – überdehnt + Dynamik kippt**  
   Erwartung: ein früher `REDUCE_REVIEW` ist möglich. Der vorhandene Zustand `overextension_with_momentum_loss` darf bei weiterhin bestätigter positiver übergeordneter Evidence den bestehenden Review-State von `HOLD` auf `REDUCE_REVIEW` routen.

3. **F3 – Korrektur bereits erfolgt + übergeordnet weiterhin positiv**  
   Erwartung: kein verspätetes Verkaufssignal nur wegen früherer Überdehnung. Ein ausschließlich aus `profit_protection_review` stammender später `REDUCE_REVIEW` wird nach `post_overextension_correction` auf `HOLD` zurückgeführt. Unabhängige strukturelle Reduktions-Evidence bleibt davon unberührt.

4. **F4 – übergeordnete Evidence kippt bestätigt negativ**  
   Erwartung: `EXIT_REVIEW`. Bestätigte negative übergeordnete Evidence bleibt auch nach einer vorherigen Korrektur maßgeblich.

## Bestehende Produktionslogik

W12 nutzt unverändert:

`Phase 7E transition -> Phase 7F portfolio action -> W7 state/history context -> W8 depot action policy`

Die fachliche Umsetzung liegt bereits in `src/scanner/research/decision_layer/depot_action_policy.py` und trägt den vorhandenen Vertrag `ferrari_f1_f4_v1`.

W12 friert diesen Vertrag nun als eigenständige Regression ein. Die erwartete Sequenz lautet:

`F1 HOLD -> F2 REDUCE_REVIEW -> F3 HOLD -> F4 EXIT_REVIEW`

## Schutzinvarianten

Für alle vier Fälle gilt zusätzlich:

- Universal Stance wird durch W8 nicht verändert.
- State/History wird nicht zu einem neuen directional vote.
- Es entsteht kein neuer Indikator-Schwellenwert.
- Es entsteht keine neue Market Evidence.
- Es entsteht keine Brokerorder.
- `execution_allowed` bleibt `False`.
- W12 fügt keinen Super-Score und keine Parallelentscheidung hinzu.

## Tests

Der eigenständige Regressionstest liegt in:

`tests/test_w12_ferrari_regression.py`

Er prüft F1–F4 einzeln sowie die komplette Sequenz `HOLD -> REDUCE_REVIEW -> HOLD -> EXIT_REVIEW` gegen die reale bestehende 7E/7F/W7/W8-Logik.
