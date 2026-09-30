#!/usr/bin/env python3
from __future__ import annotations

from bisect import bisect_right
from collections import Counter, defaultdict
import argparse
import csv
from datetime import date
from hashlib import sha256
import json
from pathlib import Path

from scanner.data.price_history import validated_rows
from scanner.reports.research_views import observed
from scanner.research.governance.qm_b_provider_outcome_availability import load_provider_outcome_contract


def digest(path: Path) -> str:
    h = sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path):
    with path.open('r', encoding='utf-8-sig', newline='') as f:
        return list(csv.DictReader(f))


def events(rows):
    seen = set()
    out = []
    for order, row in enumerate(rows):
        if not observed(row):
            continue
        symbol = str(row.get('symbol') or '').strip()
        raw = str(row.get('date') or row.get('as_of') or '').strip()
        if not symbol or not raw:
            continue
        try:
            day = date.fromisoformat(raw)
        except ValueError:
            continue
        key = (symbol, day)
        if key in seen:
            continue
        seen.add(key)
        out.append({'symbol': symbol, 'obs_date': day, 'order': order})
    return out


def price_index(rows, audit_as_of: date):
    valid, issues = validated_rows(rows)
    dates = defaultdict(list)
    closes = {}
    for row in valid:
        day = date.fromisoformat(row['date'])
        if day > audit_as_of:
            continue
        symbol = str(row['symbol']).strip()
        dates[symbol].append(day)
        closes[(symbol, day)] = float(row['close'])
    return {s: sorted(set(ds)) for s, ds in dates.items()}, closes, issues


def map_session(symbol, obs_day, dates, max_age=7):
    series = dates.get(symbol, [])
    if not series:
        return None, 'NO_PRICE_SERIES'
    pos = bisect_right(series, obs_day) - 1
    if pos < 0:
        return None, 'NO_PRIOR_SESSION'
    session = series[pos]
    age = (obs_day - session).days
    if age > max_age:
        return None, 'PRIOR_SESSION_OLDER_THAN_7D'
    if session == obs_day:
        return session, 'EXACT_SESSION'
    return session, 'PREVIOUS_SESSION_WITHIN_7D'


def horizon_available(symbol, session, horizon, dates, audit_as_of):
    series = dates.get(symbol, [])
    try:
        pos = series.index(session)
    except ValueError:
        return False
    target = pos + horizon
    return target < len(series) and series[target] <= audit_as_of


def main() -> int:
    parser = argparse.ArgumentParser(description='Quantify HistoricalMatcher calendar-run-date vs market-session semantics')
    parser.add_argument('--root', default='.')
    parser.add_argument('--contract', default='configs/qm_b_provider_outcome_availability_v1.json')
    parser.add_argument('--output')
    args = parser.parse_args()

    root = Path(args.root).resolve()
    contract = load_provider_outcome_contract(args.contract)
    inp = contract['inputs']
    recent_path = root / inp['history_recent']['path']
    price_path = root / inp['price_backfill']['path']
    metadata_path = root / inp['metadata']['path']
    if digest(recent_path) != inp['history_recent']['sha256']:
        raise SystemExit('history_recent frozen hash mismatch')
    if digest(price_path) != inp['price_backfill']['sha256']:
        raise SystemExit('price_backfill frozen hash mismatch')
    metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
    if (metadata.get('history_recent') or {}).get('sha256') != inp['history_recent']['sha256']:
        raise SystemExit('history_recent metadata hash mismatch')
    audit_as_of = date.fromisoformat((metadata.get('price_coverage') or {}).get('as_of') or metadata['as_of'])

    ev = events(read_csv(recent_path))
    dates, closes, issues = price_index(read_csv(price_path), audit_as_of)
    mapping_counts = Counter()
    mapping_weekday = Counter()
    mapped = []
    exact_available = 0
    challenger_recovered = 0

    for row in ev:
        symbol, obs_day = row['symbol'], row['obs_date']
        exact = (symbol, obs_day) in closes
        if exact:
            exact_available += 1
        session, status = map_session(symbol, obs_day, dates)
        mapping_counts[status] += 1
        mapping_weekday[(status, obs_day.strftime('%A'))] += 1
        if session is not None:
            mapped.append({**row, 'session': session, 'mapping_status': status})
            if not exact:
                challenger_recovered += 1

    # Phase-1 precedent: when several reruns map to one market session, retain the latest observation.
    by_session = defaultdict(list)
    for row in mapped:
        by_session[(row['symbol'], row['session'])].append(row)
    collision_groups = {key: rows for key, rows in by_session.items() if len(rows) > 1}
    deduped = []
    for rows in by_session.values():
        deduped.append(max(rows, key=lambda r: (r['obs_date'], r['order'])))

    horizons = contract['outcome_availability']['horizons_trading_sessions']
    exact_horizon_available = {}
    challenger_horizon_available = {}
    for horizon in horizons:
        exact_n = 0
        for row in ev:
            symbol, obs_day = row['symbol'], row['obs_date']
            if (symbol, obs_day) in closes and horizon_available(symbol, obs_day, int(horizon), dates, audit_as_of):
                exact_n += 1
        challenger_n = sum(
            horizon_available(row['symbol'], row['session'], int(horizon), dates, audit_as_of)
            for row in deduped
        )
        exact_horizon_available[str(horizon)] = exact_n
        challenger_horizon_available[str(horizon)] = challenger_n

    weekend_recovered = sum(
        count for (status, weekday), count in mapping_weekday.items()
        if status == 'PREVIOUS_SESSION_WITHIN_7D' and weekday in {'Saturday', 'Sunday'}
    )
    report = {
        'finding_id': 'QM-B-POA-001',
        'finding_status': 'METHODOLOGY_RISK',
        'scope': 'HistoricalMatcher / daily_research cross-universe outcomes',
        'phase1_affected': False,
        'reason': 'Productive MarketDate is UTC calendar run date while HistoricalMatcher requires an exact price session on that calendar date.',
        'automatic_fix_applied': False,
        'challenger_rule': 'previous observed price session at or before obs_date, maximum 7 calendar days; then deduplicate symbol/session keeping latest observation',
        'audit_as_of': audit_as_of.isoformat(),
        'recent_event_count': len(ev),
        'exact_start_available_count': exact_available,
        'exact_start_unavailable_count': len(ev) - exact_available,
        'challenger_mappable_event_count_before_session_dedup': len(mapped),
        'challenger_recovered_start_count_before_session_dedup': challenger_recovered,
        'challenger_unmappable_event_count': len(ev) - len(mapped),
        'mapping_status_counts': dict(sorted(mapping_counts.items())),
        'weekend_recovered_count': weekend_recovered,
        'session_collision_group_count': len(collision_groups),
        'events_in_collision_groups': sum(len(rows) for rows in collision_groups.values()),
        'challenger_event_count_after_session_dedup': len(deduped),
        'exact_horizon_available_event_counts': exact_horizon_available,
        'challenger_horizon_available_event_counts_after_session_dedup': challenger_horizon_available,
        'horizon_available_delta': {
            h: challenger_horizon_available[h] - exact_horizon_available[h]
            for h in exact_horizon_available
        },
        'price_validation_issue_symbol_count': len(issues),
        'interpretation_guardrails': [
            'challenger results are diagnostic only and do not replace HistoricalMatcher automatically',
            'recovered events are not independent extra evidence when multiple run dates map to one market session',
            'session deduplication is required before comparing sample sizes',
            'no current data may be used to rewrite historical scanner features',
        ],
    }
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
