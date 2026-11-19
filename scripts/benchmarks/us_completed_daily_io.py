"""Controlled before/after production-method benchmark; never connects to DB/KIS.

Run from a git checkout containing the given before revision. This measures
identical injected DB latency, not WSL production execution time.
"""
import argparse
from copy import deepcopy
from datetime import date, timedelta
import json
import subprocess
import time
import types
from unittest.mock import patch

from trader.us.data_provider import USDataProvider
from trader.us.execution.tick_context import TickExecutionContext
from trader.us.db import price_daily_repo as repo


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--before', required=True)
    parser.add_argument('--io-delay-ms', type=float, default=5)
    args = parser.parse_args()
    source = subprocess.check_output(['git', 'show', args.before + ':trader/us/data_provider.py'], text=True)
    old = types.ModuleType('benchmark_before_provider')
    exec(compile(source, 'before/trader/us/data_provider.py', 'exec'), old.__dict__)
    rows = [{'date': date(2026, 11, 17) - timedelta(days=259-i), 'open': 99,
             'high': 101, 'low': 98, 'close': 100, 'volume': 10000} for i in range(260)]
    symbols = ['SPY', 'QQQ', 'SMH', 'RSP', 'IWM', 'XLK']
    results = {}
    outputs = []
    for label, cls in [('before', old.USDataProvider), ('after', USDataProvider)]:
        calls = []
        def load(**kw):
            calls.append('load_rows')
            time.sleep(args.io_delay_ms / 1000)
            return deepcopy(rows)
        def latest(**kw):
            calls.append('latest_date')
            time.sleep(args.io_delay_ms / 1000)
            return date(2026, 11, 17)
        provider = cls(offline=True)
        provider.bind_tick_context(TickExecutionContext('2026-11-18', 'am', 'benchmark', 1, 'tick'))
        with patch.object(repo, 'load_recent_us_daily_bars', load), patch.object(repo, 'get_latest_us_daily_date', latest):
            started = time.perf_counter()
            sample = []
            for repeat in range(2):
                for symbol in symbols:
                    result = provider.get_completed_daily_prices_result(symbol, 'NYSE', trade_date='2026-11-18', required_bars=260, allow_http_sync=False)
                    assert result['quality'] == 'OK'
                    sample.append(result)
            results[label] = {'elapsed_ms': round((time.perf_counter()-started)*1000, 2),
                              'db_reads': len(calls), 'evaluations': len(sample)}
            outputs.append(sample)
    assert outputs[0] == outputs[1], 'production outputs changed'
    assert results['after']['db_reads'] < results['before']['db_reads']
    print(json.dumps({'method': 'original_and_modified_production_methods_with_identical_injected_db_latency',
                      'io_delay_ms': args.io_delay_ms, 'output_equivalence': True, **results}, indent=2))


if __name__ == '__main__':
    main()
