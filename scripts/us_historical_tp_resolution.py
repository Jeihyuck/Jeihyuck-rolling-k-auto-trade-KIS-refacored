#!/usr/bin/env python3
"""Authenticated original-day TP audit. GET-only broker, read-only DB by default.

No orders/cancels, DB repairs, fake attempts or historical transactions. Output
contains symbols, dates, counts and verdicts, never credentials/raw broker rows.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from zoneinfo import ZoneInfo
import json
import logging
import os


def restrict_audit_engine(engine) -> None:
    """Enforce read-only inside EVERY transaction, even with pooler/options overrides."""
    from sqlalchemy import event
    if engine is None:
        return
    if not str(engine.dialect.name).startswith('postgres'):
        raise RuntimeError('AUDIT_REQUIRES_POSTGRES_READ_ONLY_BARRIER')
    def read_only_transaction(conn):
        conn.exec_driver_sql('SET TRANSACTION READ ONLY')
        if str(conn.exec_driver_sql('SHOW transaction_read_only').scalar_one()).lower() != 'on':
            raise RuntimeError('AUDIT_READ_ONLY_BARRIER_NOT_CONFIRMED')
    event.listen(engine,'begin',read_only_transaction)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--trade-date', default=datetime.now(ZoneInfo('America/New_York')).date().isoformat())
    parser.add_argument('--phase', choices=['AM_START','CLOSE'], default='CLOSE')
    parser.add_argument('--budget-sec', type=float, default=30)
    parser.add_argument('--output')
    args=parser.parse_args()
    date.fromisoformat(args.trade_date)
    # Set both broker and DB read-only barriers BEFORE loading runtime modules.
    os.environ.update({'KIS_ENV':'practice','WSL_RUN_MARKET':'US','TRADING_REGION':'US',
        'US_ORDER_ARMED':'0','US_LIVE_TRADING_ENABLED':'0','LIVE_TRADING_ENABLED':'0',
        'US_PAPER_TRADING_ENABLED':'0','US_KIS_ORDER_ALLOWED':'0','DISABLE_REAL_TRADING':'1',
        'DISABLE_LIVE_TRADING':'1','ALLOW_REAL_ORDER':'0','DRY_RUN':'1'})
    previous=os.environ.get('PGOPTIONS','')
    os.environ['PGOPTIONS']=previous+' -c default_transaction_read_only=on'
    logging.basicConfig(level=logging.WARNING)
    from trader.us.data_provider import USDataProvider
    from trader.us.execution.historical_tp_resolution import resolve_historical_tp_at_boundary
    from trader.us.db.repos import _get_engine_or_none
    restrict_audit_engine(_get_engine_or_none())

    class ReadOnlyProvider:
        def __init__(self, provider): self._reader=provider
        def fork_for_stage(self, **kw): return ReadOnlyProvider(self._reader.fork_for_stage(**kw))
        def get_original_day_order_snapshot(self, day): return self._reader.get_original_day_order_snapshot(day)

    result=resolve_historical_tp_at_boundary(provider=ReadOnlyProvider(USDataProvider(offline=False,env='practice')),
        trade_date=args.trade_date,phase=args.phase,apply=False,budget_sec=args.budget_sec)
    result.update(read_only=True,operational_database_repaired=False,
                  scope='US_STANDARD_HISTORICAL_TP_ONLY_NOT_ALL_ACCOUNT_DEBT')
    rendered=json.dumps(result,ensure_ascii=False,indent=2)
    print(rendered)
    if args.output:
        from pathlib import Path
        Path(args.output).write_text(rendered+'\n',encoding='utf-8')
    return 0 if result['status']=='OK' else 2


if __name__=='__main__':
    raise SystemExit(main())
