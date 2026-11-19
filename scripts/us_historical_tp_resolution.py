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


def audit_all_source_days(*, provider, trade_date: str, phase: str, budget_sec: float) -> dict:
    """Separate read-only evidence budgets; production boundary budget stays unchanged."""
    from sqlalchemy import text
    from trader.us.db.repos import _get_engine_or_none
    from trader.us.execution.historical_tp_resolution import CANDIDATES_SQL, _scope, eligible, resolve_historical_tp_at_boundary
    with _get_engine_or_none().connect() as conn:
        scope = _scope(conn)
        rows = conn.execute(text(CANDIDATES_SQL), {'epoch':scope['trading_epoch_id'],'today':trade_date}).mappings().all()
        days = sorted({str(r['trade_date']) for r in rows if eligible(dict(r))})
    results = [resolve_historical_tp_at_boundary(provider=provider,trade_date=trade_date,phase=phase,
        apply=False,budget_sec=budget_sec,source_trade_date=day) for day in days[:10]]
    unresolved = [item for r in results for item in r['unresolved']]
    unresolved += [{'source_trade_date':day,'reason':'AUDIT_SOURCE_DAY_LIMIT'} for day in days[10:]]
    if any(r['status'] == 'UNAVAILABLE' for r in results):
        unresolved.append({'reason':'SOURCE_DAY_AUDIT_UNAVAILABLE'})
    return {'status':'HISTORICAL_TP_UNRESOLVED' if unresolved else 'OK',
        **{key:sum(r.get(key,0) for r in results) for key in
            ('checked','released','eligible','source_days_queried','candidate_count')},
        'source_day_evidence':[item for r in results for item in r['source_day_evidence']],
        'observations':[item for r in results for item in r['observations']],
        'unresolved':unresolved,'database_mutated':False,'all_source_days_audit':True}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--trade-date', default=datetime.now(ZoneInfo('America/New_York')).date().isoformat())
    parser.add_argument('--phase', choices=['AM_START','CLOSE'], default='CLOSE')
    parser.add_argument('--budget-sec', type=float, default=30)
    parser.add_argument('--all-source-days', action='store_true')
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

    reader=ReadOnlyProvider(USDataProvider(offline=False,env='practice'))
    if args.all_source_days:
        result=audit_all_source_days(provider=reader,trade_date=args.trade_date,phase=args.phase,budget_sec=args.budget_sec)
    else:
        result=resolve_historical_tp_at_boundary(provider=reader,
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
