"""Original-day resolution of stages excluded by active-claim-only recovery."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
import json
import os

import pytest
from sqlalchemy import text

from trader.us.execution.historical_tp_resolution import (
    EVIDENCE, PRIOR_STAGE_EVIDENCE, assess, digest, resolve_historical_tp_at_boundary,
)
from tests.us.test_us_postgres_integration_real import pg_engine
from tests.us.test_us_residual_flow_20261119 import _seed_boundary_case

DAY = "2026-11-04"
NOW = datetime(2026, 11, 19, 9, tzinfo=timezone.utc)
SCOPE = {"env": "practice", "market": "US", "account_id": "account-hash",
         "trading_epoch_id": "original-epoch", "raw_account": "practice-account"}


def candidate(symbol="KO", day=DAY):
    cycle = "original-cycle-" + symbol
    key = f"US_PC_{day}_{symbol}_{cycle}_TAKE_PROFIT_TP2"
    return {"symbol": symbol, "trade_date": day, "stage": "tp2", "stage_status": "NOT_TRIGGERED",
            "position_lifecycle_id": cycle, "client_order_key": key, "requested_qty": 9,
            "cumulative_filled_qty": 0, "trading_epoch_id": SCOPE["trading_epoch_id"],
            "raw_broker_order_no": "", "canonical_broker_order_no": "",
            "state": {"meta": {"last_status": "US_RECOVERY_SCOPE_UNVERIFIED", "last_stage": "tp2"}}}


def broker_row(symbol="HCA", side="SELL", order_no="0000047288", day=DAY):
    return {"symbol": symbol, "side": side, "order_no": order_no, "trade_date": day,
            "requested_qty": 1, "filled_qty": 1, "remaining_qty": 0, "status": "FILLED",
            "normalization_result": "normalized", "requested_qty_present": True,
            "filled_qty_present": True}


def snapshot(rows=None, day=DAY):
    rows = [broker_row(day=day)] if rows is None else rows
    return {"status": "OK", "scope": {"verified": True, "source": "kis_authenticated_order_book",
                "env": "practice", "market": "US", "account_id": SCOPE["account_id"], "query_trade_date": day},
            "query": {"query_id": "query-id", "complete": True, "us_trade_date": day,
                      "pages": 2, "all_symbols": True, "all_sides": True, "all_statuses": True, "all_exchanges": True},
            "rows": rows, "rows_sha256": digest(rows), "observed_at": NOW.isoformat()}


def witnesses(day=DAY):
    return [{"symbol": "HCA", "side": "SELL", "order_no": "47288", "trade_date": day,
             "qty_requested": 1, "qty_filled": 1, "account_id": None}]


@pytest.mark.parametrize("symbol", ["KO", "KR", "PG", "AAPL", "MSFT", "BRK.B", "GENERIC"])
@pytest.mark.parametrize("day", ["2026-11-04", "2026-11-11"])
def test_original_day_positive_witness_and_no_target_sell_proves_no_accepted_order(symbol, day):
    proof = assess(candidate(symbol, day), snapshot(day=day), witnesses(day), SCOPE, now=NOW)
    assert proof["verified"] and proof["reason"] == EVIDENCE
    assert proof["source_cycle"] == "original-cycle-" + symbol
    assert "NOT_A_PRE_HTTP_CERTIFICATE" in proof["meaning"]
    assert "order_no" not in proof and "raw_account" not in proof


@pytest.mark.parametrize("failure", ["empty", "no_witness", "wrong_witness_account", "wrong_date", "wrong_account",
    "incomplete", "single_exchange", "buy_only", "filled_only", "one_symbol_only", "stale", "future",
    "tampered_rows", "quarantined", "missing_qty", "missing_symbol", "nonintegral_qty", "foreign_epoch", "unknown_status"])
def test_missing_or_incomplete_history_never_releases_a_fence(failure):
    row, book, known = candidate(), snapshot(), witnesses()
    if failure == "empty": book = snapshot([])
    if failure == "no_witness": known = []
    if failure == "wrong_witness_account": known[0]["account_id"] = "foreign"
    if failure == "wrong_date": book["scope"]["query_trade_date"] = "2026-11-05"
    if failure == "wrong_account": book["scope"]["account_id"] = "foreign"
    if failure == "incomplete": book["query"]["complete"] = False
    if failure == "single_exchange": book["query"]["all_exchanges"] = False
    if failure == "buy_only": book["query"]["all_sides"] = False
    if failure == "filled_only": book["query"]["all_statuses"] = False
    if failure == "one_symbol_only": book["query"]["all_symbols"] = False
    if failure == "stale": book["observed_at"] = (NOW-timedelta(minutes=3)).isoformat()
    if failure == "future": book["observed_at"] = (NOW+timedelta(seconds=1)).isoformat()
    if failure == "tampered_rows": book["rows"][0]["order_no"] = "forged"
    if failure == "quarantined": book["rows"][0]["normalization_result"] = "quarantined"
    if failure == "missing_qty": book["rows"][0]["filled_qty_present"] = False
    if failure == "missing_symbol": book["rows"][0]["symbol"] = ""
    if failure == "nonintegral_qty": book["rows"][0]["requested_qty"] = 1.5
    if failure == "foreign_epoch": row["trading_epoch_id"] = "foreign"
    if failure == "unknown_status": row["state"]["meta"]["last_status"] = "BROKER_SUBMIT_RESULT_UNKNOWN"
    if failure in {"quarantined", "missing_qty", "missing_symbol", "nonintegral_qty"}: book["rows_sha256"] = digest(book["rows"])
    assert not assess(row, book, known, SCOPE, now=NOW)["verified"]


@pytest.mark.parametrize("status,qty,remaining", [("OPEN", 0, 9), ("PARTIALLY_FILLED", 2, 7),
                                                ("FILLED", 9, 0), ("CANCELLED", 0, 0), ("REJECTED", 0, 0)])
def test_target_sell_cannot_be_adopted_by_symbol_and_quantity_alone(status, qty, remaining):
    sell = {**broker_row(symbol="KO"), "status": status, "requested_qty": 9,
            "filled_qty": qty, "remaining_qty": remaining}
    result = assess(candidate(), snapshot([broker_row(), sell]), witnesses(), SCOPE, now=NOW)
    assert not result['verified'] and result['reason']=='BROKER_SELL_PRESENT_REQUIRES_ORIGINAL_ATTRIBUTION'
    assert result['broker_sell_observations'][0]['filled_qty']==qty
    assert 'order_no' not in result['broker_sell_observations'][0]


def test_infinite_is_never_a_resolution_target():
    assert not assess(candidate("TQQQ"), snapshot(), witnesses(), SCOPE, now=NOW)["verified"]


def settled_prior(original, stage='tp1', order='47288'):
    return {'trade_date':original['trade_date'],'symbol':original['symbol'],'side':'SELL',
        'stage':stage,'stage_status':'DONE','position_lifecycle_id':original['position_lifecycle_id'],
        'trading_epoch_id':SCOPE['trading_epoch_id'],'account_id':None,'strategy_owner':'US_STANDARD',
        'client_order_key':f"US_PC_{original['trade_date']}_{original['symbol']}_{original['position_lifecycle_id']}_TAKE_PROFIT_{stage.upper()}_R5",
        'order_no':order,'raw_broker_order_no':'00000'+order,'canonical_broker_order_no':order,
        'qty_requested':1,'qty_filled':1,'cumulative_filled_qty':1}


@pytest.mark.parametrize('symbol', ['HCA','GENERIC','KO'])
def test_exact_completed_prior_tp_identity_is_not_misattributed_to_unsubmitted_later_stage(symbol):
    original=candidate(symbol);original.update(stage='tp3',client_order_key=original['client_order_key'].replace('TP2','TP3'))
    original['state']['meta']['last_stage']='tp3'
    sells=[broker_row(symbol=symbol,order_no='47288'),broker_row(symbol=symbol,order_no='50414')]
    known=[settled_prior(original),settled_prior(original,'tp2','50414')]
    witness=[{**witnesses()[0],'symbol':symbol}]
    proof=assess(original,snapshot(sells),witness,SCOPE,now=NOW,settled_prior=known)
    assert proof['verified'] and proof['reason']==PRIOR_STAGE_EVIDENCE
    assert len(proof['settled_prior_order_sha256'])==2


@pytest.mark.parametrize('mismatch', ['stage','cycle','epoch','account','owner','key','same_stage_key',
    'order','raw_order','canonical_order','qty','cumulative','pending','partial','duplicate'])
def test_prior_sell_cannot_be_excluded_without_complete_native_prior_stage_identity(mismatch):
    original=candidate('HCA');known=settled_prior(original);sell=broker_row()
    changes={'stage':('stage','tp2'),'cycle':('position_lifecycle_id','foreign'),
        'epoch':('trading_epoch_id','foreign'),'account':('account_id','foreign'),
        'owner':('strategy_owner','INFINITE'),'key':('client_order_key','made-up'),
        'same_stage_key':('client_order_key',original['client_order_key']),
        'order':('order_no','999'),'raw_order':('raw_broker_order_no','999'),
        'canonical_order':('canonical_broker_order_no','999'),'qty':('qty_filled',0),
        'cumulative':('cumulative_filled_qty',0),'pending':('stage_status','PENDING')}
    if mismatch in changes: known[changes[mismatch][0]]=changes[mismatch][1]
    if mismatch=='partial': sell.update(status='PARTIALLY_FILLED',requested_qty=2,remaining_qty=1)
    prior=[known,dict(known)] if mismatch=='duplicate' else [known]
    proof=assess(original,snapshot([sell]),witnesses(),SCOPE,now=NOW,settled_prior=prior)
    assert not proof['verified'] and proof['reason']=='BROKER_SELL_PRESENT_REQUIRES_ORIGINAL_ATTRIBUTION'


def test_nonboundary_does_not_read_database_or_broker(monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: pytest.fail("tick recovery"))
    for phase in ("TICK", "AFTERNOON", "WORKER"):
        assert resolve_historical_tp_at_boundary(provider=object(), trade_date="2026-11-19", phase=phase)["status"] == "SKIPPED_NOT_BOUNDARY"


@pytest.mark.parametrize("failure", [False, True])
def test_provider_requires_fresh_query_evidence_not_previous_success(monkeypatch, failure):
    from trader.us.data_provider import USDataProvider
    p = USDataProvider(offline=False)
    class Client: pass
    client = Client()
    client._last_us_fills_query_evidence = snapshot()["query"]
    monkeypatch.setattr(p, "_get_client", lambda: client)
    monkeypatch.setattr(p, "get_order_lookup_provenance", lambda day: {**snapshot()["scope"], "broker_account_no": "SECRET_CANO"})
    def get(day):
        assert client._last_us_fills_query_evidence is None
        if failure: raise RuntimeError("GET failed")
        client._last_us_fills_query_evidence = snapshot()["query"]
        return snapshot()["rows"]
    monkeypatch.setattr(p, "get_today_orders", get)
    if failure:
        with pytest.raises(RuntimeError): p.get_original_day_order_snapshot(DAY)
    else:
        got = p.get_original_day_order_snapshot(DAY)
        assert got["status"] == "OK" and "broker_account_no" not in got["scope"]
        assert got["rows_sha256"] == digest(got["rows"])


@pytest.fixture
def historical_pg(pg_engine, monkeypatch):
    from trader.us.db import repos
    from trader import account_state
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *a, **kw: SCOPE["trading_epoch_id"])
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *a, **kw: SCOPE["trading_epoch_id"])
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {k:v for k,v in SCOPE.items() if k!="raw_account"})
    monkeypatch.setattr(account_state, "get_account_key", lambda **kw: SCOPE["raw_account"])
    with pg_engine.begin() as conn:
        conn.exec_driver_sql(open('migrations/0052_unified_trading_epoch.sql').read().split('INSERT INTO trading_epochs(',1)[0])
        conn.execute(text("DELETE FROM trading_epochs WHERE trading_epoch_id=:epoch"), {"epoch": SCOPE["trading_epoch_id"]})
        conn.execute(text("INSERT INTO trading_epochs(trading_epoch_id,env,account_id,status,reason) VALUES (:epoch,'practice',:account,'ACTIVE','TEST')"),
                     {"epoch": SCOPE["trading_epoch_id"], "account": SCOPE["raw_account"]})
        conn.execute(text("""INSERT INTO us_orders(trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,avg_price_usd,order_no,status,trading_epoch_id,meta)
            VALUES (:day,'witness','HCA','NYSE','SELL',1,1,100,'0000047288','FILLED',:epoch,'{}'::jsonb)"""), {"day": DAY,"epoch": SCOPE["trading_epoch_id"]})
        conn.execute(text("""INSERT INTO us_fills(trade_date,symbol,exchange,side,qty,price_usd,order_no,client_order_key,filled_at,meta,fill_idempotency_key,trading_epoch_id)
            VALUES (:day,'HCA','NYSE','SELL',1,100,'47288','witness',NOW(),CAST(:meta AS jsonb),'witness-fill',:epoch)"""),
            {"day": DAY, "epoch": SCOPE["trading_epoch_id"], "meta": json.dumps({"is_synthetic":False,"fill_evidence_type":"KIS_ORDER_CUMULATIVE_ACTUAL"})})
    for symbol in ('KO','KR','GENERIC'):
        r=candidate(symbol)
        _seed_boundary_case(pg_engine,symbol=symbol,contradiction='NO_CERTIFICATE',epoch=SCOPE['trading_epoch_id'],
                            account=SCOPE['account_id'],cycle=r['position_lifecycle_id'],key=r['client_order_key'])
        # The fixture's original date is Nov11; use actual original dates here.
        with pg_engine.begin() as conn:
            for table in ('us_profit_capture_lifecycle','us_order_intents'):
                conn.execute(text(f'UPDATE {table} SET trade_date=:day WHERE client_order_key=:key'),{'day':DAY,'key':r['client_order_key']})
    return pg_engine


class Reader:
    def __init__(self): self.calls=[]
    def get_original_day_order_snapshot(self,day):
        self.calls.append(day)
        s=snapshot(day=day);s['observed_at']=datetime.now(timezone.utc).isoformat();return s


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
@pytest.mark.parametrize('native_fill', [True,False])
def test_pg_prior_completed_tp_requires_native_order_fill_and_stage_then_preserves_existing_trades(historical_pg,native_fill):
    original=candidate('HCA');prior=settled_prior(original)
    _seed_boundary_case(historical_pg,symbol='HCA',contradiction='NO_CERTIFICATE',epoch=SCOPE['trading_epoch_id'],
        account=SCOPE['account_id'],cycle=original['position_lifecycle_id'],key=original['client_order_key'])
    with historical_pg.begin() as conn:
        for table in ('us_orders','us_fills'):
            conn.execute(text(f'UPDATE {table} SET client_order_key=:key WHERE client_order_key=\'witness\''),{'key':prior['client_order_key']})
        for table in ('us_profit_capture_lifecycle','us_order_intents'):
            conn.execute(text(f'UPDATE {table} SET trade_date=:day WHERE symbol=\'HCA\''),{'day':DAY})
        conn.execute(text("""INSERT INTO us_profit_capture_lifecycle
            (trade_date,symbol,position_lifecycle_id,stage,stage_status,client_order_key,
             requested_qty,cumulative_filled_qty,raw_broker_order_no,canonical_broker_order_no,trading_epoch_id,state)
            VALUES (:day,'HCA',:cycle,'tp1','DONE',:key,1,1,'0000047288','47288',:epoch,'{}'::jsonb)"""),
            {'day':DAY,'cycle':original['position_lifecycle_id'],'key':prior['client_order_key'],'epoch':SCOPE['trading_epoch_id']})
        if not native_fill: conn.execute(text("UPDATE us_fills SET meta='{}'::jsonb WHERE symbol='HCA'"))
    result=resolve_historical_tp_at_boundary(provider=Reader(),trade_date='2026-11-19',phase='AM_START',source_trade_date=DAY)
    assert result['released']==(4 if native_fill else 0),result
    with historical_pg.connect() as conn:
        assert conn.execute(text("SELECT stage_status FROM us_profit_capture_lifecycle WHERE symbol='HCA' AND stage='tp1'")).scalar_one()=='DONE'
        assert conn.execute(text('SELECT count(*) FROM us_orders')).scalar_one()==1
        assert conn.execute(text('SELECT count(*) FROM us_fills')).scalar_one()==1
        if native_fill:
            state=conn.execute(text("SELECT stage_status,state FROM us_profit_capture_lifecycle WHERE symbol='HCA' AND stage='tp2'")).one()
            assert state[0]=='NOT_TRIGGERED' and state[1]['meta']['last_status']=='FAILED'
            assert state[1]['meta']['last_evidence_type']==PRIOR_STAGE_EVIDENCE


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
def test_pg_readonly_audit_enumerates_actual_source_days_without_runtime_budget_starvation(historical_pg):
    from scripts.us_historical_tp_resolution import audit_all_source_days
    result=audit_all_source_days(provider=Reader(),trade_date='2026-11-19',phase='CLOSE',budget_sec=30)
    assert result['status']=='OK' and result['eligible']==3 and result['released']==0
    assert result['all_source_days_audit'] and not result['database_mutated']
    filtered=resolve_historical_tp_at_boundary(provider=Reader(),trade_date='2026-11-19',phase='CLOSE',
        apply=False,source_trade_date='2026-11-11')
    assert filtered['candidate_count']==0 and filtered['checked']==0


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
@pytest.mark.parametrize('apply',[False,True])
def test_pg_claimless_stages_reach_original_day_kis_then_atomic_resolution(historical_pg,apply):
    from trader.us.db import repos
    reader=Reader()
    with historical_pg.connect() as conn:
        before=conn.execute(text('SELECT count(*) FROM us_orders')).scalar_one(),conn.execute(text('SELECT count(*) FROM us_fills')).scalar_one()
    result=resolve_historical_tp_at_boundary(provider=reader,trade_date='2026-11-19',phase='AM_START',apply=apply)
    assert result['status']=='OK',result
    assert reader.calls==[DAY]  # all three symbols, one original-day GET
    assert result['eligible']==3 and result['released']==(3 if apply else 0),result
    with historical_pg.connect() as conn:
        assert before==(conn.execute(text('SELECT count(*) FROM us_orders')).scalar_one(),conn.execute(text('SELECT count(*) FROM us_fills')).scalar_one())
        assert conn.execute(text('SELECT count(*) FROM us_execution_attempts')).scalar_one()==0
        assert conn.execute(text('SELECT count(*) FROM us_execution_claims')).scalar_one()==0
        statuses={r[0]:r[1] for r in conn.execute(text("SELECT symbol,status FROM us_order_intents WHERE symbol<>'HCA'")).all()}
        assert set(statuses.values())==({'BLOCKED'} if apply else {'PENDING'})
    if apply:
        states=repos.load_us_profit_capture_state('2026-11-19',['KO','KR','GENERIC'],{s:'original-cycle-'+s for s in ('KO','KR','GENERIC')})
        for s,state in states.items():
            assert state['meta']['last_evidence_type']==EVIDENCE
            assert not state['tp2_done'] and not state['tp2_pending']
            assert len(state['meta']['tp2_order_keys'])==1
        again=resolve_historical_tp_at_boundary(provider=reader,trade_date='2026-11-19',phase='CLOSE')
        assert again['checked']==0 and again['released']==0


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
def test_pg_failure_rolls_back_stage_intent_and_audit_without_blocking_peer(historical_pg,monkeypatch):
    from trader.us.db import repos
    original=repos.append_us_order_event
    def fail(event,**kw):
        if event['symbol']=='KO': raise RuntimeError('injected-audit-failure')
        return original(event,**kw)
    monkeypatch.setattr(repos,'append_us_order_event',fail)
    result=resolve_historical_tp_at_boundary(provider=Reader(),trade_date='2026-11-19',phase='CLOSE')
    assert result['released']==2,result
    assert result['unresolved'][0]['symbol']=='KO'
    with historical_pg.connect() as conn:
        assert conn.execute(text("SELECT status FROM us_order_intents WHERE symbol='KO'")).scalar_one()=='PENDING'
        assert conn.execute(text("SELECT count(*) FROM us_order_events WHERE symbol='KO'")).scalar_one()==0


@pytest.mark.parametrize('symbol',['KO','KR','GENERIC'])
@pytest.mark.parametrize('evidence',[EVIDENCE,PRIOR_STAGE_EVIDENCE])
def test_verified_absence_reaches_the_real_tp2_intent_generator(symbol,evidence,monkeypatch):
    from decimal import Decimal
    from trader.us.market_state_overlay import build_profit_capture_intents
    from tests.us.test_tp_pre_http_retry_proof_20261105 import POSITION
    from trader.us.db import repos
    old=candidate(symbol)
    state={'tp1_done':True,'tp2_pending':False,'position_lifecycle_id':old['position_lifecycle_id'],
           'meta':{'tp1_done':True,'tp2_order_keys':[old['client_order_key']], 'last_stage':'tp2',
                   'last_status':'FAILED','last_evidence_type':evidence,'tp2_qty':3,'tp2_filled_qty':0}}
    monkeypatch.setattr('trader.us.profit_capture.authoritative_broker_avg',lambda *a,**kw:(Decimal('100'),{}))
    monkeypatch.setattr(repos,'mark_us_profit_capture_stage',lambda *a,**kw:None)
    pos={**POSITION,'symbol':symbol,'position_lifecycle_id':old['position_lifecycle_id']}
    intents=build_profit_capture_intents([pos],{'profit_capture_enabled':True},now=NOW,
        trade_date='2026-11-19',profit_capture_state={symbol:state})
    assert len(intents)==1 and intents[0]['reason']=='TAKE_PROFIT_TP2'
    assert intents[0]['client_order_key']!=old['client_order_key']
    assert intents[0]['position_lifecycle_id']==old['position_lifecycle_id']


def test_latest_metadata_wins_over_old_nested_failure():
    from trader.us.execution.historical_tp_resolution import eligible
    row=candidate()
    row['state']={'meta':{'last_stage':'tp2','last_status':'FAILED',
                        'meta':{'last_stage':'tp2','last_status':'US_RECOVERY_SCOPE_UNVERIFIED'}}}
    assert not eligible(row)


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
@pytest.mark.parametrize('conflict',['pending_sell','changed_epoch','missing_witness','partial_broker_sell','stale_query'])
def test_pg_preview_and_apply_keep_contradictory_evidence_fenced(historical_pg,conflict):
    reader=Reader()
    if conflict=='pending_sell':
        with historical_pg.begin() as conn:
            conn.execute(text("""INSERT INTO us_orders(trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,order_no,status,trading_epoch_id,meta)
                VALUES ('2026-11-19','current-ko-sell','KO','NYSE','SELL',3,0,'98765','ACK',:epoch,'{}'::jsonb)"""),{'epoch':SCOPE['trading_epoch_id']})
    if conflict=='changed_epoch':
        with historical_pg.begin() as conn:
            conn.execute(text("UPDATE trading_epochs SET status='ENDED' WHERE trading_epoch_id=:epoch"),{'epoch':SCOPE['trading_epoch_id']})
    if conflict=='missing_witness':
        with historical_pg.begin() as conn: conn.execute(text('DELETE FROM us_fills'))
    if conflict=='partial_broker_sell':
        old=reader.get_original_day_order_snapshot
        def snapshot_with_sell(day):
            s=old(day);s['rows'].append({**broker_row(symbol='KO'),'requested_qty':9,'filled_qty':2,'remaining_qty':7,'status':'PARTIALLY_FILLED'});s['rows_sha256']=digest(s['rows']);return s
        reader.get_original_day_order_snapshot=snapshot_with_sell
    if conflict=='stale_query':
        old=reader.get_original_day_order_snapshot
        def stale(day):
            s=old(day);s['observed_at']=(datetime.now(timezone.utc)-timedelta(minutes=5)).isoformat();return s
        reader.get_original_day_order_snapshot=stale
    preview=resolve_historical_tp_at_boundary(provider=reader,trade_date='2026-11-19',phase='CLOSE',apply=False)
    assert preview['eligible']<3,preview
    assert not preview['database_mutated']
    applied=resolve_historical_tp_at_boundary(provider=reader,trade_date='2026-11-19',phase='CLOSE')
    assert applied['released']<3,applied
    with historical_pg.connect() as conn:
        assert conn.execute(text("SELECT status FROM us_order_intents WHERE symbol='KO'")).scalar_one()=='PENDING'


def test_original_day_witness_may_be_infinite_without_adopting_its_policy():
    native=broker_row(symbol='TQQQ',side='BUY',order_no='30080')
    known=[{'symbol':'TQQQ','side':'BUY','order_no':'0000030080','trade_date':DAY,
            'qty_requested':1,'qty_filled':1,'account_id':SCOPE['account_id']}]
    assert assess(candidate('KO'),snapshot([native]),known,SCOPE,now=NOW)['verified']
    assert not assess(candidate('TQQQ'),snapshot([native]),known,SCOPE,now=NOW)['verified']


def test_readonly_workflow_and_cli_cannot_inherit_order_permissions():
    from pathlib import Path
    workflow=Path('.github/workflows/us-historical-tp-original-day.yml').read_text()
    script=Path('scripts/us_historical_tp_resolution.py').read_text()
    assert "github.event.action == 'ready_for_review'" in workflow
    assert "head.repo.full_name == github.repository" in workflow
    assert "persist-credentials: false" in workflow
    for flag in ('US_ORDER_ARMED','US_KIS_ORDER_ALLOWED','US_LIVE_TRADING_ENABLED','LIVE_TRADING_ENABLED','ALLOW_REAL_ORDER'):
        assert flag+": '0'" in workflow
    assert 'default_transaction_read_only=on' in workflow and 'default_transaction_read_only=on' in script
    assert 'apply=False' in script and 'operational_database_repaired=False' in script
    assert 'pull_request_target' not in workflow and 'schedule:' not in workflow


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
def test_pg_unrelated_pending_sell_does_not_block_other_symbols(historical_pg):
    # A genuinely pending other-key order cannot be hidden by old-day absence.
    # An unrelated holding's pending order does not globally block all exits.
    with historical_pg.begin() as conn:
        conn.execute(text("""INSERT INTO us_orders(trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,order_no,status,trading_epoch_id,meta)
            VALUES ('2026-11-19','unrelated-pending','TMO','NYSE','SELL',3,0,'100001','ACK',:epoch,'{}'::jsonb)"""),{'epoch':SCOPE['trading_epoch_id']})
    result=resolve_historical_tp_at_boundary(provider=Reader(),trade_date='2026-11-19',phase='AM_START')
    assert result['released']==3,result


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
@pytest.mark.parametrize('related',[False,True])
def test_pg_active_native_claim_stays_fenced_and_isolates_unrelated_symbols(historical_pg,related):
    life='original-cycle-KO' if related else 'unrelated-cycle-TMO'
    with historical_pg.begin() as conn:
        conn.execute(text("""INSERT INTO us_execution_claims(action_key,env,account_id,market,trading_epoch_id,
            strategy_owner,lifecycle_id,action,target_qty,active_attempt_id,action_state,trade_date)
            VALUES ('existing-claim','practice',:account,'US',:epoch,'US_STANDARD',:life,'tp2',3,
                    'existing-attempt','AMBIGUOUS_ACK','2026-11-19')"""),
                    {'account':SCOPE['account_id'],'epoch':SCOPE['trading_epoch_id'],'life':life})
        conn.execute(text("""INSERT INTO us_execution_attempts(attempt_id,action_key,attempt_no,requested_qty,
            client_order_key,attempt_state,authoritative)
            VALUES ('existing-attempt','existing-claim',1,3,'other-native-key','SUBMITTING',false)"""))
    preview=resolve_historical_tp_at_boundary(provider=Reader(),trade_date='2026-11-19',phase='AM_START',apply=False)
    assert preview['eligible']==(2 if related else 3),preview
    result=resolve_historical_tp_at_boundary(provider=Reader(),trade_date='2026-11-19',phase='CLOSE')
    assert result['released']==(2 if related else 3),result
    with historical_pg.connect() as conn:
        assert conn.execute(text("SELECT action_state FROM us_execution_claims WHERE action_key='existing-claim'")).scalar_one()=='AMBIGUOUS_ACK'


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
def test_pg_source_change_during_original_day_get_is_not_overwritten(historical_pg):
    class RacingReader(Reader):
        def get_original_day_order_snapshot(self,day):
            with historical_pg.begin() as conn:
                conn.execute(text("UPDATE us_profit_capture_lifecycle SET cumulative_filled_qty=1 WHERE symbol='KO'"))
            return super().get_original_day_order_snapshot(day)
    result=resolve_historical_tp_at_boundary(provider=RacingReader(),trade_date='2026-11-19',phase='AM_START')
    assert result['released']==2,result
    with historical_pg.connect() as conn:
        assert conn.execute(text("SELECT cumulative_filled_qty FROM us_profit_capture_lifecycle WHERE symbol='KO'")).scalar_one()==1
        assert conn.execute(text("SELECT status FROM us_order_intents WHERE symbol='KO'")).scalar_one()=='PENDING'


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
def test_pg_two_boundary_readers_cannot_release_or_audit_one_stage_twice(historical_pg):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    barrier=Barrier(2)
    class ConcurrentReader(Reader):
        def get_original_day_order_snapshot(self,day):
            barrier.wait(timeout=10)
            return super().get_original_day_order_snapshot(day)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(resolve_historical_tp_at_boundary,provider=ConcurrentReader(),trade_date='2026-11-19',phase='AM_START') for _ in range(2)]
        results=[f.result(timeout=20) for f in futures]
    assert sum(r['released'] for r in results)==3,results
    with historical_pg.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM us_order_events WHERE event_type='TP_HISTORICAL_BROKER_ABSENCE_VERIFIED'")).scalar_one()==3
        assert conn.execute(text('SELECT count(*) FROM us_execution_attempts')).scalar_one()==0


@pytest.mark.skipif(not os.getenv('PBCORE_TEST_POSTGRES_URL'),reason='requires PostgreSQL')
def test_pg_readonly_audit_barrier_rejects_actual_database_write(pg_engine):
    from sqlalchemy.exc import DBAPIError
    from scripts.us_historical_tp_resolution import restrict_audit_engine
    restrict_audit_engine(pg_engine)
    with pg_engine.connect() as conn:
        assert conn.exec_driver_sql('SHOW transaction_read_only').scalar_one()=='on'
        assert conn.exec_driver_sql('SELECT 1').scalar_one()==1
        with pytest.raises(DBAPIError):
            conn.exec_driver_sql("UPDATE us_orders SET status='FILLED'")
