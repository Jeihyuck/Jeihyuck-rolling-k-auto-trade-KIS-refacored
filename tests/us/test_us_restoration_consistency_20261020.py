"""Recovery acceptance: real journal/accounting + isolated durable claims.

Broker reads are controlled responses; order/fill persistence uses the repository's
memory backend. Claims use SQLite. This does not certify WSL/KIS operation.
"""
from datetime import date

import pytest
import sqlalchemy as sa

from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.us.db import repos
from trader.us.db.execution_claim_schema import metadata, us_execution_claims, us_execution_attempts
from trader.us.execution import order_journal as journal


@pytest.fixture
def recovery(monkeypatch, tmp_path):
    engine = sa.create_engine('sqlite:///' + str(tmp_path / 'claims.sqlite'))
    metadata.create_all(engine)
    claims = DurableExecutionClaimRepo(engine, us_execution_claims, us_execution_attempts)
    monkeypatch.setattr(repos, '_get_engine_or_none', lambda: None)
    monkeypatch.setattr(repos, '_execution_claim_repo', lambda: claims)
    monkeypatch.setattr(repos, '_MEM_ORDERS', [])
    monkeypatch.setattr(repos, '_MEM_FILLS', [])
    monkeypatch.setenv('US_ORDER_JOURNAL_DIR', str(tmp_path))
    monkeypatch.delenv('US_STANDARD_BROKER_PROOF_ONLY', raising=False)

    def seed(symbol='NET', qty=2, day='2026-10-14', synthetic=False):
        identity = SemanticActionIdentity(env='practice', account_id='test-account',
            market='US', trading_epoch_id='test-epoch', strategy_owner='US_STANDARD',
            lifecycle_id='life-'+symbol, action='TP1', trade_date=date.fromisoformat(day))
        key, attempt, number = 'key-'+symbol, 'attempt-'+symbol, 'order-'+symbol
        claims.acquire(identity, attempt_id=attempt, requested_qty=qty, client_order_key=key)
        claims.record_observation(identity, attempt_id=attempt, state='ACKED',
                                  cumulative_filled_qty=None, authoritative=False)
        meta = dict(execution_action_key=identity.action_key, submit_attempt_id=attempt,
                    strategy_owner='US_STANDARD', position_lifecycle_id='life-'+symbol)
        event = dict(trade_date=day, client_order_key=key, submit_attempt_id=attempt,
                     symbol=symbol, side='SELL', exchange='NASDAQ', qty=qty,
                     pre_order_position_qty=qty*2, limit_price=100, meta=meta)
        journal.append_order_event('BROKER_SUBMIT_STARTED', event)
        journal.append_order_event('BROKER_ACK_RECEIVED', event, broker_order_no=number)
        assert repos.save_order_ack(dict(client_order_key=key, order_no=number,
            symbol=symbol, side='SELL', exchange='NASDAQ', qty_requested=qty,
            status='ACK', meta=meta), trade_date=day)
        if synthetic:
            assert repos.mark_order_filled_by_reconcile(order_no=number, client_order_key=key,
                symbol=symbol, side='SELL', filled_qty=qty, requested_qty=qty,
                avg_price_usd=100, trade_date=day, evidence_type='BALANCE_DELTA_SYNTHETIC')['status']=='OK'
        return identity, event, number
    yield claims, seed
    engine.dispose()


class Broker:
    def __init__(self, *, detail=None, positions=None):
        self.detail = detail or {}
        self.positions = positions or []
        self.days = []
    def get_balance(self, **kwargs):
        return {'positions': self.positions}
    def get_today_orders(self, *, trade_date):
        self.days.append(trade_date)
        return []
    def get_fills_by_order_no(self, **kwargs):
        return dict(self.detail)
    def _get_client(self):
        return self
    def get_us_fills_today(self, **kwargs):
        return []


def test_balance_estimate_is_not_broker_confirmation(recovery):
    claims, seed = recovery
    identity, event, _ = seed()
    result = journal.replay_order_journal(event['trade_date'], provider=Broker(
        positions=[{'symbol':'NET', 'qty':2}]))
    assert result['broker_confirmed_count'] == result['filled_count'] == 0
    assert result['balance_estimate_count'] == 1
    assert result['unresolved_count'] == 1
    assert claims.get(identity).active_attempt_id
    assert not any(e['event_type']=='JOURNAL_REPLAY_FILL_CONFIRMED'
                   for e in journal.load_order_events(event['trade_date']))


def test_claim_write_failure_is_not_reported_as_recovered(recovery, monkeypatch):
    _, seed = recovery
    _, event, _ = seed()
    monkeypatch.setattr(repos, 'apply_broker_order_observation', lambda **kw:
        {'status':'OK', 'requires_reconcile':True, 'execution_claim_update_error':'write failed'})
    result = journal.replay_order_journal(event['trade_date'], provider=Broker(
        detail={'filled_qty':2, 'avg_price':105, 'symbol':'NET','side':'SELL'}))
    assert result['filled_count'] == 0
    assert result['unresolved_count'] == 1


def test_synthetic_price_cannot_be_reused_as_actual_price(recovery):
    claims, seed = recovery
    identity, event, number = seed(synthetic=True)
    result = repos.apply_broker_order_observation(trade_date=event['trade_date'],
        client_order_key=event['client_order_key'], raw_order_no=number,
        canonical_order_no=number, symbol='NET', side='SELL', requested_qty=2,
        filled_qty=2, remaining_qty=0, broker_status='FILLED',
        evidence_type='KIS_ORDER_CUMULATIVE_ACTUAL', raw_row={'filled_qty':2,'requested_qty':2})
    assert result['requires_reconcile']
    assert claims.get(identity).active_attempt_id
    assert all(repos.is_synthetic_fill_meta(f['meta']) for f in repos._MEM_FILLS)


def test_direct_cancel_proof_clears_only_verified_attempt(recovery):
    claims, seed = recovery
    identity, event, _ = seed()
    result = journal.replay_order_journal(event['trade_date'], provider=Broker(detail={
        'status':'CANCELLED', 'requested_qty':2, 'filled_qty':0,'remaining_qty':0}))
    assert result['unresolved_count'] == 0
    assert result['broker_rejected_count'] == 1
    assert claims.get(identity).active_attempt_id is None
    assert claims.get(identity).remaining_target_qty == 2


def test_actual_fill_remains_authoritative_after_replay(recovery):
    claims, seed = recovery
    identity, event, _ = seed(synthetic=True)
    broker = Broker(detail={'filled_qty':2, 'requested_qty':2, 'avg_price':105,
                            'symbol':'NET', 'side':'SELL'})
    for _ in range(2):
        result = journal.replay_order_journal(event['trade_date'], provider=broker)
        assert result['unresolved_count'] == 0
    assert claims.get(identity).action_state == 'SATISFIED'
    with claims.engine.connect() as conn:
        attempt = conn.execute(sa.select(us_execution_attempts)).mappings().one()
    assert attempt['authoritative'] is True
    actual = [f for f in repos._MEM_FILLS if not repos.is_synthetic_fill_meta(f['meta'])]
    assert len(actual) == 1 and actual[0]['price_usd'] == 105


def test_thirteen_historical_claims_recover_only_from_original_date_proof(recovery, monkeypatch):
    claims, seed = recovery
    cases = [('MCK',1,'2026-10-13'), ('NET',1,'2026-10-14'),
             ('COST',1,'2026-10-14'), ('ABBV',2,'2026-10-14'),
             ('PG',4,'2026-10-14'), ('GILD',4,'2026-10-14'), ('DHR',2,'2026-10-15'),
             ('AMD',1,'2026-10-06'), ('MSFT',1,'2026-10-12'), ('CL',6,'2026-10-15'),
             ('KO',7,'2026-10-15'), ('KR',10,'2026-10-15'), ('MRVL',2,'2026-10-06')]
    seeded = [seed(symbol,qty,day,synthetic=(index<7))
              for index,(symbol,qty,day) in enumerate(cases)]
    monkeypatch.setattr(repos,'load_active_execution_claim_attempts',lambda:claims.active_attempts())
    class History(Broker):
        stage = 0
        def get_fills_by_order_no(self, *, order_no, symbol, trade_date):
            matching = next(item for item in seeded if item[2]==order_no)
            assert trade_date == matching[1]['trade_date']
            if not self.stage or symbol in {'AMD','MSFT','MRVL'}:
                return {}
            qty = matching[1]['qty']
            return dict(order_no=order_no, symbol=symbol, side='SELL', requested_qty=qty,
                filled_qty=(2 if symbol=='CL' else 0 if symbol in {'KO','KR'} else qty),
                remaining_qty=0, avg_price=105,
                status=('CANCELLED' if symbol in {'CL','KO'} else 'REJECTED' if symbol=='KR' else 'FILLED'))
    broker = History()
    before = journal.replay_order_journal('2026-10-20', provider=broker,
        include_active_claims=True, active_claims_only=True)
    assert before['unresolved_count']==13 and before['filled_count']==0
    assert len(claims.active_attempts())==13
    broker.stage=1
    recovered = journal.replay_order_journal('2026-10-20', provider=broker,
        include_active_claims=True, active_claims_only=True)
    assert recovered['broker_full_fill_count']==7
    assert recovered['broker_partial_fill_count']==1
    assert recovered['broker_rejected_count']==2
    assert recovered['unresolved_count']==3
    # Re-open the durable database through a new engine/repository (restart).
    restarted_engine = sa.create_engine(claims.engine.url)
    restarted = DurableExecutionClaimRepo(restarted_engine, us_execution_claims, us_execution_attempts)
    monkeypatch.setattr(repos,'_execution_claim_repo',lambda:restarted)
    monkeypatch.setattr(repos,'load_active_execution_claim_attempts',lambda:restarted.active_attempts())
    repeated = journal.replay_order_journal('2026-10-20', provider=broker,
        include_active_claims=True, active_claims_only=True)
    assert repeated['filled_count']==0 and repeated['unresolved_count']==3
    assert len(restarted.active_attempts())==3
    for identity,event,_ in seeded:
        state = restarted.get(identity)
        if event['symbol'] in {'AMD','MSFT','MRVL'}:
            assert state.active_attempt_id
        else:
            assert state.active_attempt_id is None
        if event['symbol']=='CL':
            assert state.remaining_target_qty==4
    actual=[f for f in repos._MEM_FILLS if not repos.is_synthetic_fill_meta(f['meta'])]
    assert len(actual)==8
    assert all(f['price_usd']==105 for f in actual)
    assert '2026-10-20' not in broker.days
    restarted_engine.dispose()


@pytest.mark.parametrize('field,value', [('order_no','other'),('symbol','OTHER'),
    ('side','BUY'),('requested_qty',3),('us_trade_date','2026-10-01')])
def test_conflicting_detail_never_cleans_claim(recovery, field, value):
    claims, seed = recovery
    identity,event,_ = seed()
    detail={'filled_qty':2,'requested_qty':2,'avg_price':105,'symbol':'NET','side':'SELL'}
    detail[field]=value
    result=journal.replay_order_journal(event['trade_date'],provider=Broker(detail=detail))
    assert result['filled_count']==0 and result['unresolved_count']==1
    assert claims.get(identity).active_attempt_id


def test_incomplete_cancel_does_not_release_claim(recovery):
    claims, seed = recovery
    identity,event,_=seed()
    result=journal.replay_order_journal(event['trade_date'],provider=Broker(
        detail={'status':'CANCELLED','requested_qty':2}))
    assert result['unresolved_count']==1
    assert claims.get(identity).active_attempt_id


@pytest.mark.parametrize('flag', ['filled_qty_present', 'requested_qty_present'])
def test_explicit_missing_quantity_flag_prevents_actual_promotion(recovery, flag):
    claims, seed = recovery
    identity,event,_=seed(synthetic=True)
    result=journal.replay_order_journal(event['trade_date'],provider=Broker(detail={
        'status':'FILLED','requested_qty':2,'filled_qty':2,'avg_price':105,flag:False}))
    assert result['filled_count']==0 and result['unresolved_count']==1
    assert claims.get(identity).active_attempt_id
    assert all(repos.is_synthetic_fill_meta(f['meta']) for f in repos._MEM_FILLS)


def test_expiry_without_claim_terminalization_is_not_recovery_success(recovery):
    claims,seed=recovery
    identity,event,_=seed()
    result=journal.replay_order_journal(event['trade_date'],provider=Broker(detail={
        'status':'EXPIRED','requested_qty':2,'filled_qty':0,'remaining_qty':0}))
    assert result['broker_confirmed_count']==0 and result['unresolved_count']==1
    assert claims.get(identity).active_attempt_id


def test_close_broker_fill_does_not_hide_pending_claim(recovery):
    from trader.us.execution.reconcile import classify_ack_orders_with_final_balance
    _,seed=recovery
    _,event,number=seed()
    order={**repos._MEM_ORDERS[0],'active_claim':True,'_broker_observation':{
        'status':'FILLED','requested_qty':2,'filled_qty':2,'remaining_qty':0}}
    result=classify_ack_orders_with_final_balance(provider=Broker(),
        trade_date=event['trade_date'],orders=[order])
    assert result['pending_order_count']==1
    assert result['orders'][0]['execution_claim_reconciliation_required']
