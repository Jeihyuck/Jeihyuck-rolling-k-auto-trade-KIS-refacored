from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import Mock
import os
import pytest

from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.integration import exclude_owned_symbol, run_isolated
from trader.kr.infinite.models import CycleStatus, Decision, MarketInput, SleeveState
from trader.kr.infinite.order_router import CanonicalOrderRouter, client_order_key, ingest_fills, order_gates
from trader.kr.infinite.policy import integer_buy_quantity, net_liquidation_return, policy_checksum
from trader.kr.infinite.reconcile import ownership_check, rebuild_from_fills
from trader.kr.infinite.risk_adapter import REGIME_POLICY
from trader.kr.infinite.strategy import decide
from trader.kr.regime import STATE_ORDER

NOW = datetime(2026, 8, 13, 1, tzinfo=timezone.utc)
DAY = date(2026, 8, 13)


def market(state="KR_NORMAL", **kw):
    values = dict(trade_date=DAY, quote_price=Decimal("20000"), quote_at=NOW,
                  regime_state=state, regime_as_of=DAY, regime_at=NOW,
                  orderable_cash=Decimal("15000000"))
    values.update(kw); return MarketInput(**values)


def test_config_defaults_and_validation(monkeypatch):
    for key in list(os.environ):
        if key.startswith("KR_INFINITE_"): monkeypatch.delenv(key)
    c = InfiniteConfig.from_env()
    assert (c.enabled, c.symbol, c.capital_krw, c.units, c.unit_krw) == (True,"122630",Decimal("15000000"),40,Decimal("375000"))
    with pytest.raises(ValueError): InfiniteConfig(capital_krw=Decimal("1"))
    with pytest.raises(ValueError): InfiniteConfig(symbol="233740")
    assert c.effective_env("practice") == "practice" and c.effective_env("prod") == "real"
    with pytest.raises(ValueError): c.effective_env("sandbox")


def test_canonical_mapping_exact_and_all_states_decide():
    assert tuple(REGIME_POLICY) == STATE_ORDER
    state = SleeveState("cycle")
    for regime in STATE_ORDER:
        result = decide(InfiniteConfig(), state, market(regime, recovery_confirmed=True), NOW)
        assert result.action in {"BUY", "BLOCK"}


@pytest.mark.parametrize("change,reason", [
    ({"regime_state":"UNKNOWN"},"UNKNOWN_REGIME"),
    ({"regime_at":NOW-timedelta(hours=1)},"REGIME_SNAPSHOT_STALE"),
    ({"regime_at":NOW+timedelta(seconds=1)},"REGIME_FUTURE_TIMESTAMP"),
    ({"regime_as_of":date(2026,8,12)},"REGIME_TRADE_DATE_MISMATCH"),
    ({"data_quality":"BLOCKED"},"DATA_QUALITY_BLOCKED"),
    ({"policy_matches":False},"EXECUTION_POLICY_MISMATCH"),
    ({"kill_switch":True},"ACCOUNT_KILL_SWITCH"),
])
def test_regime_fail_closed(change, reason):
    assert decide(InfiniteConfig(), SleeveState("c"), market(**change), NOW).reason == reason


def test_caps_sizing_pending_daily_and_sell_priority():
    cfg=InfiniteConfig(); active=SleeveState("c", filled_quantity=10, buy_notional=Decimal("100000"), average_price=Decimal("10000"))
    result=decide(cfg,active,market(quote_price=Decimal("12000")),NOW)
    assert (result.action,result.quantity,result.sell_kind)==("SELL",10,"PROFIT_FULL")
    assert decide(cfg,SleeveState("c",pending_order_key="k"),market(),NOW).reason=="PENDING_ORDER"
    assert decide(cfg,SleeveState("c",last_buy_date=DAY),market(),NOW).reason=="DAILY_BUY_FILL_LIMIT"
    assert decide(cfg,SleeveState("c",last_sell_date=DAY),market(),NOW).reason=="SAME_DAY_SELL_REBUY"
    assert decide(cfg,SleeveState("c",buy_notional=Decimal("15000000")),market(),NOW).reason in {"UNIT_CAP","CAPITAL_CAP"}
    assert decide(cfg,SleeveState("c",buy_notional=Decimal("3000000")),market("KR_DEFENSE_CAUTION",recovery_confirmed=True),NOW).reason=="REGIME_EXPOSURE_CAP"
    buy=decide(cfg,SleeveState("c"),market(),NOW); assert buy.action=="BUY" and buy.quantity>0
    assert integer_buy_quantity(Decimal("999999"),Decimal(1),Decimal("375000"),Decimal("375000"),cfg)==0


def test_reconcile_fills_ack_not_counted_and_ownership():
    state=SleeveState("c")
    assert rebuild_from_fills(state,[],0).buy_notional==0
    fills=[{"side":"BUY","qty":2,"price":"10000","fee":"10","tax":0,"trade_date":str(DAY)}]
    rebuilt=rebuild_from_fills(state,fills,2)
    assert rebuilt.filled_quantity==2 and rebuilt.buy_notional==Decimal("20010") and rebuilt.last_buy_date==DAY
    assert rebuild_from_fills(state,fills,1).status==CycleStatus.RECONCILE_PENDING
    assert ownership_check(2,0)==CycleStatus.OWNERSHIP_CONFLICT


def test_gates_and_canonical_router_idempotency(monkeypatch):
    enabled={"LIVE_TRADING_ENABLED":"1","KR_LIVE_TRADING_ENABLED":"1","KR_ORDER_ARMED":"1","STRATEGY_MODE":"LIVE",
             "DRY_RUN":"0","DISABLE_LIVE_TRADING":"0","FORCE_BLOCK_LIVE":"0"}
    assert order_gates(enabled)==(True,"OK")
    for key in ("KR_ORDER_ARMED","KR_LIVE_TRADING_ENABLED"):
        assert order_gates(enabled|{key:"0"})[0] is False
    for key in ("DRY_RUN","FORCE_BLOCK_LIVE"):
        assert order_gates(enabled|{key:"1"})[0] is False
    for k,v in enabled.items(): monkeypatch.setenv(k,v)
    repo=Mock(); repo.create_intent_idempotent.return_value=("oid",True)
    broker=Mock(return_value={"rt_cd":"0","output":{"ODNO":"123"}})
    router=CanonicalOrderRouter(repo,broker,InfiniteConfig())
    out=router.route(Decision("BUY","x",3,Decimal(1)),SleeveState("c"),DAY,env="practice",run_id="r",price=10,regime="KR_NORMAL")
    assert out["status"]=="ACKED" and broker.call_count==1
    repo.create_intent_idempotent.return_value=("oid",False)
    assert router.route(Decision("BUY","x",3),SleeveState("c"),DAY,env="practice",run_id="r",price=10,regime="KR_NORMAL")["reason"]=="DUPLICATE_INTENT"
    assert broker.call_count==1
    assert client_order_key("c",DAY,"BUY")==client_order_key("c",DAY,"BUY")


def test_missing_order_number_reconciles_and_reject_is_terminal(monkeypatch):
    for k,v in {"LIVE_TRADING_ENABLED":"1","KR_LIVE_TRADING_ENABLED":"1","KR_ORDER_ARMED":"1",
                "STRATEGY_MODE":"LIVE","DRY_RUN":"0","DISABLE_LIVE_TRADING":"0","FORCE_BLOCK_LIVE":"0"}.items(): monkeypatch.setenv(k,v)
    repo=Mock(); repo.create_intent_idempotent.return_value=("oid",True)
    router=CanonicalOrderRouter(repo,Mock(return_value={"rt_cd":"0","msg_cd":"OK"}),InfiniteConfig())
    assert router.route(Decision("BUY","x",1),SleeveState("c"),DAY,env="practice",run_id="r",price=1,regime="KR_NORMAL")["status"]=="RECONCILE_PENDING"
    repo.mark_acked.assert_not_called()
    router.broker_submit=Mock(return_value={"rt_cd":"1","msg1":"rejected"})
    assert router.route(Decision("SELL","x",1),SleeveState("d"),DAY,env="practice",run_id="r",price=1,regime="KR_NORMAL")["status"]=="REJECTED"
    repo.mark_error.assert_called_once()


@pytest.mark.parametrize("realized,remaining,price", [(0,10,"110.25"),("27500",7,"110.25"),("55000",5,"110.25"),("33000",7,"110.25")])
def test_cycle_return_includes_partial_sell(realized,remaining,price):
    cfg=InfiniteConfig(buy_fee_rate=Decimal("0"),sell_fee_rate=Decimal("0"),sell_tax_rate=Decimal("0"),slippage_rate=Decimal("0"))
    expected=(Decimal(str(realized))+Decimal(price)*remaining-Decimal("1000"))/Decimal("1000")
    assert net_liquidation_return(Decimal(price),remaining,Decimal("1000"),cfg,Decimal(str(realized)))==expected


def test_fill_ingestion_requires_sleeve_attribution():
    fills=Mock(); orders=Mock(); stamp=NOW
    meta={"strategy_id":"kr_kodex_infinite_v2","book":"KR_INFINITE","cycle_id":"c","client_order_key":"k","symbol":"122630"}
    order={"order_id":"o","kis_odno":"123","client_order_key":"k","qty":2,"request_json":meta}
    evidence=[{"odno":"123","symbol":"122630","trade_id":"f1","side":"BUY","qty":2,"price":100,"filled_at":stamp}]
    assert ingest_fills(fills_repo=fills,orders_repo=orders,order=order,broker_fills=evidence,env="practice")==1
    fills.upsert_fill.assert_called_once(); orders.mark_filled.assert_called_once()
    assert ingest_fills(fills_repo=fills,orders_repo=orders,order={**order,"request_json":meta|{"book":"PB1"}},broker_fills=evidence,env="practice")==0


def test_partial_fill_does_not_terminally_fill_order():
    fills=Mock(); orders=Mock(); meta={"strategy_id":"kr_kodex_infinite_v2","book":"KR_INFINITE","cycle_id":"c","client_order_key":"k","symbol":"122630"}
    order={"order_id":"o","kis_odno":"123","client_order_key":"k","qty":3,"request_json":meta}
    evidence=[{"odno":"123","symbol":"122630","side":"BUY","qty":1,"price":100,"filled_at":NOW}]
    assert ingest_fills(fills_repo=fills,orders_repo=orders,order=order,broker_fills=evidence,env="practice")==1
    orders.mark_filled.assert_not_called()


def test_ownership_adapter_is_narrow_and_exception_isolated():
    original=["005930","122630","000660"]
    assert exclude_owned_symbol(original,True)==["005930","000660"]
    assert exclude_owned_symbol(original,False)==original
    assert exclude_owned_symbol(["233740"],True)==["233740"]
    assert run_isolated(lambda: (_ for _ in ()).throw(RuntimeError("boom"))) is None
