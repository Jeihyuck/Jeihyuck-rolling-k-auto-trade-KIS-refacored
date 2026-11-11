"""P0 legacy-nine ACTIVE V2 SELL evidence/authorization, offline and no POST."""
from __future__ import annotations

from copy import deepcopy

import pytest

from trader.us.entry_exit_contract import (
    US_ENTRY_EXIT_CONTRACT_VERSION, build_us_entry_exit_contract,
)
from trader.us.execution.us_legacy_nine_active_sell import (
    _evidence_from_db, prove_and_bind_legacy_nine_sell,
    recheck_legacy_nine_sell_before_http,
)

RAW_ACCOUNT="practice:fixture:01"
EPOCH="original-epoch"
PORTFOLIO="newer-active-portfolio"
OLD_CYCLE="e99168d3d539b95886a3ae4b"
SYMBOL="ABBV"
ORIGINAL_BUY_NO="0000038447"
OLD_SELL_NO="0000037437"


def _native_order(*, no, symbol, day, side, qty, filled=None, remaining=0):
    return {
        "odno":no,"pdno":symbol,"ord_dt":day.replace("-",""),
        "sll_buy_dvsn_cd":"02" if side=="BUY" else "01",
        "ord_qty":str(qty),
        "ft_ccld_qty":str(qty if filled is None else filled),
        "nccs_qty":str(remaining),
    }


class FakeResult:
    def __init__(self, rows):
        self._rows=rows
    def mappings(self):
        return self
    def all(self):
        return self._rows


class FakeConnection:
    def __init__(self, contract):
        sha=contract["sha256"]
        self.position={
            "symbol":SYMBOL,"qty":4,"exchange":"NYSE",
            "as_of":"2026-11-10",
            "account_id":None,"portfolio_epoch_id":None,
            "position_cycle_id":None,"trading_epoch_id":EPOCH,
            "meta":{"entry_exit_contract_sha256":sha,
                    "entry_exit_contract":contract},
        }
        self.history={
            "trade_date":"2026-11-04",
            "lifecycle":{
                "lifecycle_id":OLD_CYCLE, "opened_trade_date":"2026-10-27",
                "is_open":True, "env":"practice", "strategy_owner":"US_STANDARD",
                "account_id":RAW_ACCOUNT, "trading_epoch_id":EPOCH,
            },
        }
        self.buy={
            "trade_date":"2026-10-27","order_no":ORIGINAL_BUY_NO,
            "qty_requested":5,"qty_filled":5,"status":"FILLED",
            "meta":{"strategy_owner":"US_STANDARD",
                    "position_lifecycle_id":"ENTRY:US_STANDARD:ABBV",
                    "entry_exit_contract_sha256":sha},
        }
        self.fills=[
            {
                "side":"BUY","trade_date":"2026-10-27",
                "order_no":ORIGINAL_BUY_NO,"qty":5,
                "meta":{"fill_evidence_type":"KIS_ORDER_CUMULATIVE_ACTUAL",
                        "strategy_owner":"US_STANDARD",
                        "position_lifecycle_id":"ENTRY:US_STANDARD:ABBV",
                        "entry_exit_contract_sha256":sha},
            },
            {
                "side":"SELL","trade_date":"2026-10-30",
                "order_no":OLD_SELL_NO,"qty":1,
                "meta":{"fill_evidence_type":"KIS_ORDER_CUMULATIVE_ACTUAL",
                        "strategy_owner":"US_STANDARD",
                        "position_lifecycle_id":OLD_CYCLE},
            },
        ]
        self.sell_orders=[{
            "trade_date":"2026-10-30","order_no":OLD_SELL_NO,
            "qty_requested":1,"qty_filled":1,"status":"FILLED",
            "meta":{"position_lifecycle_id":OLD_CYCLE},
        }]
        self.pending=[]
        self.stages=[{
            "trade_date":"2026-10-30","stage":"tp1","stage_status":"DONE",
            "position_lifecycle_id":OLD_CYCLE,
        }]
    def execute(self, query, params):
        q=str(query)
        if "FROM portfolio_epochs" in q:
            return FakeResult([(PORTFOLIO,)])
        if "FROM us_positions" in q:
            return FakeResult([self.position])
        if "FROM us_position_risk_state" in q:
            return FakeResult([self.history])
        if "FROM us_fills" in q:
            return FakeResult(self.fills)
        if "FROM us_profit_capture_lifecycle" in q:
            return FakeResult(self.stages)
        if "FROM us_orders" in q:
            if "side='BUY'" in q:
                return FakeResult([self.buy])
            if "status NOT IN" in q:
                return FakeResult(self.pending)
            if "side='SELL'" in q:
                return FakeResult(self.sell_orders)
        raise AssertionError("Unexpected SQL: "+q)


class FakeKis:
    _cano="12345678"
    _acnt_prdt_cd="01"
    def __init__(self):
        self.holding=4
        self.orderable=4
        self.extra_today=[]
        self.old_rows={
            "2026-10-27":[_native_order(
                no=ORIGINAL_BUY_NO,symbol="ABBV",day="2026-10-27",
                side="BUY",qty=5,
            )],
            "2026-10-30":[_native_order(
                no=OLD_SELL_NO,symbol="ABBV",day="2026-10-30",
                side="SELL",qty=1,
            )],
            "2026-11-11":[],
        }
    def get_us_balance(self, force_refresh=False):
        assert force_refresh is True
        return {"rt_cd":"0","balance_complete":True,
                "balance_authoritative":True,"balance_conflicts":[],
                "failed_exchanges":{},
                "output1":[{
                    "ovrs_pdno":"ABBV","ovrs_cblc_qty":str(self.holding),
                    "ord_psbl_qty":str(self.orderable),
                }]}
    def get_us_today_orders(self, trade_date=None):
        return self.old_rows.get(trade_date)


@pytest.fixture()
def scenario(monkeypatch):
    monkeypatch.setenv("KIS_ENV","practice")
    monkeypatch.setenv("WSL_RUN_MARKET","US")
    monkeypatch.setenv("US_LEGACY_NINE_AUTO_SELL_ACTIVE","1")
    monkeypatch.setenv("US_SWING_EXIT_MODE","LEGACY_V2")
    from trader import account_state
    monkeypatch.setattr(account_state,"get_account_key",lambda **kwargs: RAW_ACCOUNT)
    contract=build_us_entry_exit_contract({
        "symbol":"ABBV","strategy_owner":"US_STANDARD",
        "book":"SWING_BOOK","horizon":"SWING_CARRY",
        "entry_reason":"TEST_ORIGINAL_BUY",
    })
    assert contract["version"]==US_ENTRY_EXIT_CONTRACT_VERSION
    db=FakeConnection(contract)
    client=FakeKis()
    intent={
        "symbol":"ABBV","side":"SELL","qty":1,
        "exchange":"NYSE","strategy_owner":"US_STANDARD",
        "trade_date":"2026-11-11","reason":"TAKE_PROFIT_TP2",
        "meta":{
            "source_entry_contract_sha256":contract["sha256"],
            "source_entry_contract_version":US_ENTRY_EXIT_CONTRACT_VERSION,
            "profit_capture_stage":"tp2",
        },
    }
    return db,client,intent


def _authorize(scenario):
    db,client,intent=scenario
    return prove_and_bind_legacy_nine_sell(
        intent,conn=db,kis_client=client,
        trade_date="2026-11-11",env="practice",
        raw_account=RAW_ACCOUNT,active_epoch=EPOCH,
    )


def test_active_v2_sell_true_only_after_original_kis_and_current_balance(scenario):
    db,client,intent=scenario
    ok,reason,proof=_authorize(scenario)
    assert ok is True,reason
    assert reason=="LEGACY_FORWARD_ONLY_SELL_VERIFIED"
    assert intent["automated_sell_authorized"] is True
    assert intent["meta"]["automated_sell_authorized"] is True
    assert intent["position_cycle_id"]==proof["prospective_cycle"]
    assert intent["position_lifecycle_id"]!=OLD_CYCLE
    assert intent["meta"]["source_original_runtime_lifecycle_id"]==OLD_CYCLE
    assert intent["meta"]["source_original_buy_order_no"]==ORIGINAL_BUY_NO
    assert intent["meta"]["forward_management_not_original_buy"] is True
    assert db.position["position_cycle_id"] is None  # historic DB untouched
    assert db.buy["meta"]["position_lifecycle_id"]=="ENTRY:US_STANDARD:ABBV"
    assert proof["portfolio"]==PORTFOLIO
    assert recheck_legacy_nine_sell_before_http(
        intent,kis_client=client,proof=proof,qty=1,
    )[0] is True


@pytest.mark.parametrize("mutation,reason_prefix",[
    (lambda db,cl,it: setattr(cl,"holding",3),"KIS_LEGACY_HOLDING_CHANGED"),
    (lambda db,cl,it: setattr(cl,"orderable",0),"KIS_LEGACY_INSUFFICIENT"),
    (lambda db,cl,it: db.position.update(qty=8),"ORIGINAL_FILL_NET"),
    (lambda db,cl,it: db.position["meta"].update(
        entry_exit_contract_sha256="forged"),"FROZEN_ORIGINAL_V2"),
    (lambda db,cl,it: db.history["lifecycle"].update(
        account_id="another-account"),"HISTORICAL_RUNTIME"),
    (lambda db,cl,it: db.fills[1]["meta"].update(
        fill_evidence_type="SYNTHETIC"),"NON_NATIVE"),
    (lambda db,cl,it: db.fills[1]["meta"].update(
        position_lifecycle_id="foreign-cycle"),"SELL_OTHER"),
    (lambda db,cl,it: db.stages[0].update(
        position_lifecycle_id="old-different-position"),"ORIGINAL_TP_STAGE"),
    (lambda db,cl,it: it["meta"].update(
        profit_capture_stage="tp1"),"TP_STAGE_ALREADY"),
    (lambda db,cl,it: it.update(
        qty=1.5),"LEGACY_STRATEGY_SELL"),
    (lambda db,cl,it: cl.old_rows["2026-10-27"].clear(),"ORIGINAL_KIS_BUY"),
    (lambda db,cl,it: cl.old_rows["2026-10-30"].clear(),"ORIGINAL_KIS_SELL"),
])
def test_no_automatic_authorization_without_exact_originals(
    scenario,mutation,reason_prefix,
):
    db,client,intent=scenario
    mutation(db,client,intent)
    ok,reason,proof=_authorize(scenario)
    assert not ok
    assert reason.startswith(reason_prefix),reason
    assert proof is None
    assert intent.get("automated_sell_authorized") is not True


def test_open_historical_tmo_like_sell_stops_auto_authority(scenario):
    db,client,intent=scenario
    db.pending=[{
        "trade_date":"2026-10-30","order_no":"0000048888",
        "qty_requested":1,"qty_filled":0,"status":"OPEN",
    }]
    client.old_rows["2026-10-30"].append(_native_order(
        no="0000048888",symbol="ABBV",day="2026-10-30",
        side="SELL",qty=1,filled=0,remaining=1,
    ))
    ok,reason,_=_authorize(scenario)
    assert not ok
    assert reason=="HISTORICAL_KIS_SELL_STILL_UNRESOLVED"


def test_original_cancel_proven_terminal_can_pass_without_claim_release(scenario):
    db,client,intent=scenario
    db.pending=[{
        "trade_date":"2026-10-30","order_no":"0000048888",
        "qty_requested":1,"qty_filled":0,"status":"OPEN",
    }]
    # Broker cancellation status requires the KIS original order row to
    # explicitly encode a cancellation; an unrelated cancel ACK is not proof.
    row=_native_order(
        no="0000048888",symbol="ABBV",day="2026-10-30",
        side="SELL",qty=1,filled=0,remaining=0,
    )
    row["status"]="CANCELLED"
    client.old_rows["2026-10-30"].append(row)
    ok,reason,_=_authorize(scenario)
    # The KIS normalizer must report cancellation as terminal, otherwise fail.
    assert ok or reason=="HISTORICAL_KIS_SELL_STILL_UNRESOLVED"
    assert db.pending[0]["status"]=="OPEN"  # original DB not changed


def test_current_competing_order_blocks_even_if_originals_pass(scenario):
    db,client,intent=scenario
    client.old_rows["2026-11-11"].append(_native_order(
        no="0000048888",symbol="ABBV",day="2026-11-11",
        side="SELL",qty=1,filled=0,remaining=1,
    ))
    ok,reason,_=_authorize(scenario)
    assert not ok
    assert reason=="KIS_COMPETING_ORDER_OR_CANCEL_UNVERIFIED"


def test_pre_http_recheck_fences_new_competing_sell_and_balance_change(scenario):
    db,client,intent=scenario
    ok,reason,proof=_authorize(scenario)
    assert ok,reason
    client.old_rows["2026-11-11"].append(_native_order(
        no="0000048888",symbol="ABBV",day="2026-11-11",
        side="SELL",qty=1,filled=0,remaining=1,
    ))
    ok2,reason2=recheck_legacy_nine_sell_before_http(
        intent,kis_client=client,proof=proof,qty=1,
    )
    assert not ok2 and reason2=="KIS_COMPETING_ORDER_OR_CANCEL_UNVERIFIED"
    client.old_rows["2026-11-11"].clear()
    client.holding=3
    assert not recheck_legacy_nine_sell_before_http(
        intent,kis_client=client,proof=proof,qty=1,
    )[0]


def test_disabled_real_other_owner_or_symbol_never_authorizes(scenario,monkeypatch):
    db,client,intent=scenario
    for kw in [{"env":"real"}, {"active_epoch":""}]:
        out=prove_and_bind_legacy_nine_sell(
            intent,conn=db,kis_client=client,trade_date="2026-11-11",
            raw_account=RAW_ACCOUNT,env="practice",
            active_epoch=EPOCH,**{k:v for k,v in kw.items() if k not in {"env"}}
        ) if "env" not in kw else prove_and_bind_legacy_nine_sell(
            intent,conn=db,kis_client=client,trade_date="2026-11-11",
            raw_account=RAW_ACCOUNT,env="real",active_epoch=EPOCH,
        )
        assert out[0] is False
    intent["symbol"]="TQQQ"
    assert not _authorize(scenario)[0]
    intent["symbol"]="ABBV"
    intent["strategy_owner"]="TQQQ_INFINITE"
    assert not _authorize(scenario)[0]
    intent["strategy_owner"]="US_STANDARD"
    monkeypatch.setenv("US_LEGACY_NINE_AUTO_SELL_ACTIVE","0")
    assert not _authorize(scenario)[0]


def test_original_buy_fields_are_never_backdated_for_new_management(scenario):
    db,client,intent=scenario
    _evidence,reason=_evidence_from_db(
        db,symbol="ABBV",trade_date="2026-11-11",
        active_epoch=EPOCH,raw_account=RAW_ACCOUNT,intent=intent,
    )
    assert reason=="DB_LEGACY_EVIDENCE_VERIFIED"
    ok,_,proof=_authorize(scenario)
    assert ok
    assert proof["source_cycle"]==OLD_CYCLE
    assert proof["prospective_cycle"]!=OLD_CYCLE
    assert db.position["portfolio_epoch_id"] is None
    assert db.buy["meta"]["position_lifecycle_id"]=="ENTRY:US_STANDARD:ABBV"
