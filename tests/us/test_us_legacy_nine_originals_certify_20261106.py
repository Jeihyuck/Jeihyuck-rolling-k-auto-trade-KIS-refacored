"""Practice KIS GET-only batch originals certificate never invents portfolio provenance."""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager

from scripts.us_legacy_nine_originals_readonly_certify import (
    SYMBOLS, check_current_balance, prepare_original_plan,
    certify_get_only, load_readonly_archive,
)


def _archive():
    positions = []
    fills = []
    orders = []
    for n, sym in enumerate(SYMBOLS, 1):
        day = "2026-10-27" if n % 2 else "2026-10-29"
        no = f"00000{n:05d}"
        positions.append({"symbol":sym,"qty":n,"as_of":"2026-11-05",
                          "account_id":None,"trading_epoch_id":"epoch",
                          "portfolio_epoch_id":None,"position_cycle_id":None})
        meta = {"requested_qty":n,"position_lifecycle_id":f"legacy-{sym}",
                "strategy_owner":"US_STANDARD",
                "entry_exit_contract_sha256":f"sha-{sym}"}
        fills.append({"symbol":sym,"side":"BUY","trade_date":day,"order_no":no,
                      "qty":n,"meta":dict(meta)})
        orders.append({"symbol":sym,"side":"BUY","trade_date":day,"order_no":no,
                       "qty_requested":n,"meta":dict(meta)})
    return positions, fills, orders


def test_exact_original_buys_yield_candidate_but_never_portfolio_adoption():
    plan = prepare_original_plan(*_archive())
    assert not plan["blockers"]
    assert len(plan["entries"]) == 9
    assert plan["all_legacy_scope_proven"] is False
    assert all(x["epoch_db_net_matches"] for x in plan["entries"])
    assert all(x["frozen_contract_match"] for x in plan["entries"])


def test_different_order_or_contract_blocks_original_plan():
    p,f,o = _archive()
    o[0]["meta"]["entry_exit_contract_sha256"] = "DIFFERENT"
    out = prepare_original_plan(p,f,o)
    assert "ABBV_FROZEN_BUY_CONTRACT_NOT_PROVEN" in out["blockers"]
    p,f,o = _archive()
    o[0]["order_no"] = "0000099999"
    out = prepare_original_plan(p,f,o)
    assert "ABBV_ORIGINAL_DB_ORDER_NOT_UNIQUE" in out["blockers"]


def test_current_kis_balance_requires_complete_no_conflict_exact_qty():
    p,f,o = _archive()
    rows = [{"ovrs_pdno":x["symbol"],"ovrs_cblc_qty":str(x["qty"])} for x in p]
    balance = {"rt_cd":"0","balance_complete":True,
               "balance_authoritative":True,"balance_conflicts":[],
               "output1":rows}
    entries = prepare_original_plan(p,f,o)["entries"]
    assert check_current_balance(balance, entries)["status"]=="PASS"
    balance["balance_complete"]=False
    assert check_current_balance(balance, entries)["status"]=="CURRENT_KIS_BALANCE_INCOMPLETE"
    balance["balance_complete"]=True
    rows[0]["ovrs_cblc_qty"]="999"
    assert check_current_balance(balance, entries)["status"]=="CURRENT_KIS_BALANCE_MISMATCH"
    rows[0]["ovrs_cblc_qty"]="1.5"
    assert check_current_balance(balance, entries)["status"]=="CURRENT_KIS_BALANCE_QUANTITY_INVALID"


def test_all_kis_buys_and_balance_proven_still_block_original_scope(monkeypatch):
    from scripts import us_original_order_readonly_probe as probe
    class Client:
        def __init__(self):
            self.days=[]
        def get_us_today_orders(self,trade_date):
            self.days.append(trade_date)
            return []
        def get_us_balance(self,force_refresh=False):
            p,_,_=_archive()
            return {"rt_cd":"0","balance_complete":True,
                    "balance_authoritative":True,"balance_conflicts":[],
                    "output1":[{"ovrs_pdno":x["symbol"],
                                "ovrs_cblc_qty":str(x["qty"])} for x in p]}
    def classifier(_rows,*,side,expected_qty,**kwargs):
        if side=="BUY":
            return {"classification":"PROVEN_FILLED",
                    "cumulative_filled_qty":expected_qty}
        return {"classification":"PROVEN_CANCELLED"}
    monkeypatch.setattr(probe,"classify_original_order",classifier)
    client=Client()
    out=certify_get_only(prepare_original_plan(*_archive()),client)
    assert len(out["original_buy_results"])==9
    assert all(x["status"]=="PROVEN_FILLED" for x in out["original_buy_results"])
    assert out["balance"]["status"]=="PASS"
    assert out["tmo_original_sell"]=="PROVEN_CANCELLED"
    assert out["broker_originals_and_balance_verified"] is True
    assert out["status"]=="NOT_PROVEN"
    assert "ORIGINAL_PORTFOLIO_CYCLE_UNPROVEN_POLICY_REVIEW" in out["blockers"]
    assert Counter(client.days)==Counter({"2026-10-27":1,"2026-10-29":1,
                                          "2026-10-30":1})
    assert out["broker_post_count"]==out["db_update_count"]==0


def test_tmo_open_remains_blocked_even_with_original_buys(monkeypatch):
    from scripts import us_original_order_readonly_probe as probe
    def classifier(_rows,*,side,expected_qty,**kwargs):
        return {"classification":"PROVEN_OPEN" if side=="SELL" else "PROVEN_FILLED",
                "cumulative_filled_qty":expected_qty}
    monkeypatch.setattr(probe,"classify_original_order",classifier)
    class Client:
        def get_us_today_orders(self,trade_date):
            return []
        def get_us_balance(self,force_refresh=False):
            return {"rt_cd":"0","balance_complete":False}
    out=certify_get_only(prepare_original_plan(*_archive()),Client())
    assert "TMO_UNRESOLVED_ORIGINAL_SELL_NOT_TERMINAL" in out["blockers"]
    assert "CURRENT_KIS_BALANCE_INCOMPLETE" in out["blockers"]
    assert out["claims_released"]==0


def test_archive_first_statement_readonly_and_epoch_scoped():
    calls=[]
    p,f,o=_archive()
    class Result:
        def __init__(self,rows):
            self.rows=rows
        def mappings(self):
            return self
        def all(self):
            return self.rows
    class Conn:
        def execute(self,sql,params=None):
            calls.append((sql.text,params))
            return Result([p,f,o][len(calls)-2] if len(calls)>1 else [])
    class Engine:
        @contextmanager
        def connect(self):
            yield Conn()
    out=load_readonly_archive(Engine(),trading_epoch_id="epoch",
                              trade_date="2026-11-05")
    assert calls[0][0]=="SET TRANSACTION READ ONLY"
    assert len(calls)==4
    assert all(x[1]["epoch"]=="epoch" for x in calls[1:])
    assert [len(x) for x in out]==[9,9,9]


def test_populated_columns_cannot_fabricate_original_portfolio_proof():
    positions, fills, orders = _archive()
    for row in positions:
        row["account_id"] = "a" * 64
        row["portfolio_epoch_id"] = "NEWER_2026_11_04_EPOCH"
        row["position_cycle_id"] = "FABRICATED_CYCLE"
    plan = prepare_original_plan(positions, fills, orders)
    assert all(x["scope_columns_present"] for x in plan["entries"])
    assert all(x["original_portfolio_proven"] is False for x in plan["entries"])
    assert plan["all_legacy_scope_proven"] is False
