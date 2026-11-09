"""No historical KIS BUY/SELL can be repaired from account balance or HASH alone."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

from scripts.kr_20261109_original_kis_proof import (
    Target, review_original_kr_trades, _match_db_source,
)


PB1 = Target("059090", "PB1", date(2026, 10, 29), "BUY", 79, "0000011865")
INF = Target("122630", "KR_INFINITE_V1", date(2026, 10, 27), "SELL", 6, "0000017474")


def _order():
    return {
        "trading_epoch_id": "epoch", "account_id": "practice:acct:01",
        "strategy": "pb1_pullback_close", "code": "059090", "side": "BUY",
        "kis_odno": "0000011865", "qty": 79,
        "position_cycle_id": "mico-cycle", "portfolio_epoch_id": "portfolio",
        "submitted_at": datetime(2026, 10, 29, 9, 31, tzinfo=ZoneInfo("Asia/Seoul")),
    }


def _intent():
    return {
        "trading_epoch_id": "epoch", "strategy_id": "KR_INFINITE_V1",
        "symbol": "122630", "cycle_id": "krinf-cycle",
        "trade_date": date(2026, 10, 27),
        "side": "SELL_ALL", "requested_qty": 6,
        "broker_order_id": "0000017474",
    }


def _page(day, code, side, qty, amount, avg, odno):
    return {
        "rt_cd": "0", "_sep29_complete": True, "output1": [{
            "pdno": code, "sll_buy_dvsn_cd": side, "ord_qty": str(qty),
            "odno": odno, "ord_dt": day.strftime("%Y%m%d"),
            "tot_ccld_qty": str(qty), "tot_ccld_amt": str(amount),
            "avg_prvs": str(avg),
        }],
    }


def _report(pages, targets=(PB1, INF), orders=None, intents=None):
    return review_original_kr_trades(
        pages=pages, scoped_orders=[_order()] if orders is None else orders,
        infinite_intents=[_intent()] if intents is None else intents,
        active_epoch="epoch", active_account="practice:acct:01",
        broker_balance={"rt_cd": "0", "output1": [
            {"pdno": "293490", "hldg_qty": "174"},
        ]}, balance_source="api", targets=targets,
    )


def test_mico_and_infinite_order_originals_proven_but_no_db_mutation():
    pages = {
        PB1.trade_date: _page(PB1.trade_date, "059090", "02", 79, 2057950, 26050, PB1.odno),
        INF.trade_date: _page(INF.trade_date, "122630", "01", 6, 432540, 72090, INF.odno),
    }
    result = _report(pages)
    assert result["status"] == "KIS_ORIGINAL_TRADES_PROVEN_READONLY"
    assert result["trade_proofs_verified"] == 2
    assert result["historical_economic_repair_executed"] is False
    assert result["db_rows_modified"] == 0
    assert result["broker_order_submissions"] == 0
    assert result["current_broker_holdings_by_code"] == {
        "293490": 174, "059090": 0, "122630": 0,
    }


def test_kakao_mts_qty_cannot_make_zero_fill_original_verified():
    kakao = Target("293490", "PB1", date(2026, 10, 27), "BUY", 174, "0000011280")
    order = {**_order(), "code": "293490", "qty": 174,
             "kis_odno": kakao.odno, "position_cycle_id": "kakao-cycle",
             "submitted_at": datetime(2026, 10, 27, 9, 31, tzinfo=ZoneInfo("Asia/Seoul"))}
    page = _page(kakao.trade_date, "293490", "02", 174, 1740000, 10000, kakao.odno)
    page["output1"][0]["tot_ccld_qty"] = "0"
    page["output1"][0]["tot_ccld_amt"] = "0"
    result = _report({kakao.trade_date: page}, targets=(kakao,), orders=[order], intents=[])
    assert result["status"] == "RECOVERY_UNRESOLVED_ORIGINAL_KIS_PROOF"
    assert result["results"][0]["reason"] == "BROKER_CONFIRMED_ZERO_FILL"
    assert result["current_broker_holdings_by_code"]["293490"] == 174


def test_missing_all_pages_and_foreign_account_fail_closed():
    good = _page(PB1.trade_date, PB1.code, "02", 79, 2057950, 26050, PB1.odno)
    good["_sep29_complete"] = False
    result = _report({PB1.trade_date: good}, targets=(PB1,))
    assert result["status"] == "RECOVERY_UNRESOLVED_ORIGINAL_KIS_PROOF"
    assert result["results"][0]["reason"] == "ORIGINAL_DAY_ALL_PAGES_UNVERIFIED"
    result = _report({PB1.trade_date: good}, targets=(PB1,),
                     orders=[{**_order(), "account_id": "practice:old-account:01"}])
    assert result["results"][0]["reason"] == "ORIGINAL_PB1_ACCOUNT_ORDER_OR_CYCLE_NOT_UNIQUE"


def test_same_broker_order_number_from_other_symbol_cannot_prove_mico():
    fake_page = _page(PB1.trade_date, "005930", "02", 79, 2057950, 26050, PB1.odno)
    result = _report({PB1.trade_date: fake_page}, targets=(PB1,))
    assert result["status"] == "RECOVERY_UNRESOLVED_ORIGINAL_KIS_PROOF"
    assert result["results"][0]["reason"] == "MISSING_OR_AMBIGUOUS_ORIGINAL_KIS_ORDER"


def test_recycled_same_order_number_from_different_trade_day_is_not_original():
    wrong_date = {**_order(),
                  "submitted_at": datetime(2026, 10, 30, 9, 31,
                                           tzinfo=ZoneInfo("Asia/Seoul"))}
    ok, reason = _match_db_source(
        PB1, scoped_orders=[wrong_date], infinite_intents=[],
        active_epoch="epoch", active_account="practice:acct:01",
    )
    assert ok is False
    assert reason == "ORIGINAL_PB1_ACCOUNT_ORDER_OR_CYCLE_NOT_UNIQUE"


def test_naive_order_timestamp_cannot_authorize_historical_repair():
    missing_tz = {**_order(), "submitted_at": datetime(2026, 10, 29, 9, 31)}
    ok, reason = _match_db_source(
        PB1, scoped_orders=[missing_tz], infinite_intents=[],
        active_epoch="epoch", active_account="practice:acct:01",
    )
    assert ok is False
    assert reason == "ORIGINAL_PB1_ACCOUNT_ORDER_OR_CYCLE_NOT_UNIQUE"
