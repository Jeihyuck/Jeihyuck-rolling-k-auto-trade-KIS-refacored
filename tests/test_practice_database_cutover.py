"""Exercise the actual approval entry point; no DB or broker I/O is allowed."""
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scripts import verify_new_practice_database_cutover as cutover
from trader.db.practice_database_generation import PracticeDatabaseGenerationError


def complete_balance():
    return {
        "positions": [], "balance_parse_status": "OK",
        "balance_complete": True, "balance_authoritative": True,
        "failed_exchanges": {}, "queried_exchanges": ["NASD", "NYSE", "AMEX"],
        "exchange_result_counts": {"NASD": 0, "NYSE": 0, "AMEX": 0},
    }


def page(rows=None, *, cursor="", status="D", **overrides):
    return {
        "rt_cd": "0", "output1": [] if rows is None else rows,
        "ctx_area_fk100": cursor, "ctx_area_nk100": cursor,
        "_response_meta": {"tr_cont": status}, **overrides,
    }


@pytest.fixture
def approval(monkeypatch):
    # An accidental call outside these doubles must fail instead of reaching KIS.
    def no_network(*args, **kwargs):
        raise AssertionError("network forbidden in cutover regression tests")
    monkeypatch.setattr("socket.socket.connect", no_network)
    for key, value in {
        "KIS_ENV": "practice", "STRATEGY_ENV": "practice",
        "VERIFY_PRACTICE_DB_CUTOVER": "1", "DB_CUTOVER_CONFIRM": "YES",
        "PBCORE_NEW_DB_URL": "postgresql://test@localhost/fresh",
    }.items():
        monkeypatch.setenv(key, value)
    db = Mock(return_value={"status": "CUTOVER_DB_READY"})
    kr = SimpleNamespace(
        get_balance_cached=Mock(return_value={"output1": []}),
        inquire_daily_ccld=Mock(return_value=page()),
    )
    us = SimpleNamespace(
        get_balance=Mock(return_value=complete_balance()),
        get_today_orders=Mock(return_value=[]),
    )
    monkeypatch.setattr(cutover, "get_db_url", lambda: "postgresql://test@localhost/old")
    monkeypatch.setattr(cutover, "verify_fresh_target_database", db)
    monkeypatch.setattr(cutover, "KisAPI", lambda **kw: kr)
    monkeypatch.setattr(cutover, "USDataProvider", lambda **kw: us)
    monkeypatch.setattr(cutover, "get_masked_account_key", lambda **kw: "test-masked")
    return SimpleNamespace(db=db, kr=kr, us=us)


@pytest.mark.parametrize("field,value,error", [
    ("balance_complete", False, "INCOMPLETE"),
    ("balance_complete", None, "INCOMPLETE"),
    ("balance_authoritative", False, "NOT_AUTHORITATIVE"),
    ("balance_authoritative", None, "NOT_AUTHORITATIVE"),
    ("balance_parse_status", "ERROR", "PARSE_NOT_OK"),
    ("failed_exchanges", {"NYSE": "timeout"}, "EXCHANGE_FAILURE"),
    ("failed_exchanges", None, "EXCHANGE_FAILURE"),
    ("queried_exchanges", ["NASD", "NYSE"], "COVERAGE_INCOMPLETE"),
    ("exchange_result_counts", {"NASD": 0, "NYSE": 0}, "COUNTS_INCOMPLETE"),
    ("exchange_result_counts", None, "COUNTS_MISSING"),
    ("exchange_result_counts", {"NASD": -1, "NYSE": 0, "AMEX": 0}, "QUANTITY_INVALID"),
    ("positions", [{"symbol": "AAPL", "qty": 1}], "ACCOUNT_NOT_FLAT"),
    ("positions", [{"symbol": "AAPL", "qty": "bad"}], "QUANTITY_INVALID"),
    ("positions", [{"symbol": "AAPL", "qty": 0.5}], "QUANTITY_INVALID"),
    ("positions", None, "NOT_AUTHORITATIVE"),
])
def test_actual_main_never_approves_bad_us_evidence(approval, capsys, field, value, error):
    approval.us.get_balance.return_value[field] = value
    with pytest.raises(RuntimeError, match=error):
        cutover.main()
    assert "CUTOVER_READY" not in capsys.readouterr().out
    approval.us.get_today_orders.assert_not_called()


def test_actual_main_accepts_flat_complete_account_and_cancelled_order(approval, capsys):
    approval.kr.inquire_daily_ccld.return_value = page([{
        "ord_qty": "10", "tot_ccld_qty": "0", "rmn_qty": "0", "cncl_yn": "Y",
    }])
    assert cutover.main() == 0
    assert '"status": "CUTOVER_READY"' in capsys.readouterr().out
    approval.kr.get_balance_cached.assert_called_once_with(force=True)
    approval.us.get_balance.assert_called_once_with(force_refresh=True)


@pytest.mark.parametrize("table", ["us_profit_capture_lifecycle", "kr_infinite_state"])
def test_actual_main_cannot_approve_missing_db_schema(approval, capsys, table):
    approval.db.side_effect = PracticeDatabaseGenerationError("TARGET_DATABASE_REQUIRED_TABLES_MISSING:" + table)
    with pytest.raises(PracticeDatabaseGenerationError, match=table):
        cutover.main()
    assert "CUTOVER_READY" not in capsys.readouterr().out
    approval.kr.get_balance_cached.assert_not_called()


@pytest.mark.parametrize("pages,error", [
    ([page(cursor="p2", status="M"), page([{"ord_qty": 10, "tot_ccld_qty": 3, "rmn_qty": 7}])], "PENDING_ORDERS_EXIST"),
    ([page(cursor="p2", status="M"), page(rt_cd="1")], "NOT_AUTHORITATIVE"),
    ([page(cursor="p2", status="M"), page(cursor="p2", status="M")], "STALLED"),
    ([page(status="M")], "CURSOR_MISSING"),
    ([page(status="?")], "UNKNOWN_STATUS"),
    ([{}], "NOT_AUTHORITATIVE"),
    ([page(output1=None)], "NOT_AUTHORITATIVE"),
    ([page([None])], "NOT_AUTHORITATIVE"),
    ([page([{"ord_qty": 10, "tot_ccld_qty": 0, "rmn_qty": "bad"}])], "QUANTITY_INVALID"),
    ([page(_diag_stub=True)], "NOT_AUTHORITATIVE"),
    ([page(_ccld_status="TIMEOUT")], "NOT_AUTHORITATIVE"),
])
def test_actual_main_rejects_kr_order_gaps(approval, capsys, pages, error):
    approval.kr.inquire_daily_ccld.side_effect = deepcopy(pages)
    with pytest.raises(RuntimeError, match=error):
        cutover.main()
    assert "CUTOVER_READY" not in capsys.readouterr().out
    approval.us.get_balance.assert_not_called()


def test_final_page_header_accepts_echoed_cursor(approval):
    approval.kr.inquire_daily_ccld.side_effect = [
        page(cursor="p2", status="M"), page(cursor="p2", status="D"),
    ]
    assert cutover.main() == 0
    assert approval.kr.inquire_daily_ccld.call_count == 2
    assert approval.kr.inquire_daily_ccld.call_args.kwargs["ctx_area_fk100"] == "p2"


def test_page_limit_cannot_silently_truncate_orders():
    kis = SimpleNamespace(inquire_daily_ccld=Mock(side_effect=[
        page(cursor="p2", status="M"), page(cursor="p3", status="M"),
    ]))
    with pytest.raises(RuntimeError, match="PAGINATION_INCOMPLETE"):
        cutover._kr_daily_ccld_all_pages(kis, max_pages=2)


@pytest.mark.parametrize("row,remaining", [
    ({"ord_qty": 10, "tot_ccld_qty": 0, "rmn_qty": 0, "cncl_yn": "Y"}, 0),
    ({"ord_qty": 10, "tot_ccld_qty": 3, "cncl_qty": 7}, 0),
    ({"ord_qty": 10, "tot_ccld_qty": 3, "cncl_qty": 2}, 5),
    ({"ord_qty": 10, "tot_ccld_qty": 10}, 0),
    ({"ord_qty": 10, "tot_ccld_qty": 0, "status": "CANCEL_PENDING"}, 10),
    # A revision/cancel REQUEST quantity is not proof of an executed cancellation.
    ({"ord_qty": 10, "tot_ccld_qty": 0, "rvse_cncl_qty": 10}, 10),
    ({"ord_qty": 10, "tot_ccld_qty": 0, "rmn_qty": 2, "cncl_yn": "Y"}, 2),
])
def test_kr_remaining_quantity_respects_executed_cancel_and_explicit_remainder(row, remaining):
    assert cutover._kr_remaining_qty(row) == remaining


@pytest.mark.parametrize("orders,error", [
    ([{"remaining_qty": 1}], "PENDING_ORDERS_EXIST"),
    ([{"normalization_result": "quarantined"}], "QUARANTINED"),
    ([{"remaining_qty": "bad"}], "QUANTITY_INVALID"),
    (None, "NOT_AUTHORITATIVE"),
])
def test_actual_main_rejects_us_order_gaps(approval, capsys, orders, error):
    approval.us.get_today_orders.return_value = orders
    with pytest.raises(RuntimeError, match=error):
        cutover.main()
    assert "CUTOVER_READY" not in capsys.readouterr().out


def test_kr_wrapper_sends_continuation_header_and_preserves_response_metadata(monkeypatch):
    from trader import kis_wrapper
    monkeypatch.setattr(kis_wrapper, "kis_http_enabled", lambda: True)
    monkeypatch.setattr(kis_wrapper, "_pick_tr", lambda *args: ["TEST_TR"])
    response = SimpleNamespace(status_code=200, headers={"tr_cont": "D"}, json=lambda: page())
    kis = object.__new__(kis_wrapper.KisAPI)
    kis.env = "practice"
    kis.CANO, kis.ACNT_PRDT_CD = "test", "01"
    kis._headers = lambda tr_id: {"tr_id": tr_id}
    kis.session = SimpleNamespace(request=Mock(return_value=response))
    result = kis.inquire_daily_ccld(
        start_date="20260920", end_date="20260920", ctx_area_fk100="FK", ctx_area_nk100="NK",
    )
    kwargs = kis.session.request.call_args.kwargs
    assert kwargs["headers"]["tr_cont"] == "N"
    assert kwargs["params"]["CTX_AREA_FK100"] == "FK"
    assert kwargs["params"]["CTX_AREA_NK100"] == "NK"
    assert result["_response_meta"]["tr_cont"] == "D"
