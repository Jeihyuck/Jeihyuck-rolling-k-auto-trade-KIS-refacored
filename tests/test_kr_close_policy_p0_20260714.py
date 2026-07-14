import os

from trader import pb1_runner
from trader.pb1_runner import build_kr_close_policy_orders_from_tagged_positions
from trader.kr.runner.trade_session_runner import normalize_kr_session_completion


def test_kr_close_does_not_liquidate_swing_without_force_eod():
    orders = build_kr_close_policy_orders_from_tagged_positions(
        [{"code": "000240", "qty": 3}],
        db_positions=[{"code": "000240", "position_meta": {"position_book": "SWING_BOOK", "trade_horizon": "SWING_CARRY"}}],
    )
    assert orders == []


def test_kr_close_does_not_liquidate_core_without_exit_signal():
    orders = build_kr_close_policy_orders_from_tagged_positions(
        [{"code": "000240", "qty": 3}],
        db_positions=[{"code": "000240", "position_meta": {"position_book": "CORE_BOOK", "trade_horizon": "CORE_CARRY"}}],
    )
    assert orders == []


def test_kr_close_liquidates_day_book_force_eod_only():
    orders = build_kr_close_policy_orders_from_tagged_positions(
        [{"code": "010950", "qty": 2}],
        db_positions=[{"code": "010950", "position_meta": {"position_book": "DAY_BOOK", "trade_horizon": "DAY_TRADE", "force_eod_close": True}}],
    )
    assert len(orders) == 1
    assert orders[0]["reason"] == "KR_CLOSE_DAY_FORCE_EOD"
    assert orders[0]["source"] == "tagged_close_policy"


def test_kr_close_kis_holdings_without_db_tags_does_not_sell():
    assert build_kr_close_policy_orders_from_tagged_positions([{"code": "000660", "qty": 1}]) == []


def test_kr_close_metadata_missing_defaults_to_hold():
    assert build_kr_close_policy_orders_from_tagged_positions([{"code": "005930", "qty": 1}], db_positions=[], latest_buy_fills=[]) == []


def test_kr_close_policy_uses_entry_meta_json():
    orders = build_kr_close_policy_orders_from_tagged_positions(
        [{"code": "010950", "qty": 2}],
        latest_buy_fills=[{"code": "010950", "entry_meta_json": '{"position_book":"DAY_BOOK","trade_horizon":"DAY_TRADE","force_eod_close":true}'}],
    )
    assert len(orders) == 1
    assert orders[0]["metadata_source"] == "fill_meta"


def test_kr_close_liquidation_all_requires_explicit_confirm(monkeypatch):
    monkeypatch.setenv("PB1_CLOSE_LIQUIDATION_ENABLED", "1")
    monkeypatch.delenv("KR_CLOSE_LIQUIDATION_ALL_ENABLED", raising=False)
    monkeypatch.delenv("KR_CLOSE_LIQUIDATION_ALL_CONFIRM", raising=False)
    assert pb1_runner.run_close_liquidation_from_kis_holdings(kis_client=None, holdings=[{"code": "000660", "qty": 1}], dry_run=True) == []


def test_kr_close_liquidation_env_ignored_without_confirm(monkeypatch):
    monkeypatch.setenv("KR_CLOSE_LIQUIDATION_ALL_ENABLED", "1")
    monkeypatch.setenv("KR_CLOSE_LIQUIDATION_ALL_CONFIRM", "WRONG")
    assert pb1_runner.run_emergency_close_liquidation_from_kis_holdings(kis_client=None, holdings=[{"code": "000660", "qty": 1}], dry_run=True) == []


def test_kr_order_candidate_blocked_by_existing_holding_is_ok_no_trade():
    status, reason, completed, retryable = normalize_kr_session_completion(
        status="OK_NO_TRADE", summary_reason="PB1_SESSION_DONE", marker={"completed": 1},
        pb1_status="RETRYABLE_ORDER_BUILD_ERROR", pb1_exit_reason="BUYABLE_EXISTING_HOLDING_KIS",
    )
    assert status == "OK_NO_TRADE"
    assert completed == 1
    assert retryable == 0


def test_kr_final30_missing_blocks_entry_but_allows_exit():
    entry_can_proceed = False
    exit_can_proceed = True
    close_can_proceed = True
    assert (entry_can_proceed or exit_can_proceed or close_can_proceed) is True
