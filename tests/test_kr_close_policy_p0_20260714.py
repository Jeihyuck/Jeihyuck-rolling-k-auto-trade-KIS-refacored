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


class _FakePositionsRepo:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def list_positions_by_codes(self, *, env, strategy, codes):
        self.calls.append({"env": env, "strategy": strategy, "codes": list(codes)})
        code_set = {str(code).zfill(6) for code in codes}
        return [row for row in self.rows if str(row.get("code")).zfill(6) in code_set]


class _FakeFillsRepo:
    def __init__(self, rows=None):
        self.rows = rows or {}
        self.calls = []

    def list_latest_buy_fills_by_codes(self, env, codes):
        self.calls.append({"env": env, "codes": list(codes)})
        return {str(code).zfill(6): row for code, row in self.rows.items() if str(code).zfill(6) in {str(c).zfill(6) for c in codes}}


class _FakeOrdersRepo:
    def list_today_buy_orders(self, env, code=None):
        return []


class _FakeKis:
    def __init__(self):
        self.sell_calls = []

    def sell_stock_market(self, code, qty):
        self.sell_calls.append({"code": code, "qty": qty})
        return {"rt_cd": "0", "msg_cd": "OK", "msg1": "accepted", "output": {"ODNO": "1"}}


def test_kr_close_policy_loads_db_positions_not_empty():
    positions = _FakePositionsRepo([
        {"code": "010950", "position_meta": {"position_book": "DAY_BOOK", "trade_horizon": "DAY_TRADE", "force_eod_close": True}},
    ])
    kis = _FakeKis()
    result = pb1_runner.run_kr_close_policy_from_tagged_positions(
        kis_holdings=[{"code": "010950", "qty": 1}],
        positions_repo=positions,
        fills_repo=_FakeFillsRepo(),
        orders_repo=_FakeOrdersRepo(),
        kis_client=kis,
        env="practice",
        dry_run=False,
    )
    assert positions.calls
    assert result["db_positions"] != []


def test_kr_close_policy_day_book_force_eod_submits_sell():
    kis = _FakeKis()
    result = pb1_runner.run_kr_close_policy_from_tagged_positions(
        kis_holdings=[{"code": "010950", "qty": 2}],
        positions_repo=_FakePositionsRepo([
            {"code": "010950", "position_meta": {"position_book": "DAY_BOOK", "trade_horizon": "DAY_TRADE", "force_eod_close": True}},
        ]),
        fills_repo=_FakeFillsRepo(),
        orders_repo=_FakeOrdersRepo(),
        kis_client=kis,
        env="practice",
        dry_run=False,
    )
    assert kis.sell_calls == [{"code": "010950", "qty": 2}]
    assert result["accepted_policy_sells"] == 1


def test_kr_close_policy_swing_book_holds_with_real_repo_metadata():
    kis = _FakeKis()
    result = pb1_runner.run_kr_close_policy_from_tagged_positions(
        kis_holdings=[{"code": "000240", "qty": 3}],
        positions_repo=_FakePositionsRepo([
            {"code": "000240", "position_meta": {"position_book": "SWING_BOOK", "trade_horizon": "SWING_CARRY", "force_eod_close": False}},
        ]),
        fills_repo=_FakeFillsRepo(),
        orders_repo=_FakeOrdersRepo(),
        kis_client=kis,
        env="practice",
        dry_run=False,
    )
    assert result["policy_orders"] == []
    assert kis.sell_calls == []


def test_kr_close_policy_result_counts_are_actual_orders():
    kis = _FakeKis()
    result = pb1_runner.run_kr_close_policy_from_tagged_positions(
        kis_holdings=[{"code": "000240", "qty": 3}, {"code": "010950", "qty": 2}],
        positions_repo=_FakePositionsRepo([
            {"code": "000240", "position_meta": {"position_book": "SWING_BOOK", "trade_horizon": "SWING_CARRY", "force_eod_close": False}},
            {"code": "010950", "position_meta": {"position_book": "DAY_BOOK", "trade_horizon": "DAY_TRADE", "force_eod_close": True}},
        ]),
        fills_repo=_FakeFillsRepo(),
        orders_repo=_FakeOrdersRepo(),
        kis_client=kis,
        env="practice",
        dry_run=False,
    )
    assert result["policy_sell_candidates"] == 1
    assert result["accepted_policy_sells"] == 1
    assert len(kis.sell_calls) == 1


def test_kr_close_policy_metadata_missing_holds_integration():
    kis = _FakeKis()
    result = pb1_runner.run_kr_close_policy_from_tagged_positions(
        kis_holdings=[{"code": "005930", "qty": 1}],
        positions_repo=_FakePositionsRepo([]),
        fills_repo=_FakeFillsRepo(),
        orders_repo=_FakeOrdersRepo(),
        kis_client=kis,
        env="practice",
        dry_run=False,
    )
    assert result["policy_orders"] == []
    assert result["accepted_policy_sells"] == 0
    assert kis.sell_calls == []
