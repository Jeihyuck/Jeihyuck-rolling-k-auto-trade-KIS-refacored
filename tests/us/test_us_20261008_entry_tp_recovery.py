"""Oct 8 incidents: legacy immutable BUY TP, stuck broker-open TP, DONE/0 proof."""
from __future__ import annotations

import copy
from datetime import datetime, timezone

import pytest

from trader.us import entry_exit_contract as contracts
from trader.us.market_state_overlay import build_profit_capture_intents
from trader.us.profit_capture_evidence import authoritative_tp_fill_for_backfill
from trader.us.protective_tp_recovery import (
    broker_proves_open_tp_order, broker_proves_terminal_after_cancel,\n    protective_symbols, request_protective_tp_cancel,
)

NOW = datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc)


def frozen(symbol: str, *, legacy: bool = True) -> dict:
    contract = contracts.build_us_entry_exit_contract({
        "symbol": symbol, "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "entry_strategy": "us_pb1", "entry_reason": "ENTRY_PULLBACK",
        "reasons": ["ENTRY_PULLBACK"], "book": "SWING_BOOK",
    })
    if legacy:
        contract = copy.deepcopy(contract)
        contract["management"]["profit_capture"].pop("partial_exit_allowed")
        contract.pop("sha256")
        contract["sha256"] = contracts._sha(contract)
    assert contracts.verify_us_entry_exit_contract(contract)
    return contract


def position(symbol: str, contract: dict) -> dict:
    return {
        "symbol": symbol, "qty": 20, "holding_qty": 20, "orderable_qty": 20,
        "current_price_usd": 104.0, "position_lifecycle_id": f"life-{symbol}",
        "broker_avg_price": 100.0, "broker_avg_price_source": "kis_pchs_avg_pric",
        "broker_avg_price_asof": NOW.isoformat(), "broker_avg_price_currency": "USD",
        "balance_source": "kis_balance_authoritative", "authoritative_positions": True,
        "meta": {"entry_exit_contract": contract},
    }


@pytest.mark.parametrize("symbol", ["AAPL", "QQQ", "NVDA", "MPC", "PLTR"])
def test_legacy_buy_contract_uses_original_tp_without_rehash_or_global_partial(symbol, monkeypatch):
    old = frozen(symbol)
    before = copy.deepcopy(old)
    monkeypatch.setenv("US_TP1_PCT", "0.50")
    monkeypatch.setenv("US_SELL_PARTIAL_ALLOWED", "0")
    config = contracts.contract_profit_capture(position(symbol, old))
    assert config["legacy_scoped_tp_partial_compat"] is True
    assert config["partial_exit_allowed"] is True
    intents = build_profit_capture_intents(
        [position(symbol, old)],
        {"profit_capture_enabled": True, "market_state": "NORMAL"},
        now=NOW, trade_date="2026-10-07", profit_capture_state={},
    )
    assert len(intents) == 1
    assert intents[0]["reason"] == "TAKE_PROFIT_TP1"
    assert intents[0]["partial_exit_allowed"] is True
    assert intents[0]["meta"]["source_entry_contract_sha256"] == old["sha256"]
    assert intents[0]["meta"]["tp_threshold_fraction"] == "0.03"
    assert old == before


def test_valid_but_missing_frozen_tp_stages_blocks_without_global_fallback(monkeypatch):
    contract = frozen("AAPL")
    contract["management"]["profit_capture"].pop("stages")
    contract.pop("sha256")
    contract["sha256"] = contracts._sha(contract)
    monkeypatch.setenv("US_TP1_PCT", "0.01")
    intents = build_profit_capture_intents(
        [position("AAPL", contract)],
        {"profit_capture_enabled": True}, now=NOW,
        trade_date="2026-10-07", profit_capture_state={},
    )
    assert intents == []


def test_contract_read_error_fails_closed_not_global_tp(monkeypatch):
    contract = frozen("NVDA")
    def fail(_):
        raise RuntimeError("db corrupt")
    monkeypatch.setattr(contracts, "contract_profit_capture", fail)
    assert build_profit_capture_intents(
        [position("NVDA", contract)],
        {"profit_capture_enabled": True}, now=NOW,
        trade_date="2026-10-07", profit_capture_state={},
    ) == []


def test_explicit_frozen_partial_denial_is_not_overridden():
    contract = frozen("AAPL", legacy=False)
    contract["management"]["profit_capture"]["partial_exit_allowed"] = False
    contract.pop("sha256")
    contract["sha256"] = contracts._sha(contract)
    assert build_profit_capture_intents(
        [position("AAPL", contract)],
        {"profit_capture_enabled": True}, now=NOW,
        trade_date="2026-10-07", profit_capture_state={},
    ) == []


def _protective(symbol: str = "MRVL") -> dict:
    return {
        "symbol": symbol, "side": "SELL", "exit_type": "profit_trailing_stop",
        "strategy_owner": "US_STANDARD", "qty": 4,\n        "position_lifecycle_id": f"life-{symbol}",
    }


def _old_tp(symbol: str = "MRVL") -> dict:
    return {"id": "id-1", "symbol": symbol, "trade_date": "2026-10-06",
            "order_no": "0000034237", "exchange": "NASDAQ",
            "qty_requested": 2, "qty_filled": 0,
            "client_order_key": f"us-tp2-{symbol}",
            "trading_epoch_id": "epoch-1", "meta": {
                "profit_capture_stage": "tp2", "strategy_owner": "US_STANDARD",\n                "position_lifecycle_id": f"life-{symbol}",
            }}


def _broker_open(*, remaining: int = 2, status: str = "OPEN") -> dict:
    return {"status": status, "order_no": "34237", "requested_qty": 2,
            "filled_qty": 2 - remaining, "remaining_qty": remaining,
            "filled_qty_present": True, "normalization_result": "normalized"}


@pytest.mark.parametrize("symbol", ["MRVL", "AMD"])
def test_protective_tp_recovery_requires_opt_in_and_broker_proof(monkeypatch, symbol):
    order = _old_tp(symbol)
    provider_calls = []
    class Provider:
        def get_fills_by_order_no(self, **kwargs):
            provider_calls.append(kwargs)
            return _broker_open()
    class KIS:
        cancels = []
        def cancel_us_order(self, **kwargs):
            self.cancels.append(kwargs)
            return {"rt_cd": "0"}
    kis = KIS()
    args = {"intents": [_protective(symbol)], "provider": Provider(), "kis_client": kis,
            "trade_date": "2026-10-07", "env": "practice",
            "find_open": lambda *a, **k: [order], "reserve": lambda x: True}
    monkeypatch.delenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", raising=False)
    assert request_protective_tp_cancel(**args) == []
    assert kis.cancels == []
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    assert request_protective_tp_cancel(**args)[0]["status"] == "CANCEL_ACK_RECONCILE_REQUIRED"
    assert kis.cancels == [{"symbol": symbol, "exchange": "NASDAQ", "order_no": "0000034237"}]
    assert provider_calls[0]["trade_date"] == "2026-10-06"


def test_broker_open_validation_prevents_double_sell_and_ambiguous_cancel(monkeypatch):
    order = _old_tp()
    assert broker_proves_open_tp_order(order, _broker_open())
    assert not broker_proves_open_tp_order(order, _broker_open(remaining=0, status="CANCELLED"))
    assert not broker_proves_open_tp_order(order, dict(_broker_open(), filled_qty_present=False))
    assert not broker_proves_open_tp_order(order, dict(_broker_open(), order_no="99999"))
    assert protective_symbols([dict(_protective(), symbol="TQQQ")]) == set()
    assert protective_symbols([dict(_protective(), strategy_owner="TQQQ_INFINITE")]) == set()
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    class KIS:
        called = False
        def cancel_us_order(self, **kwargs):
            self.called = True
    kis = KIS()
    class Provider:
        def get_fills_by_order_no(self, **kwargs):
            return {"status": "OPEN", "requested_qty": 2, "remaining_qty": 2}
    result = request_protective_tp_cancel(
        intents=[_protective()], provider=Provider(), kis_client=kis,
        trade_date="2026-10-07", env="practice",
        find_open=lambda *a, **k: [order], reserve=lambda _: True,
    )
    assert result[0]["status"] == "FENCED"
    assert kis.called is False


def _filled_stage_and_order():
    stage = {"stage": "tp1", "stage_status": "DONE",
             "trade_date": "2026-09-23", "symbol": "PLTR",
             "client_order_key": "US_PC_PLTR_TP1", "requested_qty": 4,
             "cumulative_filled_qty": 0, "trading_epoch_id": "active-epoch"}
    order = {"trade_date": "2026-09-23", "symbol": "PLTR",
             "client_order_key": "US_PC_PLTR_TP1", "qty_requested": 4,
             "qty_filled": 4, "side": "SELL", "status": "FILLED",
             "trading_epoch_id": "active-epoch",
             "meta": {"profit_capture_stage": "tp1", "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                      "cumulative_filled_qty": 4}}
    return stage, order


def test_pltr_done_zero_counter_backfill_only_on_proven_actual_fill():
    stage, order = _filled_stage_and_order()
    assert authoritative_tp_fill_for_backfill(stage, order) == 4
    assert authoritative_tp_fill_for_backfill(stage, dict(order, status="ACK")) is None
    assert authoritative_tp_fill_for_backfill(stage, dict(order, trading_epoch_id="other")) is None
    assert authoritative_tp_fill_for_backfill(stage, dict(order, qty_filled=2)) is None
    assert authoritative_tp_fill_for_backfill(stage, dict(order, meta={})) is None
    assert authoritative_tp_fill_for_backfill(dict(stage, stage_status="PENDING"), order) is None
    assert authoritative_tp_fill_for_backfill(dict(stage, symbol="NVDA"), order) is None


@pytest.mark.parametrize("symbol", ["MRVL", "AMD"])
def test_cancel_ack_is_not_terminal_but_broker_terminal_reconcile_is(symbol, monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    order = _old_tp(symbol)
    order["meta"]["protective_tp_cancel_requested_at"] = "2026-10-07T17:00:00Z"
    calls = []
    class Provider:
        result = _broker_open()
        def get_fills_by_order_no(self, **kwargs):
            return self.result
    class KIS:
        def cancel_us_order(self, **kwargs):
            raise AssertionError("must not retry a reserved cancellation")
    provider = Provider()
    args = dict(
        intents=[_protective(symbol)], provider=provider, kis_client=KIS(),
        trade_date="2026-10-07", env="practice",
        find_open=lambda *a, **k: [order],
        reserve=lambda _: (_ for _ in ()).throw(AssertionError("reserve twice")),
        apply_observation=lambda **kwargs: calls.append(kwargs) or
                          {"status": "OK", "authoritative": True},
    )
    assert request_protective_tp_cancel(**args)[0]["status"] == "CANCEL_PENDING_RECONCILE_REQUIRED"
    assert calls == []
    provider.result = {
        "status": "CANCELLED", "order_no": "34237",
        "requested_qty": 2, "filled_qty": 0, "remaining_qty": 0,
        "filled_qty_present": True, "normalization_result": "normalized",
    }
    assert broker_proves_terminal_after_cancel(order, provider.result)
    assert request_protective_tp_cancel(**args)[0]["status"] == "CANCEL_TERMINAL_RECONCILED"
    assert calls[0]["client_order_key"] == order["client_order_key"]
    assert calls[0]["trade_date"] == "2026-10-06"
    assert calls[0]["evidence_type"] == "KIS_TERMINAL_CANCEL"
    assert calls[0]["broker_open_qty"] == 0


def test_cancel_terminal_missing_broker_fill_keeps_action_fenced(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    order = _old_tp()
    order["meta"]["protective_tp_cancel_requested_at"] = "sent"
    class Provider:
        def get_fills_by_order_no(self, **kwargs):
            return {"status": "CANCELLED", "order_no": "34237",
                    "requested_qty": 2, "remaining_qty": 0,
                    "filled_qty_present": False}
    assert request_protective_tp_cancel(
        intents=[_protective()], provider=Provider(), kis_client=object(),
        trade_date="2026-10-07", env="practice",
        find_open=lambda *a, **k: [order],
        apply_observation=lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("must not reconcile unknown fill")),
    )[0]["status"] == "CANCEL_PENDING_RECONCILE_REQUIRED"
