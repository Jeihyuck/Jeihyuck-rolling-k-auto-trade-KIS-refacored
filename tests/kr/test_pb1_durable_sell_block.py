from __future__ import annotations

from trader.kr.pb1.durable_sell_block import durable_sell_block, durable_sell_row_decision


class _Balance:
    def __init__(self, *, freshness, qty: int):
        self.freshness = freshness
        self._qty = qty

    def holding_qty(self, code: str) -> int:
        return self._qty


class _Repo:
    def __init__(self, rows=None, exc: Exception | None = None):
        self.rows = rows or []
        self.exc = exc
        self.calls = []

    def list_today_orders(self, env, *, side, code, status_exclude):
        self.calls.append((env, side, code, status_exclude))
        if self.exc is not None:
            raise self.exc
        return self.rows


class _Logger:
    def __init__(self):
        self.messages = []

    def info(self, msg, *args):
        self.messages.append((msg, args))

    def exception(self, msg, *args):
        self.messages.append((msg, args))


def test_durable_sell_row_decision_blocks_unconfirmed_tp1_to_tp2():
    blocked, reason = durable_sell_row_decision(
        row={
            "strategy": "pb1_pullback_close",
            "status": "ACKED",
            "stage": "TP1",
            "position_cycle_id": "cycle-x",
            "request_json": {"trade_session": "day"},
        },
        strategy_name="pb1_pullback_close",
        position_cycle_id="cycle-x",
        exit_stage="TP2",
        session="day",
        balance_fresh=True,
        remaining_qty=7,
    )

    assert blocked is True
    assert reason == "execution_unconfirmed"


def test_durable_sell_row_decision_allows_fresh_legal_progression():
    blocked, reason = durable_sell_row_decision(
        row={
            "strategy": "pb1_pullback_close",
            "status": "FILLED",
            "stage": "TP1",
            "position_cycle_id": "cycle-x",
            "request_json": {"trade_session": "day", "submitted_qty": 7, "pre_order_holding_qty": 14},
        },
        strategy_name="pb1_pullback_close",
        position_cycle_id="cycle-x",
        exit_stage="FULL_EXIT",
        session="day",
        balance_fresh=True,
        remaining_qty=7,
    )

    assert blocked is False
    assert reason is None


def test_durable_sell_block_fails_open_on_lookup_error():
    repo = _Repo(exc=RuntimeError("boom"))
    logger = _Logger()

    blocked, payload = durable_sell_block(
        orders_repo=repo,
        env="practice",
        code="010060",
        strategy_name="pb1_pullback_close",
        authoritative_balance=None,
        window_internal="day",
        position_cycle_id="cycle-x",
        exit_stage="FULL_EXIT",
        logger=logger,
    )

    assert blocked is True
    assert payload == {"status": "LOOKUP_FAILED"}
    assert repo.calls == [("practice", "SELL", "010060", ())]
    assert logger.messages
