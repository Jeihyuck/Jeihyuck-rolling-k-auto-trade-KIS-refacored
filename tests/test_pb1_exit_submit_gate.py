from __future__ import annotations

from trader.kr.pb1.exit_submit_gate import resolve_exit_submit_gate_reasons
from trader.pb1_engine import PB1Engine


def test_resolve_exit_submit_gate_reasons_returns_copy_and_logs_context() -> None:
    reasons = ["EXIT_GUARD"]
    resolved = resolve_exit_submit_gate_reasons(
        order_precheck_gate_reasons=reasons,
        display_code="000001",
        order_allowed=False,
        trading_day=True,
        force_block_live=False,
        exit_holdings_source="db",
    )

    assert resolved == ["EXIT_GUARD"]
    assert resolved is not reasons


def test_pb1engine_exit_submit_gate_wrapper_matches_helper() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine.order_allowed = False
    engine.trading_day = True
    engine.force_block_live = False
    engine._exit_holdings_meta = {"source": "db"}
    engine._display_code = lambda code: code
    engine._order_precheck_gate_reasons = lambda side, stage: ["EXIT_GUARD"]

    helper = resolve_exit_submit_gate_reasons(
        order_precheck_gate_reasons=["EXIT_GUARD"],
        display_code="000001",
        order_allowed=False,
        trading_day=True,
        force_block_live=False,
        exit_holdings_source="db",
    )
    wrapper = engine._resolve_exit_submit_gate_reasons(code="000001")

    assert helper == wrapper == ["EXIT_GUARD"]
