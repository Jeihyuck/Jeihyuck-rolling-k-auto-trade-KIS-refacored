"""
[CONTRACT] 비거래일/휴일 compute-only 워크플로 계약.

- AM/PM/Close workflow에 order_mode_guard step이 존재해야 한다.
- is_trading_day != '1' 조건에서 `should_run == '1'`에 의해서만 제어되어야 한다.
- 모든 주요 step에서 `is_trading_day == '1'` 강 조건이 없어야 한다.
- Emit non trading day summary step이 Initialize trigger log 이후에 존재해야 한다.
- ORDER_ALLOWED가 Run trade loop env에 있어야 한다.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

WORKFLOWS_DIR = Path(__file__).parent.parent / ".github" / "workflows"

SESSION_MAP = {
    "am": "trade-am.yml",
    "pm": "trade-pm.yml",
    "close": "trade-close.yml",
}


def _load(session_key: str) -> str:
    path = WORKFLOWS_DIR / SESSION_MAP[session_key]
    assert path.exists(), f"workflow not found: {path}"
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("session", ["am", "pm", "close"])
def test_order_mode_guard_step_exists(session: str) -> None:
    """order_mode_guard step이 존재해야 한다."""
    text = _load(session)
    assert "Resolve order mode guard" in text, (
        f"trade-{session}.yml must contain 'Resolve order mode guard' step"
    )


@pytest.mark.parametrize("session", ["am", "pm", "close"])
def test_order_mode_guard_has_id(session: str) -> None:
    """order_mode_guard step에 id: order_mode_guard 가 있어야 한다."""
    text = _load(session)
    assert "id: order_mode_guard" in text, (
        f"trade-{session}.yml order_mode_guard must set id: order_mode_guard"
    )


@pytest.mark.parametrize("session", ["am", "pm", "close"])
def test_no_is_trading_day_gate_on_checkout(session: str) -> None:
    """Checkout step에 is_trading_day == '1' 조건이 없어야 한다."""
    text = _load(session)
    # Find the Checkout step's if condition line
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if "name: Checkout" in line:
            # Check the next 3 lines for if condition
            context = "\n".join(lines[i : i + 4])
            assert "is_trading_day" not in context, (
                f"trade-{session}.yml Checkout step must NOT have is_trading_day condition"
            )
            break


@pytest.mark.parametrize("session", ["am", "pm", "close"])
def test_run_trade_loop_has_order_allowed_env(session: str) -> None:
    """Run trade loop step에 ORDER_ALLOWED env가 있어야 한다."""
    text = _load(session)
    assert "ORDER_ALLOWED:" in text, (
        f"trade-{session}.yml Run trade loop must set ORDER_ALLOWED env"
    )


@pytest.mark.parametrize("session", ["am", "pm", "close"])
def test_run_trade_loop_has_pb1_compute_only_env(session: str) -> None:
    """Run trade loop step에 PB1_COMPUTE_ONLY env가 있어야 한다."""
    text = _load(session)
    assert "PB1_COMPUTE_ONLY:" in text, (
        f"trade-{session}.yml Run trade loop must set PB1_COMPUTE_ONLY env"
    )


@pytest.mark.parametrize("session", ["am", "pm", "close"])
def test_emit_non_trading_day_summary_after_init_log(session: str) -> None:
    """Emit non trading day summary가 Initialize trigger log 이후에 있어야 한다."""
    text = _load(session)
    init_pos = text.find(f"Initialize {session.upper() if session == 'am' else session.capitalize()} trigger log")
    if init_pos == -1:
        # Try alternate capitalizations
        for label in ["Initialize AM trigger log", "Initialize PM trigger log", "Initialize Close trigger log"]:
            init_pos = text.find(label)
            if init_pos != -1:
                break
    emit_pos = text.find("Emit non trading day summary")
    assert init_pos != -1, f"trade-{session}.yml must have Initialize trigger log step"
    assert emit_pos != -1, f"trade-{session}.yml must have Emit non trading day summary step"
    assert emit_pos > init_pos, (
        f"trade-{session}.yml: Emit non trading day summary must come AFTER Initialize trigger log"
    )


@pytest.mark.parametrize("session", ["am", "pm", "close"])
def test_emit_non_trading_day_uses_tee_append(session: str) -> None:
    """Emit non trading day summary step이 tee -a를 사용해야 한다."""
    text = _load(session)
    lines = text.splitlines()
    in_emit_step = False
    for line in lines:
        if "Emit non trading day summary" in line:
            in_emit_step = True
        if in_emit_step and "tee " in line:
            assert "tee -a" in line, (
                f"trade-{session}.yml Emit non trading day summary must use 'tee -a'"
            )
            break


@pytest.mark.parametrize("session", ["am", "pm", "close"])
def test_trading_day_guard_line_in_init_log(session: str) -> None:
    """Initialize trigger log에 TRADING_DAY_GUARD 라인이 출력돼야 한다."""
    text = _load(session)
    tag = f"TRADE_{session.upper()}" if session in ("am", "pm") else "TRADE_CLOSE"
    assert f"[{tag}][TRADING_DAY_GUARD]" in text, (
        f"trade-{session}.yml Initialize trigger log must emit [{tag}][TRADING_DAY_GUARD]"
    )


@pytest.mark.parametrize("session", ["am", "pm", "close"])
def test_order_mode_line_in_init_log(session: str) -> None:
    """Initialize trigger log에 ORDER_MODE 라인이 출력돼야 한다."""
    text = _load(session)
    tag = f"TRADE_{session.upper()}" if session in ("am", "pm") else "TRADE_CLOSE"
    assert f"[{tag}][ORDER_MODE]" in text, (
        f"trade-{session}.yml Initialize trigger log must emit [{tag}][ORDER_MODE]"
    )
