from datetime import datetime
from zoneinfo import ZoneInfo

from trader.pb1_engine import PB1Engine
from tests.kr.test_kr_entry_authoritative_gate_state import _build_candidate, _make_engine


def test_kr_buy_block_boundaries(monkeypatch, caplog):
    monkeypatch.setenv("KR_OPENING_BUY_BLOCK_ENABLED", "1")
    tz = ZoneInfo("Asia/Seoul")
    assert PB1Engine._is_kr_opening_buy_blocked(datetime(2026, 8, 24, 9, 10, tzinfo=tz)) == (True, "09:30:00")
    assert PB1Engine._is_kr_opening_buy_blocked(datetime(2026, 8, 24, 9, 30, tzinfo=tz))[0] is False


def test_kr_opening_gate_is_buy_only():
    """The helper has no exit permission input/output, so SELL remains routable."""
    blocked, _ = PB1Engine._is_kr_opening_buy_blocked(
        datetime(2026, 8, 24, 9, 10, tzinfo=ZoneInfo("Asia/Seoul"))
    )
    exit_can_proceed = True
    assert blocked is True and exit_can_proceed is True


def test_central_pre_submit_blocks_actual_buy_call_at_0910(monkeypatch, caplog):
    caplog.set_level("INFO")
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine = _make_engine()
    engine._now_kst = datetime(2026, 8, 24, 9, 10, tzinfo=ZoneInfo("Asia/Seoul"))

    status = engine._place_entry(_build_candidate("018260"))

    assert status["api_submitted"] == 0
    assert engine.kis.buy_calls == 0
    assert status["skipped_reason"] == "OPENING_30MIN_BUY_BLOCK"
    assert status["terminal_event"] == "FINAL_SKIP"
    assert "[OPENING_BUY_BLOCK][KR]" in caplog.text


def test_central_pre_submit_allows_actual_buy_call_at_0930(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda kis, code: (True, "ok"))
    engine = _make_engine()
    engine._now_kst = datetime(2026, 8, 24, 9, 30, tzinfo=ZoneInfo("Asia/Seoul"))

    status = engine._place_entry(_build_candidate("018260"))

    assert status["api_submitted"] == 1
    assert engine.kis.buy_calls == 1


def test_kr_order_candidate_always_has_terminal_submit_event(monkeypatch):
    monkeypatch.setattr(
        "trader.pb1_engine.enforce_kr_order_ownership",
        lambda *_args: (False, "ownership_reserved"),
    )
    engine = _make_engine()

    regular = engine._place_entry(_build_candidate("018260"))
    close = engine._place_entry_close(_build_candidate("018260"))

    assert regular["terminal_event"] == close["terminal_event"] == "FINAL_SKIP"


def test_kr_price_gate_block_is_terminal_event_not_exception(monkeypatch):
    monkeypatch.setattr("trader.pb1_engine.validate_tradeable", lambda *_args: (False, "price_gate_blocked"))
    engine = _make_engine()
    engine._now_kst = datetime(2026, 8, 24, 9, 30, tzinfo=ZoneInfo("Asia/Seoul"))

    status = engine._place_entry(_build_candidate("018260"))

    assert status["terminal_event"] == "FINAL_SKIP"
