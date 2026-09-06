from trader.kr.pb1.exit_reason_code import resolve_exit_reason_code
from trader.reasons import ReasonCode


def test_resolve_exit_reason_code_maps_exit_reasons():
    assert resolve_exit_reason_code("EXIT_STOP_LOSS") == ReasonCode.EXIT_STOP_LOSS
    assert resolve_exit_reason_code("EXIT_TRAILING_STOP") == ReasonCode.EXIT_TRAIL
    assert resolve_exit_reason_code("EXIT_MA50_BREAK") == ReasonCode.EXIT_REGIME
    assert resolve_exit_reason_code("EXIT_MA20_BREAK") == ReasonCode.EXIT_TRAIL
    assert resolve_exit_reason_code("EXIT_TIME_STOP") == ReasonCode.EXIT_TIME
    assert resolve_exit_reason_code("EXIT_RISK_OFF") == ReasonCode.EXIT_REGIME
    assert resolve_exit_reason_code("STOP_HIT") == ReasonCode.EXIT_STOP_LOSS
    assert resolve_exit_reason_code("FAILED_BREAKOUT") == ReasonCode.EXIT_STOP_LOSS
    assert resolve_exit_reason_code("TP1") == ReasonCode.EXIT_TP_PARTIAL
    assert resolve_exit_reason_code("TP2") == ReasonCode.EXIT_TP_PARTIAL
    assert resolve_exit_reason_code("climax_partial") == ReasonCode.EXIT_TP_PARTIAL
    assert resolve_exit_reason_code("time_stop") == ReasonCode.EXIT_TIME
    assert resolve_exit_reason_code("ma50_break_heavy_volume") == ReasonCode.EXIT_REGIME


def test_resolve_exit_reason_code_defaults_to_trail():
    assert resolve_exit_reason_code("UNKNOWN") == ReasonCode.EXIT_TRAIL
