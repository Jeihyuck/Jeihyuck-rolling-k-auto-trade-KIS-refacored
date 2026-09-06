from __future__ import annotations

from trader.reasons import ReasonCode


def resolve_exit_reason_code(reason: str) -> str:
    mapping = {
        "EXIT_STOP_LOSS": ReasonCode.EXIT_STOP_LOSS,
        "EXIT_TRAILING_STOP": ReasonCode.EXIT_TRAIL,
        "EXIT_MA50_BREAK": ReasonCode.EXIT_REGIME,
        "EXIT_MA20_BREAK": ReasonCode.EXIT_TRAIL,
        "EXIT_TIME_STOP": ReasonCode.EXIT_TIME,
        "EXIT_RISK_OFF": ReasonCode.EXIT_REGIME,
        "STOP_HIT": ReasonCode.EXIT_STOP_LOSS,
        "FAILED_BREAKOUT": ReasonCode.EXIT_STOP_LOSS,
        "TP1": ReasonCode.EXIT_TP_PARTIAL,
        "TP2": ReasonCode.EXIT_TP_PARTIAL,
        "climax_partial": ReasonCode.EXIT_TP_PARTIAL,
        "time_stop": ReasonCode.EXIT_TIME,
        "ma50_break_heavy_volume": ReasonCode.EXIT_REGIME,
    }
    return mapping.get(reason, ReasonCode.EXIT_TRAIL)
