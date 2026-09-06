from trader.kr.pb1.effective_exit_risk import resolve_effective_exit_risk_for_pos
from trader.pb1_engine import _resolve_effective_exit_risk_for_pos


def test_effective_exit_risk_helper_matches_wrapper(monkeypatch):
    monkeypatch.setenv("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "1")
    monkeypatch.setenv("PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT", "7.0")
    pos = {
        "code": "028050",
        "avg_buy_price": 50500.0,
        "stop_price_at_entry": 40739.0,
        "market": "KOSPI",
        "position_meta": {"initial_stop_price": 40739.0},
    }

    assert resolve_effective_exit_risk_for_pos(pos) == _resolve_effective_exit_risk_for_pos(pos)
