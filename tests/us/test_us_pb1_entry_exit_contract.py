# -*- coding: utf-8 -*-
"""tests/us/test_us_pb1_entry_exit_contract.py - PB1 entry/exit 계약 테스트."""
import os
import pytest


def _setup_env():
    os.environ.update({
        "TRADING_REGION": "US",
        "US_AGENT_ENABLED": "1",
        "KIS_ENV": "practice",
        "US_PAPER_TRADING_ENABLED": "1",
        "US_LIVE_TRADING_ENABLED": "0",
        "DISABLE_REAL_TRADING": "1",
        "ALLOW_REAL_ORDER": "0",
        "US_BLOCK_REBUY_AFTER_SELL_SAME_DAY": "0",
        "US_ORDER_ACCEPTED_IS_NOT_FILLED": "0",
        "US_PAPER_MAX_CAPITAL_KRW": "50000000",
        "US_BUDGET_FX_KRW_PER_USD": "1450",
        "DRY_RUN": "1",
    })


# ---------------------------------------------------------------------------
# Entry engine 계약
# ---------------------------------------------------------------------------

def test_entry_engine_returns_list():
    _setup_env()
    from trader.us.pb1.us_pb1_engine import USPb1Engine
    from trader.us.data_provider import USDataProvider

    engine = USPb1Engine(env="practice", offline=True)
    provider = USDataProvider(offline=True)

    result = engine.evaluate_entries(
        tickers=["AAPL", "MSFT"],
        provider=provider,
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
    )
    assert isinstance(result, list)


def test_entry_engine_respects_sold_today():
    """sold_today에 있는 종목은 entry intent에서 제외되어야 한다."""
    _setup_env()
    from trader.us.pb1.us_pb1_engine import USPb1Engine
    from trader.us.data_provider import USDataProvider

    engine = USPb1Engine(env="practice", offline=True)
    provider = USDataProvider(offline=True)

    result = engine.evaluate_entries(
        tickers=["AAPL"],
        provider=provider,
        sold_today={"AAPL"},
        available_cash_usd=10000.0,
        position_count=0,
    )
    # AAPL이 sold_today에 있으므로 intent에 없어야 함
    assert all(i.get("symbol") != "AAPL" for i in result), \
        "AAPL should be blocked due to sold_today"


def test_entry_intents_have_required_keys():
    """entry intent는 필수 키를 포함해야 한다."""
    _setup_env()
    from trader.us.pb1.us_pb1_engine import USPb1Engine
    from trader.us.data_provider import USDataProvider

    engine = USPb1Engine(env="practice", offline=True)
    provider = USDataProvider(offline=True)

    result = engine.evaluate_entries(
        tickers=["AAPL", "MSFT", "GOOGL"],
        provider=provider,
        sold_today=set(),
        available_cash_usd=20000.0,
        position_count=0,
    )

    required_keys = {"symbol", "exchange", "side", "qty", "notional_usd",
                     "client_order_key", "strategy"}
    for intent in result:
        missing = required_keys - set(intent.keys())
        assert not missing, f"Intent missing keys: {missing}\nintent={intent}"
        assert intent["side"] == "BUY"


# ---------------------------------------------------------------------------
# Exit engine 계약
# ---------------------------------------------------------------------------

def test_exit_engine_returns_list():
    _setup_env()
    from trader.us.pb1.us_pb1_engine import USPb1Engine
    from trader.us.data_provider import USDataProvider

    engine = USPb1Engine(env="practice", offline=True)
    provider = USDataProvider(offline=True)

    result = engine.evaluate_exits(positions=[], provider=provider)
    assert result == []


def test_exit_hard_stop_trigger():
    """entry_price 대비 7% 이상 손실 시 hard_stop 신호 생성."""
    from trader.us.pb1.us_exit_engine import evaluate_exit

    position = {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "qty": 10,
        "entry_price": 100.0,
        "max_price": 105.0,
    }
    # 8% 손실
    result = evaluate_exit(position=position, current_price=92.0)

    assert result is not None
    assert result["exit_type"] == "hard_stop"
    assert result["side"] == "SELL"
    assert result["qty"] == 10


def test_exit_trailing_stop_trigger():
    """고점 대비 5% 이상 하락 시 trailing_stop 신호 생성."""
    from trader.us.pb1.us_exit_engine import evaluate_exit

    position = {
        "symbol": "TSLA",
        "exchange": "NASDAQ",
        "qty": 5,
        "entry_price": 100.0,
        "max_price": 150.0,
    }
    # 고점 150에서 6% 하락 → 141
    result = evaluate_exit(position=position, current_price=141.0)

    assert result is not None
    assert result["exit_type"] == "trailing_stop"


def test_exit_no_signal_when_ok():
    """정상 보유 포지션에서는 청산 신호 없어야 한다."""
    from trader.us.pb1.us_exit_engine import evaluate_exit

    position = {
        "symbol": "NVDA",
        "exchange": "NASDAQ",
        "qty": 3,
        "entry_price": 100.0,
        "max_price": 105.0,
    }
    # 3% 수익 — 청산 조건 미달
    result = evaluate_exit(position=position, current_price=103.0)

    assert result is None


# ---------------------------------------------------------------------------
# Risk gate 예산 차단 테스트
# ---------------------------------------------------------------------------

def test_risk_gate_budget_exceeded_blocks():
    """effective_order_budget_usd를 초과하는 주문은 차단되어야 한다."""
    _setup_env()
    os.environ["US_PAPER_MAX_CAPITAL_KRW"] = "50000000"
    os.environ["US_BUDGET_FX_KRW_PER_USD"] = "1450"
    # cap = 34482.76 USD

    from trader.us.execution.risk_gate import check_us_capital_budget, RiskGateBlocked

    with pytest.raises(RiskGateBlocked, match="us_capital_budget_exceeded"):
        check_us_capital_budget(
            order_notional_usd=40000.0,  # cap 초과
            available_cash_usd=50000.0,
            symbol="AAPL",
        )


def test_risk_gate_budget_passes_within_cap():
    """cap 안에 드는 주문은 통과해야 한다."""
    _setup_env()
    os.environ["US_PAPER_MAX_CAPITAL_KRW"] = "50000000"
    os.environ["US_BUDGET_FX_KRW_PER_USD"] = "1450"

    from trader.us.execution.risk_gate import check_us_capital_budget

    # 예외 없이 통과
    check_us_capital_budget(
        order_notional_usd=5000.0,
        available_cash_usd=50000.0,
        symbol="MSFT",
    )


def test_risk_gate_same_day_rebuy_block():
    """sold_today에 있는 symbol BUY는 차단되어야 한다."""
    _setup_env()
    os.environ["US_BLOCK_REBUY_AFTER_SELL_SAME_DAY"] = "1"

    from trader.us.db import repos
    # 메모리 fills에 SELL fill 추가
    from datetime import date
    repos._MEM_FILLS.clear()
    repos._MEM_FILLS.append({
        "symbol": "AAPL",
        "side": "SELL",
        "trade_date": date.today().isoformat(),
        "qty": 10,
        "price": 150.0,
    })

    from trader.us.execution.risk_gate import check_same_day_rebuy, RiskGateBlocked

    with pytest.raises(RiskGateBlocked, match="same_day_rebuy_block"):
        check_same_day_rebuy(symbol="AAPL", side="BUY")

    repos._MEM_FILLS.clear()
    os.environ["US_BLOCK_REBUY_AFTER_SELL_SAME_DAY"] = "0"
