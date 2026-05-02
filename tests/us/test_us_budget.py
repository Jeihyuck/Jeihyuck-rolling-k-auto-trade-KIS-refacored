# -*- coding: utf-8 -*-
"""tests/us/test_us_budget.py - 미국장 예산 계산 테스트."""
import os
import pytest


def _set_env(**kwargs):
    for k, v in kwargs.items():
        os.environ[k] = str(v)
    return kwargs


def test_get_us_capital_krw_default():
    os.environ.pop("US_PAPER_MAX_CAPITAL_KRW", None)
    from trader.us import budget as B
    import importlib
    importlib.reload(B)
    assert B.get_us_capital_krw() == 50_000_000.0


def test_get_us_capital_krw_custom():
    os.environ["US_PAPER_MAX_CAPITAL_KRW"] = "30000000"
    from trader.us import budget as B
    import importlib
    importlib.reload(B)
    assert B.get_us_capital_krw() == 30_000_000.0
    os.environ["US_PAPER_MAX_CAPITAL_KRW"] = "50000000"


def test_get_us_budget_fx_default():
    os.environ.pop("US_BUDGET_FX_KRW_PER_USD", None)
    from trader.us import budget as B
    import importlib
    importlib.reload(B)
    assert B.get_us_budget_fx() == 1450.0


def test_get_us_capital_usd_cap():
    os.environ["US_PAPER_MAX_CAPITAL_KRW"] = "50000000"
    os.environ["US_BUDGET_FX_KRW_PER_USD"] = "1450"
    from trader.us import budget as B
    import importlib
    importlib.reload(B)
    cap = B.get_us_capital_usd_cap()
    assert abs(cap - 34482.76) < 1.0, f"expected ~34482 got {cap}"


def test_resolve_us_order_budget_cap_binding():
    """available_cash가 cap을 초과할 때 cap이 effective 결과."""
    os.environ["US_PAPER_MAX_CAPITAL_KRW"] = "50000000"
    os.environ["US_BUDGET_FX_KRW_PER_USD"] = "1450"
    from trader.us import budget as B
    import importlib
    importlib.reload(B)

    result = B.resolve_us_order_budget(available_cash_usd=50000.0)
    cap = result["capital_usd_cap"]
    assert abs(cap - 34482.76) < 1.0
    assert result["effective_order_budget_usd"] <= cap + 0.01
    assert result["effective_order_budget_usd"] < 50000.0


def test_resolve_us_order_budget_cash_binding():
    """available_cash가 cap보다 작을 때 cash가 effective 결과."""
    os.environ["US_PAPER_MAX_CAPITAL_KRW"] = "50000000"
    os.environ["US_BUDGET_FX_KRW_PER_USD"] = "1450"
    from trader.us import budget as B
    import importlib
    importlib.reload(B)

    result = B.resolve_us_order_budget(available_cash_usd=10000.0)
    assert result["effective_order_budget_usd"] == 10000.0


def test_resolve_returns_all_keys():
    from trader.us import budget as B
    import importlib
    importlib.reload(B)
    result = B.resolve_us_order_budget(12000.0)
    for key in ("capital_krw", "fx_krw_per_usd", "capital_usd_cap",
                "available_cash_usd", "effective_order_budget_usd"):
        assert key in result, f"missing key: {key}"
