# -*- coding: utf-8 -*-
"""tests/us/test_us_symbol_registry.py

US Symbol Registry 단위 테스트.
"""
import pytest
from trader.us.symbols import (
    normalize_symbol,
    resolve_exchange,
    get_quote_exchange_code,
    get_order_exchange_code,
    reject_unknown_symbol,
    register_symbol,
    is_known_symbol,
    list_known_symbols,
)


class TestNormalizeSymbol:
    def test_uppercase(self):
        assert normalize_symbol("nvda") == "NVDA"

    def test_strip_spaces(self):
        assert normalize_symbol("  AAPL  ") == "AAPL"

    def test_already_upper(self):
        assert normalize_symbol("MSFT") == "MSFT"

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            normalize_symbol("")

    def test_none_raises(self):
        with pytest.raises(ValueError):
            normalize_symbol(None)  # type: ignore

    def test_too_long_raises(self):
        with pytest.raises(ValueError):
            normalize_symbol("TOOLONG")

    def test_digits_raises(self):
        with pytest.raises(ValueError):
            normalize_symbol("123456")

    def test_valid_short(self):
        # 1-5글자 알파벳만 허용
        assert normalize_symbol("A") == "A"
        assert normalize_symbol("GOOGL") == "GOOGL"


class TestResolveExchange:
    def test_known_nasdaq(self):
        assert resolve_exchange("NVDA") == "NASDAQ"

    def test_known_nyse(self):
        assert resolve_exchange("SPY") == "NYSE"

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="unknown symbol"):
            resolve_exchange("ZZZZZ")

    def test_case_insensitive(self):
        assert resolve_exchange("nvda") == "NASDAQ"


class TestExchangeCodes:
    def test_quote_code_nasdaq(self):
        assert get_quote_exchange_code("NASDAQ") == "NAS"

    def test_quote_code_nyse(self):
        assert get_quote_exchange_code("NYSE") == "NYS"

    def test_quote_code_amex(self):
        assert get_quote_exchange_code("AMEX") == "AMS"

    def test_order_code_nasdaq(self):
        assert get_order_exchange_code("NASDAQ") == "NASD"

    def test_order_code_nyse(self):
        assert get_order_exchange_code("NYSE") == "NYSE"

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            get_quote_exchange_code("KRX")


class TestRejectUnknownSymbol:
    def test_known_passes(self):
        reject_unknown_symbol("NVDA")  # should not raise

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            reject_unknown_symbol("UNKNWN")


class TestRegisterSymbol:
    def test_register_new(self):
        register_symbol("TSLA", "NASDAQ")
        assert is_known_symbol("TSLA")
        assert resolve_exchange("TSLA") == "NASDAQ"

    def test_register_unknown_exchange_raises(self):
        with pytest.raises(ValueError):
            register_symbol("TEST", "KRX")


class TestIsKnownSymbol:
    def test_known(self):
        assert is_known_symbol("AAPL") is True

    def test_unknown(self):
        assert is_known_symbol("ZZZXXX") is False

    def test_bad_format(self):
        assert is_known_symbol("") is False


class TestListKnownSymbols:
    def test_returns_list(self):
        syms = list_known_symbols()
        assert isinstance(syms, list)
        assert "NVDA" in syms
        assert "AAPL" in syms
