"""Offline tests of the US353 read-only broker evidence exporter (no KIS)."""
import pytest

from scripts.us353_kis_practice_evidence_readonly import (
    assert_readonly, collect, selected_row,
)


def _readonly(monkeypatch):
    for k, v in (
        ("KIS_ENV", "practice"), ("WSL_RUN_MARKET", "US"),
        ("US_ORDER_ARMED", "0"), ("NO_TRADE", "1"), ("DRY_RUN", "1"),
    ):
        monkeypatch.setenv(k, v)


def test_refuses_any_live_or_unscoped_runtime(monkeypatch):
    _readonly(monkeypatch)
    assert_readonly()
    for key, forbidden in (
        ("KIS_ENV", "real"), ("WSL_RUN_MARKET", "KR"),
        ("US_ORDER_ARMED", "1"), ("NO_TRADE", "0"), ("DRY_RUN", "0"),
    ):
        with monkeypatch.context() as patch:
            patch.setenv(key, forbidden)
            with pytest.raises(RuntimeError, match="READONLY_PRACTICE_US_CHILD_REQUIRED"):
                assert_readonly()


def test_broker_evidence_get_only_redacted_and_complete(monkeypatch):
    from datetime import date
    _readonly(monkeypatch)

    class Broker:
        def __init__(self):
            self.calls = []

        def get_us_balance(self, *, force_refresh):
            assert force_refresh
            self.calls.append("GET_BALANCE")
            return {
                "balance_authoritative": True,
                "balance_complete": True,
                "queried_exchanges": ["NASD", "NYSE", "AMEX"],
                "output1": [{"CANO": "DO_NOT_PRINT"}],
            }

        def get_us_fills_today(self, *, trade_date):
            self.calls.append(("GET_FILLS", trade_date))
            return [{
                "pdno": "AMGN", "odno": "0000039784",
                "ord_dt": "20261110", "ft_ord_qty": "6",
                "ft_ccld_qty": "6", "CANO": "DO_NOT_PRINT",
                "authorization": "DO_NOT_PRINT",
            }]

    broker = Broker()
    result = collect(
        broker, date(2026, 11, 10), date(2026, 11, 10),
        lambda _raw: {"balance_parse_status": "OK",
                      "positions": [{"symbol": "AMGN", "qty": 6,
                                     "orderable_qty": 6, "exchange": "NASDAQ"}]},
        "private-account-do-not-print",
    )
    assert broker.calls == ["GET_BALANCE", ("GET_FILLS", "2026-11-10")]
    assert result["orders_posted"] == 0 and result["db_writes"] == 0
    assert result["positions"][0]["qty"] == 6
    assert result["orders"][0]["rows"][0]["order_no"] == "0000039784"
    import json
    rendered = json.dumps(result)
    assert "DO_NOT_PRINT" not in rendered
    assert "private-account-do-not-print" not in rendered
    assert "authorization" not in rendered
    assert "CANO" not in rendered


def test_incomplete_exchange_never_counts_as_authoritative(monkeypatch):
    from datetime import date
    _readonly(monkeypatch)

    class PartialBroker:
        def get_us_balance(self, *, force_refresh):
            return {"balance_authoritative": False, "balance_complete": False}

        def get_us_fills_today(self, *, trade_date):
            raise AssertionError("No fill GET after incomplete balance")

    with pytest.raises(RuntimeError, match="KIS_BALANCE_NOT_COMPLETE"):
        collect(
            PartialBroker(), date(2026, 11, 10), date(2026, 11, 11),
            lambda raw: {"balance_parse_status": "OK"}, "private-account",
        )


def test_whitelist_ignores_unknown_sensitive_keys():
    assert selected_row({
        "pdno": "TQQQ", "odno": "0000030080",
        "appsecret": "DO_NOT_PRINT", "CANO": "DO_NOT_PRINT",
    }) == {
        "symbol": "TQQQ", "order_no": "0000030080",
        "broker_date": "", "broker_time": "", "raw_side": "",
        "raw_status": "", "requested": "", "filled": "", "remaining": "",
    }
