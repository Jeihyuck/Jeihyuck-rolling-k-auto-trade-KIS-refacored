"""Regression for KIS KST order dates vs US (ET/EST) trading dates.

The 2026-10-06 AMD order at 15:14 UTC is 2026-10-07 KST. Single-day
ORD_STRT_DT=ORD_END_DT=20261006 did not even query its broker KST date.
"""
from __future__ import annotations

import pytest

from trader.us.execution.kis_us_client import KisUSClient, KisUSClientError


def _row(symbol, order, broker_day, broker_time, fill="0", qty="1"):
    return {
        "odno": order, "pdno": symbol, "sll_buy_dvsn_cd": "01",
        "ord_dt": broker_day, "ord_tmd": broker_time,
        "ft_ord_qty": qty, "ft_ccld_qty": fill,
        "nccs_qty": str(int(qty) - int(fill)),
    }


def _client_with_rows(monkeypatch, rows):
    client = object.__new__(KisUSClient)
    client.env = "practice"
    client._cano = "test"
    client._acnt_prdt_cd = "01"
    client._assert_not_offline = lambda *args: None
    client._build_headers = lambda tr_id: {"tr_id": tr_id}
    params_seen = []

    def fake_get(path, headers, params, suppress_final_log=False):
        params_seen.append(dict(params))
        return {"output": rows, "_response_meta": {"tr_cont": "D"}}

    client._get = fake_get
    return client, params_seen


def test_oct6_amd_post_midnight_kst_is_visible_in_oct6_ny_broker_query(monkeypatch):
    rows = [
        _row("MRVL", "0000034237", "20261006", "225919"),  # same KST date
        _row("AMD", "0000035281", "20261007", "001429"),   # next KST date
        _row("MSFT", "tomorrow", "20261007", "224500"),    # next US day; exclude
        _row("AAPL", "yesterday", "20261006", "004500"),   # previous US day; exclude
    ]
    client, seen = _client_with_rows(monkeypatch, rows)
    result = client.get_us_fills_today("2026-10-06")
    assert [row["odno"] for row in result] == ["0000034237", "0000035281"]
    assert seen[0]["ORD_STRT_DT"] == "20261006"
    assert seen[0]["ORD_END_DT"] == "20261007"
    assert seen[0]["ORD_DT"] == ""


def test_oct12_msft_kst_next_day_belongs_to_oct12_ny(monkeypatch):
    # 2026-10-12 15:28 UTC = 2026-10-13 00:28 KST.
    client, seen = _client_with_rows(monkeypatch, [
        _row("MSFT", "0000044796", "20261013", "002823"),
    ])
    assert [r["odno"] for r in client.get_us_today_orders("2026-10-12")] == ["0000044796"]
    assert seen[0]["ORD_END_DT"] == "20261013"


def test_est_winter_midnight_boundary_does_not_adopt_next_session(monkeypatch):
    client, seen = _client_with_rows(monkeypatch, [
        _row("AMD", "winter", "20270113", "013500"),     # 2027-01-12 11:35 EST
        _row("AAPL", "next", "20270113", "234000"),      # 2027-01-13 09:40 EST
    ])
    assert [r["odno"] for r in client.get_us_fills_today("2027-01-12")] == ["winter"]
    assert seen[0]["ORD_END_DT"] == "20270113"


def test_broker_row_without_order_clock_cannot_prove_current_us_date(monkeypatch):
    client, _ = _client_with_rows(monkeypatch, [
        {"odno": "MISSING_TIME", "pdno": "AMD", "ord_dt": "20261007",
         "ft_ord_qty": "1", "ft_ccld_qty": "0"},
    ])
    with pytest.raises(KisUSClientError, match="KIS_BROKER_ORDER_DATE_UNVERIFIED"):
        client.get_us_today_orders("2026-10-06")


def test_source_timestamp_invalid_is_contract_error(monkeypatch):
    client, _ = _client_with_rows(monkeypatch, [
        _row("AMD", "invalid", "20261007", "266199"),
    ])
    with pytest.raises(KisUSClientError, match="KIS order date/time schema invalid"):
        client.get_us_fills_today("2026-10-06")


def test_sparse_mock_rows_preserve_pagination_fixture_contract(monkeypatch):
    client, _ = _client_with_rows(monkeypatch, [{"odno": "fixture"}])
    assert client.get_us_fills_today("2026-10-06") == [{"odno": "fixture"}]


def test_exact_trade_date_bridge_is_independent_of_current_wall_clock(monkeypatch):
    client, seen = _client_with_rows(monkeypatch, [
        _row("NVDA", "older", "20260821", "020000"),
    ])
    assert [r["odno"] for r in client.get_us_fills_today("2026-08-20")] == ["older"]
    assert seen[0]["ORD_STRT_DT"] == "20260820"
    assert seen[0]["ORD_END_DT"] == "20260821"


def test_live_fill_parser_retains_actual_oct6_fill_from_oct7_kst(monkeypatch):
    from trader.us.execution.fills import get_fills_today
    client, seen = _client_with_rows(monkeypatch, [
        {
            **_row("AMD", "0000035281", "20261007", "001429", fill="1"),
            "ft_ccld_unpr3": "165.55", "ovrs_excg_cd": "NASD",
        },
        _row("NVDA", "next-us-session", "20261007", "224500", fill="1"),
    ])

    class Provider:
        _offline = False
        _tick_context = None

        def _get_client(self):
            return client

    result = get_fills_today(provider=Provider(), trade_date="2026-10-06")
    assert result["status"] == "OK"
    assert [f["order_no"] for f in result["fills"]] == ["0000035281"]
    assert result["fills"][0]["order_timestamp_utc"].startswith(
        "2026-10-06T15:14:29"
    )
    assert result["fills"][0]["meta"]["source_timestamp_date"] == "20261007"
    assert seen[0]["ORD_END_DT"] == "20261007"


def test_broker_date_filter_prevents_kst_day_order_cross_contamination(monkeypatch):
    client, _ = _client_with_rows(monkeypatch, [
        _row("AMD", "same-number-prev-us-day", "20261007", "000010"),
        _row("AMD", "same-number-new-us-day", "20261007", "230010"),
    ])
    assert [r["odno"] for r in client.get_us_today_orders("2026-10-06")] == [
        "same-number-prev-us-day",
    ]


def test_unfilled_oct6_amd_order_is_normalized_as_open_not_missing(monkeypatch):
    from trader.us.data_provider import USDataProvider
    client, seen = _client_with_rows(monkeypatch, [
        _row("AMD", "0000035281", "20261007", "001429", fill="0"),
    ])
    provider = object.__new__(USDataProvider)
    provider._offline = False
    provider._get_client = lambda: client
    orders = provider.get_today_orders("2026-10-06")
    assert len(orders) == 1
    assert orders[0]["canonical_order_no"] == "35281"
    assert orders[0]["status"] == "OPEN"
    assert orders[0]["us_trade_date"] == "2026-10-06"
    assert orders[0]["source_timestamp_date"] == "20261007"
    assert seen[0]["ORD_END_DT"] == "20261007"


def test_live_edst_session_does_not_query_future_kst_day():
    from datetime import datetime, timezone
    before_kst_midnight = datetime(2026, 10, 6, 13, 30, tzinfo=timezone.utc)
    after_kst_midnight = datetime(2026, 10, 6, 15, 30, tzinfo=timezone.utc)
    assert KisUSClient._broker_date_range_for_us_session(
        "20261006", observed_at=before_kst_midnight
    ) == ("20261006", "20261006")
    assert KisUSClient._broker_date_range_for_us_session(
        "20261006", observed_at=after_kst_midnight
    ) == ("20261006", "20261007")


def test_historical_recovery_always_queries_both_kst_dates():
    from datetime import datetime, timezone
    now = datetime(2026, 10, 13, 8, tzinfo=timezone.utc)
    assert KisUSClient._broker_date_range_for_us_session(
        "20261006", observed_at=now
    ) == ("20261006", "20261007")
