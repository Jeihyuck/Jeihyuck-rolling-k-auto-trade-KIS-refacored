"""Regression for real 2026-10-06 AMD/MRVL and 2026-10-12 MSFT events.

These are timestamps and anonymized KIS CCNL fields observed in the existing
practice ledger, not synthetic evidence that the broker order is terminal.
"""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from trader.us.data_provider import normalize_us_order_status_row
from trader.us.execution.fills import _combine_kis_date_time
from trader.us.utils.kis_order_time import resolve_kis_us_order_datetime

_ET = ZoneInfo("America/New_York")
_KST = ZoneInfo("Asia/Seoul")


@pytest.mark.parametrize(
    "market_date,kst_clock,submitted_utc,expected_kst_date",
    [
        ("20261012", "002821", "2026-10-12T15:28:21+00:00", "2026-10-13"),
        ("20261006", "001427", "2026-10-06T15:14:27+00:00", "2026-10-07"),
        ("20261006", "225917", "2026-10-06T13:59:17+00:00", "2026-10-06"),
        ("20261006", "224454", "2026-10-06T13:44:54+00:00", "2026-10-06"),
        ("20261201", "001500", "2026-12-01T15:15:00+00:00", "2026-12-02"),
    ],
)
def test_broker_us_session_date_and_korea_clock_map_to_exact_order_instant(
    market_date, kst_clock, submitted_utc, expected_kst_date,
):
    local = resolve_kis_us_order_datetime(market_date, kst_clock)
    assert local.date().isoformat() == expected_kst_date
    assert local.astimezone(timezone.utc).isoformat() == submitted_utc
    assert local.astimezone(_ET).date().strftime("%Y%m%d") == market_date
    assert local.utcoffset() == local.astimezone(_KST).utcoffset()


def test_msft_real_order_status_no_longer_claims_sunday_et_trade_date():
    raw = {
        "odno": "44796", "pdno": "MSFT", "ord_dt": "20261012",
        "ord_tmd": "002821", "ft_ord_qty": "1",
        "ft_ccld_qty": "0", "nccs_qty": "1",
        "sll_buy_dvsn_cd": "01", "ovrs_excg_cd": "NASD",
    }
    raw["dmst_ord_dt"] = "20261013"
    result = normalize_us_order_status_row(raw)
    assert result["normalization_result"] == "normalized"
    assert result["status"] == "OPEN"
    assert result["order_no"] == "44796"
    assert result["us_trade_date"] == "2026-10-12"
    assert result["submitted_at_utc"] == "2026-10-12T15:28:21+00:00"
    assert result["order_timestamp_date_basis"] == "US_TRADE_SESSION_DATE_KST_CLOCK"
    assert result["raw"] is raw


def test_amd_order_fills_parser_does_not_shift_actual_event_by_one_day():
    source = _combine_kis_date_time("20261006", "001427")
    assert source == "2026-10-07T00:14:27+09:00"
    assert datetime.fromisoformat(source).astimezone(_ET).date().isoformat() == "2026-10-06"


def test_invalid_broker_order_datetime_fail_closed():
    with pytest.raises(ValueError):
        resolve_kis_us_order_datetime("20261012", "250099")


def test_kis_domestic_date_disagreement_quarantines_order_proof():
    raw = {
        "odno": "44796", "pdno": "MSFT", "ord_dt": "20261012",
        "dmst_ord_dt": "20261012", "ord_tmd": "002821",
        "ft_ord_qty": "1", "ft_ccld_qty": "0", "nccs_qty": "1",
        "sll_buy_dvsn_cd": "01",
    }
    row = normalize_us_order_status_row(raw)
    assert row["normalization_result"] == "quarantined"
    assert row["filter_reason"] == "invalid_kis_order_datetime"
    with pytest.raises(ValueError, match="domestic order date"):
        resolve_kis_us_order_datetime("20261012", "002821", "20261012")


def test_overnight_fill_timestamp_is_cross_checked_with_domestic_order_date():
    assert _combine_kis_date_time(
        "20261012", "002821", domestic_order_date="20261013",
    ) == "2026-10-13T00:28:21+09:00"
