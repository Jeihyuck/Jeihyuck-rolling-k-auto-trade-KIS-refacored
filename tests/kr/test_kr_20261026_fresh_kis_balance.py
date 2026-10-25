"""2026-10-26 KIS balance aggregator regression.

KisAPI.inquire_balance_all returns normalized output1/output2 but drops
top-level rt_cd, even after a real page came back rt_cd=0.  A missing
rt_cd must not turn a fresh 100m/no-holdings response into a false fail.
No KIS HTTP, DB writes, orders, or token access in these tests.
"""
import pytest

from trader.kr.new_paper_epoch_preflight import (
    KrPracticeReadinessError,
    verify_fresh_kr_practice_balance,
)


def _fresh_snapshot(**changes):
    snapshot = {
        "output1": [],
        "output2": {
            "dnca_tot_amt": "100000000",
            "ord_psbl_cash": "0",  # expected outside open trading window
            "tot_evlu_amt": "100000000",
        },
        "ctx_area_fk100": "",
        "ctx_area_nk100": "",
        # rt_cd deliberately NOT included: the actual aggregator drops it
    }
    return {**snapshot, **changes}


def test_oct26_successful_broker_balance_without_aggregated_rt_cd():
    result = verify_fresh_kr_practice_balance(_fresh_snapshot(), source="api")
    assert result == {
        "kis_balance_status": "AUTHENTIC_PRACTICE_100M_CASH_NO_HOLDINGS",
        "kis_cash_krw": 100000000,
        "kis_holdings_count": 0,
    }


def test_raw_explicit_rt_cd_zero_may_be_present():
    assert verify_fresh_kr_practice_balance(
        _fresh_snapshot(rt_cd="0"), source="api",
    )["kis_cash_krw"] == 100000000


@pytest.mark.parametrize("source", ["wrapper_cache", "file_cache", "", None])
def test_unverified_source_never_authorizes_fresh_read(source):
    with pytest.raises(KrPracticeReadinessError, match="KIS_SOURCE_NOT_FRESH"):
        verify_fresh_kr_practice_balance(
            _fresh_snapshot(), source=source,
        )


@pytest.mark.parametrize("flag", ["_stub", "_diag_stub", "_kis_disabled"])
def test_http_disabled_and_diagnostic_stubs_always_block(flag):
    with pytest.raises(KrPracticeReadinessError, match="KIS_BALANCE_STUB"):
        verify_fresh_kr_practice_balance(_fresh_snapshot(**{flag: True}), source="api")


def test_http_disabled_marker_is_rejected():
    with pytest.raises(KrPracticeReadinessError, match="KIS_BALANCE_STUB"):
        verify_fresh_kr_practice_balance(
            _fresh_snapshot(_source="http_disabled_stub"), source="api",
        )


def test_explicit_nonzero_broker_code_blocks():
    with pytest.raises(KrPracticeReadinessError, match="KIS_BROKER_ERROR"):
        verify_fresh_kr_practice_balance(
            _fresh_snapshot(rt_cd="1"), source="api",
        )


@pytest.mark.parametrize("snapshot", [
    None,
    {},
    {"output1": [], "output2": {}},
    {"output1": None, "output2": {"dnca_tot_amt": "100000000"}},
    {"output1": [], "output2": {"ord_psbl_cash": "100000000"}},
    {"output1": [], "output2": {"dnca_tot_amt": "not-a-number"}},
])
def test_missing_or_invalid_broker_evidence_is_not_accepted(snapshot):
    with pytest.raises(KrPracticeReadinessError):
        verify_fresh_kr_practice_balance(snapshot, source="api")


@pytest.mark.parametrize("cash", ["0", "50000000", "98000000", "-1000"])
def test_insufficient_account_cash_blocks(cash):
    with pytest.raises(KrPracticeReadinessError):
        verify_fresh_kr_practice_balance(
            _fresh_snapshot(output2={"dnca_tot_amt": cash}), source="api",
        )


@pytest.mark.parametrize("holdings", [
    [{"pdno": "005930", "hldg_qty": "2"}],
    [{"pdno": "005930", "hldg_qty": "NaN"}],
    [{"pdno": "005930", "hldg_qty": "bad"}],
])
def test_nonempty_or_unknown_holdings_fail_closed(holdings):
    with pytest.raises(KrPracticeReadinessError):
        verify_fresh_kr_practice_balance(
            _fresh_snapshot(output1=holdings), source="api",
        )
