"""Read-only broker original evidence classifier; no real KIS / DB writes."""
from datetime import date
from copy import deepcopy

import pytest
from scripts.kr.pr249_original_kis_readonly import (
    TARGETS, classify_original_kis_rows, run_practice_readonly,
)


OBSERVED = {
    ("251970", "BUY"): (17, 1162800),
    ("251970", "SELL"): (10, 685000),
    ("086450", "BUY"): (48, 1176000),
    ("086450", "SELL"): (48, 1180800),
    ("053800", "BUY"): (12, 1082400),
    ("053800", "SELL"): (12, 1084800),
    ("144960", "BUY"): (81, 1137680),
    ("144960", "SELL"): (81, 1091430),
    ("253590", "BUY"): (79, 1274270),
    ("253590", "SELL"): (52, 836180),
}


def _valid_rows(trade_day):
    rows = []
    for (code, side, no), _requested in TARGETS[trade_day].items():
        qty, amount = OBSERVED[(code, side)]
        rows.append({
            "pdno": code, "sll_buy_dvsn_cd": "01" if side == "SELL" else "02",
            "odno": no, "ord_dt": trade_day.strftime("%Y%m%d"),
            "tot_ccld_qty": str(qty), "tot_ccld_amt": str(amount),
            "avg_prvs": str(round(amount / qty, 4)),
        })
    return {"rt_cd": "0", "_sep29_complete": True, "output1": rows}


@pytest.mark.parametrize("trade_day", [date(2026, 10, 21), date(2026, 10, 22)])
def test_complete_original_evidence_preserves_partial_sell_and_actual_amount(trade_day):
    report = classify_original_kis_rows(_valid_rows(trade_day), trade_day, TARGETS[trade_day])
    assert report["verified"] == report["targets"]
    for item in report["results"]:
        qty, amount = OBSERVED[(item["code"], item["side"])]
        assert item["broker_verified_qty"] == qty
        assert item["broker_verified_krw"] == str(amount)
        assert item["status"] == (
            "PARTIAL_VERIFIED" if (item["code"], item["side"]) in {
                ("251970", "SELL"), ("253590", "SELL")
            } else "FULL_VERIFIED"
        )


def test_unmatched_day_broker_row_not_attributed_to_other_day():
    today = date(2026, 10, 22)
    data = _valid_rows(today)
    data["output1"][0]["ord_dt"] = "20261021"
    with pytest.raises(ValueError, match="DATE_MISMATCH"):
        classify_original_kis_rows(data, today, TARGETS[today])


def test_duplicate_broker_snapshot_equal_notional_does_not_double_count():
    day = date(2026, 10, 21)
    data = _valid_rows(day)
    sell = next(x for x in data["output1"] if x["pdno"] == "251970"
                and x["sll_buy_dvsn_cd"] == "01")
    data["output1"].append(deepcopy(sell))
    proof = classify_original_kis_rows(data, day, TARGETS[day])
    actual = next(x for x in proof["results"] if x["code"] == "251970" and x["side"] == "SELL")
    assert actual["broker_verified_qty"] == 10
    assert actual["source_rows"] == 2


def test_different_notional_same_kis_order_cumulative_is_blocked():
    day = date(2026, 10, 22)
    data = _valid_rows(day)
    sell = next(x for x in data["output1"] if x["pdno"] == "253590"
                and x["sll_buy_dvsn_cd"] == "01")
    contradicted = deepcopy(sell)
    contradicted["tot_ccld_amt"] = "845000"
    contradicted["avg_prvs"] = str(845000 / 52)
    data["output1"].append(contradicted)
    proof = classify_original_kis_rows(data, day, TARGETS[day])
    actual = next(x for x in proof["results"] if x["code"] == "253590" and x["side"] == "SELL")
    assert actual["status"] == "CONFLICTING_SAME_QTY_NOTIONAL"
    assert actual["broker_verified_qty"] is None


@pytest.mark.parametrize("problem", [{"rt_cd": "1"}, {"_sep29_complete": False},
                                        {"_diag_stub": True}, {"_ccld_status": "PARTIAL"}])
def test_incomplete_kis_pagination_and_stub_fail_closed(problem):
    day = date(2026, 10, 21)
    data = _valid_rows(day)
    data.update(problem)
    with pytest.raises(ValueError, match="COMPLETE_PAGES_NOT_PROVEN"):
        classify_original_kis_rows(data, day, TARGETS[day])


def test_live_environment_denied_before_any_network(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "live")
    monkeypatch.setenv("STRATEGY_ENV", "live")
    monkeypatch.setenv("KIS_REST_URL", "https://openapi.koreainvestment.com:9443")
    with pytest.raises(RuntimeError, match="PRACTICE_READONLY_ENV_REQUIRED"):
        run_practice_readonly()
