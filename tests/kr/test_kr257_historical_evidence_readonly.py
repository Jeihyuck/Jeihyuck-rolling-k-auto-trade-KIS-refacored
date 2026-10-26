"""No-account-mixing and broker cumulative price evidence regression."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

from scripts.kr257_historical_kis_evidence_probe import (
    prove_exact_cumulative_economics, analyze_rows_for_account,
)

KST = ZoneInfo("Asia/Seoul")


def test_full_kis_cumulative_fill_economics():
    row = dict(pdno="012750", ord_dt="20261023", sll_buy_dvsn_cd="02",
               odno="0000012398", ord_qty="11",
               tot_ccld_qty="11", tot_ccld_amt="1062600",
               avg_prvs="96600")
    evidence = prove_exact_cumulative_economics(
        [row], trade_date=date(2026, 10, 23),
        code="012750", side="BUY", order_no="0000012398", requested_qty=11)
    assert evidence["verified"] and evidence["broker_cumulative_qty"] == 11
    for corrupt in [
        {**row, "tot_ccld_qty": "2"},
        {**row, "tot_ccld_amt": "0"},
        {**row, "avg_prvs": "98000"},
        {**row, "ord_dt": "20261026"},
        {**row, "sll_buy_dvsn_cd": "01"},
    ]:
        check = prove_exact_cumulative_economics(
            [corrupt], trade_date=date(2026, 10, 23),
            code="012750", side="BUY", order_no="0000012398", requested_qty=11)
        assert (not check["verified"]) or check["broker_cumulative_qty"] == 2
    assert not prove_exact_cumulative_economics(
        [row, row], trade_date=date(2026, 10, 23),
        code="012750", side="BUY", order_no="0000012398", requested_qty=11
    )["verified"]


def test_old_account_epoch_prevents_new_practice_account_evidence_reuse():
    target = dict(
        code="012750", side="BUY", qty=11, status="RECONCILED",
        kis_odno="0000012398", account_id="original-account",
        env="practice", epoch_env="practice",
        strategy="pb1_pullback_close", epoch_strategy="pb1_pullback_close",
        trading_epoch_id="old-epoch", epoch_trading_id="old-epoch",
        created_at=datetime(2026, 10, 23, 9, 35, tzinfo=KST),
    )
    copied = analyze_rows_for_account(
        [target], current_account_key="new-practice-account",
        broker_by_date={date(2026, 10, 23): [{
            "pdno": "012750", "ord_dt": "20261023",
            "sll_buy_dvsn_cd": "02", "odno": "0000012398",
            "ord_qty": "11", "tot_ccld_qty": "11",
            "tot_ccld_amt": "1062600", "avg_prvs": "96600",
        }]},
    )
    assert copied["unresolved"] == 1
    assert copied["results"][0]["reason"] == "ORIGINAL_ACCOUNT_OR_EPOCH_NOT_AUTHORIZED"
    assert not copied["database_modified"]


def test_old_account_own_trading_epoch_required():
    target = dict(
        code="036930", side="SELL", qty=4,
        created_at=datetime(2026, 10, 14, 14, 0, tzinfo=KST),
        kis_odno="0000043455", status="FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED",
        account_id="old", env="practice", epoch_env="practice",
        strategy="pb1_pullback_close", epoch_strategy="pb1_pullback_close",
        trading_epoch_id="old-epoch", epoch_trading_id="other-epoch",
    )
    result = analyze_rows_for_account(
        [target], current_account_key="old", broker_by_date={})
    assert result["unresolved"] == 1
    assert result["results"][0]["reason"] == "ORIGINAL_ACCOUNT_OR_EPOCH_NOT_AUTHORIZED"
