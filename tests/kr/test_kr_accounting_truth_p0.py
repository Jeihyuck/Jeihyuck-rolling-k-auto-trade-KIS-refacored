from __future__ import annotations

import math


def test_kis_percent_points_are_converted_by_contract() -> None:
    from trader.accounting import kis_percent_points_to_fraction

    assert math.isclose(kis_percent_points_to_fraction(-0.32), -0.0032)
    assert math.isclose(kis_percent_points_to_fraction(3.82), 0.0382)
    assert math.isclose(kis_percent_points_to_fraction(0.87), 0.0087)


def test_kr_accounting_uses_kis_total_asset_not_orderable_cash_plus_holdings(monkeypatch) -> None:
    from trader.accounting import resolve_kr_accounting
    from trader.kr.account_risk import evaluate_kr_account_risk

    monkeypatch.delenv("KR_ACCOUNT_INTRADAY_PNL_PCT", raising=False)
    monkeypatch.delenv("KR_ACCOUNT_5D_PNL_PCT", raising=False)

    snapshot = resolve_kr_accounting(
        summary={
            "tot_evlu_amt": "100005276",
            "nass_amt": "100005276",
            "dnca_tot_amt": "49050752",
            "ord_psbl_cash": "49050752",
            "asst_icdc_erng_rt": "-0.32",
        },
        invested_market_value_krw=47_552_435,
        orderable_cash_krw=49_050_752,
    )

    assert snapshot["portfolio_equity_krw"] == 100_005_276
    assert snapshot["portfolio_equity_source"] == "kis_balance:tot_evlu_amt"
    assert math.isclose(snapshot["gross_exposure_pct"], 47_552_435 / 100_005_276)
    assert math.isclose(snapshot["account_intraday_pnl_pct"], -0.0032)
    assert evaluate_kr_account_risk(snapshot)["account_loss_kill_switch_triggered"] is False

    crash = dict(snapshot)
    crash["account_intraday_pnl_pct"] = -0.018
    assert evaluate_kr_account_risk(crash)["account_loss_kill_switch_level"] == "KR_DEFENSE_CRASH"
