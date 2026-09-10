from __future__ import annotations

import math


def test_kis_percent_points_are_converted_by_contract() -> None:
    from trader.accounting import kis_percent_points_to_fraction

    assert math.isclose(kis_percent_points_to_fraction(-0.32), -0.0032)
    assert math.isclose(kis_percent_points_to_fraction(3.82), 0.0382)
    assert math.isclose(kis_percent_points_to_fraction(0.87), 0.0087)


def test_us_holdings_market_value_never_becomes_account_equity(monkeypatch) -> None:
    from trader.accounting import resolve_us_accounting
    from trader.us.capital_deployment import compute_deployment_metrics

    monkeypatch.setenv("US_EXPECTED_PRACTICE_CAPITAL_KRW", "300000000")
    monkeypatch.setenv("US_PAPER_MAX_CAPITAL_KRW", "300000000")
    monkeypatch.setenv("US_BUDGET_FX_KRW_PER_USD", "1450")
    monkeypatch.delenv("US_ACCOUNT_EQUITY_USD", raising=False)

    accounting = resolve_us_accounting(
        reconcile={
            "total_pvs": "17377.87",
            "total_pvs_source": "positions_market_value_sum",
            "total_pvs_semantics": "holdings_market_value_usd",
            "holdings_market_value_usd": 17377.87,
            "account_equity_usd": None,
            "account_equity_source": "unavailable_from_current_kis_balance_contract",
        },
        invested_market_value_usd=17377.87,
        broker_orderable_cash_usd=92018.63,
    )
    assert accounting["account_equity_usd"] is None
    assert accounting["total_pvs_eligible_as_account_equity"] is False
    assert math.isclose(accounting["risk_capital_usd"], 300_000_000 / 1450)

    metrics = compute_deployment_metrics(
        account_equity_usd=accounting["account_equity_usd"],
        risk_capital_usd=accounting["risk_capital_usd"],
        risk_capital_source=accounting["risk_capital_source"],
        invested_market_value_usd=17377.87,
        cash_usd=92018.63,
    )
    assert metrics["gross_exposure_pct"] < 0.10
    assert not math.isclose(metrics["gross_exposure_pct"], 1.0)


def test_us_balance_normalizer_labels_total_pvs_as_holdings_only() -> None:
    from trader.us.data_provider import normalize_us_balance

    raw = {
        "rt_cd": "0",
        "output1": [{
            "ovrs_pdno": "AAPL",
            "ovrs_cblc_qty": "10",
            "ord_psbl_qty": "10",
            "pchs_avg_pric": "100",
            "ovrs_stck_evlu_amt": "1100",
            "frcr_pchs_amt1": "1000",
            "frcr_evlu_pfls_amt": "100",
            "evlu_pfls_rt": "10.0",
        }],
        "output2": {"ovrs_tot_pfls": "100"},
    }
    out = normalize_us_balance(raw)
    assert out["total_pvs"] == "1100.0"
    assert out["total_pvs_source"] == "positions_market_value_sum"
    assert out["total_pvs_semantics"] == "holdings_market_value_usd"
    assert out["holdings_market_value_usd"] == 1100.0
    assert out["account_equity_usd"] is None


def test_us_kis_position_pnl_rate_uses_percent_points() -> None:
    from trader.us.market_state_overlay import _pnl_pct

    assert math.isclose(_pnl_pct({"pnl_rate": 0.87}), 0.0087)
    assert math.isclose(_pnl_pct({"evlu_pfls_rt": -0.32}), -0.0032)
    assert math.isclose(_pnl_pct({"unrealized_pnl_pct": 0.03}), 0.03)


def test_legacy_us_pnl_balance_missing_cash_is_unknown_not_zero() -> None:
    from trader.us.runner.pnl_report_runner import fetch_kis_balance

    class Client:
        def get_us_balance(self):
            return {"output1": [], "output2": {"ovrs_tot_pfls": "12.34"}}

    result = fetch_kis_balance(Client(), "practice")
    assert result["status"] == "OK"
    assert result["cash_usd"] is None
    assert result["cash_source"] == "unavailable"


def test_us_risk_capital_stable_with_or_without_reconcile(monkeypatch) -> None:
    from trader.accounting import resolve_us_accounting

    monkeypatch.setenv("US_EXPECTED_PRACTICE_CAPITAL_KRW", "300000000")
    monkeypatch.setenv("US_BUDGET_FX_KRW_PER_USD", "1450")
    monkeypatch.delenv("US_ACCOUNT_EQUITY_USD", raising=False)

    with_reconcile = resolve_us_accounting(
        reconcile={"total_pvs": "17377.87", "total_pvs_source": "positions_market_value_sum"},
        invested_market_value_usd=17377.87,
        broker_orderable_cash_usd=92018.63,
    )
    without_reconcile = resolve_us_accounting(
        reconcile={},
        invested_market_value_usd=17377.87,
        broker_orderable_cash_usd=92018.63,
    )
    assert with_reconcile["risk_capital_usd"] == without_reconcile["risk_capital_usd"]
    assert with_reconcile["account_equity_usd"] is None
    assert without_reconcile["account_equity_usd"] is None


def test_us_daily_report_does_not_label_holdings_as_account_equity(monkeypatch, tmp_path) -> None:
    from trader.us.runner.daily_report_runner import run_daily_report

    monkeypatch.setenv("US_DAILY_REPORT_BASE", str(tmp_path / "daily"))
    monkeypatch.setenv("US_EXPECTED_PRACTICE_CAPITAL_KRW", "300000000")
    monkeypatch.setenv("US_PAPER_MAX_CAPITAL_KRW", "300000000")
    monkeypatch.setenv("US_BUDGET_FX_KRW_PER_USD", "1450")
    monkeypatch.delenv("US_ACCOUNT_EQUITY_USD", raising=False)

    result = run_daily_report(
        env="practice",
        session="close",
        trade_date="2026-09-08",
        offline=True,
        final_balance={
            "total_pvs": "13329.315",
            "total_pvs_source": "positions_market_value_sum",
            "total_pvs_semantics": "holdings_market_value_usd",
            "holdings_market_value_usd": 13329.315,
            "account_equity_usd": None,
            "account_equity_source": "unavailable_from_current_kis_balance_contract",
        },
        final_positions=[{
            "symbol": "AAPL",
            "qty": 1,
            "market_value_usd": 13329.315,
            "current_price_usd": 13329.315,
        }],
        kis_fills=[],
        close_order_classification={"orders": [], "counts": {}, "pending_order_count": 0},
    )
    report = result["report"]
    assert report["account_equity_usd"] is None
    assert report["account_equity_source"] == "unavailable_from_current_kis_balance_contract"
    assert report["holdings_market_value_usd"] == 13329.315
    assert "ACCOUNT_EQUITY_UNAVAILABLE_HOLDINGS_ONLY_BALANCE" in report["warnings"]
