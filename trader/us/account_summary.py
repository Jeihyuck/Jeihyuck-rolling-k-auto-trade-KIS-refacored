"""KIS practice present-balance evidence; reporting only, never trading authority.

Official endpoint: https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/overseas_stock/inquire_present_balance
Practice output3 is supported; output2 currency-specific cash may be absent.
Do not derive net P&L, USD NAV or USD cash from mixed-currency summary fields.
"""
from datetime import datetime, timezone
from trader.accounting import safe_float


def parse_practice_present_balance(raw: dict) -> dict:
    unavailable = {"status": "UNAVAILABLE", "source": "KIS_VTRP6504R",
                   "reporting_only": True, "reason": "invalid_or_incomplete_summary"}
    if not isinstance(raw, dict) or str(raw.get("rt_cd")) != "0":
        return unavailable
    response_meta = raw.get("_response_meta") or {}
    if not isinstance(response_meta, dict):
        return unavailable
    cont = str(response_meta.get("tr_cont") or "").strip().upper()
    if cont not in {"", "D", "E"}:
        return {**unavailable, "reason": "incomplete_or_unknown_pagination"}
    summary = raw.get("output3")
    if isinstance(summary, list):
        summary = summary[0] if len(summary) == 1 else None
    if not isinstance(summary, dict):
        return unavailable
    assets = safe_float(summary.get("tot_asst_amt"))
    cash = safe_float(summary.get("tot_dncl_amt"))
    pnl = safe_float(summary.get("tot_evlu_pfls_amt"))
    if assets is None or assets <= 0:
        return {**unavailable, "reason": "positive_total_assets_missing"}
    # Currency rows may be absent on practice. Expose a USD-specific
    # broker observation only for one explicit USD row, never sum copies,
    # convert mixed KRW totals, or equate settled deposits with account NAV.
    currency_rows = raw.get("output2")
    if isinstance(currency_rows, dict):
        currency_rows = [currency_rows]
    usd_rows = [row for row in currency_rows or []
                if isinstance(row, dict) and str(row.get("crcy_cd") or "").upper() == "USD"] if isinstance(currency_rows, list) else []
    usd_deposits = safe_float(usd_rows[0].get("frcr_dncl_amt_2")) if len(usd_rows) == 1 else None
    usd_withdrawable = safe_float(usd_rows[0].get("frcr_drwg_psbl_amt_1")) if len(usd_rows) == 1 else None
    return {
        "status": "OBSERVED_REPORT_ONLY", "source": "KIS_VTRP6504R",
        "reporting_only": True, "requested_country": "840", "requested_basis": "KRW",
        "received_at_utc": datetime.now(timezone.utc).isoformat(),
        "broker_reported_total_assets_krw": assets,
        "broker_reported_total_deposits_krw": cash,
        "broker_reported_valuation_pnl_krw": pnl,
        "broker_reported_usd_deposits": usd_deposits,
        "broker_reported_usd_withdrawable": usd_withdrawable,
        "usd_currency_observation_status": "OBSERVED_REPORT_ONLY" if usd_deposits is not None else "UNAVAILABLE",
        "account_equity_usd": None, "cash_usd": None,
        "net_epoch_pnl_usd": None,
        "reason": "practice_summary_scope_and_settlement_lag_not_verified_for_trading",
    }
