"""2026-10-20 KR practice SELL broker evidence probe (strictly read only).

It authenticates to the configured *practice* KIS API, queries all pages of
inquire-daily-ccld for exactly one trade date, and matches the eleven original
broker numbers by (trade date, symbol, SELL, KIS ODNO).

This module cannot submit/cancel orders and does NOT write Supabase records.
CI reports unresolved evidence as a verification failure, not as zero PnL.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
import os
from typing import Any

EXPECTED_OCT20_SELLS = {
    "036540": ("0000004318", 140),
    "039030": ("0000004475", 2),
    "042700": ("0000004613", 5),
    "067310": ("0000004725", 32),
    "006400": ("0000035346", 1),
    "011200": ("0000035358", 52),
    "012750": ("0000035370", 15),
    "028670": ("0000035380", 235),
    "053800": ("0000035400", 15),
    "096530": ("0000035410", 29),
    "425420": ("0000035418", 21),
}


def _val(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def evaluate_broker_sell_evidence(
    rows: list[dict[str, Any]], trade_date: date,
    expected: dict[str, tuple[str, int]] | None = None,
) -> dict[str, Any]:
    """Classify only actual priced SELL executions; ACKs don't prove fills."""
    expected = expected or EXPECTED_OCT20_SELLS
    results = []
    for code, (odno, requested_qty) in expected.items():
        matched = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            raw_code = _val(row, "pdno", "PDNO", "stck_shrn_iscd", "code").lstrip("A").zfill(6)
            broker_no = _val(row, "odno", "ODNO", "ordno").zfill(10)
            side = _val(row, "sll_buy_dvsn_cd", "sll_buy_dvsn", "side").upper()
            raw_day = _val(row, "ord_dt", "ord_date", "trd_dt", "ccld_dt").replace("-", "")
            if (raw_code == code and broker_no == odno and
                    side in {"01", "1", "SELL", "S"} and
                    raw_day == trade_date.strftime("%Y%m%d")):
                matched.append(row)
        observed_qty = 0
        priced_notional = 0.0
        observed_fill_ids: set[str] = set()
        for row in matched:
            # KIS can repeat cumulative filled quantity on several rows; do
            # not aggregate it as if every occurrence were another execution.
            trade_id = _val(row, "ccld_no", "trade_id", "exec_id")
            unit_qty = _val(row, "ccld_qty", "filled_qty")
            price = _val(row, "ccld_prc", "filled_price")
            if not trade_id or not unit_qty or not price:
                continue
            if trade_id in observed_fill_ids:
                continue
            try:
                qty_i = int(float(unit_qty))
                px_f = float(price)
            except (ValueError, TypeError):
                continue
            if qty_i <= 0 or px_f <= 0:
                continue
            observed_fill_ids.add(trade_id)
            observed_qty += qty_i
            priced_notional += qty_i * px_f
        verified = observed_qty == requested_qty and observed_qty > 0
        results.append({
            "code": code,
            "kis_odno_suffix": odno[-4:],
            "requested_qty": requested_qty,
            "priced_execution_qty": observed_qty,
            "verified": verified,
            "weighted_execution_price": (round(priced_notional / observed_qty, 4) if verified else None),
            "evidence": "KIS_ACTUAL_FILL" if verified else "UNRESOLVED_KIS_EXECUTION_PROOF",
        })
    return {
        "trade_date": trade_date.isoformat(),
        "orders_checked": len(results),
        "orders_verified": sum(bool(r["verified"]) for r in results),
        "orders_unresolved": sum(not r["verified"] for r in results),
        "results": results,
    }


def diagnose_empty_kr_ccld_readonly(client: Any, trade_date: date) -> list[dict[str, Any]]:
    """Compare safe GET inquiry filters without persisting or trusting partial pages.

    A variant with rows is diagnostic only; this does not promote broker fills.
    The result deliberately includes no account numbers, tokens, or raw rows.
    """
    from trader.kis_wrapper import _pick_tr
    tr_ids = _pick_tr("practice", "DAILY_CCLD")
    if not tr_ids:
        return [{"variant": "no_tr_id", "rows": 0, "status": "UNAVAILABLE"}]
    base = os.getenv("KIS_REST_URL", "").rstrip("/")
    if "openapivts.koreainvestment.com" not in base:
        return [{"variant": "not_practice", "rows": 0, "status": "BLOCKED"}]
    url = base + "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    day = trade_date.strftime("%Y%m%d")
    params = {
        "CANO": client.CANO, "ACNT_PRDT_CD": client.ACNT_PRDT_CD,
        "INQR_STRT_DT": day, "INQR_END_DT": day,
        "SLL_BUY_DVSN_CD": "00", "INQR_DVSN": "00",
        "PDNO": "", "CCLD_DVSN": "00", "INQR_DVSN_3": "00",
        "INQR_DVSN_1": "", "ORD_GNO_BRNO": "", "ODNO": "",
        "SORT_SQN": "00", "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
    }
    variants = [
        ("ascending", {"INQR_DVSN": "01"}),
        ("sell_only", {"SLL_BUY_DVSN_CD": "01"}),
        ("exact_order", {"SLL_BUY_DVSN_CD": "01", "PDNO": "036540", "ODNO": "0000004318"}),
    ]
    report = []
    for name, overrides in variants:
        try:
            response = client.session.request(
                "GET", url, params={**params, **overrides},
                headers=client._headers(tr_ids[0]), timeout=(3.0, 7.0),
            )
            data = response.json() if response.ok else {}
            rows = data.get("output1") if isinstance(data, dict) else []
            report.append({
                "variant": name,
                "http": response.status_code,
                "rt_cd": str(data.get("rt_cd") or "") if isinstance(data, dict) else "",
                "rows": len(rows) if isinstance(rows, list) else int(bool(rows)),
                "page_complete": str(response.headers.get("tr_cont") or "").upper() not in {"F", "M"},
            })
        except Exception as exc:
            report.append({"variant": name, "status": "QUERY_FAILED", "error_type": type(exc).__name__})
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only KIS practice SELL evidence proof")
    parser.add_argument("--trade-date", default="2026-10-20")
    args = parser.parse_args(argv)
    trade_date = date.fromisoformat(args.trade_date)
    if (
        os.getenv("KIS_ENV", "").lower() != "practice"
        or os.getenv("STRATEGY_ENV", "").lower() != "practice"
        or "openapivts.koreainvestment.com" not in os.getenv("KIS_REST_URL", "")
    ):
        raise SystemExit("PRACTICE_KIS_ENDPOINT_REQUIRED")
    if not all(os.getenv(k) for k in ("KIS_APP_KEY", "KIS_APP_SECRET", "CANO", "ACNT_PRDT_CD")):
        raise SystemExit("PRACTICE_KIS_CREDENTIALS_UNAVAILABLE")
    import time
    from trader.kis_wrapper import KisAPI, KisTokenRateLimitError
    from trader.account_state import get_account_key
    import hashlib
    # Only the non-reversible prefix of the Oct-20 durable execution claim
    # account digest is retained here; never print or expose account numbers.
    expected_claim_digest_prefix = "fef5d08b17"
    actual_claim_digest_prefix = hashlib.sha256(
        get_account_key(env="practice").encode("utf-8")
    ).hexdigest()[:len(expected_claim_digest_prefix)]
    if actual_claim_digest_prefix != expected_claim_digest_prefix:
        raise SystemExit("KIS_CREDENTIAL_ACCOUNT_SCOPE_MISMATCH_WITH_OCT20_DB")
    print("KIS_ACCOUNT_SCOPE_MATCH=1", flush=True)
    from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
    # This is strictly a GET-only diagnostic. KIS already waits up to 65 s
    # on EGW00133; allow one additional AUTH attempt after that cooldown.
    client = None
    for auth_attempt in range(2):
        try:
            client = KisAPI(kis_env="practice")
            break
        except KisTokenRateLimitError:
            if auth_attempt:
                raise
            print("KIS_TOKEN_LIMIT_RECOVERY: retrying read-only authorization after broker cooldown", flush=True)
            time.sleep(2)
    if client is None:
        raise SystemExit("KIS_READONLY_AUTH_UNAVAILABLE")
    response = _daily_ccld_all_pages_for_date(client, trade_date)
    if str(response.get("rt_cd") or "") != "0" or not response.get("_sep29_complete"):
        raise SystemExit("KIS_DAILY_CCLD_INCOMPLETE")
    rows = response.get("output1") or []
    result = evaluate_broker_sell_evidence(rows, trade_date)
    result["kis_response_complete"] = True
    result["kis_pages"] = int(response.get("_sep29_pages") or 0)
    result["kis_rows"] = len(rows)
    if not rows:
        result["empty_response_diagnostics"] = diagnose_empty_kr_ccld_readonly(client, trade_date)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["orders_unresolved"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
