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
        # The official KIS VTTC0081R output is an ORDER summary, not an
        # individual execution: it exposes tot_ccld_qty, tot_ccld_amt and
        # avg_prvs but no ccld_no/ccld_qty/ccld_prc. Confirm economic truth
        # from the broker's full cumulative quantity AND two independent
        # monetary fields, never from an order's intended limit price.
        broker_cumulative_reported = 0
        broker_aggregate_proven = False
        if not observed_fill_ids:
            aggregate: list[tuple[int, float, float]] = []
            for row in matched:
                try:
                    cum = int(float(_val(row, "tot_ccld_qty", "cumulative_filled_qty") or 0))
                    amount = float(_val(row, "tot_ccld_amt", "confirmed_fill_notional") or 0)
                    avg = float(_val(row, "avg_prvs") or 0)
                except (ValueError, TypeError):
                    continue
                broker_cumulative_reported = max(broker_cumulative_reported, cum)
                if not (0 < cum <= requested_qty and amount > 0 and avg > 0):
                    continue
                # KIS average price can be rounded. 0.1% max tolerance
                # accommodates price rounding, not arbitrary stale quotes.
                if abs((amount / cum) - avg) > max(1.0, avg * 0.001):
                    continue
                aggregate.append((cum, amount, avg))
            if aggregate:
                highest_cum = max(item[0] for item in aggregate)
                full_rows = [item for item in aggregate if item[0] == highest_cum]
                if all(abs(item[1] - full_rows[0][1]) <= max(1.0, item[1] * 0.001) for item in full_rows):
                    observed_qty = highest_cum
                    priced_notional = full_rows[0][1]
                    broker_aggregate_proven = True
        verified = observed_qty == requested_qty and observed_qty > 0
        results.append({
            "code": code,
            "kis_odno_suffix": odno[-4:],
            "requested_qty": requested_qty,
            "broker_cumulative_qty_reported": broker_cumulative_reported,
            "priced_execution_qty": observed_qty,
            "verified": verified,
            "weighted_execution_price": (round(priced_notional / observed_qty, 4) if observed_qty > 0 else None),
            "evidence": ("KIS_CUMULATIVE_QTY_AMOUNT_AVG_PROVEN" if broker_aggregate_proven
                         else "KIS_ACTUAL_FILL" if verified
                         else "UNRESOLVED_KIS_EXECUTION_PROOF"),
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
    # KIS official 2025+ sample uses VTTC0081R for practice recent
    # executions while this repository currently defaults to VTTC8001R.
    # Diagnose the mismatch before changing any production KIS behavior.
    variants = [
        ("official_recent_krx", {"EXCG_ID_DVSN_CD": "KRX"}, "VTTC0081R"),
        ("official_recent_all", {"EXCG_ID_DVSN_CD": "ALL"}, "VTTC0081R"),
        ("legacy_ascending", {"INQR_DVSN": "01"}, tr_ids[0]),
        ("legacy_exact", {"SLL_BUY_DVSN_CD": "01", "PDNO": "036540", "ODNO": "0000004318"}, tr_ids[0]),
    ]
    report = []
    import time
    for name, overrides, tr_id in variants:
        time.sleep(0.40)
        try:
            response = client.session.request(
                "GET", url, params={**params, **overrides},
                headers=client._headers(tr_id), timeout=(3.0, 7.0),
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
    # Strictly read-only KIS schema/identity profiling. No account numbers,
    # token strings, personal data or unmasked order IDs are emitted.
    expected_by_od = {odno: code for code, (odno, _qty) in EXPECTED_OCT20_SELLS.items()}
    row_profile = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        code = _val(raw, "pdno", "PDNO", "stck_shrn_iscd", "code").lstrip("A").zfill(6)
        order_no = _val(raw, "odno", "ODNO", "ordno").zfill(10)
        row_profile.append({
            "expected_code": code if code in EXPECTED_OCT20_SELLS else None,
            "exact_broker_order_no_match": bool(order_no in expected_by_od and expected_by_od[order_no] == code),
            "day_source": _val(raw, "ord_dt", "ord_date", "trd_dt", "ccld_dt"),
            "side_source": _val(raw, "sll_buy_dvsn_cd", "sll_buy_dvsn", "side"),
            "has_execution_id": bool(_val(raw, "ccld_no", "trade_id", "exec_id")),
            "has_execution_qty": bool(_val(raw, "ccld_qty", "filled_qty")),
            "has_execution_price": bool(_val(raw, "ccld_prc", "filled_price")),
            "has_cumulative_qty": bool(_val(raw, "tot_ccld_qty", "cumulative_filled_qty")),
            "has_broker_avg_price": bool(_val(raw, "avg_prvs")),
            "broker_cumulative_qty": int(float(_val(raw, "tot_ccld_qty") or 0)),
            "broker_cumulative_notional_positive": float(_val(raw, "tot_ccld_amt") or 0) > 0,
            "broker_average_positive": float(_val(raw, "avg_prvs") or 0) > 0,
            "requested_qty_matches_broker_cumulative": (
                bool(order_no in expected_by_od and expected_by_od[order_no] == code)
                and int(float(_val(raw, "tot_ccld_qty") or 0))
                == EXPECTED_OCT20_SELLS[code][1]
            ) if code in EXPECTED_OCT20_SELLS else False,
        })
    result["broker_row_schema"] = sorted(str(k) for k in rows[0].keys()) if rows and isinstance(rows[0], dict) else []
    result["identity_profile"] = row_profile
    result["kis_response_complete"] = True
    result["kis_pages"] = int(response.get("_sep29_pages") or 0)
    result["kis_rows"] = len(rows)
    if not rows:
        result["empty_response_diagnostics"] = diagnose_empty_kr_ccld_readonly(client, trade_date)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["orders_unresolved"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
