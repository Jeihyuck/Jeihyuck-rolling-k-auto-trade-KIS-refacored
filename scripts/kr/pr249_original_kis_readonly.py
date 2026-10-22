"""PR249 original KIS 2026-10-21/22 order evidence read-only audit.

Never executes orders/cancels, modifies Supabase, or prints broker credentials.
This probe cannot by itself backfill historical economic fills or prove current
position ownership. It reports only complete, date-scoped KIS cumulative
execution evidence, including partial SELL.
"""
from __future__ import annotations

import argparse
import hashlib
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
import json
import os
from urllib.parse import urlparse
from typing import Any

# Grounded in 2026-10-22 read-only Supabase original order records. They are
# expected order identities, NOT proof of quantities or execution amounts.
EXPECTED_ACCOUNT_DIGEST_PREFIX = "fef5d08b17e8"  # SHA256 prefix of durable portfolio_epochs.account_id

TARGETS: dict[date, dict[tuple[str, str, str], int]] = {
    date(2026, 10, 21): {
        ("251970", "BUY", "0000013196"): 17,
        ("251970", "SELL", "0000040226"): 17,
        ("086450", "BUY", "0000013221"): 48,
        ("086450", "SELL", "0000040213"): 48,
    },
    date(2026, 10, 22): {
        ("053800", "BUY", "0000013264"): 12,
        ("053800", "SELL", "0000014123"): 12,
        ("144960", "BUY", "0000013218"): 81,
        ("144960", "SELL", "0000040070"): 81,
        ("253590", "BUY", "0000014280"): 79,
        ("253590", "SELL", "0000040083"): 79,
    },
}


def _v(row: dict, *names: str) -> str:
    for key in names:
        val = row.get(key)
        if val not in (None, ""):
            return str(val).strip()
    return ""


def _decimal(raw: str) -> Decimal | None:
    try:
        value = Decimal(raw)
        return value if value.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def classify_original_kis_rows(
    response: dict[str, Any], trade_date: date,
    targets: dict[tuple[str, str, str], int],
) -> dict[str, Any]:
    """Reject partial pagination, missing official quantities and ambiguity."""
    if not isinstance(response, dict) or (
        str(response.get("rt_cd") or "") != "0"
        or response.get("_sep29_complete") is not True
        or response.get("_diag_stub")
        or response.get("_ccld_status")
    ):
        raise ValueError("PR249_KIS_ORIGINAL_COMPLETE_PAGES_NOT_PROVEN")
    rows = response.get("output1")
    if not isinstance(rows, list):
        raise ValueError("PR249_KIS_INVALID_OUTPUT1")
    scoped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for original in rows:
        if not isinstance(original, dict):
            raise ValueError("PR249_KIS_MALFORMED_ORDER_ROW")
        code = _v(original, "pdno", "stck_shrn_iscd", "code").removeprefix("A").zfill(6)
        raw_side = _v(original, "sll_buy_dvsn_cd", "side").upper()
        side = ("SELL" if raw_side in {"01", "1", "SELL", "S"} else
                "BUY" if raw_side in {"02", "2", "BUY", "B"} else "")
        odno = _v(original, "odno", "ODNO", "ordno").zfill(10)
        day = _v(original, "ord_dt", "ord_date", "ccld_dt").replace("-", "")
        if (code, side, odno) not in targets:
            continue
        if day != trade_date.strftime("%Y%m%d"):
            raise ValueError(f"PR249_KIS_DATE_MISMATCH:{code}:{odno[-4:]}")
        scoped[(code, side, odno)].append(original)
    report = []
    for (code, side, odno), requested_qty in targets.items():
        observations = scoped.get((code, side, odno), [])
        valid = []
        for row in observations:
            # Confirm order size too; matching a recycled ODNO from another
            # account or a modified order must never qualify by execution qty.
            requested_original = _decimal(_v(row, "ord_qty", "ord_qty_sum"))
            if requested_original is None or requested_original != requested_qty:
                continue
            quantity = _decimal(_v(row, "tot_ccld_qty"))
            amount = _decimal(_v(row, "tot_ccld_amt"))
            avg = _decimal(_v(row, "avg_prvs"))
            if quantity is None or quantity != quantity.to_integral_value():
                continue
            qty = int(quantity)
            if qty == 0 and amount in (None, Decimal(0)):
                valid.append((qty, Decimal(0)))
                continue
            if not (0 < qty <= requested_qty and amount and amount > 0 and avg and avg > 0):
                continue
            if abs(amount / qty - avg) > max(Decimal("1"), avg * Decimal("0.001")):
                continue
            valid.append((qty, amount))
        if not observations or not valid:
            status, filled, krw = "UNVERIFIED_OR_MISSING", None, None
        else:
            highest = max(q for q, _ in valid)
            amounts = {n for q, n in valid if q == highest}
            if len(amounts) != 1:
                status, filled, krw = "CONFLICTING_SAME_QTY_NOTIONAL", None, None
            else:
                filled, amount = highest, amounts.pop()
                krw = str(amount)
                status = ("FULL_VERIFIED" if filled == requested_qty else
                          "PARTIAL_VERIFIED" if filled > 0 else "ZERO_CONFIRMED")
        report.append({
            "trade_date": trade_date.isoformat(), "code": code, "side": side,
            "kis_odno_suffix": odno[-4:], "requested_qty": requested_qty,
            "broker_verified_qty": filled, "broker_verified_krw": krw,
            "status": status, "source_rows": len(observations),
        })
    return {
        "trade_date": trade_date.isoformat(), "complete_kis_pages": True,
        "targets": len(report), "verified": sum(
            item["status"] in {"FULL_VERIFIED", "PARTIAL_VERIFIED", "ZERO_CONFIRMED"}
            for item in report
        ),
        "results": report,
    }


def _is_practice_kis_base_url(url: str) -> bool:
    """Check exact HTTPS authority, not untrusted substring or unused env."""
    try:
        parsed = urlparse(str(url or ""))
        return (parsed.scheme == "https"
                and parsed.hostname == "openapivts.koreainvestment.com"
                and parsed.port == 29443
                and parsed.username is None and parsed.password is None)
    except ValueError:
        return False


def run_practice_readonly() -> list[dict[str, Any]]:
    # The actual KisAPI constructs HTTP URLs from settings.API_BASE_URL.
    # KIS_REST_URL is an optional configuration override, not the source of
    # truth; requiring it would prevent GET-only evidence in normal WSL setups.
    if (os.getenv("KIS_ENV") != "practice" or
        os.getenv("STRATEGY_ENV") != "practice"):
        raise RuntimeError("PR249_PRACTICE_READONLY_ENV_REQUIRED")
    from settings import API_BASE_URL, KIS_ENV as EFFECTIVE_KIS_ENV
    optional_url = (os.getenv("KIS_REST_URL") or "").strip()
    if (EFFECTIVE_KIS_ENV != "practice" or
        not _is_practice_kis_base_url(API_BASE_URL) or
        (optional_url and not _is_practice_kis_base_url(optional_url))):
        raise RuntimeError("PR249_PRACTICE_READONLY_URL_MISMATCH")
    for required in ("KIS_APP_KEY", "KIS_APP_SECRET", "CANO", "ACNT_PRDT_CD"):
        if not os.getenv(required):
            raise RuntimeError("PR249_PRACTICE_READONLY_CREDENTIAL_UNAVAILABLE")
    from trader.kis_wrapper import KisAPI
    from trader.account_state import get_account_key
    from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
    kis = KisAPI(kis_env="practice")
    if getattr(kis, "env", None) != "practice":
        raise RuntimeError("PR249_KIS_CLIENT_ENV_MISMATCH")
    scope_digest = hashlib.sha256(
        get_account_key(env="practice", kis=kis).encode("utf-8")
    ).hexdigest()
    if not scope_digest.startswith(EXPECTED_ACCOUNT_DIGEST_PREFIX):
        raise RuntimeError("PR249_ORIGINAL_KIS_ACCOUNT_SCOPE_MISMATCH")
    # Read-only all-page historical inquiries. Absolutely no order or cancel.
    return [
        classify_original_kis_rows(
            _daily_ccld_all_pages_for_date(kis, trading_date), trading_date, targets,
        )
        for trading_date, targets in sorted(TARGETS.items())
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline-original-json", help="Complete KIS JSON array for date1/date2; offline analysis only")
    args = parser.parse_args()
    if args.offline_original_json:
        with open(args.offline_original_json, encoding="utf-8") as f:
            originals = json.load(f)
        if not isinstance(originals, list) or len(originals) != len(TARGETS):
            raise SystemExit("PR249_OFFLINE_KIS_PAIR_REQUIRED")
        reports = [
            classify_original_kis_rows(response, day, target)
            for response, (day, target) in zip(originals, sorted(TARGETS.items()))
        ]
        provenance = "OFFLINE_INPUT_UNAUTHENTICATED"
    else:
        reports = run_practice_readonly()
        provenance = "KIS_PRACTICE_READONLY_NETWORK"
    output = {"status": "READONLY_EVIDENCE_ONLY_NO_DB_MUTATIONS",
              "source": provenance, "reports": reports}
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if all(r["verified"] == r["targets"] for r in reports) else 2


if __name__ == "__main__":
    raise SystemExit(main())
