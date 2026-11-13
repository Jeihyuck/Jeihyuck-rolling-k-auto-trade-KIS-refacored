"""Read-only practice KIS source comparison for one KR PB1 symbol.

Usage: NO_TRADE=1 KIS_ENV=practice python -m scripts.kr_quote_source_probe --code 219130

This compares a normal WS-first quote with an independently fetched REST
inquire-price original response. It never submits orders or modifies DB.
No broker account IDs, credentials, raw headers or tokens are printed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def public_quote_evidence(quote: dict, *, requested_code: str, origin: str) -> dict:
    if not isinstance(quote, dict):
        quote = {}
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
    # Preserve exactly which source contained fields. Do not fill native clock
    # from local receipt time, from a different source, or from another date.
    def source_field(key):
        if key in quote:
            return quote[key], "quote"
        if key in raw:
            return raw[key], "raw"
        return None, "MISSING"
    day, day_src = source_field("stck_bsop_date")
    clock, clock_src = source_field("stck_cntg_hour")
    cumulative, vol_src = source_field("acml_vol")
    weighted, vw_src = source_field("wghn_avrg_stck_prc")
    return {
        "origin": origin, "code": requested_code,
        "market_data_source": str(quote.get("source") or raw.get("source") or "UNDECLARED"),
        "quote_code": str(quote.get("symbol") or quote.get("code") or ""),
        "native_trade_date": str(day) if day is not None else None,
        "native_exchange_clock": str(clock) if clock is not None else None,
        "quote_date_field_source": day_src, "quote_clock_field_source": clock_src,
        "cumulative_volume_field_source": vol_src,
        "native_weighted_average_field_source": vw_src,
        "cumulative_volume_present": cumulative not in (None, "", 0, "0"),
        "native_vwap_present": weighted not in (None, "", 0, "0"),
        "do_not_infer_exchange_time_from_received_at": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", required=True, help="6-digit KR symbol only")
    args = parser.parse_args()
    code = args.code.strip()
    if not (len(code) == 6 and code.isascii() and code.isdecimal()):
        parser.error("code must be exactly six decimal digits")
    if str(os.getenv("KIS_ENV") or "practice").strip().lower() != "practice":
        parser.error("only KIS practice account is allowed")
    # Explicitly disable every known order permission before module import.
    for key in ("LIVE_TRADING_ENABLED", "KR_LIVE_TRADING_ENABLED", "KR_ORDER_ARMED"):
        os.environ[key] = "0"
    os.environ["NO_TRADE"] = "1"
    from trader.kis_wrapper import KisAPI

    kis = KisAPI(kis_env="practice")
    if str(kis.env).lower() != "practice":
        raise RuntimeError("READONLY_PROBE_REQUIRES_PRACTICE_ACCOUNT")
    ws_first = kis.get_price_snapshot(code, market="J")
    fresh_rest = kis.get_kr_native_quote_readonly(code)
    result = {
        "trade_date_kst": datetime.now(KST).date().isoformat(),
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "broker_order_calls": 0,
        "probes": [
            public_quote_evidence(ws_first, requested_code=code, origin="WS_FIRST_OR_CACHE"),
            public_quote_evidence(fresh_rest, requested_code=code, origin="INDEPENDENT_NATIVE_REST_GET"),
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
