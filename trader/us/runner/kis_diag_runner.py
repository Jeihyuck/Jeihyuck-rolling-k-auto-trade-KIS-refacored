# -*- coding: utf-8 -*-
"""KIS US API Diagnostic Runner.

KIS 해외주식 모의투자 API 연결 상태 및 주요 엔드포인트를 순서대로 점검한다.
read-only 조회만 수행하며 주문 API는 호출하지 않는다.

진단 항목:
  1. price         — 현재가 조회
  2. daily         — 일봉 데이터 조회
  3. orderable_cash — 주문 가능 현금 (ITEM_CD + OVRS_ORD_UNPR 포함)
  4. balance       — 계좌 잔고 조회
  5. fills         — 당일 체결 내역 조회 (CCLD_NCCS_DVSN 포함)

CLI:
  python -m trader.us.runner.kis_diag_runner \\
    --env practice \\
    [--offline] \\
    [--symbol AAPL] \\
    [--exchange NASDAQ] \\
    [--price 100.0]
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

logger = logging.getLogger(__name__)

_OK = "OK"
_FAIL = "FAIL"
_SKIP = "SKIP"


def _diag_price(client: Any, symbol: str, exchange: str) -> dict:
    tag = "[US_KIS_DIAG][PRICE]"
    try:
        result = client.get_us_price(symbol, exchange)
        output = result.get("output", {})
        last = output.get("last", "n/a")
        logger.info("%s[%s] symbol=%s last=%s", tag, _OK, symbol, last)
        return {"item": "price", "status": _OK, "last": last}
    except Exception as exc:
        logger.error("%s[%s] %s", tag, _FAIL, exc)
        return {"item": "price", "status": _FAIL, "error": str(exc)}


def _diag_daily(client: Any, symbol: str, exchange: str) -> dict:
    tag = "[US_KIS_DIAG][DAILY]"
    try:
        rows = client.get_us_daily_price(symbol, exchange, count=5)
        logger.info("%s[%s] symbol=%s rows=%d", tag, _OK, symbol, len(rows))
        return {"item": "daily", "status": _OK, "rows": len(rows)}
    except Exception as exc:
        logger.error("%s[%s] %s", tag, _FAIL, exc)
        return {"item": "daily", "status": _FAIL, "error": str(exc)}


def _diag_orderable_cash(client: Any, symbol: str, exchange: str, price: float) -> dict:
    tag = "[US_KIS_DIAG][ORDERABLE_CASH]"
    try:
        result = client.get_us_orderable_cash(symbol=symbol, exchange=exchange, price=price)
        output = result.get("output", result)
        candidates = (
            "frcr_ord_psbl_amt1", "ord_psbl_cash", "ovrs_ord_psbl_amt",
            "orderable_cash", "cash", "psbl_amt",
        )
        cash_val = None
        for key in candidates:
            v = output.get(key)
            if v is not None:
                try:
                    cash_val = float(v)
                    break
                except (ValueError, TypeError):
                    pass
        logger.info(
            "%s[%s] symbol=%s exchange=%s price=%.2f cash=%s",
            tag, _OK, symbol, exchange, price, cash_val,
        )
        return {"item": "orderable_cash", "status": _OK, "cash_usd": cash_val}
    except Exception as exc:
        logger.error("%s[%s] %s", tag, _FAIL, exc)
        return {"item": "orderable_cash", "status": _FAIL, "error": str(exc)}


def _diag_balance(client: Any) -> dict:
    tag = "[US_KIS_DIAG][BALANCE]"
    try:
        result = client.get_us_balance()
        output2 = result.get("output2", {})
        pvs = output2.get("tot_evlu_pfls_amt", "n/a") if isinstance(output2, dict) else "n/a"
        logger.info("%s[%s] total_eval=%s", tag, _OK, pvs)
        return {"item": "balance", "status": _OK, "total_eval": pvs}
    except Exception as exc:
        logger.error("%s[%s] %s", tag, _FAIL, exc)
        return {"item": "balance", "status": _FAIL, "error": str(exc)}


def _diag_fills(client: Any) -> dict:
    tag = "[US_KIS_DIAG][FILLS]"
    try:
        fills = client.get_us_fills_today()
        logger.info("%s[%s] fills=%d", tag, _OK, len(fills))
        return {"item": "fills", "status": _OK, "count": len(fills)}
    except Exception as exc:
        logger.error("%s[%s] %s", tag, _FAIL, exc)
        return {"item": "fills", "status": _FAIL, "error": str(exc)}


def run_kis_diag(
    env: str = "practice",
    offline: bool = False,
    symbol: str = "AAPL",
    exchange: str = "NASDAQ",
    price: float = 100.0,
) -> dict:
    """KIS US API 진단 실행.

    Returns:
        {
            "status": "OK" | "PARTIAL" | "FAIL",
            "results": [{"item": ..., "status": "OK"/"FAIL"/"SKIP", ...}, ...]
        }
    """
    logger.info(
        "[US_KIS_DIAG][START] env=%s offline=%s symbol=%s exchange=%s price=%.2f",
        env, offline, symbol, exchange, price,
    )

    if offline:
        logger.warning("[US_KIS_DIAG][SKIP] offline=True — KIS HTTP calls blocked")
        return {
            "status": _SKIP,
            "results": [
                {"item": item, "status": _SKIP, "reason": "offline"}
                for item in ("price", "daily", "orderable_cash", "balance", "fills")
            ],
        }

    from trader.us.execution.kis_us_client import KisUSClient
    client = KisUSClient(env=env, offline=False)

    results = [
        _diag_price(client, symbol, exchange),
        _diag_daily(client, symbol, exchange),
        _diag_orderable_cash(client, symbol, exchange, price),
        _diag_balance(client),
        _diag_fills(client),
    ]

    total = len(results)
    ok_cnt = sum(1 for r in results if r["status"] == _OK)
    fail_cnt = sum(1 for r in results if r["status"] == _FAIL)

    if fail_cnt == 0:
        overall = _OK
    elif ok_cnt == 0:
        overall = _FAIL
    else:
        overall = "PARTIAL"

    logger.info(
        "[US_KIS_DIAG][DONE] status=%s ok=%d fail=%d total=%d",
        overall, ok_cnt, fail_cnt, total,
    )
    return {"status": overall, "results": results}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="KIS US API Diagnostic Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--exchange", default="NASDAQ")
    parser.add_argument("--price", type=float, default=100.0)
    args = parser.parse_args()

    result = run_kis_diag(
        env=args.env,
        offline=args.offline,
        symbol=args.symbol,
        exchange=args.exchange,
        price=args.price,
    )

    if result["status"] == _FAIL:
        sys.exit(1)


if __name__ == "__main__":
    main()
