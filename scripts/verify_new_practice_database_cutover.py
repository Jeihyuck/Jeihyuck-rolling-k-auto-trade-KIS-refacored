from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from trader.account_state import get_masked_account_key
from trader.db.engine import get_db_url
from trader.db.practice_database_generation import (
    PracticeDatabaseGenerationError,
    target_url_from_source,
    verify_fresh_target_database,
)
from trader.kis_wrapper import KisAPI
from trader.us.data_provider import USDataProvider

logger = logging.getLogger(__name__)


def _qty(value) -> int:
    try:
        return max(0, int(float(str(value or 0).replace(",", ""))))
    except Exception:
        return 0


def _kr_open_holdings(snapshot: dict) -> list[dict]:
    rows = (snapshot or {}).get("output1") or []
    return [
        dict(row)
        for row in rows
        if _qty((row or {}).get("hldg_qty") or (row or {}).get("ord_psbl_qty")) > 0
    ]


def _first_present(row: dict, keys: tuple[str, ...]):
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _kr_daily_ccld_all_pages(kis: KisAPI, *, max_pages: int = 20) -> dict:
    """Fetch authoritative KR daily order truth through the final cursor."""
    today = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
    fk = ""
    nk = ""
    all_rows: list[dict] = []
    last_payload: dict = {}

    for page in range(1, max_pages + 1):
        payload = kis.inquire_daily_ccld(
            start_date=today,
            end_date=today,
            ctx_area_fk100=fk,
            ctx_area_nk100=nk,
        )
        if (
            not isinstance(payload, dict)
            or str(payload.get("rt_cd") or "0") not in {"0", ""}
            or payload.get("_diag_stub")
            or payload.get("_ccld_status")
        ):
            raise RuntimeError("KR_KIS_ORDER_QUERY_NOT_AUTHORITATIVE")

        rows = payload.get("output1") or []
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            raise RuntimeError("KR_KIS_ORDER_QUERY_NOT_AUTHORITATIVE")
        all_rows.extend(dict(row) for row in rows if isinstance(row, dict))
        last_payload = payload

        next_fk = str(
            payload.get("ctx_area_fk100")
            or payload.get("CTX_AREA_FK100")
            or ""
        ).strip()
        next_nk = str(
            payload.get("ctx_area_nk100")
            or payload.get("CTX_AREA_NK100")
            or ""
        ).strip()
        if not next_fk and not next_nk:
            return {
                **last_payload,
                "output1": all_rows,
                "_cutover_pages": page,
                "_cutover_complete": True,
            }
        if next_fk == fk and next_nk == nk:
            raise RuntimeError("KR_KIS_ORDER_PAGINATION_STALLED")
        fk, nk = next_fk, next_nk

    raise RuntimeError("KR_KIS_ORDER_PAGINATION_INCOMPLETE")


def _kr_remaining_qty(row: dict) -> int:
    """Resolve broker remaining qty without treating fully-cancelled rows as open."""
    explicit_remaining = _first_present(
        row,
        ("remaining_qty", "rmn_qty", "nccs_qty", "ord_remn_qty"),
    )
    if explicit_remaining is not None:
        return _qty(explicit_remaining)

    requested = _qty(
        _first_present(row, ("ord_qty", "tot_ord_qty", "requested_qty"))
    )
    filled = _qty(
        _first_present(row, ("tot_ccld_qty", "ccld_qty", "filled_qty"))
    )
    cancelled = _qty(
        _first_present(
            row,
            ("cncl_qty", "tot_cncl_qty", "rvse_cncl_qty", "cancelled_qty"),
        )
    )
    derived = max(0, requested - filled - cancelled)

    status_text = " ".join(
        str(row.get(key) or "")
        for key in (
            "status",
            "ord_sttus",
            "ord_dvsn_name",
            "rvse_cncl_dvsn_name",
            "cncl_yn",
            "cancel_status",
        )
    ).upper()
    cancel_complete = (
        any(token in status_text for token in ("CANCEL_COMPLETE", "CANCELLED", "CANCELED", "취소완료"))
        or str(row.get("cncl_yn") or "").strip().upper() in {"Y", "1", "TRUE"}
    )
    if cancel_complete and cancelled >= max(0, requested - filled):
        return 0
    return derived


def _kr_pending_orders(kis: KisAPI) -> list[dict]:
    payload = _kr_daily_ccld_all_pages(kis)
    pending = []
    for row in payload.get("output1") or []:
        if _kr_remaining_qty(row) > 0:
            pending.append(dict(row))
    return pending


def _us_open_holdings(balance: dict) -> list[dict]:
    rows = (balance or {}).get("positions") or []
    out = []
    for row in rows:
        qty = _qty(
            (row or {}).get("qty")
            or (row or {}).get("holding_qty")
            or (row or {}).get("hldg_qty")
            or (row or {}).get("ord_psbl_qty")
        )
        if qty > 0:
            out.append(dict(row))
    return out


_EXPECTED_US_BALANCE_EXCHANGES = {"NASD", "NYSE", "AMEX"}


def _assert_us_balance_authoritative(balance: dict) -> None:
    if not isinstance(balance, dict):
        raise RuntimeError("US_KIS_BALANCE_NOT_AUTHORITATIVE")
    if str(balance.get("balance_parse_status") or "").upper() != "OK":
        raise RuntimeError("US_KIS_BALANCE_PARSE_NOT_OK")
    if balance.get("balance_complete") is not True:
        raise RuntimeError("US_KIS_BALANCE_INCOMPLETE")
    if balance.get("balance_authoritative") is not True:
        raise RuntimeError("US_KIS_BALANCE_NOT_AUTHORITATIVE")
    failed = balance.get("failed_exchanges")
    if failed not in (None, {}, []):
        raise RuntimeError("US_KIS_BALANCE_EXCHANGE_FAILURE")

    queried = {
        str(value or "").strip().upper()
        for value in (balance.get("queried_exchanges") or [])
        if str(value or "").strip()
    }
    if not _EXPECTED_US_BALANCE_EXCHANGES.issubset(queried):
        raise RuntimeError("US_KIS_BALANCE_EXCHANGE_COVERAGE_INCOMPLETE")

    counts = balance.get("exchange_result_counts")
    if not isinstance(counts, dict):
        raise RuntimeError("US_KIS_BALANCE_EXCHANGE_COUNTS_MISSING")
    count_keys = {str(key or "").strip().upper() for key in counts}
    if not _EXPECTED_US_BALANCE_EXCHANGES.issubset(count_keys):
        raise RuntimeError("US_KIS_BALANCE_EXCHANGE_COUNTS_INCOMPLETE")


def _us_pending_orders(provider: USDataProvider) -> list[dict]:
    trade_date = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    rows = provider.get_today_orders(trade_date)
    pending = []
    for row in rows or []:
        if str((row or {}).get("normalization_result") or "").lower() == "quarantined":
            raise RuntimeError("US_KIS_ORDER_QUERY_QUARANTINED")
        if _qty((row or {}).get("remaining_qty")) > 0:
            pending.append(dict(row))
    return pending


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    env = str(os.getenv("KIS_ENV") or os.getenv("STRATEGY_ENV") or "practice").lower()
    if env != "practice":
        raise PracticeDatabaseGenerationError("PRACTICE_CUTOVER_ONLY")
    if os.getenv("VERIFY_PRACTICE_DB_CUTOVER") != "1" or os.getenv("DB_CUTOVER_CONFIRM") != "YES":
        raise PracticeDatabaseGenerationError("DATABASE_CUTOVER_CONFIRMATION_REQUIRED")

    source_url = get_db_url()
    target_url = str(os.getenv("PBCORE_NEW_DB_URL") or "").strip()
    if not target_url:
        target_name = str(os.getenv("NEW_PRACTICE_DB_NAME") or "").strip()
        if not target_name:
            raise PracticeDatabaseGenerationError("NEW_PRACTICE_DB_NAME_REQUIRED")
        target_url = target_url_from_source(source_url, target_name)

    db_result = verify_fresh_target_database(
        source_url=source_url,
        target_url=target_url,
    )

    kr = KisAPI(kis_env="practice")
    kr_balance = kr.get_balance_cached(force=True)
    if not isinstance(kr_balance, dict) or kr_balance.get("_stub"):
        raise RuntimeError("KR_KIS_BALANCE_NOT_AUTHORITATIVE")
    kr_holdings = _kr_open_holdings(kr_balance)
    if kr_holdings:
        raise RuntimeError(
            "KR_KIS_ACCOUNT_NOT_FLAT:" +
            ",".join(str(row.get("pdno") or row.get("code") or "") for row in kr_holdings)
        )
    kr_pending = _kr_pending_orders(kr)
    if kr_pending:
        raise RuntimeError("KR_KIS_PENDING_ORDERS_EXIST")

    us = USDataProvider(offline=False)
    us_balance = us.get_balance(force_refresh=True)
    _assert_us_balance_authoritative(us_balance)
    us_holdings = _us_open_holdings(us_balance)
    if us_holdings:
        raise RuntimeError(
            "US_KIS_ACCOUNT_NOT_FLAT:" +
            ",".join(str(row.get("symbol") or "") for row in us_holdings)
        )
    us_pending = _us_pending_orders(us)
    if us_pending:
        raise RuntimeError("US_KIS_PENDING_ORDERS_EXIST")

    result = {
        **db_result,
        "status": "CUTOVER_READY",
        "masked_account": get_masked_account_key(env="practice", kis=kr),
        "kr_holdings": 0,
        "kr_pending_orders": 0,
        "us_holdings": 0,
        "us_pending_orders": 0,
        "next_step": (
            "Update PBCORE_DB_URL/DATABASE_URL on every runtime host to the target DB URL, "
            "then run migrations once and perform a no-order preflight before enabling schedules."
        ),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
