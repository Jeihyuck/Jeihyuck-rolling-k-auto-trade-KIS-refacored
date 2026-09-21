from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from trader.kis_wrapper import KisAPI
from trader.us.data_provider import USDataProvider


EXPECTED_US_EXCHANGES_BY_ENV = {
    "practice": {"NASD", "NYSE", "AMEX"},
    # KIS real-account contract: NASD represents the entire US market.
    "real": {"NASD"},
}


class TradingEpochBrokerGuardError(RuntimeError):
    pass


def _qty(value) -> int:
    try:
        if value is None or isinstance(value, bool):
            raise ValueError
        qty = Decimal(str(value).replace(",", "").strip())
        if not qty.is_finite() or qty < 0 or qty != qty.to_integral_value():
            raise ValueError
        return int(qty)
    except (ValueError, InvalidOperation, TypeError):
        raise TradingEpochBrokerGuardError("BROKER_QUANTITY_INVALID") from None


def _first_present(row: dict, keys: tuple[str, ...]):
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _kr_open_holdings(snapshot: dict) -> list[dict]:
    rows = snapshot.get("output1") if isinstance(snapshot, dict) else None
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise TradingEpochBrokerGuardError("KR_KIS_BALANCE_NOT_AUTHORITATIVE")
    out = []
    for row in rows:
        qty = _qty(_first_present(row, ("hldg_qty", "ord_psbl_qty")))
        if qty > 0:
            out.append(dict(row))
    return out


def _kr_daily_ccld_all_pages(kis: KisAPI, *, max_pages: int = 20) -> dict:
    today = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
    fk = ""
    nk = ""
    all_rows: list[dict] = []
    seen_cursors: set[tuple[str, str]] = set()
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
            or str(payload.get("rt_cd")) != "0"
            or payload.get("_diag_stub")
            or payload.get("_ccld_status")
        ):
            raise TradingEpochBrokerGuardError("KR_KIS_ORDER_QUERY_NOT_AUTHORITATIVE")

        rows = payload.get("output1")
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise TradingEpochBrokerGuardError("KR_KIS_ORDER_QUERY_NOT_AUTHORITATIVE")
        all_rows.extend(dict(row) for row in rows)
        last_payload = payload

        next_fk = str(payload.get("ctx_area_fk100") or payload.get("CTX_AREA_FK100") or "").strip()
        next_nk = str(payload.get("ctx_area_nk100") or payload.get("CTX_AREA_NK100") or "").strip()
        meta = payload.get("_response_meta") or {}
        tr_cont = str(meta.get("tr_cont") or "").strip().upper()
        if tr_cont and tr_cont not in {"F", "M", "D", "E"}:
            raise TradingEpochBrokerGuardError("KR_KIS_ORDER_PAGINATION_UNKNOWN_STATUS")
        if tr_cont in {"D", "E"} or (not tr_cont and not next_fk and not next_nk):
            return {**last_payload, "output1": all_rows, "_epoch_guard_pages": page}
        if not next_fk and not next_nk:
            raise TradingEpochBrokerGuardError("KR_KIS_ORDER_PAGINATION_CURSOR_MISSING")
        cursor = (next_fk, next_nk)
        if cursor in seen_cursors:
            raise TradingEpochBrokerGuardError("KR_KIS_ORDER_PAGINATION_STALLED")
        seen_cursors.add(cursor)
        fk, nk = cursor

    raise TradingEpochBrokerGuardError("KR_KIS_ORDER_PAGINATION_INCOMPLETE")


def _kr_remaining_qty(row: dict) -> int:
    explicit = _first_present(row, ("remaining_qty", "rmn_qty", "nccs_qty", "ord_remn_qty"))
    if explicit is not None:
        return _qty(explicit)

    requested = _qty(_first_present(row, ("ord_qty", "tot_ord_qty", "requested_qty")))
    filled = _qty(_first_present(row, ("tot_ccld_qty", "ccld_qty", "filled_qty")))
    cancelled = _qty(_first_present(row, ("cncl_qty", "tot_cncl_qty", "cancelled_qty")) or 0)
    return max(0, requested - filled - cancelled)


def _kr_pending_orders(kis: KisAPI) -> list[dict]:
    payload = _kr_daily_ccld_all_pages(kis)
    return [dict(row) for row in payload.get("output1") or [] if _kr_remaining_qty(row) > 0]


def _assert_us_balance_authoritative(balance: dict, *, env: str) -> None:
    if not isinstance(balance, dict):
        raise TradingEpochBrokerGuardError("US_KIS_BALANCE_NOT_AUTHORITATIVE")
    if str(balance.get("balance_parse_status") or "").upper() != "OK":
        raise TradingEpochBrokerGuardError("US_KIS_BALANCE_PARSE_NOT_OK")
    if balance.get("balance_complete") is not True:
        raise TradingEpochBrokerGuardError("US_KIS_BALANCE_INCOMPLETE")
    if balance.get("balance_authoritative") is not True:
        raise TradingEpochBrokerGuardError("US_KIS_BALANCE_NOT_AUTHORITATIVE")
    if balance.get("failed_exchanges") not in ({}, []):
        raise TradingEpochBrokerGuardError("US_KIS_BALANCE_EXCHANGE_FAILURE")

    expected_exchanges = EXPECTED_US_EXCHANGES_BY_ENV[str(env).lower()]
    queried = {
        str(value or "").strip().upper()
        for value in (balance.get("queried_exchanges") or [])
        if str(value or "").strip()
    }
    if not expected_exchanges.issubset(queried):
        raise TradingEpochBrokerGuardError("US_KIS_BALANCE_EXCHANGE_COVERAGE_INCOMPLETE")

    counts = balance.get("exchange_result_counts")
    if not isinstance(counts, dict):
        raise TradingEpochBrokerGuardError("US_KIS_BALANCE_EXCHANGE_COUNTS_MISSING")
    count_keys = {str(key or "").strip().upper() for key in counts}
    if not expected_exchanges.issubset(count_keys):
        raise TradingEpochBrokerGuardError("US_KIS_BALANCE_EXCHANGE_COUNTS_INCOMPLETE")
    for count in counts.values():
        _qty(count)


def _us_open_holdings(balance: dict) -> list[dict]:
    rows = balance.get("positions")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise TradingEpochBrokerGuardError("US_KIS_BALANCE_NOT_AUTHORITATIVE")
    out = []
    for row in rows:
        qty = _qty(_first_present(row, ("qty", "holding_qty", "hldg_qty", "ord_psbl_qty")))
        if qty > 0:
            out.append(dict(row))
    return out


def _us_pending_orders(provider: USDataProvider) -> list[dict]:
    trade_date = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    rows = provider.get_today_orders(trade_date)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise TradingEpochBrokerGuardError("US_KIS_ORDER_QUERY_NOT_AUTHORITATIVE")
    pending = []
    for row in rows:
        if str(row.get("normalization_result") or "").lower() == "quarantined":
            raise TradingEpochBrokerGuardError("US_KIS_ORDER_QUERY_QUARANTINED")
        if _qty(row.get("remaining_qty")) > 0:
            pending.append(dict(row))
    return pending


def verify_broker_flat(*, env: str) -> dict:
    env_n = str(env or "").strip().lower()
    if env_n not in {"practice", "real"}:
        raise TradingEpochBrokerGuardError(f"TRADING_EPOCH_ENV_UNSUPPORTED:{env_n}")

    kr = KisAPI(kis_env=env_n)
    kr_balance = kr.get_balance_cached(force=True)
    if (
        not isinstance(kr_balance, dict)
        or kr_balance.get("_stub")
        or kr_balance.get("_diag_stub")
        or str(kr_balance.get("rt_cd", "0")) != "0"
        or any(str(kr_balance.get(key) or "").strip() for key in (
            "ctx_area_fk100", "ctx_area_nk100", "CTX_AREA_FK100", "CTX_AREA_NK100",
        ))
    ):
        raise TradingEpochBrokerGuardError("KR_KIS_BALANCE_NOT_AUTHORITATIVE")
    kr_holdings = _kr_open_holdings(kr_balance)
    if kr_holdings:
        raise TradingEpochBrokerGuardError("KR_KIS_ACCOUNT_NOT_FLAT")
    kr_pending = _kr_pending_orders(kr)
    if kr_pending:
        raise TradingEpochBrokerGuardError("KR_KIS_PENDING_ORDERS_EXIST")

    us = USDataProvider(offline=False, env=env_n)
    us_balance = us.get_balance(force_refresh=True)
    _assert_us_balance_authoritative(us_balance, env=env_n)
    us_holdings = _us_open_holdings(us_balance)
    if us_holdings:
        raise TradingEpochBrokerGuardError("US_KIS_ACCOUNT_NOT_FLAT")
    us_pending = _us_pending_orders(us)
    if us_pending:
        raise TradingEpochBrokerGuardError("US_KIS_PENDING_ORDERS_EXIST")

    return {
        "status": "BROKER_FLAT_VERIFIED",
        "env": env_n,
        "kr_holdings": 0,
        "kr_pending_orders": 0,
        "us_holdings": 0,
        "us_pending_orders": 0,
        "us_exchanges": sorted(EXPECTED_US_EXCHANGES_BY_ENV[env_n]),
    }
