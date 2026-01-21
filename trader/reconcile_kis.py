from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from trader.config import MARKET_MAP
from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo
from trader.kis_wrapper import KisAPI, KisTemporaryError
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def _first_value(row: dict, keys: list[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return value
    return None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _parse_date_time(row: dict) -> datetime:
    date_raw = _first_value(row, ["ord_dt", "trd_dt", "ccld_dt", "ord_date", "date"])
    time_raw = _first_value(row, ["ord_tmd", "trd_tmd", "ccld_tmd", "ord_time", "time"])
    if date_raw:
        date_raw = str(date_raw).replace("-", "")
    if time_raw:
        time_raw = str(time_raw).replace(":", "")
    if date_raw and time_raw and len(date_raw) == 8 and len(time_raw) >= 4:
        try:
            return datetime.strptime(f"{date_raw}{time_raw[:6]}", "%Y%m%d%H%M%S")
        except Exception:
            pass
    if date_raw and len(date_raw) == 8:
        try:
            return datetime.strptime(date_raw, "%Y%m%d")
        except Exception:
            pass
    return now_kst()


def _parse_side(row: dict) -> str:
    raw = str(_first_value(row, ["sll_buy_dvsn_cd", "sll_buy_dvsn", "buy_sell_gb", "side"]) or "").strip()
    if raw in {"01", "1", "SELL", "S"} or "매도" in raw.upper():
        return "SELL"
    if raw in {"02", "2", "BUY", "B"} or "매수" in raw.upper():
        return "BUY"
    return "UNKNOWN"


def _normalize_code(value: Any) -> str:
    return str(value or "").strip().zfill(6)


def reconcile_today(*, engine, kis: KisAPI, env: str, run_id: str | None, strategy: str) -> dict[str, object]:
    today = now_kst().strftime("%Y%m%d")
    try:
        resp = kis.inquire_daily_ccld(start_date=today, end_date=today)
    except KisTemporaryError as exc:
        logger.warning("[RECONCILE][DEGRADED] temporary error: %s", exc)
        return {"ok": False, "reason": "temporary", "err": str(exc)}
    rows = resp.get("output1") or resp.get("output2") or resp.get("output") or []
    if isinstance(rows, dict):
        rows = [rows]
    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    ledger_repo = LedgerEventsRepo(engine)

    order_count = 0
    fill_count = 0
    for row in rows or []:
        code = _normalize_code(_first_value(row, ["pdno", "stck_shrn_iscd", "code"]))
        if not code:
            continue
        side = _parse_side(row)
        qty = _to_int(_first_value(row, ["ord_qty", "qty", "tot_ccld_qty", "ord_qty_sum"])) or 0
        price = _to_float(_first_value(row, ["ord_unpr", "ord_price", "avg_prvs", "ccld_prc"])) or 0.0
        kis_odno = str(_first_value(row, ["odno", "ODNO", "ordno"]) or "").strip() or None
        status = str(_first_value(row, ["ord_stat_cd", "ord_stat", "status"]) or "RECONCILED").strip().upper()
        market = MARKET_MAP.get(code) or str(_first_value(row, ["excg_dvsn_cd", "market"]) or "").strip() or None
        order_time = _parse_date_time(row)
        client_order_key = f"{env}:{strategy}:{today}:{code}:{side}:{kis_odno or 'reconcile'}"

        orders_repo.upsert_reconciled_order(
            env=env,
            run_id=run_id,
            strategy=strategy,
            sid=1,
            mode=1,
            code=code,
            market=market,
            side=side,
            ord_type=str(_first_value(row, ["ord_dvsn_cd", "ord_type"]) or "RECONCILED"),
            qty=qty,
            limit_price=price,
            stage="RECONCILE",
            client_order_key=client_order_key,
            kis_odno=kis_odno,
            status=status,
            request_json=row,
            response_json=row,
            submitted_at=order_time,
            acked_at=order_time,
        )
        order_count += 1

        filled_qty = _to_int(_first_value(row, ["ccld_qty", "tot_ccld_qty", "filled_qty"]))
        filled_price = _to_float(_first_value(row, ["ccld_prc", "avg_prvs", "filled_price"]))
        if filled_qty and filled_price is not None and side != "UNKNOWN":
            fills_repo.upsert_fill(
                env=env,
                run_id=run_id,
                order_id=None,
                kis_odno=kis_odno,
                trade_id=str(_first_value(row, ["ccld_no", "trade_id", "exec_id"]) or "") or None,
                code=code,
                market=market,
                side=side,
                qty=filled_qty,
                price=filled_price,
                fee=0.0,
                tax=0.0,
                filled_at=order_time,
                raw_json=row,
            )
            fill_count += 1

    ledger_repo.append_event(
        env=env,
        run_id=run_id,
        event_type="RECONCILE",
        ts=now_kst(),
        ok=True,
        reasons=[f"orders:{order_count}", f"fills:{fill_count}"],
        payload_json={"orders": order_count, "fills": fill_count},
    )
    logger.info("[RECONCILE][DONE] env=%s orders=%s fills=%s", env, order_count, fill_count)
    return {"ok": True, "orders": order_count, "fills": fill_count}
