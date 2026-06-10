from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any
from pathlib import Path

from trader.config import MARKET_MAP
from trader.account_state import account_reset_mode, env_flag, get_account_key, get_masked_account_key
from trader.runtime_paths import runtime_root
from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo, ReconcileLogRepo
from trader.reconcile_db import evaluate_stale_db_guard
from trader.run_context import RunContext
from trader.time_utils import now_kst

# Import KisAPI and KisTemporaryError from kis_wrapper
try:
    from trader.kis_wrapper import KisAPI, KisTemporaryError
except ImportError:
    # Fallback for module reorganization
    from trader.kis_wrapper import KisAPI
    class KisTemporaryError(RuntimeError):
        """Fallback KisTemporaryError if not found in kis_wrapper"""
        pass

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


def _as_dict(row: Any) -> dict:
    """SQLAlchemy Row / RowMapping / tuple / dict을 안전하게 dict로 변환."""
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    try:
        return dict(row)
    except Exception:
        return {}


def _holdings_index(rows: list[dict]) -> tuple[dict[str, int], dict[str, float]]:
    qty_by_code: dict[str, int] = {}
    avg_price_by_code: dict[str, float] = {}
    for row in rows or []:
        code = _normalize_code(_first_value(row, ["pdno", "stck_shrn_iscd", "code"]))
        if not code:
            continue
        qty = _to_int(_first_value(row, ["hldg_qty", "qty", "ord_psbl_qty"])) or 0
        avg_price = _to_float(_first_value(row, ["pchs_avg_pric", "avg_prvs", "price"])) or 0.0
        qty_by_code[code] = qty
        avg_price_by_code[code] = avg_price
    return qty_by_code, avg_price_by_code


def _promote_open_buy_orders_from_holdings(
    *,
    env: str,
    strategy: str,
    ctx_run_id: str | None,
    tick_ts: datetime,
    holdings_rows: list[dict],
    orders_repo: OrdersRepo,
    fills_repo: FillsRepo,
) -> dict[str, int]:
    qty_by_code, avg_price_by_code = _holdings_index(holdings_rows)
    promoted_orders = 0
    promoted_fills = 0
    promoted_codes: list[str] = []
    for order in orders_repo.get_open_orders(env) or []:
        side = str(order.get("side") or "").upper()
        status = str(order.get("status") or "").upper()
        if side != "BUY" or status not in {"SUBMITTED", "ACKED", "ACCEPTED", "PARTIAL_FILLED"}:
            continue
        code = _normalize_code(order.get("code"))
        if not code:
            continue
        holding_qty = int(qty_by_code.get(code) or 0)
        if holding_qty <= 0:
            continue

        request_json = order.get("request_json") if isinstance(order.get("request_json"), dict) else {}
        response_json = order.get("response_json") if isinstance(order.get("response_json"), dict) else {}
        order_qty = _to_int(order.get("qty")) or 0
        fill_qty = min(order_qty, holding_qty) if order_qty > 0 else holding_qty
        fill_price = (
            _to_float(order.get("limit_price"))
            or _to_float(request_json.get("ORD_UNPR"))
            or _to_float(response_json.get("avg_prvs"))
            or float(avg_price_by_code.get(code) or 0.0)
        )
        kis_odno = str(order.get("kis_odno") or order.get("broker_order_id") or "").strip() or None
        client_order_key = str(order.get("client_order_key") or f"{env}:{strategy}:{code}:promote").strip()
        order_time = order.get("acked_at") or order.get("submitted_at") or tick_ts

        orders_repo.upsert_reconciled_order(
            env=env,
            run_id=ctx_run_id,
            strategy=strategy,
            sid=int(order.get("sid") or 1),
            mode=int(order.get("mode") or 1),
            code=code,
            market=order.get("market"),
            side="BUY",
            ord_type=str(order.get("ord_type") or "RECONCILE_PROMOTED"),
            qty=fill_qty,
            limit_price=fill_price,
            stage=str(order.get("stage") or "RECONCILE"),
            client_order_key=client_order_key,
            kis_odno=kis_odno,
            status="FILLED",
            request_json=request_json,
            response_json={
                **response_json,
                "promotion_source": "kis_holdings",
                "promoted_from_status": status,
                "holding_qty": holding_qty,
            },
            submitted_at=order.get("submitted_at") or order_time,
            acked_at=order_time,
        )
        promoted_orders += 1

        fills_repo.upsert_fill(
            env=env,
            run_id=ctx_run_id,
            order_id=str(order.get("order_id") or "") or None,
            kis_odno=kis_odno,
            trade_id=f"PROMOTE:{kis_odno or client_order_key}",
            code=code,
            market=order.get("market"),
            side="BUY",
            qty=fill_qty,
            price=float(fill_price or 0.0),
            fee=0.0,
            tax=0.0,
            filled_at=order_time,
            raw_json={
                "promotion_source": "kis_holdings_fallback",
                "promoted_from_status": status,
                "holding_qty": holding_qty,
                "ccld_status": "timeout",
                "entry_ts": order_time.isoformat() if hasattr(order_time, "isoformat") else str(order_time),
                "entry_meta": (request_json or {}).get("entry_meta") or {},
                "entry_exit_plan": (request_json or {}).get("entry_exit_plan") or {},
            },
            fill_meta_json={
                "fill_source": "kis_holdings_fallback",
                "promoted_from_status": status,
                "ccld_status": "timeout",
                "entry_ts": order_time.isoformat() if hasattr(order_time, "isoformat") else str(order_time),
            },
        )
        promoted_fills += 1
        promoted_codes.append(code)
        logger.warning(
            "[RECONCILE][PROMOTE_FILL] env=%s source=kis_holdings_fallback ccld_status=timeout code=%s kis_odno=%s from=%s to=FILLED qty=%s holding_qty=%s",
            env,
            code,
            kis_odno,
            status,
            fill_qty,
            holding_qty,
        )
        logger.info(
            "[POSITION_AGE][ENTRY_TS] code=%s source=promote_fill.detected_time entry_ts=%s",
            code,
            order_time.isoformat() if hasattr(order_time, "isoformat") else str(order_time),
        )
    return {"orders": promoted_orders, "fills": promoted_fills, "codes": promoted_codes}


def _restore_entry_meta_for_promoted_positions(
    *,
    env: str,
    strategy: str,
    engine,
    orders_repo: OrdersRepo,
    positions_repo: PositionsRepo,
    ledger_repo: LedgerEventsRepo,
) -> int:
    """KIS holdings 복구 포지션에 entry_meta를 복원한다 (PB1_RECONCILE_RESTORE_ENTRY_META=1 시).

    복원 우선순위:
    1. orders.entry_meta_json (BUY order 중 code와 매칭되는 최신)
    2. ledger ORDER_INTENT payload
    3. 없으면 POLICY_MISSING 유지 (SWING_SAFE fallback 금지)
    """
    import json as _json

    if os.getenv("PB1_RECONCILE_RESTORE_ENTRY_META", "1") != "1":
        return 0

    try:
        import sqlalchemy as _sa
        with engine.connect() as _conn:
            rows = _conn.execute(
                _sa.text(
                    "SELECT code, position_meta, entry_meta_json FROM positions "
                    "WHERE env = :env AND status = 'OPEN' AND qty > 0"
                ),
                {"env": env},
            ).fetchall()
    except Exception as exc:
        logger.warning("[RECONCILE][META_RESTORE][ERROR] step=load_positions err=%s", exc)
        return 0

    restored_count = 0
    for row in rows or []:
        _row = _as_dict(row) if not isinstance(row, tuple) else None
        code = _normalize_code(row[0] if isinstance(row, tuple) else _row.get("code"))
        existing_meta = row[1] if isinstance(row, tuple) else _row.get("position_meta")
        existing_entry_meta = row[2] if isinstance(row, tuple) else _row.get("entry_meta_json")

        if isinstance(existing_meta, str):
            try:
                existing_meta = _json.loads(existing_meta)
            except Exception:
                existing_meta = {}
        existing_meta = existing_meta or {}

        if isinstance(existing_entry_meta, str):
            try:
                existing_entry_meta = _json.loads(existing_entry_meta)
            except Exception:
                existing_entry_meta = {}
        existing_entry_meta = existing_entry_meta or {}

        # 이미 book/trade_horizon이 있으면 스킵
        if existing_entry_meta.get("book") or existing_entry_meta.get("trade_horizon"):
            continue

        # 1. orders.entry_meta_json 조회
        entry_meta_from_order = None
        try:
            import sqlalchemy as _sa
            with engine.connect() as _conn:
                order_row = _conn.execute(
                    _sa.text(
                        "SELECT entry_meta_json FROM orders "
                        "WHERE env = :env AND code = :code AND side = 'BUY' "
                        "AND entry_meta_json IS NOT NULL "
                        "ORDER BY created_at DESC LIMIT 1"
                    ),
                    {"env": env, "code": code},
                ).fetchone()
            if order_row:
                raw = order_row[0]
                if isinstance(raw, str):
                    try:
                        raw = _json.loads(raw)
                    except Exception:
                        raw = {}
                if isinstance(raw, dict) and (raw.get("book") or raw.get("trade_horizon")):
                    entry_meta_from_order = raw
        except Exception as exc:
            logger.warning(
                "[RECONCILE][META_RESTORE] code=%s step=orders err=%s", code, exc
            )

        # 2. ledger ORDER_INTENT payload 조회
        entry_meta_from_ledger = None
        if entry_meta_from_order is None:
            try:
                import sqlalchemy as _sa
                with engine.connect() as _conn:
                    ledger_row = _conn.execute(
                        _sa.text(
                            "SELECT payload FROM ledger_events "
                            "WHERE env = :env AND code = :code "
                            "AND event_type IN ('ORDER_INTENT','ORDER_SUBMIT_ACCEPTED','ORDER_SUBMIT_ATTEMPT') "
                            "ORDER BY ts DESC LIMIT 1"
                        ),
                        {"env": env, "code": code},
                    ).fetchone()
                if ledger_row:
                    raw = ledger_row[0]
                    if isinstance(raw, str):
                        try:
                            raw = _json.loads(raw)
                        except Exception:
                            raw = {}
                    if isinstance(raw, dict):
                        nested = raw.get("entry_meta") or raw.get("entry_meta_json") or raw
                        if isinstance(nested, dict) and (nested.get("book") or nested.get("trade_horizon")):
                            entry_meta_from_ledger = nested
            except Exception as exc:
                logger.warning(
                    "[RECONCILE][META_RESTORE] code=%s step=ledger err=%s", code, exc
                )

        resolved_meta = entry_meta_from_order or entry_meta_from_ledger
        if not resolved_meta:
            logger.warning(
                "[RECONCILE][META_RESTORE][POLICY_MISSING] code=%s action=keep_policy_missing_no_swing_safe_fallback",
                code,
            )
            try:
                import sqlalchemy as _sa
                with engine.begin() as _conn:
                    _conn.execute(
                        _sa.text(
                            "UPDATE positions SET entry_thesis = COALESCE(entry_thesis, 'POLICY_MISSING'), "
                            "exit_policy_family = COALESCE(exit_policy_family, 'POLICY_MISSING'), "
                            "force_eod_close = FALSE, policy_source = COALESCE(policy_source, 'missing') "
                            "WHERE env = :env AND code = :code AND status = 'OPEN' AND qty > 0"
                        ),
                        {"env": env, "code": code},
                    )
            except Exception as exc:
                logger.warning("[RECONCILE][META_RESTORE][POLICY_MISSING_UPDATE_FAIL] code=%s err=%s", code, exc)
            continue

        try:
            import sqlalchemy as _sa
            update_payload = {
                **existing_meta,
                "book": resolved_meta.get("book"),
                "trade_horizon": resolved_meta.get("trade_horizon"),
            }
            if resolved_meta.get("exit_policy_family"):
                update_payload["exit_policy_family"] = resolved_meta["exit_policy_family"]
            source = resolved_meta.get("meta_source", "order_meta")
            update_payload["meta_source"] = source
            with engine.begin() as _conn:
                _conn.execute(
                    _sa.text(
                        "UPDATE positions SET position_meta = :meta, entry_meta_json = :emeta "
                        "WHERE env = :env AND code = :code AND status = 'OPEN'"
                    ),
                    {
                        "meta": _json.dumps(update_payload),
                        "emeta": _json.dumps(resolved_meta),
                        "env": env,
                        "code": code,
                    },
                )
            restored_count += 1
            logger.info(
                "[RECONCILE][META_RESTORE][OK] env=%s code=%s book=%s horizon=%s source=%s",
                env,
                code,
                resolved_meta.get("book"),
                resolved_meta.get("trade_horizon"),
                source,
            )
        except Exception as exc:
            logger.warning(
                "[RECONCILE][META_RESTORE][FAIL] code=%s err=%s", code, exc
            )

    return restored_count


def reconcile_today(*, engine, kis: KisAPI, ctx: RunContext) -> dict[str, object]:
    env = ctx.env
    run_id = (os.getenv("TRADER_RUN_ID") or "").strip() or None
    strategy = ctx.strategy
    today = now_kst().strftime("%Y%m%d")
    degraded_reason: str | None = None
    try:
        resp = kis.inquire_daily_ccld(start_date=today, end_date=today)
    except KisTemporaryError as exc:
        logger.warning("[RECONCILE][DEGRADED] temporary error: %s", exc)
        degraded_reason = "temporary"
        resp = {"output1": [], "output2": []}
    if not isinstance(resp, dict):
        degraded_reason = degraded_reason or "invalid_response"
        resp = {"output1": [], "output2": []}
    rows = resp.get("output1") or resp.get("output2") or resp.get("output") or []
    ccld_status = str(resp.get("_ccld_status") or ("ok" if isinstance(resp, dict) else "unknown"))
    if isinstance(rows, dict):
        rows = [rows]
    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    ledger_repo = LedgerEventsRepo(engine)
    reconcile_repo = ReconcileLogRepo(engine)

    order_count = 0
    fill_count = 0
    filled_codes: list[str] = []
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
        plan_record = None
        request_json = dict(row or {}) if isinstance(row, dict) else {"kis_row": row}
        if side == "BUY":
            try:
                plan_record = orders_repo.find_latest_buy_entry_exit_plan(env, strategy, code)
            except Exception as exc:
                logger.warning("[RECONCILE][DAILY_CCLD][ENTRY_EXIT_PLAN_LOOKUP_FAIL] code=%s err=%s", code, exc)
                plan_record = None
            if plan_record:
                request_json.update({
                    "entry_meta": plan_record.get("entry_meta") or {},
                    "entry_exit_plan": plan_record.get("entry_exit_plan") or {},
                    "entry_exit_plan_source": "latest_buy_order_request_json",
                })
                logger.info(
                    "[RECONCILE][DAILY_CCLD][ENTRY_EXIT_PLAN_MERGE] code=%s source=latest_buy_order_request_json",
                    code,
                )

        orders_repo.upsert_reconciled_order(
            env=env,
            run_id=ctx.run_id,
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
            request_json=request_json,
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
                run_id=ctx.run_id,
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
                raw_json={
                    "kis_response": row,
                    "entry_meta": (request_json.get("entry_meta") if isinstance(request_json, dict) else {}) or {},
                    "entry_exit_plan": (request_json.get("entry_exit_plan") if isinstance(request_json, dict) else {}) or {},
                    "entry_exit_plan_source": request_json.get("entry_exit_plan_source") if isinstance(request_json, dict) else None,
                },
                fill_meta_json={
                    "fill_source": "daily_ccld",
                    "ccld_status": ccld_status,
                    "entry_ts": order_time.isoformat() if hasattr(order_time, "isoformat") else str(order_time),
                },
            )
            fill_count += 1
            filled_codes.append(code)

    reasons = [f"orders:{order_count}", f"fills:{fill_count}"]
    if degraded_reason:
        reasons.append(f"degraded:{degraded_reason}")
    ledger_repo.append_event(
        env=env,
        run_id=run_id,
        strategy=strategy,
        event_type="RECONCILE",
        ts=now_kst(),
        ok=True,
        reasons=reasons,
        payload_json={"orders": order_count, "fills": fill_count, "degraded": degraded_reason},
    )
    logger.info("[RECONCILE][DONE] env=%s orders=%s fills=%s source=%s", env, order_count, fill_count, "daily_ccld")
    reconcile_repo.append_log(
        env=env,
        strategy=strategy,
        tick_ts=now_kst(),
        action="reconcile_today",
        details_json={"orders": order_count, "fills": fill_count, "degraded": degraded_reason},
    )
    return {
        "ok": True,
        "orders": order_count,
        "fills": fill_count,
        "degraded": degraded_reason,
        "ccld_status": ccld_status,
        "filled_codes": filled_codes,
        "fill_source": "daily_ccld",
    }


def reconcile_kis(
    *,
    engine,
    kis: KisAPI,
    env: str,
    run_id: str | None,
    strategy: str,
    tick_ts: datetime,
    balance_snapshot: dict | None = None,
    runtime_dir: str | None = None,
) -> dict[str, object]:
    holdings_error = None
    holdings_rows: list[dict] = []
    try:
        snapshot = balance_snapshot or kis.get_balance_cached(force=True)
        holdings_rows = snapshot.get("output1") or []
    except KisTemporaryError as exc:
        holdings_error = str(exc)
        logger.error("[KIS][HTTP][FAIL_SOFT] step=holdings err=%s", exc, exc_info=True)
        holdings_rows = []
    except Exception as exc:
        holdings_error = str(exc)
        logger.error("[KIS][HTTP][FAIL_SOFT] step=holdings err=%s", exc, exc_info=True)
        holdings_rows = []

    ctx: RunContext | None = None
    try:
        exec_mode = "LIVE" if env == "real" else "DIAG"
        ctx = RunContext.new(
            account_env=env,
            exec_mode=exec_mode,
            strategy=strategy,
            dry_run=False,
        )
        reconcile_result = reconcile_today(engine=engine, kis=kis, ctx=ctx)
    except Exception as exc:
        logger.error("[KIS][HTTP][FAIL_SOFT] step=reconcile_today err=%s", exc, exc_info=True)
        reconcile_result = {"ok": False, "reason": "reconcile_today_failed", "err": str(exc)}
    orders_count = int(reconcile_result.get("orders") or 0)
    fills_count = int(reconcile_result.get("fills") or 0)
    reset_mode = bool(env == "practice" and account_reset_mode())
    account_key = get_account_key(env=env, kis=kis)
    masked_account = get_masked_account_key(env=env, kis=kis)

    orders_repo = OrdersRepo(engine)
    fills_repo = FillsRepo(engine)
    positions_repo = PositionsRepo(engine)
    promoted = _promote_open_buy_orders_from_holdings(
        env=env,
        strategy=strategy,
        ctx_run_id=run_id,
        tick_ts=tick_ts,
        holdings_rows=holdings_rows,
        orders_repo=orders_repo,
        fills_repo=fills_repo,
    )
    orders_count += int(promoted.get("orders") or 0)
    fills_count += int(promoted.get("fills") or 0)
    refresh_codes = sorted(
        {
            str(code).zfill(6)
            for code in (list(reconcile_result.get("filled_codes") or []) + list(promoted.get("codes") or []))
            if str(code).strip()
        }
    )
    if refresh_codes:
        try:
            kis.invalidate_balance_cache(reason="reconcile_fill", codes=refresh_codes)
            refreshed_snapshot = kis.get_balance_cached(force=True)
            refreshed_holdings = refreshed_snapshot.get("output1") or []
            logger.info("[POSITIONS][REFRESH_AFTER_FILL] source=kis_balance holdings=%s", len(refreshed_holdings))
            logger.info("[PNL][SNAPSHOT] source=refreshed_holdings after_fills=1")
            holdings_rows = refreshed_holdings
        except Exception as exc:
            logger.warning("[POSITIONS][REFRESH_AFTER_FILL][FAIL] err=%s", exc)
    if int(promoted.get("fills") or 0) > 0:
        logger.info(
            "[RECONCILE][DONE] env=%s orders=%s fills=%s source=holdings_fallback",
            env,
            orders_count,
            fills_count,
        )
    restored = 0
    if reset_mode:
        allow_holdings = env_flag("RESET_ALLOW_KIS_HOLDINGS", default=False)
        if holdings_rows and not allow_holdings:
            logger.warning(
                "[RECONCILE][POSITIONS][RESTORE_BLOCKED] env=%s account=%s reset_mode=1 holdings=%s reason=reset_mode_kis_holdings_present",
                env,
                masked_account,
                len(holdings_rows),
            )
        else:
            logger.info(
                "[RECONCILE][POSITIONS][RESTORE_SKIP] env=%s account=%s reset_mode=1 holdings=%s",
                env,
                masked_account,
                len(holdings_rows),
            )
    else:
        restored = positions_repo.restore_missing_from_holdings(
            env=env,
            strategy=strategy,
            sid=1,
            mode=1,
            holdings=holdings_rows,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
        )

        # KIS holdings가 있고 DB positions가 0이면 upsert 복구
        if holdings_rows and env in {"practice", "live"}:
            db_positions_count = 0
            try:
                import sqlalchemy as _sa
                with engine.connect() as _conn:
                    result = _conn.execute(
                        _sa.text(
                            "SELECT COUNT(*) FROM positions WHERE env = :env AND status = 'OPEN' AND qty > 0"
                        ),
                        {"env": env},
                    )
                    db_positions_count = int(result.scalar() or 0)
            except Exception:
                pass
            kis_holdings_count = len([h for h in holdings_rows if int(float(h.get("hldg_qty") or h.get("qty") or 0)) > 0])
            if kis_holdings_count > 0 and db_positions_count == 0:
                logger.warning(
                    "[RECONCILE][POSITIONS_EMPTY_BUT_KIS_HAS_HOLDINGS] env=%s kis_holdings=%s db_positions=%s action=upsert_from_kis",
                    env,
                    kis_holdings_count,
                    db_positions_count,
                )
                account_key = get_account_key(env=env, kis=kis)
                positions_repo.upsert_positions_from_kis_holdings(
                    env=env,
                    account_key=account_key,
                    holdings=holdings_rows,
                )
    if restored:
        logger.warning("[RECONCILE][POSITIONS][RESTORE] env=%s restored=%s", env, restored)

    # [2026-05-21] entry meta 복원 (KIS holdings 복구 포지션 대상)
    meta_restored = 0
    try:
        ledger_repo = LedgerEventsRepo(engine)
        meta_restored = _restore_entry_meta_for_promoted_positions(
            env=env,
            strategy=strategy,
            engine=engine,
            orders_repo=orders_repo,
            positions_repo=positions_repo,
            ledger_repo=ledger_repo,
        )
        if meta_restored:
            logger.info("[RECONCILE][META_RESTORE][DONE] env=%s count=%s", env, meta_restored)
    except Exception as _meta_exc:
        logger.warning("[RECONCILE][META_RESTORE][SKIP] err=%s", _meta_exc)

    guard_result = None
    guard_reason = None
    allow_purge = None
    runtime_dir = Path(runtime_dir) if runtime_dir else runtime_root()
    allow_purge, guard_reason, guard_result = evaluate_stale_db_guard(
        runtime_dir=runtime_dir,
        tick_ts=tick_ts,
        kis_holdings_empty=len(holdings_rows) == 0,
        orders_count=orders_count,
        fills_count=fills_count,
        had_kis_error=holdings_error is not None,
    )
    if holdings_error:
        allow_purge = False
        guard_reason = guard_reason or "holdings_error"
        logger.warning(
            "[RECONCILE][STALE_DB_GUARD] allow_purge=0 reason=holdings_error err=%s",
            holdings_error,
        )
    if allow_purge:
        logger.warning(
            "[RECONCILE][STALE_DB_GUARD] allow_purge=1 empty_streak=%s",
            guard_result.get("empty_streak") if isinstance(guard_result, dict) else None,
        )
    else:
        logger.info(
            "[RECONCILE][STALE_DB_GUARD] allow_purge=0 reason=%s empty_streak=%s",
            guard_reason,
            guard_result.get("empty_streak") if isinstance(guard_result, dict) else None,
        )

    reconcile_repo = ReconcileLogRepo(engine)
    reconcile_repo.append_log(
        env=env,
        strategy=strategy,
        tick_ts=now_kst(),
        action="reconcile_kis",
        details_json={
            "orders": orders_count,
            "fills": fills_count,
            "promoted_orders": int(promoted.get("orders") or 0),
            "promoted_fills": int(promoted.get("fills") or 0),
            "filled_codes": refresh_codes,
            "holdings": len(holdings_rows),
            "restored_positions": restored,
            "account_key": account_key,
            "masked_account": masked_account,
            "reset_mode": reset_mode,
            "holdings_error": holdings_error,
            "guard_reason": guard_reason,
            "allow_purge": allow_purge,
            "run_id": run_id,
        },
    )
    reconcile_result.update(
        {
            "holdings": len(holdings_rows),
            "promoted_orders": int(promoted.get("orders") or 0),
            "promoted_fills": int(promoted.get("fills") or 0),
            "filled_codes": refresh_codes,
            "restored_positions": restored,
            "account_key": account_key,
            "masked_account": masked_account,
            "reset_mode": reset_mode,
            "holdings_error": holdings_error,
            "guard_reason": guard_reason,
            "allow_purge": allow_purge,
        }
    )
    return reconcile_result
