from __future__ import annotations

import logging
import os
from typing import Any

from trader.eventlog import emit_event

logger = logging.getLogger(__name__)


def submit_exit_sell_order(
    *,
    engine: Any,
    exit_eval_payload: dict[str, Any],
    exit_eval: Any,
    code: str,
    display_code: str,
    stock_name: str,
    market: str | None,
    mode: int,
    sid: int,
    orderable_qty: int,
    mark: float,
    stage: str,
    client_key: str,
    exit_policy_family: str,
    reason_family: str,
    lifecycle_id: str,
    ret_pct: float,
    avg: float,
    kis_qty: int,
    kis_sellable_qty: int,
    position_meta: dict[str, Any],
    cycle_id: str | None,
    sell_baseline: Any | None,
    days_held: int,
    cooldown_until: str | None,
    exit_meta: dict[str, Any],
    order_id: str,
) -> dict[str, Any]:
    exit_eval_payload["submit_attempted"] = 1
    emit_event(
        as_of=engine._today,
        event="ORDER_SUBMIT",
        side="SELL",
        code=code,
        qty=orderable_qty,
        price=float(mark),
        order_type="MARKET",
        client_order_key=client_key,
    )
    resp = None
    kis_odno = None
    requested_qty = int(orderable_qty or 0)
    try:
        resp = engine.kis.sell_stock_market(code, requested_qty)
        kis_odno = (resp.get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
    except Exception:
        logger.exception("[PB1][EXIT][FAIL] code=%s", display_code)
    execution_meta = (
        resp.get("_order_execution")
        if isinstance(resp, dict) and isinstance(resp.get("_order_execution"), dict)
        else {}
    )
    submitted_qty = int(execution_meta.get("submitted_qty") or requested_qty)
    engine.orders_repo.mark_submitted(
        engine.env,
        client_key,
        kis_odno,
        resp if isinstance(resp, dict) else {"resp": resp},
        submitted_qty=submitted_qty,
    )
    ok = bool(resp and isinstance(resp, dict) and resp.get("rt_cd") == "0")
    rt_cd = resp.get("rt_cd") if isinstance(resp, dict) else None
    msg_cd = resp.get("msg_cd") if isinstance(resp, dict) else None
    msg1 = resp.get("msg1") if isinstance(resp, dict) else None
    emit_event(
        as_of=engine._today,
        event="ORDER_RESULT",
        side="SELL",
        code=code,
        ok=ok,
        rt_cd=rt_cd,
        msg_cd=msg_cd,
        msg1=msg1,
        kis_odno=kis_odno,
    )
    logger.info(
        "[PB1][ORDER][RESULT] side=SELL code=%s name=%s ok=%s reason=%s rt_cd=%s msg_cd=%s msg1=%s",
        code,
        stock_name,
        int(ok),
        engine._format_order_result_reason(resp if isinstance(resp, dict) else None),
        rt_cd,
        msg_cd,
        msg1,
    )
    exit_eval_payload["order_result"] = engine._format_order_result_reason(resp if isinstance(resp, dict) else None)
    logger.info(
        "[TRADE][ORDER][SELL] code=%s name=%s oid=%s qty=%s price=%.2f result=%s",
        code,
        stock_name,
        kis_odno or order_id,
        submitted_qty,
        float(mark or 0.0),
        "ACCEPTED" if ok else "REJECTED",
    )
    if ok:
        exit_eval_payload["submitted"] = 1
        engine.session_exit_submitted_codes.add(display_code)
        engine._register_session_sell_accepted(
            code=code,
            qty=submitted_qty,
            price=float(mark or 0.0),
            order_id=str(kis_odno or order_id or ""),
        )
        logger.info(
            "[SELL_SESSION_BLOCK][DURABLE_REGISTER] code=%s cycle=%s order_id=%s session=%s",
            code,
            cycle_id or "",
            kis_odno or order_id,
            str(os.getenv("PB1_SESSION_KIND") or engine.window_internal or "day").lower(),
        )
        if hasattr(engine.kis, "invalidate_balance_cache"):
            engine.kis.invalidate_balance_cache(reason=f"sell_ack:{code}", codes=[display_code])
        try:
            engine.orders_repo.mark_acked(engine.env, kis_odno, resp)
            logger.info("[ORDER][DB_ACK][OK][SELL] code=%s kis_odno=%s", code, kis_odno)
        except Exception as _sell_ack_exc:
            _soft_ack = os.getenv("PB1_ORDER_DB_ACK_FAIL_SOFT", "1") == "1"
            logger.warning(
                "[ORDER][DB_ACK][TIMEOUT][SELL] code=%s kis_odno=%s err=%s",
                code,
                kis_odno,
                _sell_ack_exc,
            )
            if not _soft_ack:
                raise
        logger.info(
            "[ORDER][ACCEPTED] side=SELL code=%s name=%s odno=%s fill_status=pending submitted_qty=%s requested_qty=%s",
            code,
            stock_name,
            kis_odno or order_id,
            submitted_qty,
            requested_qty,
        )
        if cooldown_until:
            engine.positions_repo.update_position_fields(
                env=engine.env,
                strategy=engine.STRATEGY_NAME,
                sid=sid,
                mode=mode,
                code=code,
                fields={"cooldown_until": cooldown_until},
            )
        exit_eval_payload["terminal_event"] = "API_RESULT"
    else:
        reject_reason = str(engine._format_order_result_reason(resp if isinstance(resp, dict) else None) or "")
        msg_cd_norm = str(msg_cd or "").upper()
        msg1_norm = str(msg1 or "").lower()
        if (
            msg_cd_norm == "NO_SELLABLE_QTY"
            or "ORDER_SKIP_NO_SELLABLE_QTY" in reject_reason
            or "sellable quantity is zero" in msg1_norm
        ):
            engine._register_session_no_sellable(code=code, reason="KIS_NO_SELLABLE_QTY")
            engine.no_sellable_qty_terminal_codes.add(display_code)
        engine.orders_repo.mark_error(engine.env, client_key, resp if isinstance(resp, dict) else {"resp": resp})
        exit_eval_payload["rejected"] = 1
        exit_eval_payload["failed"] = int(exit_eval_payload.get("failed", 0) or 0) + 1
        exit_eval_payload["submit_terminal_status"] = engine._classify_submit_terminal_status(
            api_submitted=int(exit_eval_payload.get("api_submitted", 0) or 0),
            accepted=int(exit_eval_payload.get("accepted", 0) or 0),
            skipped_reason=str(exit_eval_payload.get("skipped_reason") or ""),
            response=resp if isinstance(resp, dict) else None,
        )
        exit_eval_payload["terminal_event"] = "API_RESULT"
    engine.positions_repo.update_position_fields(
        env=engine.env,
        strategy=engine.STRATEGY_NAME,
        sid=sid,
        mode=mode,
        code=code,
        fields={
            "last_exit_eval_json": exit_eval_payload,
            "last_exit_plan_eval_json": exit_meta,
        },
    )
    return exit_eval_payload
