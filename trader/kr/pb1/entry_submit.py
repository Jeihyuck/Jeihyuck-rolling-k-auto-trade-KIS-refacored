from __future__ import annotations

import logging
import os
from typing import Any

from trader.eventlog import emit_event
from trader.kis_wrapper import extract_order_no, is_order_accepted
from trader.kr_price_utils import normalize_kr_order_price
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def _authoritative_holding_qty(snapshot: dict[str, Any]) -> int:
    if "kis_holding_qty" in snapshot and snapshot.get("kis_holding_qty") is not None:
        try:
            return max(0, int(float(snapshot.get("kis_holding_qty") or 0)))
        except (TypeError, ValueError):
            return 0
    try:
        return max(0, int(float(snapshot.get("holding_qty") or 0)))
    except (TypeError, ValueError):
        return 0


def submit_entry_buy_order(
    *,
    engine: Any,
    cf: Any,
    display_code: str,
    stock_name: str,
    cap: float,
    reasons: list[str],
    base_from: str | None,
    base: float | None,
    cap_buffer_pct: float,
    entry_meta: dict[str, Any],
    entry_exit_plan_dict: dict[str, Any],
    gate_snapshot: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    effective_client_order_key = cf.client_order_key or ""
    existing_order = (
        engine.orders_repo.get_order_by_client_order_key(engine.env, effective_client_order_key)
        if hasattr(engine.orders_repo, "get_order_by_client_order_key") and effective_client_order_key
        else None
    )
    existing_status = str((existing_order or {}).get("status") or "").upper()
    if existing_order and engine._is_retryable_entry_order_status(existing_status):
        effective_client_order_key = engine._next_retry_client_order_key(effective_client_order_key)
        logger.info(
            "[ORDER][PRE_SUBMIT][RETRY_KEY] code=%s old_key=%s new_key=%s prior_status=%s",
            display_code,
            cf.client_order_key,
            effective_client_order_key,
            existing_status,
        )
    try:
        order_id, created = engine.orders_repo.create_intent_idempotent(
            env=engine.env,
            run_id=engine.run_id,
            strategy=engine.STRATEGY_NAME,
            sid=1,
            mode=cf.mode,
            code=cf.code,
            market=cf.market,
            side="BUY",
            ord_type="LIMIT",
            qty=cf.planned_qty,
            limit_price=cap,
            stage="PB1-CLOSE",
            client_order_key=effective_client_order_key,
            request_json={
                "features": cf.features,
                "reasons": reasons,
                "base_from": base_from,
                "base": base,
                "cap_buffer_pct": cap_buffer_pct,
                "entry_meta": entry_meta,
                "entry_exit_plan": entry_exit_plan_dict,
                "pre_order_holding_qty": _authoritative_holding_qty(gate_snapshot),
                "requested_qty": int(cf.planned_qty or 0),
                "submitted_qty": int(cf.planned_qty or 0),
            },
            status="CREATED",
            entry_meta_json=entry_meta,
        )
    except Exception:
        logger.exception("[PB1][CLOSE_ENTRY][DB_FAIL] code=%s", display_code)
        if not engine.dry_run:
            raise
        status["skipped_reason"] = "db_fail"
        status["skipped"] = 1
        status["terminal_event"] = "FINAL_SKIP"
        return status
    if not created:
        engine._log_final_skip(
            cf=cf,
            reason_code="DUPLICATE_ORDER_EXISTS",
            reason_detail=f"client_order_key={effective_client_order_key}",
            stage="PB1-CLOSE",
            price=float(cap or 0.0),
        )
        status["skipped_reason"] = "DUPLICATE_ORDER_EXISTS"
        status["skipped"] = 1
        status["terminal_event"] = "FINAL_SKIP"
        return status
    cf.client_order_key = effective_client_order_key
    logger.info(
        "[ENTRY][META][SAVE] code=%s entry_reason=%s decision_family=%s stop=%s pivot=%s score=%s",
        display_code,
        entry_meta.get("entry_reason"),
        entry_meta.get("entry_decision_family"),
        entry_meta.get("stop_price_at_entry"),
        entry_meta.get("pivot_price_at_entry"),
        entry_meta.get("score_final_at_entry"),
    )
    if engine.dry_run:
        logger.info(
            "[PB1][CLOSE_ENTRY-DRY] code=%s qty=%s cap=%s key=%s order_id=%s",
            display_code,
            cf.planned_qty,
            cap,
            effective_client_order_key,
            order_id,
        )
        logger.info(
            "[TRADE][ORDER][BUY] code=%s name=%s oid=%s qty=%s price=%.2f result=DRY_RUN",
            cf.code,
            str(engine._name_for_code(cf.code) or cf.features.get("name") or cf.code),
            order_id,
            cf.planned_qty,
            float(cap or 0.0),
        )
        status["skipped"] = 1
        status["submit_attempted"] = 1
        status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
        status["terminal_event"] = "FINAL_SKIP"
        return status
    if not engine.kis:
        logger.warning("[PB1][CLOSE_ENTRY][SKIP] KIS missing code=%s", display_code)
        engine._log_final_skip(
            cf=cf,
            reason_code="KIS_MISSING",
            reason_detail="kis client unavailable",
            stage="PB1-CLOSE",
            price=float(cap or 0.0),
        )
        status["skipped"] = 1
        status["skipped_reason"] = "KIS_MISSING"
        status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
        status["terminal_event"] = "FINAL_SKIP"
        return status
    if not engine._pretrade_check(
        code=cf.code,
        market=cf.market,
        mode=cf.mode,
        side="BUY",
        qty=cf.planned_qty,
        price=float(cap or 0.0),
        client_order_key=effective_client_order_key,
        stage="PB1-CLOSE",
    ):
        engine._log_final_skip(
            cf=cf,
            reason_code="PRETRADE_CHECK_FAILED",
            reason_detail="validate_tradeable returned false",
            stage="PB1-CLOSE",
            price=float(cap or 0.0),
        )
        status["skipped"] = 1
        status["skipped_reason"] = "PRETRADE_CHECK_FAILED"
        status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
        status["terminal_event"] = "FINAL_SKIP"
        return status
    engine._append_ledger_event(
        event_type="ORDER_INTENT",
        code=cf.code,
        market=cf.market,
        mode=cf.mode,
        side="BUY",
        qty=cf.planned_qty,
        price=float(cap or 0.0),
        client_order_key=effective_client_order_key,
        ok=True,
        reasons=reasons,
        stage="PB1-CLOSE",
        payload_json={"entry_meta": entry_meta, "trace_id": entry_meta.get("trace_id")},
    )
    emit_event(
        as_of=engine._today,
        event="ORDER_SUBMIT",
        side="BUY",
        code=cf.code,
        qty=cf.planned_qty,
        price=float(cap),
        order_type="LIMIT",
        client_order_key=effective_client_order_key,
    )
    resp = None
    kis_odno = None
    try:
        status["submit_attempted"] = 1
        logger.info(
            "[ORDER][API_REQUEST] code=%s name=%s qty=%s price=%s order_type=LIMIT",
            cf.code,
            str(engine._name_for_code(cf.code) or cf.features.get("name") or cf.code),
            cf.planned_qty,
            float(cap or 0.0),
        )
        raw_submit_price = float(cap)
        final_price, tick_size = normalize_kr_order_price(raw_submit_price, side="BUY")
        if int(round(raw_submit_price)) != int(final_price):
            logger.info(
                "[ORDER][PRICE_NORMALIZE] code=%s side=BUY raw=%s normalized=%s tick=%s source=pre_kis_final",
                cf.code,
                int(round(raw_submit_price)),
                final_price,
                tick_size,
            )
        assert isinstance(final_price, int) and final_price > 0 and final_price % tick_size == 0
        resp = engine.kis.buy_stock_limit(cf.code, cf.planned_qty, final_price)
        kis_odno = extract_order_no(resp)
        status["broker_submit_called"] = 1
        status["api_submitted"] = 1
    except Exception:
        logger.exception("[PB1][CLOSE_ENTRY][FAIL] code=%s", display_code)
        status["failed"] = 1
    submitted_qty = int(cf.planned_qty or 0)
    if isinstance(resp, dict):
        execution_meta = resp.get("_order_execution") if isinstance(resp.get("_order_execution"), dict) else {}
        submitted_qty = int(execution_meta.get("submitted_qty") or submitted_qty)
    engine.orders_repo.mark_submitted(
        engine.env,
        effective_client_order_key or "",
        kis_odno,
        resp if isinstance(resp, dict) else {"resp": resp},
        entry_meta_json=entry_meta,
        submitted_qty=submitted_qty,
    )
    status["submitted"] = int(status.get("api_submitted", 0) or 0)
    status["broker_order_no"] = kis_odno
    engine._append_ledger_event(
        event_type="ORDER_SUBMIT_ATTEMPT",
        code=cf.code,
        market=cf.market,
        mode=cf.mode,
        side="BUY",
        qty=cf.planned_qty,
        price=float(cap or 0.0),
        client_order_key=effective_client_order_key,
        ok=bool(status.get("api_submitted")),
        reasons=["submit"],
        stage="PB1-CLOSE",
        payload_json={"entry_meta": entry_meta, "entry_exit_plan": entry_exit_plan_dict, "trace_id": entry_meta.get("trace_id"), "kis_odno": kis_odno},
    )
    ok = bool(is_order_accepted(resp, kis_env=engine.env))
    rt_cd = resp.get("rt_cd") if isinstance(resp, dict) else None
    msg_cd = resp.get("msg_cd") if isinstance(resp, dict) else None
    msg1 = resp.get("msg1") if isinstance(resp, dict) else None
    status["broker_response_code"] = msg_cd or rt_cd
    status["broker_message"] = msg1
    logger.info(
        "[ORDER][API_RESULT] code=%s name=%s rt_cd=%s msg_cd=%s accepted=%s rejected=%s",
        cf.code,
        str(engine._name_for_code(cf.code) or cf.features.get("name") or cf.code),
        rt_cd,
        msg_cd,
        int(bool(ok)),
        int(not bool(ok)),
    )
    emit_event(
        as_of=engine._today,
        event="ORDER_RESULT",
        side="BUY",
        code=cf.code,
        ok=ok,
        rt_cd=rt_cd,
        msg_cd=msg_cd,
        msg1=msg1,
        kis_odno=kis_odno,
    )
    reason_code = engine._format_order_result_reason(resp if isinstance(resp, dict) else None)
    engine._append_ledger_event(
        event_type="ORDER_SUBMIT_ACCEPTED" if ok else "ORDER_SUBMIT_REJECTED",
        code=cf.code,
        market=cf.market,
        mode=cf.mode,
        side="BUY",
        qty=cf.planned_qty,
        price=float(cap or 0.0),
        client_order_key=effective_client_order_key,
        ok=ok,
        reasons=[reason_code],
        stage="PB1-CLOSE",
        payload_json={"entry_meta": entry_meta, "entry_exit_plan": entry_exit_plan_dict, "trace_id": entry_meta.get("trace_id"), "rt_cd": rt_cd, "msg_cd": msg_cd, "msg1": msg1},
    )
    logger.info(
        "[PB1][ORDER][RESULT] side=BUY code=%s ok=%s reason=%s rt_cd=%s msg_cd=%s msg1=%s",
        display_code,
        int(ok),
        reason_code,
        rt_cd,
        msg_cd,
        msg1,
    )
    logger.info(
        "[TRADE][ORDER][BUY] code=%s name=%s oid=%s qty=%s price=%.2f result=%s",
        cf.code,
        str(engine._name_for_code(cf.code) or cf.features.get("name") or cf.code),
        kis_odno or order_id,
        cf.planned_qty,
        float(cap or 0.0),
        "ACCEPTED" if ok else "REJECTED",
    )
    if ok:
        # [2026-05-21] KIS 주문 성공 후 DB ACK 소프트 실패 처리
        status["accepted"] = 1
        status["submit_terminal_status"] = "ACCEPTED_PENDING_FILL"
        try:
            engine.orders_repo.mark_acked(engine.env, kis_odno, resp, entry_meta_json=entry_meta)
            logger.info("[ORDER][DB_ACK][OK][CLOSE_ENTRY] code=%s kis_odno=%s", cf.code, kis_odno)
        except Exception as _ack_exc:
            _soft_ack = os.getenv("PB1_ORDER_DB_ACK_FAIL_SOFT", "1") == "1"
            logger.warning(
                "[ORDER][DB_ACK][TIMEOUT][CLOSE_ENTRY] code=%s kis_odno=%s err=%s",
                cf.code,
                kis_odno,
                _ack_exc,
            )
            status["db_ack_timeout"] = 1
            status["submit_terminal_status"] = "ACK_PENDING_RECONCILE"
            if not _soft_ack:
                raise
        engine._append_close_entry_record(
            {
                "order_id": order_id,
                "code": cf.code,
                "qty": cf.planned_qty,
                "cap_price": cap,
                "client_order_key": effective_client_order_key,
                "kis_odno": kis_odno,
                "created_at": now_kst().isoformat(),
            }
        )
        status["terminal_event"] = "API_RESULT"
    else:
        engine.orders_repo.mark_error(engine.env, effective_client_order_key or "", resp if isinstance(resp, dict) else {"resp": resp})
        status["rejected"] = 1
        status["failed"] = int(status.get("failed", 0) or 0) + 1
        status["submit_terminal_status"] = engine._classify_submit_terminal_status(
            api_submitted=int(status.get("api_submitted", 0) or 0),
            accepted=int(status.get("accepted", 0) or 0),
            skipped_reason=str(status.get("skipped_reason") or ""),
            response=resp if isinstance(resp, dict) else None,
        )
        status["terminal_event"] = "API_RESULT"
    return status
