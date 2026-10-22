"""Fail-closed, read-only broker proof for handing full KR reconciliation after SELL.

This never submits/cancels or writes economic state. It is an optimization
gate, NOT a substitute for durable pre-submit order/claim/balance checks.
Any unrecognized order, partial ledger application, duplicate ID, KIS
pagination error, account/epoch ambiguity or stale holding forces the
unchanged full pre-exit reconciliation.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import math
from typing import Any

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.kr.reconcile_parity_fastpath import historical_repair_unnecessary
from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
from trader.reconcile_kis import _normalize_code, _parse_side, _parse_date_time
from trader.time_utils import now_kst


def prove_safe_protective_sell_handoff(
    *, engine: Any, kis: Any, env: str, strategy: str,
    trade_date: date, balance_snapshot: dict | None,
) -> tuple[bool, str]:
    """Prove existing broker evidence/ledger parity before owner exits."""
    if env != "practice" or strategy != "pb1_pullback_close":
        return False, "SCOPE_NOT_VALIDATED"
    if not isinstance(balance_snapshot, dict):
        return False, "NO_BROKER_BALANCE"
    try:
        seen_at = datetime.fromisoformat(str(balance_snapshot.get("_kr_verified_fresh_at_kst") or ""))
        age = (now_kst() - seen_at).total_seconds()
        if not seen_at.tzinfo or not 0 <= age <= 20:
            return False, "STALE_KIS_BALANCE"
        if not isinstance(balance_snapshot.get("output1"), list):
            return False, "INVALID_KIS_HOLDINGS"
        orders_repo = OrdersRepo(engine)
        if orders_repo.get_open_orders(env, include_stale=True):
            return False, "OPEN_OR_UNRESOLVED_ORDER"
        health = orders_repo.execution_claim_health(market="KR")
        if (not isinstance(health, dict) or health.get("available") is not True
                or any(int(health.get(k) or 0) for k in ("in_flight", "uncertain", "unresolved"))):
            return False, "UNRESOLVED_EXECUTION_CLAIM"
        positions = PositionsRepo(engine).list_active_positions_for_exit(env, strategy)
        parity, reason = historical_repair_unnecessary(
            holdings=balance_snapshot["output1"], positions=positions,
            reconcile_ok=True, holdings_error=None, order_or_fill_changes=0,
            unresolved=False, expected_strategy=strategy,
        )
        if not parity:
            return False, reason
        broker = _daily_ccld_all_pages_for_date(kis, trade_date)
        if broker.get("_sep29_complete") is not True or str(broker.get("rt_cd")) != "0":
            return False, "KIS_DAILY_PROOF_INCOMPLETE"
        rows = broker.get("output1")
        if not isinstance(rows, list):
            return False, "KIS_DAILY_ROWS_INVALID"
        if len(rows) > 300:
            return False, "KIS_DAILY_ORDER_COUNT_TOO_LARGE"
        kst_start = datetime.combine(trade_date, time.min, tzinfo=ZoneInfo("Asia/Seoul"))
        kst_end = kst_start + timedelta(days=1)
        schema = schema_for_engine(engine)
        with engine.connect() as conn:
            stored = conn.execute(sa.select(schema.orders).where(sa.and_(
                schema.orders.c.env == env,
                schema.orders.c.created_at >= kst_start,
                schema.orders.c.created_at < kst_end,
            ))).mappings().all()
            broker_rows: dict[tuple[str, str, str], dict] = {}
            for row in rows:
                if not isinstance(row, dict):
                    return False, "MALFORMED_KIS_ORDER"
                code = _normalize_code(row.get("pdno") or row.get("code"))
                side = _parse_side(row)
                no = str(row.get("odno") or row.get("ODNO") or "").strip()
                if not code or code == "000000" or side not in {"BUY", "SELL"} or not no:
                    return False, "KIS_ORDER_IDENTITY_MISSING"
                if _parse_date_time(row).date() != trade_date:
                    return False, "KIS_ORDER_DATE_MISMATCH"
                key = (code, side, no)
                if key in broker_rows:
                    return False, "BROKER_ORDER_MULTIPLE_ROWS_REQUIRE_RECONCILE"
                broker_rows[key] = row
            db_by_key: dict[tuple[str, str, str], dict] = {}
            # orders intentionally has no account_id column: its durable
            # account/strategy scope is owned by portfolio_epochs. Query in
            # one batch, never trust a missing order field or code-only join.
            portfolio_ids = {str(item.get("portfolio_epoch_id") or "") for item in stored}
            if stored and "" in portfolio_ids:
                return False, "DURABLE_ORDER_PORTFOLIO_EPOCH_MISSING"
            epochs = {}
            if portfolio_ids:
                epoch_rows = conn.execute(sa.select(schema.portfolio_epochs).where(
                    schema.portfolio_epochs.c.portfolio_epoch_id.in_(portfolio_ids)
                )).mappings().all()
                epochs = {str(row["portfolio_epoch_id"]): row for row in epoch_rows}
            scope = get_account_key(env=env, kis=kis)
            for order in stored:
                no = str(order.get("kis_odno") or order.get("broker_order_id") or "").strip()
                epoch = epochs.get(str(order.get("portfolio_epoch_id") or ""))
                if (
                    not no or epoch is None
                    or str(epoch.get("account_id") or "") != scope
                    or str(epoch.get("env") or "") != env
                    or str(epoch.get("strategy") or "") != str(order.get("strategy") or "")
                    or (order.get("trading_epoch_id") and str(order["trading_epoch_id"]) != str(epoch.get("trading_epoch_id") or ""))
                ):
                    return False, "DURABLE_ORDER_ACCOUNT_IDENTITY_MISSING"
                key = (str(order["code"]).zfill(6), str(order["side"]).upper(), no)
                if key in db_by_key:
                    return False, "DURABLE_ORDER_AMBIGUOUS"
                db_by_key[key] = order
            if set(db_by_key) != set(broker_rows):
                return False, "BROKER_DURABLE_ORDER_SET_MISMATCH"
            for key, broker_row in broker_rows.items():
                source = db_by_key[key]
                requested = int(source.get("qty") or 0)
                observed = int(float(broker_row.get("tot_ccld_qty") or 0))
                order_req = int(float(broker_row.get("ord_qty") or requested))
                if requested <= 0 or order_req != requested or observed < 0 or observed > requested:
                    return False, "BROKER_ORDER_QUANTITY_CONFLICT"
                response = source.get("response_json") or {}
                if not isinstance(response, dict):
                    return False, "DURABLE_ORDER_RESPONSE_MALFORMED"
                confirmed = response.get("confirmed_fill_qty")
                if confirmed is None or int(confirmed) != observed:
                    return False, "ORDER_CUMULATIVE_WATERMARK_UNPROVEN"
                fills = conn.execute(sa.select(schema.fills).where(
                    schema.fills.c.order_id == source["order_id"]
                )).mappings().all()
                if sum(int(item.get("qty") or 0) for item in fills) != observed:
                    return False, "ECONOMIC_FILL_QUANTITY_DRIFT"
                if observed:
                    broker_amt = float(broker_row.get("tot_ccld_amt") or 0)
                    durable_amt = float(response.get("confirmed_fill_notional") or 0)
                    if not math.isfinite(broker_amt) or broker_amt <= 0 or not math.isfinite(durable_amt):
                        return False, "BROKER_PRICE_EVIDENCE_UNPROVEN"
                    if abs(broker_amt - durable_amt) > 0.01:
                        return False, "ECONOMIC_NOTIONAL_WATERMARK_DRIFT"
        return True, "EXACT_BROKER_LEDGER_PRETRADE_PROOF"
    except Exception as exc:
        return False, f"PROOF_READ_FAILED_{type(exc).__name__}"
