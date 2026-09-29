"""KR runtime convergence guards for the 2026-09-29 broker-truth incidents.

Implementation-only: no PB1 entry/exit/sizing policy or KR Infinite ownership
is changed. Every repair fails closed and forbids symbol-only policy attachment.
"""
from __future__ import annotations

from datetime import date, datetime
import functools
import json
import logging
import math
import os
import time
from typing import Any, Callable
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import FillsRepo
from trader.db.schema import schema_for_engine
from trader.db.trading_epoch import active_trading_epoch_id, trading_epoch_enforced
from trader.kis_wrapper import KisAPI, KisTemporaryError, is_order_endpoint, kr_tick_remaining_sec
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)
_INSTALLED = False
_KST = ZoneInfo("Asia/Seoul")
_OPEN_STATES = {"SUBMITTED", "ACKED", "ACCEPTED", "PARTIAL_FILLED", "UNRESOLVED_ACK"}
_VALID_TR_CONT = {"F", "M", "D", "E"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return int(default)


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _qty(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _px(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("A") and text[1:].isdigit():
        text = text[1:]
    return text.zfill(6) if text.isdigit() else text


def _kst_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_KST)
    return parsed.astimezone(_KST)


def _active_epoch(engine: Any, env: str) -> str | None:
    enforced = bool(trading_epoch_enforced())
    try:
        return active_trading_epoch_id(
            engine,
            env=env,
            account_id=get_account_key(env=env),
            required=enforced,
        )
    except Exception:
        if enforced:
            raise
        logger.warning("[KR_P0][EPOCH_LOOKUP_FAIL] env=%s enforced=0 action=no_mutation_scope", env)
        return None


def _holdings_index(rows: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for raw in rows or []:
        row = dict(raw or {})
        code = _normalize_code(
            row.get("pdno") or row.get("PDNO") or row.get("code") or row.get("stck_shrn_iscd")
        )
        if not code or code == "000000":
            continue
        result[code] = {
            "qty": _qty(row.get("hldg_qty") or row.get("HLDG_QTY") or row.get("qty")),
            "avg": _px(
                row.get("pchs_avg_pric")
                or row.get("PCHS_AVG_PRIC")
                or row.get("pchs_avg_price")
                or row.get("avg_price")
            ),
        }
    return result


def _order_baseline_qty(order: dict[str, Any]) -> int | None:
    request = _json_dict(order.get("request_json"))
    response = _json_dict(order.get("response_json"))
    execution = _json_dict(response.get("_order_execution"))
    for source in (request, response, execution):
        if "pre_order_holding_qty" in source and source.get("pre_order_holding_qty") is not None:
            return _qty(source.get("pre_order_holding_qty"))
    return None


def _entry_contract(order: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    request = _json_dict(order.get("request_json"))
    meta = _json_dict(request.get("entry_meta")) or _json_dict(order.get("entry_meta_json"))
    plan = _json_dict(request.get("entry_exit_plan"))
    if not plan:
        plan = _json_dict(_json_dict(request.get("entry_plan")).get("entry_exit_plan"))
    return meta, plan


def _explicit_policy(meta: dict[str, Any], plan: dict[str, Any]) -> bool:
    family = str(plan.get("exit_policy_family") or meta.get("exit_policy_family") or "").strip().upper()
    horizon = str(plan.get("trade_horizon") or meta.get("trade_horizon") or "").strip().upper()
    return bool(family and family != "POLICY_MISSING" and horizon)


def _is_buy_order_request(url: str, kwargs: dict[str, Any]) -> bool:
    if not is_order_endpoint(url):
        return False
    headers = kwargs.get("headers") if isinstance(kwargs.get("headers"), dict) else {}
    tr_id = str(headers.get("tr_id") or headers.get("TR_ID") or "").upper()
    return tr_id.endswith(("0012U", "0802U"))


def _build_order_submit_budget_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(self, method: str, url: str, *args: Any, **kwargs: Any):
        if _is_buy_order_request(url, kwargs):
            remaining = kr_tick_remaining_sec(getattr(self, "_kr_stage_deadline", None))
            minimum = max(1.0, _env_float("KR_ORDER_SUBMIT_MIN_REMAINING_SEC", 20.0))
            if math.isfinite(remaining) and remaining < minimum:
                logger.warning(
                    "[KR_P0][BUY_PRE_SUBMIT_DEFER] remaining_sec=%.3f min_required_sec=%.3f action=DO_NOT_CROSS_BROKER_HTTP_BOUNDARY",
                    max(0.0, remaining),
                    minimum,
                )
                raise KisTemporaryError("KR_TICK_DEADLINE_EXHAUSTED_BEFORE_KIS_REQUEST")
        return original(self, method, url, *args, **kwargs)

    return guarded


def _daily_rows(payload: Any) -> list[dict[str, Any]] | None:
    if not isinstance(payload, dict):
        return None
    rows = payload.get("output1") or payload.get("output") or []
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return None
    return [dict(row) for row in rows]


def _daily_payload_base_authoritative(payload: Any) -> bool:
    return bool(
        isinstance(payload, dict)
        and str(payload.get("rt_cd") or "") == "0"
        and not payload.get("_diag_stub")
        and not payload.get("_ccld_status")
        and _daily_rows(payload) is not None
    )


def _pagination_state(payload: dict[str, Any]) -> tuple[str, str, str]:
    next_fk = str(payload.get("ctx_area_fk100") or payload.get("CTX_AREA_FK100") or "").strip()
    next_nk = str(payload.get("ctx_area_nk100") or payload.get("CTX_AREA_NK100") or "").strip()
    meta = payload.get("_response_meta") or {}
    tr_cont = str(meta.get("tr_cont") or "").strip().upper()
    return tr_cont, next_fk, next_nk


def _daily_payload_complete(payload: Any) -> bool:
    if not _daily_payload_base_authoritative(payload):
        return False
    tr_cont, next_fk, next_nk = _pagination_state(payload)
    if tr_cont and tr_cont not in _VALID_TR_CONT:
        return False
    if tr_cont in {"F", "M"}:
        return False
    if tr_cont in {"D", "E"}:
        return True
    return not next_fk and not next_nk


def _daily_ccld_codes(result: dict[str, Any] | None) -> set[str]:
    if not _daily_payload_base_authoritative(result):
        return set()
    codes: set[str] = set()
    for raw in _daily_rows(result) or []:
        code = _normalize_code(
            raw.get("pdno") or raw.get("PDNO") or raw.get("stck_shrn_iscd") or raw.get("code")
        )
        if code and code != "000000":
            codes.add(code)
    return codes


def _build_daily_ccld_capture_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(self, *args: Any, **kwargs: Any):
        result = original(self, *args, **kwargs)
        if _daily_payload_complete(result):
            start = str(kwargs.get("start_date") or (args[0] if args else "") or "")
            end = str(kwargs.get("end_date") or (args[1] if len(args) > 1 else "") or "")
            try:
                self._kr_20260929_daily_ccld_snapshot = {
                    "captured_mono": time.monotonic(),
                    "start_date": start,
                    "end_date": end,
                    "codes": sorted(_daily_ccld_codes(result)),
                    "authoritative_complete": True,
                }
            except Exception:
                pass
        return result

    return guarded


def _fresh_daily_codes(kis: Any, *, trade_date: date) -> set[str] | None:
    snap = getattr(kis, "_kr_20260929_daily_ccld_snapshot", None)
    if not isinstance(snap, dict) or snap.get("authoritative_complete") is not True:
        return None
    try:
        age = time.monotonic() - float(snap.get("captured_mono") or 0.0)
    except Exception:
        return None
    max_age = max(1.0, _env_float("KR_UNRESOLVED_DAILY_CCLD_MAX_AGE_SEC", 120.0))
    if age < 0 or age > max_age:
        return None
    expected = trade_date.strftime("%Y%m%d")
    if str(snap.get("start_date") or "") != expected or str(snap.get("end_date") or "") != expected:
        return None
    return {_normalize_code(code) for code in (snap.get("codes") or [])}


def _daily_ccld_all_pages_for_date(kis: Any, trade_date: date, *, max_pages: int = 20) -> dict[str, Any]:
    day = trade_date.strftime("%Y%m%d")
    fk = ""
    nk = ""
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    last: dict[str, Any] = {}
    for page in range(1, max_pages + 1):
        payload = kis.inquire_daily_ccld(
            start_date=day,
            end_date=day,
            ctx_area_fk100=fk,
            ctx_area_nk100=nk,
        )
        if not _daily_payload_base_authoritative(payload):
            raise RuntimeError("KR_DAILY_CCLD_NOT_AUTHORITATIVE")
        page_rows = _daily_rows(payload)
        if page_rows is None:
            raise RuntimeError("KR_DAILY_CCLD_ROWS_INVALID")
        rows.extend(page_rows)
        last = dict(payload)
        tr_cont, next_fk, next_nk = _pagination_state(payload)
        if tr_cont and tr_cont not in _VALID_TR_CONT:
            raise RuntimeError("KR_DAILY_CCLD_PAGINATION_UNKNOWN")
        if tr_cont in {"D", "E"} or (not tr_cont and not next_fk and not next_nk):
            return {**last, "output1": rows, "_sep29_pages": page, "_sep29_complete": True}
        if not next_fk and not next_nk:
            raise RuntimeError("KR_DAILY_CCLD_CURSOR_MISSING")
        cursor = (next_fk, next_nk)
        if cursor in seen:
            raise RuntimeError("KR_DAILY_CCLD_PAGINATION_STALLED")
        seen.add(cursor)
        fk, nk = cursor
    raise RuntimeError("KR_DAILY_CCLD_PAGINATION_INCOMPLETE")


def _row_side_is_buy(row: dict[str, Any]) -> bool:
    raw = str(
        row.get("sll_buy_dvsn_cd")
        or row.get("side")
        or row.get("sll_buy_dvsn_name")
        or ""
    ).strip().upper()
    if not raw:
        return True
    return raw in {"02", "BUY", "매수"} or "BUY" in raw or "매수" in raw


def _execution_proof_for_order(kis: Any, order: dict[str, Any]) -> dict[str, Any] | None:
    created = _kst_dt(order.get("created_at"))
    odno = str(order.get("kis_odno") or order.get("broker_order_id") or "").strip()
    code = _normalize_code(order.get("code"))
    requested = _qty(order.get("qty"))
    if created is None or not odno or not code or requested <= 0:
        return None
    payload = _daily_ccld_all_pages_for_date(kis, created.date())
    matches = []
    for row in payload.get("output1") or []:
        row_odno = str(row.get("odno") or row.get("ODNO") or row.get("ord_no") or "").strip()
        row_code = _normalize_code(row.get("pdno") or row.get("PDNO") or row.get("code") or row.get("symbol"))
        if row_odno == odno and row_code == code and _row_side_is_buy(row):
            matches.append(dict(row))
    if len(matches) != 1:
        return None
    row = matches[0]
    row_requested = _qty(row.get("ord_qty") or row.get("tot_ord_qty") or row.get("requested_qty"))
    if row_requested > 0 and row_requested != requested:
        return None
    filled_qty = _qty(row.get("tot_ccld_qty") or row.get("ccld_qty") or row.get("filled_qty"))
    avg = _px(row.get("avg_prvs") or row.get("avg_price") or row.get("ccld_unpr"))
    if filled_qty <= 0 or avg <= 0:
        return None
    return {
        "filled_qty": filled_qty,
        "avg_price": avg,
        "filled_notional": filled_qty * avg,
        "trade_date": created.date().isoformat(),
        "odno": odno,
        "row": row,
    }


def _no_durable_fill_or_position(conn: Any, schema: Any, order: dict[str, Any]) -> bool:
    order_id = str(order.get("order_id") or "")
    cycle = str(order.get("position_cycle_id") or "")
    checks = []
    if order_id:
        checks.append(schema.fills.c.order_id == order_id)
    if cycle:
        checks.append(schema.fills.c.position_cycle_id == cycle)
    if checks and conn.execute(sa.select(schema.fills.c.fill_id).where(sa.or_(*checks)).limit(1)).scalar() is not None:
        return False
    if cycle:
        exists = conn.execute(
            sa.select(schema.positions.c.position_id)
            .where(
                sa.and_(
                    schema.positions.c.env == order.get("env"),
                    schema.positions.c.strategy == order.get("strategy"),
                    schema.positions.c.code == order.get("code"),
                    schema.positions.c.position_cycle_id == cycle,
                    schema.positions.c.status == "OPEN",
                    schema.positions.c.qty > 0,
                )
            )
            .limit(1)
        ).scalar()
        if exists is not None:
            return False
    return True


def _terminalize_unresolved_orders(
    *,
    engine: Any,
    kis: Any,
    env: str,
    strategy: str,
    holdings_rows: list[dict[str, Any]] | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = (now or now_kst()).astimezone(_KST)
    today = now.date()
    holdings = _holdings_index(holdings_rows)
    daily_codes = _fresh_daily_codes(kis, trade_date=today)
    schema = schema_for_engine(engine)
    epoch_id = _active_epoch(engine, env)
    min_age = max(0.0, _env_float("KR_UNRESOLVED_NEGATIVE_MIN_AGE_SEC", 30.0))
    required_proofs = max(2, _env_int("KR_UNRESOLVED_NEGATIVE_PROOFS", 2))
    terminalized: list[str] = []
    observed: list[str] = []
    conditions = [
        schema.orders.c.env == env,
        schema.orders.c.strategy == strategy,
        schema.orders.c.status == "UNRESOLVED_ACK",
    ]
    if epoch_id and hasattr(schema.orders.c, "trading_epoch_id"):
        conditions.append(schema.orders.c.trading_epoch_id == epoch_id)
    with engine.begin() as conn:
        orders = [
            dict(row)
            for row in conn.execute(
                sa.select(schema.orders).where(sa.and_(*conditions)).order_by(schema.orders.c.created_at.asc())
            ).mappings().all()
        ]
        for order in orders:
            if str(order.get("side") or "").upper() != "BUY":
                continue
            code = _normalize_code(order.get("code"))
            baseline = _order_baseline_qty(order)
            if baseline is None:
                continue
            broker_qty = int((holdings.get(code) or {}).get("qty") or 0)
            if broker_qty > baseline:
                continue
            if str(order.get("kis_odno") or order.get("broker_order_id") or "").strip():
                continue
            created = _kst_dt(order.get("created_at"))
            if created is None:
                continue
            response = _json_dict(order.get("response_json"))
            if created.date() < today:
                if not _no_durable_fill_or_position(conn, schema, order):
                    continue
                response.update(
                    {
                        "manual_resolution": "BROKER_TRUTH_EXPIRED_DAY_ORDER_NO_HOLDING",
                        "auto_terminalized": True,
                        "auto_terminalized_at": now.isoformat(),
                        "negative_broker_truth": {
                            "source": "NEXT_DAY_EXPIRY_PLUS_FRESH_HOLDING",
                            "broker_qty": broker_qty,
                            "pre_order_holding_qty": baseline,
                        },
                    }
                )
                result = conn.execute(
                    sa.update(schema.orders)
                    .where(
                        sa.and_(
                            schema.orders.c.order_id == order.get("order_id"),
                            schema.orders.c.status == "UNRESOLVED_ACK",
                        )
                    )
                    .values(status="ERROR", response_json=response, updated_at=sa.func.now())
                )
                if int(result.rowcount or 0) == 1:
                    terminalized.append(code)
                continue
            age_sec = max(0.0, (now - created).total_seconds())
            if age_sec < min_age or daily_codes is None or code in daily_codes:
                continue
            proof = _json_dict(response.get("negative_broker_truth"))
            snap = getattr(kis, "_kr_20260929_daily_ccld_snapshot", None)
            daily_token = str((snap or {}).get("captured_mono") or "")
            if daily_token and str(proof.get("last_daily_ccld_token") or "") == daily_token:
                continue
            count = int(proof.get("count") or 0) + 1
            proof.update(
                {
                    "count": count,
                    "last_observed_at": now.isoformat(),
                    "last_daily_ccld_token": daily_token,
                    "source": "FRESH_DAILY_CCLD_ABSENT_PLUS_FRESH_HOLDING",
                    "broker_qty": broker_qty,
                    "pre_order_holding_qty": baseline,
                }
            )
            response["negative_broker_truth"] = proof
            observed.append(code)
            values: dict[str, Any] = {"response_json": response, "updated_at": sa.func.now()}
            if count >= required_proofs:
                response.update(
                    {
                        "manual_resolution": "BROKER_TRUTH_NO_ORDER_OBSERVED",
                        "auto_terminalized": True,
                        "auto_terminalized_at": now.isoformat(),
                    }
                )
                values["status"] = "ERROR"
            result = conn.execute(
                sa.update(schema.orders)
                .where(
                    sa.and_(
                        schema.orders.c.order_id == order.get("order_id"),
                        schema.orders.c.status == "UNRESOLVED_ACK",
                    )
                )
                .values(**values)
            )
            if int(result.rowcount or 0) == 1 and count >= required_proofs:
                terminalized.append(code)
    return {"terminalized": terminalized, "negative_observed": observed}


def _durable_fill_totals(engine: Any, schema: Any, order_id: str) -> tuple[int, float]:
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(schema.fills.c.qty, schema.fills.c.price).where(schema.fills.c.order_id == order_id)
        ).mappings().all()
    qty = sum(_qty(row.get("qty")) for row in rows)
    notional = sum(_qty(row.get("qty")) * _px(row.get("price")) for row in rows)
    return qty, notional


def _persist_missing_fill(
    *,
    engine: Any,
    schema: Any,
    order: dict[str, Any],
    proof: dict[str, Any],
    env: str,
    code: str,
    source_cycle: str,
    source_epoch: str,
    source_odno: str,
    entry_ts: Any,
    now: datetime,
) -> bool:
    order_id = str(order.get("order_id") or "")
    target_qty = _qty(proof.get("filled_qty"))
    target_notional = float(proof.get("filled_notional") or 0.0)
    durable_qty, durable_notional = _durable_fill_totals(engine, schema, order_id)
    if durable_qty > target_qty:
        return False
    remaining = target_qty - durable_qty
    if remaining <= 0:
        return durable_qty == target_qty
    remaining_notional = target_notional - durable_notional
    price = (remaining_notional / remaining) if remaining_notional > 0 else _px(proof.get("avg_price"))
    if price <= 0:
        return False
    try:
        FillsRepo(engine).upsert_fill(
            env=env,
            run_id=None,
            order_id=order_id,
            kis_odno=source_odno,
            trade_id=f"kr-crossday-ccld:{order_id}:{durable_qty}:{target_qty}",
            code=code,
            market=order.get("market") or "KOSPI",
            side="BUY",
            qty=remaining,
            price=price,
            fee=0.0,
            tax=0.0,
            filled_at=entry_ts or now,
            raw_json={
                "source": "KR_CROSS_DAY_EXACT_DAILY_CCLD",
                "broker_execution_trade_date": proof.get("trade_date"),
                "broker_execution_qty": target_qty,
                "broker_execution_avg_price": proof.get("avg_price"),
                "durable_qty_before": durable_qty,
            },
            fill_meta_json={"fill_source": "cross_day_exact_daily_ccld"},
            position_cycle_id=source_cycle,
            portfolio_epoch_id=source_epoch,
        )
    except Exception as exc:
        logger.exception("[KR_P0][CROSS_DAY_FILL_PERSIST_FAIL] code=%s order_id=%s err=%s", code, order_id, exc)
        return False
    durable_qty_after, _ = _durable_fill_totals(engine, schema, order_id)
    return durable_qty_after == target_qty


def _recover_cross_day_contract_from_holdings(
    *,
    engine: Any,
    kis: Any,
    env: str,
    strategy: str,
    holdings_rows: list[dict[str, Any]] | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = (now or now_kst()).astimezone(_KST)
    today = now.date()
    max_age_days = max(1, _env_int("KR_CROSS_DAY_CONTRACT_MAX_AGE_DAYS", 4))
    holdings = _holdings_index(holdings_rows)
    schema = schema_for_engine(engine)
    epoch_id = _active_epoch(engine, env)
    recovered: list[str] = []
    review_required: list[str] = []
    pos_conditions = [
        schema.positions.c.env == env,
        schema.positions.c.strategy == strategy,
        schema.positions.c.status == "OPEN",
        schema.positions.c.qty > 0,
    ]
    if epoch_id and hasattr(schema.positions.c, "trading_epoch_id"):
        pos_conditions.append(schema.positions.c.trading_epoch_id == epoch_id)
    with engine.connect() as conn:
        positions = [
            dict(row)
            for row in conn.execute(sa.select(schema.positions).where(sa.and_(*pos_conditions))).mappings().all()
        ]
    for position in positions:
        if str(position.get("position_origin") or "").upper() not in {"IMPORTED", "RECOVERY"}:
            continue
        family = str(position.get("exit_policy_family") or "").strip().upper()
        if family and family != "POLICY_MISSING":
            continue
        code = _normalize_code(position.get("code"))
        broker = holdings.get(code) or {}
        broker_qty = int(broker.get("qty") or 0)
        if broker_qty <= 0 or int(position.get("qty") or 0) != broker_qty:
            continue
        order_conditions = [
            schema.orders.c.env == env,
            schema.orders.c.strategy == strategy,
            schema.orders.c.code == code,
            schema.orders.c.side == "BUY",
            schema.orders.c.status.in_(["ACKED", "ACCEPTED", "PARTIAL_FILLED"]),
            schema.orders.c.kis_odno.is_not(None),
            schema.orders.c.position_cycle_id.is_not(None),
            schema.orders.c.portfolio_epoch_id.is_not(None),
        ]
        if epoch_id and hasattr(schema.orders.c, "trading_epoch_id"):
            order_conditions.append(schema.orders.c.trading_epoch_id == epoch_id)
        with engine.connect() as conn:
            orders = [
                dict(row)
                for row in conn.execute(
                    sa.select(schema.orders)
                    .where(sa.and_(*order_conditions))
                    .order_by(schema.orders.c.created_at.desc())
                ).mappings().all()
            ]
        candidates: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for order in orders:
            created = _kst_dt(order.get("created_at"))
            if created is None or created.date() >= today or (today - created.date()).days > max_age_days:
                continue
            if _order_baseline_qty(order) != 0 or broker_qty != _qty(order.get("qty")):
                continue
            meta, plan = _entry_contract(order)
            if not _explicit_policy(meta, plan):
                continue
            try:
                proof = _execution_proof_for_order(kis, order)
            except Exception as exc:
                logger.warning("[KR_P0][CROSS_DAY_EXECUTION_PROOF_FAIL] code=%s err=%s", code, exc)
                continue
            if proof is None or _qty(proof.get("filled_qty")) != broker_qty:
                continue
            candidates.append((order, meta, plan, proof))
        if len(candidates) != 1:
            if candidates:
                review_required.append(code)
            continue
        order, meta, plan, proof = candidates[0]
        source_order_id = str(order.get("order_id") or "")
        source_cycle = str(order.get("position_cycle_id") or "")
        source_epoch = str(order.get("portfolio_epoch_id") or "")
        source_odno = str(order.get("kis_odno") or order.get("broker_order_id") or "")
        if not source_order_id or not source_cycle or not source_epoch or not source_odno:
            continue
        risk_plan = _json_dict(plan.get("risk_plan"))
        initial_stop = _px(risk_plan.get("initial_stop") or meta.get("initial_stop_price")) or None
        position_meta = _json_dict(position.get("position_meta"))
        position_meta.update(
            {
                "provenance_verified": True,
                "policy_recovery_source": "prior_day_exact_odno_daily_ccld_plus_fresh_holding",
                "source_buy_order_id": source_order_id,
                "source_buy_kis_odno": source_odno,
                "recovered_from_cycle_id": source_cycle,
                "recovered_from_epoch_id": source_epoch,
                "broker_execution_trade_date": proof.get("trade_date"),
                "broker_execution_qty": proof.get("filled_qty"),
                "holding_age_unknown": False,
            }
        )
        entry_ts = order.get("acked_at") or order.get("submitted_at") or order.get("created_at")
        if not _persist_missing_fill(
            engine=engine,
            schema=schema,
            order=order,
            proof=proof,
            env=env,
            code=code,
            source_cycle=source_cycle,
            source_epoch=source_epoch,
            source_odno=source_odno,
            entry_ts=entry_ts,
            now=now,
        ):
            review_required.append(code)
            continue
        updated_at_type = getattr(schema.positions.c.updated_at, "type", None)
        updated_at_value = now if isinstance(updated_at_type, sa.DateTime) else now.isoformat()
        fields: dict[str, Any] = {
            "position_cycle_id": source_cycle,
            "portfolio_epoch_id": source_epoch,
            "position_origin": "SYSTEM",
            "position_meta": position_meta,
            "entry_ts": entry_ts.isoformat() if hasattr(entry_ts, "isoformat") else str(entry_ts or ""),
            "entry_reason": plan.get("entry_reason") or meta.get("entry_reason"),
            "entry_style_selected": plan.get("entry_style_selected") or meta.get("entry_style_selected"),
            "entry_decision_family": meta.get("entry_decision_family") or order.get("entry_decision_family"),
            "entry_thesis": plan.get("entry_thesis") or meta.get("entry_thesis"),
            "trade_horizon": plan.get("trade_horizon") or meta.get("trade_horizon"),
            "exit_policy_family": plan.get("exit_policy_family") or meta.get("exit_policy_family"),
            "eod_action": plan.get("eod_action") or meta.get("eod_action"),
            "force_eod_close": bool(
                plan.get("force_eod_close")
                if plan.get("force_eod_close") is not None
                else meta.get("force_eod_close") or False
            ),
            "entry_exit_plan_json": plan,
            "entry_meta_json": meta,
            "policy_source": plan.get("policy_source") or meta.get("policy_source") or "recovered_exact_broker_execution",
            "policy_version": plan.get("policy_version") or meta.get("policy_version"),
            "tp1_done": bool(meta.get("tp1_done", False)),
            "tp2_done": bool(meta.get("tp2_done", False)),
            "updated_at": updated_at_value,
        }
        if initial_stop is not None:
            fields["initial_stop_price"] = initial_stop
            fields["stop_price"] = initial_stop
        if order.get("trading_epoch_id") is not None and hasattr(schema.positions.c, "trading_epoch_id"):
            fields["trading_epoch_id"] = order.get("trading_epoch_id")
        with engine.begin() as conn:
            live = conn.execute(
                sa.select(schema.positions.c.position_id, schema.positions.c.exit_policy_family)
                .where(schema.positions.c.position_id == position.get("position_id"))
                .limit(1)
            ).mappings().first()
            if not live:
                continue
            live_family = str(live.get("exit_policy_family") or "").strip().upper()
            if live_family and live_family != "POLICY_MISSING":
                continue
            result = conn.execute(
                sa.update(schema.positions)
                .where(
                    sa.and_(
                        schema.positions.c.position_id == position.get("position_id"),
                        schema.positions.c.status == "OPEN",
                        schema.positions.c.qty == broker_qty,
                    )
                )
                .values(**{key: value for key, value in fields.items() if value is not None})
            )
            if int(result.rowcount or 0) != 1:
                continue
            response = _json_dict(order.get("response_json"))
            response["cross_day_execution_proof"] = {
                "recovered_at": now.isoformat(),
                "broker_qty": broker_qty,
                "pre_order_holding_qty": 0,
                "source": "EXACT_ODNO_DAILY_CCLD_PLUS_FRESH_HOLDING",
                "trade_date": proof.get("trade_date"),
                "filled_qty": proof.get("filled_qty"),
                "avg_price": proof.get("avg_price"),
            }
            conn.execute(
                sa.update(schema.orders)
                .where(schema.orders.c.order_id == source_order_id)
                .values(status="FILLED", response_json=response, updated_at=sa.func.now())
            )
        recovered.append(code)
    return {"recovered": recovered, "review_required": review_required}


def _count_open_activity(*, engine: Any, env: str, strategy: str) -> int:
    schema = schema_for_engine(engine)
    epoch_id = _active_epoch(engine, env)
    conditions = [
        schema.orders.c.env == env,
        schema.orders.c.strategy == strategy,
        schema.orders.c.status.in_(sorted(_OPEN_STATES)),
    ]
    if epoch_id and hasattr(schema.orders.c, "trading_epoch_id"):
        conditions.append(schema.orders.c.trading_epoch_id == epoch_id)
    with engine.connect() as conn:
        return int(
            conn.execute(sa.select(sa.func.count()).select_from(schema.orders).where(sa.and_(*conditions))).scalar()
            or 0
        )


def _extract_final_holdings(result: dict[str, Any], balance_snapshot: Any) -> list[dict[str, Any]] | None:
    rows = result.get("_final_holdings_rows") if isinstance(result, dict) else None
    if isinstance(rows, list):
        return rows
    if isinstance(balance_snapshot, dict):
        raw = balance_snapshot.get("output1") or []
        if isinstance(raw, list):
            return list(raw)
        if isinstance(raw, dict):
            return [raw]
        return []
    return None


def _build_reconcile_convergence_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def guarded(*args: Any, **kwargs: Any):
        result = original(*args, **kwargs)
        if not isinstance(result, dict):
            return result
        engine = kwargs.get("engine")
        kis = kwargs.get("kis")
        env = str(
            kwargs.get("env")
            or os.getenv("STRATEGY_ENV")
            or os.getenv("KIS_ENV")
            or "practice"
        ).lower()
        strategy = str(kwargs.get("strategy") or "pb1_pullback_close")
        if engine is None or kis is None:
            return result
        holdings_rows = _extract_final_holdings(result, kwargs.get("balance_snapshot"))
        if holdings_rows is None or result.get("holdings_error"):
            return result
        try:
            recovery = _recover_cross_day_contract_from_holdings(
                engine=engine,
                kis=kis,
                env=env,
                strategy=strategy,
                holdings_rows=holdings_rows,
            )
        except Exception as exc:
            recovery = {"recovered": [], "review_required": []}
            logger.exception("[KR_P0][CROSS_DAY_POLICY_RECOVERY_FAIL] err=%s", exc)
        try:
            convergence = _terminalize_unresolved_orders(
                engine=engine,
                kis=kis,
                env=env,
                strategy=strategy,
                holdings_rows=holdings_rows,
            )
        except Exception as exc:
            convergence = {"terminalized": [], "negative_observed": []}
            logger.exception("[KR_P0][UNRESOLVED_CONVERGENCE_FAIL] err=%s", exc)
        result["cross_day_policy_recovered"] = list(recovery.get("recovered") or [])
        result["cross_day_policy_review_required"] = list(recovery.get("review_required") or [])
        result["unresolved_terminalized"] = list(convergence.get("terminalized") or [])
        try:
            open_count = _count_open_activity(engine=engine, env=env, strategy=strategy)
            result["unresolved_broker_activity"] = bool(open_count)
            result["open_broker_activity_count_after_convergence"] = open_count
            if not open_count and result.get("guard_reason") == "unresolved_broker_activity":
                result["guard_reason"] = "broker_truth_converged"
        except Exception:
            pass
        return result

    return guarded


def install_kr_20260929_runtime_integrity() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    import trader.reconcile_kis as rk

    if not getattr(KisAPI, "_kr_p0_20260929_order_budget_installed", False):
        KisAPI._safe_request = _build_order_submit_budget_guard(KisAPI._safe_request)
        KisAPI._kr_p0_20260929_order_budget_installed = True
    if not getattr(KisAPI, "_kr_p0_20260929_daily_ccld_capture_installed", False):
        KisAPI.inquire_daily_ccld = _build_daily_ccld_capture_guard(KisAPI.inquire_daily_ccld)
        KisAPI._kr_p0_20260929_daily_ccld_capture_installed = True
    if not getattr(rk, "_kr_p0_20260929_convergence_installed", False):
        rk.reconcile_kis = _build_reconcile_convergence_guard(rk.reconcile_kis)
        rk._kr_p0_20260929_convergence_installed = True
    _INSTALLED = True
    logger.info(
        "[KR_P0][20260929][INSTALLED] buy_pre_submit_budget=1 unresolved_negative_convergence=1 exact_cross_day_execution_recovery=1"
    )
