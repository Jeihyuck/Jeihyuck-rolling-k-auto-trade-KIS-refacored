"""One-time, broker-proven amendment for the *existing* Samsung E&A PB1 SWING.

Not a migration, not a retrofit of arbitrary existing holdings, and NOT a
broker order. A signed overlay may be persisted only after independent current
KIS balance + original KIS trade, and exact current-cycle DB proof all match.
The frozen v1 entry policy / original BUY contract are never rewritten.
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import date, datetime, timezone
from typing import Any, Mapping

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from trader.account_state import get_account_key
from trader.db.repos import active_trading_epoch_id
from trader.db.schema import schema_for_engine
from trader.kr.us_standard_profit_parity import (
    build_new_swing_parity_contract, verified_contract,
)

LOG = logging.getLogger(__name__)
VERSION = "kr_pb1_position_amendment_v1"
TARGET = {
    "code": "028050",  # Samsung E&A, NOT 122630 KR_INFINITE.
    "strategy": "pb1_pullback_close",
    "epoch": "09772c87-27ab-412d-b1c7-0a4a55196462",
    "cycle": "2e2c9785-62eb-4e9a-8381-be215cfab642",
    "original_buy_odno": "0000011628",
    "original_buy_trade_date": "20261106",
    "original_entry_contract_sha256":
        "2ce9e09fea089a023d418ea5b5418796a335fada1a4d7fcff4e3bbc1de032c88",
    "qty": 33,
    "avg_price": 60200.0,
}
_ATTEMPTED: dict[tuple[str, str], float] = {}


def _exact_positive_int(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("BOOLEAN_QUANTITY")
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdecimal() and int(value) > 0:
        return int(value)
    raise ValueError("BROKER_QUANTITY_INVALID")


def _num(value: Any) -> float:
    import math
    value = float(value or 0)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("PRICE_NOT_POSITIVE_FINITE")
    return value


def _same(a: Any, b: Any) -> bool:
    return str(a or "").lstrip("0") == str(b or "").lstrip("0")


def _json(value: Any) -> dict:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        import json
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def _broker_proof(kis: Any) -> dict:
    """Reject empty/error/stub/paginated API results; never infer KIS truth from DB."""
    balances = kis.get_balance(force=True)
    if (
        not isinstance(balances, dict)
        or balances.get("_diag_stub")
        or str(balances.get("rt_cd", "0")) != "0"
        or not isinstance(balances.get("output1"), list)
    ):
        raise ValueError("KIS_CURRENT_BALANCE_UNVERIFIED")
    holdings = [
        item for item in balances["output1"]
        if isinstance(item, dict)
        and str(item.get("pdno") or "").lstrip("A").zfill(6) == TARGET["code"]
    ]
    if len(holdings) != 1:
        raise ValueError("KIS_SAMSUNG_HOLDING_NOT_UNIQUE")
    row = holdings[0]
    qty = _exact_positive_int(row.get("hldg_qty"))
    orderable_qty = _exact_positive_int(row.get("ord_psbl_qty"))
    avg = _num(row.get("pchs_avg_pric"))
    if (
        qty != TARGET["qty"] or orderable_qty != qty
        or abs(avg - TARGET["avg_price"]) > 0.01
    ):
        raise ValueError("KIS_33_SHARE_AVG_ORDERABLE_MISMATCH")

    today = datetime.now(timezone.utc).astimezone(
        __import__("zoneinfo").ZoneInfo("Asia/Seoul")
    ).strftime("%Y%m%d")
    if today < TARGET["original_buy_trade_date"]:
        raise ValueError("KIS_ORIGINAL_TRADE_IN_FUTURE")
    trades = kis.inquire_daily_ccld(
        start_date=TARGET["original_buy_trade_date"], end_date=today,
    )
    if (
        not isinstance(trades, dict)
        or str(trades.get("rt_cd", "-1")) != "0"
        or trades.get("_diag_stub")
        or trades.get("_ccld_status")
        or not isinstance(trades.get("output1"), list)
        or str(trades.get("ctx_area_fk100") or "").strip()
        or str(trades.get("ctx_area_nk100") or "").strip()
        or str(trades.get("CTX_AREA_FK100") or "").strip()
        or str(trades.get("CTX_AREA_NK100") or "").strip()
        or str((trades.get("_response_meta") or {}).get("tr_cont") or "").upper() in {"F", "M"}
    ):
        raise ValueError("KIS_ORIGINAL_DAILY_CCLD_UNVERIFIED_OR_PAGINATED")
    same_code = [
        x for x in trades["output1"]
        if isinstance(x, dict)
        and str(x.get("pdno") or "").lstrip("A").zfill(6) == TARGET["code"]
        and str(x.get("ord_dt") or "") >= TARGET["original_buy_trade_date"]
    ]
    bought = [
        x for x in same_code
        if str(x.get("ord_dt") or "") == TARGET["original_buy_trade_date"]
        and _same(x.get("odno"), TARGET["original_buy_odno"])
        and str(x.get("sll_buy_dvsn_cd") or "") == "02"
        and str(x.get("cncl_yn") or "N").upper() == "N"
    ]
    if len(bought) != 1:
        raise ValueError("KIS_ORIGINAL_BUY_ODNO_NOT_PROVEN")
    original = bought[0]
    if (
        _exact_positive_int(original.get("tot_ccld_qty")) != TARGET["qty"]
        or abs(_num(original.get("avg_prvs")) - TARGET["avg_price"]) > 0.01
    ):
        raise ValueError("KIS_ORIGINAL_BUY_FILL_MISMATCH")
    if len(same_code) != 1:
        # No SELL, outstanding ACK, unrelated extra BUY, cancellation or
        # turnover after the original 33-share BUY may be guessed away.
        raise ValueError("KIS_POST_BUY_ORDERS_REQUIRE_MANUAL_RECONCILIATION")
    return {
        "version": VERSION,
        "source": "KIS_DAILY_CCLD_AND_FRESH_BALANCE",
        "original_buy_odno": TARGET["original_buy_odno"],
        "original_buy_trade_date": date.fromisoformat("2026-11-06").isoformat(),
        "original_entry_contract_sha256": TARGET["original_entry_contract_sha256"],
        "verified_qty": qty,
        "verified_avg_price": avg,
    }


def _receipt_key(contract: Mapping[str, Any]) -> str:
    return "KR_PB1_AMENDMENT:" + contract["sha256"]


def verified_amendment_readback(*, engine: Any, contract: Mapping[str, Any]) -> bool:
    """Every existing-position SELL re-reads the authoritative immutable receipt."""
    if not verified_contract(contract) or contract.get("position_action") != "EXPLICIT_POSITION_AMENDMENT":
        return False
    if (
        contract.get("code") != TARGET["code"]
        or contract.get("trading_epoch_id") != TARGET["epoch"]
        or contract.get("position_cycle_id") != TARGET["cycle"]
        or contract.get("amendment_proof", {}).get("original_buy_odno") != TARGET["original_buy_odno"]
    ):
        return False
    s = schema_for_engine(engine)
    with engine.connect() as conn:
        row = conn.execute(sa.select(s.positions.c.position_meta).where(sa.and_(
            s.positions.c.env == "practice",
            s.positions.c.strategy == TARGET["strategy"],
            s.positions.c.code == TARGET["code"],
            s.positions.c.position_cycle_id == TARGET["cycle"],
            s.positions.c.trading_epoch_id == TARGET["epoch"],
            s.positions.c.status == "OPEN",
        ))).scalar_one_or_none()
        receipt = conn.execute(sa.select(s.job_checkpoints.c.payload).where(
            s.job_checkpoints.c.job_key == _receipt_key(contract)
        )).scalar_one_or_none()
    meta = _json(row)
    audit = _json(meta.get("kr_us_profit_amendment"))
    return (
        meta.get("kr_us_profit_contract") == dict(contract)
        and audit.get("version") == VERSION
        and audit.get("contract_sha256") == contract["sha256"]
        and audit.get("original_v1_policy_sha256") == TARGET["original_entry_contract_sha256"]
        and audit.get("status") == "ACTIVATED_WITH_KIS_PROOF"
        and _json(receipt).get("contract_sha256") == contract["sha256"]
        and _json(receipt).get("status") == "ACTIVATED_WITH_KIS_PROOF"
    )


def maybe_amend_samsung_ea_position(
    *, engine: Any, kis: Any, env: str, position_cycle_id: str,
) -> dict | None:
    """One-time same-cycle activation only when all KIS and DB proofs exist.

    No SELL/BUY ever occurs in this function. Missing proof preserves original
    v1 policy; the PB1 exit engine may still independently sell for hard risk.
    """
    if (
        os.getenv("KR_SAMSUNG_EA_AMENDMENT_ENABLED", "1").strip() != "1"
        or str(env).lower() != "practice"
        or str(getattr(kis, "env", "")).lower() != "practice"
        or str(position_cycle_id or "") != TARGET["cycle"]
        or os.getenv("DRY_RUN", "0").strip() == "1"
    ):
        return None
    attempt_scope = (TARGET["epoch"], TARGET["cycle"])
    # Transient KIS/DB failure must not suppress this user-approved
    # amendment for the lifetime of an AM worker. Bound retries to <= one
    # verification per 10 minutes; never query KIS on every trading tick.
    now_monotonic = time.monotonic()
    if now_monotonic < _ATTEMPTED.get(attempt_scope, -1.0):
        return None
    _ATTEMPTED[attempt_scope] = now_monotonic + 600.0
    if engine.dialect.name != "postgresql":
        raise ValueError("KR_AMENDMENT_POSTGRES_DURABLE_REQUIRED")

    account_id = get_account_key(env=env, kis=kis)
    epoch = active_trading_epoch_id(engine, env=env, account_id=account_id, required=True)
    if str(epoch or "") != TARGET["epoch"]:
        raise ValueError("KR_AMENDMENT_ACTIVE_EPOCH_MISMATCH")
    evidence = _broker_proof(kis)
    contract = build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="EXPLICIT_POSITION_AMENDMENT",
        account_id=str(account_id), trading_epoch_id=TARGET["epoch"],
        position_cycle_id=TARGET["cycle"], runner_version="US_V3",
        code=TARGET["code"], amendment_proof=evidence,
    )
    if not verified_contract(contract) or contract.get("activation_mode") != "ACTIVE_PRACTICE":
        raise ValueError("KR_AMENDMENT_ACTIVE_CONTRACT_NOT_APPROVED")

    s = schema_for_engine(engine)
    with engine.begin() as conn:
        row = conn.execute(sa.select(s.positions).where(sa.and_(
            s.positions.c.env == "practice",
            s.positions.c.strategy == TARGET["strategy"],
            s.positions.c.code == TARGET["code"],
            s.positions.c.position_cycle_id == TARGET["cycle"],
            s.positions.c.trading_epoch_id == TARGET["epoch"],
            s.positions.c.status == "OPEN",
        )).with_for_update()).mappings().one_or_none()
        if row is None:
            raise ValueError("KR_AMENDMENT_CURRENT_OPEN_POSITION_MISSING")
        existing = _json(row.get("position_meta"))
        entry = _json(row.get("entry_meta_json"))
        plan = _json(row.get("entry_exit_plan_json"))
        if (
            row.get("qty") != TARGET["qty"]
            or abs(_num(row.get("avg_buy_price")) - TARGET["avg_price"]) > 0.01
            or row.get("exit_policy_family") != "SWING_STAGED_EXIT"
            or row.get("trade_horizon") != "SWING"
            or plan.get("policy_version") != "pb1_entry_exit_plan_v1"
            or entry.get("entry_contract_sha256") != TARGET["original_entry_contract_sha256"]
            or existing.get("kr_us_profit_contract") is not None
            or existing.get("kr_us_profit_amendment") is not None
        ):
            raise ValueError("KR_AMENDMENT_OLD_POLICY_OR_HOLDING_MISMATCH")
        original_orders = conn.execute(sa.select(s.orders).where(sa.and_(
            s.orders.c.env == "practice",
            s.orders.c.strategy == TARGET["strategy"],
            s.orders.c.code == TARGET["code"],
            s.orders.c.position_cycle_id == TARGET["cycle"],
            s.orders.c.trading_epoch_id == TARGET["epoch"],
        ))).mappings().all()
        if (
            len(original_orders) != 1
            or original_orders[0]["side"] != "BUY"
            or original_orders[0]["status"] not in {"FILLED", "RECONCILED"}
            or original_orders[0]["qty"] != TARGET["qty"]
            or not _same(original_orders[0].get("kis_odno"), TARGET["original_buy_odno"])
        ):
            raise ValueError("KR_AMENDMENT_DB_BUY_OR_SELL_ORDER_CONFLICT")
        matched_fills = conn.execute(sa.select(s.fills.c.qty, s.fills.c.price).where(sa.and_(
            s.fills.c.env == "practice",
            s.fills.c.code == TARGET["code"],
            s.fills.c.side == "BUY",
            s.fills.c.position_cycle_id == TARGET["cycle"],
            s.fills.c.trading_epoch_id == TARGET["epoch"],
            s.fills.c.kis_odno == TARGET["original_buy_odno"],
        ))).all()
        if (
            sum(x.qty for x in matched_fills) != TARGET["qty"]
            or not matched_fills
            or any(abs(_num(x.price) - TARGET["avg_price"]) > 0.01 for x in matched_fills)
        ):
            raise ValueError("KR_AMENDMENT_DB_ORIGINAL_FILL_MISMATCH")
        audit = {
            "version": VERSION,
            "status": "ACTIVATED_WITH_KIS_PROOF",
            "contract_sha256": contract["sha256"],
            "original_v1_policy_sha256": TARGET["original_entry_contract_sha256"],
            "original_v1_policy_version": "pb1_entry_exit_plan_v1",
            "source": evidence["source"],
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }
        payload = {
            "version": VERSION, "status": audit["status"],
            "contract_sha256": contract["sha256"],
            "original_entry_contract_sha256": TARGET["original_entry_contract_sha256"],
            "epoch": TARGET["epoch"], "cycle": TARGET["cycle"],
        }
        conn.execute(pg_insert(s.job_checkpoints).values(
            job_key=_receipt_key(contract),
            updated_ts=datetime.now(timezone.utc),
            payload=payload,
        ).on_conflict_do_nothing(index_elements=["job_key"]))
        receipt = conn.execute(sa.select(s.job_checkpoints.c.payload).where(
            s.job_checkpoints.c.job_key == _receipt_key(contract)
        ).with_for_update()).scalar_one_or_none()
        if _json(receipt) != payload:
            raise ValueError("KR_AMENDMENT_RECEIPT_CONFLICT")
        conn.execute(sa.update(s.positions).where(
            s.positions.c.position_id == row["position_id"]
        ).values(position_meta={
            **existing, "kr_us_profit_contract": contract,
            "kr_us_profit_amendment": audit,
        }))
        saved = conn.execute(sa.select(s.positions.c.position_meta).where(
            s.positions.c.position_id == row["position_id"]
        )).scalar_one()
        if _json(saved).get("kr_us_profit_contract") != contract:
            raise ValueError("KR_AMENDMENT_ATOMIC_READBACK_MISMATCH")
    if not verified_amendment_readback(engine=engine, contract=contract):
        raise RuntimeError("KR_AMENDMENT_COMMITTED_BUT_READBACK_UNVERIFIED")
    LOG.warning("[KR_PROFIT_PARITY][SAMSUNG_AMENDMENT_ACTIVATED] code=%s cycle=%s sha=%s",
                TARGET["code"], TARGET["cycle"], contract["sha256"])
    return contract
