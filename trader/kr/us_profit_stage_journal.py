"""Durable KR PB1 TP stage journal: account/epoch/cycle fenced and fail-closed.

Pure persistence boundary: no broker I/O and NO PERMISSION TO SUBMIT orders.
An accepted KIS ACK is NOT a fill. A completed stage requires cumulative
broker fill evidence and exact original order identity.
"""
from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from trader.db.schema import schema_for_engine
from trader.kr.us_standard_profit_parity import STAGES, verified_contract

STAGE_VERSION = "kr_pb1_tp_durable_stage_v1"
# The US V3 runner uses two independently claimable native SELL stages.
# RUNNER_FINAL may legitimately fire without RUNNER_PARTIAL; both require TP3.
TP_STAGE_NAMES = tuple(s[0] for s in STAGES)
RUNNER_STAGE_NAMES = ("PROFIT_DEFENSE_PARTIAL", "RUNNER_FINAL")
STAGE_NAMES = TP_STAGE_NAMES + RUNNER_STAGE_NAMES
TRANSIENT = {"PENDING", "ACK", "PARTIAL_FILLED", "UNRESOLVED"}


@dataclass(frozen=True)
class StageJournalResult:
    accepted: bool
    status: str
    reason: str
    # The KR execution router must separately prove fresh KIS price, qty,
    # owner order claim and submit deadline. Journal success is never enough.
    order_authorized: bool = False


def _key(contract: Mapping[str, Any], stage: str) -> str:
    identity = {
        "journal": STAGE_VERSION,
        "account_sha256": contract["account_sha256"],
        "trading_epoch_id": contract["trading_epoch_id"],
        "position_cycle_id": contract["position_cycle_id"],
        "strategy_owner": contract["strategy_owner"],
        "symbol": contract["code"],
        "contract_sha256": contract["sha256"],
        "stage": stage,
    }
    return "KR_PB1_TP:" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def _exact_qty(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean shares")
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        return int(value)
    raise ValueError("unverified shares")


def _assert_scope(
    *, contract: Mapping[str, Any], account_id: str,
    trading_epoch_id: str, position_cycle_id: str, code: str,
) -> None:
    if not verified_contract(contract):
        raise ValueError("INVALID_FROZEN_PROFIT_CONTRACT")
    if (
        hashlib.sha256(str(account_id).encode()).hexdigest() != contract["account_sha256"]
        or str(trading_epoch_id) != contract["trading_epoch_id"]
        or str(position_cycle_id) != contract["position_cycle_id"]
        or str(code).zfill(6) != contract["code"]
    ):
        raise ValueError("CROSS_OWNER_OR_LIFECYCLE_SCOPE")


def load_frozen_stage_states(
    *, engine: Any, contract: Mapping[str, Any], account_id: str,
    trading_epoch_id: str, position_cycle_id: str, code: str,
) -> dict[str, dict[str, Any]]:
    """Return only exact durable stage states. DB errors must propagate."""
    _assert_scope(
        contract=contract, account_id=account_id,
        trading_epoch_id=trading_epoch_id, position_cycle_id=position_cycle_id,
        code=code,
    )
    table = schema_for_engine(engine).job_checkpoints
    result: dict[str, dict[str, Any]] = {}
    with engine.connect() as conn:
        for name in STAGE_NAMES:
            raw = conn.execute(
                sa.select(table.c.payload).where(table.c.job_key == _key(contract, name))
            ).scalar_one_or_none()
            if raw is None:
                result[name] = {"status": "NOT_TRIGGERED"}
                continue
            payload = dict(raw)
            if (
                payload.get("version") != STAGE_VERSION
                or payload.get("contract_sha256") != contract["sha256"]
                or payload.get("stage") != name
            ):
                raise RuntimeError("KR_TP_DURABLE_STAGE_INTEGRITY_INVALID")
            result[name] = payload
    return result


def reserve_tp_stage(
    *, engine: Any, contract: Mapping[str, Any], account_id: str,
    trading_epoch_id: str, position_cycle_id: str, code: str,
    stage: str, target_qty: int, client_order_key: str, trade_date: date,
) -> StageJournalResult:
    """Atomic claim of one stage after KIS-verified predecessor fills.

    Only callable after the router has checked submission price/stock, account,
    current open SELLs, and KIS claim safety. This never calls KIS or allows
    any order by itself.
    """
    try:
        _assert_scope(
            contract=contract, account_id=account_id,
            trading_epoch_id=trading_epoch_id,
            position_cycle_id=position_cycle_id, code=code,
        )
        quantity = _exact_qty(target_qty)
        if quantity == 0 or stage not in STAGE_NAMES or not client_order_key:
            raise ValueError("INVALID_STAGE_OR_ORDER_IDENTITY")
        if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
            raise ValueError("INVALID_TRADE_DATE")
        if engine.dialect.name != "postgresql":
            raise ValueError("POSTGRES_DURABLE_STAGE_REQUIRED")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        return StageJournalResult(False, "BLOCK", str(exc))
    stage_ix = STAGE_NAMES.index(stage)
    table = schema_for_engine(engine).job_checkpoints
    key = _key(contract, stage)
    try:
        with engine.begin() as conn:
            # FOR UPDATE cannot lock a row that has not been inserted yet.
            # Serialize EVERY TP/runner stage on the same account/epoch/cycle
            # contract-scoped PostgreSQL transaction advisory lock. Otherwise
            # runner partial and final can simultaneously claim absent rows.
            lock_material = (
                STAGE_VERSION + "|" + contract["account_sha256"] + "|"
                + contract["trading_epoch_id"] + "|"
                + contract["position_cycle_id"] + "|"
                + contract["sha256"]
            )
            lock_key = int.from_bytes(
                hashlib.sha256(lock_material.encode("utf-8")).digest()[:8],
                byteorder="big", signed=True,
            )
            locked = conn.execute(
                sa.text("SELECT pg_try_advisory_xact_lock(:lock_id)"),
                {"lock_id": lock_key},
            ).scalar_one()
            if locked is not True:
                return StageJournalResult(False, "BLOCK", "KR_TP_JOURNAL_CONCURRENT_CLAIM")
            # The predecessor rows are now stable within this contract.
            for prior in TP_STAGE_NAMES[:min(stage_ix, len(TP_STAGE_NAMES))]:
                value = conn.execute(
                    sa.select(table.c.payload).where(table.c.job_key == _key(contract, prior))
                    .with_for_update()
                ).scalar_one_or_none()
                state = dict(value or {})
                if (
                    state.get("status") != "FILLED"
                    or state.get("broker_fill_proven") is not True
                    or not str(state.get("broker_odno") or "")
                    or _exact_qty(state.get("cumulative_filled_qty") or 0)
                        != _exact_qty(state.get("target_qty") or -1)
                    or _exact_qty(state.get("target_qty") or 0) < 1
                ):
                    return StageJournalResult(False, "BLOCK", f"{prior}_NOT_BROKER_FILLED")
            if stage == "PROFIT_DEFENSE_PARTIAL":
                # Final may be claimed directly after TP3: no late partial.
                final_raw = conn.execute(
                    sa.select(table.c.payload).where(
                        table.c.job_key == _key(contract, "RUNNER_FINAL")
                    ).with_for_update()
                ).scalar_one_or_none()
                if final_raw is not None:
                    return StageJournalResult(False, "BLOCK", "RUNNER_FINAL_ALREADY_CLAIMED")
            if stage == "RUNNER_FINAL":
                # A pending partial has reserved shares; a final liquidation
                # must never submit for the full holding until the partial
                # has a terminal, broker-proven fill.
                partial_raw = conn.execute(
                    sa.select(table.c.payload).where(
                        table.c.job_key == _key(contract, "PROFIT_DEFENSE_PARTIAL")
                    ).with_for_update()
                ).scalar_one_or_none()
                partial = dict(partial_raw or {})
                if partial:
                    # Never infer broker finality merely from a FILLED label:
                    # the durable quantity proof must exactly match the
                    # claimed target, using strict integer shares.
                    try:
                        p_target = _exact_qty(partial.get("target_qty"))
                        p_filled = _exact_qty(partial.get("cumulative_filled_qty"))
                    except (ValueError, TypeError):
                        return StageJournalResult(
                            False, "BLOCK", "RUNNER_PARTIAL_FILL_INTEGRITY_INVALID",
                        )
                    if (
                        partial.get("status") != "FILLED"
                        or partial.get("broker_fill_proven") is not True
                        or not str(partial.get("broker_odno") or "").strip()
                        or p_target <= 0 or p_filled != p_target
                    ):
                        return StageJournalResult(
                            False, "BLOCK", "RUNNER_PARTIAL_UNSETTLED",
                        )
            conn.execute(pg_insert(table).values(
                job_key=key, updated_ts=datetime.now(timezone.utc),
                payload={
                    "version": STAGE_VERSION, "status": "PENDING",
                    "stage": stage, "contract_sha256": contract["sha256"],
                    "target_qty": quantity, "cumulative_filled_qty": 0,
                    "client_order_key": client_order_key,
                    "trade_date": trade_date.isoformat(),
                },
            ).on_conflict_do_nothing(index_elements=["job_key"]))
            state = dict(conn.execute(
                sa.select(table.c.payload).where(table.c.job_key == key)
                .with_for_update()
            ).scalar_one())
            if (
                state.get("version") != STAGE_VERSION
                or state.get("stage") != stage
                or state.get("contract_sha256") != contract["sha256"]
            ):
                return StageJournalResult(False, "BLOCK", "STAGE_CONTRACT_CONFLICT")
            if state.get("client_order_key") != client_order_key:
                return StageJournalResult(False, "BLOCK", "DUPLICATE_OR_OTHER_CLIENT_ORDER_KEY")
            if state.get("status") != "PENDING":
                return StageJournalResult(False, "BLOCK", "STAGE_ALREADY_ACTIVE_OR_COMPLETE")
            if state.get("target_qty") != quantity:
                return StageJournalResult(False, "BLOCK", "STAGE_TARGET_MISMATCH")
            # Existing PENDING is NOT permission for a retry after WSL crash.
            if state.get("owner_claimed_once") is True:
                return StageJournalResult(False, "BLOCK", "STAGE_INTENT_ALREADY_CLAIMED")
            state["owner_claimed_once"] = True
            conn.execute(sa.update(table).where(table.c.job_key == key).values(
                updated_ts=datetime.now(timezone.utc), payload=state,
            ))
        return StageJournalResult(True, "PENDING", "DURABLE_CLAIM_SAVED")
    except Exception as exc:
        # Missing DB proof never authorizes a broker order.
        return StageJournalResult(False, "BLOCK", f"DURABLE_JOURNAL_UNAVAILABLE:{type(exc).__name__}")


def observe_broker_stage(
    *, engine: Any, contract: Mapping[str, Any], account_id: str,
    trading_epoch_id: str, position_cycle_id: str, code: str,
    stage: str, client_order_key: str, broker_odno: str,
    cumulative_filled_qty: int, broker_order_accepted: bool,
    broker_fill_source: str, source_trade_date: date,
    _conn: Any = None,
) -> StageJournalResult:
    """Idempotent cumulative KIS fill update for an exact claimed TP order.

    Caller must independently match order date, original KIS ODNO, lifecycle,
    unit-quantity and timestamp against a read-only KIS transaction. Only an
    authoritative KIS daily-ccld observation can ever mark a stage FILLED.
    """
    try:
        _assert_scope(
            contract=contract, account_id=account_id,
            trading_epoch_id=trading_epoch_id,
            position_cycle_id=position_cycle_id, code=code,
        )
        filled = _exact_qty(cumulative_filled_qty)
        if (
            stage not in STAGE_NAMES or not str(client_order_key).strip()
            or not str(broker_odno).strip() or not isinstance(source_trade_date, date)
            or not isinstance(broker_order_accepted, bool)
            or broker_fill_source != "KIS_DAILY_CCLD_VERIFIED"
        ):
            raise ValueError("KIS_ORIGINAL_ORDER_FILL_PROOF_REQUIRED")
    except (ValueError, TypeError, KeyError) as exc:
        return StageJournalResult(False, "BLOCK", str(exc))
    table = schema_for_engine(engine).job_checkpoints
    try:
        with (nullcontext(_conn) if _conn is not None else engine.begin()) as conn:
            key = _key(contract, stage)
            raw = conn.execute(
                sa.select(table.c.payload).where(table.c.job_key == key)
                .with_for_update()
            ).scalar_one_or_none()
            payload = dict(raw or {})
            if (
                payload.get("version") != STAGE_VERSION
                or payload.get("contract_sha256") != contract["sha256"]
                or payload.get("client_order_key") != client_order_key
                or payload.get("trade_date") != source_trade_date.isoformat()
                or payload.get("status") not in {"PENDING", "ACK", "PARTIAL_FILLED", "FILLED"}
                or (
                    payload.get("broker_odno")
                    and str(payload["broker_odno"]) != str(broker_odno)
                )
            ):
                return StageJournalResult(False, "BLOCK", "ORDER_OR_TRADE_DATE_MISMATCH")
            target = _exact_qty(payload.get("target_qty"))
            prev = _exact_qty(payload.get("cumulative_filled_qty"))
            if filled > target or filled < prev:
                return StageJournalResult(False, "BLOCK", "CUMULATIVE_FILL_NON_MONOTONIC")
            if not broker_order_accepted:
                return StageJournalResult(False, "BLOCK", "BROKER_ACCEPTANCE_UNVERIFIED")
            payload["broker_odno"] = str(broker_odno)
            payload["cumulative_filled_qty"] = filled
            payload["broker_fill_proven"] = filled == target and target > 0
            payload["status"] = (
                "FILLED" if filled == target else
                "PARTIAL_FILLED" if filled > 0 else "ACK"
            )
            conn.execute(sa.update(table).where(table.c.job_key == key).values(
                updated_ts=datetime.now(timezone.utc), payload=payload,
            ))
        return StageJournalResult(True, payload["status"], "KIS_CUMULATIVE_PROOF_STORED")
    except Exception as exc:
        return StageJournalResult(False, "BLOCK", f"DURABLE_FILL_UPDATE_FAILED:{type(exc).__name__}")
