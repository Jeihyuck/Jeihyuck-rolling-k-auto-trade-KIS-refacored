"""KR full-historical recovery journal; separate from same-day broker reconciliation.

Activation requires KR_HISTORICAL_RECOVERY_ONCE_ENABLED=1 on a KIS practice
runtime. Default OFF. Unknown ownership, DB failure or missing proof never
licenses the current-only fast path for new BUYs.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import uuid4
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from trader.db.schema import schema_for_engine
from trader.kr.recovery_scope_20261104 import resolve_kr_recovery_scope

logger = logging.getLogger(__name__)
VERSION = "kr_historical_recovery_once_v1"


@dataclass(frozen=True)
class RecoveryLane:
    mode: str
    job_key: str | None = None
    fencing_token: str | None = None
    reason: str = ""

    @property
    def historical_recovery(self) -> bool:
        return self.mode in {"FULL", "LEGACY"}

    @property
    def allow_buy_from_recovery(self) -> bool:
        return self.mode in {"FULL", "LEGACY", "CURRENT_ONLY"}


def lane_enabled(env: str) -> bool:
    return (
        str(env).lower() == "practice"
        and os.getenv("KIS_ENV", "").lower() == "practice"
        and os.getenv("KR_HISTORICAL_RECOVERY_ONCE_ENABLED", "0") == "1"
    )


def _key(*, scope: Any, strategy: str, trading_date: date, lane_name: str = "AM") -> str:
    proof = {
        "version": VERSION, "env": scope.env,
        "account_sha256": hashlib.sha256(scope.account_key.encode()).hexdigest(),
        "epoch": scope.trading_epoch_id,
        "strategy": strategy, "trade_date": trading_date.isoformat(),
        "lane_name": lane_name,
    }
    return f"KR_HIST_RECOVERY:{hashlib.sha256(json.dumps(proof, sort_keys=True).encode()).hexdigest()}"


def choose_historical_lane(
    *, engine: Any, kis: Any, env: str,
    strategy: str, trading_date: date, lane_name: str = "AM",
) -> RecoveryLane:
    """Transactional claim via row FOR UPDATE, with a nonce fencing token.

    No stale CHECKING lease is silently reassigned mid-session: on a crashed
    worker we fence new BUY until CLOSE's dedicated broker recovery. This is
    deliberately conservative to avoid duplicate historical economic writes.
    """
    if lane_name not in {"AM", "CLOSE"}:
        return RecoveryLane("CURRENT_ONLY_BLOCK", reason="UNKNOWN_RECOVERY_LANE")
    if not lane_enabled(env):
        return RecoveryLane("LEGACY", reason="NOT_ACTIVATED")
    try:
        scope = resolve_kr_recovery_scope(engine=engine, env=env, kis=kis)
        if scope is None or not scope.account_key or not scope.trading_epoch_id:
            return RecoveryLane("LEGACY", reason="EPOCH_SCOPE_UNPROVEN")
        if engine.dialect.name != "postgresql":
            return RecoveryLane("LEGACY", reason="NO_POSTGRES_ADVISORY_PROOF")
        key = _key(scope=scope, strategy=strategy, trading_date=trading_date, lane_name=lane_name)
        table = schema_for_engine(engine).job_checkpoints
        nonce = str(uuid4())
        with engine.begin() as conn:
            conn.execute(pg_insert(table).values(
                job_key=key,
                updated_ts=datetime.now(timezone.utc),
                payload={"status": "PENDING", "version": VERSION},
            ).on_conflict_do_nothing(index_elements=["job_key"]))
            record = conn.execute(
                sa.select(table.c.payload).where(table.c.job_key == key).with_for_update()
            ).scalar_one()
            data = dict(record or {})
            status = str(data.get("status") or "")
            if data.get("version") != VERSION or status not in {
                "PENDING", "CHECKING", "COMPLETE", "UNRESOLVED",
            }:
                return RecoveryLane("CURRENT_ONLY_BLOCK", key, reason="INVALID_JOURNAL_STATE")
            if status == "COMPLETE":
                if data.get("verified") is True and data.get("broker_reconcile_ok") is True:
                    return RecoveryLane("CURRENT_ONLY", key, reason="FULL_RECOVERY_DURABLY_PROVEN")
                return RecoveryLane("CURRENT_ONLY_BLOCK", key, reason="COMPLETION_PROOF_INVALID")
            if status in {"CHECKING", "UNRESOLVED"}:
                return RecoveryLane("CURRENT_ONLY_BLOCK", key, reason=status)
            # A single claimant moves PENDING -> CHECKING atomically.
            conn.execute(
                sa.update(table).where(table.c.job_key == key).values(
                    updated_ts=datetime.now(timezone.utc),
                    payload={
                        "status": "CHECKING", "version": VERSION,
                        "fencing_token": nonce,
                        "trade_date": trading_date.isoformat(),
                        "lane_name": lane_name,
                        "trading_epoch_id": scope.trading_epoch_id,
                        "account_sha256": hashlib.sha256(scope.account_key.encode()).hexdigest(),
                    },
                )
            )
        return RecoveryLane("FULL", key, nonce, "FIRST_BROKER_RECOVERY")
    except Exception as exc:
        # Error reading/writing journal cannot authorize skipping recovery.
        logger.error(
            "[KR_HIST_RECOVERY][JOURNAL_FAIL] type=%s action=LEGACY_FULL_NO_SKIP",
            type(exc).__name__, exc_info=True,
        )
        return RecoveryLane("LEGACY", reason="JOURNAL_UNAVAILABLE")


def record_historical_result(
    *, engine: Any, lane: RecoveryLane, result: dict | None,
) -> bool:
    """Persist terminal state only for the exact claiming fencing token."""
    if lane.mode != "FULL":
        return lane.mode == "CURRENT_ONLY"
    if not lane.job_key or not lane.fencing_token:
        return False
    success = bool(
        isinstance(result, dict) and result.get("ok") is True
        and not result.get("degraded")
        and result.get("frozen_contract_invariant_violations", 0) == 0
        # A successfully repaired historical position is still a successful
        # FULL recovery. The parity fastpath is an optimization, not the
        # completion criterion: it can be false precisely when work was done.
        and result.get("historical_recovery_executed") is True
        and not result.get("unresolved_broker_activity")
        and not result.get("holdings_error")
        and not result.get("guard_reason")
    )
    try:
        table = schema_for_engine(engine).job_checkpoints
        with engine.begin() as conn:
            original = conn.execute(
                sa.select(table.c.payload)
                .where(table.c.job_key == lane.job_key).with_for_update()
            ).scalar_one_or_none()
            current = dict(original or {})
            if (current.get("status") != "CHECKING"
                    or current.get("fencing_token") != lane.fencing_token):
                logger.error("[KR_HIST_RECOVERY][FENCE_MISMATCH] action=BLOCK_BUY")
                return False
            current.update({
                "status": "COMPLETE" if success else "UNRESOLVED",
                "verified": bool(success), "broker_reconcile_ok": bool(success),
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "failed_reason": (
                    "" if success else str((result or {}).get("degraded")
                                           or (result or {}).get("reason")
                                           or "FULL_RECOVERY_UNVERIFIED")
                ),
            })
            conn.execute(sa.update(table).where(table.c.job_key == lane.job_key).values(
                updated_ts=datetime.now(timezone.utc), payload=current,
            ))
        logger.info("[KR_HIST_RECOVERY][RESULT] state=%s key=%s", current["status"], lane.job_key)
        return success
    except Exception as exc:
        logger.error(
            "[KR_HIST_RECOVERY][WRITE_FAIL] type=%s action=BLOCK_BUY",
            type(exc).__name__, exc_info=True,
        )
        return False
