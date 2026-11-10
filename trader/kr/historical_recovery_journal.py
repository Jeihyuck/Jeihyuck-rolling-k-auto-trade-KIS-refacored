"""KR full-historical recovery journal; separate from same-day broker reconciliation.

Enabled by default on a KIS practice runtime. An explicit
KR_HISTORICAL_RECOVERY_ONCE_ENABLED=0 is the emergency legacy override.
Unknown ownership, DB failure or missing proof never licenses the current-only
fast path for new BUYs.
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
from trader.kr.reconcile_guard_classification import broker_reconcile_guard_blocks

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
        and os.getenv("KR_HISTORICAL_RECOVERY_ONCE_ENABLED", "1") == "1"
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
            return RecoveryLane("CURRENT_ONLY_BLOCK", reason="EPOCH_SCOPE_UNPROVEN")
        if engine.dialect.name != "postgresql":
            return RecoveryLane("CURRENT_ONLY_BLOCK", reason="NO_POSTGRES_ADVISORY_PROOF")
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
                # One AM/CLOSE attempt only. A failed old execution remains
                # visible in the journal, never replays on every trading tick.
                # Today's independent orders still require fresh broker and
                # exact-order duplicate proofs; the old order stays fenced.
                return RecoveryLane("CURRENT_ONLY", key, reason="HISTORICAL_" + status)
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
            "[KR_HIST_RECOVERY][JOURNAL_FAIL] type=%s action=CURRENT_ONLY_BLOCK_NO_RETRY",
            type(exc).__name__, exc_info=True,
        )
        return RecoveryLane("CURRENT_ONLY_BLOCK", reason="JOURNAL_UNAVAILABLE")



def current_tick_reconcile_proves_buy_safe(
    *, lane: RecoveryLane, result: dict | None, durable_record_ok: bool,
) -> bool:
    """A historical COMPLETE marker is never a current-tick BUY permit.

    The per-tick broker order/fill check, account/claim scope and frozen
    contracts must independently succeed. A raised exception, missing
    result, stale parity or degraded daily-ccld always blocks new BUYs.
    """
    # If the AM historical pass found ONLY dated previous-day debt, the
    # journal must first DURABLY record UNRESOLVED; then independently proven
    # today's orders can proceed without re-running that failed historical
    # recovery. Any journal error, current/unknown claim, stale broker balance,
    # claim write failure, or broker conflict remains fail-closed.
    historical_debt_recorded = bool(
        lane.mode == "FULL"
        and isinstance(result, dict)
        and result.get("historical_debt_only_recorded") is True
    )
    return bool(
        lane.mode in {"FULL", "CURRENT_ONLY"}
        and (durable_record_ok is True or historical_debt_recorded)
        and isinstance(result, dict)
        and result.get("ok") is True
        and not result.get("degraded")
        and not broker_reconcile_guard_blocks(result)
        and not result.get("unresolved_broker_activity")
        and result.get("frozen_contract_invariant_violations") == 0
        and result.get("historical_recovery_executed") is lane.historical_recovery
        and (
            result.get("current_only_fresh_broker_proven") is True
            or lane.mode == "FULL" and durable_record_ok is True
        )
    )

def record_historical_result(
    *, engine: Any, lane: RecoveryLane, result: dict | None,
) -> bool:
    """Persist terminal state only for the exact claiming fencing token."""
    if lane.mode != "FULL":
        return lane.mode == "CURRENT_ONLY"
    if not lane.job_key or not lane.fencing_token:
        return False
    health = (
        result.get("execution_claim_health")
        if isinstance(result, dict) and isinstance(result.get("execution_claim_health"), dict)
        else {}
    )
    required_dates = (
        "unresolved_source_date_current", "unresolved_source_date_prior",
        "unresolved_source_date_other_or_unknown",
    )
    dated = bool(
        health.get("available") is True
        and all(k in health for k in required_dates)
        and int(health.get("unresolved_execution_actions") or 0)
        == sum(int(health[k] or 0) for k in required_dates)
        and all(int(health[k] or 0) >= 0 for k in required_dates)
    )
    history_debt_only = bool(
        dated and int(health["unresolved_source_date_prior"] or 0) > 0
        and int(health["unresolved_source_date_current"] or 0) == 0
        and int(health["unresolved_source_date_other_or_unknown"] or 0) == 0
        and isinstance(result, dict)
        and result.get("ok") is True and not result.get("degraded")
        and result.get("historical_recovery_executed") is True
        and result.get("current_only_fresh_broker_proven") is True
        and result.get("frozen_contract_invariant_violations", 0) == 0
        and not result.get("unresolved_broker_activity")
        and not result.get("holdings_error")
        and not broker_reconcile_guard_blocks(result)
    )
    success = bool(
        dated and int(health["unresolved_source_date_prior"] or 0) == 0
        and int(health["unresolved_source_date_current"] or 0) == 0
        and int(health["unresolved_source_date_other_or_unknown"] or 0) == 0
        and isinstance(result, dict) and result.get("ok") is True
        and not result.get("degraded")
        and result.get("frozen_contract_invariant_violations", 0) == 0
        # A successfully repaired historical position is still a successful
        # FULL recovery. The parity fastpath is an optimization, not the
        # completion criterion: it can be false precisely when work was done.
        and result.get("historical_recovery_executed") is True
        and not result.get("unresolved_broker_activity")
        and not result.get("holdings_error")
        and not broker_reconcile_guard_blocks(result)
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
        # The marker is issued only AFTER the DB transaction committed.
        # A failed journal write must not authorize BUY based on an in-memory
        # inference. Failed historical economics remain unresolved.
        if isinstance(result, dict):
            result["historical_debt_only_recorded"] = bool(
                history_debt_only and not success
            )
        logger.info("[KR_HIST_RECOVERY][RESULT] state=%s key=%s", current["status"], lane.job_key)
        return success
    except Exception as exc:
        logger.error(
            "[KR_HIST_RECOVERY][WRITE_FAIL] type=%s action=BLOCK_BUY",
            type(exc).__name__, exc_info=True,
        )
        return False
