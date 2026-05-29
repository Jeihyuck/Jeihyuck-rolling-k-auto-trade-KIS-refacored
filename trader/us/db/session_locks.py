# -*- coding: utf-8 -*-
"""trader/us/db/session_locks.py

미국장 DB 기반 세션 lock.

GitHub concurrency는 보조 장치일 뿐이다. 이 DB lock이 주요 중복 방어 수단이다.

사용 방법:
    claimed, existing = claim_us_session_lock(
        env="practice",
        trade_date=date(2026, 5, 28),
        session="am",
        github_run_id="...",
        github_workflow="US Trade AM",
        github_run_attempt="1",
    )
    if not claimed:
        logger.info("[US_SESSION_LOCK][DUPLICATE_SKIP] ...")
        return  # trade loop 진입 금지

    try:
        # trade loop ...
        finish_us_session_lock(env, trade_date, session, github_run_id, "DONE", "ok", {})
    except Exception:
        finish_us_session_lock(env, trade_date, session, github_run_id, "FAILED", "exception", {})
        raise

상태값:
    RUNNING         — 실행 중
    DONE            — 정상 완료
    DONE_WITH_WARNINGS — 경고 포함 완료
    SKIP_PHASE_WINDOW  — phase window 밖이어서 skip
    SKIP_STALE         — 늦게 시작해서 주문 금지
    SKIP_MARKET_CLOSED — 시장 닫힘
    FAILED          — 오류 종료
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# stale RUNNING으로 간주하는 기준 (이 시간 이상 경과 시 recovery 검토)
_STALE_RUNNING_THRESHOLD_HOURS = 6

_VALID_STATUSES = frozenset({
    "RUNNING",
    "DONE",
    "DONE_WITH_WARNINGS",
    "SKIP_PHASE_WINDOW",
    "SKIP_STALE",
    "SKIP_MARKET_CLOSED",
    "FAILED",
})


def _get_engine():
    """SQLAlchemy engine을 가져온다."""
    from trader.us.db.repos import get_engine
    return get_engine()


def claim_us_session_lock(
    env: str,
    trade_date: date,
    session: str,
    github_run_id: str | None,
    github_workflow: str | None,
    github_run_attempt: str | None,
    metadata: dict | None = None,
) -> tuple[bool, dict | None]:
    """DB session lock을 획득한다.

    INSERT ... ON CONFLICT DO NOTHING 패턴 사용.

    Returns:
        (claimed, existing_row)
        - claimed=True: lock 획득 성공 → trade loop 진입 가능
        - claimed=False: 이미 다른 run이 실행 중 → trade loop 진입 금지
    """
    from sqlalchemy import text as sa_text

    meta_json = json.dumps(metadata or {}, default=str)
    trade_date_str = trade_date.isoformat() if isinstance(trade_date, date) else str(trade_date)

    try:
        engine = _get_engine()
    except Exception as exc:
        logger.warning(
            "[US_SESSION_LOCK][ENGINE_FAIL] env=%s trade_date=%s session=%s error=%s — "
            "proceeding without DB lock (file guard only)",
            env, trade_date_str, session, exc,
        )
        return True, None

    try:
        with engine.begin() as conn:
            result = conn.execute(
                sa_text("""
                    INSERT INTO us_session_locks
                        (market, env, trade_date, session, status,
                         github_run_id, github_workflow, github_run_attempt,
                         started_at, metadata)
                    VALUES
                        ('US', :env, :trade_date, :session, 'RUNNING',
                         :run_id, :workflow, :attempt,
                         NOW(), :metadata::jsonb)
                    ON CONFLICT ON CONSTRAINT uq_us_session_once
                    DO NOTHING
                    RETURNING id
                """),
                {
                    "env": env,
                    "trade_date": trade_date_str,
                    "session": session,
                    "run_id": github_run_id or "",
                    "workflow": github_workflow or "",
                    "attempt": github_run_attempt or "1",
                    "metadata": meta_json,
                },
            )
            row = result.fetchone()

        if row is not None:
            # INSERT 성공 — lock 획득
            logger.info(
                "[US_SESSION_LOCK][CLAIM_OK] market=US env=%s trade_date=%s session=%s run_id=%s",
                env, trade_date_str, session, github_run_id or "local",
            )
            return True, None

        # CONFLICT — 기존 row 조회
        existing = _fetch_existing_lock(env, trade_date_str, session)
        if existing is None:
            # 조회 실패 — 안전하게 진행 허용 (file guard가 2차 방어)
            logger.warning(
                "[US_SESSION_LOCK][CONFLICT_FETCH_FAIL] env=%s trade_date=%s session=%s",
                env, trade_date_str, session,
            )
            return True, None

        existing_status = existing.get("status", "UNKNOWN")
        existing_run_id = existing.get("github_run_id", "")
        started_at = existing.get("started_at")

        # stale RUNNING 체크
        if existing_status == "RUNNING" and started_at:
            age_hours = _age_hours(started_at)
            if age_hours >= _STALE_RUNNING_THRESHOLD_HOURS:
                logger.warning(
                    "[US_SESSION_LOCK][STALE_RUNNING] env=%s trade_date=%s session=%s "
                    "existing_run_id=%s age_hours=%.1f — stale, but duplicate guard still applies",
                    env, trade_date_str, session, existing_run_id, age_hours,
                )
                # stale RUNNING이어도 기본 duplicate skip — 명시적 recovery는 별도 처리
                logger.info(
                    "[US_SESSION_LOCK][DUPLICATE_SKIP] market=US env=%s trade_date=%s "
                    "session=%s existing_status=%s existing_run_id=%s",
                    env, trade_date_str, session, existing_status, existing_run_id,
                )
                return False, existing

        logger.info(
            "[US_SESSION_LOCK][DUPLICATE_SKIP] market=US env=%s trade_date=%s "
            "session=%s existing_status=%s existing_run_id=%s",
            env, trade_date_str, session, existing_status, existing_run_id,
        )
        return False, existing

    except Exception as exc:
        # DB 오류 시 file guard만으로 진행
        logger.warning(
            "[US_SESSION_LOCK][CLAIM_ERROR] env=%s trade_date=%s session=%s error=%s — "
            "proceeding without DB lock",
            env, trade_date_str, session, exc,
        )
        return True, None


def finish_us_session_lock(
    env: str,
    trade_date: date,
    session: str,
    github_run_id: str | None,
    status: str,
    reason: str,
    metadata: dict | None = None,
) -> None:
    """세션 완료 시 DB lock을 업데이트한다.

    Args:
        status: "DONE" | "DONE_WITH_WARNINGS" | "SKIP_PHASE_WINDOW" |
                "SKIP_STALE" | "SKIP_MARKET_CLOSED" | "FAILED"
    """
    from sqlalchemy import text as sa_text

    if status not in _VALID_STATUSES:
        logger.warning(
            "[US_SESSION_LOCK][FINISH_WARN] unknown status=%s — using FAILED", status
        )
        status = "FAILED"

    meta_json = json.dumps(metadata or {}, default=str)
    trade_date_str = trade_date.isoformat() if isinstance(trade_date, date) else str(trade_date)

    try:
        engine = _get_engine()
    except Exception as exc:
        logger.warning(
            "[US_SESSION_LOCK][FINISH_ENGINE_FAIL] env=%s session=%s error=%s",
            env, session, exc,
        )
        return

    try:
        with engine.begin() as conn:
            conn.execute(
                sa_text("""
                    UPDATE us_session_locks
                    SET
                        status = :status,
                        result_status = :status,
                        reason = :reason,
                        finished_at = NOW(),
                        updated_at = NOW(),
                        metadata = metadata || :metadata::jsonb
                    WHERE
                        market = 'US'
                        AND env = :env
                        AND trade_date = :trade_date
                        AND session = :session
                        AND (github_run_id = :run_id OR github_run_id IS NULL OR github_run_id = '')
                """),
                {
                    "status": status,
                    "reason": reason,
                    "env": env,
                    "trade_date": trade_date_str,
                    "session": session,
                    "run_id": github_run_id or "",
                    "metadata": meta_json,
                },
            )
        logger.info(
            "[US_SESSION_LOCK][FINISH_OK] env=%s trade_date=%s session=%s status=%s reason=%s",
            env, trade_date_str, session, status, reason,
        )
    except Exception as exc:
        logger.warning(
            "[US_SESSION_LOCK][FINISH_ERROR] env=%s session=%s error=%s",
            env, session, exc,
        )


def _fetch_existing_lock(env: str, trade_date_str: str, session: str) -> dict | None:
    """기존 lock row를 읽어온다."""
    from sqlalchemy import text as sa_text

    try:
        engine = _get_engine()
        with engine.connect() as conn:
            row = conn.execute(
                sa_text("""
                    SELECT id, status, github_run_id, started_at, finished_at, reason
                    FROM us_session_locks
                    WHERE market = 'US' AND env = :env
                      AND trade_date = :trade_date AND session = :session
                """),
                {"env": env, "trade_date": trade_date_str, "session": session},
            ).fetchone()
        if row is None:
            return None
        return dict(row._mapping)
    except Exception:
        return None


def _age_hours(started_at: Any) -> float:
    """started_at으로부터 경과 시간(시간 단위)을 계산한다."""
    try:
        if isinstance(started_at, str):
            started_at = datetime.fromisoformat(started_at)
        if isinstance(started_at, datetime):
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - started_at).total_seconds() / 3600
    except Exception:
        pass
    return 0.0


def make_client_order_id(
    trade_date: str,
    env: str,
    session: str,
    strategy: str,
    symbol: str,
    side: str,
    reason: str,
) -> str:
    """주문 idempotency용 deterministic client_order_id를 생성한다.

    형식: US:{trade_date}:{env}:{session}:{strategy}:{symbol}:{side}:{reason}
    예:   US:2026-05-28:practice:am:PB1:NVDA:BUY:ENTRY

    특수문자는 포함하지 않도록 각 요소를 대문자로 정규화한다.
    """
    parts = [
        "US",
        str(trade_date),
        str(env).lower(),
        str(session).lower(),
        str(strategy).upper(),
        str(symbol).upper(),
        str(side).upper(),
        str(reason).upper(),
    ]
    return ":".join(parts)
