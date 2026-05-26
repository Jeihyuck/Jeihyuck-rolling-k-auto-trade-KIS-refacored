#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import sys
from typing import Any

import pandas as pd

from trader.db.engine import get_engine
from trader.db.repos import LedgerEventsRepo, load_final30_scored_db_only, save_job_checkpoint
from trader.final30_quality import build_canonical_prep_verdict, summarize_final30_quality
from trader.time_utils import now_kst, resolve_trade_context


logger = logging.getLogger(__name__)


def _flag_enabled(raw: str | None) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _write_output(name: str, value: Any) -> None:
    output_path = os.getenv("GITHUB_OUTPUT")
    if not output_path:
        return
    with open(output_path, "a", encoding="utf-8") as handle:
        handle.write(f"{name}={value}\n")


def _db_store_required() -> bool:
    return str(os.getenv("DB_STORE_REQUIRED", "0")).strip().lower() not in {"0", "false", "no", "off"}


def _load_final30_snapshot(env: str, as_of: str, strategy: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    try:
        df = load_final30_scored_db_only(
            get_engine(),
            env=env,
            as_of=as_of,
            strategy=strategy,
            require_exact_rows=30,
            fail_if_missing=False,
        )
    except Exception as exc:
        return pd.DataFrame(), {"status": "MISSING", "quality_ok": 0, "trade_can_proceed": 0, "error": str(exc)}

    if df.empty or len(df) != 30:
        return df, {"status": "MISSING", "quality_ok": 0, "trade_can_proceed": 0}

    quality = summarize_final30_quality(df, required_rows=30)
    verdict = build_canonical_prep_verdict(
        quality=quality,
        flow_failed_ratio=0.0,
        flow_fail_reason_counts={},
    )
    return df, verdict


def _upsert_duplicate_ready_event(
    *,
    env: str,
    as_of: str,
    trade_date: str,
    final30_count: int,
) -> str | None:
    repo = LedgerEventsRepo(get_engine())
    if not hasattr(repo, "upsert_prep_event"):
        message = "LedgerEventsRepo.upsert_prep_event missing"
        logger.error("[PREP][DUPLICATE_GUARD][LEDGER_UPSERT][MISSING_METHOD] %s", message)
        if _db_store_required():
            raise AttributeError(message)
        return None
    try:
        return repo.upsert_prep_event(
            env=env,
            strategy="pb1",
            as_of=as_of,
            trade_date=trade_date,
            event_type="PREP_DONE",
            status="READY_FROM_CANONICAL",
            reason="canonical_prep_already_ready",
            final30_count=final30_count,
            quality_ok=True,
            trade_can_proceed=True,
            run_id=None,
            run_window="prep",
            workflow_run_id=os.getenv("GITHUB_RUN_ID"),
            workflow_attempt=os.getenv("GITHUB_RUN_ATTEMPT"),
            git_sha=os.getenv("GITHUB_SHA"),
            source="duplicate_guard",
        )
    except Exception as exc:
        logger.warning(
            "[DB][LEDGER_EVENT][APPEND_SOFT_FAIL] event_type=PREP_DONE fallback=job_checkpoint err=%s",
            exc,
        )
        try:
            save_job_checkpoint(
                get_engine(),
                f"PREP_DONE:{env}:{as_of}",
                {
                    "env": env,
                    "as_of": str(as_of),
                    "trade_date": str(trade_date),
                    "final30_count": final30_count,
                    "status": "READY_FROM_CANONICAL",
                    "reason": "canonical_prep_already_ready",
                    "workflow_run_id": os.getenv("GITHUB_RUN_ID"),
                    "workflow_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
                    "git_sha": os.getenv("GITHUB_SHA"),
                    "source": "duplicate_guard",
                },
            )
            logger.info(
                "[DB][JOB_CHECKPOINT][PREP_DONE][UPSERT_OK] env=%s as_of=%s trade_date=%s",
                env,
                as_of,
                trade_date,
            )
        except Exception as cp_exc:
            logger.warning(
                "[DB][JOB_CHECKPOINT][PREP_DONE][FAIL] env=%s err=%s",
                env,
                cp_exc,
            )
        if _db_store_required():
            raise exc
        return None


def _print_prep_owner_info(
    *,
    env: str,
    as_of: str,
    final30_count: int,
    canonical_status: str,
) -> None:
    """Prep duplicate guard blocked 시 owner 정보를 출력한다."""
    current_workflow = os.getenv("GITHUB_WORKFLOW", "unknown")
    current_run_id = os.getenv("GITHUB_RUN_ID", "unknown")
    current_job = os.getenv("GITHUB_JOB", "unknown")
    current_started_at = os.getenv("GITHUB_RUN_STARTED_AT", "unknown")

    print(
        "[PREP][DUPLICATE_GUARD][BLOCKED] "
        f"current_workflow={current_workflow} "
        f"current_run_id={current_run_id} "
        f"current_job={current_job} "
        f"current_started_at={current_started_at} "
        f"reason=canonical_prep_already_ready"
    )

    # DB에서 가장 최근 PREP_DONE 이벤트를 owner로 조회한다
    try:
        engine = get_engine()
        from sqlalchemy import text

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        strategy,
                        as_of,
                        event_type,
                        status,
                        workflow_run_id,
                        workflow_attempt,
                        git_sha,
                        created_at,
                        updated_at
                    FROM ledger_events
                    WHERE env = :env
                      AND as_of::text = :as_of
                      AND event_type = 'PREP_DONE'
                      AND status = 'READY_FROM_CANONICAL'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"env": env, "as_of": as_of},
            ).fetchone()

        if row:
            owner_run_id = row[4] or "unknown"
            owner_attempt = row[5] or "unknown"
            owner_git_sha = (str(row[6] or "")[:8]) or "unknown"
            owner_created_at = str(row[7] or "unknown")
            owner_updated_at = str(row[8] or "unknown")
            print(
                "[PREP][DUPLICATE_GUARD][OWNER] "
                f"lock_name=prep:{env}:{as_of} "
                f"owner_workflow={current_workflow} "
                f"owner_run_id={owner_run_id} "
                f"owner_job=prep "
                f"owner_attempt={owner_attempt} "
                f"owner_git_sha={owner_git_sha} "
                f"owner_created_at={owner_created_at} "
                f"owner_updated_at={owner_updated_at} "
                f"current_workflow={current_workflow} "
                f"current_run_id={current_run_id} "
                f"current_job={current_job} "
                f"current_started_at={current_started_at} "
                f"as_of={as_of} final30_count={final30_count} "
                f"canonical_status={canonical_status} "
                f"reason=canonical_prep_already_ready"
            )
        else:
            print(
                "[PREP][DUPLICATE_GUARD][OWNER] "
                f"lock_name=prep:{env}:{as_of} "
                f"owner_run_id=unknown "
                f"owner_job=unknown "
                f"current_workflow={current_workflow} "
                f"current_run_id={current_run_id} "
                f"current_job={current_job} "
                f"as_of={as_of} final30_count={final30_count} "
                f"canonical_status={canonical_status} "
                f"reason=canonical_prep_already_ready_no_ledger_row"
            )

        # Stale check: 마지막 prep 이후 경과 시간 확인
        with engine.connect() as conn:
            stale_row = conn.execute(
                text(
                    """
                    SELECT
                        created_at,
                        EXTRACT(EPOCH FROM (NOW() - created_at))::int AS age_sec
                    FROM ledger_events
                    WHERE env = :env
                      AND as_of::text = :as_of
                      AND event_type = 'PREP_DONE'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"env": env, "as_of": as_of},
            ).fetchone()

        stale_threshold_sec = int(os.getenv("PREP_STALE_THRESHOLD_SEC", "7200"))  # 2h default
        if stale_row and stale_row[1] is not None:
            lock_age_sec = int(stale_row[1])
            is_stale = lock_age_sec > stale_threshold_sec
            print(
                "[PREP][DUPLICATE_GUARD][STALE_CHECK] "
                f"lock_age_sec={lock_age_sec} "
                f"stale_threshold_sec={stale_threshold_sec} "
                f"is_stale={int(is_stale)}"
            )
        else:
            print(
                "[PREP][DUPLICATE_GUARD][STALE_CHECK] "
                f"lock_age_sec=unknown stale_threshold_sec={stale_threshold_sec} is_stale=0"
            )

    except Exception as exc:
        logger.warning(
            "[PREP][DUPLICATE_GUARD][OWNER_LOOKUP_FAIL] err=%s", exc
        )
        print(
            "[PREP][DUPLICATE_GUARD][OWNER] "
            f"lock_name=prep:{env}:{as_of} "
            f"owner_run_id=lookup_failed "
            f"current_workflow={current_workflow} "
            f"current_run_id={current_run_id} "
            f"reason=owner_lookup_failed err={exc}"
        )


def main() -> int:
    env = (os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice").strip().lower() or "practice"
    strategy = os.getenv("WATCHLIST_FINAL_STRATEGY_KEY") or "pb1_watchlist_final_scored"
    event_name = (os.getenv("GITHUB_EVENT_NAME") or "unknown").strip().lower() or "unknown"
    allow_duplicate = _flag_enabled(os.getenv("ALLOW_DUPLICATE_PREP"))
    ctx = resolve_trade_context(now=now_kst(), env=env)
    as_of = str(ctx.get("as_of") or "")
    trade_date = str(ctx.get("trade_date") or now_kst().date().isoformat())

    df, verdict = _load_final30_snapshot(env=env, as_of=as_of, strategy=strategy)
    final30_count = int(len(df.index)) if not df.empty else 0
    quality_ok = int(verdict.get("quality_ok") or 0)
    trade_can_proceed = int(verdict.get("trade_can_proceed") or 0)
    canonical_status = str(verdict.get("status") or "MISSING").upper()
    is_ready = (
        final30_count == 30
        and quality_ok == 1
        and trade_can_proceed == 1
        and canonical_status == "OK"
    )
    should_skip = int(is_ready and not allow_duplicate)
    skip_reason = "canonical_prep_already_ready" if should_skip else ""

    print(
        "[PREP][DUPLICATE_GUARD][CHECK] "
        f"as_of={as_of} env={env} final30_count={final30_count} quality_ok={quality_ok} "
        f"trade_can_proceed={trade_can_proceed} canonical_status={canonical_status} "
        f"allow_duplicate={int(allow_duplicate)}"
    )

    if should_skip:
        # --- owner 정보 조회 및 출력 ---
        _print_prep_owner_info(
            env=env,
            as_of=as_of,
            final30_count=final30_count,
            canonical_status=canonical_status,
        )
        _upsert_duplicate_ready_event(
            env=env,
            as_of=as_of,
            trade_date=trade_date,
            final30_count=final30_count,
        )
        print(
            "[PREP][DUPLICATE_GUARD][LEDGER_UPSERT] "
            f"event=PREP_DONE status=READY_FROM_CANONICAL reason={skip_reason} "
            f"as_of={as_of} trade_date={trade_date} final30_count={final30_count}"
        )
        print(
            "[PREP][DUPLICATE_GUARD][SKIP] "
            f"as_of={as_of} reason={skip_reason} trade_can_proceed=1"
        )
        print(
            "[RUN_SUMMARY][RESULT] "
            f"status=SKIP_DUPLICATE_PREP reason={skip_reason} trade_can_proceed=1 event={event_name}"
        )
    elif allow_duplicate and is_ready:
        print(
            "[PREP][DUPLICATE_GUARD][BYPASS] "
            f"as_of={as_of} reason=allow_duplicate_prep_requested"
        )
    else:
        print(
            "[PREP][DUPLICATE_GUARD][RUN] "
            f"as_of={as_of} reason={verdict.get('error') or 'canonical_prep_not_ready'}"
        )

    _write_output("as_of", as_of)
    _write_output("final30_count", final30_count)
    _write_output("quality_ok", quality_ok)
    _write_output("trade_can_proceed", trade_can_proceed)
    _write_output("canonical_status", canonical_status)
    _write_output("should_skip", should_skip)
    _write_output("skip_reason", skip_reason)
    return 0


if __name__ == "__main__":
    sys.exit(main())