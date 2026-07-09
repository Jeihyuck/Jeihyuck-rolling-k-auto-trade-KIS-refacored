# -*- coding: utf-8 -*-
"""US Prep Runner — Dual-Agent 구조.

실행 순서:
1. setup logging
2. America/New_York 기준 trade_date 계산
3. save_us_prep_run 시작
4. config/us_universe.yaml manual_seed 로드
5. USDataProvider 초기화
6. build_us_dynamic_universe 실행
7. build_us_candidate_pool 실행
8. build_us_watchlist 실행
9. verify_us_final30_scored_rows 실행
10. final30_scored 저장 (runtime/signals/bot_state mirror)
11. prep_contract 생성 + 저장
12. summary report 생성
13. final status 결정 + finish_us_prep_run 완료

한국장 파일 import 금지:
  from trader.candidate_pool_builder import ...  ← 금지
  from trader.watchlist_builder import ...       ← 금지
  from trader.final30_quality import ...         ← 금지
  from trader.path_contract import ...           ← 금지
  from trader.runtime_paths import ...           ← 금지
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from trader.us.db.repos import (
    save_us_prep_run,
    finish_us_prep_run,
    clear_and_save_locked_us_watchlist,
)

logger = logging.getLogger(__name__)

# Critical ETFs (legacy compat)
CRITICAL_ETFS = {"QQQ", "SPY", "SMH", "SOXX"}


# ── Env helper ────────────────────────────────────────────────────────────────

def _env_true(name: str, default: str = "0") -> bool:
    """환경변수를 boolean으로 읽는다."""
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def should_bypass_already_prepared_guard(
    *,
    force_rebuild_prep: bool,
    event_name: str,
    confirm_ok: bool,
) -> bool:
    """already_prepared guard를 우회할지 결정한다.

    workflow_dispatch + confirm_ok + force_rebuild_prep 일 때만 True.
    schedule 이벤트에서는 반드시 False.
    """
    return bool(force_rebuild_prep and event_name == "workflow_dispatch" and confirm_ok)


# ── Backward-compat stubs (used by existing tests) ────────────────────────────

def _resolve_final_prep_status(
    provisional_status: str,
    saved_count: int,
    score_nonzero_count: int,
    score_contract_ok: bool,
    has_fatal_error: bool = False,
) -> tuple[str, bool]:
    """Legacy compat stub."""
    if has_fatal_error:
        return "ERROR", True
    if saved_count > 0 and score_nonzero_count == 0:
        return "ERROR", True
    if saved_count > 0 and not score_contract_ok:
        return "ERROR", True
    return provisional_status, False


def _determine_prep_status(
    total_symbols: int,
    locked_unique_count: int,
    data_failed_symbols: set,
    critical_etf_failures: set,
    has_fatal_error: bool,
    min_locked_unique: int = 10,
    target_locked_unique: int = 15,
    warn_on_data_failure: bool = True,
) -> str:
    """Legacy compat stub."""
    if has_fatal_error or total_symbols <= 0:
        return "ERROR"
    if critical_etf_failures:
        return "DEGRADED"
    if locked_unique_count <= 0:
        return "ERROR"
    if locked_unique_count < min_locked_unique:
        return "DEGRADED"
    if warn_on_data_failure and data_failed_symbols:
        return "OK_WITH_WARNINGS"
    if locked_unique_count < target_locked_unique:
        return "OK_WITH_WARNINGS"
    return "OK"


def _save_json_file(path: Path, data: object) -> None:
    """JSON 저장 헬퍼."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def run_prep(env: str = "practice", offline: bool = False, force_now: str | None = None) -> dict:
    """Dual-Agent US Prep 실행.

    Universe Discovery Agent (A) + Strategy Scoring Agent (B) 구조.

    Returns:
        {"status": "OK"|"OK_WITH_WARNINGS"|"ERROR", ...}
    """
    logger.info("[US_PREP][START] env=%s offline=%s", env, offline)
    logger.info(
        "[US_PREP][FORCE_NOW] enabled=%s force_now=%s",
        1 if force_now else 0,
        force_now or "",
    )

    # ── force_rebuild_prep env 읽기 ───────────────────────────────────────
    force_rebuild_prep = _env_true("US_FORCE_REBUILD_PREP", "0")
    logger.info(
        "[US_PREP][FORCE_REBUILD] enabled=%s env=%s",
        int(force_rebuild_prep),
        os.getenv("US_FORCE_REBUILD_PREP", "0"),
    )

    # 시간 및 run_id 초기화
    from zoneinfo import ZoneInfo
    _NY_TZ = ZoneInfo("America/New_York")
    if force_now:
        _now = datetime.fromisoformat(force_now).astimezone(_NY_TZ)
    else:
        _now = datetime.now(tz=_NY_TZ)
    trade_date = _now.strftime("%Y-%m-%d")
    as_of_date = trade_date
    logger.info(
        "[US_PREP][DATE_POLICY] trade_date=%s as_of_date=%s source=%s",
        trade_date,
        as_of_date,
        "force_now" if force_now else "now_ny",
    )

    # ── already_prepared guard bypass 체크 ───────────────────────────────
    if force_rebuild_prep:
        logger.warning(
            "[US_PREP][CHECK_ALREADY_PREPARED][BYPASS] reason=force_rebuild_prep trade_date=%s",
            trade_date,
        )
    else:
        logger.info("[US_PREP][CHECK_ALREADY_PREPARED][START] trade_date=%s", trade_date)

    # us_agent_runs에 prep run 시작 기록
    run_id = ""
    try:
        run_id = save_us_prep_run(
            trade_date=trade_date,
            agent_name="us_prep",
            mode="prep",
            env=env,
        )
        logger.info("[US_PREP][RUN_ID] %s", run_id)
    except Exception as exc:
        logger.error("[US_PREP][ERROR] save_us_prep_run failed: %s", exc)
        return {"status": "ERROR", "stage": "prep_run_init", "error": str(exc)}

    # ── 경로 초기화 ────────────────────────────────────────────────────────
    from trader.us.runtime_paths import (
        us_dynamic_universe_path,
        us_candidate_pool_path,
        us_top50_scored_path,
        us_final30_scored_path,
        us_signals_final30_scored_path,
        us_signals_latest_final30_scored_path,
        us_bot_state_final30_path,
        us_bot_state_candidate_pool_path,
        get_us_prep_paths,
    )
    paths = get_us_prep_paths(trade_date)

    # ── 1. manual_seed 로드 ───────────────────────────────────────────────
    try:
        from trader.us.universe import load_universe, get_all_tickers
        load_universe(force=True)
        manual_seed = get_all_tickers()
        logger.info(
            "[US_PREP][MANUAL_SEED] count=%d source=config/us_universe.yaml",
            len(manual_seed),
        )
    except Exception as exc:
        logger.error("[US_PREP][ERROR] manual_seed load failed: %s", exc)
        finish_us_prep_run(run_id, status="ERROR", result=str(exc))
        return {"status": "ERROR", "stage": "manual_seed", "error": str(exc)}

    # ── 2. Data Provider ─────────────────────────────────────────────────
    from trader.us.data_provider import USDataProvider
    provider = USDataProvider(offline=offline, cache_enabled=True)

    # ── 3. Dynamic Universe ───────────────────────────────────────────────
    logger.info("[US_PREP][HEARTBEAT] stage=dynamic_universe status=start")
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
        dynamic_universe_result = build_us_dynamic_universe(
            trade_date=trade_date,
            as_of_date=as_of_date,
            env=env,
            provider=provider,
            manual_seed=manual_seed,
            force_rebuild=True,
        )
        du_count = dynamic_universe_result.get("filtered_count", 0)
        du_status = dynamic_universe_result.get("status", "ERROR")
        logger.info("[US_PREP][HEARTBEAT] stage=dynamic_universe status=done filtered=%d", du_count)

        if du_status == "ERROR" or du_count < 30:
            logger.error("[US_PREP][ERROR] dynamic_universe hard fail count=%d status=%s", du_count, du_status)
            finish_us_prep_run(run_id, status="ERROR", result={"stage": "dynamic_universe", "count": du_count})
            return {"status": "ERROR", "stage": "dynamic_universe", "count": du_count}

        # 저장
        try:
            _save_json_file(us_dynamic_universe_path(trade_date), dynamic_universe_result)
        except Exception as exc:
            logger.warning("[US_PREP][WARN] dynamic_universe save failed: %s", exc)

    except Exception as exc:
        logger.error("[US_PREP][ERROR] dynamic_universe failed: %s", exc)
        finish_us_prep_run(run_id, status="ERROR", result=str(exc))
        return {"status": "ERROR", "stage": "dynamic_universe", "error": str(exc)}

    # ── 4. Candidate Pool ─────────────────────────────────────────────────
    logger.info("[US_PREP][HEARTBEAT] stage=candidate_pool status=start")
    try:
        from trader.us.candidate_pool_builder import build_us_candidate_pool
        candidate_pool_result = build_us_candidate_pool(
            trade_date=trade_date,
            as_of_date=as_of_date,
            env=env,
            dynamic_universe=dynamic_universe_result["symbols"],
            provider=provider,
            force_rebuild=True,
        )
        cp_count = candidate_pool_result.get("selected_count", 0)
        cp_status = candidate_pool_result.get("status", "ERROR")
        logger.info("[US_PREP][HEARTBEAT] stage=candidate_pool status=done selected=%d", cp_count)

        if cp_status == "ERROR" or cp_count < 50:
            logger.error("[US_PREP][ERROR] candidate_pool hard fail count=%d status=%s", cp_count, cp_status)
            finish_us_prep_run(run_id, status="ERROR", result={"stage": "candidate_pool", "count": cp_count})
            return {"status": "ERROR", "stage": "candidate_pool", "count": cp_count}

        # 저장
        try:
            _save_json_file(us_candidate_pool_path(trade_date), candidate_pool_result)
            _save_json_file(us_bot_state_candidate_pool_path(env, trade_date), candidate_pool_result)
        except Exception as exc:
            logger.warning("[US_PREP][WARN] candidate_pool save failed: %s", exc)

    except Exception as exc:
        logger.error("[US_PREP][ERROR] candidate_pool failed: %s", exc)
        finish_us_prep_run(run_id, status="ERROR", result=str(exc))
        return {"status": "ERROR", "stage": "candidate_pool", "error": str(exc)}

    # ── 5. Watchlist (Dual-Agent Scoring) ─────────────────────────────────
    logger.info("[US_PREP][HEARTBEAT] stage=watchlist_scoring status=start")
    try:
        from trader.us.watchlist_builder import build_us_watchlist
        watchlist_result = build_us_watchlist(
            trade_date=trade_date,
            as_of_date=as_of_date,
            env=env,
            candidate_pool=candidate_pool_result["rows"],
            provider=provider,
            force_rebuild=True,
        )
        wl_final30 = watchlist_result.get("final30_scored_count", 0)
        logger.info("[US_PREP][HEARTBEAT] stage=watchlist_scoring status=done final30=%d", wl_final30)
        logger.info(
            "[US_STRATEGY][SCORED] top50=%d final30=%d",
            watchlist_result.get("top50_count", 0),
            wl_final30,
        )

        if wl_final30 < 30:
            logger.warning("[US_PREP][CLUSTER_CAP][CONTINUE] final30_scored_below_30 count=%d reason=cap_safe_refill", wl_final30)

        # top50 저장
        try:
            _save_json_file(us_top50_scored_path(trade_date), {"trade_date": trade_date, "top50_scored": watchlist_result["top50_scored"]})
        except Exception as exc:
            logger.warning("[US_PREP][WARN] top50_scored save failed: %s", exc)

    except Exception as exc:
        logger.error("[US_PREP][ERROR] watchlist failed: %s", exc)
        finish_us_prep_run(run_id, status="ERROR", result=str(exc))
        return {"status": "ERROR", "stage": "watchlist", "error": str(exc)}

    # ── 6. Final30 Validation ─────────────────────────────────────────────
    try:
        from trader.us.final30_quality import verify_us_final30_scored_rows
        final30_scored = watchlist_result["final30_scored"]

        # close 필드 채우기 (price → close alias)
        for row in final30_scored:
            if "close" not in row or not row.get("close"):
                row["close"] = row.get("price", 0.0)

        cluster_cap_failed_early = bool(watchlist_result.get("cap_violations") or not watchlist_result.get("cluster_contract_ok", True) or wl_final30 < 30)
        validation_required_rows = wl_final30 if cluster_cap_failed_early else 30
        validation = verify_us_final30_scored_rows(
            final30_scored,
            required_rows=validation_required_rows,
            source="US_PREP_CORE_SAVE_INPUT",
        )
        validation["required_rows_original"] = 30
        validation["required_rows_effective"] = validation_required_rows
        validation["cluster_cap_failed_early"] = cluster_cap_failed_early

        if not validation.get("ok", False):
            if cluster_cap_failed_early:
                logger.warning("[US_PREP][CLUSTER_CAP][VALIDATION_CONTINUE] final30 validation failed but preserving contract artifacts errors=%s", validation.get("errors"))
            else:
                logger.error("[US_PREP][ERROR] final30 contract failed errors=%s", validation.get("errors"))
                finish_us_prep_run(run_id, status="ERROR", result={"stage": "validation", "errors": validation.get("errors", [])})
                return {"status": "ERROR", "stage": "final30_validation", "validation": validation}

    except Exception as exc:
        logger.error("[US_PREP][ERROR] final30 validation failed: %s", exc)
        finish_us_prep_run(run_id, status="ERROR", result=str(exc))
        return {"status": "ERROR", "stage": "final30_validation", "error": str(exc)}

    # ── 7. final30_scored 저장 (runtime/signals/bot_state mirror) ─────────
    try:
        final30_payload = {"trade_date": trade_date, "env": env, "final30_scored": final30_scored, "rotation_context": watchlist_result.get("rotation_context", {}), "final30_cluster_counts": watchlist_result.get("final30_cluster_counts", {})}
        # runtime
        _save_json_file(us_final30_scored_path(trade_date), final30_payload)
        # signals mirror
        _save_json_file(us_signals_final30_scored_path(), final30_payload)
        _save_json_file(us_signals_latest_final30_scored_path(), final30_payload)
        # bot_state mirror
        _save_json_file(us_bot_state_final30_path(env, trade_date), final30_payload)
        logger.info("[US_PREP][FINAL30_SAVED] paths=runtime,signals,bot_state")
    except Exception as exc:
        logger.error("[US_PREP][ERROR] final30_scored save failed: %s", exc)
        finish_us_prep_run(run_id, status="ERROR", result={"stage": "final30_save", "error": str(exc)})
        return {"status": "ERROR", "stage": "final30_save", "error": str(exc)}

    # ── 8. Final Status 결정 ──────────────────────────────────────────────
    du_warn = du_status == "OK_WITH_WARNINGS"
    cp_warn = cp_status == "OK_WITH_WARNINGS"
    if du_status == "ERROR" or cp_status == "ERROR":
        final_status = "ERROR"
    elif du_warn or cp_warn:
        final_status = "OK_WITH_WARNINGS"
    else:
        final_status = "OK"

    score_nonzero_count = validation.get("score_nonzero_count", 0)
    contract_ok = validation.get("ok", False)
    cap_violations = list(watchlist_result.get("cap_violations") or [])
    cluster_contract_ok = bool(watchlist_result.get("cluster_contract_ok", not cap_violations)) and not cap_violations
    if (watchlist_result.get("rotation_context") or {}).get("rotation_context_suspect") and str((watchlist_result.get("rotation_context") or {}).get("rotation_suspect_policy") or "block") == "block":
        cluster_contract_ok = False
        if "ROTATION_CONTEXT_SUSPECT" not in cap_violations:
            cap_violations.append("ROTATION_CONTEXT_SUSPECT")
    if not cluster_contract_ok:
        final_status = "FAILED_CLUSTER_CAP_CONTRACT"

    trade_can_proceed = int(
        final_status in ("OK", "OK_WITH_WARNINGS")
        and contract_ok
        and cluster_contract_ok
        and score_nonzero_count == wl_final30
        and not cap_violations
    )

    logger.info(
        "[US_PREP][FINAL_STATUS] final=%s final30=%d score_nonzero=%d contract_ok=%d trade_can_proceed=%d",
        final_status, wl_final30, score_nonzero_count, int(contract_ok), trade_can_proceed,
    )

    # ── 9. Prep Contract 생성 + 저장 ──────────────────────────────────────
    try:
        from trader.us.prep_contract import build_us_prep_contract, save_us_prep_contract, save_us_prep_summary
        contract = build_us_prep_contract(
            trade_date=trade_date,
            env=env,
            status=final_status,
            dynamic_universe_result=dynamic_universe_result,
            candidate_pool_result=candidate_pool_result,
            watchlist_result=watchlist_result,
            validation=validation,
            paths=paths,
        )
        contract_save = save_us_prep_contract(contract)
        if not contract_save["ok"]:
            logger.error("[US_PREP][ERROR] prep_contract save failed: %s", contract_save["errors"])
            finish_us_prep_run(run_id, status="ERROR", result={"stage": "contract_save", "errors": contract_save["errors"]})
            return {"status": "ERROR", "stage": "contract_save"}

        save_us_prep_summary(contract)
        logger.info("[US_PREP][CONTRACT_SAVED] paths=%d", len(contract_save["saved_paths"]))

    except Exception as exc:
        logger.error("[US_PREP][ERROR] prep_contract creation failed: %s", exc)
        finish_us_prep_run(run_id, status="ERROR", result=str(exc))
        return {"status": "ERROR", "stage": "prep_contract", "error": str(exc)}

    # ── 10. legacy locked watchlist 저장 (DB, backward compat) ───────────
    # 기존 DB 저장 로직도 유지해 AM/Afternoon tick runner가 DB에서도 읽을 수 있도록 한다
    try:
        legacy_entries = [
            {
                "symbol": row.get("symbol"),
                "exchange": row.get("exchange", "NASDAQ"),
                "strategy": row.get("entry_style_selected", "dual_agent"),
                "score": row.get("score_final", 0.0),
                "score_final": row.get("score_final", 0.0),
                "final_score": row.get("score_final", 0.0),
                "scores": {"final": row.get("score_final", 0.0)},
                "reason_json": row.get("reason_json", {}),
                "meta": {
                    "run_id": run_id,
                    "rank_final30": row.get("rank_final30"),
                    "agent_a_score": row.get("agent_a_score"),
                    "agent_b_score": row.get("agent_b_score"),
                    "theme_cluster": row.get("theme_cluster"),
                    "rotation_regime": row.get("rotation_regime"),
                },
            }
            for row in final30_scored
        ]
        save_result = clear_and_save_locked_us_watchlist(
            entries=legacy_entries,
            trade_date=trade_date,
            run_id=run_id,
            prep_status=final_status,
        )
        saved_count = save_result.get("unique_count", len(final30_scored)) if isinstance(save_result, dict) else len(final30_scored)
        logger.info("[US_PREP][LEGACY_WATCHLIST][SAVED] unique=%d", saved_count)
    except Exception as exc:
        logger.warning("[US_PREP][WARN] legacy locked watchlist save failed: %s", exc)
        saved_count = len(final30_scored)

    # ── 11. finish run ────────────────────────────────────────────────────
    result_dict = {
        "status": final_status,
        "dynamic_universe_count": du_count,
        "candidate_pool_count": cp_count,
        "top50_count": watchlist_result.get("top50_count", 0),
        "final30_count": wl_final30,
        "final30_scored_count": wl_final30,
        "score_nonzero_count": score_nonzero_count,
        "contract_ok": contract_ok,
        "cluster_contract_ok": cluster_contract_ok,
        "trade_can_proceed": trade_can_proceed,
        "watchlist_count": saved_count,
        "rotation_regime": watchlist_result.get("rotation_regime"),
        "rotation_context": watchlist_result.get("rotation_context", {}),
        "final30_cluster_counts": watchlist_result.get("final30_cluster_counts", {}),
        "final30_ai_tech_ratio": watchlist_result.get("final30_ai_tech_ratio", 0.0),
        "portfolio_cluster_weights": watchlist_result.get("portfolio_cluster_weights", {}),
        "cap_violations": cap_violations,
        "final30_cluster_cap_clean": watchlist_result.get("final30_cluster_cap_clean", cluster_contract_ok),
        "blocked_by_cluster_cap": watchlist_result.get("blocked_by_cluster_cap", []),
        "selected_by_bucket_champion": watchlist_result.get("selected_by_bucket_champion", False),
        "fallback_fill_used": watchlist_result.get("fallback_fill_used", False),
        "fallback_fill_count": watchlist_result.get("fallback_fill_count", 0),
        "fallback_fill_cap_safe": watchlist_result.get("fallback_fill_cap_safe", True),
    }
    finish_us_prep_run(run_id=run_id, status=final_status, result=result_dict)

    logger.info(
        "[US_PREP][FINISH] status=%s final30=%d score_nonzero=%d trade_can_proceed=%d",
        final_status, wl_final30, score_nonzero_count, trade_can_proceed,
    )

    # ── 12. prep_status.json 생성 ─────────────────────────────────────────
    # watchdog, trade-am, trade-afternoon이 prep 성공 여부를 파일로 판단할 수 있도록 저장
    try:
        from trader.us.runner_paths import get_us_prep_paths as _gpp
    except ImportError:
        _gpp = get_us_prep_paths
    try:
        _status_dir = Path(f"runtime/us/prep_status/{trade_date}")
        _status_dir.mkdir(parents=True, exist_ok=True)
        _status_file = _status_dir / "prep_status.json"
        _event_name = os.environ.get("GITHUB_EVENT_NAME", "")
        _workflow_name = os.environ.get("GITHUB_WORKFLOW", "US Trade Prep")
        _status_ok = final_status in ("OK", "OK_WITH_WARNINGS", "SUCCESS", "COMPLETED")
        prep_status_payload = {
            "market": "US",
            "as_of": trade_date,
            "trade_date": trade_date,
            "status": final_status,
            "status_ok": _status_ok,
            "contract_ok": contract_ok,
            "cluster_contract_ok": cluster_contract_ok,
            "trade_can_proceed": trade_can_proceed,
            "final30_rows": wl_final30,
            "watchlist_rows": saved_count,
            "score_nonzero_count": score_nonzero_count,
            "locked_count": saved_count,
            "run_id": run_id,
            "env": env,
            "event": _event_name,
            "workflow": _workflow_name,
            "rotation_regime": watchlist_result.get("rotation_regime"),
            "rotation_context": watchlist_result.get("rotation_context", {}),
            "final30_cluster_counts": watchlist_result.get("final30_cluster_counts", {}),
            "final30_ai_tech_ratio": watchlist_result.get("final30_ai_tech_ratio", 0.0),
            "portfolio_cluster_weights": watchlist_result.get("portfolio_cluster_weights", {}),
            "cap_violations": cap_violations,
            "final30_cluster_cap_clean": watchlist_result.get("final30_cluster_cap_clean", cluster_contract_ok),
            "blocked_by_cluster_cap": watchlist_result.get("blocked_by_cluster_cap", []),
            "selected_by_bucket_champion": watchlist_result.get("selected_by_bucket_champion", False),
            "fallback_fill_used": watchlist_result.get("fallback_fill_used", False),
            "fallback_fill_count": watchlist_result.get("fallback_fill_count", 0),
            "fallback_fill_cap_safe": watchlist_result.get("fallback_fill_cap_safe", True),
        }
        _save_json_file(_status_file, prep_status_payload)
        logger.info(
            "[US_PREP][PREP_STATUS_JSON][SAVED] path=%s status=%s trade_can_proceed=%s",
            _status_file, final_status, trade_can_proceed,
        )
    except Exception as exc:
        logger.warning("[US_PREP][WARN] prep_status.json save failed: %s", exc)

    return {
        "status": final_status,
        "run_id": run_id,
        "trade_date": trade_date,
        "dynamic_universe_count": du_count,
        "candidate_pool_count": cp_count,
        "top50_count": watchlist_result.get("top50_count", 0),
        "final30_count": wl_final30,
        "final30_scored_count": wl_final30,
        "score_nonzero_count": score_nonzero_count,
        "contract_ok": contract_ok,
        "cluster_contract_ok": cluster_contract_ok,
        "trade_can_proceed": trade_can_proceed,
        "rotation_regime": watchlist_result.get("rotation_regime"),
        "final30_cluster_counts": watchlist_result.get("final30_cluster_counts", {}),
        "final30_ai_tech_ratio": watchlist_result.get("final30_ai_tech_ratio", 0.0),
        "cap_violations": cap_violations,
        "final30_cluster_cap_clean": watchlist_result.get("final30_cluster_cap_clean", cluster_contract_ok),
        "blocked_by_cluster_cap": watchlist_result.get("blocked_by_cluster_cap", []),
        "selected_by_bucket_champion": watchlist_result.get("selected_by_bucket_champion", False),
        "fallback_fill_used": watchlist_result.get("fallback_fill_used", False),
        "fallback_fill_count": watchlist_result.get("fallback_fill_count", 0),
        "fallback_fill_cap_safe": watchlist_result.get("fallback_fill_cap_safe", True),
    }


def main() -> None:
    from trader.us.utils.logging_utils import setup_us_logging
    setup_us_logging()
    parser = argparse.ArgumentParser(description="US Dual-Agent Prep Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("--force-now", dest="force_now", default=None)
    args = parser.parse_args()

    result = run_prep(env=args.env, offline=args.offline, force_now=args.force_now)

    allow_empty_offline = int(os.getenv("US_PREP_ALLOW_EMPTY_WATCHLIST_OFFLINE", "0")) != 0
    if args.offline and allow_empty_offline:
        if result["status"] in ("OK", "OK_WITH_WARNINGS", "ERROR"):
            logger.info("[US_PREP][OFFLINE_EXIT] status=%s allowed", result["status"])
            sys.exit(0)

    if result["status"] not in ("OK", "OK_WITH_WARNINGS"):
        sys.exit(1)


if __name__ == "__main__":
    main()

