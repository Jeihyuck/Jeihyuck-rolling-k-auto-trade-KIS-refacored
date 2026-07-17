# -*- coding: utf-8 -*-
"""US Prep Contract 생성.

미국장 prep 완료 후 AM/Afternoon/Close session이 읽을
authoritative prep contract를 생성하고 저장한다.

한국장 파일 import 금지.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trader.us.runtime_paths import (
    us_prep_contract_path,
    us_signals_latest_prep_contract_path,
    us_signals_prep_contract_path,
    us_prep_summary_json_path,
    us_prep_summary_md_path,
    us_prep_latest_summary_json_path,
    us_prep_latest_summary_md_path,
    get_us_prep_paths,
)

logger = logging.getLogger(__name__)


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _current_git_commit_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return "unknown"

def build_us_prep_contract(
    *,
    trade_date: str,
    env: str,
    status: str,
    dynamic_universe_result: dict,
    candidate_pool_result: dict,
    watchlist_result: dict,
    validation: dict,
    paths: dict,
) -> dict:
    """US Prep Contract 생성.

    Args:
        trade_date: YYYY-MM-DD
        env: practice / live
        status: 최종 prep 상태 (OK / OK_WITH_WARNINGS / ERROR)
        dynamic_universe_result: build_us_dynamic_universe() 결과
        candidate_pool_result: build_us_candidate_pool() 결과
        watchlist_result: build_us_watchlist() 결과
        validation: verify_us_final30_scored_rows() 결과
        paths: get_us_prep_paths(trade_date) 결과

    Returns:
        prep_contract dict
    """
    dynamic_universe_count = dynamic_universe_result.get("filtered_count", 0)
    candidate_pool_count = candidate_pool_result.get("selected_count", 0)
    top50_count = watchlist_result.get("top50_count", 0)
    final30_count = watchlist_result.get("final30_count", 0)
    final30_scored_count = watchlist_result.get("final30_scored_count", 0)
    score_nonzero_count = validation.get("score_nonzero_count", 0)
    daily_sync_target_count = int(dynamic_universe_result.get("daily_sync_target_count", 0) or 0)
    daily_sync_ok_count = int(dynamic_universe_result.get("daily_sync_ok_count", daily_sync_target_count) or 0)
    daily_sync_failed_count = int(dynamic_universe_result.get("daily_sync_failed_count", max(0, daily_sync_target_count - daily_sync_ok_count)) or 0)
    daily_sync_success_ratio = (daily_sync_ok_count / daily_sync_target_count) if daily_sync_target_count else 1.0
    benchmark_sync_failed_count = int(dynamic_universe_result.get("benchmark_sync_failed_count", 0) or 0)
    open_position_sync_failed_count = int(dynamic_universe_result.get("open_position_sync_failed_count", 0) or 0)
    final30_sync_failed_count = int(watchlist_result.get("final30_sync_failed_count", 0) or 0)
    exchange_failed_symbols = list(dynamic_universe_result.get("exchange_resolution_failed_symbols") or [])

    cap_violations = list(watchlist_result.get("cap_violations") or [])
    rotation_context = watchlist_result.get("rotation_context") or {}
    raw_cluster_contract_ok = bool(watchlist_result.get("cluster_contract_ok", not cap_violations)) and not cap_violations
    cluster_contract_ok = raw_cluster_contract_ok
    if rotation_context.get("rotation_context_suspect") and str(rotation_context.get("rotation_suspect_policy") or "block") == "block":
        cluster_contract_ok = False
        if "ROTATION_CONTEXT_SUSPECT" not in cap_violations:
            cap_violations.append("ROTATION_CONTEXT_SUSPECT")

    market_state = watchlist_result.get("market_state_overlay") or watchlist_result.get("market_state") or {}
    if not isinstance(market_state, dict):
        market_state = {}
    market_regime = str(market_state.get("market_regime") or "NEUTRAL")
    force_entry_block = bool(market_state.get("force_entry_block", False))
    allow_new_buy = bool(market_state.get("allow_new_buy", True))

    absolute_min_final30 = _env_int("US_ABSOLUTE_MIN_FINAL30_FOR_TRADE", 15)
    degraded_min_final30 = _env_int("US_DEGRADED_MIN_FINAL30_FOR_TRADE", 20)
    normal_min_final30 = _env_int("US_NORMAL_MIN_FINAL30_FOR_TRADE", 25)

    degraded_haircut = _env_float("US_UNDERFILLED_DEGRADED_CAPITAL_HAIRCUT", 0.75)
    severe_haircut = _env_float("US_UNDERFILLED_SEVERE_CAPITAL_HAIRCUT", 0.50)

    final30_complete = final30_scored_count == 30
    final30_empty = final30_scored_count <= 0

    validation_ok = bool(validation.get("ok", False))
    score_contract_ok = (
        final30_scored_count > 0
        and score_nonzero_count == final30_scored_count
    )

    if final30_scored_count >= normal_min_final30:
        underfilled_tier = "normal" if final30_complete else "normal_underfilled"
        final30_trade_ready = True
        underfilled_capital_haircut = 1.0
    elif final30_scored_count >= degraded_min_final30:
        underfilled_tier = "degraded_underfilled"
        final30_trade_ready = True
        underfilled_capital_haircut = degraded_haircut
    elif final30_scored_count >= absolute_min_final30:
        if market_regime in {"RISK_ON", "GROWTH_LEADERSHIP"}:
            underfilled_tier = "severe_underfilled"
            final30_trade_ready = True
            underfilled_capital_haircut = severe_haircut
        else:
            underfilled_tier = "severe_underfilled_blocked"
            final30_trade_ready = False
            underfilled_capital_haircut = 0.0
    else:
        underfilled_tier = "blocked_underfilled"
        final30_trade_ready = False
        underfilled_capital_haircut = 0.0

    underfilled_final30 = bool(final30_scored_count < 30 and final30_trade_ready)

    # Entry quality is deliberately separated from session/exit liveness.
    # Final30 underfill, cluster caps, and risk-off regimes block only new BUYs;
    # they must not stop balance reconcile, existing-position exit monitoring, or close.
    hard_system_failure = status == "ERROR" or bool(validation.get("hard_system_failure", False))
    # Exit/close liveness must not depend on Final30/watchlist/score/cluster/regime quality.
    # Those are entry-quality signals only. System/broker hard failures are the only
    # prep-time reason to disable exit monitoring.
    exit_quality_ok = bool(not hard_system_failure)
    contract_ok = bool(exit_quality_ok)
    if not cluster_contract_ok:
        status = "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP"
    elif not final30_complete and final30_trade_ready:
        status = "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE"
    elif not final30_trade_ready:
        status = "OK_WITH_WARNINGS_ENTRY_BLOCKED_UNDERFILLED"

    agent_a_ok = validation.get("agent_a_nonzero_count", 0) >= 25
    agent_b_ok = validation.get("agent_b_nonzero_count", 0) >= 25

    trade_block_reason = "ok"

    if status == "ERROR":
        trade_block_reason = "prep_status_error"
    elif cap_violations:
        trade_block_reason = "sector_cap_violation_block"
    elif not cluster_contract_ok:
        trade_block_reason = "cluster_cap_contract_failed"
    elif final30_empty:
        trade_block_reason = "final30_empty"
    elif not final30_trade_ready:
        if final30_scored_count < absolute_min_final30:
            trade_block_reason = "final30_below_absolute_min"
        else:
            trade_block_reason = "final30_underfilled_regime_block"
    elif not validation_ok:
        trade_block_reason = "validation_failed"
    elif not score_contract_ok:
        trade_block_reason = "score_contract_failed"
    elif market_regime == "RISK_OFF":
        trade_block_reason = "risk_off_entry_block"
    elif force_entry_block:
        trade_block_reason = "force_entry_block"
    elif not allow_new_buy:
        trade_block_reason = "allow_new_buy_false"

    volume_missing_fallback_used = bool(dynamic_universe_result.get("volume_missing_fallback_used"))
    allow_entry_with_volume_missing = _env_int("US_ALLOW_ENTRY_WITH_VOLUME_MISSING_FALLBACK", 0) == 1

    exit_can_proceed = int(exit_quality_ok)
    close_can_proceed = int(not hard_system_failure)
    entry_can_proceed = int(trade_block_reason == "ok")
    if volume_missing_fallback_used and not allow_entry_with_volume_missing:
        entry_can_proceed = 0
        if trade_block_reason == "ok":
            trade_block_reason = "volume_missing_provider_entry_block"
    if cap_violations or not cluster_contract_ok:
        # Degrade to liveness/exit/reconcile; block new BUYs in risky clusters.
        entry_can_proceed = 0
    min_sync_ratio = _env_float("US_FINAL30_DAILY_SYNC_MIN_RATIO", 0.90)
    if daily_sync_success_ratio < min_sync_ratio or benchmark_sync_failed_count > 0 or final30_sync_failed_count > 0 or exchange_failed_symbols:
        entry_can_proceed = 0
        trade_block_reason = "data_sync_quality_entry_block"
    trade_can_proceed = int(exit_can_proceed or entry_can_proceed)
    degraded_reason = "" if entry_can_proceed else trade_block_reason

    base_capital_scale = float(market_state.get("capital_scale", market_state.get("exposure_multiplier", 1.0)) or 1.0)
    effective_capital_scale = base_capital_scale * underfilled_capital_haircut

    base_max_new_positions = int(market_state.get("max_new_positions", 10) or 10)

    if underfilled_tier == "severe_underfilled":
        effective_max_new_positions = min(5, final30_scored_count, base_max_new_positions)
    elif underfilled_tier in {"normal_underfilled", "degraded_underfilled", "normal"}:
        effective_max_new_positions = min(final30_scored_count, base_max_new_positions)
    else:
        effective_max_new_positions = 0

    warnings: list[str] = []
    errors: list[str] = []

    warnings.extend(dynamic_universe_result.get("warnings", []))
    warnings.extend(candidate_pool_result.get("status") == "OK_WITH_WARNINGS"
                    and ["candidate_pool_count_below_target"] or [])
    warnings.extend(validation.get("warnings", []))

    errors.extend(dynamic_universe_result.get("errors", []))
    errors.extend(validation.get("errors", []))

    if market_regime == "RISK_OFF":
        entry_can_proceed = 0
        trade_block_reason = "risk_off_entry_block"
        status = "RISK_OFF_ENTRY_BLOCKED" if market_state.get("market_state") != "DEFENSE_CRASH" else "DEFENSE_CRASH_ENTRY_BLOCKED"
    if market_state.get("market_state") == "DEFENSE_CRASH" or force_entry_block:
        entry_can_proceed = 0
        trade_block_reason = "risk_off_entry_block" if market_regime == "RISK_OFF" else "force_entry_block"
        status = "DEFENSE_CRASH_ENTRY_BLOCKED"

    trade_can_proceed = int(entry_can_proceed == 1 or exit_can_proceed == 1 or close_can_proceed == 1)
    if entry_can_proceed == 0:
        effective_capital_scale = 0.0
        effective_max_new_positions = 0
        degraded_reason = trade_block_reason

    contract = {
        "market": "US",
        "contract_version": "us_sector_rotation_v3",
        "market_regime_version": "us_leading_regime_v1",
        "selector_version": watchlist_result.get("selector_version", "bucket_champion_v2"),
        "sector_cap_enforced": True,
        "env": env,
        "trade_date": trade_date,
        "daily_sync_target_count": daily_sync_target_count,
        "daily_sync_ok_count": daily_sync_ok_count,
        "daily_sync_failed_count": daily_sync_failed_count,
        "daily_sync_success_ratio": daily_sync_success_ratio,
        "benchmark_sync_failed_count": benchmark_sync_failed_count,
        "open_position_sync_failed_count": open_position_sync_failed_count,
        "final30_sync_failed_count": final30_sync_failed_count,
        "exchange_resolution_failed_count": len(exchange_failed_symbols),
        "exchange_resolution_failed_symbols": exchange_failed_symbols,
        "status": status,
        "trade_can_proceed": trade_can_proceed,
        "entry_can_proceed": entry_can_proceed,
        "exit_can_proceed": exit_can_proceed,
        "close_can_proceed": close_can_proceed,
        "trade_block_reason": trade_block_reason,
        "degraded_reason": degraded_reason,
        "volume_missing_fallback_used": volume_missing_fallback_used,
        "volume_missing_fallback_count": int(dynamic_universe_result.get("volume_missing_fallback_count", 0) or 0),
        "raw_final30_count": final30_count,
        "effective_final30_count": final30_scored_count,
        "dynamic_universe_count": dynamic_universe_count,
        "candidate_pool_count": candidate_pool_count,
        "top50_count": top50_count,
        "final30_count": final30_count,
        "final30_scored_count": final30_scored_count,
        "score_nonzero_count": score_nonzero_count,
        "contract_ok": contract_ok,
        "cluster_contract_ok": cluster_contract_ok,
        "absolute_min_final30_for_trade": absolute_min_final30,
        "degraded_min_final30_for_trade": degraded_min_final30,
        "normal_min_final30_for_trade": normal_min_final30,
        "final30_complete": final30_complete,
        "final30_trade_ready": final30_trade_ready,
        "final30_empty": final30_empty,
        "underfilled_final30": underfilled_final30,
        "underfilled_tier": underfilled_tier,
        "underfilled_capital_haircut": underfilled_capital_haircut,
        "effective_capital_scale": effective_capital_scale,
        "effective_max_new_positions": effective_max_new_positions,
        "rotation_regime": watchlist_result.get("rotation_regime") or rotation_context.get("rotation_regime"),
        "rotation_context": rotation_context,
        "final30_cluster_counts": watchlist_result.get("final30_cluster_counts", {}),
        "final30_ai_tech_ratio": watchlist_result.get("final30_ai_tech_ratio", 0.0),
        "cap_violations": cap_violations,
        "final30_cluster_cap_clean": watchlist_result.get("final30_cluster_cap_clean", cluster_contract_ok),
        "blocked_by_cluster_cap": watchlist_result.get("blocked_by_cluster_cap", []),
        "selected_by_bucket_champion": watchlist_result.get("selected_by_bucket_champion", False),
        "fallback_fill_used": watchlist_result.get("fallback_fill_used", False),
        "fallback_fill_count": watchlist_result.get("fallback_fill_count", 0),
        "fallback_fill_cap_safe": watchlist_result.get("fallback_fill_cap_safe", True),
        "agent_a_ok": agent_a_ok,
        "agent_b_ok": agent_b_ok,
        "validation": validation,
        "warnings": warnings,
        "errors": errors,
        "paths": paths,
        "market_state": market_state.get("market_state", "NORMAL"),
        "market_regime": market_regime,
        "regime_score": market_state.get("regime_score", 0),
        "risk_score": market_state.get("risk_score", 0),
        "growth_score": market_state.get("growth_score", 0),
        "breadth_score": market_state.get("breadth_score", 0),
        "defensive_score": market_state.get("defensive_score", 0),
        "capital_scale": base_capital_scale,
        "max_ai_tech_ratio": market_state.get("max_ai_tech_ratio", 0.35),
        "max_single_cluster_ratio": market_state.get("max_single_cluster_ratio", 0.20),
        "max_new_positions": base_max_new_positions,
        "entry_aggressiveness": market_state.get("entry_aggressiveness", "normal"),
        "take_profit_mode": market_state.get("take_profit_mode", "staged_take_profit"),
        "trailing_stop_pct": market_state.get("trailing_stop_pct", 0.02),
        "stop_tightening_level": market_state.get("stop_tightening_level", "normal"),
        "allow_defensive_buy": market_state.get("allow_defensive_buy", True),
        "regime_reasons": market_state.get("regime_reasons", market_state.get("market_state_reasons", [])),
        "data_quality": market_state.get("data_quality", "ok"),
        "data_quality_warnings": market_state.get("data_quality_warnings", []),
        "leading_indicators": market_state.get("leading_indicators", {}),
        "defense_regime": market_state.get("defense_regime", "NONE"),
        "risk_on_regime": market_state.get("risk_on_regime", "NONE"),
        "market_state_reasons": market_state.get("market_state_reasons", []),
        "exposure_multiplier": market_state.get("exposure_multiplier", 1.0),
        "allow_new_buy": allow_new_buy,
        "allow_ai_tech_buy": market_state.get("allow_ai_tech_buy", False),
        "force_entry_block": force_entry_block,
        "profit_capture_enabled": market_state.get("profit_capture_enabled", True),
        "trailing_stop_mode": market_state.get("trailing_stop_mode", "normal"),
        "account_loss_kill_switch_triggered": market_state.get("account_loss_kill_switch_triggered", False),
        "forbidden_hedge_symbols": market_state.get("forbidden_hedge_symbols", []),
        "git_commit_sha": _current_git_commit_sha(),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    return contract


def save_us_prep_contract(contract: dict) -> dict:
    """prep_contract를 모든 경로에 저장.

    Returns:
        {"ok": bool, "saved_paths": list[str], "errors": list[str]}
    """
    trade_date = contract["trade_date"]
    saved_paths: list[str] = []
    save_errors: list[str] = []

    # runtime/us/watchlist/{date}/prep_contract.json
    try:
        path = us_prep_contract_path(trade_date)
        _write_json(path, contract)
        saved_paths.append(str(path))
        logger.info("[US_PREP_CONTRACT][SAVED] path=%s", path)
    except Exception as exc:
        err = f"runtime_contract: {exc}"
        save_errors.append(err)
        logger.error("[US_PREP_CONTRACT][ERROR] %s", err)

    # signals/us/prep_contract.json
    try:
        path = us_signals_prep_contract_path()
        _write_json(path, contract)
        saved_paths.append(str(path))
    except Exception as exc:
        save_errors.append(f"signals_contract: {exc}")

    # signals/us/latest_prep_contract.json
    try:
        path = us_signals_latest_prep_contract_path()
        _write_json(path, contract)
        saved_paths.append(str(path))
    except Exception as exc:
        save_errors.append(f"latest_contract: {exc}")

    ok = len(save_errors) == 0
    return {"ok": ok, "saved_paths": saved_paths, "errors": save_errors}


def save_us_prep_summary(contract: dict) -> None:
    """prep summary JSON + Markdown 저장."""
    trade_date = contract["trade_date"]

    # JSON
    try:
        _write_json(us_prep_summary_json_path(trade_date), contract)
        _write_json(us_prep_latest_summary_json_path(), contract)
    except Exception as exc:
        logger.warning("[US_PREP_SUMMARY][WARN] json save failed: %s", exc)

    # Markdown
    try:
        lines = [
            f"# US Prep Summary — {trade_date}",
            "",
            f"**status**: {contract.get('status')}",
            f"**trade_can_proceed**: {contract.get('trade_can_proceed')}",
            f"**env**: {contract.get('env')}",
            "",
            "## Stage Counts",
            f"- dynamic_universe_count: {contract.get('dynamic_universe_count')}",
            f"- candidate_pool_count: {contract.get('candidate_pool_count')}",
            f"- top50_count: {contract.get('top50_count')}",
            f"- final30_count: {contract.get('final30_count')}",
            f"- final30_scored_count: {contract.get('final30_scored_count')}",
            f"- score_nonzero_count: {contract.get('score_nonzero_count')}",
            "",
            "## Contract",
            f"- contract_ok: {contract.get('contract_ok')}",
            f"- trade_block_reason: {contract.get('trade_block_reason')}",
            f"- final30_complete: {contract.get('final30_complete')}",
            f"- final30_trade_ready: {contract.get('final30_trade_ready')}",
            f"- underfilled_final30: {contract.get('underfilled_final30')}",
            f"- underfilled_tier: {contract.get('underfilled_tier')}",
            f"- underfilled_capital_haircut: {contract.get('underfilled_capital_haircut')}",
            f"- effective_capital_scale: {contract.get('effective_capital_scale')}",
            f"- effective_max_new_positions: {contract.get('effective_max_new_positions')}",
            f"- agent_a_ok: {contract.get('agent_a_ok')}",
            f"- agent_b_ok: {contract.get('agent_b_ok')}",
            "",
        ]
        if contract.get("warnings"):
            lines += ["## Warnings"] + [f"- {w}" for w in contract["warnings"]] + [""]
        if contract.get("errors"):
            lines += ["## Errors"] + [f"- {e}" for e in contract["errors"]] + [""]

        md = "\n".join(lines)
        _write_text(us_prep_summary_md_path(trade_date), md)
        _write_text(us_prep_latest_summary_md_path(), md)
    except Exception as exc:
        logger.warning("[US_PREP_SUMMARY][WARN] md save failed: %s", exc)


def _score_positive(row: dict) -> bool:
    """row dict에서 양수 score가 있는지 확인."""
    for key in ("score_final", "final_score", "score"):
        try:
            if float(row.get(key) or 0) > 0:
                return True
        except Exception:
            pass
    scores = row.get("scores")
    if isinstance(scores, dict):
        for key in ("final", "score_final", "final_score"):
            try:
                if float(scores.get(key) or 0) > 0:
                    return True
            except Exception:
                pass
    return False


def _check_us_prep_guard_from_db(trade_date: str) -> dict:
    """파일 contract가 없을 때 DB fallback으로 prep guard를 수행."""
    from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist

    prep = load_latest_us_prep_status(trade_date, timeout_sec=20) or {}
    status = prep.get("status", "UNKNOWN")
    run_id = prep.get("run_id", "")
    result = prep.get("result") or {}
    if isinstance(result, str):
        try:
            result = json.loads(result)
        except Exception:
            result = {}
    if not isinstance(result, dict):
        result = {}

    trade_can_proceed_raw = (
        result.get("trade_can_proceed")
        if "trade_can_proceed" in result
        else prep.get("trade_can_proceed")
    )
    base_payload = {
        "contract": None,
        "source": "db",
        "session_can_run": True,
        "entry_can_proceed": False,
        "exit_can_proceed": True,
        "close_can_proceed": True,
        "new_buy_budget": 0,
        "prep_status": status,
        "run_id": run_id,
        "trade_block_reason": result.get("trade_block_reason", "ok"),
        "underfilled_tier": result.get("underfilled_tier"),
        "effective_capital_scale": result.get("effective_capital_scale"),
        "effective_max_new_positions": result.get("effective_max_new_positions"),
    }

    if trade_can_proceed_raw is not None:
        try:
            trade_can_proceed = int(trade_can_proceed_raw or 0)
        except (TypeError, ValueError):
            trade_can_proceed = 0
        if trade_can_proceed != 1:
            return {
                **base_payload,
                "ok": True,
                "guard_state": "PREP_DEGRADED_ENTRY_BLOCKED",
                "trade_can_proceed": True,
                "reason": f"PREP_DEGRADED_ENTRY_BLOCKED:db_trade_can_proceed=0:{result.get('trade_block_reason') or status}",
            }
    elif status not in ("OK", "OK_WITH_WARNINGS"):
        logger.warning(
            "[US_PREP_GUARD][DB_FALLBACK][FAIL] trade_date=%s reason=db_prep_status_not_ok status=%s",
            trade_date, status,
        )
        return {
            **base_payload,
            "ok": True,
            "guard_state": "PREP_MISSING_EXIT_ONLY",
            "trade_can_proceed": True,
            "reason": f"PREP_MISSING_EXIT_ONLY:db_prep_status_not_ok:{status}",
        }

    rows = load_locked_us_watchlist(
        trade_date=trade_date,
        min_count=10,
        allow_degraded=False,
        timeout_sec=20,
    ) or []

    locked_count = len(rows)
    score_nonzero_count = sum(1 for r in rows if _score_positive(r))
    count_payload = {**base_payload, "locked_count": locked_count, "final30_scored_count": locked_count, "score_nonzero_count": score_nonzero_count}

    if locked_count < 10:
        return {
            **count_payload,
            "ok": True,
            "guard_state": "PREP_DEGRADED_ENTRY_BLOCKED",
            "trade_can_proceed": True,
            "reason": f"PREP_DEGRADED_ENTRY_BLOCKED:db_locked_watchlist_count={locked_count}<10",
        }

    if score_nonzero_count <= 0:
        return {
            **count_payload,
            "ok": True,
            "guard_state": "PREP_DEGRADED_ENTRY_BLOCKED",
            "trade_can_proceed": True,
            "reason": "PREP_DEGRADED_ENTRY_BLOCKED:db_locked_watchlist_score_nonzero=0",
        }

    if score_nonzero_count != locked_count:
        return {
            **count_payload,
            "ok": True,
            "guard_state": "PREP_DEGRADED_ENTRY_BLOCKED",
            "trade_can_proceed": True,
            "reason": f"PREP_DEGRADED_ENTRY_BLOCKED:db_score_contract_failed:{score_nonzero_count}!={locked_count}",
        }

    logger.info(
        "[US_PREP_GUARD][OK] source=db trade_date=%s prep_status=%s run_id=%s "
        "final30=%d score_nonzero=%d trade_block_reason=%s underfilled_tier=%s effective_capital_scale=%s effective_max_new_positions=%s",
        trade_date, status, run_id, locked_count, score_nonzero_count,
        count_payload.get("trade_block_reason"), count_payload.get("underfilled_tier"),
        count_payload.get("effective_capital_scale"), count_payload.get("effective_max_new_positions"),
    )
    return {
        **count_payload,
        "ok": True,
        "guard_state": "PREP_OK",
        "trade_can_proceed": True,
        "entry_can_proceed": True,
        "exit_can_proceed": True,
        "close_can_proceed": True,
        "reason": "ok_db_fallback",
    }

def check_us_prep_guard(trade_date: str, session: str = "am") -> dict:
    """Session-aware contract-authoritative prep guard check."""
    from trader.us.path_contract import load_us_prep_contract

    contract = load_us_prep_contract(trade_date)
    if contract is None:
        logger.warning("[US_PREP_GUARD][CONTRACT_MISSING][DB_FALLBACK] trade_date=%s", trade_date)
        return _check_us_prep_guard_from_db(trade_date)

    if contract.get("trade_date") != trade_date:
        return {
            "ok": True,
            "guard_state": "PREP_STALE_EXIT_ONLY",
            "session_can_run": True,
            "trade_can_proceed": True,
            "entry_can_proceed": False,
            "exit_can_proceed": True,
            "close_can_proceed": True,
            "new_buy_budget": 0,
            "reason": f"PREP_STALE_EXIT_ONLY:prep_contract_trade_date_mismatch:{contract.get('trade_date')}!={trade_date}",
            "contract": contract,
            "source": "stale",
        }

    status = contract.get("status", "")
    try:
        trade_can_proceed = int(contract.get("trade_can_proceed", 0) or 0)
    except (TypeError, ValueError):
        trade_can_proceed = 0
    trade_block_reason = str(contract.get("trade_block_reason") or "")
    contract_ok = bool(contract.get("contract_ok", False))
    final30_trade_ready = bool(contract.get("final30_trade_ready", contract_ok and trade_can_proceed == 1))
    final30_scored_count = int(contract.get("final30_scored_count", 0) or 0)
    score_nonzero_count = int(contract.get("score_nonzero_count", 0) or 0)

    session_name = str(session or "am").lower()

    base_payload = {
        "contract": contract,
        "source": "runtime",
        "session": session_name,
        "prep_status": status,
        "trade_block_reason": trade_block_reason,
        "final30_scored_count": final30_scored_count,
        "score_nonzero_count": score_nonzero_count,
        "underfilled_tier": contract.get("underfilled_tier"),
        "effective_capital_scale": contract.get("effective_capital_scale"),
        "effective_max_new_positions": contract.get("effective_max_new_positions"),
        "final30_trade_ready": final30_trade_ready,
        "entry_can_proceed": bool(contract.get("entry_can_proceed", trade_can_proceed)),
        "exit_can_proceed": bool(contract.get("exit_can_proceed", trade_can_proceed)),
        "close_can_proceed": bool(contract.get("close_can_proceed", trade_can_proceed)),
    }

    has_split_permissions = any(k in contract for k in ("entry_can_proceed", "exit_can_proceed", "close_can_proceed"))
    entry_can_proceed = int(contract.get("entry_can_proceed", trade_can_proceed) or 0)
    exit_can_proceed = int(contract.get("exit_can_proceed", trade_can_proceed) or 0)
    close_can_proceed = int(contract.get("close_can_proceed", exit_can_proceed or trade_can_proceed) or 0)
    if not has_split_permissions:
        if trade_can_proceed != 1 and (status == "FAILED_CLUSTER_CAP_CONTRACT" or trade_block_reason == "sector_cap_violation_block"):
            entry_can_proceed = 0
            exit_can_proceed = 1
            close_can_proceed = 1
            logger.warning("[US_PREP_GUARD][LEGACY_CONTRACT_DEGRADED] split_permissions_missing entry_can_proceed=0 exit_can_proceed=1 close_can_proceed=1 original_status=%s", status)
        elif trade_can_proceed != 1:
            return {**base_payload, "ok": False, "trade_can_proceed": False, "reason": f"trade_can_proceed=0:{trade_block_reason or status}"}
    session_permission_ok = bool(close_can_proceed) if session_name == "close" else bool(entry_can_proceed or exit_can_proceed)
    if not session_permission_ok:
        reason = "close_can_proceed=0" if session_name == "close" else "entry_and_exit_can_proceed_false"
        return {**base_payload, "ok": False, "trade_can_proceed": False, "entry_can_proceed": bool(entry_can_proceed), "exit_can_proceed": bool(exit_can_proceed), "close_can_proceed": bool(close_can_proceed), "reason": f"{reason}:{trade_block_reason or status}"}
    reason = trade_block_reason or "ok"
    if session_name != "close":
        if (not final30_trade_ready) or final30_scored_count <= 0 or score_nonzero_count != final30_scored_count:
            entry_can_proceed = 0
            reason = reason if reason != "ok" else "entry_blocked_by_final30_quality"
    prep_sha = str(contract.get("git_commit_sha") or "")
    current_sha = _current_git_commit_sha()
    if prep_sha and current_sha != "unknown" and prep_sha != current_sha:
        entry_can_proceed = 0
        reason = "entry_blocked_by_prep_contract_version_mismatch"
        logger.warning("[US_CONTRACT_VERSION][MISMATCH] prep_sha=%s current_sha=%s action=entry_block_exit_allowed", prep_sha, current_sha)
    ok_payload = {**base_payload, "ok": True, "trade_can_proceed": True, "entry_can_proceed": bool(entry_can_proceed), "exit_can_proceed": bool(exit_can_proceed), "close_can_proceed": bool(close_can_proceed), "session_can_run": True, "reason": reason}
    ok_payload["guard_state"] = "PREP_OK" if bool(entry_can_proceed) else "PREP_DEGRADED_ENTRY_BLOCKED"
    if not bool(entry_can_proceed):
        ok_payload["new_buy_budget"] = 0
    logger.info("[US_PREP_GUARD][OK] session=%s entry_can_proceed=%d exit_can_proceed=%d close_can_proceed=%d reason=%s final30=%d", session_name, int(bool(entry_can_proceed)), int(bool(exit_can_proceed)), int(bool(close_can_proceed)), reason, final30_scored_count)
    return ok_payload
