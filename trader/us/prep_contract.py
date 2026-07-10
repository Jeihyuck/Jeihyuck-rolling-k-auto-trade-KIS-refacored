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

    final30_complete = final30_scored_count == 30
    cap_violations = list(watchlist_result.get("cap_violations") or [])
    rotation_context = watchlist_result.get("rotation_context") or {}
    cluster_contract_ok = bool(watchlist_result.get("cluster_contract_ok", not cap_violations)) and not cap_violations
    if rotation_context.get("rotation_context_suspect") and str(rotation_context.get("rotation_suspect_policy") or "block") == "block":
        cluster_contract_ok = False
        if "ROTATION_CONTEXT_SUSPECT" not in cap_violations:
            cap_violations.append("ROTATION_CONTEXT_SUSPECT")
    contract_ok = (
        validation.get("ok", False)
        and final30_scored_count == 30
        and score_nonzero_count == final30_scored_count
    )
    if not cluster_contract_ok:
        status = "FAILED_CLUSTER_CAP_CONTRACT"
    elif not final30_complete:
        status = "OK_WITH_WARNINGS_CLUSTER_INCOMPLETE"

    agent_a_ok = validation.get("agent_a_nonzero_count", 0) >= 25
    agent_b_ok = validation.get("agent_b_nonzero_count", 0) >= 25

    market_state = watchlist_result.get("market_state_overlay") or watchlist_result.get("market_state") or {}
    if not isinstance(market_state, dict):
        market_state = {}
    market_regime = str(market_state.get("market_regime") or "NEUTRAL")
    force_entry_block = bool(market_state.get("force_entry_block", False))
    allow_new_buy = bool(market_state.get("allow_new_buy", True))
    trade_block_reason = "ok"
    if not cluster_contract_ok:
        trade_block_reason = "cluster_cap_contract_failed"
    elif cap_violations:
        trade_block_reason = "sector_cap_violation_block"
    elif not contract_ok:
        trade_block_reason = "contract_ok_false"
    elif not final30_complete:
        trade_block_reason = "final30_incomplete"
    elif score_nonzero_count != final30_scored_count:
        trade_block_reason = "score_contract_failed"
    elif market_regime == "RISK_OFF":
        trade_block_reason = "risk_off_entry_block"
    elif force_entry_block:
        trade_block_reason = "force_entry_block"
    elif not allow_new_buy:
        trade_block_reason = "allow_new_buy_false"
    elif status not in ("OK", "OK_WITH_WARNINGS"):
        trade_block_reason = "prep_status_error"
    trade_can_proceed = int(trade_block_reason == "ok")

    warnings: list[str] = []
    errors: list[str] = []

    warnings.extend(dynamic_universe_result.get("warnings", []))
    warnings.extend(candidate_pool_result.get("status") == "OK_WITH_WARNINGS"
                    and ["candidate_pool_count_below_target"] or [])
    warnings.extend(validation.get("warnings", []))

    errors.extend(dynamic_universe_result.get("errors", []))
    errors.extend(validation.get("errors", []))

    if market_regime == "RISK_OFF":
        trade_can_proceed = 0
        status = "RISK_OFF_ENTRY_BLOCKED" if market_state.get("market_state") != "DEFENSE_CRASH" else "DEFENSE_CRASH_ENTRY_BLOCKED"
    elif force_entry_block:
        trade_can_proceed = 0
        status = "DEFENSE_CRASH_ENTRY_BLOCKED"

    contract = {
        "market": "US",
        "contract_version": "us_sector_rotation_v3",
        "market_regime_version": "us_leading_regime_v1",
        "selector_version": watchlist_result.get("selector_version", "bucket_champion_v2"),
        "sector_cap_enforced": True,
        "env": env,
        "trade_date": trade_date,
        "status": status,
        "trade_can_proceed": trade_can_proceed,
        "trade_block_reason": trade_block_reason,
        "dynamic_universe_count": dynamic_universe_count,
        "candidate_pool_count": candidate_pool_count,
        "top50_count": top50_count,
        "final30_count": final30_count,
        "final30_scored_count": final30_scored_count,
        "score_nonzero_count": score_nonzero_count,
        "contract_ok": contract_ok,
        "cluster_contract_ok": cluster_contract_ok,
        "final30_complete": final30_complete,
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
        "capital_scale": market_state.get("capital_scale", market_state.get("exposure_multiplier", 1.0)),
        "max_ai_tech_ratio": market_state.get("max_ai_tech_ratio", 0.35),
        "max_single_cluster_ratio": market_state.get("max_single_cluster_ratio", 0.20),
        "max_new_positions": market_state.get("max_new_positions", 10),
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

    if status not in ("OK", "OK_WITH_WARNINGS"):
        logger.warning(
            "[US_PREP_GUARD][DB_FALLBACK][FAIL] trade_date=%s reason=db_prep_status_not_ok status=%s",
            trade_date, status,
        )
        return {
            "ok": False,
            "trade_can_proceed": False,
            "reason": f"db_prep_status_not_ok:{status}",
            "contract": None,
            "source": "db",
            "prep_status": status,
            "run_id": run_id,
        }

    rows = load_locked_us_watchlist(
        trade_date=trade_date,
        min_count=10,
        allow_degraded=False,
        timeout_sec=20,
    ) or []

    locked_count = len(rows)
    score_nonzero_count = sum(1 for r in rows if _score_positive(r))

    if locked_count < 10:
        logger.warning(
            "[US_PREP_GUARD][DB_FALLBACK][FAIL] trade_date=%s reason=db_locked_watchlist_count=%d<10",
            trade_date, locked_count,
        )
        return {
            "ok": False,
            "trade_can_proceed": False,
            "reason": f"db_locked_watchlist_count={locked_count}<10",
            "contract": None,
            "source": "db",
            "prep_status": status,
            "run_id": run_id,
            "locked_count": locked_count,
            "score_nonzero_count": score_nonzero_count,
        }

    if score_nonzero_count <= 0:
        logger.warning(
            "[US_PREP_GUARD][DB_FALLBACK][FAIL] trade_date=%s reason=db_locked_watchlist_score_nonzero=0 locked_count=%d",
            trade_date, locked_count,
        )
        return {
            "ok": False,
            "trade_can_proceed": False,
            "reason": "db_locked_watchlist_score_nonzero=0",
            "contract": None,
            "source": "db",
            "prep_status": status,
            "run_id": run_id,
            "locked_count": locked_count,
            "score_nonzero_count": score_nonzero_count,
        }

    logger.info(
        "[US_PREP_GUARD][OK] source=db trade_date=%s prep_status=%s run_id=%s "
        "final30=%d score_nonzero=%d",
        trade_date, status, run_id, locked_count, score_nonzero_count,
    )
    return {
        "ok": True,
        "trade_can_proceed": True,
        "reason": "ok_db_fallback",
        "contract": None,
        "source": "db",
        "prep_status": status,
        "run_id": run_id,
        "final30_scored_count": locked_count,
        "score_nonzero_count": score_nonzero_count,
        "locked_count": locked_count,
    }


def check_us_prep_guard(trade_date: str) -> dict:
    """AM/Afternoon session이 사용하는 prep guard 체크.

    Returns:
        {
            "ok": bool,
            "trade_can_proceed": bool,
            "reason": str,
            "contract": dict | None,
            "source": str,
        }
    """
    from trader.us.path_contract import load_us_prep_contract

    contract = load_us_prep_contract(trade_date)
    if contract is None:
        logger.warning("[US_PREP_GUARD][CONTRACT_MISSING][DB_FALLBACK] trade_date=%s", trade_date)
        return _check_us_prep_guard_from_db(trade_date)

    # contract trade_date 검증
    if contract.get("trade_date") != trade_date:
        return {
            "ok": False,
            "trade_can_proceed": False,
            "reason": f"prep_contract_trade_date_mismatch:{contract.get('trade_date')}!={trade_date}",
            "contract": contract,
            "source": "stale",
        }

    # contract 상태 검증
    status = contract.get("status", "")
    if status not in ("OK", "OK_WITH_WARNINGS"):
        return {
            "ok": False,
            "trade_can_proceed": False,
            "reason": f"prep_status_not_ok:{status}",
            "contract": contract,
            "source": "runtime",
        }

    if not contract.get("contract_ok", False):
        return {
            "ok": False,
            "trade_can_proceed": False,
            "reason": "contract_ok=false",
            "contract": contract,
            "source": "runtime",
        }

    if contract.get("final30_scored_count", 0) < 30:
        return {
            "ok": False,
            "trade_can_proceed": False,
            "reason": f"final30_scored_count={contract.get('final30_scored_count')}<30",
            "contract": contract,
            "source": "runtime",
        }

    if not contract.get("trade_can_proceed", 0):
        return {
            "ok": False,
            "trade_can_proceed": False,
            "reason": "trade_can_proceed=0",
            "contract": contract,
            "source": "runtime",
        }

    return {
        "ok": True,
        "trade_can_proceed": True,
        "reason": "ok",
        "contract": contract,
        "source": "runtime",
        "final30_scored_count": contract.get("final30_scored_count", 30),
        "score_nonzero_count": contract.get("score_nonzero_count", 0),
    }
