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
from datetime import datetime
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

    contract_ok = (
        validation.get("ok", False)
        and final30_scored_count == 30
        and score_nonzero_count == 30
    )

    agent_a_ok = validation.get("agent_a_nonzero_count", 0) >= 25
    agent_b_ok = validation.get("agent_b_nonzero_count", 0) >= 25

    # trade_can_proceed 기준
    trade_can_proceed = int(
        status in ("OK", "OK_WITH_WARNINGS")
        and contract_ok
        and final30_scored_count == 30
        and score_nonzero_count == 30
    )

    warnings: list[str] = []
    errors: list[str] = []

    warnings.extend(dynamic_universe_result.get("warnings", []))
    warnings.extend(candidate_pool_result.get("status") == "OK_WITH_WARNINGS"
                    and ["candidate_pool_count_below_target"] or [])
    warnings.extend(validation.get("warnings", []))

    errors.extend(dynamic_universe_result.get("errors", []))
    errors.extend(validation.get("errors", []))

    contract = {
        "market": "US",
        "env": env,
        "trade_date": trade_date,
        "status": status,
        "trade_can_proceed": trade_can_proceed,
        "dynamic_universe_count": dynamic_universe_count,
        "candidate_pool_count": candidate_pool_count,
        "top50_count": top50_count,
        "final30_count": final30_count,
        "final30_scored_count": final30_scored_count,
        "score_nonzero_count": score_nonzero_count,
        "contract_ok": contract_ok,
        "agent_a_ok": agent_a_ok,
        "agent_b_ok": agent_b_ok,
        "validation": validation,
        "warnings": warnings,
        "errors": errors,
        "paths": paths,
        "created_at": datetime.utcnow().isoformat() + "Z",
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
        return {
            "ok": False,
            "trade_can_proceed": False,
            "reason": "prep_contract_missing",
            "contract": None,
            "source": "none",
        }

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
