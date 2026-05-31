# -*- coding: utf-8 -*-
"""US Path Contract — 미국장 prep 경로 계약 검증.

한국장 path_contract 파일 import 금지.
미국장 전용 경로만 사용한다.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from trader.us.runtime_paths import (
    us_dynamic_universe_path,
    us_candidate_pool_path,
    us_top50_scored_path,
    us_final30_scored_path,
    us_prep_contract_path,
    us_signals_latest_final30_scored_path,
    us_signals_latest_prep_contract_path,
)

logger = logging.getLogger(__name__)


def verify_us_prep_paths(trade_date: str) -> dict[str, Any]:
    """prep 결과 파일들이 존재하는지 검증.

    Returns:
        {"ok": bool, "missing": list[str], "present": list[str]}
    """
    required_paths = {
        "dynamic_universe": us_dynamic_universe_path(trade_date),
        "candidate_pool": us_candidate_pool_path(trade_date),
        "top50_scored": us_top50_scored_path(trade_date),
        "final30_scored": us_final30_scored_path(trade_date),
        "prep_contract": us_prep_contract_path(trade_date),
    }

    missing = []
    present = []
    for name, path in required_paths.items():
        if path.exists():
            present.append(name)
        else:
            missing.append(name)

    ok = len(missing) == 0
    return {"ok": ok, "missing": missing, "present": present}


def load_us_prep_contract(trade_date: str) -> dict | None:
    """prep_contract.json 로드.

    fallback:
    1. runtime/us/watchlist/{date}/prep_contract.json
    2. signals/us/latest_prep_contract.json

    Returns:
        dict 또는 None (파일 없거나 로드 실패 시)
    """
    primary = us_prep_contract_path(trade_date)
    if primary.exists():
        try:
            return json.loads(primary.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("[US_PATH_CONTRACT][WARN] load failed: %s path=%s", exc, primary)

    fallback = us_signals_latest_prep_contract_path()
    if fallback.exists():
        try:
            contract = json.loads(fallback.read_text(encoding="utf-8"))
            if contract.get("trade_date") == trade_date:
                return contract
            logger.warning(
                "[US_PATH_CONTRACT][STALE] latest_prep_contract trade_date=%s != %s",
                contract.get("trade_date"),
                trade_date,
            )
        except Exception as exc:
            logger.warning("[US_PATH_CONTRACT][WARN] fallback load failed: %s", exc)

    return None


def load_us_final30_scored(trade_date: str) -> list[dict] | None:
    """final30_scored.json 로드.

    fallback:
    1. runtime/us/watchlist/{date}/final30_scored.json
    2. signals/us/latest_final30_scored.json

    Returns:
        list[dict] 또는 None
    """
    primary = us_final30_scored_path(trade_date)
    if primary.exists():
        try:
            data = json.loads(primary.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("final30_scored") or data.get("rows") or []
        except Exception as exc:
            logger.warning("[US_PATH_CONTRACT][WARN] final30_scored load failed: %s", exc)

    fallback = us_signals_latest_final30_scored_path()
    if fallback.exists():
        try:
            data = json.loads(fallback.read_text(encoding="utf-8"))
            rows = data if isinstance(data, list) else data.get("final30_scored") or []
            if rows and str(rows[0].get("trade_date", "")) == trade_date:
                return rows
            logger.warning(
                "[US_PATH_CONTRACT][STALE] latest_final30_scored trade_date mismatch expected=%s",
                trade_date,
            )
        except Exception as exc:
            logger.warning("[US_PATH_CONTRACT][WARN] fallback final30_scored load failed: %s", exc)

    return None
