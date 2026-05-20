# -*- coding: utf-8 -*-
"""trader/us/utils/session_guard.py

미국장 전용 파일 기반 세션 가드.

DB guard만 의존하지 않고 파일 기반 중복 실행 방지 guard를 추가한다.
한국장 session guard는 수정하지 않는다.

guard 파일 경로:
  runtime/session_guard/us/YYYY-MM-DD/{session}.done

세션 완료 시 JSON 저장 예:
  {
    "market": "US",
    "session": "am",
    "trade_date": "2026-05-20",
    "run_id": "...",
    "started_at_et": "...",
    "finished_at_et": "...",
    "status": "OK_WITH_WARNINGS",
    "ticks": 31
  }

미국장 실행 시작 순서:
  1. stale window check
  2. file guard check       ← 이 모듈
  3. DB guard check
  4. session lock acquire
  5. trading loop start
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

NY_TZ = ZoneInfo("America/New_York")

_GUARD_BASE = Path("runtime/session_guard/us")


def _guard_path(trade_date: str, session: str) -> Path:
    return _GUARD_BASE / trade_date / f"{session}.done"


def check_us_session_file_guard(trade_date: str, session: str) -> dict:
    """파일 기반 세션 guard를 확인한다.

    Returns:
        dict with:
            already_ran (bool): True이면 해당 세션이 이미 완료됨
            guard_status: "DONE_FILE_FOUND" | "NOT_FOUND" | "READ_ERROR"
            payload (dict): 완료 파일 내용 (있는 경우)
    """
    path = _guard_path(trade_date, session)

    if not path.exists():
        return {
            "already_ran": False,
            "guard_status": "NOT_FOUND",
            "payload": {},
        }

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        logger.info(
            "[US_FILE_GUARD][DONE_FOUND] session=%s trade_date=%s path=%s status=%s",
            session,
            trade_date,
            str(path),
            payload.get("status"),
        )
        return {
            "already_ran": True,
            "guard_status": "DONE_FILE_FOUND",
            "payload": payload,
        }
    except Exception as exc:
        logger.warning(
            "[US_FILE_GUARD][READ_ERROR] session=%s trade_date=%s error=%s",
            session,
            trade_date,
            exc,
        )
        return {
            "already_ran": False,
            "guard_status": "READ_ERROR",
            "payload": {},
        }


def write_us_session_done_file(
    trade_date: str,
    session: str,
    run_id: str,
    started_at_et: str,
    finished_at_et: str,
    status: str,
    ticks: int = 0,
    extra: dict | None = None,
) -> Path | None:
    """세션 완료 파일을 기록한다.

    Args:
        trade_date: 거래일 (YYYY-MM-DD)
        session: "am" | "afternoon" | "close"
        run_id: GitHub run ID 또는 로컬 ID
        started_at_et: 시작 시각 (ISO format)
        finished_at_et: 종료 시각 (ISO format)
        status: "OK" | "OK_WITH_WARNINGS" | "FAILED" | ...
        ticks: 실행 tick 수
        extra: 추가 메타데이터 dict

    Returns:
        Path to written file, or None on failure
    """
    path = _guard_path(trade_date, session)

    payload: dict = {
        "market": "US",
        "session": session,
        "trade_date": trade_date,
        "run_id": run_id,
        "started_at_et": started_at_et,
        "finished_at_et": finished_at_et,
        "status": status,
        "ticks": ticks,
    }
    if extra:
        payload.update(extra)

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(
            "[US_FILE_GUARD][WRITE_DONE] session=%s trade_date=%s path=%s status=%s",
            session,
            trade_date,
            str(path),
            status,
        )
        return path
    except Exception as exc:
        logger.warning(
            "[US_FILE_GUARD][WRITE_ERROR] session=%s trade_date=%s error=%s",
            session,
            trade_date,
            exc,
        )
        return None


def now_et_iso() -> str:
    """현재 시각 ET ISO 문자열을 반환한다."""
    return datetime.now(tz=NY_TZ).isoformat()
