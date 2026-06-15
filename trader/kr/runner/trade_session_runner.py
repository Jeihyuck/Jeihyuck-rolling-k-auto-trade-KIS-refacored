# -*- coding: utf-8 -*-
"""KR PB1 session-specific runner.

This module is intentionally thin: WSL session scripts call this KR/PB1 entrypoint
instead of the old all-in-one trader entrypoint. It enforces prep artifacts,
preopen handling, and KIS balance SAFE_STOP before handing live sessions to the
PB1 runner.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from trader.kis_wrapper import KisAPI, KisBalanceUnavailable

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]


def _setup_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


def _now_kst() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        return datetime.now()


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _rows_from_payload(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("data") or payload.get("items") or []
        return rows if isinstance(rows, list) else []
    return []


def _find_final30_source() -> tuple[Path | None, int]:
    today = _now_kst().strftime("%Y-%m-%d")
    candidates = [
        ROOT / "signals/kr/final30_scored.json",
        ROOT / "signals/kr/latest_final30_scored.json",
        ROOT / "runtime/kr/watchlist" / today / "final30_scored.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            rows = _rows_from_payload(_load_json(path))
        except Exception as exc:
            logger.warning("[KR_PREP_GUARD][WARN] source=%s err=%s", path, exc)
            continue
        if rows:
            return path, len(rows)
    return None, 0


def _write_prep_contract(rows: int) -> None:
    contract = {
        "contract_ok": rows == 30,
        "trade_can_proceed": rows == 30,
        "rows": rows,
        "written_by": "trader.kr.runner.trade_session_runner",
        "updated_at": _now_kst().isoformat(),
    }
    path = ROOT / "signals/kr/prep_contract.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_prep_summary(rows: int, status: str, reason: str) -> None:
    today = _now_kst().strftime("%Y-%m-%d")
    out_dir = ROOT / "reports/kr_prep" / today
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {"status": status, "reason": reason, "final30_rows": rows, "updated_at": _now_kst().isoformat()}
    (out_dir / "kr_prep_summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_prep(env: str) -> dict[str, Any]:
    os.environ.update({
        "STRATEGY_ENV": env,
        "KIS_ENV": os.getenv("KIS_ENV") or env,
        "STRATEGY_MODE": "PREP",
        "DRY_RUN": "1",
        "DISABLE_LIVE_TRADING": "1",
        "LIVE_TRADING_ENABLED": "0",
        "PB1_SESSION": "prep",
    })
    logger.info("[KR_SESSION][START] session=prep env=%s", env)
    import trader.prep_runner as prep_runner

    exit_code = int(prep_runner.main() or 0)
    source, rows = _find_final30_source()
    _write_prep_contract(rows)
    if rows != 30:
        _write_prep_summary(rows, "FAIL", "FINAL30_ROW_COUNT")
        logger.error("[KR_PREP][FAIL] reason=FINAL30_ROW_COUNT rows=%s", rows)
        logger.info("[RUN_SUMMARY][RESULT] session=prep status=FAIL reason=FINAL30_ROW_COUNT")
        return {"status": "FAIL", "reason": "FINAL30_ROW_COUNT", "rows": rows, "exit_code": exit_code}
    _write_prep_summary(rows, "OK", "KR_PREP_DONE")
    logger.info("[KR_PREP][ARTIFACT] path=%s rows=%s", source, rows)
    logger.info("[KR_PREP][CONTRACT] contract_ok=1 trade_can_proceed=1")
    logger.info("[KR_PREP][DONE] status=OK")
    logger.info("[RUN_SUMMARY][RESULT] session=prep status=OK reason=KR_PREP_DONE")
    return {"status": "OK", "reason": "KR_PREP_DONE", "rows": rows, "exit_code": exit_code}


def _guard_trade_session(session: str) -> dict[str, Any] | None:
    now = _now_kst()
    tag = f"KR_{session.upper()}" if session != "afternoon" else "KR_AFTERNOON"
    if session == "am" and now.hour < 9:
        logger.info("[%s][SKIP] reason=PREOPEN_NO_ORDER", tag)
        logger.info("[RUN_SUMMARY][RESULT] session=%s status=SKIP reason=PREOPEN_NO_ORDER", session)
        return {"status": "SKIP", "reason": "PREOPEN_NO_ORDER"}
    source, rows = _find_final30_source()
    if not source:
        logger.info("[%s][SKIP] reason=KR_PREP_ARTIFACT_MISSING", tag)
        logger.info("[RUN_SUMMARY][RESULT] session=%s status=SKIP reason=KR_PREP_ARTIFACT_MISSING", session)
        return {"status": "SKIP", "reason": "KR_PREP_ARTIFACT_MISSING"}
    logger.info("[KR_PREP_GUARD][OK] final30=%s source=%s", rows, source)
    return None


def _assert_balance_available(session: str) -> dict[str, Any] | None:
    try:
        KisAPI().get_balance_cached()
        return None
    except KisBalanceUnavailable as exc:
        logger.error("[KR_SESSION][SAFE_STOP] session=%s reason=KIS_BALANCE_UNAVAILABLE err=%s", session, exc, exc_info=True)
        logger.info("[RUN_SUMMARY][RESULT] session=%s status=SAFE_STOP reason=KIS_BALANCE_UNAVAILABLE", session)
        return {
            "status": "SAFE_STOP",
            "reason": "KIS_BALANCE_UNAVAILABLE",
            "order_allowed": 0,
            "entry_allowed": 0,
            "exit_allowed": 0,
            "performance_reliable": 0,
        }


def _run_pb1_session(session: str, env: str) -> dict[str, Any]:
    os.environ.update({
        "STRATEGY_ENV": env,
        "KIS_ENV": os.getenv("KIS_ENV") or env,
        "STRATEGY_MODE": "LIVE",
        "DRY_RUN": os.getenv("DRY_RUN", "0"),
        "DISABLE_LIVE_TRADING": os.getenv("DISABLE_LIVE_TRADING", "0"),
        "LIVE_TRADING_ENABLED": os.getenv("LIVE_TRADING_ENABLED", "1"),
        "KR_LIVE_TRADING_ENABLED": os.getenv("KR_LIVE_TRADING_ENABLED", "1"),
        "KR_ORDER_ARMED": os.getenv("KR_ORDER_ARMED", "1"),
        "PB1_SESSION": session,
    })
    logger.info("[KR_SESSION][START] session=%s env=%s", session, env)
    guarded = _guard_trade_session(session)
    if guarded is not None:
        return guarded
    stopped = _assert_balance_available(session)
    if stopped is not None:
        return stopped

    window = {"am": "morning", "afternoon": "day", "close": "close"}[session]
    import trader.pb1_runner as pb1_runner

    old_argv = sys.argv[:]
    try:
        sys.argv = ["kr-pb1-session", "--window", window, "--phase", "auto", "--env", env]
        exit_code = int(pb1_runner.main() or 0)
    finally:
        sys.argv = old_argv
    status = "OK" if exit_code == 0 else "FAIL"
    logger.info("[KR_SESSION][DONE] session=%s status=%s exit_code=%s", session, status, exit_code)
    logger.info("[RUN_SUMMARY][RESULT] session=%s status=%s", session, status)
    return {"status": status, "reason": "PB1_SESSION_DONE", "exit_code": exit_code}


def run_session(session: str, env: str = "practice") -> dict[str, Any]:
    session = session.strip().lower()
    if session == "prep":
        return _run_prep(env)
    if session in {"am", "afternoon", "close"}:
        return _run_pb1_session(session, env)
    raise ValueError(f"unsupported session={session!r}")


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    parser = argparse.ArgumentParser(description="KR PB1 session runner")
    parser.add_argument("--session", required=True, choices=["prep", "am", "afternoon", "close"])
    parser.add_argument("--env", default="practice")
    args = parser.parse_args(argv)
    result = run_session(args.session, args.env)
    return 0 if result.get("status") in {"OK", "SKIP", "SAFE_STOP"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
